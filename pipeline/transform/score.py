# pipeline/transform/score.py
#
# Pre-compute fato_score_detalhe for all fornecedores.
#
# Design decisions:
#   - This module duplicates the weight constants from api/domain/fornecedor/score.py.
#     ADR (below) explains why we do not import from the api package.
#   - Each indicator is computed as a Polars group-by operation over the full
#     supplier population, producing one row per (fornecedor, indicator) pair.
#   - The result DataFrame schema matches fato_score_detalhe in schema.sql.
#   - Indicators that require cross-referencing multiple DataFrames (e.g.
#     SOCIO_EM_MULTIPLAS_FORNECEDORAS) are computed with explicit joins so
#     each step remains understandable in isolation.
#
# ADR: Why not import from api/domain?
#   The pipeline is an offline standalone artefact. Importing from the API
#   package at pipeline build time would couple the pipeline's runtime
#   environment to the web stack (FastAPI, Pydantic, etc.) and make the
#   offline build dependent on web stack installation. Constants copied here
#   are annotated with their source so divergence is caught in code review.
#   Source of truth: api/domain/fornecedor/score.py :: PESOS
#
# ADR: Score and Alerts are independent dimensions (mirrors api/).
#   This module NEVER imports or calls alertas.py. The two modules must
#   never cross-call each other — an alert does not contribute to the score
#   and a score indicator does not generate an alert.
#
# ADR: Vectorized indicator functions (performance).
#   All indicator functions previously used `iter_rows` + Python dict-append
#   loops to build the output list. These have been replaced with Polars
#   `select` + `concat_str` + `to_dicts()` to keep all transformation work
#   inside the Polars engine (zero Python-level row iteration).
#   The only exception is `_socio_em_multiplas_batch`, which retains
#   `iter_rows` because its evidence field requires zipping two list-columns
#   (nomes × qtd_empresas) whose contents can only be assembled at Python
#   level after aggregation. The aggregated frame is already small (one row
#   per fornecedor), so per-row Python work is negligible there.
#   set→list→is_in anti-patterns have been replaced with semi-joins, which
#   avoid materialising the full pk_fornecedor list in Python heap memory.
#
# Invariants:
#   - calcular_scores_batch is a pure function over DataFrames. No IO.
#   - Output columns match fato_score_detalhe schema exactly.
#   - pk_score_detalhe is a sequential integer assigned at the end.
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import polars as pl

from pipeline.transform.cnae_mapping import CNAE_CATEGORIES, INCOMPATIBLE_COMBOS
from pipeline.transform.cruzamentos import detectar_mesmo_endereco

# ---------------------------------------------------------------------------
# Weight constants — source of truth: api/domain/fornecedor/score.py :: PESOS
# ---------------------------------------------------------------------------
_PESO_CAPITAL_SOCIAL_BAIXO: int = 15
_PESO_EMPRESA_RECENTE: int = 10
_PESO_CNAE_INCOMPATIVEL: int = 10
_PESO_SOCIO_EM_MULTIPLAS_FORNECEDORAS: int = 20
_PESO_MESMO_ENDERECO: int = 15
_PESO_FORNECEDOR_EXCLUSIVO: int = 10
_PESO_SEM_FUNCIONARIOS: int = 10
_PESO_CRESCIMENTO_SUBITO: int = 10
_PESO_SANCAO_HISTORICA: int = 5

# ---------------------------------------------------------------------------
# Business-rule thresholds — source of truth: api/application/services/score_service.py
# ---------------------------------------------------------------------------
_CAPITAL_THRESHOLD_GENERICO = Decimal("10000")
_CONTRATO_MINIMO_PARA_CAPITAL = Decimal("100000")
_MESES_EMPRESA_RECENTE = 6
_MULTIPLAS_FORNECEDORAS_THRESHOLD = 3


def calcular_scores_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    sancoes_df: pl.DataFrame,
) -> pl.DataFrame:
    """Pre-compute score indicator rows for every fornecedor.

    Args:
        empresas_df:  DataFrame with dim_fornecedor columns, including:
                      pk_fornecedor (int), cnpj_basico (str), capital_social (float|null),
                      data_abertura (date|null), cnpj (str), cnae_principal (str|null),
                      logradouro (str|null).
        socios_df:    DataFrame with: cnpj_basico (str), nome_socio (str),
                      qtd_empresas_governo (int) — must have been enriched by
                      cruzamentos.enriquecer_socios before calling this function.
        contratos_df: DataFrame with: fk_fornecedor (int), valor (float),
                      data_assinatura (date|null), fk_orgao (int|str),
                      objeto_categoria (str|null).
        sancoes_df:   DataFrame with: fk_fornecedor (int), data_fim (date|null).

    Returns:
        DataFrame matching fato_score_detalhe schema:
          pk_score_detalhe (int), fk_fornecedor (int), indicador (str),
          peso (int), descricao (str), evidencia (str), calculado_em (datetime).
    """
    calculado_em = datetime.now(tz=UTC).replace(tzinfo=None)
    rows: list[dict[str, object]] = []

    rows.extend(_capital_social_baixo_batch(empresas_df, contratos_df, calculado_em))
    rows.extend(_empresa_recente_batch(empresas_df, contratos_df, calculado_em))
    rows.extend(_sancao_historica_batch(empresas_df, sancoes_df, calculado_em))
    rows.extend(_socio_em_multiplas_batch(empresas_df, socios_df, calculado_em))
    rows.extend(_fornecedor_exclusivo_batch(empresas_df, contratos_df, calculado_em))
    rows.extend(_cnae_incompativel_batch(empresas_df, contratos_df, calculado_em))
    rows.extend(_mesmo_endereco_batch(empresas_df, calculado_em))
    rows.extend(_crescimento_subito_batch(empresas_df, contratos_df, calculado_em))
    rows.extend(_sem_funcionarios_batch(empresas_df, contratos_df, calculado_em))

    if not rows:
        return _empty_score_df()

    result = pl.DataFrame(rows)
    result = result.with_row_index(name="pk_score_detalhe", offset=1)
    return result.select(
        [
            "pk_score_detalhe",
            "fk_fornecedor",
            "indicador",
            "peso",
            "descricao",
            "evidencia",
            "calculado_em",
        ]
    )


# ---------------------------------------------------------------------------
# Private indicator computers
# ---------------------------------------------------------------------------


def _capital_social_baixo_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """CAPITAL_SOCIAL_BAIXO: capital < 10k AND total contracts > 100k."""
    if "pk_fornecedor" not in empresas_df.columns or contratos_df.is_empty():
        return []

    # Aggregate total contract value per fornecedor.
    valor_col = "valor" if "valor" in contratos_df.columns else None
    if valor_col is None:
        return []

    totais = contratos_df.group_by("fk_fornecedor").agg(pl.col(valor_col).cast(pl.Float64).sum().alias("valor_total"))

    # Join against empresas.
    joined = empresas_df.select(
        [
            pl.col("pk_fornecedor"),
            pl.col("capital_social").cast(pl.Float64).alias("capital_social"),
        ]
    ).join(totais, left_on="pk_fornecedor", right_on="fk_fornecedor", how="inner")

    capital_threshold = float(_CAPITAL_THRESHOLD_GENERICO)
    contrato_threshold = float(_CONTRATO_MINIMO_PARA_CAPITAL)

    flagged = joined.filter(
        pl.col("capital_social").is_not_null()
        & (pl.col("capital_social") < capital_threshold)
        & (pl.col("valor_total") > contrato_threshold)
    )

    if flagged.is_empty():
        return []

    result_df = flagged.select(
        pl.col("pk_fornecedor").alias("fk_fornecedor"),
        pl.lit("CAPITAL_SOCIAL_BAIXO").alias("indicador"),
        pl.lit(_PESO_CAPITAL_SOCIAL_BAIXO).alias("peso"),
        pl.concat_str(
            [
                pl.lit("Capital social R$"),
                pl.col("capital_social").round(2).cast(pl.Utf8),
                pl.lit(" desproporcional a contratos R$"),
                pl.col("valor_total").round(2).cast(pl.Utf8),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("capital="),
                pl.col("capital_social").cast(pl.Utf8),
                pl.lit(", valor_total_contratos="),
                pl.col("valor_total").cast(pl.Utf8),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _empresa_recente_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """EMPRESA_RECENTE: company opened < 6 months before first contract."""
    if contratos_df.is_empty() or "data_assinatura" not in contratos_df.columns:
        return []
    if "data_abertura" not in empresas_df.columns:
        return []

    # Earliest contract date per fornecedor.
    primeiros = (
        contratos_df.filter(pl.col("data_assinatura").is_not_null())
        .group_by("fk_fornecedor")
        .agg(pl.col("data_assinatura").min().alias("primeiro_contrato"))
    )

    joined = empresas_df.select(
        [
            pl.col("pk_fornecedor"),
            pl.col("data_abertura"),
        ]
    ).join(primeiros, left_on="pk_fornecedor", right_on="fk_fornecedor", how="inner")

    joined = joined.filter(pl.col("data_abertura").is_not_null())

    if joined.is_empty():
        return []

    # Compute days from abertura to first contract.
    joined = joined.with_columns(
        (pl.col("primeiro_contrato").cast(pl.Date) - pl.col("data_abertura").cast(pl.Date))
        .dt.total_days()
        .alias("dias")
    )

    # 6 months ≈ 6 * 30.44 days
    threshold_days = _MESES_EMPRESA_RECENTE * 30.44
    flagged = joined.filter(pl.col("dias") < threshold_days)

    if flagged.is_empty():
        return []

    result_df = flagged.select(
        pl.col("pk_fornecedor").alias("fk_fornecedor"),
        pl.lit("EMPRESA_RECENTE").alias("indicador"),
        pl.lit(_PESO_EMPRESA_RECENTE).alias("peso"),
        pl.concat_str(
            [
                pl.lit("Empresa aberta em "),
                pl.col("data_abertura").cast(pl.Utf8),
                pl.lit(" obteve primeiro contrato em "),
                pl.col("primeiro_contrato").cast(pl.Utf8),
                pl.lit(" ("),
                pl.col("dias").cast(pl.Utf8),
                pl.lit(" dias depois)"),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("data_abertura="),
                pl.col("data_abertura").cast(pl.Utf8),
                pl.lit(", primeiro_contrato="),
                pl.col("primeiro_contrato").cast(pl.Utf8),
                pl.lit(", dias="),
                pl.col("dias").cast(pl.Utf8),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _sancao_historica_batch(
    empresas_df: pl.DataFrame,
    sancoes_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """SANCAO_HISTORICA: expired sanction (data_fim not null and in the past)."""
    if sancoes_df.is_empty():
        return []

    today = datetime.now(tz=UTC).date()

    # Expired = data_fim is not null AND data_fim <= today.
    if "data_fim" not in sancoes_df.columns:
        return []

    expiradas = sancoes_df.filter(
        pl.col("data_fim").is_not_null() & (pl.col("data_fim").cast(pl.Date) <= pl.lit(today))
    )

    if expiradas.is_empty():
        return []

    # Count expired sanctions per fornecedor.
    contagem = expiradas.group_by("fk_fornecedor").agg(pl.len().alias("qtd_expiradas"))

    # Restrict to known fornecedores via semi-join (avoids materialising a Python set).
    contagem = contagem.join(
        empresas_df.select("pk_fornecedor").rename({"pk_fornecedor": "fk_fornecedor"}),
        on="fk_fornecedor",
        how="semi",
    )

    if contagem.is_empty():
        return []

    result_df = contagem.select(
        pl.col("fk_fornecedor"),
        pl.lit("SANCAO_HISTORICA").alias("indicador"),
        pl.lit(_PESO_SANCAO_HISTORICA).alias("peso"),
        pl.concat_str(
            [
                pl.col("qtd_expiradas").cast(pl.Utf8),
                pl.lit(" sancao(oes) historica(s) expirada(s)"),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("sancoes_expiradas="),
                pl.col("qtd_expiradas").cast(pl.Utf8),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _socio_em_multiplas_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """SOCIO_EM_MULTIPLAS_FORNECEDORAS: at least one sócio appears in 3+ companies.

    ADR: iter_rows retained for evidence field.
      The aggregation yields list-columns (nomes, qtd_empresas) that must be
      zipped together to build a human-readable evidence string. Polars cannot
      express a cross-element zip of two list columns with concat_str. Since
      the aggregated frame has at most one row per fornecedor (already small),
      per-row Python iteration here is negligible.
    """
    if socios_df.is_empty() or "qtd_empresas_governo" not in socios_df.columns:
        return []

    # Find sócios that appear in >= 3 government-supplier companies.
    socios_multiplas = socios_df.filter(pl.col("qtd_empresas_governo") >= _MULTIPLAS_FORNECEDORAS_THRESHOLD)

    if socios_multiplas.is_empty():
        return []

    # Need to map cnpj_basico back to pk_fornecedor.
    if "pk_fornecedor" not in empresas_df.columns or "cnpj_basico" not in empresas_df.columns:
        return []

    basico_to_pk = empresas_df.select(
        [
            pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
            pl.col("pk_fornecedor"),
        ]
    )

    # Join socios_multiplas against empresas to get fk_fornecedor.
    socios_norm = socios_multiplas.with_columns(pl.col("cnpj_basico").str.zfill(8))

    joined = socios_norm.join(basico_to_pk, on="cnpj_basico", how="inner")

    if joined.is_empty():
        return []

    # Aggregate: per fornecedor, count how many of its sócios are in multiplas.
    per_fornecedor = joined.group_by(["pk_fornecedor", "cnpj_basico"]).agg(
        [
            pl.len().alias("qtd_socios_multiplas"),
            pl.col("nome_socio").alias("nomes"),
            pl.col("qtd_empresas_governo").alias("qtd_empresas"),
        ]
    )

    rows: list[dict[str, object]] = []
    for row in per_fornecedor.iter_rows(named=True):
        nomes = row["nomes"]
        qtds = row["qtd_empresas"]
        pairs = list(zip(nomes, qtds, strict=False))
        rows.append(
            {
                "fk_fornecedor": row["pk_fornecedor"],
                "indicador": "SOCIO_EM_MULTIPLAS_FORNECEDORAS",
                "peso": _PESO_SOCIO_EM_MULTIPLAS_FORNECEDORAS,
                "descricao": (
                    f"{row['qtd_socios_multiplas']} socio(s) presente(s) em 3+ empresas fornecedoras do governo"
                ),
                "evidencia": f"socios={pairs}",
                "calculado_em": calculado_em,
            }
        )
    return rows


def _fornecedor_exclusivo_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """FORNECEDOR_EXCLUSIVO: all contracts are with a single orgao."""
    if contratos_df.is_empty() or "fk_orgao" not in contratos_df.columns:
        return []

    orgao_counts = (
        contratos_df.group_by("fk_fornecedor")
        .agg(
            [
                pl.col("fk_orgao").n_unique().alias("qtd_orgaos"),
                pl.col("fk_orgao").first().alias("orgao_unico"),
                pl.len().alias("qtd_contratos"),
            ]
        )
        .filter(pl.col("qtd_orgaos") == 1)
    )

    if orgao_counts.is_empty():
        return []

    # Restrict to known fornecedores via semi-join (avoids materialising a Python set).
    orgao_counts = orgao_counts.join(
        empresas_df.select("pk_fornecedor").rename({"pk_fornecedor": "fk_fornecedor"}),
        on="fk_fornecedor",
        how="semi",
    )

    if orgao_counts.is_empty():
        return []

    result_df = orgao_counts.select(
        pl.col("fk_fornecedor"),
        pl.lit("FORNECEDOR_EXCLUSIVO").alias("indicador"),
        pl.lit(_PESO_FORNECEDOR_EXCLUSIVO).alias("peso"),
        pl.concat_str(
            [
                pl.lit("Todos os "),
                pl.col("qtd_contratos").cast(pl.Utf8),
                pl.lit(" contrato(s) sao com o mesmo orgao"),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("orgao_codigo="),
                pl.col("orgao_unico").cast(pl.Utf8),
                pl.lit(", qtd_contratos="),
                pl.col("qtd_contratos").cast(pl.Utf8),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _cnae_incompativel_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """CNAE_INCOMPATIVEL: company CNAE is incompatible with contract object category.

    ADR: Materialized incompatibility table replaces per-row Python calls.
      The original implementation iterated each (fornecedor, cnae, objeto) row
      and called cnae_incompativel_com_objeto() for each one — O(n) Python
      function calls. We now expand CNAE_CATEGORIES × INCOMPATIBLE_COMBOS into
      a flat DataFrame of (cnae_principal, incompat_objeto, cnae_category) pairs
      and perform a single inner join. All incompatibility checks run inside
      Polars, eliminating per-row Python overhead entirely.
    """
    if contratos_df.is_empty() or "cnae_principal" not in empresas_df.columns:
        return []
    if "objeto_categoria" not in contratos_df.columns:
        return []
    if "pk_fornecedor" not in empresas_df.columns:
        return []

    # Get distinct (fk_fornecedor, objeto_categoria) pairs from contracts.
    categorias = (
        contratos_df.filter(pl.col("objeto_categoria").is_not_null())
        .select(["fk_fornecedor", "objeto_categoria"])
        .unique()
    )

    if categorias.is_empty():
        return []

    # Join against empresas to get cnae_principal.
    joined = categorias.join(
        empresas_df.select(["pk_fornecedor", "cnae_principal"]).filter(pl.col("cnae_principal").is_not_null()),
        left_on="fk_fornecedor",
        right_on="pk_fornecedor",
        how="inner",
    )

    if joined.is_empty():
        return []

    # Build the incompatibility lookup as a DataFrame so the check runs inside Polars.
    incompat_rows: list[dict[str, str]] = []
    for cnae, category in CNAE_CATEGORIES.items():
        for incompat_category in INCOMPATIBLE_COMBOS.get(category, set()):
            incompat_rows.append(
                {
                    "cnae_principal": cnae,
                    "incompat_objeto": incompat_category,
                    "cnae_category": category,
                }
            )

    if not incompat_rows:
        return []

    incompat_df = pl.DataFrame(incompat_rows)

    # Inner join: keep only rows where (cnae_principal, objeto_categoria) is incompatible.
    # unique(subset=["fk_fornecedor"]) ensures at most one indicator row per fornecedor.
    flagged = joined.join(
        incompat_df,
        left_on=["cnae_principal", "objeto_categoria"],
        right_on=["cnae_principal", "incompat_objeto"],
        how="inner",
    ).unique(subset=["fk_fornecedor"], keep="first")

    if flagged.is_empty():
        return []

    result_df = flagged.select(
        pl.col("fk_fornecedor"),
        pl.lit("CNAE_INCOMPATIVEL").alias("indicador"),
        pl.lit(_PESO_CNAE_INCOMPATIVEL).alias("peso"),
        pl.concat_str(
            [
                pl.lit("CNAE "),
                pl.col("cnae_principal"),
                pl.lit(" ("),
                pl.col("cnae_category"),
                pl.lit(") incompativel com objeto contratado ("),
                pl.col("objeto_categoria"),
                pl.lit(")"),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("cnae="),
                pl.col("cnae_principal"),
                pl.lit(", categoria_cnae="),
                pl.col("cnae_category"),
                pl.lit(", objeto_categoria="),
                pl.col("objeto_categoria"),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _mesmo_endereco_batch(
    empresas_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """MESMO_ENDERECO: company shares address with another government supplier."""
    if "logradouro" not in empresas_df.columns or "cnpj" not in empresas_df.columns:
        return []
    if "pk_fornecedor" not in empresas_df.columns:
        return []

    pairs = detectar_mesmo_endereco(empresas_df)

    if pairs.is_empty():
        return []

    # Build pk mapping once and reuse for both sides of the pair.
    pk_map = empresas_df.select(["cnpj", "pk_fornecedor"])

    # Side A: cnpj_a is the flagged supplier, cnpj_b is the partner.
    side_a = (
        pairs.select(
            pl.col("cnpj_a").alias("cnpj"),
            pl.col("cnpj_b").alias("cnpj_parceiro"),
            pl.col("endereco_compartilhado"),
        )
        .join(pk_map, on="cnpj", how="inner")
        .rename({"pk_fornecedor": "fk_fornecedor"})
    )

    # Side B: cnpj_b is the flagged supplier, cnpj_a is the partner.
    side_b = (
        pairs.select(
            pl.col("cnpj_b").alias("cnpj"),
            pl.col("cnpj_a").alias("cnpj_parceiro"),
            pl.col("endereco_compartilhado"),
        )
        .join(pk_map, on="cnpj", how="inner")
        .rename({"pk_fornecedor": "fk_fornecedor"})
    )

    flagged = pl.concat([side_a, side_b]).unique(subset=["fk_fornecedor"], keep="first")

    if flagged.is_empty():
        return []

    result_df = flagged.select(
        pl.col("fk_fornecedor"),
        pl.lit("MESMO_ENDERECO").alias("indicador"),
        pl.lit(_PESO_MESMO_ENDERECO).alias("peso"),
        pl.concat_str(
            [
                pl.lit("Compartilha endereco com "),
                pl.col("cnpj_parceiro"),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("endereco="),
                pl.col("endereco_compartilhado"),
                pl.lit(", cnpj_parceiro="),
                pl.col("cnpj_parceiro"),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _crescimento_subito_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """CRESCIMENTO_SUBITO: contract value jumps 5x+ between consecutive years."""
    if contratos_df.is_empty() or "data_assinatura" not in contratos_df.columns:
        return []
    if "valor" not in contratos_df.columns or "fk_fornecedor" not in contratos_df.columns:
        return []

    # Compute annual contract value per fornecedor.
    with_year = contratos_df.filter(pl.col("data_assinatura").is_not_null()).with_columns(
        pl.col("data_assinatura").cast(pl.Date).dt.year().alias("ano")
    )

    if with_year.is_empty():
        return []

    anuais = with_year.group_by(["fk_fornecedor", "ano"]).agg(
        pl.col("valor").cast(pl.Float64).sum().alias("valor_anual")
    )

    # Self-join: compare year N with year N-1.
    prev = anuais.rename({"ano": "ano_prev", "valor_anual": "valor_prev"})
    curr = anuais.with_columns((pl.col("ano") - 1).alias("ano_prev_join"))

    joined = curr.join(
        prev,
        left_on=["fk_fornecedor", "ano_prev_join"],
        right_on=["fk_fornecedor", "ano_prev"],
        how="inner",
    )

    # Flag when current year >= 5x previous year and previous year > 0.
    flagged = joined.filter((pl.col("valor_prev") > 0) & (pl.col("valor_anual") >= pl.col("valor_prev") * 5))

    if flagged.is_empty():
        return []

    # Take the most recent jump per fornecedor; restrict to known suppliers via semi-join.
    dedup = flagged.sort("ano", descending=True).unique(subset=["fk_fornecedor"], keep="first")
    dedup = dedup.join(
        empresas_df.select("pk_fornecedor").rename({"pk_fornecedor": "fk_fornecedor"}),
        on="fk_fornecedor",
        how="semi",
    )

    if dedup.is_empty():
        return []

    result_df = dedup.with_columns((pl.col("valor_anual") / pl.col("valor_prev")).alias("razao")).select(
        pl.col("fk_fornecedor"),
        pl.lit("CRESCIMENTO_SUBITO").alias("indicador"),
        pl.lit(_PESO_CRESCIMENTO_SUBITO).alias("peso"),
        pl.concat_str(
            [
                pl.lit("Valor contratado saltou "),
                pl.col("razao").round(1).cast(pl.Utf8),
                pl.lit("x entre "),
                (pl.col("ano") - 1).cast(pl.Utf8),
                pl.lit(" e "),
                pl.col("ano").cast(pl.Utf8),
            ]
        ).alias("descricao"),
        pl.concat_str(
            [
                pl.lit("ano_anterior="),
                (pl.col("ano") - 1).cast(pl.Utf8),
                pl.lit(", valor_anterior="),
                pl.col("valor_prev").round(2).cast(pl.Utf8),
                pl.lit(", ano_atual="),
                pl.col("ano").cast(pl.Utf8),
                pl.lit(", valor_atual="),
                pl.col("valor_anual").round(2).cast(pl.Utf8),
                pl.lit(", razao="),
                pl.col("razao").round(1).cast(pl.Utf8),
                pl.lit("x"),
            ]
        ).alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _sem_funcionarios_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """SEM_FUNCIONARIOS: qtd_funcionarios == 0 with at least one active contract.

    This indicator flags companies that report zero employees in RAIS but have
    government contracts. A legitimate company cannot execute contracts with
    zero staff — this pattern is a strong signal of a shell company.

    The indicator only activates when empresas_df carries the qtd_funcionarios
    column (populated by the RAIS enrichment step in main.py). If the column
    is absent (e.g. RAIS was not yet merged), the function returns no rows
    rather than crashing, preserving the defensive-guard pattern used by all
    other indicators.

    Args:
        empresas_df:  dim_fornecedor staging DataFrame. May contain
                      qtd_funcionarios (int | null) if RAIS was merged.
        contratos_df: fato_contrato staging DataFrame.
        calculado_em: Timestamp of the batch computation.

    Returns:
        One dict per flagged fornecedor, with fato_score_detalhe columns.
    """
    # Guard: indicator requires the RAIS enrichment column.
    if "qtd_funcionarios" not in empresas_df.columns:
        return []

    if "pk_fornecedor" not in empresas_df.columns:
        return []

    if contratos_df.is_empty() or "fk_fornecedor" not in contratos_df.columns:
        return []

    # Find fornecedores with qtd_funcionarios == 0 (not null — null means
    # RAIS data was not available for that CNPJ, which is different from
    # explicitly reporting zero employees).
    sem_func = empresas_df.filter(pl.col("qtd_funcionarios").is_not_null() & (pl.col("qtd_funcionarios") == 0)).select(
        ["pk_fornecedor"]
    )

    if sem_func.is_empty():
        return []

    # Cross with contratos to keep only those that actually have contracts.
    fornecedores_com_contrato = contratos_df.select("fk_fornecedor").unique()

    flagged = sem_func.join(
        fornecedores_com_contrato,
        left_on="pk_fornecedor",
        right_on="fk_fornecedor",
        how="inner",
    )

    if flagged.is_empty():
        return []

    result_df = flagged.select(
        pl.col("pk_fornecedor").alias("fk_fornecedor"),
        pl.lit("SEM_FUNCIONARIOS").alias("indicador"),
        pl.lit(_PESO_SEM_FUNCIONARIOS).alias("peso"),
        pl.lit("Empresa declara zero funcionarios no RAIS mas possui contratos governamentais").alias("descricao"),
        pl.lit("qtd_funcionarios=0, contratos_governamentais=sim").alias("evidencia"),
        pl.lit(calculado_em).alias("calculado_em"),
    )
    return result_df.to_dicts()


def _empty_score_df() -> pl.DataFrame:
    """Return an empty DataFrame with the fato_score_detalhe schema."""
    return pl.DataFrame(
        {
            "pk_score_detalhe": pl.Series([], dtype=pl.Int64),
            "fk_fornecedor": pl.Series([], dtype=pl.Int64),
            "indicador": pl.Series([], dtype=pl.Utf8),
            "peso": pl.Series([], dtype=pl.Int64),
            "descricao": pl.Series([], dtype=pl.Utf8),
            "evidencia": pl.Series([], dtype=pl.Utf8),
            "calculado_em": pl.Series([], dtype=pl.Datetime),
        }
    )
