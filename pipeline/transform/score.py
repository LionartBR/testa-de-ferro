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
# Invariants:
#   - calcular_scores_batch is a pure function over DataFrames. No IO.
#   - Output columns match fato_score_detalhe schema exactly.
#   - pk_score_detalhe is a sequential integer assigned at the end.
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import polars as pl

from pipeline.transform.cnae_mapping import cnae_incompativel_com_objeto, get_cnae_category
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

    rows: list[dict[str, object]] = []
    for row in flagged.iter_rows(named=True):
        rows.append(
            {
                "fk_fornecedor": row["pk_fornecedor"],
                "indicador": "CAPITAL_SOCIAL_BAIXO",
                "peso": _PESO_CAPITAL_SOCIAL_BAIXO,
                "descricao": (
                    f"Capital social R${row['capital_social']:,.2f} "
                    f"desproporcional a contratos R${row['valor_total']:,.2f}"
                ),
                "evidencia": (f"capital={row['capital_social']}, valor_total_contratos={row['valor_total']}"),
                "calculado_em": calculado_em,
            }
        )
    return rows


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

    rows: list[dict[str, object]] = []
    for row in flagged.iter_rows(named=True):
        rows.append(
            {
                "fk_fornecedor": row["pk_fornecedor"],
                "indicador": "EMPRESA_RECENTE",
                "peso": _PESO_EMPRESA_RECENTE,
                "descricao": (
                    f"Empresa aberta em {row['data_abertura']} obteve "
                    f"primeiro contrato em {row['primeiro_contrato']} "
                    f"({row['dias']} dias depois)"
                ),
                "evidencia": (
                    f"data_abertura={row['data_abertura']}, "
                    f"primeiro_contrato={row['primeiro_contrato']}, "
                    f"dias={row['dias']}"
                ),
                "calculado_em": calculado_em,
            }
        )
    return rows


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

    # Restrict to known fornecedores.
    known_fornecedores = set(empresas_df["pk_fornecedor"].to_list())
    contagem = contagem.filter(pl.col("fk_fornecedor").is_in(list(known_fornecedores)))

    rows: list[dict[str, object]] = []
    for row in contagem.iter_rows(named=True):
        rows.append(
            {
                "fk_fornecedor": row["fk_fornecedor"],
                "indicador": "SANCAO_HISTORICA",
                "peso": _PESO_SANCAO_HISTORICA,
                "descricao": f"{row['qtd_expiradas']} sancao(oes) historica(s) expirada(s)",
                "evidencia": f"sancoes_expiradas={row['qtd_expiradas']}",
                "calculado_em": calculado_em,
            }
        )
    return rows


def _socio_em_multiplas_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """SOCIO_EM_MULTIPLAS_FORNECEDORAS: at least one sócio appears in 3+ companies."""
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

    known_fornecedores = set(empresas_df["pk_fornecedor"].to_list())
    orgao_counts = orgao_counts.filter(pl.col("fk_fornecedor").is_in(list(known_fornecedores)))

    rows: list[dict[str, object]] = []
    for row in orgao_counts.iter_rows(named=True):
        rows.append(
            {
                "fk_fornecedor": row["fk_fornecedor"],
                "indicador": "FORNECEDOR_EXCLUSIVO",
                "peso": _PESO_FORNECEDOR_EXCLUSIVO,
                "descricao": (f"Todos os {row['qtd_contratos']} contrato(s) sao com o mesmo orgao"),
                "evidencia": (f"orgao_codigo={row['orgao_unico']}, qtd_contratos={row['qtd_contratos']}"),
                "calculado_em": calculado_em,
            }
        )
    return rows


def _cnae_incompativel_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    calculado_em: datetime,
) -> list[dict[str, object]]:
    """CNAE_INCOMPATIVEL: company CNAE is incompatible with contract object category."""
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

    rows: list[dict[str, object]] = []
    seen: set[int] = set()

    for row in joined.iter_rows(named=True):
        fk = row["fk_fornecedor"]
        if fk in seen:
            continue
        cnae = row["cnae_principal"]
        obj_cat = row["objeto_categoria"]
        if cnae_incompativel_com_objeto(cnae, obj_cat):
            seen.add(fk)
            cat = get_cnae_category(cnae)
            rows.append(
                {
                    "fk_fornecedor": fk,
                    "indicador": "CNAE_INCOMPATIVEL",
                    "peso": _PESO_CNAE_INCOMPATIVEL,
                    "descricao": (f"CNAE {cnae} ({cat}) incompativel com objeto contratado ({obj_cat})"),
                    "evidencia": f"cnae={cnae}, categoria_cnae={cat}, objeto_categoria={obj_cat}",
                    "calculado_em": calculado_em,
                }
            )
    return rows


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

    # Map CNPJs back to pk_fornecedor.
    cnpj_to_pk = dict(
        zip(
            empresas_df["cnpj"].to_list(),
            empresas_df["pk_fornecedor"].to_list(),
            strict=False,
        )
    )

    # Collect unique fornecedores that share an address with at least one other.
    flagged: dict[int, tuple[str, str]] = {}
    for row in pairs.iter_rows(named=True):
        cnpj_a = row["cnpj_a"]
        cnpj_b = row["cnpj_b"]
        endereco = row["endereco_compartilhado"]
        pk_a = cnpj_to_pk.get(cnpj_a)
        pk_b = cnpj_to_pk.get(cnpj_b)
        if pk_a is not None and pk_a not in flagged:
            flagged[pk_a] = (cnpj_b, endereco)
        if pk_b is not None and pk_b not in flagged:
            flagged[pk_b] = (cnpj_a, endereco)

    rows: list[dict[str, object]] = []
    for fk, (cnpj_parceiro, endereco) in flagged.items():
        rows.append(
            {
                "fk_fornecedor": fk,
                "indicador": "MESMO_ENDERECO",
                "peso": _PESO_MESMO_ENDERECO,
                "descricao": f"Compartilha endereco com {cnpj_parceiro}",
                "evidencia": f"endereco={endereco}, cnpj_parceiro={cnpj_parceiro}",
                "calculado_em": calculado_em,
            }
        )
    return rows


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

    known_fornecedores = set(empresas_df["pk_fornecedor"].to_list())

    # Take the most recent jump per fornecedor.
    dedup = flagged.sort("ano", descending=True).unique(subset=["fk_fornecedor"], keep="first")
    dedup = dedup.filter(pl.col("fk_fornecedor").is_in(list(known_fornecedores)))

    rows: list[dict[str, object]] = []
    for row in dedup.iter_rows(named=True):
        razao = row["valor_anual"] / row["valor_prev"] if row["valor_prev"] > 0 else 0
        rows.append(
            {
                "fk_fornecedor": row["fk_fornecedor"],
                "indicador": "CRESCIMENTO_SUBITO",
                "peso": _PESO_CRESCIMENTO_SUBITO,
                "descricao": (f"Valor contratado saltou {razao:.1f}x entre {row['ano'] - 1} e {row['ano']}"),
                "evidencia": (
                    f"ano_anterior={row['ano'] - 1}, valor_anterior={row['valor_prev']:.2f}, "
                    f"ano_atual={row['ano']}, valor_atual={row['valor_anual']:.2f}, razao={razao:.1f}x"
                ),
                "calculado_em": calculado_em,
            }
        )
    return rows


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
