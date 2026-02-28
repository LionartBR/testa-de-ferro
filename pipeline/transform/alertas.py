# pipeline/transform/alertas.py
#
# Pre-compute fato_alerta_critico for all fornecedores.
#
# Design decisions:
#   - This module mirrors the logic in api/application/services/alerta_service.py
#     but operates on Polars DataFrames over the full supplier population.
#   - Each alert type is a separate function that returns a list of row dicts,
#     which are concatenated at the end. This keeps each rule independent and
#     testable in isolation.
#   - The output schema matches fato_alerta_critico in schema.sql exactly.
#   - pk_alerta is a sequential integer assigned at the end (not a UUID),
#     because DuckDB uses an INTEGER primary key, not UUID.
#
# ADR: Why not import from api/domain?
#   The pipeline is a standalone offline process. Importing from the API
#   package at pipeline build time would couple the pipeline environment to
#   the web stack. Constants duplicated here carry their source annotation.
#   Source of truth: api/application/services/alerta_service.py
#
# ADR: Score and Alerts are independent dimensions (mirrors api/).
#   This module NEVER imports or calls score.py. The two dimensions do not
#   cross-call each other.
#   Source of truth: api/application/services/alerta_service.py line 3-6.
#
# Invariants:
#   - detectar_alertas_batch is a pure function over DataFrames. No IO.
#   - Output columns match fato_alerta_critico schema exactly.
#   - pk_alerta is a sequential integer starting at 1.
#   - fk_socio is null when the alert is at company level (not sócio level).
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import polars as pl

# ---------------------------------------------------------------------------
# Thresholds — source of truth: api/application/services/alerta_service.py
# ---------------------------------------------------------------------------
_DOACAO_THRESHOLD = Decimal("10000")
_CONTRATO_THRESHOLD_DOACAO = Decimal("500000")

# ---------------------------------------------------------------------------
# Severidade strings — source of truth: api/domain/fornecedor/enums.py
# ---------------------------------------------------------------------------
_GRAVISSIMO = "GRAVISSIMO"
_GRAVE = "GRAVE"

# ---------------------------------------------------------------------------
# TipoAlerta strings — source of truth: api/domain/fornecedor/enums.py
# ---------------------------------------------------------------------------
_SOCIO_SERVIDOR_PUBLICO = "SOCIO_SERVIDOR_PUBLICO"
_EMPRESA_SANCIONADA_CONTRATANDO = "EMPRESA_SANCIONADA_CONTRATANDO"
_DOACAO_PARA_CONTRATANTE = "DOACAO_PARA_CONTRATANTE"
_SOCIO_SANCIONADO_EM_OUTRA = "SOCIO_SANCIONADO_EM_OUTRA"
_RODIZIO_LICITACAO = "RODIZIO_LICITACAO"
_TESTA_DE_FERRO = "TESTA_DE_FERRO"

# ---------------------------------------------------------------------------
# Thresholds for new alert detectors
# ---------------------------------------------------------------------------

# Minimum number of shared licitações that flags a pair as a bid-rigging ring.
_RODIZIO_MIN_LICITACOES = 3

# Maximum capital_social (BRL) for the testa-de-ferro composite pattern.
_TESTA_DE_FERRO_MAX_CAPITAL = 10_000.0

# Maximum days between company opening and first contract for the
# "empresa_recente" condition in the testa-de-ferro detector.
_TESTA_DE_FERRO_MAX_DAYS_TO_FIRST_CONTRACT = 365

# Minimum number of government companies a sócio must appear in to trigger
# the "socio reincidente" branch of the testa-de-ferro condition 4.
_TESTA_DE_FERRO_MIN_EMPRESAS_GOVERNO = 3


def detectar_alertas_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    sancoes_df: pl.DataFrame,
    doacoes_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Pre-detect all critical alerts for every fornecedor.

    Alert types computed:
      - SOCIO_SERVIDOR_PUBLICO (GRAVISSIMO): sócio is a federal servant.
      - EMPRESA_SANCIONADA_CONTRATANDO (GRAVISSIMO): active sanction + active contracts.
      - DOACAO_PARA_CONTRATANTE (GRAVE): material donation + large contract total.
      - RODIZIO_LICITACAO (GRAVISSIMO): shared-socio pair appears in 3+ same bids.
      - TESTA_DE_FERRO (GRAVISSIMO): composite pattern — all 4 conditions true.
      - SOCIO_SANCIONADO_EM_OUTRA (GRAVE): sócio is sócio of a sanctioned company.

    Args:
        empresas_df:  dim_fornecedor staging DataFrame.
        socios_df:    dim_socio staging DataFrame, enriched by cruzamentos and
                      match_servidor_socio (must have is_servidor_publico, is_sancionado).
        contratos_df: fato_contrato staging DataFrame.
        sancoes_df:   dim_sancao staging DataFrame (with data_fim column).
        doacoes_df:   fato_doacao staging DataFrame, or None if not available.

    Returns:
        DataFrame matching fato_alerta_critico schema:
          pk_alerta, fk_fornecedor, fk_socio, tipo_alerta, severidade,
          descricao, evidencia, detectado_em.
    """
    detectado_em = datetime.now(tz=UTC).replace(tzinfo=None)
    rows: list[dict[str, object]] = []

    rows.extend(_socio_servidor_publico_batch(empresas_df, socios_df, detectado_em))
    rows.extend(_empresa_sancionada_contratando_batch(empresas_df, contratos_df, sancoes_df, detectado_em))
    if doacoes_df is not None and not doacoes_df.is_empty():
        rows.extend(_doacao_para_contratante_batch(empresas_df, contratos_df, doacoes_df, detectado_em))
    rows.extend(_rodizio_licitacao_batch(empresas_df, socios_df, contratos_df, detectado_em))
    rows.extend(_testa_de_ferro_batch(empresas_df, socios_df, contratos_df, detectado_em))
    rows.extend(_socio_sancionado_em_outra_batch(empresas_df, socios_df, detectado_em))

    if not rows:
        return _empty_alerta_df()

    result = pl.DataFrame(rows)
    result = result.with_row_index(name="pk_alerta", offset=1)
    return result.select(
        [
            "pk_alerta",
            "fk_fornecedor",
            "fk_socio",
            "tipo_alerta",
            "severidade",
            "descricao",
            "evidencia",
            "detectado_em",
        ]
    )


# ---------------------------------------------------------------------------
# Private alert detectors
# ---------------------------------------------------------------------------


def _socio_servidor_publico_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    detectado_em: datetime,
) -> list[dict[str, object]]:
    """SOCIO_SERVIDOR_PUBLICO: any sócio flagged as federal servant → GRAVISSIMO."""
    if socios_df.is_empty() or "is_servidor_publico" not in socios_df.columns:
        return []
    if "pk_fornecedor" not in empresas_df.columns or "cnpj_basico" not in empresas_df.columns:
        return []

    servidores = socios_df.filter(pl.col("is_servidor_publico"))
    if servidores.is_empty():
        return []

    # Map cnpj_basico → pk_fornecedor.
    basico_to_pk = empresas_df.select(
        [
            pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
            pl.col("pk_fornecedor"),
        ]
    )

    servidores_norm = servidores.with_columns(pl.col("cnpj_basico").str.zfill(8))

    joined = servidores_norm.join(basico_to_pk, on="cnpj_basico", how="inner")
    if joined.is_empty():
        return []

    rows: list[dict[str, object]] = []
    pk_socio_col = "pk_socio" if "pk_socio" in joined.columns else None

    for row in joined.iter_rows(named=True):
        orgao = row.get("orgao_lotacao")
        descricao = f"Socio {row['nome_socio']} e servidor publico"
        if orgao:
            descricao += f" ({orgao})"

        evidencia = f"nome={row['nome_socio']}"
        if "cpf_hmac" in row and row["cpf_hmac"]:
            evidencia = f"socio_cpf_hmac={row['cpf_hmac']}, " + evidencia
        if orgao:
            evidencia += f", orgao={orgao}"

        rows.append(
            {
                "fk_fornecedor": row["pk_fornecedor"],
                "fk_socio": row[pk_socio_col] if pk_socio_col else None,
                "tipo_alerta": _SOCIO_SERVIDOR_PUBLICO,
                "severidade": _GRAVISSIMO,
                "descricao": descricao,
                "evidencia": evidencia,
                "detectado_em": detectado_em,
            }
        )
    return rows


def _empresa_sancionada_contratando_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    sancoes_df: pl.DataFrame,
    detectado_em: datetime,
) -> list[dict[str, object]]:
    """EMPRESA_SANCIONADA_CONTRATANDO: active sanction + contracts → GRAVISSIMO."""
    if sancoes_df.is_empty() or contratos_df.is_empty():
        return []
    if "data_fim" not in sancoes_df.columns:
        return []

    today = datetime.now(tz=UTC).date()

    # Active sanction: data_fim is null (indefinite) OR data_fim > today.
    vigentes = sancoes_df.filter(pl.col("data_fim").is_null() | (pl.col("data_fim").cast(pl.Date) > pl.lit(today)))

    if vigentes.is_empty():
        return []

    # Count active sanctions per fornecedor.
    sancoes_count = vigentes.group_by("fk_fornecedor").agg(pl.len().alias("qtd_vigentes"))

    # Count contracts per fornecedor.
    contratos_count = contratos_df.group_by("fk_fornecedor").agg(pl.len().alias("qtd_contratos"))

    # Intersection: fornecedores with both active sanctions AND contracts.
    both = sancoes_count.join(contratos_count, on="fk_fornecedor", how="inner")

    if both.is_empty():
        return []

    known_fornecedores = set(empresas_df["pk_fornecedor"].to_list())
    both = both.filter(pl.col("fk_fornecedor").is_in(list(known_fornecedores)))

    rows: list[dict[str, object]] = []
    for row in both.iter_rows(named=True):
        rows.append(
            {
                "fk_fornecedor": row["fk_fornecedor"],
                "fk_socio": None,
                "tipo_alerta": _EMPRESA_SANCIONADA_CONTRATANDO,
                "severidade": _GRAVISSIMO,
                "descricao": (
                    f"Empresa com {row['qtd_vigentes']} sancao(oes) vigente(s) "
                    f"e {row['qtd_contratos']} contrato(s) ativo(s)"
                ),
                "evidencia": (f"sancoes_vigentes={row['qtd_vigentes']}, qtd_contratos={row['qtd_contratos']}"),
                "detectado_em": detectado_em,
            }
        )
    return rows


def _doacao_para_contratante_batch(
    empresas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    doacoes_df: pl.DataFrame,
    detectado_em: datetime,
) -> list[dict[str, object]]:
    """DOACAO_PARA_CONTRATANTE: donation > R$10k AND contract total > R$500k → GRAVE."""
    if doacoes_df.is_empty() or contratos_df.is_empty():
        return []
    if "valor" not in doacoes_df.columns or "fk_fornecedor" not in doacoes_df.columns:
        return []

    doacao_threshold = float(_DOACAO_THRESHOLD)
    contrato_threshold = float(_CONTRATO_THRESHOLD_DOACAO)

    # Material donations: valor > R$10k.
    materiais = doacoes_df.filter(pl.col("valor").cast(pl.Float64) > doacao_threshold)

    if materiais.is_empty():
        return []

    materiais_count = materiais.group_by("fk_fornecedor").agg(pl.len().alias("qtd_doacoes_materiais"))

    # Total contract value per fornecedor.
    contrato_totais = (
        contratos_df.group_by("fk_fornecedor")
        .agg(pl.col("valor").cast(pl.Float64).sum().alias("valor_total_contratos"))
        .filter(pl.col("valor_total_contratos") > contrato_threshold)
    )

    # Intersection.
    both = materiais_count.join(contrato_totais, on="fk_fornecedor", how="inner")

    if both.is_empty():
        return []

    known_fornecedores = set(empresas_df["pk_fornecedor"].to_list())
    both = both.filter(pl.col("fk_fornecedor").is_in(list(known_fornecedores)))

    rows: list[dict[str, object]] = []
    for row in both.iter_rows(named=True):
        rows.append(
            {
                "fk_fornecedor": row["fk_fornecedor"],
                "fk_socio": None,
                "tipo_alerta": _DOACAO_PARA_CONTRATANTE,
                "severidade": _GRAVE,
                "descricao": (
                    f"{row['qtd_doacoes_materiais']} doacao(oes) material(is) "
                    f"com contratos totalizando R${row['valor_total_contratos']:,.2f}"
                ),
                "evidencia": (
                    f"doacoes_materiais={row['qtd_doacoes_materiais']}, "
                    f"valor_total_contratos={row['valor_total_contratos']}"
                ),
                "detectado_em": detectado_em,
            }
        )
    return rows


def _socio_sancionado_em_outra_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    detectado_em: datetime,
) -> list[dict[str, object]]:
    """SOCIO_SANCIONADO_EM_OUTRA: sócio flagged is_sancionado → GRAVE."""
    if socios_df.is_empty() or "is_sancionado" not in socios_df.columns:
        return []
    if "pk_fornecedor" not in empresas_df.columns or "cnpj_basico" not in empresas_df.columns:
        return []

    sancionados = socios_df.filter(pl.col("is_sancionado"))
    if sancionados.is_empty():
        return []

    basico_to_pk = empresas_df.select(
        [
            pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
            pl.col("pk_fornecedor"),
        ]
    )

    sancionados_norm = sancionados.with_columns(pl.col("cnpj_basico").str.zfill(8))

    joined = sancionados_norm.join(basico_to_pk, on="cnpj_basico", how="inner")
    if joined.is_empty():
        return []

    rows: list[dict[str, object]] = []
    pk_socio_col = "pk_socio" if "pk_socio" in joined.columns else None

    for row in joined.iter_rows(named=True):
        evidencia = f"nome={row['nome_socio']}"
        if "cpf_hmac" in row and row["cpf_hmac"]:
            evidencia = f"socio_cpf_hmac={row['cpf_hmac']}, " + evidencia

        rows.append(
            {
                "fk_fornecedor": row["pk_fornecedor"],
                "fk_socio": row[pk_socio_col] if pk_socio_col else None,
                "tipo_alerta": _SOCIO_SANCIONADO_EM_OUTRA,
                "severidade": _GRAVE,
                "descricao": f"Socio {row['nome_socio']} e socio de outra empresa sancionada",
                "evidencia": evidencia,
                "detectado_em": detectado_em,
            }
        )
    return rows


def _rodizio_licitacao_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    detectado_em: datetime,
) -> list[dict[str, object]]:
    """RODIZIO_LICITACAO: shared-socio pair in >= 3 same bids → GRAVISSIMO.

    Detection pipeline:
      1. Self-join socios_df on nome_socio to find (cnpj_a, cnpj_b) pairs
         that share at least one sócio.
      2. Map each cnpj in the pair to fk_fornecedor via empresas_df.
      3. Find licitações where both fornecedores in the pair participated by
         joining contratos_df on num_licitacao for each side of the pair.
      4. Count shared licitações per (fk_a, fk_b) pair.
      5. Flag pairs with count >= _RODIZIO_MIN_LICITACOES.
      6. Emit one alert per unique fk_fornecedor (deduped — a company may
         appear in multiple flagged pairs but gets only one alert row).

    Defensive guards:
      - Returns [] immediately if any required column is absent or DFs are empty.
      - Null num_licitacao rows are excluded before counting.
      - Self-pairs (cnpj_a == cnpj_b) are excluded.

    ADR: We join on nome_socio rather than cpf_hmac because cpf_hmac may be
    null in the QSA data (anonymous partners). Nome matching is noisier but
    ensures we never miss an obvious ring due to data gaps. For this alert the
    false positive rate is acceptable because 3+ shared bids is already a
    strong signal independent of the sócio identity.
    """
    required_socio_cols = {"cnpj_basico", "nome_socio"}
    required_empresa_cols = {"pk_fornecedor", "cnpj_basico"}
    required_contrato_cols = {"fk_fornecedor", "num_licitacao"}

    if socios_df.is_empty() or empresas_df.is_empty() or contratos_df.is_empty():
        return []
    if not required_socio_cols.issubset(socios_df.columns):
        return []
    if not required_empresa_cols.issubset(empresas_df.columns):
        return []
    if not required_contrato_cols.issubset(contratos_df.columns):
        return []

    # Normalize cnpj_basico to 8 chars (matches _socio_servidor_publico_batch pattern).
    basico_to_pk = empresas_df.select(
        [
            pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
            pl.col("pk_fornecedor"),
        ]
    )

    socios_norm = socios_df.select(["cnpj_basico", "nome_socio"]).with_columns(pl.col("cnpj_basico").str.zfill(8))

    # Map each cnpj_basico in socios to its pk_fornecedor.
    socios_with_pk = socios_norm.join(basico_to_pk, on="cnpj_basico", how="inner")
    if socios_with_pk.is_empty():
        return []

    # Self-join on nome_socio to get all (fk_a, fk_b) pairs sharing a sócio.
    # Suffix _right distinguishes the right side.
    pairs = (
        socios_with_pk.join(
            socios_with_pk.rename({"pk_fornecedor": "fk_b", "cnpj_basico": "cnpj_b"}),
            on="nome_socio",
            how="inner",
        )
        .filter(
            # Exclude self-pairs and keep canonical order (fk_a < fk_b) to avoid
            # counting (1,2) and (2,1) as separate pairs.
            pl.col("pk_fornecedor") < pl.col("fk_b")
        )
        .select(
            [
                pl.col("pk_fornecedor").alias("fk_a"),
                pl.col("fk_b"),
            ]
        )
        .unique()
    )

    if pairs.is_empty():
        return []

    # Discard contracts with null num_licitacao — these are direct contracts.
    licitacoes = contratos_df.filter(pl.col("num_licitacao").is_not_null()).select(["fk_fornecedor", "num_licitacao"])

    if licitacoes.is_empty():
        return []

    # For each pair, count licitações where BOTH companies participated.
    # Strategy: join pairs with licitacoes for side A, then inner-join with
    # licitacoes for side B on num_licitacao, then count per pair.
    licitacoes_a = licitacoes.rename({"fk_fornecedor": "fk_a", "num_licitacao": "num_licitacao_a"})
    licitacoes_b = licitacoes.rename({"fk_fornecedor": "fk_b", "num_licitacao": "num_licitacao_b"})

    pairs_with_a = pairs.join(licitacoes_a, on="fk_a", how="inner")
    pairs_with_both = pairs_with_a.join(
        licitacoes_b,
        left_on=["fk_b", "num_licitacao_a"],
        right_on=["fk_b", "num_licitacao_b"],
        how="inner",
    )

    if pairs_with_both.is_empty():
        return []

    # Count distinct shared licitações per pair.
    shared_counts = (
        pairs_with_both.select(["fk_a", "fk_b", "num_licitacao_a"])
        .unique()
        .group_by(["fk_a", "fk_b"])
        .agg(pl.len().alias("qtd_licitacoes_comuns"))
    ).filter(pl.col("qtd_licitacoes_comuns") >= _RODIZIO_MIN_LICITACOES)

    if shared_counts.is_empty():
        return []

    # Collect unique fornecedor PKs that appear in any flagged pair.
    flagged_fk_a = shared_counts.select(pl.col("fk_a").alias("fk_fornecedor"))
    flagged_fk_b = shared_counts.select(pl.col("fk_b").alias("fk_fornecedor"))
    flagged_all = (
        pl.concat([flagged_fk_a, flagged_fk_b])
        .unique()
        .join(shared_counts, left_on="fk_fornecedor", right_on="fk_a", how="left")
        .select("fk_fornecedor")
        .unique()
    )

    rows: list[dict[str, object]] = []
    for row in flagged_all.iter_rows(named=True):
        # Find the maximum shared licitações count for this fornecedor to
        # include in the evidence string.
        max_shared = shared_counts.filter(
            (pl.col("fk_a") == row["fk_fornecedor"]) | (pl.col("fk_b") == row["fk_fornecedor"])
        )["qtd_licitacoes_comuns"].max()
        rows.append(
            {
                "fk_fornecedor": row["fk_fornecedor"],
                "fk_socio": None,
                "tipo_alerta": _RODIZIO_LICITACAO,
                "severidade": _GRAVISSIMO,
                "descricao": ("Empresa participa de licitacoes com outra empresa que compartilha socios"),
                "evidencia": f"max_licitacoes_comuns_com_par={max_shared}",
                "detectado_em": detectado_em,
            }
        )
    return rows


def _testa_de_ferro_batch(
    empresas_df: pl.DataFrame,
    socios_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
    detectado_em: datetime,
) -> list[dict[str, object]]:
    """TESTA_DE_FERRO: composite pattern — all 4 conditions must be true → GRAVISSIMO.

    Conditions (ALL must hold):
      1. capital_social < R$10,000 (undercapitalized shell company).
      2. Company opened < 12 months before its first government contract
         (empresa_recente — fast entry into public procurement).
      3. All contracts belong to a single orgão (fornecedor exclusivo —
         suspicious single-buyer dependency).
      4. At least one sócio satisfies either:
         a. is_servidor_publico == True, OR
         b. qtd_empresas_governo >= 3 (serially reused front-man).

    Detection pipeline:
      1. Filter empresas with capital_social < _TESTA_DE_FERRO_MAX_CAPITAL.
      2. For each candidate empresa, compute earliest data_assinatura from
         contratos and check the gap against data_abertura (condition 2).
         Empresas with null data_abertura are excluded conservatively.
      3. Check that the empresa has contracts with exactly one distinct
         codigo_orgao (or fk_orgao, whichever is available) — condition 3.
      4. Join with socios_df and check condition 4.
      5. Map cnpj_basico → pk_fornecedor and emit one alert per empresa.

    ADR: We use codigo_orgao when available (staging pre-FK-resolution) and
    fall back to fk_orgao for the single-orgao check. This makes the detector
    work at both staging and post-transform DataFrame shapes.

    ADR: Condition 2 uses data_assinatura of the *earliest* contract as a
    proxy for "first procurement contact". Using data_abertura as the anchor
    is conservative — a company may have existed before its first bid. The
    < 12 month window filters only the most suspicious cases.
    """
    required_empresa_cols = {"pk_fornecedor", "cnpj_basico", "capital_social", "data_abertura"}
    required_socio_cols = {"cnpj_basico", "nome_socio"}
    required_contrato_cols = {"fk_fornecedor"}

    if empresas_df.is_empty() or socios_df.is_empty() or contratos_df.is_empty():
        return []
    if not required_empresa_cols.issubset(empresas_df.columns):
        return []
    if not required_socio_cols.issubset(socios_df.columns):
        return []
    if not required_contrato_cols.issubset(contratos_df.columns):
        return []

    # Condition 1: undercapitalized.
    low_capital = empresas_df.filter(
        pl.col("capital_social").cast(pl.Float64, strict=False) < _TESTA_DE_FERRO_MAX_CAPITAL
    )
    if low_capital.is_empty():
        return []

    # Determine which orgão column to use for the exclusivity check (condition 3).
    orgao_col = "codigo_orgao" if "codigo_orgao" in contratos_df.columns else "fk_orgao"

    # We need data_assinatura for condition 2. Guard if absent.
    has_data_assinatura = "data_assinatura" in contratos_df.columns

    # Aggregate contratos per fk_fornecedor:
    #   - earliest data_assinatura (for condition 2)
    #   - distinct orgão count (for condition 3)
    agg_exprs: list[pl.Expr] = [pl.col(orgao_col).n_unique().alias("qtd_orgaos")]
    if has_data_assinatura:
        agg_exprs.append(pl.col("data_assinatura").min().alias("primeiro_contrato"))

    contrato_stats = contratos_df.group_by("fk_fornecedor").agg(agg_exprs)

    # Condition 3: single orgão only.
    exclusivos = contrato_stats.filter(pl.col("qtd_orgaos") == 1)
    if exclusivos.is_empty():
        return []

    # Join low_capital empresas with contract stats (inner join preserves only
    # empresas that actually have contracts).
    candidates = low_capital.join(exclusivos, left_on="pk_fornecedor", right_on="fk_fornecedor", how="inner")
    if candidates.is_empty():
        return []

    # Condition 2: empresa opened < 12 months before first contract.
    if has_data_assinatura:
        candidates = candidates.filter(
            pl.col("data_abertura").is_not_null()
            & pl.col("primeiro_contrato").is_not_null()
            & (
                (pl.col("primeiro_contrato") - pl.col("data_abertura")).dt.total_days()
                < _TESTA_DE_FERRO_MAX_DAYS_TO_FIRST_CONTRACT
            )
        )
    else:
        # Without date information we cannot verify condition 2 — exclude all.
        return []

    if candidates.is_empty():
        return []

    # Condition 4: at least one sócio is servidor OR in 3+ gov companies.
    socio_flag_available = "is_servidor_publico" in socios_df.columns and "qtd_empresas_governo" in socios_df.columns
    if not socio_flag_available:
        return []

    basico_to_pk = candidates.select(
        [
            pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
            pl.col("pk_fornecedor"),
        ]
    )

    socios_norm = socios_df.with_columns(pl.col("cnpj_basico").str.zfill(8))

    socios_suspeitos = socios_norm.filter(
        pl.col("is_servidor_publico") | (pl.col("qtd_empresas_governo") >= _TESTA_DE_FERRO_MIN_EMPRESAS_GOVERNO)
    )

    if socios_suspeitos.is_empty():
        return []

    socios_with_pk = socios_suspeitos.join(basico_to_pk, on="cnpj_basico", how="inner")
    if socios_with_pk.is_empty():
        return []

    # One alert per unique pk_fornecedor (there may be multiple flagged sócios).
    flagged_pks = socios_with_pk.select("pk_fornecedor").unique()

    rows: list[dict[str, object]] = []
    for row in flagged_pks.iter_rows(named=True):
        pk = row["pk_fornecedor"]

        # Gather evidence from the candidate empresa row.
        empresa_row = candidates.filter(pl.col("pk_fornecedor") == pk)
        if empresa_row.is_empty():
            continue

        capital = empresa_row["capital_social"][0]
        data_abertura = empresa_row["data_abertura"][0]
        primeiro_contrato = empresa_row["primeiro_contrato"][0] if "primeiro_contrato" in empresa_row.columns else None

        rows.append(
            {
                "fk_fornecedor": pk,
                "fk_socio": None,
                "tipo_alerta": _TESTA_DE_FERRO,
                "severidade": _GRAVISSIMO,
                "descricao": (
                    "Empresa apresenta perfil composite de testa-de-ferro: "
                    "capital baixo, entrada rapida em licitacoes, fornecedor exclusivo "
                    "e socio suspeito"
                ),
                "evidencia": (
                    f"capital_social={capital}, data_abertura={data_abertura}, primeiro_contrato={primeiro_contrato}"
                ),
                "detectado_em": detectado_em,
            }
        )
    return rows


def _empty_alerta_df() -> pl.DataFrame:
    """Return an empty DataFrame with the fato_alerta_critico schema."""
    return pl.DataFrame(
        {
            "pk_alerta": pl.Series([], dtype=pl.Int64),
            "fk_fornecedor": pl.Series([], dtype=pl.Int64),
            "fk_socio": pl.Series([], dtype=pl.Int64),
            "tipo_alerta": pl.Series([], dtype=pl.Utf8),
            "severidade": pl.Series([], dtype=pl.Utf8),
            "descricao": pl.Series([], dtype=pl.Utf8),
            "evidencia": pl.Series([], dtype=pl.Utf8),
            "detectado_em": pl.Series([], dtype=pl.Datetime),
        }
    )
