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

from datetime import datetime, timezone
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
    detectado_em = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    rows: list[dict[str, object]] = []

    rows.extend(_socio_servidor_publico_batch(empresas_df, socios_df, detectado_em))
    rows.extend(_empresa_sancionada_contratando_batch(
        empresas_df, contratos_df, sancoes_df, detectado_em
    ))
    if doacoes_df is not None and not doacoes_df.is_empty():
        rows.extend(_doacao_para_contratante_batch(
            empresas_df, contratos_df, doacoes_df, detectado_em
        ))
    rows.extend(_socio_sancionado_em_outra_batch(empresas_df, socios_df, detectado_em))

    if not rows:
        return _empty_alerta_df()

    result = pl.DataFrame(rows)
    result = result.with_row_index(name="pk_alerta", offset=1)
    return result.select([
        "pk_alerta",
        "fk_fornecedor",
        "fk_socio",
        "tipo_alerta",
        "severidade",
        "descricao",
        "evidencia",
        "detectado_em",
    ])


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
    basico_to_pk = empresas_df.select([
        pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
        pl.col("pk_fornecedor"),
    ])

    servidores_norm = servidores.with_columns(
        pl.col("cnpj_basico").str.zfill(8)
    )

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

        rows.append({
            "fk_fornecedor": row["pk_fornecedor"],
            "fk_socio": row[pk_socio_col] if pk_socio_col else None,
            "tipo_alerta": _SOCIO_SERVIDOR_PUBLICO,
            "severidade": _GRAVISSIMO,
            "descricao": descricao,
            "evidencia": evidencia,
            "detectado_em": detectado_em,
        })
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

    today = datetime.now(tz=timezone.utc).date()

    # Active sanction: data_fim is null (indefinite) OR data_fim > today.
    vigentes = sancoes_df.filter(
        pl.col("data_fim").is_null()
        | (pl.col("data_fim").cast(pl.Date) > pl.lit(today))
    )

    if vigentes.is_empty():
        return []

    # Count active sanctions per fornecedor.
    sancoes_count = (
        vigentes
        .group_by("fk_fornecedor")
        .agg(pl.len().alias("qtd_vigentes"))
    )

    # Count contracts per fornecedor.
    contratos_count = (
        contratos_df
        .group_by("fk_fornecedor")
        .agg(pl.len().alias("qtd_contratos"))
    )

    # Intersection: fornecedores with both active sanctions AND contracts.
    both = sancoes_count.join(contratos_count, on="fk_fornecedor", how="inner")

    if both.is_empty():
        return []

    known_fornecedores = set(empresas_df["pk_fornecedor"].to_list())
    both = both.filter(pl.col("fk_fornecedor").is_in(list(known_fornecedores)))

    rows: list[dict[str, object]] = []
    for row in both.iter_rows(named=True):
        rows.append({
            "fk_fornecedor": row["fk_fornecedor"],
            "fk_socio": None,
            "tipo_alerta": _EMPRESA_SANCIONADA_CONTRATANDO,
            "severidade": _GRAVISSIMO,
            "descricao": (
                f"Empresa com {row['qtd_vigentes']} sancao(oes) vigente(s) "
                f"e {row['qtd_contratos']} contrato(s) ativo(s)"
            ),
            "evidencia": (
                f"sancoes_vigentes={row['qtd_vigentes']}, "
                f"qtd_contratos={row['qtd_contratos']}"
            ),
            "detectado_em": detectado_em,
        })
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
    materiais = doacoes_df.filter(
        pl.col("valor").cast(pl.Float64) > doacao_threshold
    )

    if materiais.is_empty():
        return []

    materiais_count = (
        materiais
        .group_by("fk_fornecedor")
        .agg(pl.len().alias("qtd_doacoes_materiais"))
    )

    # Total contract value per fornecedor.
    contrato_totais = (
        contratos_df
        .group_by("fk_fornecedor")
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
        rows.append({
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
        })
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

    basico_to_pk = empresas_df.select([
        pl.col("cnpj_basico").str.zfill(8).alias("cnpj_basico"),
        pl.col("pk_fornecedor"),
    ])

    sancionados_norm = sancionados.with_columns(
        pl.col("cnpj_basico").str.zfill(8)
    )

    joined = sancionados_norm.join(basico_to_pk, on="cnpj_basico", how="inner")
    if joined.is_empty():
        return []

    rows: list[dict[str, object]] = []
    pk_socio_col = "pk_socio" if "pk_socio" in joined.columns else None

    for row in joined.iter_rows(named=True):
        evidencia = f"nome={row['nome_socio']}"
        if "cpf_hmac" in row and row["cpf_hmac"]:
            evidencia = f"socio_cpf_hmac={row['cpf_hmac']}, " + evidencia

        rows.append({
            "fk_fornecedor": row["pk_fornecedor"],
            "fk_socio": row[pk_socio_col] if pk_socio_col else None,
            "tipo_alerta": _SOCIO_SANCIONADO_EM_OUTRA,
            "severidade": _GRAVE,
            "descricao": f"Socio {row['nome_socio']} e socio de outra empresa sancionada",
            "evidencia": evidencia,
            "detectado_em": detectado_em,
        })
    return rows


def _empty_alerta_df() -> pl.DataFrame:
    """Return an empty DataFrame with the fato_alerta_critico schema."""
    return pl.DataFrame({
        "pk_alerta": pl.Series([], dtype=pl.Int64),
        "fk_fornecedor": pl.Series([], dtype=pl.Int64),
        "fk_socio": pl.Series([], dtype=pl.Int64),
        "tipo_alerta": pl.Series([], dtype=pl.Utf8),
        "severidade": pl.Series([], dtype=pl.Utf8),
        "descricao": pl.Series([], dtype=pl.Utf8),
        "evidencia": pl.Series([], dtype=pl.Utf8),
        "detectado_em": pl.Series([], dtype=pl.Datetime),
    })
