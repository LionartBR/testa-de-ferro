# pipeline/sources/pncp/_record.py
#
# Internal module shared between download and parse.
#
# Design decisions:
#   - extract_record and build_window_df are extracted from parse.py so that
#     download.py can write per-window Parquet files without importing the
#     full parse module.
#   - CONTRATOS_SCHEMA is the single source of truth for the expected column
#     names and types of the contratos staging DataFrame.
#   - build_window_df handles date/valor casting so each Parquet file is
#     already typed — parse just needs to scan and collect.
from __future__ import annotations

from typing import Any

import polars as pl

# Schema canônico para o DataFrame de contratos PNCP.
# Usado tanto no build_window_df quanto no empty DataFrame de parse_contratos.
CONTRATOS_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "pk_contrato": pl.Int64,
    "num_licitacao": pl.Utf8,
    "cnpj_fornecedor": pl.Utf8,
    "codigo_orgao": pl.Utf8,
    "nome_orgao": pl.Utf8,
    "poder_orgao": pl.Utf8,
    "esfera_orgao": pl.Utf8,
    "modalidade_nome": pl.Utf8,
    "modalidade_codigo": pl.Utf8,
    "valor": pl.Float64,
    "objeto": pl.Utf8,
    "data_assinatura": pl.Date,
    "data_vigencia": pl.Date,
    "fk_fornecedor": pl.Int64,
    "fk_orgao": pl.Int64,
    "fk_tempo": pl.Int64,
    "fk_modalidade": pl.Int64,
}


def extract_record(record: dict[str, Any], index: int) -> dict[str, Any]:
    """Extract a flat dict from a PNCP contract record.

    Args:
        record: Single contract object from the PNCP API JSON response.
        index:  Row index used as pk_contrato placeholder.

    Returns:
        Flat dict with all columns required for fato_contrato staging.
    """
    orgao = record.get("orgaoEntidade", {}) or {}
    fornecedor = record.get("fornecedor", {}) or {}

    return {
        "pk_contrato": index + 1,
        "num_licitacao": record.get("numeroContratoEmpenho"),
        "cnpj_fornecedor": fornecedor.get("cnpjFormatado"),
        "codigo_orgao": orgao.get("cnpj"),
        "nome_orgao": orgao.get("razaoSocial"),
        "poder_orgao": orgao.get("poderId"),
        "esfera_orgao": orgao.get("esferaId"),
        "modalidade_nome": record.get("modalidadeNome"),
        "modalidade_codigo": str(record.get("modalidadeCodigo", "") or ""),
        "valor": record.get("valorInicial"),
        "objeto": record.get("objetoContrato"),
        "data_assinatura": record.get("dataAssinatura"),
        "data_vigencia": record.get("dataVigencia"),
        "fk_fornecedor": None,
        "fk_orgao": None,
        "fk_tempo": None,
        "fk_modalidade": None,
    }


def build_window_df(records: list[dict[str, Any]], offset: int = 0) -> pl.DataFrame:
    """Build a typed Polars DataFrame from a list of raw PNCP API records.

    Args:
        records: Raw API response dicts for a single window.
        offset:  Starting index for pk_contrato numbering.

    Returns:
        Typed DataFrame with date columns as pl.Date and valor as Float64.
    """
    rows = [extract_record(r, offset + i) for i, r in enumerate(records)]
    df = pl.DataFrame(rows)

    # Cast date columns from string ISO-8601 to Date type.
    for date_col in ("data_assinatura", "data_vigencia"):
        if date_col in df.columns and df[date_col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(date_col).str.strip_chars().str.to_date(format="%Y-%m-%d", strict=False).alias(date_col)
            )

    # Ensure valor is Float64.
    if "valor" in df.columns:
        df = df.with_columns(pl.col("valor").cast(pl.Float64, strict=False))

    return df
