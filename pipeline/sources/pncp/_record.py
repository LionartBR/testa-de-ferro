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

    Handles two response formats:
      - **Real API** (v1/contratos): supplier CNPJ is a flat field ``niFornecedor``,
        vigência end date is ``dataVigenciaFim``, and modalidade lives inside
        ``categoriaProcesso``.
      - **Test fixture** (sample_pncp.json): supplier is a nested ``fornecedor``
        object with ``cnpjFormatado``, vigência is ``dataVigencia``, and
        ``modalidadeNome`` / ``modalidadeCodigo`` are top-level.

    The function tries the real API field first and falls back to the legacy
    fixture field so both paths produce correct data.

    Args:
        record: Single contract object from the PNCP API JSON response.
        index:  Row index used as pk_contrato placeholder.

    Returns:
        Flat dict with all columns required for fato_contrato staging.
    """
    orgao = record.get("orgaoEntidade", {}) or {}

    # --- cnpj_fornecedor ---
    # Real API: flat ``niFornecedor`` (unformatted CNPJ, e.g. "55623647000161").
    # Test fixture: nested ``fornecedor.cnpjFormatado`` (formatted, e.g. "11.222.333/0001-81").
    cnpj_fornecedor = record.get("niFornecedor")
    if cnpj_fornecedor is None:
        fornecedor_obj = record.get("fornecedor", {}) or {}
        cnpj_fornecedor = fornecedor_obj.get("cnpjFormatado")
    # Strip formatting (dots, slashes, dashes) so downstream always sees raw digits.
    if cnpj_fornecedor is not None:
        cnpj_fornecedor = str(cnpj_fornecedor).replace(".", "").replace("/", "").replace("-", "")

    # --- data_vigencia ---
    # Real API: ``dataVigenciaFim``.  Test fixture: ``dataVigencia``.
    data_vigencia = record.get("dataVigenciaFim") or record.get("dataVigencia")

    # --- modalidade ---
    # Real API: ``categoriaProcesso`` dict with ``id`` and ``nome``.
    # Test fixture: ``modalidadeNome`` and ``modalidadeCodigo`` at top level.
    categoria = record.get("categoriaProcesso", {}) or {}
    modalidade_nome = record.get("modalidadeNome") or categoria.get("nome")
    modalidade_codigo_raw = record.get("modalidadeCodigo") or categoria.get("id")
    modalidade_codigo = str(modalidade_codigo_raw) if modalidade_codigo_raw is not None else ""

    return {
        "pk_contrato": index + 1,
        "num_licitacao": record.get("numeroContratoEmpenho"),
        "cnpj_fornecedor": cnpj_fornecedor,
        "codigo_orgao": orgao.get("cnpj"),
        "nome_orgao": orgao.get("razaoSocial"),
        "poder_orgao": orgao.get("poderId"),
        "esfera_orgao": orgao.get("esferaId"),
        "modalidade_nome": modalidade_nome,
        "modalidade_codigo": modalidade_codigo,
        "valor": record.get("valorInicial"),
        "objeto": record.get("objetoContrato"),
        "data_assinatura": record.get("dataAssinatura"),
        "data_vigencia": data_vigencia,
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
