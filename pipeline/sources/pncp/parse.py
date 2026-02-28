# pipeline/sources/pncp/parse.py
#
# Parse PNCP (Portal Nacional de Contratações Públicas) JSON into a staging
# DataFrame for fato_contrato.
#
# Design decisions:
#   - The PNCP API returns paginated JSON with a "data" array of contract
#     objects. This parser accepts a single JSON file representing one page
#     (or all pages concatenated by the download step).
#   - Foreign keys (fk_fornecedor, fk_orgao, fk_tempo, fk_modalidade) are
#     NOT resolved here. They are filled as null placeholders and resolved by
#     the transform layer after all dimension tables are built.
#   - pk_contrato is set to row index and must be re-keyed by the transform
#     layer to avoid collisions when multiple pages are merged.
#   - cnpj_fornecedor and codigo_orgao are preserved as raw strings so the
#     transform layer can JOIN them to dim_fornecedor and dim_orgao.
#   - objeto is truncated to 1000 characters at the validate step (not here)
#     to keep this function purely structural.
#   - valor is stored as Float64; DECIMAL precision is enforced at DuckDB load
#     via the schema.sql DECIMAL(18,2) column.
#
# Invariants:
#   - Output must contain cnpj_fornecedor and codigo_orgao for downstream FK
#     resolution.
#   - data_assinatura is parsed as a Date or null — never a raw string.
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl


def _extract_record(record: dict[str, Any], index: int) -> dict[str, Any]:
    """Extract a flat dict of relevant fields from a PNCP contract record.

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
        # FK placeholders resolved by the transform layer.
        "fk_fornecedor": None,
        "fk_orgao": None,
        "fk_tempo": None,
        "fk_modalidade": None,
    }


def parse_contratos(raw_path: Path) -> pl.DataFrame:
    """Parse a PNCP JSON response file into a typed contratos DataFrame.

    Accepts a JSON file produced by download_pncp (which may contain one page
    or all pages merged). The file must have a top-level "data" array or be a
    JSON array directly.

    Args:
        raw_path: Path to the PNCP JSON file.

    Returns:
        Polars DataFrame with one row per contract, with FK columns as nulls.
    """
    text = raw_path.read_text(encoding="utf-8")
    payload: Any = json.loads(text)

    # Accept either {"data": [...]} envelope or a bare array.
    if isinstance(payload, dict):
        records: list[dict[str, Any]] = payload.get("data", [])
    else:
        records = list(payload)

    if not records:
        return pl.DataFrame(
            schema={
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
        )

    rows = [_extract_record(r, i) for i, r in enumerate(records)]

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
