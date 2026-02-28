# pipeline/sources/rais/parse.py
#
# Parse RAIS (Relação Anual de Informações Sociais) CSV into a staging
# DataFrame used to enrich dim_fornecedor with employee count data.
#
# Design decisions:
#   - RAIS CSVs use semicolons as separators and Latin-1 encoding — the same
#     convention used by Receita Federal and Portal da Transparência files.
#   - The RAIS extract groups employment data by CNPJ root (cnpj_basico,
#     8 digits). This matches the Receita Federal linkage key already used
#     in the QSA and empresas tables.
#   - The relevant metric is QTD_VINC_ATIVOS (active employment links), which
#     is the closest proxy available in public RAIS data for employee headcount.
#     We alias this to qtd_funcionarios to match the dim_fornecedor column name.
#   - porte_empresa (company size class: MICRO, PEQUENA, MEDIA, GRANDE) is
#     preserved as a nullable string. Not all RAIS extracts include it.
#   - Column names are normalised to uppercase before any access so the parser
#     handles minor year-to-year variations in header capitalisation.
#
# Invariants:
#   - cnpj_basico is stored as a raw string (not zero-padded here; the
#     validate step normalises it via str.zfill(8) during dedup).
#   - qtd_funcionarios is cast to Int64; invalid values become null.
#   - porte_empresa may be null (not all source rows carry this field).
from __future__ import annotations

from pathlib import Path

import polars as pl


def parse_rais(raw_path: Path) -> pl.DataFrame:
    """Parse a RAIS CSV file into a typed employee-count staging DataFrame.

    The result carries cnpj_basico as the join key to dim_fornecedor and
    qtd_funcionarios as the metric used by the SEM_FUNCIONARIOS score
    indicator.

    Args:
        raw_path: Path to the raw RAIS CSV file (Latin-1, semicolon-delimited).

    Returns:
        Polars DataFrame with columns: cnpj_basico (str), qtd_funcionarios
        (int), porte_empresa (str | null).
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    # Normalise column names to uppercase and stripped.
    raw = raw.rename({col: col.strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    # QTD_VINC_ATIVOS is the active employment link count per establishment.
    # Cast to Int64; invalid strings become null (strict=False).
    if "QTD_VINC_ATIVOS" in raw.columns:
        qtd_funcionarios = raw["QTD_VINC_ATIVOS"].str.strip_chars().cast(pl.Int64, strict=False)
    else:
        qtd_funcionarios = pl.Series("qtd_funcionarios", [None] * n, dtype=pl.Int64)

    # porte_empresa may not be present in all yearly extracts — fall back to null.
    porte_col = "PORTE" if "PORTE" in raw.columns else "PORTE_EMPRESA"
    porte_series = _safe_str(porte_col)

    # Build base DataFrame and then clean porte_empresa with expressions.
    result = pl.DataFrame(
        {
            "cnpj_basico": _safe_str("CNPJ_BASICO"),
            "qtd_funcionarios": qtd_funcionarios.alias("qtd_funcionarios"),
            "porte_empresa": porte_series.alias("porte_empresa"),
        }
    )

    # Replace empty-string porte with null for consistent downstream handling.
    return result.with_columns(
        pl.when(pl.col("porte_empresa") == "")
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.col("porte_empresa"))
        .alias("porte_empresa")
    )
