# pipeline/sources/sancoes/parse_ceis.py
#
# Parse CEIS (Cadastro de Empresas Inidôneas e Suspensas) CSV from the
# Portal da Transparência into a dim_sancao staging DataFrame.
#
# Design decisions:
#   - CEIS CSV uses semicolons and Latin-1 encoding (Portal da Transparência
#     standard). Column names use descriptive Portuguese with spaces and
#     accented characters (e.g. "CPF OU CNPJ DO SANCIONADO"). After reading
#     with encoding="latin1", we strip surrounding whitespace/quotes and
#     normalise to uppercase, then look up columns by their real names.
#   - data_fim null means the sanction is still active (vigente). The pipeline
#     preserves null so the domain layer can distinguish vigente vs expirada.
#   - tipo_sancao is taken from the "CADASTRO" column (always "CEIS" in a
#     CEIS extract) but we hard-code "CEIS" to be safe against formatting
#     variations.
#   - razao_social prefers "RAZÃO SOCIAL - CADASTRO RECEITA"; falls back to
#     "NOME DO SANCIONADO" when the Receita name is missing.
#   - fk_fornecedor is left as null; it is resolved in the transform layer
#     after dim_fornecedor is built.
#   - pk_sancao is set to row index and re-keyed by the transform layer after
#     all three sanction sources (CEIS, CNEP, CEPIM) are merged.
#   - Dates are in DD/MM/YYYY format (Portal da Transparência standard).
#
# Invariants:
#   - cnpj is non-null in every row (enforced by validate_sancoes).
#   - data_inicio is non-null (required by dim_sancao schema).
#   - tipo_sancao is always "CEIS".
from __future__ import annotations

from pathlib import Path

import polars as pl

# ADR: Real column names from the Portal da Transparência CEIS/CNEP/CEPIM CSV.
#
# The header row uses descriptive Portuguese with accents and spaces, wrapped
# in double quotes, semicolon-separated, Latin-1 encoded. After Polars reads
# the file with encoding="latin1", the decoded column names preserve the
# original accented characters. We strip quotes/whitespace and uppercase them
# for resilient matching.
#
# Mapping from real CSV column → output column:
#   "CPF OU CNPJ DO SANCIONADO"        → cnpj
#   "RAZÃO SOCIAL - CADASTRO RECEITA"  → razao_social (primary)
#   "NOME DO SANCIONADO"               → razao_social (fallback)
#   "ÓRGÃO SANCIONADOR"                → orgao_sancionador
#   "CATEGORIA DA SANÇÃO"              → motivo
#   "DATA INÍCIO SANÇÃO"               → data_inicio (DD/MM/YYYY)
#   "DATA FINAL SANÇÃO"                → data_fim    (DD/MM/YYYY)
#   "CADASTRO"                         → tipo_sancao (hard-coded to "CEIS")

_COL_CNPJ = "CPF OU CNPJ DO SANCIONADO"
_COL_RAZAO_SOCIAL = "RAZÃO SOCIAL - CADASTRO RECEITA"
_COL_NOME_SANCIONADO = "NOME DO SANCIONADO"
_COL_ORGAO = "ÓRGÃO SANCIONADOR"
_COL_MOTIVO = "CATEGORIA DA SANÇÃO"
_COL_DATA_INICIO = "DATA INÍCIO SANÇÃO"
_COL_DATA_FIM = "DATA FINAL SANÇÃO"


def parse_ceis(raw_path: Path) -> pl.DataFrame:
    """Parse CEIS CSV into a dim_sancao staging DataFrame.

    Args:
        raw_path: Path to the CEIS CSV file from Portal da Transparência.

    Returns:
        Polars DataFrame with dim_sancao columns and tipo_sancao fixed to CEIS.
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    # Strip surrounding whitespace and quotes from column names, then uppercase.
    raw = raw.rename({col: col.strip().strip('"').strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        """Extract a string column, stripping whitespace. Returns null series if missing."""
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    def _parse_date(col: str) -> pl.Series:
        """Parse DD/MM/YYYY date column. Returns Date or null."""
        if col in raw.columns:
            return raw[col].str.strip_chars().str.to_date(format="%d/%m/%Y", strict=False)
        return pl.Series(col, [None] * n, dtype=pl.Date)

    # razao_social: prefer the Receita name, fall back to the sancionado name.
    razao_primary = _safe_str(_COL_RAZAO_SOCIAL)
    razao_fallback = _safe_str(_COL_NOME_SANCIONADO)
    razao_social = pl.DataFrame({"primary": razao_primary, "fallback": razao_fallback}).select(
        pl.when(pl.col("primary").is_not_null() & (pl.col("primary") != ""))
        .then(pl.col("primary"))
        .otherwise(pl.col("fallback"))
        .alias("razao_social")
    )["razao_social"]

    return pl.DataFrame(
        {
            "pk_sancao": list(range(1, n + 1)),
            "cnpj": _safe_str(_COL_CNPJ),
            "razao_social": razao_social,
            "tipo_sancao": pl.Series("tipo_sancao", ["CEIS"] * n, dtype=pl.Utf8),
            "orgao_sancionador": _safe_str(_COL_ORGAO),
            "motivo": _safe_str(_COL_MOTIVO),
            "data_inicio": _parse_date(_COL_DATA_INICIO),
            "data_fim": _parse_date(_COL_DATA_FIM),
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
        }
    )
