# pipeline/sources/sancoes/parse_cepim.py
#
# Parse CEPIM (Cadastro de Entidades Privadas Sem Fins Lucrativos Impedidas)
# CSV from the Portal da Transparência into a dim_sancao staging DataFrame.
#
# Design decisions:
#   - CEPIM targets non-profit entities (OSCs) with irregular accounts in
#     federal agreements (convênios). The CSV structure is identical to CEIS
#     and CNEP at the column level.
#   - tipo_sancao is "CEPIM" for all rows.
#   - All column mapping decisions match parse_ceis. See that module for the
#     full ADR on column names, date format, and encoding.
#
# Invariants:
#   - tipo_sancao is always "CEPIM".
#   - cnpj and data_inicio are non-null in validated output.
from __future__ import annotations

from pathlib import Path

import polars as pl

# Real column names from the Portal da Transparência CSV header.
# See parse_ceis.py for the full ADR on column mapping.
_COL_CNPJ = "CPF OU CNPJ DO SANCIONADO"
_COL_RAZAO_SOCIAL = "RAZÃO SOCIAL - CADASTRO RECEITA"
_COL_NOME_SANCIONADO = "NOME DO SANCIONADO"
_COL_ORGAO = "ÓRGÃO SANCIONADOR"
_COL_MOTIVO = "CATEGORIA DA SANÇÃO"
_COL_DATA_INICIO = "DATA INÍCIO SANÇÃO"
_COL_DATA_FIM = "DATA FINAL SANÇÃO"


def parse_cepim(raw_path: Path) -> pl.DataFrame:
    """Parse CEPIM CSV into a dim_sancao staging DataFrame.

    Args:
        raw_path: Path to the CEPIM CSV file from Portal da Transparência.

    Returns:
        Polars DataFrame with dim_sancao columns and tipo_sancao fixed to CEPIM.
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
            "tipo_sancao": pl.Series("tipo_sancao", ["CEPIM"] * n, dtype=pl.Utf8),
            "orgao_sancionador": _safe_str(_COL_ORGAO),
            "motivo": _safe_str(_COL_MOTIVO),
            "data_inicio": _parse_date(_COL_DATA_INICIO),
            "data_fim": _parse_date(_COL_DATA_FIM),
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
        }
    )
