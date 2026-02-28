# pipeline/sources/sancoes/parse_ceis.py
#
# Parse CEIS (Cadastro de Empresas Inidôneas e Suspensas) CSV from the
# Portal da Transparência into a dim_sancao staging DataFrame.
#
# Design decisions:
#   - CEIS CSV uses semicolons and Latin-1 encoding (Portal da Transparência
#     standard). Column names vary slightly across yearly extracts, so we
#     normalise them to uppercase before any access.
#   - data_fim null means the sanction is still active (vigente). The pipeline
#     preserves null so the domain layer can distinguish vigente vs expirada.
#   - tipo_sancao is hard-coded to "CEIS" so the domain layer can filter by
#     source without needing a separate flag column.
#   - fk_fornecedor is left as null; it is resolved in the transform layer
#     after dim_fornecedor is built.
#   - pk_sancao is set to row index and re-keyed by the transform layer after
#     all three sanction sources (CEIS, CNEP, CEPIM) are merged.
#
# Invariants:
#   - cnpj is non-null in every row (enforced by validate_sancoes).
#   - data_inicio is non-null (required by dim_sancao schema).
#   - tipo_sancao is always "CEIS".
from __future__ import annotations

from pathlib import Path

import polars as pl


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

    raw = raw.rename({col: col.strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    def _parse_date(col: str) -> pl.Series:
        """Parse ISO-8601 date column (YYYY-MM-DD). Returns Date or null."""
        if col in raw.columns:
            return raw[col].str.strip_chars().str.to_date(format="%Y-%m-%d", strict=False)
        return pl.Series(col, [None] * n, dtype=pl.Date)

    return pl.DataFrame(
        {
            "pk_sancao": list(range(1, n + 1)),
            "cnpj": _safe_str("CNPJ"),
            "razao_social": _safe_str("RAZAO_SOCIAL"),
            "tipo_sancao": pl.Series("tipo_sancao", ["CEIS"] * n, dtype=pl.Utf8),
            "orgao_sancionador": _safe_str("ORGAO_SANCIONADOR"),
            "motivo": _safe_str("MOTIVO"),
            "data_inicio": _parse_date("DATA_INICIO_SANCAO"),
            "data_fim": _parse_date("DATA_FIM_SANCAO"),
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
        }
    )
