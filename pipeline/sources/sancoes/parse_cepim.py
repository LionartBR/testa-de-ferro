# pipeline/sources/sancoes/parse_cepim.py
#
# Parse CEPIM (Cadastro de Entidades Privadas Sem Fins Lucrativos Impedidas)
# CSV from the Portal da Transparência into a dim_sancao staging DataFrame.
#
# Design decisions:
#   - CEPIM targets non-profit entities (OSCs) with irregular accounts in
#     federal agreements (convênios). The entity may be identified by CNPJ
#     only — there may not be a razao_social column in all extracts.
#   - tipo_sancao is "CEPIM" for all rows.
#   - All other design decisions match parse_ceis. See that module.
#
# Invariants:
#   - tipo_sancao is always "CEPIM".
#   - cnpj and data_inicio are non-null in validated output.
from __future__ import annotations

from pathlib import Path

import polars as pl


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

    raw = raw.rename({col: col.strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    def _parse_date(col: str) -> pl.Series:
        if col in raw.columns:
            return (
                raw[col]
                .str.strip_chars()
                .str.to_date(format="%Y-%m-%d", strict=False)
            )
        return pl.Series(col, [None] * n, dtype=pl.Date)

    return pl.DataFrame(
        {
            "pk_sancao": list(range(1, n + 1)),
            "cnpj": _safe_str("CNPJ"),
            "razao_social": _safe_str("RAZAO_SOCIAL"),
            "tipo_sancao": pl.Series("tipo_sancao", ["CEPIM"] * n, dtype=pl.Utf8),
            "orgao_sancionador": _safe_str("ORGAO_SANCIONADOR"),
            "motivo": _safe_str("MOTIVO"),
            "data_inicio": _parse_date("DATA_INICIO_SANCAO"),
            "data_fim": _parse_date("DATA_FIM_SANCAO"),
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
        }
    )
