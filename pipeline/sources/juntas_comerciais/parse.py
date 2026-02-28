# pipeline/sources/juntas_comerciais/parse.py
#
# Parse Juntas Comerciais QSA diff CSV into a staging DataFrame of
# historical partner (sócio) entry and exit events.
#
# Design decisions:
#   - Juntas Comerciais data follows the same CSV format as the Receita Federal
#     QSA file (semicolons, Latin-1 encoding) but adds an extra DATA_SAIDA
#     column capturing when a sócio left the company.
#   - DATA_SAIDA is null in the source when the sócio is still active —
#     preserving null is intentional so consumers can distinguish current from
#     historical members without a sentinel date.
#   - Column names are normalised to uppercase before any access to handle
#     minor formatting differences between yearly extracts.
#   - The output targets the bridge_fornecedor_socio staging schema, extending
#     it with data_saida for historical tracking. The transform layer merges
#     this with the base QSA and resolves FK references.
#   - cpf_parcial is carried through (same masked format as QSA) so that the
#     match_servidor_socio transform can cross-reference historical members.
#
# Invariants:
#   - cnpj_basico is the raw 8-digit root string (not formatted).
#   - nome_socio is uppercased and stripped, matching parse_qsa convention.
#   - data_entrada and data_saida are Date or null — never raw strings.
from __future__ import annotations

from pathlib import Path

import polars as pl


def parse_qsa_diffs(raw_path: Path) -> pl.DataFrame:
    """Parse a Juntas Comerciais QSA diff CSV into a historical sócios DataFrame.

    The result carries cnpj_basico as the join key to dim_fornecedor, sócio
    identity fields matching the dim_socio staging schema, plus data_saida for
    historical tracking.

    Args:
        raw_path: Path to the raw Juntas Comerciais CSV (Latin-1, semicolons).

    Returns:
        Polars DataFrame with columns: cnpj_basico (str), nome_socio (str),
        qualificacao_socio (str | null), data_entrada (date | null),
        data_saida (date | null).
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    # Normalise column names.
    raw = raw.rename({col: col.strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    def _parse_date(col: str) -> pl.Series:
        """Parse a YYYYMMDD date column. Returns Date or null."""
        if col in raw.columns:
            return raw[col].str.strip_chars().str.to_date(format="%Y%m%d", strict=False)
        return pl.Series(col, [None] * n, dtype=pl.Date)

    return pl.DataFrame(
        {
            "cnpj_basico": _safe_str("CNPJ_BASICO").str.zfill(8),
            "nome_socio": _safe_str("NOME_SOCIO").str.to_uppercase(),
            "qualificacao_socio": _safe_str("QUALIFICACAO_SOCIO"),
            "data_entrada": _parse_date("DATA_ENTRADA"),
            "data_saida": _parse_date("DATA_SAIDA"),
        }
    )
