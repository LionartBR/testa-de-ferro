# pipeline/sources/pncp/validate.py
#
# Validate and clean the contratos DataFrame from PNCP.
#
# Design decisions:
#   - objeto is truncated to 1000 characters here (not in parse) to keep the
#     parser purely structural and the validator responsible for DuckDB schema
#     compliance.
#   - Negative or zero valor rows are dropped: a contract with non-positive
#     value is either a test record, a reversal entry (not handled here), or
#     a data error. Reversals are intentionally excluded from scoring.
#   - Deduplication is on num_licitacao (the contract's unique identifier in
#     PNCP). If the same contract appears in multiple API pages, we keep the
#     first occurrence.
#   - cnpj_fornecedor null-check: contracts without a linked supplier CNPJ
#     cannot be FK-resolved and would fail the DuckDB FK constraint. Drop them.
#
# Invariants:
#   - valor > 0 in every surviving row.
#   - objeto is at most 1000 characters.
#   - cnpj_fornecedor is non-null.
from __future__ import annotations

import polars as pl

_OBJETO_MAX_LEN = 1000


def validate_contratos(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean a contratos DataFrame from parse_contratos.

    Steps applied:
        1. Drop rows where cnpj_fornecedor is null.
        2. Drop rows where valor is null, zero, or negative.
        3. Truncate objeto to 1000 characters.
        4. Deduplicate by num_licitacao, keeping first occurrence.
        5. Reset pk_contrato to a contiguous 1-based index.

    Args:
        df: DataFrame returned by parse_contratos().

    Returns:
        Cleaned DataFrame.
    """
    # Step 1: require cnpj_fornecedor.
    df = df.filter(pl.col("cnpj_fornecedor").is_not_null())

    # Step 2: require positive valor.
    if "valor" in df.columns:
        df = df.filter(
            pl.col("valor").is_not_null() & (pl.col("valor") > 0)
        )

    # Step 3: truncate objeto to max length.
    if "objeto" in df.columns:
        df = df.with_columns(
            pl.col("objeto")
            .str.slice(0, _OBJETO_MAX_LEN)
            .alias("objeto")
        )

    # Step 4: deduplicate by num_licitacao.
    if "num_licitacao" in df.columns:
        df = df.unique(subset=["num_licitacao"], keep="first", maintain_order=True)

    # Step 5: re-assign pk_contrato.
    n = len(df)
    df = df.with_columns(
        pl.Series("pk_contrato", list(range(1, n + 1)))
    )

    return df
