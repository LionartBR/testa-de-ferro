# pipeline/sources/sancoes/validate.py
#
# Validate and clean the sancoes DataFrame (CEIS, CNEP, or CEPIM).
#
# Design decisions:
#   - The same validate function is shared across all three sanction sources
#     because they produce identical column schemas. The only structural
#     difference is the tipo_sancao value, which is already fixed at parse time.
#   - data_fim null means vigente (active sanction) — this is a domain rule
#     documented in CLAUDE.md. The validator must NOT fill null data_fim with
#     anything; it preserves the semantics.
#   - Rows without cnpj are dropped: without a CNPJ we cannot JOIN the
#     sanction to dim_fornecedor. These are likely header rows or blank lines.
#   - Rows without data_inicio are dropped: the DuckDB schema defines
#     data_inicio as NOT NULL. A sanction without a start date is unqueryable.
#   - Deduplication key is (cnpj, tipo_sancao, data_inicio) — two sanctions
#     of the same type issued on the same date against the same company are
#     treated as duplicates.
#
# Invariants:
#   - cnpj and data_inicio are non-null in every surviving row.
#   - data_fim may be null (vigente semantics preserved).
from __future__ import annotations

import polars as pl


def validate_sancoes(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean a sancoes DataFrame (CEIS, CNEP, or CEPIM).

    Steps applied:
        1. Drop rows where cnpj is null or empty.
        2. Drop rows where data_inicio is null (required by schema).
        3. Deduplicate by (cnpj, tipo_sancao, data_inicio), keeping first.
        4. Reset pk_sancao to a contiguous 1-based index.

    Args:
        df: DataFrame returned by any of parse_ceis / parse_cnep / parse_cepim.

    Returns:
        Cleaned DataFrame. data_fim may remain null (vigente semantics).
    """
    # Step 1: require cnpj.
    df = df.filter(pl.col("cnpj").is_not_null() & (pl.col("cnpj").str.strip_chars() != ""))

    # Step 2: require data_inicio.
    df = df.filter(pl.col("data_inicio").is_not_null())

    # Step 3: deduplicate.
    df = df.unique(
        subset=["cnpj", "tipo_sancao", "data_inicio"],
        keep="first",
        maintain_order=True,
    )

    # Step 4: re-key pk_sancao.
    n = len(df)
    df = df.with_columns(pl.Series("pk_sancao", list(range(1, n + 1))))

    return df
