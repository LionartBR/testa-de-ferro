# pipeline/sources/rais/validate.py
#
# Validate and clean the RAIS DataFrame produced by parse_rais.
#
# Design decisions:
#   - Rows without cnpj_basico cannot be linked to any fornecedor — they are
#     dropped immediately. An empty cnpj_basico is treated the same as null.
#   - Duplicate cnpj_basico rows are resolved by keeping the one with the
#     highest qtd_funcionarios. This is conservative: if a CNPJ root appears
#     in multiple RAIS establishment records (e.g. multiple branches), using
#     the maximum avoids incorrectly flagging SEM_FUNCIONARIOS when at least
#     one branch has employees.
#   - Rows where qtd_funcionarios is negative are dropped. Negative counts
#     indicate data errors or correction records from RAIS; they are not
#     meaningful headcounts and must not pollute the score computation.
#   - Rows where qtd_funcionarios is null (parse failed) are also dropped —
#     the score indicator requires a numeric value to make a decision.
#   - porte_empresa is left as-is (nullable) — it is informational only.
#
# Invariants:
#   - cnpj_basico is non-null in every surviving row.
#   - qtd_funcionarios >= 0 in every surviving row.
#   - Each cnpj_basico appears at most once (deduped by max).
from __future__ import annotations

import polars as pl


def validate_rais(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean a RAIS DataFrame from parse_rais.

    Steps applied:
        1. Drop rows where cnpj_basico is null or empty.
        2. Drop rows where qtd_funcionarios is null or negative.
        3. Deduplicate by cnpj_basico, keeping the row with the highest
           qtd_funcionarios (max per group).

    Args:
        df: DataFrame returned by parse_rais().

    Returns:
        Cleaned DataFrame. porte_empresa may remain null.
    """
    if df.is_empty():
        return df

    # Step 1: require non-null, non-empty cnpj_basico.
    df = df.filter(pl.col("cnpj_basico").is_not_null() & (pl.col("cnpj_basico").str.strip_chars() != ""))

    # Step 2: require non-null, non-negative qtd_funcionarios.
    df = df.filter(pl.col("qtd_funcionarios").is_not_null() & (pl.col("qtd_funcionarios") >= 0))

    if df.is_empty():
        return df

    # Step 3: deduplicate by cnpj_basico, keeping the row with the highest
    # qtd_funcionarios. We use group_by + agg to find the max, then join
    # back to retrieve the porte_empresa of the winning row.
    max_qtd = df.group_by("cnpj_basico").agg(pl.col("qtd_funcionarios").max().alias("qtd_funcionarios"))

    # Re-join to get porte_empresa from the row that matches the max count.
    # If two rows tie on the max, unique(keep="last") after sorting picks one
    # deterministically.
    df_sorted = df.sort("qtd_funcionarios", descending=True)
    deduped = df_sorted.unique(subset=["cnpj_basico"], keep="first", maintain_order=True)

    # Replace qtd_funcionarios with the verified max (handles ties correctly).
    deduped = deduped.drop("qtd_funcionarios").join(max_qtd, on="cnpj_basico", how="left")

    return deduped.select(["cnpj_basico", "qtd_funcionarios", "porte_empresa"])
