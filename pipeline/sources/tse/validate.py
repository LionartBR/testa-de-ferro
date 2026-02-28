# pipeline/sources/tse/validate.py
#
# Validate and clean the doacoes DataFrame from TSE.
#
# Design decisions:
#   - Zero-value donations are dropped: they represent void records or
#     corrections that are not meaningful for scoring. Negative values are
#     also dropped (reversals are out of scope for the current model).
#   - Rows without doc_doador are dropped: without a donor document we cannot
#     link the donation to any fornecedor or sócio.
#   - Deduplication key is (doc_doador, doc_candidato, ano_eleicao, valor):
#     the same donor can give to the same candidate in the same year but with
#     different dates (installments). We only deduplicate exact same-value,
#     same-year, same-candidate records to avoid over-dropping.
#   - pk_doacao is re-keyed after deduplication.
#
# Invariants:
#   - valor > 0 in every surviving row.
#   - doc_doador is non-null in every surviving row.
#   - ano_eleicao is non-null.
from __future__ import annotations

import polars as pl


def validate_doacoes(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean the doacoes DataFrame from parse_doacoes.

    Steps applied:
        1. Drop rows where valor is null, zero, or negative.
        2. Drop rows where doc_doador is null or empty.
        3. Drop rows where ano_eleicao is null.
        4. Deduplicate by (doc_doador, doc_candidato, ano_eleicao, valor).
        5. Reset pk_doacao to a contiguous 1-based index.

    Args:
        df: DataFrame returned by parse_doacoes().

    Returns:
        Cleaned DataFrame. valor > 0, doc_doador non-null.
    """
    # Step 1: require positive valor.
    if "valor" in df.columns:
        df = df.filter(pl.col("valor").is_not_null() & (pl.col("valor") > 0))

    # Step 2: require doc_doador.
    if "doc_doador" in df.columns:
        df = df.filter(pl.col("doc_doador").is_not_null() & (pl.col("doc_doador").str.strip_chars() != ""))

    # Step 3: require ano_eleicao.
    if "ano_eleicao" in df.columns:
        df = df.filter(pl.col("ano_eleicao").is_not_null())

    # Step 4: deduplicate.
    dedup_cols = [c for c in ("doc_doador", "doc_candidato", "ano_eleicao", "valor") if c in df.columns]
    if dedup_cols:
        df = df.unique(subset=dedup_cols, keep="first", maintain_order=True)

    # Step 5: re-key pk_doacao.
    n = len(df)
    df = df.with_columns(pl.Series("pk_doacao", list(range(1, n + 1))))

    return df
