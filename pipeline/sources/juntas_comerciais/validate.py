# pipeline/sources/juntas_comerciais/validate.py
#
# Validate and clean the Juntas Comerciais QSA diffs DataFrame.
#
# Design decisions:
#   - Rows without cnpj_basico cannot be linked to any fornecedor and are
#     dropped. An empty string is treated the same as null.
#   - Rows without nome_socio are dropped: a partner record without a name
#     provides no information for the match_servidor_socio transform and
#     would create a useless row in dim_socio.
#   - Deduplication key is (cnpj_basico, nome_socio, data_entrada). This
#     matches the convention used by validate_qsa and avoids inflating the
#     partner count if the same event appears more than once in the diff file.
#   - data_saida may be null (meaning the sócio is still active) and must NOT
#     be part of the dedup key — two records for the same entry event should
#     collapse even if one has data_saida and the other does not.
#
# Invariants:
#   - cnpj_basico and nome_socio are non-null in every surviving row.
#   - data_saida may remain null (active member semantics preserved).
from __future__ import annotations

import polars as pl


def validate_qsa_diffs(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean a Juntas Comerciais QSA diffs DataFrame.

    Steps applied:
        1. Drop rows where cnpj_basico is null or empty.
        2. Drop rows where nome_socio is null or empty.
        3. Deduplicate by (cnpj_basico, nome_socio, data_entrada), keeping first.

    Args:
        df: DataFrame returned by parse_qsa_diffs().

    Returns:
        Cleaned DataFrame. data_saida may remain null.
    """
    # Step 1: require cnpj_basico.
    df = df.filter(pl.col("cnpj_basico").is_not_null() & (pl.col("cnpj_basico").str.strip_chars() != ""))

    # Step 2: require nome_socio.
    df = df.filter(pl.col("nome_socio").is_not_null() & (pl.col("nome_socio").str.strip_chars() != ""))

    # Step 3: deduplicate on (cnpj_basico, nome_socio, data_entrada).
    df = df.unique(
        subset=["cnpj_basico", "nome_socio", "data_entrada"],
        keep="first",
        maintain_order=True,
    )

    return df
