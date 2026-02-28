# pipeline/sources/servidores/validate.py
#
# Validate and clean the servidores DataFrame.
#
# Design decisions:
#   - Deduplication key is (nome, digitos_visiveis): two rows with the same
#     name and the same visible CPF digits are almost certainly the same person
#     (see CLAUDE.md for the collision probability analysis). We keep the first
#     occurrence (which preserves the most recent file order).
#   - Rows without nome are dropped: nameless records cannot be matched against
#     QSA data and have no value in dim_socio.
#   - digitos_visiveis null rows are NOT dropped: some Portal exports include
#     blank CPF columns for political appointees (DAS) who are not required to
#     disclose CPFs. These servants may still match by name alone in a looser
#     analysis, so they are kept with null digitos_visiveis.
#
# Invariants:
#   - nome is non-null in every surviving row.
#   - is_servidor_publico is always True.
from __future__ import annotations

import polars as pl


def validate_servidores(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean the servidores DataFrame from parse_servidores.

    Steps applied:
        1. Drop rows where nome is null or empty.
        2. Deduplicate by (nome, digitos_visiveis), keeping first.

    Args:
        df: DataFrame returned by parse_servidores().

    Returns:
        Cleaned DataFrame. nome is guaranteed non-null.
    """
    # Step 1: require nome.
    df = df.filter(pl.col("nome").is_not_null() & (pl.col("nome").str.strip_chars() != ""))

    # Step 2: deduplicate by (nome, digitos_visiveis).
    df = df.unique(subset=["nome", "digitos_visiveis"], keep="first", maintain_order=True)

    return df
