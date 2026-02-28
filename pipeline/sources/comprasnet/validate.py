# pipeline/sources/comprasnet/validate.py
#
# Validate and clean the Comprasnet contratos DataFrame.
#
# Design decisions:
#   - The validate function is structurally identical to validate_contratos
#     (PNCP) because both sources produce the same staging schema. The logic
#     is not shared via import to keep sources self-contained (see the same
#     rationale in pipeline/transform/score.py ADR for why cross-imports
#     between offline sources are avoided).
#   - Deduplication key is (cnpj_fornecedor, num_licitacao, data_assinatura).
#     SIASG may include the same contract in multiple yearly extracts. The
#     triple key avoids conflating different contracts with the same number
#     that were signed on different dates (amendments, re-issues).
#   - Rows with null cnpj_fornecedor are dropped: without a CNPJ we cannot
#     resolve the FK to dim_fornecedor.
#   - Rows with non-positive valor are dropped: SIASG occasionally contains
#     zero-value or negative adjustment entries that are not real contracts.
#   - objeto is truncated to 1000 characters to match the schema.sql VARCHAR
#     limit for fato_contrato.objeto.
#
# Invariants:
#   - cnpj_fornecedor is non-null in every surviving row.
#   - valor > 0 in every surviving row.
#   - objeto is at most 1000 characters.
from __future__ import annotations

import polars as pl

_OBJETO_MAX_LEN = 1000


def validate_comprasnet(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean a Comprasnet contratos DataFrame.

    Steps applied:
        1. Drop rows where cnpj_fornecedor is null.
        2. Drop rows where valor is null, zero, or negative.
        3. Truncate objeto to 1000 characters.
        4. Deduplicate by (cnpj_fornecedor, num_licitacao, data_assinatura).
        5. Reset pk_contrato to a contiguous 1-based index.

    Args:
        df: DataFrame returned by parse_comprasnet().

    Returns:
        Cleaned DataFrame.
    """
    # Step 1: require cnpj_fornecedor.
    df = df.filter(pl.col("cnpj_fornecedor").is_not_null())

    # Step 2: require positive valor.
    if "valor" in df.columns:
        df = df.filter(pl.col("valor").is_not_null() & (pl.col("valor") > 0))

    # Step 3: truncate objeto.
    if "objeto" in df.columns:
        df = df.with_columns(pl.col("objeto").str.slice(0, _OBJETO_MAX_LEN).alias("objeto"))

    # Step 4: deduplicate by the triple key.
    df = df.unique(
        subset=["cnpj_fornecedor", "num_licitacao", "data_assinatura"],
        keep="first",
        maintain_order=True,
    )

    # Step 5: re-assign pk_contrato.
    n = len(df)
    df = df.with_columns(pl.Series("pk_contrato", list(range(1, n + 1))))

    return df
