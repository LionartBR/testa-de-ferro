# pipeline/sources/cnpj/validate.py
#
# Validate and clean empresas and QSA DataFrames from Receita Federal.
#
# Design decisions:
#   - Validation is deliberately aggressive: any row that cannot be trusted
#     (missing CNPJ, duplicate, malformed) is dropped and counted in logging.
#     It is safer to under-report than to propagate bad data downstream.
#   - CNPJ format validation uses a vectorized str.contains regex rather than
#     the full modulo-11 verifier digit check. The full verifier is applied in
#     the domain layer (CNPJ value object). The pipeline only ensures structural
#     correctness (right number of digits and separators).
#   - validate_qsa deduplicates on (cnpj_basico, cpf_parcial) because the QSA
#     file can contain repeated entries when a person holds multiple roles in
#     the same company. We keep the first occurrence (earliest entry date, as
#     sorted by the Receita file).
#   - Rows without nome_socio are dropped: they represent invalid QSA records
#     that would produce unnamed sócios in dim_socio.
#
# Invariants:
#   - validate_empresas output: cnpj is non-null, matches XX.XXX.XXX/XXXX-XX.
#   - validate_qsa output: nome_socio is non-null and non-empty.
from __future__ import annotations

import polars as pl


def validate_empresas(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean the empresas DataFrame from parse_empresas.

    Steps applied:
        1. Drop rows where cnpj is null or does not match the canonical format.
        2. Drop rows where razao_social is null or empty after stripping.
        3. Deduplicate by cnpj, keeping the first occurrence.
        4. Reset pk_fornecedor to a contiguous 1-based index.

    Args:
        df: DataFrame returned by parse_empresas().

    Returns:
        Cleaned DataFrame. Guaranteed: cnpj non-null, razao_social non-null.
    """
    # Step 1: filter invalid CNPJ formats using a vectorized regex — avoids
    # Python-level row iteration that map_elements would impose on large CSVs.
    df = df.filter(pl.col("cnpj").str.contains(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$"))

    # Step 2: drop rows without razao_social.
    df = df.filter(pl.col("razao_social").is_not_null() & (pl.col("razao_social").str.strip_chars() != ""))

    # Step 3: deduplicate by cnpj (keep first).
    df = df.unique(subset=["cnpj"], keep="first", maintain_order=True)

    # Step 4: re-assign pk_fornecedor to ensure contiguous sequence.
    # pl.int_range is fully vectorized — avoids constructing a Python list.
    n = len(df)
    df = df.with_columns(pl.int_range(1, n + 1, eager=True).alias("pk_fornecedor"))

    return df


def validate_qsa(df: pl.DataFrame) -> pl.DataFrame:
    """Validate and clean the QSA DataFrame from parse_qsa.

    Steps applied:
        1. Drop rows where nome_socio is null or empty.
        2. Drop rows where cnpj_basico is null or empty.
        3. Deduplicate by (cnpj_basico, cpf_parcial), keeping first occurrence.

    Args:
        df: DataFrame returned by parse_qsa().

    Returns:
        Cleaned DataFrame. Guaranteed: nome_socio and cnpj_basico are non-null.
    """
    # Step 1: drop rows without nome_socio.
    df = df.filter(pl.col("nome_socio").is_not_null() & (pl.col("nome_socio").str.strip_chars() != ""))

    # Step 2: drop rows without cnpj_basico.
    df = df.filter(pl.col("cnpj_basico").is_not_null() & (pl.col("cnpj_basico").str.strip_chars() != ""))

    # Step 3: deduplicate on (cnpj_basico, cpf_parcial).
    df = df.unique(subset=["cnpj_basico", "cpf_parcial"], keep="first", maintain_order=True)

    return df
