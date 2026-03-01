# pipeline/transform/resolve_fks.py
#
# Resolve NULL fk_fornecedor in staging fact tables by joining on CNPJ.
#
# Design decisions:
#   - Each function is a pure transformation: DataFrame in, DataFrame out.
#   - CNPJ matching strips all punctuation (dots, slashes, dashes) from both
#     sides before joining. This handles the format mismatch between empresas
#     (formatted: "11.222.333/0001-81") and fact tables (digits only or mixed).
#   - For sancoes, the cnpj column may contain CPFs (11 digits) — these are
#     excluded from FK resolution since they are not CNPJ references.
#   - For doacoes, only rows with tipo_doador == "CNPJ" are resolved.
#   - The left join preserves all original rows: unmatched rows keep NULL FK.
#   - All original columns are preserved in the output.
#
# ADR: Why resolve FKs in a separate phase?
#   The parse/validate layer produces NULL FKs because it has no access to the
#   empresas dimension table (each source is parsed independently). The FK
#   resolution runs after all sources are merged (Phase A) and before any
#   cross-referencing transforms (Phase B+). This keeps the parse layer pure
#   and the FK resolution centralised in one module.
#
# Invariants:
#   - Output has the same number of rows as input.
#   - Output preserves all input columns.
#   - fk_fornecedor is populated where a CNPJ match exists, NULL otherwise.
from __future__ import annotations

import polars as pl


def _strip_cnpj_punctuation(expr: pl.Expr) -> pl.Expr:
    """Strip dots, slashes, and dashes from a CNPJ string expression."""
    return expr.str.replace_all(r"[.\-/]", "")


def _build_lookup(empresas_df: pl.DataFrame) -> pl.DataFrame:
    """Build a CNPJ → pk_fornecedor lookup from the empresas DataFrame.

    Selects only the two columns needed and strips CNPJ punctuation for
    consistent matching.
    """
    return empresas_df.select(
        _strip_cnpj_punctuation(pl.col("cnpj")).alias("_cnpj_join"),
        pl.col("pk_fornecedor").alias("_resolved_fk"),
    )


def resolver_fk_contratos(
    contratos_df: pl.DataFrame,
    empresas_df: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve fk_fornecedor in contratos by joining on cnpj_fornecedor.

    Args:
        contratos_df: Staging contratos with cnpj_fornecedor (digits only)
                      and NULL fk_fornecedor.
        empresas_df:  Staging empresas with pk_fornecedor and cnpj (formatted).

    Returns:
        contratos_df with fk_fornecedor populated where a match exists.
    """
    lookup = _build_lookup(empresas_df)

    # cnpj_fornecedor is already digits-only from PNCP parse, but strip
    # just in case other sources (Comprasnet) have different formatting.
    result = contratos_df.with_columns(
        _strip_cnpj_punctuation(pl.col("cnpj_fornecedor")).alias("_cnpj_join")
    )

    result = result.join(lookup, on="_cnpj_join", how="left")

    # Overwrite fk_fornecedor with resolved value (keep NULL if no match).
    result = result.with_columns(
        pl.col("_resolved_fk").alias("fk_fornecedor")
    ).drop("_cnpj_join", "_resolved_fk")

    return result


def resolver_fk_sancoes(
    sancoes_df: pl.DataFrame,
    empresas_df: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve fk_fornecedor in sancoes by joining on cnpj.

    Only resolves CNPJ entries (14 digits after stripping). CPF entries
    (11 digits) are left with NULL fk_fornecedor since they reference
    individuals, not companies.

    Args:
        sancoes_df:  Staging sancoes with cnpj (formatted or mixed) and
                     NULL fk_fornecedor.
        empresas_df: Staging empresas with pk_fornecedor and cnpj (formatted).

    Returns:
        sancoes_df with fk_fornecedor populated for CNPJ matches.
    """
    lookup = _build_lookup(empresas_df)

    # Strip punctuation and compute digit length to filter out CPFs.
    result = sancoes_df.with_columns(
        _strip_cnpj_punctuation(pl.col("cnpj")).alias("_cnpj_stripped")
    )

    # Only join rows that are CNPJs (14 digits); CPFs (11 digits) get NULL.
    result = result.with_columns(
        pl.when(pl.col("_cnpj_stripped").str.len_chars() == 14)
        .then(pl.col("_cnpj_stripped"))
        .otherwise(pl.lit(None))
        .alias("_cnpj_join")
    )

    result = result.join(lookup, on="_cnpj_join", how="left")

    result = result.with_columns(
        pl.col("_resolved_fk").alias("fk_fornecedor")
    ).drop("_cnpj_stripped", "_cnpj_join", "_resolved_fk")

    return result


def resolver_fk_doacoes(
    doacoes_df: pl.DataFrame,
    empresas_df: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve fk_fornecedor in doacoes by joining on doc_doador.

    Only resolves rows where tipo_doador == "CNPJ". CPF donors are left
    with NULL fk_fornecedor.

    Args:
        doacoes_df:  Staging doacoes with doc_doador and NULL fk_fornecedor.
        empresas_df: Staging empresas with pk_fornecedor and cnpj (formatted).

    Returns:
        doacoes_df with fk_fornecedor populated for CNPJ donor matches.
    """
    lookup = _build_lookup(empresas_df)

    # doc_doador contains the raw document; only join when tipo_doador is CNPJ.
    result = doacoes_df.with_columns(
        pl.when(pl.col("tipo_doador") == "CNPJ")
        .then(_strip_cnpj_punctuation(pl.col("doc_doador")))
        .otherwise(pl.lit(None))
        .alias("_cnpj_join")
    )

    result = result.join(lookup, on="_cnpj_join", how="left")

    result = result.with_columns(
        pl.col("_resolved_fk").alias("fk_fornecedor")
    ).drop("_cnpj_join", "_resolved_fk")

    return result
