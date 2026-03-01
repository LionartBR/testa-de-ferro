# pipeline/transform/cruzamentos.py
#
# Cross-reference enrichments between data sources.
#
# Design decisions:
#   - enriquecer_socios and detectar_mesmo_endereco are pure functions over
#     Polars DataFrames. No IO, no global state.
#   - enriquecer_socios marks a sócio as "sancionado" when they appear as a
#     sócio of ANY company that has at least one active sanction. This is done
#     via a join between socios' cnpj_basico and sancoes' fk_fornecedor/cnpj.
#   - qtd_empresas_governo counts distinct CNPJs per (cpf_hmac OR nome_socio)
#     to measure how many government suppliers share the same individual.
#   - detectar_mesmo_endereco matches on (logradouro, numero) without
#     complemento (ADR: see below).
#
# ADR: MESMO_ENDERECO match ignores complemento.
#   Complemento (floor/suite) is inconsistently filled in company registrations.
#   Matching with it would produce false negatives for companies in the same
#   building. The accepted trade-off: companies in large office buildings
#   generate noise — this indicator only scores additively (never raises
#   an alert alone) and needs combination with other weak signals.
#
# ADR: Why not import from api/?
#   The pipeline is an offline standalone process. Duplicating constants is
#   intentional — the pipeline must not depend on the web stack at build time.
#
# Invariants:
#   - All input DataFrames are treated as immutable (no in-place mutation).
#   - Returned DataFrames carry a superset of the input columns.
from __future__ import annotations

from datetime import date

import polars as pl


def enriquecer_socios(
    socios_df: pl.DataFrame,
    sancoes_df: pl.DataFrame,
    empresas_df: pl.DataFrame,
    *,
    referencia: date | None = None,
) -> pl.DataFrame:
    """Enrich sócios with sanction and multi-company flags.

    Computes derived fields for every row in *socios_df*:

    - ``is_sancionado`` (bool): True when this sócio appears as a sócio of
      at least one company that has an **active** sanction (data_fim is NULL
      or data_fim > referencia). Expired sanctions never set this flag.

    - ``sancionado_cnpj_basico`` (str|null): CNPJ root of the sanctioned
      company (for traceability in alert evidence).

    - ``sancionado_razao_social`` (str|null): Razão social of the sanctioned
      company (for traceability in alert evidence).

    - ``qtd_empresas_governo``: count of distinct CNPJs (from *empresas_df*)
      in which this sócio (identified by ``nome_socio``) appears.

    Args:
        socios_df:   DataFrame with columns: cnpj_basico (str), nome_socio (str).
                     May include additional columns (all preserved).
        sancoes_df:  DataFrame with column: cnpj (str, formatted or basico),
                     data_fim (date|null). Represents companies with sanctions.
        empresas_df: DataFrame with columns: cnpj_basico (str), cnpj (str).
                     Represents all government-supplier companies.
        referencia:  Reference date for filtering active sanctions. Defaults to
                     today if not provided. Passed explicitly to keep the
                     function pure and testable.

    Returns:
        socios_df with additional columns: is_sancionado, sancionado_cnpj_basico,
        sancionado_razao_social, qtd_empresas_governo.
    """
    if referencia is None:
        referencia = date.today()

    # ------------------------------------------------------------------
    # is_sancionado: sócio's company has an ACTIVE sanction
    # ------------------------------------------------------------------
    # ADR: "Sanção expirada → indicador SANCAO_HISTORICA (score). Nunca alerta."
    # Filter to active sanctions only: data_fim is NULL (indefinite) or > referencia.
    if "data_fim" in sancoes_df.columns:
        sancoes_vigentes = sancoes_df.filter(
            pl.col("data_fim").is_null() | (pl.col("data_fim").cast(pl.Date) > pl.lit(referencia))
        )
    else:
        sancoes_vigentes = sancoes_df

    # Extract the 8-digit CNPJ root from the sancoes table so we can join
    # against the QSA cnpj_basico.
    if "cnpj_basico" in sancoes_vigentes.columns:
        sancionados_basico = sancoes_vigentes.select(
            pl.col("cnpj_basico").alias("_cnpj_basico_sancionado"),
        ).unique()
    else:
        # Fall back: derive basico from formatted CNPJ by stripping punctuation.
        sancionados_basico = sancoes_vigentes.select(
            pl.col("cnpj").str.replace_all(r"[.\-/]", "").str.slice(0, 8).alias("_cnpj_basico_sancionado"),
        ).unique()

    # is_in on a Polars Series is fully vectorized — no Python-level row
    # iteration unlike map_elements with a set lookup lambda.
    sancionados_list = sancionados_basico["_cnpj_basico_sancionado"].drop_nulls().to_list()
    is_sancionado_series = socios_df["cnpj_basico"].is_in(sancionados_list).alias("is_sancionado")

    # ------------------------------------------------------------------
    # Propagate sanctioned company identity for evidence traceability
    # ------------------------------------------------------------------
    # Build a lookup: cnpj_basico → razao_social of the sanctioned company.
    if "razao_social" in sancoes_vigentes.columns and "cnpj_basico" in sancoes_vigentes.columns:
        sancionados_info = sancoes_vigentes.select(
            pl.col("cnpj_basico").alias("_sanc_cnpj_basico"),
            pl.col("razao_social").alias("sancionado_razao_social"),
        ).unique(subset=["_sanc_cnpj_basico"])
    else:
        sancionados_info = None

    # ------------------------------------------------------------------
    # qtd_empresas_governo: count of distinct CNPJs per sócio by name
    # ------------------------------------------------------------------
    if "cnpj_basico" in empresas_df.columns:
        empresas_cnpj_basico = empresas_df["cnpj_basico"].drop_nulls().unique().to_list()
    else:
        empresas_cnpj_basico = []

    # Filter socios to those that belong to known government-supplier companies.
    # empresas_cnpj_basico is already a plain Python list from to_list() — is_in
    # accepts it directly; no intermediate set conversion is needed.
    socios_in_gov = socios_df.filter(socios_df["cnpj_basico"].is_in(empresas_cnpj_basico))

    # Count how many distinct cnpj_basico each nome_socio appears in.
    nome_count = socios_in_gov.group_by("nome_socio").agg(
        pl.col("cnpj_basico").n_unique().alias("_qtd_empresas_governo")
    )

    enriched = socios_df.with_columns(is_sancionado_series)

    # Join sanctioned company info for evidence traceability.
    if sancionados_info is not None:
        enriched = (
            enriched.join(sancionados_info, left_on="cnpj_basico", right_on="_sanc_cnpj_basico", how="left")
            .with_columns(
                pl.col("cnpj_basico").alias("sancionado_cnpj_basico"),
            )
        )
        # Only keep sancionado columns when is_sancionado is True.
        enriched = enriched.with_columns(
            pl.when(pl.col("is_sancionado"))
            .then(pl.col("sancionado_cnpj_basico"))
            .otherwise(pl.lit(None))
            .alias("sancionado_cnpj_basico"),
            pl.when(pl.col("is_sancionado"))
            .then(pl.col("sancionado_razao_social"))
            .otherwise(pl.lit(None))
            .alias("sancionado_razao_social"),
        )
    else:
        enriched = enriched.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("sancionado_cnpj_basico"),
            pl.lit(None).cast(pl.Utf8).alias("sancionado_razao_social"),
        )

    enriched = (
        enriched.join(nome_count, on="nome_socio", how="left")
        .with_columns(pl.col("_qtd_empresas_governo").fill_null(0).alias("qtd_empresas_governo"))
        .drop("_qtd_empresas_governo")
    )

    return enriched


def detectar_mesmo_endereco(
    empresas_df: pl.DataFrame,
) -> pl.DataFrame:
    """Detect pairs of companies registered at the same address.

    Matches companies on (logradouro, numero) without complemento.

    ADR: Complemento is intentionally excluded — see module docstring.

    Args:
        empresas_df: DataFrame with columns: cnpj (str), logradouro (str|null).

    Returns:
        DataFrame with columns:
          - cnpj_a (str)
          - cnpj_b (str)
          - endereco_compartilhado (str) — the shared "logradouro|numero" key.

        Only pairs where cnpj_a < cnpj_b are included (avoids (A,B) and (B,A)
        duplicates). Returns an empty DataFrame if no matches are found.
    """
    # Build a normalised address key per company.
    # We take the first word-sequence (street name) from logradouro before the
    # number, then append the number. Complemento is deliberately excluded.
    required_cols = {"cnpj", "logradouro"}
    missing = required_cols - set(empresas_df.columns)
    if missing:
        raise ValueError(f"empresas_df is missing columns: {missing}")

    # Build the normalised address key using vectorized Polars string ops.
    # Equivalent to the old _normalise_address_key Python function but without
    # row-level Python iteration.
    #
    # Strategy mirrors the scalar function:
    #   1. Upper-case and strip the logradouro.
    #   2. Extract the street number (first digit sequence, optionally preceded
    #      by a comma and optional spaces).
    #   3. Extract the street name: everything before that digit sequence,
    #      then strip trailing commas and whitespace.
    #   4. Concatenate as "STREET|NUMBER"; rows where either part is null
    #      (no number found, or no text before it) are filtered out.
    log_upper = pl.col("logradouro").str.strip_chars().str.to_uppercase()
    numero = log_upper.str.extract(r",?\s*(\d+)", 1)
    rua = log_upper.str.extract(r"^(.*?),?\s*\d+", 1).str.strip_chars().str.strip_chars_end(",").str.strip_chars()

    with_key = (
        empresas_df.select([pl.col("cnpj"), pl.col("logradouro")])
        .with_columns(
            pl.concat_str([rua, pl.lit("|"), numero]).alias("_address_key"),
        )
        .filter(pl.col("_address_key").is_not_null())
    )

    # Self-join on the address key to find companies at the same address.
    pairs = with_key.join(
        with_key.rename({"cnpj": "cnpj_b", "logradouro": "_logradouro_b"}),
        on="_address_key",
        how="inner",
    )

    # Keep only canonical pairs (cnpj_a < cnpj_b) to avoid duplicates.
    pairs = pairs.filter(pl.col("cnpj") < pl.col("cnpj_b"))

    return pairs.select(
        [
            pl.col("cnpj").alias("cnpj_a"),
            pl.col("cnpj_b"),
            pl.col("_address_key").alias("endereco_compartilhado"),
        ]
    )
