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

import re

import polars as pl

# Regex to extract the street number from a logradouro string.
# Accepts "Rua das Flores, 123" or "AV BRASIL 456" — captures the first
# continuous digit sequence that appears in or after the street name.
_NUMERO_PATTERN = re.compile(r",?\s*(\d+)")


def _extrair_numero(logradouro: str | None) -> str | None:
    """Extract the street number from a logradouro string.

    Args:
        logradouro: Full address string, possibly including the number.

    Returns:
        First digit sequence found in the string, or None if absent.
    """
    if not logradouro:
        return None
    match = _NUMERO_PATTERN.search(logradouro)
    if match:
        return match.group(1)
    return None


def enriquecer_socios(
    socios_df: pl.DataFrame,
    sancoes_df: pl.DataFrame,
    empresas_df: pl.DataFrame,
) -> pl.DataFrame:
    """Enrich sócios with sanction and multi-company flags.

    Computes two derived fields for every row in *socios_df*:

    - ``is_sancionado`` (bool): True when this sócio appears as a sócio of
      at least one company that has a sanction record in *sancoes_df*.

    - ``qtd_empresas_governo``: count of distinct CNPJs (from *empresas_df*)
      in which this sócio (identified by ``nome_socio``) appears.

    Args:
        socios_df:   DataFrame with columns: cnpj_basico (str), nome_socio (str).
                     May include additional columns (all preserved).
        sancoes_df:  DataFrame with column: cnpj (str, formatted or basico).
                     Represents companies with at least one sanction.
        empresas_df: DataFrame with columns: cnpj_basico (str), cnpj (str).
                     Represents all government-supplier companies.

    Returns:
        socios_df with two additional columns: is_sancionado, qtd_empresas_governo.
    """
    # ------------------------------------------------------------------
    # is_sancionado: sócio's company has a sanction
    # ------------------------------------------------------------------
    # Extract the 8-digit CNPJ root from the sancoes table so we can join
    # against the QSA cnpj_basico.
    if "cnpj_basico" in sancoes_df.columns:
        sancionados_basico = sancoes_df.select(pl.col("cnpj_basico").alias("_cnpj_basico_sancionado")).unique()
    else:
        # Fall back: derive basico from formatted CNPJ by stripping punctuation.
        sancionados_basico = sancoes_df.select(
            pl.col("cnpj").str.replace_all(r"[.\-/]", "").str.slice(0, 8).alias("_cnpj_basico_sancionado")
        ).unique()

    cnpjs_sancionados: set[str] = set(sancionados_basico["_cnpj_basico_sancionado"].drop_nulls().to_list())

    is_sancionado_series = (
        socios_df["cnpj_basico"]
        .map_elements(
            lambda v: v in cnpjs_sancionados if v is not None else False,
            return_dtype=pl.Boolean,
        )
        .alias("is_sancionado")
    )

    # ------------------------------------------------------------------
    # qtd_empresas_governo: count of distinct CNPJs per sócio by name
    # ------------------------------------------------------------------
    if "cnpj_basico" in empresas_df.columns:
        empresas_cnpj_basico = empresas_df["cnpj_basico"].drop_nulls().unique().to_list()
    else:
        empresas_cnpj_basico = []

    empresas_set: set[str] = set(empresas_cnpj_basico)

    # Filter socios to those that belong to known government-supplier companies.
    socios_in_gov = socios_df.filter(socios_df["cnpj_basico"].is_in(list(empresas_set)))

    # Count how many distinct cnpj_basico each nome_socio appears in.
    nome_count = socios_in_gov.group_by("nome_socio").agg(
        pl.col("cnpj_basico").n_unique().alias("_qtd_empresas_governo")
    )

    enriched = (
        socios_df.with_columns(is_sancionado_series)
        .join(nome_count, on="nome_socio", how="left")
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

    with_key = (
        empresas_df.select(
            [
                pl.col("cnpj"),
                pl.col("logradouro"),
            ]
        )
        .with_columns(
            pl.col("logradouro")
            .map_elements(
                _normalise_address_key,
                return_dtype=pl.Utf8,
            )
            .alias("_address_key"),
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


def _normalise_address_key(logradouro: str | None) -> str | None:
    """Build a normalised address key from a logradouro string.

    Extracts (street_name_normalised|street_number).  Returns None when the
    logradouro is blank or has no recognisable number.

    Args:
        logradouro: Full address line, possibly including building number.

    Returns:
        Normalised key string, e.g. 'RUA DAS FLORES|123', or None.
    """
    if not logradouro:
        return None

    upper = logradouro.strip().upper()

    # Extract the number first (first digit sequence in the string).
    num_match = _NUMERO_PATTERN.search(upper)
    if not num_match:
        return None
    numero = num_match.group(1)

    # Street name: everything before the number match, stripped.
    street_part = upper[: num_match.start()].strip().rstrip(",").strip()
    if not street_part:
        return None

    return f"{street_part}|{numero}"
