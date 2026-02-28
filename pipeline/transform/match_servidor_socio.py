# pipeline/transform/match_servidor_socio.py
#
# Match QSA sócios against federal servants by name + visible CPF digits.
#
# Design decisions:
#   - The Portal da Transparência publishes CPFs in partially masked format
#     ('***.222.333-**'). The Receita Federal QSA data uses a similar scheme
#     ('***222333**'). Both expose the same 6 middle digits (positions 3-8
#     of the 11-digit CPF). We extract these 6 digits from the QSA cpf_parcial
#     and join on (nome_normalised, 6_digits) against the servidores table.
#   - Name matching is case-insensitive (both sides are already uppercased by
#     their respective parsers). We apply an additional strip_chars() here as a
#     defensive measure against whitespace introduced during joins.
#   - The probability of a homonym with the same 6 middle CPF digits is
#     negligible. This justifies skipping full CPF comparison, which would
#     require storing CPF in clear text — a LGPD violation.
#   - A left join is used so all sócios are preserved: unmatched rows get
#     is_servidor_publico=False and orgao_lotacao=null.
#   - The cpf_hmac column (if present) is carried through unchanged.
#
# Invariants:
#   - socios_df must have columns: nome_socio, cpf_parcial.
#   - servidores_df must have columns: nome, digitos_visiveis, orgao_lotacao.
#   - Output has all columns from socios_df plus is_servidor_publico (bool)
#     and orgao_lotacao (str|null).
from __future__ import annotations

import re

import polars as pl

# Pattern for QSA masked CPF format from Receita: '***222333**'
# Captures the 6 visible middle digits (no separators in the QSA format).
_QSA_CPF_MASK_PATTERN = re.compile(r"\*{3}(\d{3})(\d{3})\*{2}")

# Pattern for Portal da Transparência format: '***.222.333-**'
# Captures the 6 visible middle digits (with dots/dashes as separators).
_PORTAL_CPF_MASK_PATTERN = re.compile(r"\*{3}\.(\d{3})\.(\d{3})-\*{2}")


def _extrair_digitos_qsa(cpf_parcial: str | None) -> str | None:
    """Extract 6 visible digits from a QSA-format masked CPF.

    Handles two possible formats from the Receita:
      - '***222333**'  (no separators)
      - '***.222.333-**' (with separators, same as Portal da Transparência)

    Args:
        cpf_parcial: Raw masked CPF string from QSA parse.

    Returns:
        6-character digit string, or None if format is unrecognised.
    """
    if not cpf_parcial:
        return None
    stripped = cpf_parcial.strip()

    # Try format without separators first (most common in Receita exports).
    match = _QSA_CPF_MASK_PATTERN.match(stripped)
    if match:
        return match.group(1) + match.group(2)

    # Fall back to format with separators.
    match = _PORTAL_CPF_MASK_PATTERN.match(stripped)
    if match:
        return match.group(1) + match.group(2)

    return None


def match_servidor_socio(
    socios_df: pl.DataFrame,
    servidores_df: pl.DataFrame,
) -> pl.DataFrame:
    """Enrich sócios DataFrame with federal-servant flags.

    Performs a left join of sócios against servidores on:
      - Normalised name: socios.nome_socio == servidores.nome
      - Visible CPF digits: extracted from socios.cpf_parcial == servidores.digitos_visiveis

    Both conditions must match simultaneously.  Rows without a match receive
    is_servidor_publico=False and orgao_lotacao=null.

    Args:
        socios_df:     DataFrame with at least: nome_socio (str), cpf_parcial (str|null).
                       May also contain cpf_hmac (str) and other columns, all
                       of which are preserved in the output.
        servidores_df: DataFrame with at least: nome (str), digitos_visiveis (str|null),
                       orgao_lotacao (str|null).

    Returns:
        socios_df enriched with:
          - is_servidor_publico (bool)   — True for matched rows.
          - orgao_lotacao (str|null)     — filled from servidores for matches.
    """
    # Extract 6 visible digits from the QSA masked CPF column.
    digitos_socio = (
        socios_df["cpf_parcial"]
        .map_elements(
            _extrair_digitos_qsa,
            return_dtype=pl.Utf8,
        )
        .alias("_digitos_socio")
    )

    socios_with_digits = socios_df.with_columns(digitos_socio)

    # Prepare the servidores side with unambiguous join-key column names.
    servidores_keys = servidores_df.select(
        [
            pl.col("nome").alias("_nome_servidor"),
            pl.col("digitos_visiveis").alias("_digitos_servidor"),
            pl.col("orgao_lotacao").alias("_orgao_servidor"),
        ]
    )

    # Add the socio name as a normalised join key (already uppercase from parser,
    # but strip again defensively).
    socios_keyed = socios_with_digits.with_columns(
        pl.col("nome_socio").str.strip_chars().alias("_nome_socio_norm"),
    )

    # Left join on both conditions. Polars join requires a single column on each
    # side, so we construct a composite key by concatenation.
    # ADR: Composite key via string concat is safe here because both parts are
    # fixed-format (uppercase name + exactly 6 digits). A separator character
    # that cannot appear in names/digits ("|") eliminates false collisions.
    _separator = "|"

    socios_keyed = socios_keyed.with_columns(
        (pl.col("_nome_socio_norm") + _separator + pl.col("_digitos_socio").fill_null("")).alias("_match_key"),
    )

    servidores_keys = servidores_keys.with_columns(
        (pl.col("_nome_servidor") + _separator + pl.col("_digitos_servidor").fill_null("")).alias("_match_key"),
    )

    # Drop rows from servidores where either name or digits is null — they cannot
    # form a valid match anyway (the filled "" sentinel creates false matches
    # only when both sides are null, which we exclude here).
    servidores_valid = servidores_keys.filter(
        pl.col("_digitos_servidor").is_not_null() & pl.col("_nome_servidor").is_not_null()
    )

    joined = socios_keyed.join(
        servidores_valid.select(["_match_key", "_orgao_servidor"]),
        on="_match_key",
        how="left",
    )

    # Derive is_servidor_publico from whether orgao_servidor was filled.
    result = joined.with_columns(
        pl.col("_orgao_servidor").is_not_null().alias("is_servidor_publico"),
        pl.col("_orgao_servidor").alias("orgao_lotacao"),
    )

    # Drop all temporary join-key columns.
    cols_to_drop = [
        "_digitos_socio",
        "_nome_socio_norm",
        "_match_key",
        "_orgao_servidor",
    ]
    existing_drops = [c for c in cols_to_drop if c in result.columns]

    return result.drop(existing_drops)
