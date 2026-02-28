# pipeline/transform/hmac_cpf.py
#
# CPF anonymisation via HMAC-SHA256.
#
# Design decisions:
#   - HMAC-SHA256 is used instead of plain SHA-256 because the CPF space is
#     small (~200 million possibilities) and a keyed construction is required
#     to prevent rainbow-table attacks. Without the salt, an adversary could
#     reverse the entire column in O(200M) hash computations.
#   - The salt lives exclusively as an environment variable (CPF_HMAC_SALT)
#     injected at pipeline run-time. It is never co-located with the data.
#   - Output is always 64 lowercase hex characters (256-bit HMAC → hex).
#     This fits in a VARCHAR(64) column with no truncation risk.
#   - map_elements is used instead of native Polars expressions because Python's
#     hmac module has no Polars binding. Acceptable: pipeline runs offline and
#     throughput is dominated by source download I/O, not this CPU step.
#
# Invariants:
#   - hmac_sha256_cpf is pure: same (cpf, salt) always → same output.
#   - apply_hmac_to_df never mutates the input DataFrame.
#   - The original CPF column is absent from the returned DataFrame.
from __future__ import annotations

import hashlib
import hmac as _hmac_stdlib

import polars as pl


def hmac_sha256_cpf(cpf: str, salt: str) -> str:
    """Compute HMAC-SHA256 of a CPF string using the given salt.

    Args:
        cpf:  Raw CPF string (any format — treated as UTF-8 bytes).
        salt: Secret key. Must remain confidential; loss of salt is acceptable,
              but a compromised salt makes all CPF-HMACs reversible.

    Returns:
        64-character lowercase hex string (256-bit HMAC output).
    """
    return _hmac_stdlib.new(
        salt.encode("utf-8"),
        cpf.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def apply_hmac_to_df(df: pl.DataFrame, cpf_col: str, salt: str) -> pl.DataFrame:
    """Replace a CPF column with its HMAC-SHA256 values in a new DataFrame.

    The column named *cpf_col* is dropped and replaced by a new column named
    ``cpf_hmac``.  Null values in the source column produce null values in
    ``cpf_hmac`` — they are not hashed.

    Args:
        df:       Input Polars DataFrame that contains *cpf_col*.
        cpf_col:  Name of the column holding raw CPF strings.
        salt:     HMAC secret key (CPF_HMAC_SALT from environment).

    Returns:
        New DataFrame identical to *df* except the CPF column is replaced by
        ``cpf_hmac``.
    """
    hmac_series = (
        df[cpf_col]
        .map_elements(
            lambda v: hmac_sha256_cpf(v, salt) if v is not None else None,
            return_dtype=pl.Utf8,
        )
        .alias("cpf_hmac")
    )

    return df.drop(cpf_col).with_columns(hmac_series)
