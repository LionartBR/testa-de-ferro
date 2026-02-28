# tests/pipeline/test_hmac_cpf.py
#
# Specification tests for the CPF HMAC-SHA256 anonymisation transform.
from __future__ import annotations

import polars as pl

from pipeline.transform.hmac_cpf import apply_hmac_to_df, hmac_sha256_cpf


def test_hmac_sha256_deterministic() -> None:
    """Same CPF + same salt always produces the same hash."""
    result_a = hmac_sha256_cpf("12345678901", "my-secret-salt")
    result_b = hmac_sha256_cpf("12345678901", "my-secret-salt")

    assert result_a == result_b


def test_hmac_sha256_different_salt_different_hash() -> None:
    """Same CPF with a different salt produces a different hash."""
    hash_salt_a = hmac_sha256_cpf("12345678901", "salt-a")
    hash_salt_b = hmac_sha256_cpf("12345678901", "salt-b")

    assert hash_salt_a != hash_salt_b


def test_hmac_sha256_returns_64_hex_chars() -> None:
    """The output is always exactly 64 lowercase hexadecimal characters."""
    result = hmac_sha256_cpf("98765432100", "any-salt")

    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_apply_hmac_removes_original_column() -> None:
    """apply_hmac_to_df drops the raw CPF column and adds 'cpf_hmac'."""
    df = pl.DataFrame({
        "nome": ["JOAO SILVA", "MARIA SOUZA"],
        "cpf": ["12345678901", "98765432100"],
        "outro_campo": [1, 2],
    })

    result = apply_hmac_to_df(df, cpf_col="cpf", salt="test-salt")

    assert "cpf" not in result.columns
    assert "cpf_hmac" in result.columns
    # Other columns must be preserved unchanged.
    assert "nome" in result.columns
    assert "outro_campo" in result.columns
    assert len(result) == 2
    # Each HMAC must be 64 hex chars.
    for hmac_val in result["cpf_hmac"].drop_nulls().to_list():
        assert len(hmac_val) == 64
