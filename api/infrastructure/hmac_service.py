# api/infrastructure/hmac_service.py
#
# HMAC-SHA256 service for CPF anonymisation in the API layer.
#
# Design decisions:
#   - Mirrors pipeline/transform/hmac_cpf.py exactly: same algorithm, same
#     encoding (UTF-8 key, UTF-8 message, SHA-256 digest, lowercase hex output).
#   - Salt is read from Settings via get_settings() — never hard-coded.
#   - The function is intentionally thin: no caching, no batching. The API
#     uses this to convert a caller-supplied CPF into a cpf_hmac before
#     querying dim_socio. This happens at most once per request.
#   - Output is always 64 lowercase hex characters — matches the VARCHAR(64)
#     column produced by the pipeline.
#
# Invariants:
#   - Same (cpf, salt) always produces the same output (HMAC is deterministic).
#   - An empty salt ("") still produces a valid HMAC; it is just insecure.
#     The application startup should validate that CPF_HMAC_SALT is non-empty.
from __future__ import annotations

import hashlib
import hmac as _hmac_stdlib

from api.infrastructure.config import get_settings


def hmac_sha256_cpf(cpf: str) -> str:
    """Compute HMAC-SHA256 of a CPF string using the application salt.

    Reads the secret salt from Settings (CPF_HMAC_SALT environment variable).
    Produces the same 64-character lowercase hex digest as the pipeline's
    pipeline/transform/hmac_cpf.py when called with the same salt.

    Args:
        cpf: Raw CPF string in any format — treated as UTF-8 bytes. The caller
             is responsible for normalising the CPF before passing it here
             (e.g. strip punctuation) if the pipeline stored it normalised.

    Returns:
        64-character lowercase hex string (256-bit HMAC output).
    """
    salt = get_settings().cpf_hmac_salt
    return _hmac_stdlib.new(
        salt.encode("utf-8"),
        cpf.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
