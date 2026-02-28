# tests/pipeline/test_cnae_mapping.py
#
# Specification tests for the CNAE category mapping and incompatibility detection.
from __future__ import annotations

from pipeline.transform.cnae_mapping import (
    cnae_incompativel_com_objeto,
    get_cnae_category,
)


def test_lookup_cnae_conhecido() -> None:
    """A known CNAE code returns the correct broad category."""
    # 6201-5 is "Desenvolvimento de programas de computador sob encomenda"
    result = get_cnae_category("6201-5")

    assert result == "TECNOLOGIA"


def test_lookup_cnae_desconhecido() -> None:
    """An unknown CNAE code returns None (no false positives for unmapped codes)."""
    result = get_cnae_category("9999-9")

    assert result is None


def test_incompatibilidade_detectada() -> None:
    """A TECNOLOGIA CNAE with a CONSTRUCAO contract object is incompatible."""
    # A software company (6201-5) cannot legitimately supply construction services.
    result = cnae_incompativel_com_objeto("6201-5", "CONSTRUCAO")

    assert result is True


def test_compatibilidade_ok() -> None:
    """A TECNOLOGIA CNAE with a TECNOLOGIA contract object is compatible."""
    result = cnae_incompativel_com_objeto("6201-5", "TECNOLOGIA")

    assert result is False
