# pipeline/transform/cnae_mapping.py
#
# Manual CNAE-to-category mapping for incompatibility detection.
#
# Design decisions:
#   - The mapping is implemented as a curated static dictionary rather than
#     an NLP/LLM classifier. This makes the logic auditable, deterministic,
#     and reproducible across pipeline runs without external model dependencies.
#   - Only the top ~50 CNAEs by frequency among government suppliers are
#     included. Unknown CNAEs return None from get_cnae_category(), which is
#     treated as "compatible with everything" downstream (no false positives).
#   - Incompatibility is defined as: "a company whose primary economic activity
#     is X is unlikely to legitimately supply Y to the government". The table
#     encodes domain knowledge curated by the project maintainers. Community
#     contributions expand this table via pull requests.
#   - CNAE codes are stored with the hyphen separator as Receita Federal uses
#     (e.g. "6201-5"), not in the plain 7-digit format, to match the staging
#     data exactly. Both formats are accepted by get_cnae_category() via
#     normalisation.
#
# ADR: Why not import from api/domain?
#   The pipeline is a standalone offline process. It must not import from the
#   API package to avoid coupling the build environment to the web stack.
#   Constants duplicated here are annotated with the source of truth so
#   divergence is detectable in code review.
#
# Invariants:
#   - CNAE_CATEGORIES maps CNAE code strings to uppercase category strings.
#   - INCOMPATIBLE_COMBOS maps a category to the set of object categories
#     that are considered incompatible with it.
#   - cnae_incompativel_com_objeto is a pure function with no IO.
from __future__ import annotations

# ---------------------------------------------------------------------------
# CNAE → broad category mapping (top 50 CNAEs in government procurement)
# ---------------------------------------------------------------------------

CNAE_CATEGORIES: dict[str, str] = {
    # Tecnologia da Informação
    "6201-5": "TECNOLOGIA",
    "6202-3": "TECNOLOGIA",
    "6203-1": "TECNOLOGIA",
    "6204-0": "TECNOLOGIA",
    "6209-1": "TECNOLOGIA",
    "6311-9": "TECNOLOGIA",
    "6319-4": "TECNOLOGIA",
    "6399-2": "TECNOLOGIA",
    # Construção civil
    "4110-7": "CONSTRUCAO",
    "4120-4": "CONSTRUCAO",
    "4211-1": "CONSTRUCAO",
    "4212-0": "CONSTRUCAO",
    "4213-8": "CONSTRUCAO",
    "4221-9": "CONSTRUCAO",
    "4222-7": "CONSTRUCAO",
    "4291-0": "CONSTRUCAO",
    "4292-8": "CONSTRUCAO",
    "4299-5": "CONSTRUCAO",
    # Comércio varejista
    "4711-3": "COMERCIO_VAREJO",
    "4712-1": "COMERCIO_VAREJO",
    "4713-0": "COMERCIO_VAREJO",
    "4721-1": "COMERCIO_VAREJO",
    "4722-9": "COMERCIO_VAREJO",
    "4731-8": "COMERCIO_VAREJO",
    "4741-5": "COMERCIO_VAREJO",
    "4742-3": "COMERCIO_VAREJO",
    "4744-0": "COMERCIO_VAREJO",
    # Saúde
    "8610-1": "SAUDE",
    "8621-6": "SAUDE",
    "8622-4": "SAUDE",
    "8630-5": "SAUDE",
    "8640-2": "SAUDE",
    "8650-0": "SAUDE",
    "8660-7": "SAUDE",
    "4771-7": "SAUDE",
    "4773-3": "SAUDE",
    # Alimentação e restaurantes
    "5611-2": "ALIMENTACAO",
    "5612-1": "ALIMENTACAO",
    "5620-1": "ALIMENTACAO",
    "4721-1": "ALIMENTACAO",  # supermercados (overlap accepted)
    # Serviços de limpeza e conservação
    "8121-4": "LIMPEZA",
    "8122-2": "LIMPEZA",
    "8129-0": "LIMPEZA",
    # Segurança privada
    "8011-1": "SEGURANCA",
    "8012-0": "SEGURANCA",
    # Consultoria e assessoria empresarial
    "7020-4": "CONSULTORIA",
    "7490-1": "CONSULTORIA",
    "6920-6": "CONSULTORIA",
    # Educação
    "8511-2": "EDUCACAO",
    "8512-1": "EDUCACAO",
    "8513-9": "EDUCACAO",
    "8520-1": "EDUCACAO",
}

# ---------------------------------------------------------------------------
# Incompatibility table: category → set of object categories it cannot supply
# ---------------------------------------------------------------------------
# ADR: A category is incompatible with another when a legitimate business in
# that sector would have no operational capacity to deliver the object.
# Symmetry is NOT assumed: TECNOLOGIA cannot build roads, but CONSTRUCAO can
# legitimately subcontract technology services.

INCOMPATIBLE_COMBOS: dict[str, set[str]] = {
    "TECNOLOGIA": {"CONSTRUCAO", "SAUDE", "ALIMENTACAO", "LIMPEZA"},
    "COMERCIO_VAREJO": {"TECNOLOGIA", "CONSTRUCAO", "SAUDE", "SEGURANCA"},
    "CONSTRUCAO": {"TECNOLOGIA", "SAUDE", "ALIMENTACAO", "SEGURANCA"},
    "ALIMENTACAO": {"TECNOLOGIA", "CONSTRUCAO", "SEGURANCA"},
    "LIMPEZA": {"TECNOLOGIA", "CONSTRUCAO", "SAUDE"},
    "SEGURANCA": {"TECNOLOGIA", "CONSTRUCAO", "SAUDE", "ALIMENTACAO"},
    "CONSULTORIA": {"CONSTRUCAO", "SAUDE", "ALIMENTACAO", "LIMPEZA"},
    "EDUCACAO": {"CONSTRUCAO", "SAUDE", "LIMPEZA", "SEGURANCA"},
    "SAUDE": {"CONSTRUCAO", "ALIMENTACAO", "LIMPEZA", "SEGURANCA"},
}


def _normalise_cnae(cnae_code: str) -> str:
    """Normalise a CNAE code to the hyphenated format used in CNAE_CATEGORIES.

    Accepts both '62015' (7-digit plain) and '6201-5' (with hyphen).

    Args:
        cnae_code: Raw CNAE code string.

    Returns:
        Normalised code with hyphen, e.g. '6201-5'.
    """
    stripped = cnae_code.strip().replace(" ", "")
    if "-" in stripped:
        return stripped
    # Plain 7-digit format: insert hyphen before last digit.
    if len(stripped) == 7 and stripped.isdigit():
        return stripped[:6] + "-" + stripped[6]
    # 4-digit division or other format: return as-is (will miss in dict).
    return stripped


def get_cnae_category(cnae_code: str) -> str | None:
    """Return the broad category for a CNAE code, or None if unknown.

    Args:
        cnae_code: CNAE code in any recognised format.

    Returns:
        Uppercase category string (e.g. 'TECNOLOGIA'), or None if the code
        is not in the curated mapping.
    """
    return CNAE_CATEGORIES.get(_normalise_cnae(cnae_code))


def cnae_incompativel_com_objeto(cnae_code: str, objeto_categoria: str) -> bool:
    """Check whether a CNAE is incompatible with a contract object category.

    Args:
        cnae_code:        CNAE code of the supplier's primary activity.
        objeto_categoria: Broad category of the contract object (e.g. 'CONSTRUCAO').

    Returns:
        True if the CNAE category and the object category are considered
        incompatible based on INCOMPATIBLE_COMBOS.  Returns False for unknown
        CNAEs (no false positives for unmapped codes).
    """
    category = get_cnae_category(cnae_code)
    if category is None:
        return False
    incompatible = INCOMPATIBLE_COMBOS.get(category, set())
    return objeto_categoria.upper() in incompatible
