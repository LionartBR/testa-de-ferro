# api/application/services/cnae_mapping.py
#
# CNAE-to-category mapping for the CNAE_INCOMPATIVEL score indicator.
#
# ADR: Intentional duplication of pipeline/transform/cnae_mapping.py.
#   The API layer must not import from the pipeline package to maintain
#   clean architectural boundaries. The pipeline is an offline standalone
#   process with different dependencies. Constants are annotated with the
#   source of truth so divergence is detectable in code review.
#   Source of truth: pipeline/transform/cnae_mapping.py
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
    """Normalise a CNAE code to the hyphenated format used in CNAE_CATEGORIES."""
    stripped = cnae_code.strip().replace(" ", "")
    if "-" in stripped:
        return stripped
    if len(stripped) == 7 and stripped.isdigit():
        return stripped[:6] + "-" + stripped[6]
    return stripped


def get_cnae_category(cnae_code: str) -> str | None:
    """Return the broad category for a CNAE code, or None if unknown."""
    return CNAE_CATEGORIES.get(_normalise_cnae(cnae_code))


def cnae_incompativel_com_objeto(cnae_code: str, objeto_categoria: str) -> bool:
    """Check whether a CNAE is incompatible with a contract object category."""
    category = get_cnae_category(cnae_code)
    if category is None:
        return False
    incompatible = INCOMPATIBLE_COMBOS.get(category, set())
    return objeto_categoria.upper() in incompatible
