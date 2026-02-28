# tests/domain/test_sancao_entity.py
from datetime import date

from api.domain.sancao.entities import Sancao
from api.domain.sancao.value_objects import TipoSancao


def test_sancao_vigente_quando_data_fim_none():
    """data_fim NULL = sancao ativa indefinidamente."""
    s = Sancao(
        tipo=TipoSancao.CEIS,
        orgao_sancionador="CGU",
        data_inicio=date(2023, 1, 1),
        data_fim=None,
    )
    assert s.vigente(referencia=date(2026, 2, 27)) is True


def test_sancao_vigente_quando_data_fim_futura():
    s = Sancao(
        tipo=TipoSancao.CEIS,
        orgao_sancionador="CGU",
        data_inicio=date(2023, 1, 1),
        data_fim=date(2027, 12, 31),
    )
    assert s.vigente(referencia=date(2026, 2, 27)) is True


def test_sancao_vigente_quando_data_fim_igual_referencia():
    """Ultimo dia de vigencia conta como vigente."""
    s = Sancao(
        tipo=TipoSancao.CNEP,
        orgao_sancionador="TCU",
        data_inicio=date(2023, 1, 1),
        data_fim=date(2026, 2, 27),
    )
    assert s.vigente(referencia=date(2026, 2, 27)) is True


def test_sancao_expirada():
    s = Sancao(
        tipo=TipoSancao.CEIS,
        orgao_sancionador="CGU",
        data_inicio=date(2020, 1, 1),
        data_fim=date(2022, 12, 31),
    )
    assert s.vigente(referencia=date(2026, 2, 27)) is False
