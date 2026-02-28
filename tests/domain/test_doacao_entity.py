# tests/domain/test_doacao_entity.py
from decimal import Decimal

import pytest

from api.domain.doacao.entities import DoacaoEleitoral
from api.domain.doacao.value_objects import ValorDoacao
from api.domain.fornecedor.value_objects import CNPJ


def _doacao(valor: Decimal = Decimal("15000")) -> DoacaoEleitoral:
    return DoacaoEleitoral(
        fornecedor_cnpj=CNPJ("11222333000181"),
        socio_cpf_hmac=None,
        candidato_nome="Dep. Fulano",
        candidato_partido="PXX",
        candidato_cargo="Deputado Federal",
        valor=ValorDoacao(valor),
        ano_eleicao=2022,
    )


def test_doacao_acima_threshold_materialidade():
    """Doacao > R$10k e material."""
    doacao = _doacao(Decimal("15000"))
    assert doacao.material() is True


def test_doacao_abaixo_threshold_nao_e_material():
    """Doacao < R$10k nao e material."""
    doacao = _doacao(Decimal("5000"))
    assert doacao.material() is False


def test_doacao_igual_threshold_nao_e_material():
    """Doacao == R$10k nao e material (precisa ser estritamente maior)."""
    doacao = _doacao(Decimal("10000"))
    assert doacao.material() is False


def test_doacao_sem_valor_zero_nao_e_material():
    """Doacao com valor zero nao e material."""
    doacao = _doacao(Decimal("0"))
    assert doacao.material() is False


def test_doacao_com_threshold_customizado():
    """Threshold customizado de materialidade."""
    doacao = _doacao(Decimal("600000"))
    assert doacao.material(threshold=Decimal("500000")) is True
    assert doacao.material(threshold=Decimal("1000000")) is False


def test_valor_doacao_negativo_invalido():
    """Valor de doacao nunca pode ser negativo."""
    with pytest.raises(ValueError, match="negativo"):
        ValorDoacao(Decimal("-1"))


def test_doacao_via_socio():
    """Doacao pode ser vinculada via socio (cpf_hmac) sem CNPJ."""
    doacao = DoacaoEleitoral(
        fornecedor_cnpj=None,
        socio_cpf_hmac="hmac_abc123",
        candidato_nome="Dep. Fulano",
        candidato_partido="PXX",
        candidato_cargo="Deputado Federal",
        valor=ValorDoacao(Decimal("20000")),
        ano_eleicao=2022,
    )
    assert doacao.fornecedor_cnpj is None
    assert doacao.socio_cpf_hmac == "hmac_abc123"
    assert doacao.material() is True
