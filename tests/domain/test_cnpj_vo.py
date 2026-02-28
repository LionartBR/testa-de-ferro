# tests/domain/test_cnpj_vo.py
import dataclasses

import pytest

from api.domain.fornecedor.value_objects import CNPJ


def test_cnpj_valido_formatado():
    """Aceita CNPJ com pontuacao e armazena sem formatacao."""
    cnpj = CNPJ("11.222.333/0001-81")
    assert cnpj.valor == "11222333000181"


def test_cnpj_valido_sem_formatacao():
    """Aceita CNPJ sem pontuacao e gera formatacao."""
    cnpj = CNPJ("11222333000181")
    assert cnpj.formatado == "11.222.333/0001-81"


def test_cnpj_digitos_verificadores_invalidos():
    """Rejeita CNPJ com digitos verificadores errados."""
    with pytest.raises(ValueError, match="CNPJ invalido"):
        CNPJ("11.222.333/0001-99")


def test_cnpj_todos_iguais_invalido():
    """CNPJs com todos digitos iguais sao invalidos."""
    with pytest.raises(ValueError):
        CNPJ("00.000.000/0000-00")
    with pytest.raises(ValueError):
        CNPJ("11111111111111")


def test_cnpj_comprimento_errado():
    """Rejeita strings com menos ou mais de 14 digitos."""
    with pytest.raises(ValueError):
        CNPJ("123")
    with pytest.raises(ValueError):
        CNPJ("123456789012345")


def test_cnpj_imutavel():
    """frozen=True impede atribuicao."""
    cnpj = CNPJ("11222333000181")
    with pytest.raises(dataclasses.FrozenInstanceError):
        cnpj._valor = "outro"  # type: ignore[misc]


def test_cnpj_igualdade_por_valor():
    """Dois CNPJs com mesmo numero sao iguais, independente de formatacao."""
    a = CNPJ("11222333000181")
    b = CNPJ("11.222.333/0001-81")
    assert a == b
    assert hash(a) == hash(b)


def test_cnpj_desigualdade():
    a = CNPJ("11222333000181")
    b = CNPJ("33000167000101")  # outro CNPJ valido
    assert a != b


def test_cnpj_repr_mostra_formatado():
    cnpj = CNPJ("11222333000181")
    assert "11.222.333/0001-81" in repr(cnpj)
