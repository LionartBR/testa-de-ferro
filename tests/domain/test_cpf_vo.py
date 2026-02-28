# tests/domain/test_cpf_vo.py
import pytest

from api.domain.societario.value_objects import CPF, CPFMascarado


def test_cpf_valido_formatado():
    cpf = CPF("111.444.777-35")
    assert cpf.valor == "11144477735"


def test_cpf_valido_sem_formatacao():
    cpf = CPF("11144477735")
    assert cpf.valor == "11144477735"


def test_cpf_digito_verificador_invalido():
    with pytest.raises(ValueError, match="CPF invalido"):
        CPF("111.444.777-00")


def test_cpf_todos_iguais_invalido():
    with pytest.raises(ValueError):
        CPF("111.111.111-11")
    with pytest.raises(ValueError):
        CPF("00000000000")


def test_cpf_comprimento_errado():
    with pytest.raises(ValueError):
        CPF("123")


def test_cpf_repr_nunca_mostra_completo():
    """CPF nunca aparece completo em logs/repr — exigencia LGPD."""
    cpf = CPF("11144477735")
    assert "11144477735" not in repr(cpf)
    assert "***" in repr(cpf)


def test_cpf_str_nunca_mostra_completo():
    cpf = CPF("11144477735")
    assert "11144477735" not in str(cpf)


def test_cpf_igualdade_por_valor():
    a = CPF("11144477735")
    b = CPF("111.444.777-35")
    assert a == b
    assert hash(a) == hash(b)


def test_cpf_mascarado_parse():
    """Parse do formato do Portal da Transparencia: ***.444.777-**"""
    mascarado = CPFMascarado("***.444.777-**")
    assert mascarado.digitos_visiveis == "444777"


def test_cpf_mascarado_match_com_cpf_completo():
    mascarado = CPFMascarado("***.444.777-**")
    cpf = CPF("11144477735")
    assert mascarado.bate_com(cpf)


def test_cpf_mascarado_nao_bate_com_cpf_diferente():
    mascarado = CPFMascarado("***.444.777-**")
    cpf_outro = CPF("52998224725")  # CPF valido diferente
    assert not mascarado.bate_com(cpf_outro)
