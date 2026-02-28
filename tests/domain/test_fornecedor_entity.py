# tests/domain/test_fornecedor_entity.py
import uuid
from datetime import datetime
from decimal import Decimal

import pytest

from api.domain.fornecedor.entities import AlertaCritico, Fornecedor
from api.domain.fornecedor.enums import (
    Severidade,
    SituacaoCadastral,
    TipoAlerta,
)
from api.domain.fornecedor.value_objects import CNPJ, RazaoSocial


def _cnpj() -> CNPJ:
    return CNPJ("11222333000181")


def test_fornecedor_criado_com_dados_minimos():
    f = Fornecedor(
        cnpj=_cnpj(),
        razao_social=RazaoSocial("Empresa Teste LTDA"),
        situacao=SituacaoCadastral.ATIVA,
    )
    assert f.cnpj.formatado == "11.222.333/0001-81"
    assert f.alertas_criticos == ()
    assert f.score_risco is None
    assert f.total_contratos == 0
    assert f.valor_total_contratos == Decimal("0")


def test_fornecedor_sem_calculo_tem_score_nulo():
    f = Fornecedor(
        cnpj=_cnpj(),
        razao_social=RazaoSocial("Empresa"),
        situacao=SituacaoCadastral.ATIVA,
    )
    assert f.score_risco is None


def test_alerta_critico_valido():
    alerta = AlertaCritico(
        id=uuid.uuid4(),
        tipo=TipoAlerta.SOCIO_SERVIDOR_PUBLICO,
        severidade=Severidade.GRAVISSIMO,
        descricao="Socio Joao e servidor do Min. Saude",
        evidencia="socio_cpf_hmac=abc123, orgao=Min. Saude",
        fornecedor_cnpj=_cnpj(),
        detectado_em=datetime(2026, 2, 27, 10, 0, 0),
    )
    assert alerta.tipo == TipoAlerta.SOCIO_SERVIDOR_PUBLICO
    assert alerta.severidade == Severidade.GRAVISSIMO


def test_alerta_critico_rejeita_evidencia_vazia():
    with pytest.raises(ValueError, match="evidencia"):
        AlertaCritico(
            id=uuid.uuid4(),
            tipo=TipoAlerta.SOCIO_SERVIDOR_PUBLICO,
            severidade=Severidade.GRAVISSIMO,
            descricao="Socio e servidor",
            evidencia="",
            fornecedor_cnpj=_cnpj(),
            detectado_em=datetime.now(),
        )


def test_alerta_critico_rejeita_evidencia_so_espacos():
    with pytest.raises(ValueError, match="evidencia"):
        AlertaCritico(
            id=uuid.uuid4(),
            tipo=TipoAlerta.SOCIO_SERVIDOR_PUBLICO,
            severidade=Severidade.GRAVISSIMO,
            descricao="Socio e servidor",
            evidencia="   ",
            fornecedor_cnpj=_cnpj(),
            detectado_em=datetime.now(),
        )


def test_fornecedor_imutavel():
    f = Fornecedor(
        cnpj=_cnpj(),
        razao_social=RazaoSocial("Empresa"),
        situacao=SituacaoCadastral.ATIVA,
    )
    with pytest.raises(AttributeError):
        f.total_contratos = 5  # type: ignore[misc]
