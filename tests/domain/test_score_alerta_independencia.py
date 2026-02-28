# tests/domain/test_score_alerta_independencia.py
"""Invariante arquitetural: alertas e score sao dimensoes 100% independentes.
Estes testes garantem que nunca se contaminam mutuamente."""
import inspect
from datetime import date

from api.application.services.alerta_service import detectar_alertas
from api.application.services.score_service import calcular_score_cumulativo
from api.domain.fornecedor.entities import Fornecedor
from api.domain.fornecedor.enums import SituacaoCadastral
from api.domain.fornecedor.value_objects import CNPJ, RazaoSocial
from api.domain.societario.entities import Socio


def _fornecedor() -> Fornecedor:
    return Fornecedor(
        cnpj=CNPJ("11222333000181"),
        razao_social=RazaoSocial("Empresa Teste"),
        situacao=SituacaoCadastral.ATIVA,
    )


def test_alerta_nao_contamina_score():
    """Socio servidor gera alerta GRAVISSIMO mas NAO deve inflar o score."""
    socio = Socio(cpf_hmac="abc", nome="Maria", is_servidor_publico=True)
    fornecedor = _fornecedor()
    alertas = detectar_alertas(fornecedor, socios=[socio], sancoes=[], contratos=[],
                               referencia=date(2026, 2, 27))
    score = calcular_score_cumulativo(fornecedor, socios=[socio], sancoes=[], contratos=[],
                                      referencia=date(2026, 2, 27))
    assert len(alertas) >= 1
    # Nenhum indicador de score deve ter nome referente a servidor
    assert not any(i.tipo.name == "SOCIO_SERVIDOR_PUBLICO" for i in score.indicadores)


def test_score_service_nunca_importa_alerta_service():
    """Garantia estrutural via inspecao de codigo-fonte."""
    import api.application.services.score_service as mod
    source = inspect.getsource(mod)
    assert "alerta_service" not in source
    assert "detectar_alertas" not in source


def test_alerta_service_nunca_importa_score_service():
    import api.application.services.alerta_service as mod
    source = inspect.getsource(mod)
    assert "score_service" not in source
    assert "calcular_score" not in source
