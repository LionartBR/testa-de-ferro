# tests/domain/test_alertas.py
from datetime import date
from decimal import Decimal

from api.application.services.alerta_service import detectar_alertas
from api.domain.contrato.entities import Contrato
from api.domain.contrato.value_objects import ValorContrato
from api.domain.fornecedor.entities import Fornecedor
from api.domain.fornecedor.enums import (
    Severidade,
    SituacaoCadastral,
    TipoAlerta,
)
from api.domain.fornecedor.value_objects import CNPJ, RazaoSocial
from api.domain.sancao.entities import Sancao
from api.domain.sancao.value_objects import TipoSancao
from api.domain.societario.entities import Socio


def _fornecedor(**kwargs: object) -> Fornecedor:
    defaults: dict[str, object] = {
        "cnpj": CNPJ("11222333000181"),
        "razao_social": RazaoSocial("Empresa Teste LTDA"),
        "situacao": SituacaoCadastral.ATIVA,
    }
    defaults.update(kwargs)
    return Fornecedor(**defaults)  # type: ignore[arg-type]


def _contrato(valor: Decimal = Decimal("500000")) -> Contrato:
    return Contrato(
        fornecedor_cnpj=CNPJ("11222333000181"),
        orgao_codigo="00001",
        valor=ValorContrato(valor),
        data_assinatura=date(2025, 6, 1),
    )


# ---------- SOCIO_SERVIDOR_PUBLICO ----------

def test_alerta_socio_servidor_publico_gera_gravissimo():
    socio = Socio(cpf_hmac="abc123", nome="Joao da Silva", is_servidor_publico=True,
                  orgao_lotacao="Ministerio da Saude")
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[socio],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    servidores = [a for a in alertas if a.tipo == TipoAlerta.SOCIO_SERVIDOR_PUBLICO]
    assert len(servidores) >= 1
    assert servidores[0].severidade == Severidade.GRAVISSIMO
    assert "Joao da Silva" in servidores[0].evidencia


def test_socio_nao_servidor_nao_gera_alerta():
    socio = Socio(cpf_hmac="abc123", nome="Joao", is_servidor_publico=False)
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[socio],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert not any(a.tipo == TipoAlerta.SOCIO_SERVIDOR_PUBLICO for a in alertas)


def test_multiplos_socios_servidores_geram_alerta_para_cada():
    s1 = Socio(cpf_hmac="a", nome="Joao", is_servidor_publico=True)
    s2 = Socio(cpf_hmac="b", nome="Maria", is_servidor_publico=True)
    s3 = Socio(cpf_hmac="c", nome="Pedro", is_servidor_publico=False)
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[s1, s2, s3],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    servidores = [a for a in alertas if a.tipo == TipoAlerta.SOCIO_SERVIDOR_PUBLICO]
    assert len(servidores) == 2


# ---------- EMPRESA_SANCIONADA_CONTRATANDO ----------

def test_empresa_sancionada_vigente_com_contrato_gera_gravissimo():
    sancao = Sancao(tipo=TipoSancao.CEIS, orgao_sancionador="CGU",
                    data_inicio=date(2023, 1, 1), data_fim=None)
    contrato = _contrato()
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[sancao],
        contratos=[contrato],
        referencia=date(2026, 2, 27),
    )
    sancionados = [a for a in alertas if a.tipo == TipoAlerta.EMPRESA_SANCIONADA_CONTRATANDO]
    assert len(sancionados) >= 1
    assert sancionados[0].severidade == Severidade.GRAVISSIMO


def test_sancao_expirada_nao_gera_alerta_critico():
    """Sancao expirada -> indicador SANCAO_HISTORICA (score), nunca alerta."""
    sancao = Sancao(tipo=TipoSancao.CEIS, orgao_sancionador="CGU",
                    data_inicio=date(2020, 1, 1), data_fim=date(2022, 12, 31))
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[sancao],
        contratos=[_contrato()],
        referencia=date(2026, 2, 27),
    )
    assert not any(a.tipo == TipoAlerta.EMPRESA_SANCIONADA_CONTRATANDO for a in alertas)


def test_empresa_sancionada_sem_contrato_nao_gera_alerta():
    """Sem contrato = nao esta 'contratando', logo sem alerta."""
    sancao = Sancao(tipo=TipoSancao.CEIS, orgao_sancionador="CGU",
                    data_inicio=date(2023, 1, 1), data_fim=None)
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[sancao],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert not any(a.tipo == TipoAlerta.EMPRESA_SANCIONADA_CONTRATANDO for a in alertas)


def test_sem_socios_e_sem_sancoes_retorna_lista_vazia():
    alertas = detectar_alertas(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert alertas == []
