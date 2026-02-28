# tests/domain/test_score.py
from datetime import date
from decimal import Decimal

from api.application.services.score_service import calcular_score_cumulativo
from api.domain.contrato.entities import Contrato
from api.domain.contrato.value_objects import ValorContrato
from api.domain.fornecedor.entities import Fornecedor
from api.domain.fornecedor.enums import FaixaRisco, SituacaoCadastral, TipoIndicador
from api.domain.fornecedor.value_objects import CNPJ, CapitalSocial, RazaoSocial
from api.domain.sancao.entities import Sancao
from api.domain.sancao.value_objects import TipoSancao
from api.domain.societario.entities import Socio


def _fornecedor(
    capital: Decimal | None = None,
    data_abertura: date | None = None,
    cnae: str | None = None,
) -> Fornecedor:
    return Fornecedor(
        cnpj=CNPJ("11222333000181"),
        razao_social=RazaoSocial("Empresa Teste LTDA"),
        situacao=SituacaoCadastral.ATIVA,
        capital_social=CapitalSocial(capital) if capital is not None else None,
        data_abertura=data_abertura,
        cnae_principal=cnae,
    )


def _contrato(valor: Decimal = Decimal("500000"), orgao: str = "00001") -> Contrato:
    return Contrato(
        fornecedor_cnpj=CNPJ("11222333000181"),
        orgao_codigo=orgao,
        valor=ValorContrato(valor),
        data_assinatura=date(2025, 6, 1),
    )


# ---------- CAPITAL_SOCIAL_BAIXO (peso 15) ----------


def test_capital_social_baixo_com_contrato_alto_ativa():
    """Capital R$800, contratos > R$100k -> ativa indicador."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(capital=Decimal("800")),
        socios=[],
        sancoes=[],
        contratos=[_contrato(Decimal("150000"))],
        referencia=date(2026, 2, 27),
    )
    ind = [i for i in score.indicadores if i.tipo == TipoIndicador.CAPITAL_SOCIAL_BAIXO]
    assert len(ind) == 1
    assert ind[0].peso == 15


def test_capital_social_adequado_nao_ativa():
    """Capital R$1.000.000 -> nao ativa mesmo com contratos altos."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(capital=Decimal("1000000")),
        socios=[],
        sancoes=[],
        contratos=[_contrato(Decimal("5000000"))],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.CAPITAL_SOCIAL_BAIXO for i in score.indicadores)


def test_capital_social_baixo_sem_contrato_nao_ativa():
    """Sem contratos -> sem referencia de desproporcionalidade."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(capital=Decimal("100")),
        socios=[],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.CAPITAL_SOCIAL_BAIXO for i in score.indicadores)


# ---------- EMPRESA_RECENTE (peso 10) ----------


def test_empresa_recente_ativa_indicador():
    """Empresa aberta < 6 meses antes do primeiro contrato."""
    abertura = date(2025, 3, 1)  # ~4 meses antes do contrato em jun/2025
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(data_abertura=abertura),
        socios=[],
        sancoes=[],
        contratos=[_contrato()],
        referencia=date(2026, 2, 27),
    )
    assert any(i.tipo == TipoIndicador.EMPRESA_RECENTE for i in score.indicadores)


def test_empresa_antiga_nao_ativa_recente():
    """Empresa aberta > 6 meses antes do primeiro contrato -> sem indicador."""
    abertura = date(2020, 1, 1)  # ~5 anos antes
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(data_abertura=abertura),
        socios=[],
        sancoes=[],
        contratos=[_contrato()],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.EMPRESA_RECENTE for i in score.indicadores)


def test_empresa_sem_data_abertura_nao_ativa_recente():
    """Se data_abertura nao disponivel, nao ativar (fail-safe)."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(data_abertura=None),
        socios=[],
        sancoes=[],
        contratos=[_contrato()],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.EMPRESA_RECENTE for i in score.indicadores)


# ---------- SANCAO_HISTORICA (peso 5) ----------


def test_sancao_expirada_gera_indicador_peso_5():
    """Sancao expirada -> SANCAO_HISTORICA no score (nunca alerta)."""
    sancao = Sancao(
        tipo=TipoSancao.CEIS, orgao_sancionador="CGU", data_inicio=date(2020, 1, 1), data_fim=date(2022, 12, 31)
    )
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[sancao],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    hist = [i for i in score.indicadores if i.tipo == TipoIndicador.SANCAO_HISTORICA]
    assert len(hist) == 1
    assert hist[0].peso == 5


def test_sancao_vigente_nao_gera_indicador_historica():
    """Sancao vigente gera alerta (Step 7), nao indicador de score."""
    sancao = Sancao(tipo=TipoSancao.CEIS, orgao_sancionador="CGU", data_inicio=date(2023, 1, 1), data_fim=None)
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[sancao],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.SANCAO_HISTORICA for i in score.indicadores)


# ---------- SOCIO_EM_MULTIPLAS_FORNECEDORAS (peso 20) ----------


def test_socio_em_3_ou_mais_empresas_ativa_indicador():
    """Socio com qtd_empresas_governo >= 3 -> ativa indicador peso 20."""
    socio = Socio(cpf_hmac="abc", nome="Joao", qtd_empresas_governo=3)
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[socio],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    ind = [i for i in score.indicadores if i.tipo == TipoIndicador.SOCIO_EM_MULTIPLAS_FORNECEDORAS]
    assert len(ind) == 1
    assert ind[0].peso == 20


def test_socio_em_2_empresas_nao_ativa():
    """Socio com qtd_empresas_governo = 2 -> nao ativa."""
    socio = Socio(cpf_hmac="abc", nome="Joao", qtd_empresas_governo=2)
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[socio],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.SOCIO_EM_MULTIPLAS_FORNECEDORAS for i in score.indicadores)


def test_multiplos_socios_em_multiplas_gera_um_indicador():
    """Mesmo com varios socios qualificados, gera apenas 1 indicador."""
    socios = [
        Socio(cpf_hmac="a", nome="Joao", qtd_empresas_governo=5),
        Socio(cpf_hmac="b", nome="Maria", qtd_empresas_governo=4),
    ]
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=socios,
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    ind = [i for i in score.indicadores if i.tipo == TipoIndicador.SOCIO_EM_MULTIPLAS_FORNECEDORAS]
    assert len(ind) == 1


# ---------- FORNECEDOR_EXCLUSIVO (peso 10) ----------


def test_fornecedor_todos_contratos_mesmo_orgao_ativa():
    """Todos contratos com mesmo orgao -> ativa indicador peso 10."""
    contratos = [_contrato(orgao="26000"), _contrato(Decimal("100000"), orgao="26000")]
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[],
        contratos=contratos,
        referencia=date(2026, 2, 27),
    )
    ind = [i for i in score.indicadores if i.tipo == TipoIndicador.FORNECEDOR_EXCLUSIVO]
    assert len(ind) == 1
    assert ind[0].peso == 10


def test_fornecedor_contratos_multiplos_orgaos_nao_ativa():
    """Contratos com orgaos diferentes -> nao ativa."""
    contratos = [_contrato(orgao="26000"), _contrato(orgao="54000")]
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[],
        contratos=contratos,
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.FORNECEDOR_EXCLUSIVO for i in score.indicadores)


def test_fornecedor_sem_contratos_nao_ativa_exclusivo():
    """Sem contratos -> nao ativa."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(),
        socios=[],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert not any(i.tipo == TipoIndicador.FORNECEDOR_EXCLUSIVO for i in score.indicadores)


# ---------- SCORE GERAL ----------


def test_score_nunca_excede_100():
    """Mesmo com todos indicadores ativos, cap em 100."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(
            capital=Decimal("100"),
            data_abertura=date(2025, 5, 1),
        ),
        socios=[Socio(cpf_hmac="a", nome="Joao", qtd_empresas_governo=5)],
        sancoes=[
            Sancao(
                tipo=TipoSancao.CEIS, orgao_sancionador="CGU", data_inicio=date(2020, 1, 1), data_fim=date(2022, 12, 31)
            )
        ],
        contratos=[_contrato(Decimal("200000"))],
        referencia=date(2026, 2, 27),
    )
    assert 0 <= score.valor <= 100


def test_sem_indicadores_score_zero():
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(capital=Decimal("1000000"), data_abertura=date(2010, 1, 1)),
        socios=[],
        sancoes=[],
        contratos=[],
        referencia=date(2026, 2, 27),
    )
    assert score.valor == 0
    assert score.faixa == FaixaRisco.BAIXO


def test_faixa_risco_moderado():
    """Score 15 (capital baixo) + 10 (empresa recente) = 25 -> Moderado.
    Usa 2 orgaos distintos para nao ativar FORNECEDOR_EXCLUSIVO."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(
            capital=Decimal("100"),
            data_abertura=date(2025, 5, 1),
        ),
        socios=[],
        sancoes=[],
        contratos=[_contrato(Decimal("100000"), orgao="26000"), _contrato(Decimal("100000"), orgao="54000")],
        referencia=date(2026, 2, 27),
    )
    # capital_baixo(15) + empresa_recente(10) = 25 -> Moderado
    assert score.valor == 25
    assert score.faixa == FaixaRisco.MODERADO


def test_score_com_todos_novos_indicadores():
    """capital_baixo(15) + empresa_recente(10) + sancao(5) + multiplas(20) + exclusivo(10) = 60 -> Alto."""
    score = calcular_score_cumulativo(
        fornecedor=_fornecedor(
            capital=Decimal("100"),
            data_abertura=date(2025, 5, 1),
        ),
        socios=[Socio(cpf_hmac="a", nome="Joao", qtd_empresas_governo=5)],
        sancoes=[
            Sancao(
                tipo=TipoSancao.CEIS, orgao_sancionador="CGU", data_inicio=date(2020, 1, 1), data_fim=date(2022, 12, 31)
            )
        ],
        contratos=[_contrato(Decimal("200000"))],
        referencia=date(2026, 2, 27),
    )
    assert score.valor == 60
    assert score.faixa == FaixaRisco.ALTO
