# api/application/services/score_service.py
"""Calculo de score cumulativo. Funcao pura — zero IO.

ADR: Score e Alertas sao dimensoes INDEPENDENTES.
Este modulo NUNCA deve importar o servico de alertas.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from api.domain.contrato.entities import Contrato
from api.domain.fornecedor.entities import Fornecedor
from api.domain.fornecedor.enums import TipoIndicador
from api.domain.fornecedor.score import PESOS, IndicadorCumulativo, ScoreDeRisco
from api.domain.sancao.entities import Sancao
from api.domain.societario.entities import Socio

# Threshold para considerar capital social "baixo" relativo ao valor contratado.
_CAPITAL_THRESHOLD_GENERICO = Decimal("10000")
_CONTRATO_MINIMO_PARA_CAPITAL = Decimal("100000")

# Empresa aberta ha menos de 6 meses antes do primeiro contrato.
_MESES_EMPRESA_RECENTE = 6


def calcular_score_cumulativo(
    fornecedor: Fornecedor,
    socios: list[Socio],
    sancoes: list[Sancao],
    contratos: list[Contrato],
    referencia: date,
) -> ScoreDeRisco:
    """Funcao pura. Mesma entrada = mesma saida. Zero IO.
    Fase 1: CAPITAL_SOCIAL_BAIXO, EMPRESA_RECENTE, SANCAO_HISTORICA."""
    indicadores: list[IndicadorCumulativo] = []

    ind = _avaliar_capital_social_baixo(fornecedor, contratos)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_empresa_recente(fornecedor, contratos)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_sancao_historica(sancoes, referencia)
    if ind is not None:
        indicadores.append(ind)

    return ScoreDeRisco(
        indicadores=tuple(indicadores),
        calculado_em=datetime.now(),
    )


def _avaliar_capital_social_baixo(
    fornecedor: Fornecedor,
    contratos: list[Contrato],
) -> IndicadorCumulativo | None:
    """CAPITAL_SOCIAL_BAIXO (peso 15): capital < threshold E contratos > R$100k."""
    if fornecedor.capital_social is None or not contratos:
        return None

    valor_total = sum(c.valor.valor for c in contratos)
    if valor_total < _CONTRATO_MINIMO_PARA_CAPITAL:
        return None

    if fornecedor.capital_social.valor >= _CAPITAL_THRESHOLD_GENERICO:
        return None

    return IndicadorCumulativo(
        tipo=TipoIndicador.CAPITAL_SOCIAL_BAIXO,
        peso=PESOS[TipoIndicador.CAPITAL_SOCIAL_BAIXO],
        descricao=f"Capital social R${fornecedor.capital_social.valor:,.2f} "
                  f"desproporcional a contratos R${valor_total:,.2f}",
        evidencia=f"capital={fornecedor.capital_social.valor}, "
                  f"valor_total_contratos={valor_total}",
    )


def _avaliar_empresa_recente(
    fornecedor: Fornecedor,
    contratos: list[Contrato],
) -> IndicadorCumulativo | None:
    """EMPRESA_RECENTE (peso 10): aberta < 6 meses antes do primeiro contrato."""
    if fornecedor.data_abertura is None or not contratos:
        return None

    datas = [c.data_assinatura for c in contratos if c.data_assinatura is not None]
    if not datas:
        return None

    primeiro_contrato = min(datas)
    dias_ate_contrato = (primeiro_contrato - fornecedor.data_abertura).days
    meses_ate_contrato = dias_ate_contrato / 30.44  # media de dias/mes

    if meses_ate_contrato >= _MESES_EMPRESA_RECENTE:
        return None

    return IndicadorCumulativo(
        tipo=TipoIndicador.EMPRESA_RECENTE,
        peso=PESOS[TipoIndicador.EMPRESA_RECENTE],
        descricao=f"Empresa aberta em {fornecedor.data_abertura} obteve "
                  f"primeiro contrato em {primeiro_contrato} "
                  f"({dias_ate_contrato} dias depois)",
        evidencia=f"data_abertura={fornecedor.data_abertura}, "
                  f"primeiro_contrato={primeiro_contrato}, "
                  f"dias={dias_ate_contrato}",
    )


def _avaliar_sancao_historica(
    sancoes: list[Sancao],
    referencia: date,
) -> IndicadorCumulativo | None:
    """SANCAO_HISTORICA (peso 5): sancao expirada -> indicador cumulativo, nunca alerta."""
    expiradas = [s for s in sancoes if not s.vigente(referencia)]
    if not expiradas:
        return None

    return IndicadorCumulativo(
        tipo=TipoIndicador.SANCAO_HISTORICA,
        peso=PESOS[TipoIndicador.SANCAO_HISTORICA],
        descricao=f"{len(expiradas)} sancao(oes) historica(s) expirada(s)",
        evidencia=f"sancoes_expiradas={[s.tipo.value for s in expiradas]}",
    )
