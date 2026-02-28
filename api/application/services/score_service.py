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

from .cnae_mapping import cnae_incompativel_com_objeto, get_cnae_category

# Threshold para considerar capital social "baixo" relativo ao valor contratado.
_CAPITAL_THRESHOLD_GENERICO = Decimal("10000")
_CONTRATO_MINIMO_PARA_CAPITAL = Decimal("100000")

# Empresa aberta ha menos de 6 meses antes do primeiro contrato.
_MESES_EMPRESA_RECENTE = 6

# Socio em multiplas fornecedoras: threshold de empresas com governo.
_MULTIPLAS_FORNECEDORAS_THRESHOLD = 3

# CRESCIMENTO_SUBITO thresholds.
_CRESCIMENTO_RAZAO_MINIMA = Decimal("5")
_CRESCIMENTO_VALOR_MINIMO = Decimal("200000")


def calcular_score_cumulativo(
    fornecedor: Fornecedor,
    socios: list[Socio],
    sancoes: list[Sancao],
    contratos: list[Contrato],
    referencia: date,
) -> ScoreDeRisco:
    """Funcao pura. Mesma entrada = mesma saida. Zero IO."""
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

    ind = _avaliar_socio_em_multiplas(socios)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_fornecedor_exclusivo(contratos)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_cnae_incompativel(fornecedor, contratos)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_mesmo_endereco(fornecedor)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_sem_funcionarios(fornecedor, contratos)
    if ind is not None:
        indicadores.append(ind)

    ind = _avaliar_crescimento_subito(contratos, referencia)
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
        evidencia=f"capital={fornecedor.capital_social.valor}, valor_total_contratos={valor_total}",
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


def _avaliar_socio_em_multiplas(
    socios: list[Socio],
) -> IndicadorCumulativo | None:
    """SOCIO_EM_MULTIPLAS_FORNECEDORAS (peso 20): socio com qtd_empresas_governo >= 3."""
    socios_multiplas = [s for s in socios if s.qtd_empresas_governo >= _MULTIPLAS_FORNECEDORAS_THRESHOLD]
    if not socios_multiplas:
        return None

    return IndicadorCumulativo(
        tipo=TipoIndicador.SOCIO_EM_MULTIPLAS_FORNECEDORAS,
        peso=PESOS[TipoIndicador.SOCIO_EM_MULTIPLAS_FORNECEDORAS],
        descricao=f"{len(socios_multiplas)} socio(s) presente(s) em 3+ empresas fornecedoras do governo",
        evidencia=f"socios={[(s.nome, s.qtd_empresas_governo) for s in socios_multiplas]}",
    )


def _avaliar_fornecedor_exclusivo(
    contratos: list[Contrato],
) -> IndicadorCumulativo | None:
    """FORNECEDOR_EXCLUSIVO (peso 10): todos contratos com mesmo orgao."""
    if not contratos:
        return None

    orgaos = {c.orgao_codigo for c in contratos}
    if len(orgaos) > 1:
        return None

    orgao_unico = next(iter(orgaos))
    return IndicadorCumulativo(
        tipo=TipoIndicador.FORNECEDOR_EXCLUSIVO,
        peso=PESOS[TipoIndicador.FORNECEDOR_EXCLUSIVO],
        descricao=f"Todos os {len(contratos)} contrato(s) sao com o mesmo orgao",
        evidencia=f"orgao_codigo={orgao_unico}, qtd_contratos={len(contratos)}",
    )


def _avaliar_cnae_incompativel(
    fornecedor: Fornecedor,
    contratos: list[Contrato],
) -> IndicadorCumulativo | None:
    """CNAE_INCOMPATIVEL (peso 10): CNAE principal incompativel com objeto contratado."""
    if fornecedor.cnae_principal is None or not contratos:
        return None

    categoria_cnae = get_cnae_category(fornecedor.cnae_principal)
    if categoria_cnae is None:
        return None

    for contrato in contratos:
        if contrato.objeto is None:
            continue
        # Infer object category from keywords in objeto text
        obj_cat = _inferir_categoria_objeto(contrato.objeto)
        if obj_cat is not None and cnae_incompativel_com_objeto(fornecedor.cnae_principal, obj_cat):
            return IndicadorCumulativo(
                tipo=TipoIndicador.CNAE_INCOMPATIVEL,
                peso=PESOS[TipoIndicador.CNAE_INCOMPATIVEL],
                descricao=f"CNAE {fornecedor.cnae_principal} ({categoria_cnae}) "
                f"incompativel com objeto contratado ({obj_cat})",
                evidencia=f"cnae={fornecedor.cnae_principal}, "
                f"categoria_cnae={categoria_cnae}, objeto_categoria={obj_cat}",
            )
    return None


def _inferir_categoria_objeto(objeto: str) -> str | None:
    """Infer contract object category from keywords in the object description.

    Returns an uppercase category string matching INCOMPATIBLE_COMBOS keys,
    or None if no category can be inferred.
    """
    obj_lower = objeto.lower()
    keyword_map: dict[str, list[str]] = {
        "TECNOLOGIA": ["software", "sistema", "ti ", "informatica", "computador", "rede", "dados"],
        "CONSTRUCAO": ["obra", "construcao", "reforma", "pavimentacao", "edificacao", "engenharia civil"],
        "SAUDE": ["medicamento", "hospitalar", "saude", "medico", "farmac", "laboratorio"],
        "ALIMENTACAO": ["alimentacao", "refeicao", "merenda", "alimento"],
        "LIMPEZA": ["limpeza", "conservacao", "higienizacao", "asseio"],
        "SEGURANCA": ["vigilancia", "seguranca patrimonial", "monitoramento eletronico"],
        "CONSULTORIA": ["consultoria", "assessoria", "auditoria"],
        "EDUCACAO": ["ensino", "treinamento", "capacitacao", "curso"],
    }
    for category, keywords in keyword_map.items():
        if any(kw in obj_lower for kw in keywords):
            return category
    return None


def _avaliar_mesmo_endereco(
    fornecedor: Fornecedor,
) -> IndicadorCumulativo | None:
    """MESMO_ENDERECO (peso 15): fornecedor compartilha endereco com 2+ outros fornecedores."""
    if fornecedor.qtd_fornecedores_mesmo_endereco < 2:
        return None

    return IndicadorCumulativo(
        tipo=TipoIndicador.MESMO_ENDERECO,
        peso=PESOS[TipoIndicador.MESMO_ENDERECO],
        descricao=f"Compartilha endereco com {fornecedor.qtd_fornecedores_mesmo_endereco} "
        f"outro(s) fornecedor(es) do governo",
        evidencia=f"qtd_mesmo_endereco={fornecedor.qtd_fornecedores_mesmo_endereco}",
    )


def _avaliar_sem_funcionarios(
    fornecedor: Fornecedor,
    contratos: list[Contrato],
) -> IndicadorCumulativo | None:
    """SEM_FUNCIONARIOS (peso 10): 0 funcionarios com contratos ativos. None = fail-safe."""
    if fornecedor.qtd_funcionarios is None:
        return None
    if fornecedor.qtd_funcionarios > 0:
        return None
    if not contratos:
        return None

    return IndicadorCumulativo(
        tipo=TipoIndicador.SEM_FUNCIONARIOS,
        peso=PESOS[TipoIndicador.SEM_FUNCIONARIOS],
        descricao=f"Empresa sem funcionarios registrados com {len(contratos)} contrato(s) ativo(s)",
        evidencia=f"qtd_funcionarios=0, qtd_contratos={len(contratos)}",
    )


def _avaliar_crescimento_subito(
    contratos: list[Contrato],
    referencia: date,
) -> IndicadorCumulativo | None:
    """CRESCIMENTO_SUBITO (peso 10): valor contratado salta 5x+ entre anos consecutivos, > R$200k."""
    if not contratos:
        return None

    # Group contract values by year
    anuais: dict[int, Decimal] = {}
    for c in contratos:
        if c.data_assinatura is None:
            continue
        ano = c.data_assinatura.year
        anuais[ano] = anuais.get(ano, Decimal("0")) + c.valor.valor

    # Compare consecutive years
    anos_sorted = sorted(anuais.keys())
    for i in range(1, len(anos_sorted)):
        ano_prev = anos_sorted[i - 1]
        ano_curr = anos_sorted[i]
        if ano_curr != ano_prev + 1:
            continue
        valor_prev = anuais[ano_prev]
        valor_curr = anuais[ano_curr]
        if valor_prev <= 0:
            continue
        razao = valor_curr / valor_prev
        if razao >= _CRESCIMENTO_RAZAO_MINIMA and valor_curr >= _CRESCIMENTO_VALOR_MINIMO:
            return IndicadorCumulativo(
                tipo=TipoIndicador.CRESCIMENTO_SUBITO,
                peso=PESOS[TipoIndicador.CRESCIMENTO_SUBITO],
                descricao=f"Valor contratado saltou {razao:.1f}x entre {ano_prev} e {ano_curr}",
                evidencia=f"ano_anterior={ano_prev}, valor_anterior={valor_prev:.2f}, "
                f"ano_atual={ano_curr}, valor_atual={valor_curr:.2f}, razao={razao:.1f}x",
            )
    return None
