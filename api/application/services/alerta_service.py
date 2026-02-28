# api/application/services/alerta_service.py
"""Deteccao de alertas criticos. Funcao pura — zero IO.

ADR: Score e Alertas sao dimensoes INDEPENDENTES.
Este modulo NUNCA deve importar o servico de score.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from api.domain.contrato.entities import Contrato
from api.domain.doacao.entities import DoacaoEleitoral
from api.domain.fornecedor.entities import AlertaCritico, Fornecedor
from api.domain.fornecedor.enums import Severidade, TipoAlerta
from api.domain.sancao.entities import Sancao
from api.domain.societario.entities import Socio

# Thresholds para DOACAO_PARA_CONTRATANTE
_DOACAO_THRESHOLD = Decimal("10000")
_CONTRATO_THRESHOLD_DOACAO = Decimal("500000")


def detectar_alertas(
    fornecedor: Fornecedor,
    socios: list[Socio],
    sancoes: list[Sancao],
    contratos: list[Contrato],
    referencia: date,
    doacoes: list[DoacaoEleitoral] | None = None,
) -> list[AlertaCritico]:
    """Funcao pura. Mesma entrada = mesma saida. Zero IO."""
    alertas: list[AlertaCritico] = []
    alertas.extend(_detectar_socio_servidor(fornecedor, socios))
    alertas.extend(_detectar_empresa_sancionada(fornecedor, sancoes, contratos, referencia))
    alertas.extend(_detectar_doacao_para_contratante(fornecedor, doacoes or [], contratos))
    alertas.extend(_detectar_socio_sancionado(fornecedor, socios))
    return alertas


def _detectar_socio_servidor(
    fornecedor: Fornecedor,
    socios: list[Socio],
) -> list[AlertaCritico]:
    """SOCIO_SERVIDOR_PUBLICO: socio e servidor publico federal ativo -> GRAVISSIMO."""
    alertas: list[AlertaCritico] = []
    for socio in socios:
        if socio.is_servidor_publico:
            alertas.append(AlertaCritico(
                id=uuid.uuid4(),
                tipo=TipoAlerta.SOCIO_SERVIDOR_PUBLICO,
                severidade=Severidade.GRAVISSIMO,
                descricao=f"Socio {socio.nome} e servidor publico"
                          + (f" ({socio.orgao_lotacao})" if socio.orgao_lotacao else ""),
                evidencia=f"socio_cpf_hmac={socio.cpf_hmac}, nome={socio.nome}"
                          + (f", orgao={socio.orgao_lotacao}" if socio.orgao_lotacao else ""),
                fornecedor_cnpj=fornecedor.cnpj,
                detectado_em=datetime.now(),
            ))
    return alertas


def _detectar_empresa_sancionada(
    fornecedor: Fornecedor,
    sancoes: list[Sancao],
    contratos: list[Contrato],
    referencia: date,
) -> list[AlertaCritico]:
    """EMPRESA_SANCIONADA_CONTRATANDO: sancao vigente + contratos ativos -> GRAVISSIMO.
    Sancao expirada NAO gera alerta (vira SANCAO_HISTORICA no score)."""
    sancoes_vigentes = [s for s in sancoes if s.vigente(referencia)]
    if not sancoes_vigentes or not contratos:
        return []

    return [AlertaCritico(
        id=uuid.uuid4(),
        tipo=TipoAlerta.EMPRESA_SANCIONADA_CONTRATANDO,
        severidade=Severidade.GRAVISSIMO,
        descricao=f"Empresa com {len(sancoes_vigentes)} sancao(oes) vigente(s) "
                  f"e {len(contratos)} contrato(s) ativo(s)",
        evidencia=f"sancoes_vigentes={[s.tipo.value for s in sancoes_vigentes]}, "
                  f"qtd_contratos={len(contratos)}",
        fornecedor_cnpj=fornecedor.cnpj,
        detectado_em=datetime.now(),
    )]


def _detectar_doacao_para_contratante(
    fornecedor: Fornecedor,
    doacoes: list[DoacaoEleitoral],
    contratos: list[Contrato],
) -> list[AlertaCritico]:
    """DOACAO_PARA_CONTRATANTE: doacao > R$10k E contrato > R$500k -> GRAVE.
    ADR: Threshold de materialidade — doacao pequena + contrato grande = ruido."""
    if not doacoes or not contratos:
        return []

    doacoes_materiais = [d for d in doacoes if d.material(_DOACAO_THRESHOLD)]
    if not doacoes_materiais:
        return []

    valor_total_contratos = sum(c.valor.valor for c in contratos)
    if valor_total_contratos <= _CONTRATO_THRESHOLD_DOACAO:
        return []

    return [AlertaCritico(
        id=uuid.uuid4(),
        tipo=TipoAlerta.DOACAO_PARA_CONTRATANTE,
        severidade=Severidade.GRAVE,
        descricao=f"{len(doacoes_materiais)} doacao(oes) material(is) "
                  f"com contratos totalizando R${valor_total_contratos:,.2f}",
        evidencia=f"doacoes_materiais={len(doacoes_materiais)}, "
                  f"valor_total_contratos={valor_total_contratos}, "
                  f"candidatos={[d.candidato_nome for d in doacoes_materiais]}",
        fornecedor_cnpj=fornecedor.cnpj,
        detectado_em=datetime.now(),
    )]


def _detectar_socio_sancionado(
    fornecedor: Fornecedor,
    socios: list[Socio],
) -> list[AlertaCritico]:
    """SOCIO_SANCIONADO_EM_OUTRA: socio que e socio de outra empresa sancionada -> GRAVE."""
    alertas: list[AlertaCritico] = []
    for socio in socios:
        if socio.is_sancionado:
            alertas.append(AlertaCritico(
                id=uuid.uuid4(),
                tipo=TipoAlerta.SOCIO_SANCIONADO_EM_OUTRA,
                severidade=Severidade.GRAVE,
                descricao=f"Socio {socio.nome} e socio de outra empresa sancionada",
                evidencia=f"socio_cpf_hmac={socio.cpf_hmac}, nome={socio.nome}",
                fornecedor_cnpj=fornecedor.cnpj,
                detectado_em=datetime.now(),
            ))
    return alertas
