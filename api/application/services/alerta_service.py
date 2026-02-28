# api/application/services/alerta_service.py
"""Deteccao de alertas criticos. Funcao pura — zero IO.

ADR: Score e Alertas sao dimensoes INDEPENDENTES.
Este modulo NUNCA deve importar o servico de score.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime

from api.domain.contrato.entities import Contrato
from api.domain.fornecedor.entities import AlertaCritico, Fornecedor
from api.domain.fornecedor.enums import Severidade, TipoAlerta
from api.domain.sancao.entities import Sancao
from api.domain.societario.entities import Socio


def detectar_alertas(
    fornecedor: Fornecedor,
    socios: list[Socio],
    sancoes: list[Sancao],
    contratos: list[Contrato],
    referencia: date,
) -> list[AlertaCritico]:
    """Funcao pura. Mesma entrada = mesma saida. Zero IO.
    Fase 1: SOCIO_SERVIDOR_PUBLICO + EMPRESA_SANCIONADA_CONTRATANDO."""
    alertas: list[AlertaCritico] = []
    alertas.extend(_detectar_socio_servidor(fornecedor, socios))
    alertas.extend(_detectar_empresa_sancionada(fornecedor, sancoes, contratos, referencia))
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
