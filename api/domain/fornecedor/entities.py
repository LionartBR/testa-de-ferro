# api/domain/fornecedor/entities.py
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from .enums import Severidade, SituacaoCadastral, TipoAlerta
from .score import ScoreDeRisco
from .value_objects import CNPJ, CapitalSocial, Endereco, RazaoSocial


@dataclass(frozen=True)
class AlertaCritico:
    """Alerta binario. Se a condicao e detectada, o alerta existe. Independe do score."""
    id: uuid.UUID
    tipo: TipoAlerta
    severidade: Severidade
    descricao: str
    evidencia: str  # deve ser rastreavel (CNPJs, nomes, datas)
    fornecedor_cnpj: CNPJ
    detectado_em: datetime

    def __post_init__(self) -> None:
        if not self.evidencia.strip():
            raise ValueError("AlertaCritico exige evidencia nao-vazia")


@dataclass(frozen=True)
class Fornecedor:
    """Aggregate Root. Imutavel — alertas e score sao computados externamente
    por funcoes puras e passados na construcao (Functional Core pattern)."""
    cnpj: CNPJ
    razao_social: RazaoSocial
    situacao: SituacaoCadastral
    data_abertura: date | None = None
    capital_social: CapitalSocial | None = None
    cnae_principal: str | None = None
    cnae_descricao: str | None = None
    endereco: Endereco | None = None
    alertas_criticos: tuple[AlertaCritico, ...] = ()
    score_risco: ScoreDeRisco | None = None
    total_contratos: int = 0
    valor_total_contratos: Decimal = Decimal("0")
