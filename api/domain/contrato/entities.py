# api/domain/contrato/entities.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from api.domain.fornecedor.value_objects import CNPJ

from .value_objects import ValorContrato


@dataclass(frozen=True)
class Contrato:
    fornecedor_cnpj: CNPJ
    orgao_codigo: str
    valor: ValorContrato
    data_assinatura: date | None = None
    objeto: str | None = None
    num_licitacao: str | None = None
    data_vigencia: date | None = None
