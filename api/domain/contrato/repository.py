# api/domain/contrato/repository.py
from __future__ import annotations

from typing import Protocol

from api.domain.fornecedor.value_objects import CNPJ

from .entities import Contrato


class ContratoRepository(Protocol):
    def listar_por_fornecedor(self, cnpj: CNPJ) -> list[Contrato]: ...
