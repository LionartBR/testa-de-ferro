# api/domain/doacao/repository.py
from __future__ import annotations

from typing import Protocol

from api.domain.fornecedor.value_objects import CNPJ

from .entities import DoacaoEleitoral


class DoacaoRepository(Protocol):
    def listar_por_fornecedor(self, cnpj: CNPJ) -> list[DoacaoEleitoral]: ...
