# api/domain/fornecedor/repository.py
from __future__ import annotations

from typing import Protocol

from .entities import Fornecedor
from .value_objects import CNPJ


class FornecedorRepository(Protocol):
    def buscar_por_cnpj(self, cnpj: CNPJ) -> Fornecedor | None: ...
    def ranking_por_score(self, limit: int, offset: int) -> list[Fornecedor]: ...
    def buscar_por_nome_ou_cnpj(self, query: str, limit: int) -> list[Fornecedor]: ...
