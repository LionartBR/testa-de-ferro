# api/domain/societario/repository.py
from __future__ import annotations

from typing import Protocol

from api.domain.fornecedor.value_objects import CNPJ

from .entities import Socio


class SocietarioRepository(Protocol):
    def listar_socios_por_fornecedor(self, cnpj: CNPJ) -> list[Socio]: ...

    def grafo_2_niveis(
        self,
        cnpj: CNPJ,
        max_nos: int = 50,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]: ...
