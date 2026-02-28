# api/domain/servidor/repository.py
from __future__ import annotations

from typing import Protocol

from .entities import ServidorPublico


class ServidorRepository(Protocol):
    def buscar_por_cpf_hmac(self, cpf_hmac: str) -> ServidorPublico | None: ...
