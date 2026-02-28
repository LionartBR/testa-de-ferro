# api/domain/servidor/entities.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ServidorPublico:
    """Servidor publico federal. cpf_hmac ja vem hashado do pipeline."""

    cpf_hmac: str
    nome: str
    cargo: str | None = None
    orgao_lotacao: str | None = None
