# api/domain/doacao/value_objects.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ValorDoacao:
    """Valor monetario em Decimal. Nunca float, nunca negativo."""
    valor: Decimal

    def __post_init__(self) -> None:
        if self.valor < Decimal("0"):
            raise ValueError("Valor de doacao nao pode ser negativo")


@dataclass(frozen=True)
class AnoCampanha:
    """Ano de eleicao (2018, 2020, 2022, ...)."""
    valor: int

    def __post_init__(self) -> None:
        if self.valor < 1998 or self.valor > 2030:
            raise ValueError(f"Ano de campanha invalido: {self.valor}")
