# api/domain/contrato/value_objects.py
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ValorContrato:
    """Valor em Decimal. Nunca float. Nunca negativo."""

    valor: Decimal

    def __post_init__(self) -> None:
        if self.valor < Decimal("0"):
            raise ValueError("Valor de contrato nao pode ser negativo")


@dataclass(frozen=True)
class ModalidadeLicitacao:
    codigo: str
    descricao: str


@dataclass(frozen=True)
class NumeroLicitacao:
    valor: str
