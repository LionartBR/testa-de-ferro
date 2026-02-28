# api/domain/fornecedor/value_objects.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


def _verificar_cnpj(digitos: str) -> bool:
    """Algoritmo padrao brasileiro de verificacao de CNPJ."""
    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos_1[i] for i in range(12))
    resto = soma % 11
    d1 = 0 if resto < 2 else 11 - resto
    if int(digitos[12]) != d1:
        return False

    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos_2[i] for i in range(13))
    resto = soma % 11
    d2 = 0 if resto < 2 else 11 - resto
    return int(digitos[13]) == d2


@dataclass(frozen=True)
class CNPJ:
    """Value Object imutavel para CNPJ. Valida digitos verificadores no construtor."""

    _valor: str  # sempre 14 digitos sem formatacao

    def __init__(self, raw: str) -> None:
        digitos = "".join(c for c in raw if c.isdigit())
        if len(digitos) != 14:
            raise ValueError(f"CNPJ invalido: comprimento {len(digitos)}, esperado 14")
        if len(set(digitos)) == 1:
            raise ValueError("CNPJ invalido: todos digitos iguais")
        if not _verificar_cnpj(digitos):
            raise ValueError("CNPJ invalido: digitos verificadores incorretos")
        object.__setattr__(self, "_valor", digitos)

    @property
    def valor(self) -> str:
        """14 digitos sem formatacao."""
        return self._valor

    @property
    def formatado(self) -> str:
        """XX.XXX.XXX/XXXX-XX"""
        d = self._valor
        return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CNPJ):
            return NotImplemented
        return self._valor == other._valor

    def __hash__(self) -> int:
        return hash(self._valor)

    def __repr__(self) -> str:
        return f"CNPJ({self.formatado!r})"

    def __str__(self) -> str:
        return self.formatado


@dataclass(frozen=True)
class RazaoSocial:
    """Razao social nao-vazia, trimada."""

    valor: str

    def __post_init__(self) -> None:
        stripped = self.valor.strip()
        if not stripped:
            raise ValueError("Razao social nao pode ser vazia")
        object.__setattr__(self, "valor", stripped)


@dataclass(frozen=True)
class CapitalSocial:
    """Valor monetario em Decimal. Nunca negativo. Nunca float."""

    valor: Decimal

    def __post_init__(self) -> None:
        if self.valor < Decimal("0"):
            raise ValueError("Capital social nao pode ser negativo")


@dataclass(frozen=True)
class Endereco:
    """Endereco sem complemento (ver ADR MESMO_ENDERECO no CLAUDE.md)."""

    logradouro: str
    municipio: str
    uf: str
    cep: str
