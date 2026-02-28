# api/domain/societario/value_objects.py
from __future__ import annotations

from dataclasses import dataclass


def _verificar_cpf(digitos: str) -> bool:
    """Algoritmo padrao brasileiro de verificacao de CPF."""
    pesos_1 = [10, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos_1[i] for i in range(9))
    resto = soma % 11
    d1 = 0 if resto < 2 else 11 - resto
    if int(digitos[9]) != d1:
        return False

    pesos_2 = [11, 10, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(digitos[i]) * pesos_2[i] for i in range(10))
    resto = soma % 11
    d2 = 0 if resto < 2 else 11 - resto
    return int(digitos[10]) == d2


@dataclass(frozen=True)
class CPF:
    """Value Object imutavel para CPF. NUNCA expoe valor completo em repr/str (LGPD)."""
    _valor: str  # sempre 11 digitos

    def __init__(self, raw: str) -> None:
        digitos = "".join(c for c in raw if c.isdigit())
        if len(digitos) != 11:
            raise ValueError(f"CPF invalido: comprimento {len(digitos)}, esperado 11")
        if len(set(digitos)) == 1:
            raise ValueError("CPF invalido: todos digitos iguais")
        if not _verificar_cpf(digitos):
            raise ValueError("CPF invalido: digitos verificadores incorretos")
        object.__setattr__(self, "_valor", digitos)

    @property
    def valor(self) -> str:
        """11 digitos sem formatacao. Usar com cuidado — nunca logar."""
        return self._valor

    @property
    def mascarado(self) -> str:
        """***.XXX.XXX-** — formato seguro para logs."""
        d = self._valor
        return f"***.{d[3:6]}.{d[6:9]}-**"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CPF):
            return NotImplemented
        return self._valor == other._valor

    def __hash__(self) -> int:
        return hash(self._valor)

    def __repr__(self) -> str:
        return f"CPF({self.mascarado!r})"

    def __str__(self) -> str:
        return self.mascarado


@dataclass(frozen=True)
class CPFMascarado:
    """CPF parcialmente mascarado do Portal da Transparencia: ***.XXX.XXX-**"""
    _raw: str
    _digitos_visiveis: str

    def __init__(self, raw: str) -> None:
        object.__setattr__(self, "_raw", raw)
        apenas_digitos = "".join(c for c in raw if c.isdigit())
        object.__setattr__(self, "_digitos_visiveis", apenas_digitos)

    @property
    def digitos_visiveis(self) -> str:
        return self._digitos_visiveis

    def bate_com(self, cpf: CPF) -> bool:
        """Verifica se os digitos visiveis correspondem ao CPF completo."""
        return cpf.valor[3:9] == self._digitos_visiveis


@dataclass(frozen=True)
class QualificacaoSocio:
    """Qualificacao do socio (ex: Socio-Administrador, Socio, etc.)"""
    valor: str
