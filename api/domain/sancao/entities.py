# api/domain/sancao/entities.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .value_objects import TipoSancao


@dataclass(frozen=True)
class Sancao:
    tipo: TipoSancao
    orgao_sancionador: str
    data_inicio: date
    data_fim: date | None  # None = vigente indefinidamente
    motivo: str = ""

    def vigente(self, referencia: date) -> bool:
        """Puro — recebe data de referencia como parametro, nunca chama date.today()."""
        if self.data_fim is None:
            return True
        return self.data_fim >= referencia
