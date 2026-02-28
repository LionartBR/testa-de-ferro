# api/domain/doacao/entities.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from api.domain.fornecedor.value_objects import CNPJ

from .value_objects import ValorDoacao


@dataclass(frozen=True)
class DoacaoEleitoral:
    """Doacao eleitoral vinculada a fornecedor (via CNPJ) ou socio (via cpf_hmac)."""
    fornecedor_cnpj: CNPJ | None
    socio_cpf_hmac: str | None
    candidato_nome: str
    candidato_partido: str
    candidato_cargo: str
    valor: ValorDoacao
    ano_eleicao: int

    def material(self, threshold: Decimal = Decimal("10000")) -> bool:
        """Doacao e considerada material se valor > threshold."""
        return self.valor.valor > threshold
