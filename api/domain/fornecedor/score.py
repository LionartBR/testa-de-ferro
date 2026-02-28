# api/domain/fornecedor/score.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .enums import FaixaRisco, TipoIndicador

# ADR: Pesos como constante de modulo, nao hardcoded em funcoes.
PESOS: dict[TipoIndicador, int] = {
    TipoIndicador.CAPITAL_SOCIAL_BAIXO: 15,
    TipoIndicador.EMPRESA_RECENTE: 10,
    TipoIndicador.CNAE_INCOMPATIVEL: 10,
    TipoIndicador.SOCIO_EM_MULTIPLAS_FORNECEDORAS: 20,
    TipoIndicador.MESMO_ENDERECO: 15,
    TipoIndicador.FORNECEDOR_EXCLUSIVO: 10,
    TipoIndicador.SEM_FUNCIONARIOS: 10,
    TipoIndicador.CRESCIMENTO_SUBITO: 10,
    TipoIndicador.SANCAO_HISTORICA: 5,
}
# Soma teorica maxima: 15+10+10+20+15+10+10+10+5 = 105, cap em 100.


@dataclass(frozen=True)
class IndicadorCumulativo:
    """Um indicador individual ativo. Peso vem da tabela PESOS."""

    tipo: TipoIndicador
    peso: int
    descricao: str
    evidencia: str


@dataclass(frozen=True)
class ScoreDeRisco:
    """Score cumulativo calculado. Imutavel, derivado de indicadores."""

    indicadores: tuple[IndicadorCumulativo, ...]
    calculado_em: datetime

    @property
    def valor(self) -> int:
        """Soma dos pesos, cap em 100."""
        return min(100, sum(i.peso for i in self.indicadores))

    @property
    def faixa(self) -> FaixaRisco:
        v = self.valor
        if v <= 20:
            return FaixaRisco.BAIXO
        if v <= 40:
            return FaixaRisco.MODERADO
        if v <= 65:
            return FaixaRisco.ALTO
        return FaixaRisco.CRITICO
