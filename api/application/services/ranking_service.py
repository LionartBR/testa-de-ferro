# api/application/services/ranking_service.py
from __future__ import annotations

from api.domain.fornecedor.repository import FornecedorRepository

from ..dtos.fornecedor_dto import FornecedorResumoDTO


class RankingService:
    def __init__(self, fornecedor_repo: FornecedorRepository) -> None:
        self._fornecedor_repo = fornecedor_repo

    def ranking(self, limit: int, offset: int) -> list[FornecedorResumoDTO]:
        fornecedores = self._fornecedor_repo.ranking_por_score(limit, offset)
        return [
            FornecedorResumoDTO(
                cnpj=f.cnpj.formatado,
                razao_social=f.razao_social.valor,
                situacao=f.situacao.value,
                score_risco=f.score_risco.valor if f.score_risco else 0,
                faixa_risco=f.score_risco.faixa.value if f.score_risco else "Baixo",
                qtd_alertas=len(f.alertas_criticos),
                max_severidade=max(
                    (a.severidade.value for a in f.alertas_criticos), default=None,
                ),
                total_contratos=f.total_contratos,
                valor_total=str(f.valor_total_contratos),
            )
            for f in fornecedores
        ]
