# api/interfaces/api/routes/ranking_routes.py
from fastapi import APIRouter, Depends, Query

from api.application.dtos.fornecedor_dto import FornecedorResumoDTO
from api.application.services.ranking_service import RankingService
from api.interfaces.api.dependencies import get_ranking_service

router = APIRouter()


@router.get("/fornecedores/ranking", response_model=list[FornecedorResumoDTO])
def get_ranking(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: RankingService = Depends(get_ranking_service),  # noqa: B008
) -> list[FornecedorResumoDTO]:
    return service.ranking(limit, offset)
