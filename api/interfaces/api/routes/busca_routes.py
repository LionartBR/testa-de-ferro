# api/interfaces/api/routes/busca_routes.py
from fastapi import APIRouter, Depends, Query

from api.application.dtos.fornecedor_dto import FornecedorResumoDTO
from api.application.services.busca_service import BuscaService
from api.interfaces.api.dependencies import get_busca_service

router = APIRouter()


@router.get("/busca", response_model=list[FornecedorResumoDTO])
def buscar(
    q: str = Query(..., min_length=1, max_length=200),
    limit: int = Query(default=20, ge=1, le=100),
    service: BuscaService = Depends(get_busca_service),  # noqa: B008
) -> list[FornecedorResumoDTO]:
    return service.buscar(q, limit)
