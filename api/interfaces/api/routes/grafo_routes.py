# api/interfaces/api/routes/grafo_routes.py
from fastapi import APIRouter, Depends, HTTPException

from api.application.dtos.grafo_dto import GrafoDTO
from api.application.services.grafo_service import GrafoService
from api.domain.fornecedor.value_objects import CNPJ
from api.interfaces.api.dependencies import get_grafo_service

router = APIRouter()


@router.get("/fornecedores/{cnpj_raw}/grafo", response_model=GrafoDTO)
def get_grafo(
    cnpj_raw: str,
    service: GrafoService = Depends(get_grafo_service),  # noqa: B008
) -> GrafoDTO:
    try:
        cnpj = CNPJ(cnpj_raw)
    except ValueError as err:
        raise HTTPException(status_code=422, detail="CNPJ invalido") from err

    grafo = service.obter_grafo(cnpj)
    if not grafo.nos:
        raise HTTPException(status_code=404, detail="Fornecedor nao encontrado")
    return grafo
