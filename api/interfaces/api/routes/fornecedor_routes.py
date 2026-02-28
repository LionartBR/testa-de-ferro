# api/interfaces/api/routes/fornecedor_routes.py
from fastapi import APIRouter, Depends, HTTPException

from api.application.dtos.ficha_dto import FichaCompletaDTO
from api.application.services.ficha_service import FichaService
from api.domain.fornecedor.value_objects import CNPJ
from api.interfaces.api.dependencies import get_ficha_service

router = APIRouter()


@router.get("/fornecedores/{cnpj_raw}", response_model=FichaCompletaDTO)
def get_ficha(
    cnpj_raw: str,
    service: FichaService = Depends(get_ficha_service),  # noqa: B008
) -> FichaCompletaDTO:
    try:
        cnpj = CNPJ(cnpj_raw)
    except ValueError as err:
        raise HTTPException(status_code=422, detail="CNPJ invalido") from err

    ficha = service.obter_ficha(cnpj)
    if ficha is None:
        raise HTTPException(status_code=404, detail="Fornecedor nao encontrado")
    return ficha
