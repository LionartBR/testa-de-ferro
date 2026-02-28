# api/interfaces/api/routes/contrato_routes.py
from fastapi import APIRouter, Depends, Query

from api.application.dtos.contrato_dto import ContratoResumoDTO
from api.infrastructure.repositories.duckdb_contrato_repo import DuckDBContratoRepo
from api.interfaces.api.dependencies import get_contrato_repo

router = APIRouter()


@router.get("/contratos", response_model=list[ContratoResumoDTO])
def get_contratos(
    cnpj: str | None = None,
    orgao_codigo: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    repo: DuckDBContratoRepo = Depends(get_contrato_repo),  # noqa: B008
) -> list[ContratoResumoDTO]:
    contratos = repo.listar(limit, offset, cnpj=cnpj, orgao_codigo=orgao_codigo)
    return [
        ContratoResumoDTO(
            orgao_codigo=c.orgao_codigo,
            valor=str(c.valor.valor),
            data_assinatura=c.data_assinatura.isoformat() if c.data_assinatura else None,
            objeto=c.objeto,
        )
        for c in contratos
    ]
