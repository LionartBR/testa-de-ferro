# api/interfaces/api/routes/alerta_routes.py
from fastapi import APIRouter, Depends, HTTPException, Query

from api.application.dtos.alerta_dto import AlertaFeedItemDTO
from api.domain.fornecedor.enums import TipoAlerta
from api.infrastructure.repositories.duckdb_alerta_repo import DuckDBAlertaRepo
from api.interfaces.api.dependencies import get_alerta_repo

router = APIRouter()


@router.get("/alertas", response_model=list[AlertaFeedItemDTO])
def get_alertas_feed(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    repo: DuckDBAlertaRepo = Depends(get_alerta_repo),  # noqa: B008
) -> list[AlertaFeedItemDTO]:
    rows = repo.listar_feed(limit, offset)
    return [AlertaFeedItemDTO(**r) for r in rows]  # type: ignore[arg-type]


@router.get("/alertas/{tipo}", response_model=list[AlertaFeedItemDTO])
def get_alertas_por_tipo(
    tipo: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    repo: DuckDBAlertaRepo = Depends(get_alerta_repo),  # noqa: B008
) -> list[AlertaFeedItemDTO]:
    # Validar tipo contra enum
    valid_tipos = [t.value for t in TipoAlerta]
    if tipo not in valid_tipos:
        raise HTTPException(
            status_code=422,
            detail=f"Tipo de alerta invalido. Valores aceitos: {valid_tipos}",
        )
    rows = repo.listar_por_tipo(tipo, limit, offset)
    return [AlertaFeedItemDTO(**r) for r in rows]  # type: ignore[arg-type]
