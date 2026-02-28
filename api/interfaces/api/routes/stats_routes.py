# api/interfaces/api/routes/stats_routes.py
from fastapi import APIRouter, Depends

from api.application.dtos.stats_dto import StatsDTO
from api.infrastructure.repositories.duckdb_stats_repo import DuckDBStatsRepo
from api.interfaces.api.dependencies import get_stats_repo

router = APIRouter()


@router.get("/stats", response_model=StatsDTO)
def get_stats(
    repo: DuckDBStatsRepo = Depends(get_stats_repo),  # noqa: B008
) -> StatsDTO:
    data = repo.obter_stats()
    return StatsDTO(**data)  # type: ignore[arg-type]
