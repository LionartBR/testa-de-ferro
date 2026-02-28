# api/interfaces/api/dependencies.py
from api.application.services.busca_service import BuscaService
from api.application.services.export_service import ExportService
from api.application.services.ficha_service import FichaService
from api.application.services.grafo_service import GrafoService
from api.application.services.ranking_service import RankingService
from api.infrastructure.duckdb_connection import get_connection
from api.infrastructure.repositories.duckdb_alerta_repo import DuckDBAlertaRepo
from api.infrastructure.repositories.duckdb_contrato_repo import DuckDBContratoRepo
from api.infrastructure.repositories.duckdb_doacao_repo import DuckDBDoacaoRepo
from api.infrastructure.repositories.duckdb_fornecedor_repo import DuckDBFornecedorRepo
from api.infrastructure.repositories.duckdb_sancao_repo import DuckDBSancaoRepo
from api.infrastructure.repositories.duckdb_societario_repo import DuckDBSocietarioRepo
from api.infrastructure.repositories.duckdb_stats_repo import DuckDBStatsRepo


def get_ficha_service() -> FichaService:
    conn = get_connection()
    return FichaService(
        fornecedor_repo=DuckDBFornecedorRepo(conn),
        contrato_repo=DuckDBContratoRepo(conn),
        sancao_repo=DuckDBSancaoRepo(conn),
        societario_repo=DuckDBSocietarioRepo(conn),
        doacao_repo=DuckDBDoacaoRepo(conn),
        alerta_repo=DuckDBAlertaRepo(conn),
    )


def get_ranking_service() -> RankingService:
    conn = get_connection()
    return RankingService(fornecedor_repo=DuckDBFornecedorRepo(conn))


def get_busca_service() -> BuscaService:
    conn = get_connection()
    return BuscaService(fornecedor_repo=DuckDBFornecedorRepo(conn))


def get_grafo_service() -> GrafoService:
    conn = get_connection()
    return GrafoService(societario_repo=DuckDBSocietarioRepo(conn))


def get_export_service() -> ExportService:
    return ExportService()


def get_alerta_repo() -> DuckDBAlertaRepo:
    return DuckDBAlertaRepo(get_connection())


def get_stats_repo() -> DuckDBStatsRepo:
    return DuckDBStatsRepo(get_connection())


def get_contrato_repo() -> DuckDBContratoRepo:
    return DuckDBContratoRepo(get_connection())
