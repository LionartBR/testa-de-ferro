# api/interfaces/api/dependencies.py
from api.application.services.ficha_service import FichaService
from api.infrastructure.duckdb_connection import get_connection
from api.infrastructure.repositories.duckdb_contrato_repo import DuckDBContratoRepo
from api.infrastructure.repositories.duckdb_fornecedor_repo import DuckDBFornecedorRepo
from api.infrastructure.repositories.duckdb_sancao_repo import DuckDBSancaoRepo
from api.infrastructure.repositories.duckdb_societario_repo import DuckDBSocietarioRepo


def get_ficha_service() -> FichaService:
    conn = get_connection()
    return FichaService(
        fornecedor_repo=DuckDBFornecedorRepo(conn),
        contrato_repo=DuckDBContratoRepo(conn),
        sancao_repo=DuckDBSancaoRepo(conn),
        societario_repo=DuckDBSocietarioRepo(conn),
    )
