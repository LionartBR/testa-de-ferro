# api/infrastructure/duckdb_connection.py
from __future__ import annotations

import duckdb

from .config import get_settings

_connection: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    global _connection  # noqa: PLW0603
    if _connection is None:
        path = get_settings().duckdb_path
        read_only = path != ":memory:"
        _connection = duckdb.connect(path, read_only=read_only)
    return _connection


def set_connection(conn: duckdb.DuckDBPyConnection) -> None:
    """Usado em testes para injetar DuckDB in-memory."""
    global _connection  # noqa: PLW0603
    _connection = conn
