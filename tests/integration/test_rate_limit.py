# tests/integration/test_rate_limit.py
from __future__ import annotations

import os
from collections.abc import Generator

import duckdb
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def rate_limited_client(test_db: duckdb.DuckDBPyConnection) -> Generator[TestClient, None, None]:
    """Client com rate limit ativo (3 req/min para teste rapido)."""
    from api.infrastructure import duckdb_connection
    duckdb_connection.set_connection(test_db)

    from api.infrastructure.config import get_settings
    get_settings.cache_clear()

    old_val = os.environ.get("API_RATE_LIMIT_PER_MINUTE", "0")
    os.environ["API_RATE_LIMIT_PER_MINUTE"] = "3"
    get_settings.cache_clear()

    from api.interfaces.api.main import app
    with TestClient(app) as c:
        yield c

    os.environ["API_RATE_LIMIT_PER_MINUTE"] = old_val
    get_settings.cache_clear()


def test_rate_limit_permite_dentro_do_limite(rate_limited_client: TestClient) -> None:
    for _ in range(3):
        response = rate_limited_client.get("/api/stats")
        assert response.status_code == 200


def test_rate_limit_bloqueia_apos_limite(rate_limited_client: TestClient) -> None:
    for _ in range(3):
        rate_limited_client.get("/api/stats")
    response = rate_limited_client.get("/api/stats")
    assert response.status_code == 429
    assert "Rate limit" in response.json()["detail"]


def test_rate_limit_bypass_com_api_key(rate_limited_client: TestClient) -> None:
    # Esgotar o limite
    for _ in range(3):
        rate_limited_client.get("/api/stats")
    # Com API key, deve passar
    response = rate_limited_client.get("/api/stats", headers={"X-API-Key": "test-key"})
    assert response.status_code == 200
