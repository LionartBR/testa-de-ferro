# tests/integration/test_api_stats.py
from fastapi.testclient import TestClient


def test_stats_retorna_200(client: TestClient) -> None:
    response = client.get("/api/stats")
    assert response.status_code == 200


def test_stats_contem_totais(client: TestClient) -> None:
    response = client.get("/api/stats")
    data = response.json()
    assert "total_fornecedores" in data
    assert "total_contratos" in data
    assert "total_alertas" in data
    assert data["total_fornecedores"] >= 2
    assert data["total_contratos"] >= 2
    assert data["total_alertas"] >= 2


def test_stats_contem_fontes(client: TestClient) -> None:
    response = client.get("/api/stats")
    data = response.json()
    assert "fontes" in data
    assert "fornecedores" in data["fontes"]
    assert "registros" in data["fontes"]["fornecedores"]
