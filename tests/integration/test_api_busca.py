# tests/integration/test_api_busca.py
from fastapi.testclient import TestClient


def test_busca_por_nome_retorna_resultado(client: TestClient) -> None:
    response = client.get("/api/busca?q=Empresa")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any("Empresa" in item["razao_social"] for item in data)


def test_busca_por_cnpj_retorna_resultado(client: TestClient) -> None:
    response = client.get("/api/busca?q=11.222.333")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1


def test_busca_query_vazia_retorna_422(client: TestClient) -> None:
    response = client.get("/api/busca?q=")
    assert response.status_code == 422


def test_busca_query_muito_longa_retorna_422(client: TestClient) -> None:
    query = "a" * 201
    response = client.get(f"/api/busca?q={query}")
    assert response.status_code == 422
