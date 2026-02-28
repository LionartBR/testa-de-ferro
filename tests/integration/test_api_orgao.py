# tests/integration/test_api_orgao.py
from fastapi.testclient import TestClient


def test_dashboard_orgao_retorna_200(client: TestClient) -> None:
    response = client.get("/api/orgaos/26000/dashboard")
    assert response.status_code == 200
    data = response.json()
    assert data["orgao"]["nome"] == "Ministerio da Educacao"
    assert data["orgao"]["codigo"] == "26000"


def test_dashboard_orgao_contem_top_fornecedores(client: TestClient) -> None:
    response = client.get("/api/orgaos/26000/dashboard")
    data = response.json()
    assert "top_fornecedores" in data
    assert isinstance(data["top_fornecedores"], list)
    assert len(data["top_fornecedores"]) >= 1
    assert data["qtd_contratos"] >= 1


def test_dashboard_orgao_inexistente_retorna_404(client: TestClient) -> None:
    response = client.get("/api/orgaos/99999/dashboard")
    assert response.status_code == 404
