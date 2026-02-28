# tests/integration/test_api_grafo.py
from fastapi.testclient import TestClient


def test_grafo_retorna_nos_e_arestas(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181/grafo")
    assert response.status_code == 200
    data = response.json()
    assert "nos" in data
    assert "arestas" in data
    assert len(data["nos"]) >= 1


def test_grafo_contem_tipo_empresa_e_socio(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181/grafo")
    data = response.json()
    tipos = {n["tipo"] for n in data["nos"]}
    assert "empresa" in tipos
    assert "socio" in tipos


def test_grafo_cnpj_invalido_retorna_422(client: TestClient) -> None:
    response = client.get("/api/fornecedores/00000000000000/grafo")
    assert response.status_code == 422


def test_grafo_cnpj_inexistente_retorna_404(client: TestClient) -> None:
    response = client.get("/api/fornecedores/99888777000100/grafo")
    assert response.status_code == 404
