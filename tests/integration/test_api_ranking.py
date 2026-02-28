# tests/integration/test_api_ranking.py
from fastapi.testclient import TestClient


def test_ranking_retorna_200_com_lista(client: TestClient) -> None:
    response = client.get("/api/fornecedores/ranking")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1


def test_ranking_ordenado_por_score_desc(client: TestClient) -> None:
    response = client.get("/api/fornecedores/ranking")
    data = response.json()
    scores = [item["score_risco"] for item in data]
    assert scores == sorted(scores, reverse=True)


def test_ranking_respeita_limit(client: TestClient) -> None:
    response = client.get("/api/fornecedores/ranking?limit=1")
    data = response.json()
    assert len(data) == 1


def test_ranking_limit_maximo_100(client: TestClient) -> None:
    response = client.get("/api/fornecedores/ranking?limit=200")
    assert response.status_code == 422


def test_ranking_offset(client: TestClient) -> None:
    all_response = client.get("/api/fornecedores/ranking?limit=100")
    offset_response = client.get("/api/fornecedores/ranking?limit=100&offset=1")
    all_data = all_response.json()
    offset_data = offset_response.json()
    assert len(offset_data) == len(all_data) - 1
