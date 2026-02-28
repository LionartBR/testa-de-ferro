# tests/integration/test_api_alertas.py
from fastapi.testclient import TestClient


def test_alertas_feed_retorna_200(client: TestClient) -> None:
    response = client.get("/api/alertas")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1


def test_alertas_contem_cnpj_e_razao_social(client: TestClient) -> None:
    response = client.get("/api/alertas")
    data = response.json()
    for alerta in data:
        assert "cnpj" in alerta
        assert "razao_social" in alerta
        assert "tipo" in alerta
        assert "severidade" in alerta


def test_alertas_por_tipo_valido(client: TestClient) -> None:
    response = client.get("/api/alertas/SOCIO_SERVIDOR_PUBLICO")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    for alerta in data:
        assert alerta["tipo"] == "SOCIO_SERVIDOR_PUBLICO"


def test_alertas_tipo_invalido_retorna_422(client: TestClient) -> None:
    response = client.get("/api/alertas/TIPO_INEXISTENTE")
    assert response.status_code == 422


def test_alertas_limit(client: TestClient) -> None:
    response = client.get("/api/alertas?limit=1")
    data = response.json()
    assert len(data) <= 1
