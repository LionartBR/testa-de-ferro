# tests/integration/test_api_export.py
from fastapi.testclient import TestClient


def test_export_json_retorna_200(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181/export?formato=json")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    data = response.json()
    assert data["cnpj"] == "11.222.333/0001-81"


def test_export_csv_retorna_text_csv(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181/export?formato=csv")
    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "CNPJ" in response.text
    assert "11.222.333/0001-81" in response.text


def test_export_pdf_retorna_501(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181/export?formato=pdf")
    assert response.status_code == 501


def test_export_formato_invalido_retorna_422(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181/export?formato=xml")
    assert response.status_code == 422
