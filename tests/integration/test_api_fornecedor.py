# tests/integration/test_api_fornecedor.py
from fastapi.testclient import TestClient


def test_ficha_fornecedor_retorna_200(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181")
    assert response.status_code == 200
    data = response.json()
    assert data["cnpj"] == "11.222.333/0001-81"
    assert data["razao_social"] == "Empresa Teste LTDA"
    assert "alertas_criticos" in data
    assert "score" in data


def test_ficha_fornecedor_nao_encontrado_retorna_404(client: TestClient) -> None:
    response = client.get("/api/fornecedores/99888777000100")
    # CNPJ valido mas nao existe no banco de testes
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data
    assert "stack" not in data.get("detail", "")


def test_cnpj_invalido_retorna_422(client: TestClient) -> None:
    response = client.get("/api/fornecedores/00000000000000")
    assert response.status_code == 422


def test_cnpj_formato_errado_retorna_422(client: TestClient) -> None:
    response = client.get("/api/fornecedores/abc")
    assert response.status_code == 422


def test_headers_seguranca_presentes(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181")
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("X-Frame-Options") == "DENY"
    assert response.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"


def test_ficha_contem_socios(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181")
    data = response.json()
    assert "socios" in data
    assert len(data["socios"]) >= 1


def test_ficha_contem_sancoes(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181")
    data = response.json()
    assert "sancoes" in data


def test_ficha_contem_contratos(client: TestClient) -> None:
    response = client.get("/api/fornecedores/11222333000181")
    data = response.json()
    assert "contratos" in data
