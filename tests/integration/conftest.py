# tests/integration/conftest.py
from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

SCHEMA_PATH = Path(__file__).parent.parent.parent / "pipeline" / "output" / "schema.sql"

# Desabilitar rate limit em testes
os.environ["API_RATE_LIMIT_PER_MINUTE"] = "0"


@pytest.fixture(scope="session")
def test_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Cria DuckDB in-memory com schema e dados deterministicos."""
    conn = duckdb.connect(":memory:")
    conn.execute(SCHEMA_PATH.read_text(encoding="utf-8"))

    # --- Fornecedores ---
    conn.execute("""
        INSERT INTO dim_fornecedor VALUES
        (1, '11.222.333/0001-81', 'Empresa Teste LTDA', '2023-01-15',
         1000.00, '6201-5', 'Desenvolvimento de programas',
         'Rua das Flores, 100', 'Sao Paulo', 'SP', '01001-000',
         'ATIVA', 25, 'Moderado', 2, 'GRAVISSIMO', 3, 750000.00,
         CURRENT_TIMESTAMP),
        (2, '33.000.167/0001-01', 'Empresa Limpa SA', '2010-06-01',
         5000000.00, '4711-3', 'Comercio varejista',
         'Av. Paulista, 1000', 'Sao Paulo', 'SP', '01310-100',
         'ATIVA', 0, 'Baixo', 0, NULL, 5, 2000000.00,
         CURRENT_TIMESTAMP)
    """)

    # --- Socios ---
    conn.execute("""
        INSERT INTO dim_socio VALUES
        (1, 'hmac_abc123', 'Joao da Silva', 'Socio-Administrador',
         TRUE, 'Ministerio da Saude', FALSE, 3),
        (2, 'hmac_def456', 'Maria Santos', 'Socia',
         FALSE, NULL, FALSE, 1)
    """)

    # --- Bridge Fornecedor-Socio ---
    conn.execute("""
        INSERT INTO bridge_fornecedor_socio VALUES
        (1, 1, '2023-01-15', NULL, 60.00),
        (1, 2, '2023-01-15', NULL, 40.00),
        (2, 2, '2010-06-01', NULL, 100.00)
    """)

    # --- Orgaos ---
    conn.execute("""
        INSERT INTO dim_orgao VALUES
        (1, '26000', 'Ministerio da Educacao', 'MEC', 'Executivo', 'Federal', 'DF'),
        (2, '54000', 'Ministerio da Fazenda', 'MF', 'Executivo', 'Federal', 'DF')
    """)

    # --- Tempo ---
    conn.execute("""
        INSERT INTO dim_tempo VALUES
        (1, '2025-06-01', 2025, 6, 2, 1, 1, 'Junho')
    """)

    # --- Candidato (para futura doacao) ---
    conn.execute("""
        INSERT INTO dim_candidato VALUES
        (1, 'Dep. Fulano', 'hmac_candidato1', 'PXX', 'Deputado Federal', 'SP', 2022)
    """)

    # --- Contratos ---
    conn.execute("""
        INSERT INTO fato_contrato VALUES
        (1, 1, 1, 1, NULL, 250000.00, 'Servicos de TI', 'PE-001/2025', '2025-06-01', '2026-06-01'),
        (2, 1, 1, 1, NULL, 500000.00, 'Manutencao sistemas', 'PE-002/2025', '2025-06-15', '2026-06-15'),
        (3, 2, 2, 1, NULL, 1000000.00, 'Compra de material', 'PE-003/2025', '2025-06-20', '2026-06-20'),
        (4, 2, 2, 1, NULL, 500000.00, 'Compra de equipamento', 'PE-003/2025', '2025-07-01', '2026-07-01'),
        (5, 2, 1, 1, NULL, 500000.00, 'Servicos gerais', 'PE-004/2025', '2025-08-01', '2026-08-01')
    """)

    # --- Doacao ---
    conn.execute("""
        INSERT INTO fato_doacao VALUES
        (1, 1, NULL, 1, 1, 15000.00, 'Transferencia', 2022)
    """)

    # --- Sancoes (1 vigente, 1 expirada) ---
    conn.execute("""
        INSERT INTO dim_sancao VALUES
        (1, 1, 'CEIS', 'CGU', 'Fraude em licitacao', '2024-01-01', NULL),
        (2, 1, 'CNEP', 'TCU', 'Irregularidade contabil', '2018-01-01', '2020-12-31')
    """)

    # --- Alertas pre-computados ---
    conn.execute("""
        INSERT INTO fato_alerta_critico VALUES
        (1, 1, 1, 'SOCIO_SERVIDOR_PUBLICO', 'GRAVISSIMO',
         'Socio Joao da Silva e servidor do Ministerio da Saude',
         'socio_cpf_hmac=hmac_abc123, orgao=Ministerio da Saude',
         CURRENT_TIMESTAMP),
        (2, 1, NULL, 'EMPRESA_SANCIONADA_CONTRATANDO', 'GRAVISSIMO',
         'Empresa com sancao CEIS vigente e contratos ativos',
         'sancao_tipo=CEIS, qtd_contratos=2',
         CURRENT_TIMESTAMP)
    """)

    # --- Score detalhe ---
    conn.execute("""
        INSERT INTO fato_score_detalhe VALUES
        (1, 1, 'CAPITAL_SOCIAL_BAIXO', 15, 'Capital desproporcional', 'capital=1000', CURRENT_TIMESTAMP)
    """)

    yield conn
    conn.close()


@pytest.fixture(scope="session")
def client(test_db: duckdb.DuckDBPyConnection) -> Generator[TestClient, None, None]:
    """TestClient do FastAPI com DuckDB in-memory injetado."""
    from api.infrastructure import duckdb_connection
    duckdb_connection.set_connection(test_db)

    # Limpar cache de settings para pegar API_RATE_LIMIT_PER_MINUTE=0
    from api.infrastructure.config import get_settings
    get_settings.cache_clear()

    from api.interfaces.api.main import app
    with TestClient(app) as c:
        yield c
