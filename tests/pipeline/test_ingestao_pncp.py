# tests/pipeline/test_ingestao_pncp.py
#
# Specification tests for PNCP contratos ingestao.
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.sources.pncp.parse import parse_contratos
from pipeline.sources.pncp.validate import validate_contratos

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SAMPLE_PNCP = FIXTURES_DIR / "sample_pncp.json"


def test_parse_pncp_extrai_campos_obrigatorios() -> None:
    """All required fields are extracted from the PNCP JSON fixture."""
    df = parse_contratos(SAMPLE_PNCP)

    assert "num_licitacao" in df.columns
    assert "cnpj_fornecedor" in df.columns
    assert "codigo_orgao" in df.columns
    assert "valor" in df.columns
    assert "objeto" in df.columns
    assert len(df) == 5  # fixture has 5 contracts


def test_parse_pncp_data_assinatura_como_date() -> None:
    """data_assinatura is parsed as a Date type, not a string."""
    df = parse_contratos(SAMPLE_PNCP)

    assert "data_assinatura" in df.columns
    assert df["data_assinatura"].dtype == pl.Date
    # First contract: 2024-03-15
    assert df["data_assinatura"][0] == datetime.date(2024, 3, 15)


def test_parse_pncp_valor_como_float64() -> None:
    """valor is parsed as Float64."""
    df = parse_contratos(SAMPLE_PNCP)

    assert df["valor"].dtype == pl.Float64
    assert df["valor"][0] == pytest.approx(150000.00)


def test_parse_pncp_fk_columns_sao_null() -> None:
    """FK columns are null placeholders at parse time."""
    df = parse_contratos(SAMPLE_PNCP)

    for fk_col in ("fk_fornecedor", "fk_orgao", "fk_tempo", "fk_modalidade"):
        assert fk_col in df.columns
        assert df[fk_col].is_null().all()


def test_validate_trunca_objeto_a_1000_chars() -> None:
    """objeto field is truncated to 1000 characters."""
    long_objeto = "X" * 1500
    df = pl.DataFrame({
        "pk_contrato": [1],
        "num_licitacao": ["C-001"],
        "cnpj_fornecedor": ["11.222.333/0001-81"],
        "codigo_orgao": ["00394460000141"],
        "nome_orgao": [None],
        "poder_orgao": [None],
        "esfera_orgao": [None],
        "modalidade_nome": [None],
        "modalidade_codigo": [None],
        "valor": [50000.0],
        "objeto": [long_objeto],
        "data_assinatura": pl.Series([None], dtype=pl.Date),
        "data_vigencia": pl.Series([None], dtype=pl.Date),
        "fk_fornecedor": pl.Series([None], dtype=pl.Int64),
        "fk_orgao": pl.Series([None], dtype=pl.Int64),
        "fk_tempo": pl.Series([None], dtype=pl.Int64),
        "fk_modalidade": pl.Series([None], dtype=pl.Int64),
    })
    result = validate_contratos(df)

    assert result["objeto"][0] is not None
    assert len(result["objeto"][0]) <= 1000


def test_validate_remove_duplicatas_por_num_licitacao() -> None:
    """Dedup by num_licitacao keeps first occurrence."""
    df = pl.DataFrame({
        "pk_contrato": [1, 2],
        "num_licitacao": ["C-DUP", "C-DUP"],
        "cnpj_fornecedor": ["11.222.333/0001-81", "11.222.333/0001-81"],
        "codigo_orgao": ["ORG1", "ORG1"],
        "nome_orgao": [None, None],
        "poder_orgao": [None, None],
        "esfera_orgao": [None, None],
        "modalidade_nome": [None, None],
        "modalidade_codigo": [None, None],
        "valor": [1000.0, 2000.0],
        "objeto": ["Objeto A", "Objeto B"],
        "data_assinatura": pl.Series([None, None], dtype=pl.Date),
        "data_vigencia": pl.Series([None, None], dtype=pl.Date),
        "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
        "fk_orgao": pl.Series([None, None], dtype=pl.Int64),
        "fk_tempo": pl.Series([None, None], dtype=pl.Int64),
        "fk_modalidade": pl.Series([None, None], dtype=pl.Int64),
    })
    result = validate_contratos(df)

    assert len(result) == 1
    assert result["objeto"][0] == "Objeto A"


def test_validate_rejeita_valor_negativo() -> None:
    """Rows with negative valor are dropped."""
    df = pl.DataFrame({
        "pk_contrato": [1, 2],
        "num_licitacao": ["C-001", "C-002"],
        "cnpj_fornecedor": ["11.222.333/0001-81", "22.333.444/0001-52"],
        "codigo_orgao": ["ORG1", "ORG2"],
        "nome_orgao": [None, None],
        "poder_orgao": [None, None],
        "esfera_orgao": [None, None],
        "modalidade_nome": [None, None],
        "modalidade_codigo": [None, None],
        "valor": [-500.0, 1000.0],
        "objeto": ["Objeto A", "Objeto B"],
        "data_assinatura": pl.Series([None, None], dtype=pl.Date),
        "data_vigencia": pl.Series([None, None], dtype=pl.Date),
        "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
        "fk_orgao": pl.Series([None, None], dtype=pl.Int64),
        "fk_tempo": pl.Series([None, None], dtype=pl.Int64),
        "fk_modalidade": pl.Series([None, None], dtype=pl.Int64),
    })
    result = validate_contratos(df)

    assert len(result) == 1
    assert result["valor"][0] == pytest.approx(1000.0)
