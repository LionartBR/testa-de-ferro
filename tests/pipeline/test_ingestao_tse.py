# tests/pipeline/test_ingestao_tse.py
#
# Specification tests for TSE doacoes eleitorais ingestao.
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.sources.tse.parse import classificar_doador, parse_doacoes
from pipeline.sources.tse.validate import validate_doacoes

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SAMPLE_TSE = FIXTURES_DIR / "sample_tse.csv"


def test_classifica_cnpj_com_14_digitos() -> None:
    """A 14-digit document string is classified as CNPJ."""
    assert classificar_doador("11222333000181") == "CNPJ"
    assert classificar_doador("11.222.333/0001-81") == "CNPJ"


def test_classifica_cpf_com_11_digitos() -> None:
    """An 11-digit document string is classified as CPF."""
    assert classificar_doador("12345678901") == "CPF"
    assert classificar_doador("123.456.789-01") == "CPF"


def test_classifica_documento_invalido_retorna_none() -> None:
    """Documents with other digit counts return None."""
    assert classificar_doador("12345") is None
    assert classificar_doador("") is None
    assert classificar_doador(None) is None


def test_parse_doacoes_valor_como_float64() -> None:
    """valor is parsed as Float64."""
    df = parse_doacoes(SAMPLE_TSE)

    assert "valor" in df.columns
    assert df["valor"].dtype == pl.Float64
    # All values in the fixture should be positive
    valores = df["valor"].drop_nulls().to_list()
    assert all(v > 0 for v in valores)


def test_parse_doacoes_data_como_date() -> None:
    """data_receita is parsed as a Date type."""
    df = parse_doacoes(SAMPLE_TSE)

    assert "data_receita" in df.columns
    assert df["data_receita"].dtype == pl.Date


def test_parse_doacoes_tipo_doador_classificado() -> None:
    """tipo_doador is either CPF or CNPJ for all rows in the fixture."""
    df = parse_doacoes(SAMPLE_TSE)

    assert "tipo_doador" in df.columns
    tipos = df["tipo_doador"].drop_nulls().to_list()
    for tipo in tipos:
        assert tipo in ("CPF", "CNPJ"), f"Unexpected tipo_doador: {tipo}"


def test_validate_rejeita_valor_zero() -> None:
    """Rows with zero valor are dropped."""
    df = pl.DataFrame({
        "pk_doacao": [1, 2],
        "ano_eleicao": [2022, 2022],
        "doc_doador": ["11222333000181", "99887766554"],
        "tipo_doador": ["CNPJ", "CPF"],
        "cnpj_doador": ["11222333000181", None],
        "cpf_doador": [None, "99887766554"],
        "nome_doador": ["EMPRESA A", "FULANO"],
        "doc_candidato": ["12345678901", "12345678901"],
        "nome_candidato": ["CANDIDATO X", "CANDIDATO X"],
        "partido_candidato": ["PT", "PT"],
        "cargo_candidato": ["DEPUTADO", "DEPUTADO"],
        "uf_candidato": ["SP", "SP"],
        "tipo_recurso": ["Recursos proprios", "Recursos proprios"],
        "valor": [0.0, 5000.0],
        "data_receita": pl.Series([None, None], dtype=pl.Date),
        "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
        "fk_socio": pl.Series([None, None], dtype=pl.Int64),
        "fk_candidato": pl.Series([None, None], dtype=pl.Int64),
        "fk_tempo": pl.Series([None, None], dtype=pl.Int64),
    })
    result = validate_doacoes(df)

    assert len(result) == 1
    assert result["valor"][0] == pytest.approx(5000.0)


def test_validate_remove_duplicatas() -> None:
    """Dedup by (doc_doador, doc_candidato, ano_eleicao, valor) keeps first."""
    df = pl.DataFrame({
        "pk_doacao": [1, 2],
        "ano_eleicao": [2022, 2022],
        "doc_doador": ["11222333000181", "11222333000181"],
        "tipo_doador": ["CNPJ", "CNPJ"],
        "cnpj_doador": ["11222333000181", "11222333000181"],
        "cpf_doador": [None, None],
        "nome_doador": ["EMPRESA A", "EMPRESA A"],
        "doc_candidato": ["12345678901", "12345678901"],
        "nome_candidato": ["CANDIDATO X", "CANDIDATO X"],
        "partido_candidato": ["PT", "PT"],
        "cargo_candidato": ["DEPUTADO", "DEPUTADO"],
        "uf_candidato": ["SP", "SP"],
        "tipo_recurso": ["Recursos proprios", "Recursos proprios"],
        "valor": [10000.0, 10000.0],
        "data_receita": pl.Series([None, None], dtype=pl.Date),
        "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
        "fk_socio": pl.Series([None, None], dtype=pl.Int64),
        "fk_candidato": pl.Series([None, None], dtype=pl.Int64),
        "fk_tempo": pl.Series([None, None], dtype=pl.Int64),
    })
    result = validate_doacoes(df)

    assert len(result) == 1
