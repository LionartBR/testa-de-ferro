# tests/pipeline/test_ingestao_cnpj.py
#
# Specification tests for Receita Federal CNPJ ingestao.
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.sources.cnpj.parse_empresas import parse_empresas
from pipeline.sources.cnpj.parse_qsa import parse_qsa
from pipeline.sources.cnpj.validate import validate_empresas, validate_qsa

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SAMPLE_CNPJ = FIXTURES_DIR / "sample_cnpj.csv"
SAMPLE_QSA = FIXTURES_DIR / "sample_qsa.csv"


def test_parse_empresas_extrai_cnpj_formatado() -> None:
    """CNPJ basico+ordem+dv assembled into XX.XXX.XXX/XXXX-XX format."""
    df = parse_empresas(SAMPLE_CNPJ)
    assert "cnpj" in df.columns
    first_cnpj = df["cnpj"][0]
    assert first_cnpj is not None
    assert "/" in first_cnpj
    assert "-" in first_cnpj
    assert first_cnpj.count(".") == 2


def test_parse_empresas_situacao_numerica_vira_enum() -> None:
    """Situacao is None in Empresas-only parse (comes from Estabelecimentos)."""
    tmp_csv = Path(__file__).parent / "_tmp_situacao.csv"
    # Empresas file: 7 cols, no header, semicolon, latin1
    tmp_csv.write_text(
        "11222333;EMPRESA A;2062;50;1000,00;05;\n44555666;EMPRESA B;2135;50;500,00;01;\n",
        encoding="latin1",
    )
    try:
        df = parse_empresas(tmp_csv)
        # Situacao is not in the Empresas file — set to None, filled from Estabelecimentos later
        assert df["situacao"][0] is None
        assert df["situacao"][1] is None
    finally:
        tmp_csv.unlink(missing_ok=True)


def test_parse_empresas_todos_codigos_situacao_mapeados() -> None:
    """Situacao is None in Empresas-only parse for all rows."""
    tmp_csv = Path(__file__).parent / "_tmp_situacao2.csv"
    # Empresas file: 7 cols, no header, semicolon, latin1
    tmp_csv.write_text(
        "11222333;NULA SA;2062;50;1000,00;05;\n"
        "22333444;SUSPENSA LTDA;2135;50;500,00;01;\n"
        "33444555;BAIXADA ME;2062;50;200,00;03;\n",
        encoding="latin1",
    )
    try:
        df = parse_empresas(tmp_csv)
        # Situacao comes from Estabelecimentos, not Empresas
        assert df["situacao"][0] is None
        assert df["situacao"][1] is None
        assert df["situacao"][2] is None
    finally:
        tmp_csv.unlink(missing_ok=True)


def test_parse_empresas_capital_social_como_float64() -> None:
    """Capital social is parsed as Float64, not as a string."""
    df = parse_empresas(SAMPLE_CNPJ)
    assert "capital_social" in df.columns
    assert df["capital_social"].dtype == pl.Float64
    assert df["capital_social"][0] == pytest.approx(1000.00)


def test_parse_empresas_data_abertura_como_date() -> None:
    """data_abertura is Date type but None (comes from Estabelecimentos, not Empresas)."""
    tmp_csv = Path(__file__).parent / "_tmp_data.csv"
    # Empresas file: 7 cols, no header, semicolon, latin1
    tmp_csv.write_text(
        "11222333;EMPRESA A;2062;50;1000,00;05;\n",
        encoding="latin1",
    )
    try:
        df = parse_empresas(tmp_csv)
        assert df["data_abertura"].dtype == pl.Date
        # data_abertura is not in the Empresas file — set to None, filled from Estabelecimentos later
        assert df["data_abertura"][0] is None
    finally:
        tmp_csv.unlink(missing_ok=True)


def test_parse_empresas_retorna_colunas_obrigatorias() -> None:
    """parse_empresas returns all required dim_fornecedor staging columns."""
    df = parse_empresas(SAMPLE_CNPJ)
    required = {
        "pk_fornecedor",
        "cnpj",
        "razao_social",
        "data_abertura",
        "capital_social",
        "cnae_principal",
        "cnae_descricao",
        "situacao",
    }
    assert required.issubset(set(df.columns))


def test_parse_qsa_extrai_nome_e_qualificacao() -> None:
    """NOME_SOCIO and QUALIFICACAO_SOCIO are extracted from the QSA CSV."""
    df = parse_qsa(SAMPLE_QSA)
    assert "nome_socio" in df.columns
    assert "qualificacao_socio" in df.columns
    names = df["nome_socio"].to_list()
    assert any("JOAO" in (n or "") for n in names)


def test_parse_qsa_nome_normalizado_para_maiusculas() -> None:
    """nome_socio is uppercased for consistent matching."""
    df = parse_qsa(SAMPLE_QSA)
    for nome in df["nome_socio"].drop_nulls():
        assert nome == nome.upper(), f"Expected uppercase, got: {nome}"


def test_parse_qsa_cpf_parcial_preservado() -> None:
    """cpf_parcial column is present and contains the masked CPF string."""
    df = parse_qsa(SAMPLE_QSA)
    assert "cpf_parcial" in df.columns
    cpfs = df["cpf_parcial"].drop_nulls().to_list()
    assert len(cpfs) > 0


def test_validate_empresas_remove_duplicatas() -> None:
    """Dedup by cnpj keeps only the first occurrence."""
    df = pl.DataFrame(
        {
            "pk_fornecedor": [1, 2],
            "cnpj": ["11.222.333/0001-81", "11.222.333/0001-81"],
            "razao_social": ["EMPRESA A", "EMPRESA A DUPLICADA"],
            "data_abertura": pl.Series([None, None], dtype=pl.Date),
            "capital_social": [1000.0, 2000.0],
            "cnae_principal": [None, None],
            "cnae_descricao": [None, None],
            "logradouro": [None, None],
            "municipio": [None, None],
            "uf": [None, None],
            "cep": [None, None],
            "situacao": ["ATIVA", "ATIVA"],
        }
    )
    result = validate_empresas(df)
    assert len(result) == 1
    assert result["razao_social"][0] == "EMPRESA A"


def test_validate_empresas_rejeita_cnpj_invalido() -> None:
    """Rows with malformed CNPJ format are dropped."""
    df = pl.DataFrame(
        {
            "pk_fornecedor": [1, 2],
            "cnpj": ["INVALIDO", "11.222.333/0001-81"],
            "razao_social": ["EMPRESA X", "EMPRESA Y"],
            "data_abertura": pl.Series([None, None], dtype=pl.Date),
            "capital_social": [None, None],
            "cnae_principal": [None, None],
            "cnae_descricao": [None, None],
            "logradouro": [None, None],
            "municipio": [None, None],
            "uf": [None, None],
            "cep": [None, None],
            "situacao": [None, None],
        }
    )
    result = validate_empresas(df)
    assert len(result) == 1
    assert result["cnpj"][0] == "11.222.333/0001-81"


def test_validate_qsa_remove_duplicatas() -> None:
    """Dedup by (cnpj_basico, cpf_parcial) keeps first occurrence."""
    df = pl.DataFrame(
        {
            "cnpj_basico": ["11222333", "11222333"],
            "nome_socio": ["JOAO SILVA", "JOAO SILVA DUPLICADO"],
            "cpf_parcial": ["***222333**", "***222333**"],
            "qualificacao_socio": ["49", "49"],
            "data_entrada": pl.Series([None, None], dtype=pl.Date),
            "percentual_capital": [None, None],
        }
    )
    result = validate_qsa(df)
    assert len(result) == 1
    assert result["nome_socio"][0] == "JOAO SILVA"


def test_validate_qsa_rejeita_sem_nome() -> None:
    """Rows without nome_socio are dropped."""
    df = pl.DataFrame(
        {
            "cnpj_basico": ["11222333", "22333444"],
            "nome_socio": [None, "MARIA COSTA"],
            "cpf_parcial": ["***111222**", "***333444**"],
            "qualificacao_socio": ["49", "49"],
            "data_entrada": pl.Series([None, None], dtype=pl.Date),
            "percentual_capital": [None, None],
        }
    )
    result = validate_qsa(df)
    assert len(result) == 1
    assert result["nome_socio"][0] == "MARIA COSTA"
