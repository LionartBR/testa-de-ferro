# tests/pipeline/test_ingestao_sancoes.py
#
# Specification tests for CEIS/CNEP/CEPIM sancoes ingestao.
#
# The fixture sample_sancoes.csv uses the real Portal da Transparência format:
#   - Semicolon-separated, Latin-1 encoding
#   - Column names with accents and spaces (e.g. "CPF OU CNPJ DO SANCIONADO")
#   - Dates in DD/MM/YYYY format
#   - Values wrapped in double quotes
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

from pipeline.sources.sancoes.parse_ceis import parse_ceis
from pipeline.sources.sancoes.parse_cnep import parse_cnep
from pipeline.sources.sancoes.parse_cepim import parse_cepim
from pipeline.sources.sancoes.validate import validate_sancoes

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SAMPLE_SANCOES = FIXTURES_DIR / "sample_sancoes.csv"


def test_parse_ceis_extrai_campos_obrigatorios() -> None:
    """CNPJ, tipo_sancao, orgao_sancionador, data_inicio extracted from CEIS CSV."""
    df = parse_ceis(SAMPLE_SANCOES)

    assert "cnpj" in df.columns
    assert "tipo_sancao" in df.columns
    assert "orgao_sancionador" in df.columns
    assert "data_inicio" in df.columns
    assert "data_fim" in df.columns
    assert len(df) > 0


def test_parse_ceis_tipo_sancao_fixo() -> None:
    """tipo_sancao is always CEIS when parsed by parse_ceis."""
    df = parse_ceis(SAMPLE_SANCOES)
    assert all(t == "CEIS" for t in df["tipo_sancao"].to_list())


def test_parse_cnep_tipo_correto() -> None:
    """tipo_sancao is always CNEP when parsed by parse_cnep."""
    df = parse_cnep(SAMPLE_SANCOES)
    assert all(t == "CNEP" for t in df["tipo_sancao"].to_list())


def test_parse_cepim_tipo_correto() -> None:
    """tipo_sancao is always CEPIM when parsed by parse_cepim."""
    df = parse_cepim(SAMPLE_SANCOES)
    assert all(t == "CEPIM" for t in df["tipo_sancao"].to_list())


def test_parse_sancao_data_fim_null_significa_vigente() -> None:
    """data_fim null is preserved as null (vigente semantics)."""
    # The fixture has rows with empty DATA FINAL SANÇÃO — they must remain null.
    df = parse_ceis(SAMPLE_SANCOES)

    null_data_fim = df.filter(pl.col("data_fim").is_null())
    assert len(null_data_fim) > 0, "Fixture must have at least one vigente sanction"


def test_parse_sancao_datas_como_date() -> None:
    """data_inicio is parsed as a Date type, not a string."""
    df = parse_ceis(SAMPLE_SANCOES)

    assert df["data_inicio"].dtype == pl.Date
    assert df["data_fim"].dtype == pl.Date


def test_parse_ceis_mapeia_cnpj_corretamente() -> None:
    """CNPJ is extracted from 'CPF OU CNPJ DO SANCIONADO' column."""
    df = parse_ceis(SAMPLE_SANCOES)

    cnpjs = df["cnpj"].to_list()
    assert "11.222.333/0001-81" in cnpjs


def test_parse_ceis_mapeia_orgao_sancionador_corretamente() -> None:
    """orgao_sancionador is extracted from 'ORGAO SANCIONADOR' column."""
    df = parse_ceis(SAMPLE_SANCOES)

    orgaos = df["orgao_sancionador"].to_list()
    assert "MINISTERIO DA TRANSPARENCIA" in orgaos


def test_parse_ceis_mapeia_motivo_corretamente() -> None:
    """motivo is extracted from 'CATEGORIA DA SANCAO' column."""
    df = parse_ceis(SAMPLE_SANCOES)

    motivos = df["motivo"].to_list()
    assert "Fraude em licitacao" in motivos


def test_parse_ceis_data_inicio_formato_ddmmyyyy() -> None:
    """data_inicio is correctly parsed from DD/MM/YYYY format."""
    df = parse_ceis(SAMPLE_SANCOES)

    # First row: "15/01/2023" should parse to 2023-01-15
    datas = df["data_inicio"].to_list()
    assert datetime.date(2023, 1, 15) in datas


def test_parse_ceis_data_fim_formato_ddmmyyyy() -> None:
    """data_fim is correctly parsed from DD/MM/YYYY format when present."""
    df = parse_ceis(SAMPLE_SANCOES)

    # Second row: "01/06/2023" should parse to 2023-06-01
    non_null_dates = df.filter(pl.col("data_fim").is_not_null())["data_fim"].to_list()
    assert datetime.date(2023, 6, 1) in non_null_dates


def test_parse_ceis_razao_social_da_receita() -> None:
    """razao_social prefers RAZAO SOCIAL - CADASTRO RECEITA column."""
    df = parse_ceis(SAMPLE_SANCOES)

    razoes = df["razao_social"].to_list()
    assert "EMPRESA TESTE LTDA" in razoes


def test_validate_remove_duplicatas() -> None:
    """Dedup by (cnpj, tipo_sancao, data_inicio) keeps first occurrence."""
    df = pl.DataFrame(
        {
            "pk_sancao": [1, 2],
            "cnpj": ["11.222.333/0001-81", "11.222.333/0001-81"],
            "razao_social": ["EMPRESA A", "EMPRESA A"],
            "tipo_sancao": ["CEIS", "CEIS"],
            "orgao_sancionador": ["CGU", "CGU"],
            "motivo": ["Fraude", "Fraude"],
            "data_inicio": pl.Series([datetime.date(2023, 1, 1), datetime.date(2023, 1, 1)], dtype=pl.Date),
            "data_fim": pl.Series([None, None], dtype=pl.Date),
            "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    result = validate_sancoes(df)

    assert len(result) == 1


def test_validate_rejeita_sem_cnpj() -> None:
    """Rows without CNPJ are dropped."""
    df = pl.DataFrame(
        {
            "pk_sancao": [1, 2],
            "cnpj": [None, "11.222.333/0001-81"],
            "razao_social": ["EMPRESA X", "EMPRESA Y"],
            "tipo_sancao": ["CEIS", "CEIS"],
            "orgao_sancionador": ["CGU", "CGU"],
            "motivo": [None, None],
            "data_inicio": pl.Series([datetime.date(2023, 1, 1), datetime.date(2023, 2, 1)], dtype=pl.Date),
            "data_fim": pl.Series([None, None], dtype=pl.Date),
            "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    result = validate_sancoes(df)

    assert len(result) == 1
    assert result["cnpj"][0] == "11.222.333/0001-81"


def test_validate_preserva_dados_apos_parse_com_formato_real() -> None:
    """Full integration: parse real-format CSV then validate preserves valid rows."""
    df = parse_ceis(SAMPLE_SANCOES)
    result = validate_sancoes(df)

    # All rows in the fixture have CNPJ and data_inicio, so none should be dropped.
    assert len(result) == len(df)
    assert result["data_inicio"].null_count() == 0
    assert result["cnpj"].null_count() == 0
