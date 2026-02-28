# tests/pipeline/test_ingestao_sancoes.py
#
# Specification tests for CEIS/CNEP/CEPIM sancoes ingestao.
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

from pipeline.sources.sancoes.parse_ceis import parse_ceis
from pipeline.sources.sancoes.parse_cnep import parse_cnep
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


def test_parse_sancao_data_fim_null_significa_vigente() -> None:
    """data_fim null is preserved as null (vigente semantics)."""
    # The fixture has rows with empty DATA_FIM_SANCAO — they must remain null.
    df = parse_ceis(SAMPLE_SANCOES)

    null_data_fim = df.filter(pl.col("data_fim").is_null())
    assert len(null_data_fim) > 0, "Fixture must have at least one vigente sanction"


def test_parse_sancao_datas_como_date() -> None:
    """data_inicio is parsed as a Date type, not a string."""
    df = parse_ceis(SAMPLE_SANCOES)

    assert df["data_inicio"].dtype == pl.Date
    assert df["data_fim"].dtype == pl.Date


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
