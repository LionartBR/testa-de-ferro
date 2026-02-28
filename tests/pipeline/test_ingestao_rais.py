# tests/pipeline/test_ingestao_rais.py
#
# Specification tests for RAIS ingestao (parse + validate).
# Pure function tests — no IO, no network, no filesystem fixtures beyond
# in-memory DataFrames and a single small CSV written to a temp path.
from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import pytest

from pipeline.sources.rais.parse import parse_rais
from pipeline.sources.rais.validate import validate_rais

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_sample_csv(content: str) -> Path:
    """Write a UTF-8 CSV string to a temporary file and return its path."""
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        encoding="latin1",
        delete=False,
    ) as tmp:
        tmp.write(content)
        tmp.flush()
        return Path(tmp.name)


_SAMPLE_RAIS_CSV = "CNPJ_BASICO;QTD_VINC_ATIVOS;PORTE\n12345678;10;PEQUENA\n87654321;0;MICRO\n11223344;5;\n"

_SAMPLE_RAIS_CSV_DUPS = "CNPJ_BASICO;QTD_VINC_ATIVOS;PORTE\n12345678;3;MICRO\n12345678;10;PEQUENA\n99887766;7;MEDIA\n"

_SAMPLE_RAIS_CSV_NULL_CNPJ = "CNPJ_BASICO;QTD_VINC_ATIVOS;PORTE\n;20;GRANDE\n55667788;5;MICRO\n"

_SAMPLE_RAIS_CSV_NEGATIVE = (
    "CNPJ_BASICO;QTD_VINC_ATIVOS;PORTE\n12345678;-1;MICRO\n87654321;0;MICRO\n11111111;5;PEQUENA\n"
)


# ---------------------------------------------------------------------------
# parse_rais
# ---------------------------------------------------------------------------


def test_parse_rais_extrai_colunas_obrigatorias() -> None:
    """parse_rais extracts cnpj_basico, qtd_funcionarios, and porte_empresa."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)

    assert "cnpj_basico" in df.columns
    assert "qtd_funcionarios" in df.columns
    assert "porte_empresa" in df.columns


def test_parse_rais_conta_linhas_corretas() -> None:
    """parse_rais returns one row per data line (excluding header)."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)

    assert len(df) == 3


def test_parse_rais_qtd_funcionarios_como_int() -> None:
    """qtd_funcionarios is cast to an integer type."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)

    assert df["qtd_funcionarios"].dtype in (pl.Int32, pl.Int64)


def test_parse_rais_cnpj_basico_como_string() -> None:
    """cnpj_basico is stored as a string (raw 8-digit root)."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)

    assert df["cnpj_basico"].dtype == pl.Utf8


def test_parse_rais_porte_nulo_preservado() -> None:
    """porte_empresa may be null when the source row has an empty value."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)

    # Third row has empty PORTE — must be null, not an empty string.
    null_porte = df.filter(pl.col("porte_empresa").is_null())
    assert len(null_porte) >= 1


def test_parse_rais_valores_corretos() -> None:
    """parse_rais correctly maps the first data row to expected values."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)

    first = df.row(0, named=True)
    assert first["cnpj_basico"] == "12345678"
    assert first["qtd_funcionarios"] == 10
    assert first["porte_empresa"] == "PEQUENA"


# ---------------------------------------------------------------------------
# validate_rais
# ---------------------------------------------------------------------------


def test_validate_rais_remove_cnpj_basico_nulo() -> None:
    """Rows with null cnpj_basico are dropped."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV_NULL_CNPJ)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    assert all(v is not None for v in result["cnpj_basico"].to_list())
    assert len(result) == 1


def test_validate_rais_dedup_por_cnpj_basico_mantendo_max() -> None:
    """Dedup keeps the row with the highest qtd_funcionarios per cnpj_basico."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV_DUPS)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    # 12345678 appears twice (3 and 10 employees) — must keep the max (10).
    deduped = result.filter(pl.col("cnpj_basico") == "12345678")
    assert len(deduped) == 1
    assert deduped["qtd_funcionarios"][0] == 10


def test_validate_rais_preserva_cnpj_sem_duplicata() -> None:
    """CNPJs that appear only once are preserved unchanged."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV_DUPS)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    unico = result.filter(pl.col("cnpj_basico") == "99887766")
    assert len(unico) == 1
    assert unico["qtd_funcionarios"][0] == 7


def test_validate_rais_rejeita_qtd_negativa() -> None:
    """Rows with qtd_funcionarios < 0 are dropped."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV_NEGATIVE)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    # Only rows with qtd_funcionarios >= 0 survive.
    assert all(v >= 0 for v in result["qtd_funcionarios"].to_list())


def test_validate_rais_aceita_zero_funcionarios() -> None:
    """Rows with qtd_funcionarios == 0 are kept (zero is a valid value)."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV_NEGATIVE)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    zero_rows = result.filter(pl.col("qtd_funcionarios") == 0)
    assert len(zero_rows) == 1


def test_validate_rais_vazio_retorna_df_vazio() -> None:
    """An already-empty DataFrame returns an empty DataFrame without error."""
    empty_df = pl.DataFrame(
        {
            "cnpj_basico": pl.Series([], dtype=pl.Utf8),
            "qtd_funcionarios": pl.Series([], dtype=pl.Int64),
            "porte_empresa": pl.Series([], dtype=pl.Utf8),
        }
    )
    result = validate_rais(empty_df)

    assert result.is_empty()


def test_validate_rais_resultado_contem_colunas_esperadas() -> None:
    """Validated DataFrame preserves all three required columns."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    assert "cnpj_basico" in result.columns
    assert "qtd_funcionarios" in result.columns
    assert "porte_empresa" in result.columns


def test_validate_rais_dedup_e_filtragem_combinados() -> None:
    """Dedup and negative-filter work correctly together on the same dataset."""
    combined_csv = (
        "CNPJ_BASICO;QTD_VINC_ATIVOS;PORTE\n"
        "12345678;-5;MICRO\n"
        "12345678;8;PEQUENA\n"
        "12345678;3;PEQUENA\n"
        "99000000;0;MICRO\n"
    )
    csv_path = _write_sample_csv(combined_csv)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    # Negative row is dropped first; then dedup keeps max(8, 3) = 8.
    deduped = result.filter(pl.col("cnpj_basico") == "12345678")
    assert len(deduped) == 1
    assert deduped["qtd_funcionarios"][0] == 8

    # Zero-employee row must survive.
    assert len(result.filter(pl.col("cnpj_basico") == "99000000")) == 1


# ---------------------------------------------------------------------------
# Integration: parse then validate on the sample CSV
# ---------------------------------------------------------------------------


def test_parse_e_validate_rais_pipeline_completo() -> None:
    """Full parse → validate pipeline produces a clean, non-empty DataFrame."""
    csv_path = _write_sample_csv(_SAMPLE_RAIS_CSV)
    df = parse_rais(csv_path)
    result = validate_rais(df)

    # All three original rows survive (no dups, no nulls, no negatives).
    assert len(result) == 3
    assert result["qtd_funcionarios"].min() >= 0


# Make pytest aware this is a standard module
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
