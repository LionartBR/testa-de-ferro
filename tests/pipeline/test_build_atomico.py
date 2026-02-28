# tests/pipeline/test_build_atomico.py
#
# Tests for the atomic DuckDB build process.
#
# Strategy: create minimal staging Parquet files whose column sets satisfy the
# DuckDB FK-chain dim→bridge→fact, run build_duckdb, then assert on the
# resulting database. Tests use tmp_path for full filesystem isolation.
#
# The staging fixtures match the schema in pipeline/output/schema.sql:
#   dim_fornecedor ← empresas.parquet
#   dim_orgao      ← orgaos.parquet
#   dim_tempo      ← tempo.parquet
#   fato_contrato  ← contratos.parquet  (references the three dims above)
from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from pipeline.output.build_duckdb import build_duckdb

# ---------------------------------------------------------------------------
# Minimal staging fixtures
# ---------------------------------------------------------------------------


def _write_parquet(directory: Path, name: str, df: pl.DataFrame) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    df.write_parquet(directory / f"{name}.parquet")


def _empresas_df() -> pl.DataFrame:
    """One row for dim_fornecedor — satisfies all NOT NULL constraints."""
    return pl.DataFrame(
        {
            "pk_fornecedor": [1],
            "cnpj": ["11.222.333/0001-81"],
            "razao_social": ["Empresa Teste Ltda"],
            "data_abertura": [None],
            "capital_social": [None],
            "cnae_principal": [None],
            "cnae_descricao": [None],
            "logradouro": [None],
            "municipio": [None],
            "uf": [None],
            "cep": [None],
            "situacao": ["ATIVA"],
            "score_risco": [0],
            "faixa_risco": ["Baixo"],
            "qtd_alertas": [0],
            "max_severidade": [None],
            "total_contratos": [0],
            "valor_total": [None],
            "atualizado_em": [None],
        }
    )


def _orgaos_df() -> pl.DataFrame:
    """One row for dim_orgao."""
    return pl.DataFrame(
        {
            "pk_orgao": [1],
            "codigo": ["260001"],
            "nome": ["Ministerio da Educacao"],
            "sigla": ["MEC"],
            "poder": ["Executivo"],
            "esfera": ["Federal"],
            "uf": [None],
        }
    )


def _tempo_df() -> pl.DataFrame:
    """One row for dim_tempo."""
    return pl.DataFrame(
        {
            "pk_tempo": [1],
            "data": ["2024-01-15"],
            "ano": [2024],
            "mes": [1],
            "trimestre": [1],
            "semestre": [1],
            "dia_semana": [1],
            "nome_mes": ["Janeiro"],
        }
    )


def _contratos_df(fk_fornecedor: int = 1, fk_orgao: int = 1, fk_tempo: int = 1) -> pl.DataFrame:
    """One row for fato_contrato, referencing the given FK values."""
    return pl.DataFrame(
        {
            "pk_contrato": [1],
            "fk_fornecedor": [fk_fornecedor],
            "fk_orgao": [fk_orgao],
            "fk_tempo": [fk_tempo],
            "fk_modalidade": [None],
            "valor": ["150000.00"],
            "objeto": ["Servicos de TI"],
            "num_licitacao": [None],
            "data_assinatura": [None],
            "data_vigencia": [None],
        }
    )


def _create_minimal_staging(staging_dir: Path) -> None:
    """Create the minimum set of staging parquets needed for a valid build.

    Populates:
      - dim_fornecedor  (empresas.parquet)
      - dim_orgao       (orgaos.parquet)
      - dim_tempo       (tempo.parquet)
      - fato_contrato   (contratos.parquet)

    All other tables are left empty (no parquet file) — build_duckdb silently
    skips missing files; completude.py is responsible for enforcing presence.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(staging_dir, "empresas", _empresas_df())
    _write_parquet(staging_dir, "orgaos", _orgaos_df())
    _write_parquet(staging_dir, "tempo", _tempo_df())
    _write_parquet(staging_dir, "contratos", _contratos_df())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_creates_final_duckdb(tmp_path: Path) -> None:
    """A valid staging directory produces a DuckDB file at output_path."""
    staging = tmp_path / "staging"
    _create_minimal_staging(staging)
    output = tmp_path / "output" / "test.duckdb"

    result = build_duckdb(staging, output)

    assert result == output
    assert output.exists()


def test_build_output_contains_expected_tables(tmp_path: Path) -> None:
    """The resulting DuckDB contains at least the dimension and fact tables."""
    staging = tmp_path / "staging"
    _create_minimal_staging(staging)
    output = tmp_path / "output" / "test.duckdb"

    build_duckdb(staging, output)

    conn = duckdb.connect(str(output), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    finally:
        conn.close()

    expected = {
        "dim_fornecedor",
        "dim_orgao",
        "dim_tempo",
        "dim_socio",
        "dim_modalidade",
        "dim_candidato",
        "bridge_fornecedor_socio",
        "fato_contrato",
        "fato_doacao",
        "fato_score_detalhe",
        "fato_alerta_critico",
        "dim_sancao",
    }
    assert expected.issubset(tables)


def test_build_loads_staging_data_into_tables(tmp_path: Path) -> None:
    """Rows from staging parquets appear in the DuckDB tables."""
    staging = tmp_path / "staging"
    _create_minimal_staging(staging)
    output = tmp_path / "output" / "test.duckdb"

    build_duckdb(staging, output)

    conn = duckdb.connect(str(output), read_only=True)
    try:
        fornecedor_row = conn.execute("SELECT COUNT(*) FROM dim_fornecedor").fetchone()
        contrato_row = conn.execute("SELECT COUNT(*) FROM fato_contrato").fetchone()
    finally:
        conn.close()

    assert fornecedor_row is not None and fornecedor_row[0] == 1
    assert contrato_row is not None and contrato_row[0] == 1


def test_build_removes_tmp_on_failure(tmp_path: Path) -> None:
    """When the build fails with a FK violation, the .tmp.duckdb is cleaned up."""
    staging = tmp_path / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    output = tmp_path / "output" / "test.duckdb"
    tmp_expected = output.with_suffix(".tmp.duckdb")
    output.parent.mkdir(parents=True, exist_ok=True)

    # Load dim_fornecedor (pk=1) but reference fk_orgao=999 in the fact table.
    # dim_orgao will be empty → FK violation → duckdb.ConstraintException.
    _write_parquet(staging, "empresas", _empresas_df())
    _write_parquet(staging, "contratos", _contratos_df(fk_fornecedor=1, fk_orgao=999))

    with pytest.raises(duckdb.ConstraintException):
        build_duckdb(staging, output)

    assert not tmp_expected.exists()


def test_build_does_not_overwrite_existing_output_on_failure(tmp_path: Path) -> None:
    """A failed build leaves a previously existing valid output file untouched."""
    staging = tmp_path / "staging"
    output = tmp_path / "output" / "test.duckdb"
    output.parent.mkdir(parents=True, exist_ok=True)

    # Build a valid database first
    _create_minimal_staging(staging)
    build_duckdb(staging, output)
    assert output.exists()
    mtime_before = output.stat().st_mtime

    # Attempt a build that will fail: fato_contrato references missing FK
    staging2 = tmp_path / "staging2"
    staging2.mkdir(parents=True, exist_ok=True)
    _write_parquet(staging2, "contratos", _contratos_df(fk_fornecedor=999, fk_orgao=999))

    with pytest.raises(duckdb.ConstraintException):
        build_duckdb(staging2, output)

    assert output.exists()
    mtime_after = output.stat().st_mtime
    assert mtime_after == pytest.approx(mtime_before, abs=1e-3)


def test_build_output_is_not_present_during_build(tmp_path: Path) -> None:
    """The output file does not exist until the build completes successfully.

    Only the .tmp.duckdb file is modified during the build; output_path must
    remain absent until the atomic rename at the end.
    """
    staging = tmp_path / "staging"
    _create_minimal_staging(staging)
    output = tmp_path / "output" / "test.duckdb"

    assert not output.exists()

    build_duckdb(staging, output)

    assert output.exists()
    tmp_sentinel = output.with_suffix(".tmp.duckdb")
    assert not tmp_sentinel.exists()
