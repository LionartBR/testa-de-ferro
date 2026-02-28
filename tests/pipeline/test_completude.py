# tests/pipeline/test_completude.py
#
# Tests for the staging completude validator.
# All tests are pure: no mocks, no network, no external DB — only tmp_path.
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeline.output.completude import (
    REQUIRED_SOURCES,
    CompletudeError,
    validar_completude,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(directory: Path, name: str, rows: int) -> None:
    """Write a minimal parquet file with *rows* rows to directory/name.parquet."""
    directory.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({"id": list(range(rows)), "value": ["x"] * rows})
    df.write_parquet(directory / f"{name}.parquet")


def _create_full_staging(staging_dir: Path) -> None:
    """Populate staging_dir with all required source files, each with 2 rows."""
    for source in REQUIRED_SOURCES:
        _write_parquet(staging_dir, source, rows=2)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_completude_ok_with_all_sources(tmp_path: Path) -> None:
    """No error is raised when all required staging files are present with rows."""
    staging = tmp_path / "staging"
    _create_full_staging(staging)

    # Must not raise
    validar_completude(staging)


def test_completude_fails_missing_file(tmp_path: Path) -> None:
    """CompletudeError is raised when a required staging file is missing."""
    staging = tmp_path / "staging"
    _create_full_staging(staging)

    (staging / "contratos.parquet").unlink()

    with pytest.raises(CompletudeError, match="contratos.parquet"):
        validar_completude(staging)


def test_completude_fails_empty_file(tmp_path: Path) -> None:
    """CompletudeError is raised when a staging file exists but has zero rows."""
    staging = tmp_path / "staging"
    _create_full_staging(staging)

    # Overwrite sancoes with an empty DataFrame (schema doesn't matter for the check)
    empty = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
    empty.write_parquet(staging / "sancoes.parquet")

    with pytest.raises(CompletudeError, match="sancoes.parquet"):
        validar_completude(staging)


def test_completude_error_message_names_the_offending_source(tmp_path: Path) -> None:
    """CompletudeError message includes the name of the offending source."""
    staging = tmp_path / "staging"
    _create_full_staging(staging)
    (staging / "servidores.parquet").unlink()

    with pytest.raises(CompletudeError) as exc_info:
        validar_completude(staging)

    assert "servidores" in str(exc_info.value)
