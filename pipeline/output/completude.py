# pipeline/output/completude.py
#
# Completude validation: asserts that all required staging sources are present
# and non-empty before the DuckDB build begins.
#
# Design decisions:
#   - This is a pure guard function — it reads files but does not write or
#     modify anything. Raised exceptions are the only side effect.
#   - REQUIRED_SOURCES is a module-level tuple so it can be imported by tests
#     and the orchestrator without instantiating anything.
#   - The check is intentionally strict: a file with 0 rows is treated as
#     missing, because downstream SQL JOINs on an empty table would silently
#     produce an empty DuckDB (worse than an explicit failure).
#   - The error message always includes the offending source name so the
#     operator can pinpoint which source pipeline failed without reading logs.
from __future__ import annotations

from pathlib import Path

import polars as pl

# All staging files that must be present and non-empty before the DuckDB build.
#
# ADR: rais is included as a required source even though the MTE endpoint may
# occasionally be unavailable. If RAIS is missing, the SEM_FUNCIONARIOS
# indicator silently produces no rows (the indicator is guarded by a column
# existence check in score.py). Making it required forces the operator to
# investigate rather than silently skip a key indicator.
REQUIRED_SOURCES: tuple[str, ...] = (
    "empresas",
    "qsa",
    "contratos",
    "sancoes",
    "servidores",
    "doacoes",
    "rais",
)


class CompletudeError(Exception):
    """Raised when one or more required staging files are missing or empty.

    The error message always identifies the offending source file so the
    operator can immediately identify which pipeline step to re-run.
    """


def validar_completude(staging_dir: Path) -> None:
    """Assert that all required staging Parquet files exist and have rows.

    Args:
        staging_dir: Directory containing staging ``.parquet`` files, one per
            source (e.g. ``empresas.parquet``, ``contratos.parquet``).

    Raises:
        CompletudeError: if any required file is absent or contains zero rows.
            The message names the offending file.
    """
    for source in REQUIRED_SOURCES:
        path = staging_dir / f"{source}.parquet"

        if not path.exists():
            raise CompletudeError(f"Missing staging file: {source}.parquet (expected at {path})")

        row_count = pl.scan_parquet(path).select(pl.len()).collect().item()
        if row_count == 0:
            raise CompletudeError(
                f"Empty staging file: {source}.parquet (0 rows). Re-run the corresponding source pipeline."
            )
