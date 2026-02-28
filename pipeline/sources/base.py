# pipeline/sources/base.py
#
# Protocol definition for all pipeline source implementations.
#
# Design decisions:
#   - Uses typing.Protocol (structural subtyping) rather than ABC so that
#     concrete source classes don't need to inherit from a base — they just
#     need to expose the right interface. This keeps sources decoupled.
#   - runtime_checkable is set so the orchestrator (main.py) can use
#     isinstance() guards defensively when iterating over sources.
#   - The three methods follow the same immutable data-flow pattern:
#       download  — side-effectful: fetches bytes, writes to disk.
#       parse     — pure transformation: raw bytes → structured DataFrame.
#       validate  — pure cleaning: DataFrame in, clean DataFrame out.
#     This separation makes each step individually testable.
#   - `name` is a plain str attribute (not a method) so callers can read it
#     without invoking the object.
from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class SourcePipeline(Protocol):
    """Contract for pipeline source implementations.

    Implementors must expose one attribute and three methods:

        name        — human-readable identifier used in logging and completude checks.
        download    — fetches raw data from the external source into raw_dir.
        parse       — parses the raw file into a typed Polars DataFrame.
        validate    — cleans and deduplicates the DataFrame; rejects invalid rows.

    Invariant: parse(raw_path) must be callable with the exact path returned by
    download(raw_dir), and validate(df) must accept the DataFrame returned by parse.
    """

    name: str

    def download(self, raw_dir: Path) -> Path:
        """Download raw data to raw_dir.

        Args:
            raw_dir: Directory where raw files should be saved. May not exist yet;
                the implementation is responsible for creating it.

        Returns:
            Absolute path to the downloaded file (or directory, for multi-file
            sources).
        """
        ...

    def parse(self, raw_path: Path) -> pl.DataFrame:
        """Parse a raw file into a clean, typed DataFrame.

        Args:
            raw_path: Path returned by download().

        Returns:
            DataFrame with typed columns. Column names and types are source-specific
            but must match the staging schema expected by the DuckDB loader.
        """
        ...

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate, deduplicate, and clean a parsed DataFrame.

        Args:
            df: DataFrame returned by parse().

        Returns:
            Cleaned DataFrame. Invalid rows are silently dropped (counter them
            via logging inside the implementation). The result must never be empty
            unless the source genuinely has no data.
        """
        ...
