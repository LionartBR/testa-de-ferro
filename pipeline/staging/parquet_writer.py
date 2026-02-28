# pipeline/staging/parquet_writer.py
#
# Standardised Parquet read/write for staging data.
#
# Design decisions:
#   - Functions are intentionally thin wrappers around Polars I/O so that the
#     rest of the pipeline never calls polars directly for file I/O, keeping
#     the storage format swappable.
#   - write_parquet creates parent directories automatically so callers don't
#     need to worry about directory existence.
#   - Both functions are pure with respect to application state: they have no
#     global side effects beyond the filesystem.
#   - No schema enforcement is done here — enforcement happens in the validate()
#     step of each SourcePipeline. The writer's job is only persistence.
from __future__ import annotations

from pathlib import Path

import polars as pl


def write_parquet(df: pl.DataFrame, path: Path) -> Path:
    """Write a DataFrame to a Parquet file, creating parent directories as needed.

    Args:
        df:   DataFrame to persist. May have any schema.
        path: Destination file path. Does not need to exist; parent directories
              are created automatically.

    Returns:
        The resolved absolute path where the file was written (same as ``path``
        after resolution, returned for call-chain convenience).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def read_parquet(path: Path) -> pl.DataFrame:
    """Read a Parquet file into a DataFrame.

    Args:
        path: Path to the Parquet file. Must exist.

    Returns:
        DataFrame with the schema stored in the file.

    Raises:
        FileNotFoundError: if ``path`` does not exist (raised by Polars).
    """
    return pl.read_parquet(path)
