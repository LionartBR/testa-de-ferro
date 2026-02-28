# pipeline/sources/pncp/parse.py
#
# Parse PNCP data into a staging DataFrame for fato_contrato.
#
# Design decisions:
#   - Accepts either a directory of per-window Parquet files (streaming
#     download output) or a single JSON file (backward compat for tests).
#   - The Parquet path uses pl.scan_parquet for lazy, memory-efficient reads.
#   - The JSON path preserves the exact original behavior for test fixtures.
#   - Foreign keys are null placeholders resolved by the transform layer.
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from pipeline.sources.pncp._record import CONTRATOS_SCHEMA, extract_record


def parse_contratos(raw_path: Path) -> pl.DataFrame:
    """Parse PNCP data into a typed contratos DataFrame.

    Accepts either:
      - A directory of per-window Parquet files (from streaming download)
      - A JSON file (backward compat for tests using sample_pncp.json)

    Args:
        raw_path: Path to directory of Parquets or a single JSON file.

    Returns:
        Polars DataFrame with one row per contract, FK columns as nulls.
    """
    if raw_path.is_dir():
        return _parse_from_parquet_dir(raw_path)
    return _parse_from_json(raw_path)


def _parse_from_parquet_dir(dir_path: Path) -> pl.DataFrame:
    """Read and concatenate per-window Parquet files via lazy scan."""
    parquet_files = sorted(dir_path.glob("window_*.parquet"))
    if not parquet_files:
        return pl.DataFrame(schema=CONTRATOS_SCHEMA)

    lf = pl.scan_parquet([str(p) for p in parquet_files])
    return lf.collect()


def _parse_from_json(raw_path: Path) -> pl.DataFrame:
    """Parse from JSON file (backward compat for tests).

    Identical to the previous parse_contratos implementation.
    """
    text = raw_path.read_text(encoding="utf-8")
    payload: Any = json.loads(text)

    if isinstance(payload, dict):
        records: list[dict[str, Any]] = payload.get("data", [])
    else:
        records = list(payload)

    if not records:
        return pl.DataFrame(schema=CONTRATOS_SCHEMA)

    rows = [extract_record(r, i) for i, r in enumerate(records)]
    df = pl.DataFrame(rows)

    # Cast date columns from string ISO-8601 to Date type.
    for date_col in ("data_assinatura", "data_vigencia"):
        if date_col in df.columns and df[date_col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(date_col).str.strip_chars().str.to_date(format="%Y-%m-%d", strict=False).alias(date_col)
            )

    # Ensure valor is Float64.
    if "valor" in df.columns:
        df = df.with_columns(pl.col("valor").cast(pl.Float64, strict=False))

    return df
