# pipeline/sources/pncp/download.py
#
# IO-only: paginate through the PNCP API and save contracts as per-window
# Parquet files.
#
# Design decisions:
#   - Each monthly date window is fetched in a separate thread and written
#     directly to a Parquet file inside a temporary directory.
#   - The temp directory is renamed atomically to the final path only after
#     ALL windows complete successfully. If any window fails, the temp
#     directory is deleted and the exception re-raised (all-or-nothing).
#   - This replaces the previous approach of accumulating ALL records in a
#     single Python list (~10 GB) before serializing to JSON. Peak memory
#     is now bounded to one window's data (~200 MB) instead of all windows.
#   - Per-window Parquet files are named window_{dataInicial}_{dataFinal}.parquet
#     so each thread writes to a unique file — no locking needed.
from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

from pipeline.log import log
from pipeline.sources.pncp._record import build_window_df

_PAGE_SIZE = 500
_MAX_WORKERS = 6


def download_pncp(
    base_url: str,
    raw_dir: Path,
    timeout: int = 60,
    max_pages_per_month: int = 1000,
    days_back: int = 365,
) -> Path:
    """Download contracts from the PNCP API as per-window Parquet files.

    Each monthly window is fetched in parallel and written to its own Parquet
    file. On success, the temp directory is renamed to the final path. On
    any error, the temp directory is deleted.

    Args:
        base_url:            PNCP API base URL.
        raw_dir:             Parent directory for output.
        timeout:             HTTP timeout per request in seconds.
        max_pages_per_month: Safety cap on pages per monthly window.
        days_back:           How many days of history to fetch.

    Returns:
        Path to the directory containing per-window Parquet files.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    windows_tmp = raw_dir / "pncp_windows_tmp"
    windows_final = raw_dir / "pncp_windows"

    # Skip download if final directory already exists with parquet files.
    if windows_final.exists() and any(windows_final.glob("*.parquet")):
        existing = list(windows_final.glob("*.parquet"))
        log(f"  PNCP: {len(existing)} window files already exist, skipping download")
        return windows_final

    # Clean stale temp dir from a previous crashed run.
    if windows_tmp.exists():
        shutil.rmtree(windows_tmp)
    windows_tmp.mkdir()

    # Build monthly date windows.
    end = date.today()
    start = end - timedelta(days=days_back)
    windows: list[tuple[str, str]] = []
    window_start = start
    while window_start < end:
        window_end = min(window_start + timedelta(days=30), end)
        windows.append(
            (
                window_start.strftime("%Y%m%d"),
                window_end.strftime("%Y%m%d"),
            )
        )
        window_start = window_end + timedelta(days=1)

    try:
        total = 0
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {
                pool.submit(
                    _fetch_and_save_window,
                    base_url,
                    di,
                    df,
                    timeout,
                    max_pages_per_month,
                    windows_tmp,
                ): (di, df)
                for di, df in windows
            }
            for future in as_completed(futures):
                di, df_date = futures[future]
                count = future.result()  # raises on error → except block
                total += count
                log(f"  PNCP {di}-{df_date}: {count} contratos")
    except Exception:
        shutil.rmtree(windows_tmp, ignore_errors=True)
        raise

    # Atomic swap: remove previous final dir, rename tmp → final.
    # shutil.rmtree is needed because Path.rename on Windows fails if
    # the destination already exists.
    if windows_final.exists():
        shutil.rmtree(windows_final)
    windows_tmp.rename(windows_final)
    log(f"  PNCP total: {total} contratos")
    return windows_final


def _fetch_and_save_window(
    base_url: str,
    data_inicial: str,
    data_final: str,
    timeout: int,
    max_pages: int,
    windows_dir: Path,
) -> int:
    """Fetch all pages for a date window, extract, and write as Parquet.

    Each thread writes to a unique filename (window_{di}_{df}.parquet),
    so no locking is needed.

    Returns:
        Number of records fetched for this window.
    """
    records: list[dict[str, Any]] = []
    page = 1

    while page <= max_pages:
        try:
            response = httpx.get(
                base_url,
                params={
                    "dataInicial": data_inicial,
                    "dataFinal": data_final,
                    "pagina": page,
                    "tamanhoPagina": _PAGE_SIZE,
                },
                timeout=timeout,
                follow_redirects=True,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError:
            break

        payload: dict[str, Any] = response.json()
        page_records: list[dict[str, Any]] = payload.get("data", [])
        if not page_records:
            break
        records.extend(page_records)

        total_pages = int(payload.get("totalPaginas", 1))
        if page % 10 == 0:
            log(f"  PNCP {data_inicial}-{data_final}: page {page}/{total_pages}")
        if page >= total_pages:
            break
        page += 1

    # Write this window's records as a single Parquet file.
    # records contains only ONE window (~500K records max, ~200 MB).
    # The list is GC'd when the function returns.
    if records:
        df = build_window_df(records)
        parquet_path = windows_dir / f"window_{data_inicial}_{data_final}.parquet"
        df.write_parquet(parquet_path)

    return len(records)
