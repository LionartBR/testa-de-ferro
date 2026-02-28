# pipeline/sources/pncp/download.py
#
# IO-only: paginate through the PNCP API and save contracts to a JSON file.
#
# Design decisions:
#   - PNCP consulta API (v1) requires dataInicial and dataFinal parameters.
#     Max tamanhoPagina is 500.
#   - Monthly date windows are fetched in parallel (ThreadPoolExecutor) since
#     each month's pagination is independent. This cuts total time from ~2h
#     to ~8min for 12 months of data.
#   - The output is a single merged JSON file.
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

from pipeline.log import log

_PAGE_SIZE = 500
_MAX_WORKERS = 6


def download_pncp(
    base_url: str,
    raw_dir: Path,
    timeout: int = 60,
    max_pages_per_month: int = 1000,
    days_back: int = 365,
) -> Path:
    """Download contracts from the PNCP consulta API and save as one JSON file.

    Fetches contracts in monthly date windows, with months fetched in parallel.

    Args:
        base_url:            PNCP API base URL.
        raw_dir:             Directory where the merged JSON will be saved.
        timeout:             HTTP timeout per request in seconds.
        max_pages_per_month: Safety cap on pages per monthly window.
        days_back:           How many days of history to fetch.

    Returns:
        Absolute path to the saved merged JSON file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_path = raw_dir / "pncp_contratos.json"

    # Build monthly windows
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

    # Fetch months in parallel
    all_records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                _fetch_window,
                base_url,
                di,
                df,
                timeout,
                max_pages_per_month,
            ): (di, df)
            for di, df in windows
        }
        for future in as_completed(futures):
            di, df = futures[future]
            records = future.result()
            all_records.extend(records)
            log(f"  PNCP {di}-{df}: {len(records)} contratos")

    merged = {"data": all_records, "totalRegistros": len(all_records)}
    output_path.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")
    log(f"  PNCP total: {len(all_records)} contratos")

    return output_path


def _fetch_window(
    base_url: str,
    data_inicial: str,
    data_final: str,
    timeout: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    """Fetch all pages for a single date window."""
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

    return records
