# pipeline/sources/pncp/download.py
#
# IO-only: paginate through the PNCP API and save contracts to a JSON file.
#
# Design decisions:
#   - PNCP consulta API (v1) requires dataInicial and dataFinal parameters and
#     accepts tamanhoPagina up to 10. We fetch the last 365 days of contracts
#     in monthly chunks to stay within API limits.
#   - httpx is used with a configurable timeout per page request.
#   - The output is a single merged JSON file.
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx


def download_pncp(
    base_url: str,
    raw_dir: Path,
    timeout: int = 60,
    max_pages_per_month: int = 500,
    days_back: int = 365,
) -> Path:
    """Download contracts from the PNCP consulta API and save as one JSON file.

    Fetches contracts in monthly date windows over the last `days_back` days.

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

    all_records: list[dict[str, Any]] = []
    end = date.today()
    start = end - timedelta(days=days_back)

    # Iterate in monthly chunks
    window_start = start
    while window_start < end:
        window_end = min(window_start + timedelta(days=30), end)
        di = window_start.strftime("%Y%m%d")
        df = window_end.strftime("%Y%m%d")

        page = 1
        while page <= max_pages_per_month:
            try:
                response = httpx.get(
                    base_url,
                    params={
                        "dataInicial": di,
                        "dataFinal": df,
                        "pagina": page,
                        "tamanhoPagina": 10,
                    },
                    timeout=timeout,
                    follow_redirects=True,
                )
                response.raise_for_status()
            except httpx.HTTPStatusError:
                break

            payload: dict[str, Any] = response.json()
            records: list[dict[str, Any]] = payload.get("data", [])
            if not records:
                break
            all_records.extend(records)

            total_pages = int(payload.get("totalPaginas", 1))
            if page >= total_pages:
                break
            page += 1

        window_start = window_end + timedelta(days=1)

    merged = {"data": all_records, "totalRegistros": len(all_records)}
    output_path.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")

    return output_path
