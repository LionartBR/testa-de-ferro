# pipeline/sources/pncp/download.py
#
# IO-only: paginate through the PNCP API and save contracts to a JSON file.
#
# Design decisions:
#   - PNCP uses cursor-based pagination via "pagina" and "totalPaginas" fields.
#     All pages are fetched and their "data" arrays are merged into a single
#     JSON file containing {"data": [...], "totalRegistros": N}.
#   - httpx is used with a configurable timeout per page request. Network
#     errors on individual pages propagate immediately — retry is the caller's
#     responsibility.
#   - The output is a single merged JSON file so parse_contratos works on one
#     path regardless of how many pages were fetched.
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx


def download_pncp(
    base_url: str,
    raw_dir: Path,
    timeout: int = 60,
    max_pages: int = 1000,
) -> Path:
    """Download all contract pages from the PNCP API and save as one JSON file.

    Args:
        base_url:  PNCP API base URL (without pagination params).
        raw_dir:   Directory where the merged JSON will be saved.
        timeout:   HTTP timeout per request in seconds.
        max_pages: Safety cap on the number of pages to fetch.

    Returns:
        Absolute path to the saved merged JSON file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_path = raw_dir / "pncp_contratos.json"

    all_records: list[dict[str, Any]] = []
    page = 1

    while page <= max_pages:
        response = httpx.get(
            base_url,
            params={"pagina": page, "tamanhoPagina": 500},
            timeout=timeout,
            follow_redirects=True,
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json()

        records: list[dict[str, Any]] = payload.get("data", [])
        all_records.extend(records)

        total_pages = int(payload.get("totalPaginas", 1))
        if page >= total_pages:
            break
        page += 1

    merged = {"data": all_records, "totalRegistros": len(all_records)}
    output_path.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")

    return output_path
