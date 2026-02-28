# pipeline/sources/sancoes/download.py
#
# IO-only: download sanction ZIP files from Portal da Transparência.
#
# Design decisions:
#   - Portal da Transparência provides CEIS, CNEP, and CEPIM as separate ZIPs.
#     Each is downloaded and extracted independently. This module handles all
#     three with a shared helper.
#   - Files are large (~hundreds of MB). httpx streaming with 8MB chunks is
#     used to avoid memory pressure.
#   - The extracted CSV path is returned for use by the parse_* functions.
#   - Not unit-tested — requires network access.
from __future__ import annotations

import re
import zipfile
from pathlib import Path

import httpx

from pipeline.log import log

# ADR: Portal da Transparência download URLs require a date suffix
#
# The base URL (e.g. .../download-de-dados/cepim) returns an HTML page.
# The actual ZIP download requires appending the latest available date:
#   .../download-de-dados/cepim/20260226  →  302 → S3 ZIP
#
# The available date is NOT today — it lags by 1-3 days. Each dataset
# (CEIS, CNEP, CEPIM) may have a different latest date. We scrape the
# available date from the HTML page's embedded JS `arquivos` array.

_ARQUIVOS_RE = re.compile(r'"ano"\s*:\s*"(\d{4})"\s*,\s*"mes"\s*:\s*"(\d{2})"\s*,\s*"dia"\s*:\s*"(\d{2})"')


def _scrape_latest_date(page_url: str, timeout: int) -> str:
    """Scrape the latest available YYYYMMDD date from a Portal page.

    The page embeds a JS array like:
        arquivos.push({"ano":"2026","mes":"02","dia":"26","origem":"CEPIM"});
    We parse all entries and return the most recent date string.
    """
    resp = httpx.get(page_url, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    matches = _ARQUIVOS_RE.findall(resp.text)
    if not matches:
        raise RuntimeError(
            f"Could not find available dates on {page_url}. The Portal da Transparência page format may have changed."
        )
    latest = max(matches)
    return f"{latest[0]}{latest[1]}{latest[2]}"


def _download_and_extract(url: str, raw_dir: Path, timeout: int) -> Path:
    """Download a ZIP file and extract its first CSV.

    For Portal da Transparência URLs (no date suffix), the latest available
    date is scraped from the page and appended automatically.

    Args:
        url:     Base URL of the dataset page (date suffix auto-resolved).
        raw_dir: Destination directory (created if absent).
        timeout: HTTP timeout in seconds.

    Returns:
        Path to the extracted CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    stripped = url.rstrip("/")
    last_segment = stripped.split("/")[-1]
    if last_segment.isdigit() and len(last_segment) == 8:
        resolved = stripped
    else:
        date_suffix = _scrape_latest_date(stripped, timeout)
        resolved = f"{stripped}/{date_suffix}"
        log(f"  Resolved {last_segment} -> {date_suffix}")

    zip_name = stripped.split("/")[-1] + ".zip"
    zip_path = raw_dir / zip_name

    if not zip_path.exists():
        with httpx.stream("GET", resolved, timeout=timeout, follow_redirects=True) as resp:
            resp.raise_for_status()
            downloaded = 0
            with zip_path.open("wb") as fh:
                for chunk in resp.iter_bytes(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (50 * 1024 * 1024) < len(chunk):
                        log(f"  {zip_name}: {downloaded // (1024 * 1024)} MB downloaded...")
    else:
        log(f"  {zip_name}: already exists, skipping download")

    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"No CSV found inside {zip_path}")
        archive.extract(csv_names[0], raw_dir)

    return raw_dir / csv_names[0]


def download_ceis(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the CEIS dataset."""
    return _download_and_extract(url, raw_dir, timeout)


def download_cnep(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the CNEP dataset."""
    return _download_and_extract(url, raw_dir, timeout)


def download_cepim(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the CEPIM dataset."""
    return _download_and_extract(url, raw_dir, timeout)
