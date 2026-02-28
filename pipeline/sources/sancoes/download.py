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

import zipfile
from pathlib import Path

import httpx

from pipeline.log import log


def _download_and_extract(url: str, raw_dir: Path, timeout: int) -> Path:
    """Download a ZIP file and extract its first CSV.

    Args:
        url:     URL of the ZIP file.
        raw_dir: Destination directory (created if absent).
        timeout: HTTP timeout in seconds.

    Returns:
        Path to the extracted CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_name = url.rstrip("/").split("/")[-1] + ".zip"
    zip_path = raw_dir / zip_name

    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as resp:
        resp.raise_for_status()
        downloaded = 0
        with zip_path.open("wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
                if downloaded % (50 * 1024 * 1024) < len(chunk):
                    log(f"  {zip_name}: {downloaded // (1024 * 1024)} MB downloaded...")

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
