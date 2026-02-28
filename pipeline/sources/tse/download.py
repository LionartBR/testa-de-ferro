# pipeline/sources/tse/download.py
#
# IO-only: download TSE prestação de contas ZIP and extract the CSV.
#
# Design decisions:
#   - TSE distributes campaign finance data as large ZIP files with a single
#     CSV inside. The extraction pattern is identical to the sanções download.
#   - Not unit-tested — requires network access.
from __future__ import annotations

import zipfile
from pathlib import Path

import httpx

from pipeline.log import log


def download_doacoes(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the TSE doacoes dataset.

    Args:
        url:     URL of the TSE doacoes ZIP file.
        raw_dir: Destination directory (created if absent).
        timeout: HTTP timeout in seconds.

    Returns:
        Path to the extracted CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_name = url.rstrip("/").split("/")[-1]
    if not zip_name.endswith(".zip"):
        zip_name += ".zip"
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
