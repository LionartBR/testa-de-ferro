# pipeline/sources/servidores/download.py
#
# IO-only: download servidores CSV from Portal da Transparência.
#
# Design decisions:
#   - Portal da Transparência provides servidores data as a ZIP. The extraction
#     logic is identical to the sanções download helper.
#   - Not unit-tested — requires network access.
from __future__ import annotations

import zipfile
from pathlib import Path

import httpx

from pipeline.log import log


def download_servidores(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the servidores dataset.

    Args:
        url:     URL of the servidores ZIP file.
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
