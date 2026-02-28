# pipeline/sources/rais/download.py
#
# IO-only: download the RAIS (Relação Anual de Informações Sociais) dataset
# from the MTE (Ministério do Trabalho e Emprego) and extract its CSV.
#
# Design decisions:
#   - RAIS is distributed as a single ZIP containing one or more CSVs.
#     We extract the first CSV found, matching the convention used by sancoes/.
#   - Files can be large (hundreds of MB). httpx streaming with 8 MB chunks
#     avoids loading the entire archive into memory.
#   - The download uses follow_redirects because the MTE URL may redirect to a
#     CDN mirror before serving the file.
#   - Not unit-tested — requires network access.
#
# Invariants:
#   - Returns the path to the extracted CSV, not the ZIP.
#   - raw_dir is created if it does not exist.
from __future__ import annotations

import zipfile
from pathlib import Path

import httpx

from pipeline.log import log


def download_rais(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download the RAIS ZIP and extract the CSV it contains.

    Args:
        url:     Full URL of the RAIS ZIP file (MTE distribution endpoint).
        raw_dir: Directory where raw files should be saved. Created if absent.
        timeout: HTTP timeout in seconds for the entire download.

    Returns:
        Absolute path to the extracted CSV file.

    Raises:
        httpx.HTTPError:   if the HTTP request fails.
        FileNotFoundError: if the ZIP contains no CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_name = url.rstrip("/").split("/")[-1]
    if not zip_name.lower().endswith(".zip"):
        zip_name = zip_name + ".zip"
    zip_path = raw_dir / zip_name

    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
        response.raise_for_status()
        downloaded = 0
        with zip_path.open("wb") as file_handle:
            for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                file_handle.write(chunk)
                downloaded += len(chunk)
                if downloaded % (50 * 1024 * 1024) < len(chunk):
                    log(f"  {zip_name}: {downloaded // (1024 * 1024)} MB downloaded...")

    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"No CSV found inside {zip_path}")
        archive.extract(csv_names[0], raw_dir)

    return raw_dir / csv_names[0]
