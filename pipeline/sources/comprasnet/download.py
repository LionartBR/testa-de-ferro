# pipeline/sources/comprasnet/download.py
#
# IO-only: download Comprasnet (SIASG) contract CSV files from the
# repositorio.dados.gov.br portal and save them to the raw directory.
#
# Design decisions:
#   - Comprasnet files are served as direct CSV downloads from
#     repositorio.dados.gov.br — no ZIP wrapping, unlike the Receita
#     Federal sources. The file is written directly without extraction.
#   - httpx streaming with 8 MB chunks avoids loading large files into memory.
#     CSVs from SIASG can reach several hundred MB.
#   - follow_redirects is enabled because the portal may use HTTP redirects
#     to route requests to storage backends.
#   - The filename is derived from the URL path. If the URL carries no
#     extension, ".csv" is appended so downstream parsers can rely on the
#     suffix.
#   - Cache: if the CSV already exists on disk, the download is skipped.
#     Delete the file manually to force a re-download.
#   - Not unit-tested — requires network access.
#
# Invariants:
#   - Returns the path to the saved CSV file (not a ZIP).
#   - raw_dir is created if it does not exist.
from __future__ import annotations

from pathlib import Path

import httpx

from pipeline.log import log


def download_comprasnet(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download a Comprasnet CSV file from repositorio.dados.gov.br and save it locally.

    Args:
        url:     Full URL of the Comprasnet CSV download endpoint.
        raw_dir: Directory where the raw file should be saved. Created if absent.
        timeout: HTTP timeout in seconds for the entire download.

    Returns:
        Absolute path to the saved CSV file.

    Raises:
        httpx.HTTPError: if the HTTP request fails.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    file_name = url.rstrip("/").split("/")[-1]
    if "." not in file_name:
        file_name = file_name + ".csv"
    csv_path = raw_dir / file_name

    # Cache: skip download if file already exists on disk.
    if csv_path.exists() and csv_path.stat().st_size > 0:
        log(f"  {file_name}: already exists ({csv_path.stat().st_size // (1024 * 1024)} MB), skipping download")
        return csv_path

    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
        response.raise_for_status()
        downloaded = 0
        with csv_path.open("wb") as file_handle:
            for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                file_handle.write(chunk)
                downloaded += len(chunk)
                if downloaded % (50 * 1024 * 1024) < len(chunk):
                    log(f"  {file_name}: {downloaded // (1024 * 1024)} MB downloaded...")

    return csv_path
