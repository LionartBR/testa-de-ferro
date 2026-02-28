# pipeline/sources/cnpj/download.py
#
# IO-only: download CNPJ ZIP files from Receita Federal and extract them.
#
# Design decisions:
#   - This module is intentionally IO-only and is not unit-tested. Integration
#     tests would require network access or a large test fixture, which is
#     outside the scope of the offline pipeline test suite.
#   - The download uses httpx with streaming to avoid loading multi-GB ZIPs
#     into memory. The file is written incrementally in 8MB chunks.
#   - The extracted CSV is expected at a known path inside the ZIP. If the ZIP
#     structure changes, the extraction step raises FileNotFoundError.
#   - retry_count is handled by the caller (main.py orchestrator). This function
#     does not retry internally — it raises on any error.
from __future__ import annotations

import zipfile
from pathlib import Path

import httpx


def download_cnpj(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download a Receita Federal CNPJ ZIP and extract the CSV it contains.

    Downloads the ZIP file from the given URL into raw_dir, then extracts
    the first .csv file found in the archive. Returns the path to the
    extracted CSV file.

    Args:
        url:     Full URL of the Receita Federal CNPJ ZIP file.
        raw_dir: Directory where raw files should be saved. Created if absent.
        timeout: HTTP timeout in seconds for the entire download.

    Returns:
        Absolute path to the extracted CSV file.

    Raises:
        httpx.HTTPError:   if the HTTP request fails.
        FileNotFoundError: if the ZIP contains no CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_name = url.split("/")[-1] or "cnpj_download.zip"
    zip_path = raw_dir / zip_name

    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as response:
        response.raise_for_status()
        with zip_path.open("wb") as file_handle:
            for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                file_handle.write(chunk)

    # Extract the first CSV from the ZIP archive.
    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"No CSV found inside {zip_path}")
        archive.extract(csv_names[0], raw_dir)

    extracted_path = raw_dir / csv_names[0]
    return extracted_path
