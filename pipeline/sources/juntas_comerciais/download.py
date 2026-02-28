# pipeline/sources/juntas_comerciais/download.py
#
# IO-only: download Juntas Comerciais QSA diff files from the Receita Federal
# WebDAV share and extract the CSV they contain.
#
# Design decisions:
#   - The Receita Federal serves Juntas Comerciais data via the same Nextcloud
#     WebDAV endpoint used for CNPJ files (arquivos.receitafederal.gov.br).
#     The same public share token and Basic Auth header are therefore reused.
#   - The download uses httpx streaming with 8 MB chunks to handle large files
#     without loading the entire archive into memory.
#   - A single download_juntas_comerciais function is provided because, unlike
#     the CNPJ source (which has separate empresas/QSA downloads), the Juntas
#     Comerciais extract ships as a single file containing QSA diffs.
#   - Not unit-tested — requires network access.
#
# Invariants:
#   - Returns the path to the extracted CSV, never the ZIP.
#   - raw_dir is created if it does not exist.
from __future__ import annotations

import base64
import zipfile
from pathlib import Path

import httpx

from pipeline.log import log

_SHARE_TOKEN = "YggdBLfdninEJX9"  # noqa: S105  — public share token, not a password
_WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"


def _auth_header() -> str:
    """Return the Basic Auth header for the Receita Federal WebDAV share."""
    return "Basic " + base64.b64encode(f"{_SHARE_TOKEN}:".encode()).decode()


def download_juntas_comerciais(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download a Receita Federal Juntas Comerciais ZIP and extract its CSV.

    Handles the WebDAV URL scheme used by arquivos.receitafederal.gov.br.
    If the URL already points to the WebDAV base, the Authorization header is
    added automatically.

    Args:
        url:     Full URL of the Juntas Comerciais ZIP on the Receita Federal
                 WebDAV share (or any direct HTTPS URL).
        raw_dir: Directory where raw files should be saved. Created if absent.
        timeout: HTTP timeout in seconds for the entire download.

    Returns:
        Absolute path to the extracted CSV file.

    Raises:
        httpx.HTTPError:   if the HTTP request fails.
        FileNotFoundError: if the ZIP contains no CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    headers: dict[str, str] = {}
    if _WEBDAV_BASE in url or "arquivos.receitafederal.gov.br" in url:
        headers["Authorization"] = _auth_header()

    zip_name = url.rstrip("/").split("/")[-1]
    if not zip_name.lower().endswith(".zip"):
        zip_name = zip_name + ".zip"
    zip_path = raw_dir / zip_name

    with httpx.stream("GET", url, timeout=timeout, follow_redirects=True, headers=headers) as response:
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
