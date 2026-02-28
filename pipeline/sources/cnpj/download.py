# pipeline/sources/cnpj/download.py
#
# IO-only: download CNPJ ZIP files from Receita Federal and extract them.
#
# Design decisions:
#   - Receita Federal now serves CNPJ data via a Nextcloud WebDAV share at
#     arquivos.receitafederal.gov.br. The public share token (YggdBLfdninEJX9)
#     is used for Basic Auth on the WebDAV endpoint.
#   - The download uses httpx with streaming to avoid loading multi-GB ZIPs
#     into memory. The file is written incrementally in 8MB chunks.
#   - Not unit-tested — requires network access.
from __future__ import annotations

import base64
import zipfile
from pathlib import Path

import httpx

_SHARE_TOKEN = "YggdBLfdninEJX9"
_WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"


def _auth_header() -> str:
    return "Basic " + base64.b64encode(f"{_SHARE_TOKEN}:".encode()).decode()


def download_cnpj(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download a Receita Federal CNPJ ZIP and extract the CSV it contains.

    If the URL starts with the old dadosabertos.rfb.gov.br domain, it is
    transparently rewritten to the new WebDAV endpoint.

    Args:
        url:     Full URL or WebDAV path of the CNPJ ZIP file.
        raw_dir: Directory where raw files should be saved. Created if absent.
        timeout: HTTP timeout in seconds for the entire download.

    Returns:
        Absolute path to the extracted CSV file.

    Raises:
        httpx.HTTPError:   if the HTTP request fails.
        FileNotFoundError: if the ZIP contains no CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Rewrite old URLs to new WebDAV
    actual_url = url
    headers: dict[str, str] = {}
    if "dadosabertos.rfb.gov.br" in url or _WEBDAV_BASE in url:
        # Extract filename from URL (e.g. Empresas0.zip)
        zip_name = url.rstrip("/").split("/")[-1]
        # Extract period from URL if present (e.g. 2026-02)
        parts = url.split("/")
        period = next((p for p in parts if len(p) == 7 and p[4] == "-"), None)
        if period:
            actual_url = f"{_WEBDAV_BASE}/{period}/{zip_name}"
        elif _WEBDAV_BASE not in url:
            actual_url = f"{_WEBDAV_BASE}/{zip_name}"
        headers["Authorization"] = _auth_header()
    else:
        zip_name = url.split("/")[-1] or "cnpj_download.zip"

    zip_path = raw_dir / zip_name

    with httpx.stream(
        "GET", actual_url, timeout=timeout, follow_redirects=True, headers=headers,
    ) as response:
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

    return raw_dir / csv_names[0]
