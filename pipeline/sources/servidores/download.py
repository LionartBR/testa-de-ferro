# pipeline/sources/servidores/download.py
#
# IO-only: download servidores CSV from Portal da Transparência.
#
# Design decisions:
#   - Portal da Transparência provides servidores data as monthly ZIPs, with
#     each "origem" (Servidores_SIAPE, Militares, etc.) as a separate file.
#   - The download URL format is: .../servidores/YYYYMM_Origem
#     (type ANO_MES_ORIGEM), unlike sanções which use YYYYMMDD.
#   - We download only Servidores_SIAPE (federal civil servants from SIAPE),
#     which is the origin the parse/match pipeline expects.
#   - The latest available month is scraped from the page's JS `arquivos`
#     array, filtered to Servidores_SIAPE entries.
#   - Not unit-tested — requires network access.
from __future__ import annotations

import re
import zipfile
from pathlib import Path

import httpx

from pipeline.log import log

_ORIGEM = "Servidores_SIAPE"

_ARQUIVOS_RE = re.compile(
    r'"ano"\s*:\s*"(\d{4})"\s*,\s*"mes"\s*:\s*"(\d{2})"\s*,\s*"dia"\s*:\s*""\s*,'
    r'\s*"origem"\s*:\s*"' + re.escape(_ORIGEM) + r'"'
)


def _scrape_latest_month(page_url: str, timeout: int) -> str:
    """Scrape the latest available YYYYMM for Servidores_SIAPE."""
    resp = httpx.get(page_url, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    matches = _ARQUIVOS_RE.findall(resp.text)
    if not matches:
        raise RuntimeError(
            f"Could not find {_ORIGEM} entries on {page_url}. The Portal da Transparência page format may have changed."
        )
    latest = max(matches)
    return f"{latest[0]}{latest[1]}"


def download_servidores(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the servidores dataset.

    Scrapes the latest available month from the Portal page and downloads
    the Servidores_SIAPE ZIP.

    Args:
        url:     Base URL of the servidores download page.
        raw_dir: Destination directory (created if absent).
        timeout: HTTP timeout in seconds.

    Returns:
        Path to the extracted CSV file.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    stripped = url.rstrip("/")
    yyyymm = _scrape_latest_month(stripped, timeout)
    resolved = f"{stripped}/{yyyymm}_{_ORIGEM}"
    log(f"  Resolved servidores -> {yyyymm}_{_ORIGEM}")

    zip_name = f"servidores_{yyyymm}.zip"
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

    return _extract_cadastro_csv(zip_path, raw_dir)


def _extract_cadastro_csv(zip_path: Path, dest_dir: Path) -> Path:
    """Extract the *Cadastro* CSV from a servidores ZIP archive.

    The ZIP contains multiple CSVs (Afastamentos, Cadastro, Observacoes,
    Remuneracao). We need Cadastro specifically because it contains
    ORGAO_LOTACAO, which is required for the servidor-socio match.

    Args:
        zip_path: Path to the downloaded ZIP file.
        dest_dir: Destination directory for extraction.

    Returns:
        Path to the extracted Cadastro CSV file.

    Raises:
        FileNotFoundError: If no Cadastro CSV is found in the archive.
    """
    with zipfile.ZipFile(zip_path) as archive:
        csv_names = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise FileNotFoundError(f"No CSV found inside {zip_path}")

        cadastro = [n for n in csv_names if "cadastro" in n.lower()]
        if not cadastro:
            raise FileNotFoundError(
                f"No *Cadastro* CSV found inside {zip_path}. "
                f"Available CSVs: {csv_names}"
            )

        archive.extract(cadastro[0], dest_dir)

    return dest_dir / cadastro[0]
