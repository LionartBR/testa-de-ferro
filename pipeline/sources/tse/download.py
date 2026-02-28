# pipeline/sources/tse/download.py
#
# IO-only: download TSE prestação de contas ZIP and extract receitas CSVs.
#
# Design decisions:
#   - TSE distributes campaign finance data as large ZIP files containing
#     multiple per-state CSVs for receitas (donations), despesas (expenses),
#     and doador_originário. We only need "receitas_candidatos_*.csv".
#   - The ZIP contains ~100+ files; we extract only the receitas CSVs to
#     save disk space and parse time.
#   - Returns the directory containing extracted CSVs (not a single file),
#     because parse_doacoes must read and concatenate all state-level CSVs.
#   - Not unit-tested — requires network access.
from __future__ import annotations

import zipfile
from pathlib import Path

import httpx

from pipeline.log import log


def download_doacoes(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download and extract the TSE doacoes dataset.

    The TSE ZIP contains per-state CSVs for receitas, despesas, and
    doador_originário. This function extracts only the receitas_candidatos
    CSVs (one per state), which contain campaign donation records.

    Args:
        url:     URL of the TSE doacoes ZIP file.
        raw_dir: Destination directory (created if absent).
        timeout: HTTP timeout in seconds.

    Returns:
        Path to the directory containing the extracted receitas CSVs.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    zip_name = url.rstrip("/").split("/")[-1]
    if not zip_name.endswith(".zip"):
        zip_name += ".zip"
    zip_path = raw_dir / zip_name

    if not zip_path.exists():
        with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as resp:
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

    # ADR: Extract only receitas_candidatos CSVs (not despesas or doador_originario).
    # The "BRASIL" file is a national aggregate that duplicates state-level rows,
    # so we exclude it to avoid double-counting. The "doador_originario" files
    # track the original donor in indirect donation chains and have a different
    # schema, so they are excluded too.
    with zipfile.ZipFile(zip_path) as archive:
        receitas_csvs = [
            n
            for n in archive.namelist()
            if n.lower().startswith("receitas_candidatos_")
            and n.lower().endswith(".csv")
            and "_brasil" not in n.lower()
            and "doador_originario" not in n.lower()
        ]
        if not receitas_csvs:
            raise FileNotFoundError(f"No receitas_candidatos CSV found inside {zip_path}")
        for csv_name in receitas_csvs:
            archive.extract(csv_name, raw_dir)
        log(f"  Extracted {len(receitas_csvs)} receitas_candidatos CSVs")

    return raw_dir
