# pipeline/sources/rais/download.py
#
# IO-only: download the RAIS (Relação Anual de Informações Sociais) dataset
# from the MTE FTP server and extract its CSV.
#
# Design decisions:
#   - RAIS is distributed via FTP at ftp.mtps.gov.br/pdet/microdados/RAIS/
#     as .7z archives containing semicolon-delimited CSVs.
#   - Uses ftplib.FTP from the standard library — no new HTTP dependency needed.
#   - py7zr is used for .7z extraction. Falls back to zipfile for .zip archives.
#   - The function lists the FTP directory and picks the most recent RAIS file
#     by sorting filenames that match the RAIS{year} pattern.
#   - Cache: if the extracted CSV already exists on disk, download is skipped.
#   - Graceful fallback: if FTP is unreachable or extraction fails, the function
#     raises an exception that is caught as an optional source in main.py.
#   - Not unit-tested — requires network access.
#
# Invariants:
#   - Returns the path to the extracted CSV, not the archive.
#   - raw_dir is created if it does not exist.
from __future__ import annotations

import re
import zipfile
from ftplib import FTP
from pathlib import Path
from urllib.parse import urlparse

from pipeline.log import log


def download_rais(url: str, raw_dir: Path, timeout: int = 300) -> Path:
    """Download the most recent RAIS archive via FTP and extract its CSV.

    Args:
        url:     FTP URL of the RAIS directory (e.g. ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/).
        raw_dir: Directory where raw files should be saved. Created if absent.
        timeout: FTP connection timeout in seconds.

    Returns:
        Absolute path to the extracted CSV file.

    Raises:
        ConnectionError: if the FTP server is unreachable.
        FileNotFoundError: if no RAIS archive or CSV is found.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    parsed = urlparse(url)
    host = parsed.hostname or "ftp.mtps.gov.br"
    ftp_path = parsed.path or "/pdet/microdados/RAIS/"

    log(f"  RAIS: connecting to FTP {host}...")
    ftp = FTP(timeout=timeout)  # noqa: S321
    ftp.connect(host)
    ftp.login()  # anonymous login

    try:
        ftp.cwd(ftp_path)
        file_list = ftp.nlst()
    except Exception as exc:
        ftp.quit()
        msg = f"Failed to list RAIS FTP directory {ftp_path} on {host}: {exc}"
        raise ConnectionError(msg) from exc

    # Find the most recent RAIS archive (RAIS2023.7z, RAIS2022.zip, etc.)
    rais_pattern = re.compile(r"RAIS(\d{4})\.(7z|zip)", re.IGNORECASE)
    candidates: list[tuple[int, str]] = []
    for fname in file_list:
        match = rais_pattern.search(fname)
        if match:
            candidates.append((int(match.group(1)), fname))

    if not candidates:
        ftp.quit()
        msg = f"No RAIS archive found in FTP directory {ftp_path}. Files: {file_list[:20]}"
        raise FileNotFoundError(msg)

    # Pick the most recent year
    candidates.sort(key=lambda x: x[0], reverse=True)
    year, archive_name = candidates[0]
    archive_path = raw_dir / archive_name

    log(f"  RAIS: latest archive is {archive_name} (year {year})")

    # Cache: check if a CSV extracted from this archive already exists
    csv_glob = list(raw_dir.glob(f"*RAIS*{year}*.csv")) + list(raw_dir.glob(f"*rais*{year}*.csv"))
    if csv_glob:
        csv_path = csv_glob[0]
        if csv_path.stat().st_size > 0:
            log(f"  RAIS: CSV already extracted ({csv_path.name}), skipping download")
            ftp.quit()
            return csv_path

    # Download the archive via FTP
    if not archive_path.exists() or archive_path.stat().st_size == 0:
        log(f"  RAIS: downloading {archive_name}...")
        downloaded = 0
        with archive_path.open("wb") as fh:

            def _write_chunk(data: bytes) -> None:
                nonlocal downloaded
                fh.write(data)
                downloaded += len(data)
                if downloaded % (50 * 1024 * 1024) < len(data):
                    log(f"  RAIS: {downloaded // (1024 * 1024)} MB downloaded...")

            ftp.retrbinary(f"RETR {archive_name}", _write_chunk, blocksize=8 * 1024 * 1024)

        log(f"  RAIS: download complete ({downloaded // (1024 * 1024)} MB)")
    else:
        log(f"  RAIS: archive already on disk ({archive_path.name}), skipping download")

    ftp.quit()

    # Extract CSV from archive
    csv_path = _extract_csv(archive_path, raw_dir)
    return csv_path


def _extract_csv(archive_path: Path, raw_dir: Path) -> Path:
    """Extract the first CSV found inside a .7z or .zip archive.

    Args:
        archive_path: Path to the archive file.
        raw_dir: Directory to extract into.

    Returns:
        Path to the extracted CSV.

    Raises:
        FileNotFoundError: if no CSV is found inside the archive.
    """
    suffix = archive_path.suffix.lower()

    if suffix == ".7z":
        return _extract_from_7z(archive_path, raw_dir)
    if suffix == ".zip":
        return _extract_from_zip(archive_path, raw_dir)

    msg = f"Unsupported archive format: {suffix} (expected .7z or .zip)"
    raise FileNotFoundError(msg)


def _extract_from_7z(archive_path: Path, raw_dir: Path) -> Path:
    """Extract the first CSV from a .7z archive using py7zr."""
    try:
        import py7zr
    except ImportError as exc:
        msg = (
            "py7zr is required to extract RAIS .7z archives. "
            "Install it with: pip install py7zr"
        )
        raise ImportError(msg) from exc

    with py7zr.SevenZipFile(archive_path, mode="r") as archive:
        all_names = archive.getnames()
        csv_names = [name for name in all_names if name.lower().endswith(".csv")]
        if not csv_names:
            msg = f"No CSV found inside {archive_path}. Contents: {all_names[:20]}"
            raise FileNotFoundError(msg)

        # Extract only the CSV file(s)
        archive.extract(path=raw_dir, targets=csv_names[:1])

    csv_path = raw_dir / csv_names[0]
    log(f"  RAIS: extracted {csv_names[0]} from {archive_path.name}")
    return csv_path


def _extract_from_zip(archive_path: Path, raw_dir: Path) -> Path:
    """Extract the first CSV from a .zip archive."""
    with zipfile.ZipFile(archive_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            msg = f"No CSV found inside {archive_path}"
            raise FileNotFoundError(msg)
        archive.extract(csv_names[0], raw_dir)

    csv_path = raw_dir / csv_names[0]
    log(f"  RAIS: extracted {csv_names[0]} from {archive_path.name}")
    return csv_path
