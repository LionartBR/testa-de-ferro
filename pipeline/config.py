# pipeline/config.py
#
# Pipeline configuration loaded from environment variables.
#
# Design decisions:
#   - Uses a frozen dataclass (not pydantic Settings) because the pipeline is a
#     standalone offline process and pydantic is reserved for the API layer.
#   - CPF_HMAC_SALT has no default: the pipeline must refuse to run without it
#     to prevent accidentally writing unprotected CPF data into the DuckDB.
#   - SOURCE_URLS are declared here so every source module reads from one place.
#     They can be overridden via environment variables for testing or mirror use.
#   - Paths default to pipeline/data relative to this file's directory so the
#     pipeline works out of the box after a fresh checkout.
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Sentinel — base directory of the pipeline package
# ---------------------------------------------------------------------------
_PIPELINE_DIR = Path(__file__).parent


@dataclass(frozen=True)
class SourceUrls:
    """URLs for each external data source.

    Invariant: all fields are non-empty strings pointing to publicly available
    government datasets. Override individual fields via environment variables
    for mirror use or testing.
    """

    cnpj_empresas: str = (
        "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-02/Empresas0.zip"
    )
    cnpj_qsa: str = (
        "https://arquivos.receitafederal.gov.br/public.php/webdav/2026-02/Socios0.zip"
    )
    pncp_contratos: str = "https://pncp.gov.br/api/consulta/v1/contratos"
    comprasnet_base: str = "https://dadosabertos.compras.gov.br/modulo-download"
    tse_doacoes: str = (
        "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas"
        "/prestacao_de_contas_eleitorais_candidatos_2022.zip"
    )
    ceis: str = (
        "https://portaldatransparencia.gov.br/download-de-dados/ceis"
    )
    cnep: str = (
        "https://portaldatransparencia.gov.br/download-de-dados/cnep"
    )
    cepim: str = (
        "https://portaldatransparencia.gov.br/download-de-dados/cepim"
    )
    servidores: str = (
        "https://portaldatransparencia.gov.br/download-de-dados/servidores"
    )


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable pipeline configuration.

    Invariants:
      - cpf_hmac_salt is always non-empty (enforced by load_config).
      - data_dir / staging_dir / output_dir are absolute Path objects.
      - download_timeout and download_retries are positive integers.
    """

    data_dir: Path
    cpf_hmac_salt: str
    duckdb_output_path: Path
    source_urls: SourceUrls = field(default_factory=SourceUrls)
    download_timeout: int = 300
    download_retries: int = 3

    @property
    def raw_dir(self) -> Path:
        """Directory for downloaded raw files."""
        return self.data_dir / "raw"

    @property
    def staging_dir(self) -> Path:
        """Directory for cleaned Parquet staging files."""
        return self.data_dir / "staging"

    @property
    def output_dir(self) -> Path:
        """Directory for the final DuckDB output."""
        return self.data_dir / "output"


def load_config() -> PipelineConfig:
    """Build PipelineConfig from environment variables.

    Raises:
        ValueError: if CPF_HMAC_SALT is not set. The pipeline must never run
            without the salt — doing so would write plain CPF data.
    """
    salt = os.environ.get("CPF_HMAC_SALT")
    if not salt:
        raise ValueError(
            "CPF_HMAC_SALT environment variable is required. "
            "Set it before running the pipeline. "
            "See .env.example for instructions."
        )

    data_dir = Path(
        os.environ.get("PIPELINE_DATA_DIR", str(_PIPELINE_DIR / "data"))
    )
    duckdb_output_path = Path(
        os.environ.get(
            "DUCKDB_OUTPUT_PATH",
            str(data_dir / "output" / "testa_de_ferro.duckdb"),
        )
    )

    download_timeout_raw = os.environ.get("PIPELINE_DOWNLOAD_TIMEOUT", "300")
    download_retries_raw = os.environ.get("PIPELINE_DOWNLOAD_RETRIES", "3")

    return PipelineConfig(
        data_dir=data_dir,
        cpf_hmac_salt=salt,
        duckdb_output_path=duckdb_output_path,
        source_urls=SourceUrls(),
        download_timeout=int(download_timeout_raw),
        download_retries=int(download_retries_raw),
    )
