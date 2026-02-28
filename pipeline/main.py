# pipeline/main.py
#
# Pipeline orchestrator: runs all source pipelines, transforms, and builds the
# final DuckDB database.
#
# Design decisions:
#   - run_pipeline is the single entry point. It accepts a PipelineConfig and an
#     optional skip_download flag for testing (uses pre-existing staging data).
#   - The orchestration follows a strict dependency order:
#       1. Download + parse + validate each source (produces staging parquets)
#       2. Enrich socios with servidor match, sanction cross-references
#       3. Pre-compute scores and alerts
#       4. Validate completude of all staging files
#       5. Build DuckDB atomically
#   - Each step logs progress to stdout. No structured logging framework is used
#     because the pipeline is a batch job, not a long-running service.
#   - If skip_download is True, the pipeline reads directly from existing staging
#     parquets — useful for testing transforms + build without hitting the network.
#
# Invariant: the DuckDB file is never replaced unless ALL sources completed
# successfully and completude validation passed.
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from pipeline.config import PipelineConfig, load_config
from pipeline.output.build_duckdb import build_duckdb
from pipeline.output.completude import validar_completude
from pipeline.staging.parquet_writer import read_parquet, write_parquet
from pipeline.transform.alertas import detectar_alertas_batch
from pipeline.transform.cruzamentos import enriquecer_socios
from pipeline.transform.hmac_cpf import apply_hmac_to_df
from pipeline.transform.match_servidor_socio import match_servidor_socio
from pipeline.transform.score import calcular_scores_batch


def run_pipeline(config: PipelineConfig, *, skip_download: bool = False) -> Path:
    """Execute the full pipeline and produce the DuckDB database.

    Args:
        config: Pipeline configuration with paths, salt, and URLs.
        skip_download: If True, skip download+parse+validate steps and read
            directly from existing staging parquets. Used for testing.

    Returns:
        Path to the final DuckDB database file.

    Raises:
        pipeline.output.completude.CompletudeError: if any required staging file
            is missing or empty after the source step.
    """
    staging_dir = config.staging_dir
    staging_dir.mkdir(parents=True, exist_ok=True)

    if not skip_download:
        _run_sources(config)

    # ---- Read staging data ----
    _log("Reading staging parquets...")
    empresas_df = read_parquet(staging_dir / "empresas.parquet")
    qsa_df = read_parquet(staging_dir / "qsa.parquet")
    contratos_df = read_parquet(staging_dir / "contratos.parquet")
    sancoes_df = read_parquet(staging_dir / "sancoes.parquet")
    servidores_df = read_parquet(staging_dir / "servidores.parquet")
    doacoes_df = read_parquet(staging_dir / "doacoes.parquet")

    # ---- Transforms ----
    _log("Running transforms...")

    # 1. Match servidores against socios (needs cpf_parcial, so run BEFORE HMAC)
    socios_enriched = match_servidor_socio(qsa_df, servidores_df)

    # 2. HMAC CPFs in enriched socios (if cpf column exists)
    if "cpf_parcial" in socios_enriched.columns:
        socios_enriched = apply_hmac_to_df(socios_enriched, "cpf_parcial", config.cpf_hmac_salt)

    # 3. Cross-reference enrichments
    socios_enriched = enriquecer_socios(socios_enriched, sancoes_df, empresas_df)

    # 4. Pre-compute scores
    _log("Computing scores...")
    scores_df = calcular_scores_batch(empresas_df, socios_enriched, contratos_df, sancoes_df)
    write_parquet(scores_df, staging_dir / "score_detalhe.parquet")

    # 5. Pre-compute alerts
    _log("Computing alerts...")
    alertas_df = detectar_alertas_batch(empresas_df, socios_enriched, contratos_df, sancoes_df, doacoes_df)
    write_parquet(alertas_df, staging_dir / "alertas.parquet")

    # 6. Write enriched socios to staging — rename to match dim_socio schema
    socios_for_staging = _prepare_socios_staging(socios_enriched)
    write_parquet(socios_for_staging, staging_dir / "socios.parquet")

    # ---- Validate completude ----
    _log("Validating completude...")
    validar_completude(staging_dir)

    # ---- Build DuckDB ----
    _log("Building DuckDB...")
    output_path = build_duckdb(staging_dir, config.duckdb_output_path)
    _log(f"Done. DuckDB written to: {output_path}")
    return output_path


def _prepare_socios_staging(socios_df: pl.DataFrame) -> pl.DataFrame:
    """Map enriched socios DataFrame to dim_socio staging schema.

    The transforms produce columns like nome_socio, qualificacao_socio, etc.
    dim_socio in schema.sql expects: pk_socio, cpf_hmac, nome, qualificacao,
    is_servidor_publico, orgao_lotacao, is_sancionado, qtd_empresas_governo.
    """
    n = len(socios_df)

    def _get_col(df: pl.DataFrame, name: str) -> pl.Series:
        return df[name] if name in df.columns else pl.Series(name, [None] * n)

    # Map source columns → dim_socio columns
    nome = _get_col(socios_df, "nome_socio").alias("nome")
    cpf_hmac = _get_col(socios_df, "cpf_hmac").alias("cpf_hmac")
    qualificacao = _get_col(socios_df, "qualificacao_socio").alias("qualificacao")
    is_servidor = _get_col(socios_df, "is_servidor_publico").alias("is_servidor_publico")
    orgao = _get_col(socios_df, "orgao_lotacao").alias("orgao_lotacao")
    is_sancionado = _get_col(socios_df, "is_sancionado").alias("is_sancionado")
    qtd_empresas = _get_col(socios_df, "qtd_empresas_governo").alias("qtd_empresas_governo")

    return pl.DataFrame(
        {
            "pk_socio": list(range(1, n + 1)),
            "cpf_hmac": cpf_hmac,
            "nome": nome,
            "qualificacao": qualificacao,
            "is_servidor_publico": is_servidor,
            "orgao_lotacao": orgao,
            "is_sancionado": is_sancionado,
            "qtd_empresas_governo": qtd_empresas,
        }
    )


def _run_sources(config: PipelineConfig) -> None:
    """Download, parse, and validate all data sources to staging parquets.

    Downloads run in parallel (ThreadPoolExecutor) since they are IO-bound.
    Parse + validate run sequentially after all downloads complete.
    Errors in any source abort the pipeline.
    """
    from collections.abc import Callable
    from concurrent.futures import Future, ThreadPoolExecutor, as_completed

    from pipeline.sources.cnpj.download import download_cnpj
    from pipeline.sources.cnpj.parse_empresas import parse_empresas
    from pipeline.sources.cnpj.parse_qsa import parse_qsa
    from pipeline.sources.cnpj.validate import validate_empresas, validate_qsa
    from pipeline.sources.pncp.download import download_pncp
    from pipeline.sources.pncp.parse import parse_contratos
    from pipeline.sources.pncp.validate import validate_contratos
    from pipeline.sources.sancoes.download import download_ceis, download_cepim, download_cnep
    from pipeline.sources.sancoes.parse_ceis import parse_ceis
    from pipeline.sources.sancoes.parse_cepim import parse_cepim
    from pipeline.sources.sancoes.parse_cnep import parse_cnep
    from pipeline.sources.sancoes.validate import validate_sancoes
    from pipeline.sources.servidores.download import download_servidores
    from pipeline.sources.servidores.parse import parse_servidores
    from pipeline.sources.servidores.validate import validate_servidores
    from pipeline.sources.tse.download import download_doacoes
    from pipeline.sources.tse.parse import parse_doacoes
    from pipeline.sources.tse.validate import validate_doacoes

    raw_dir = config.raw_dir
    staging_dir = config.staging_dir
    raw_dir.mkdir(parents=True, exist_ok=True)
    urls = config.source_urls
    t = config.download_timeout

    # ---- Phase 1: Parallel downloads ----
    _log("Downloading all sources in parallel...")
    DownloadFn = Callable[[str, Path, int], Path]
    download_tasks: dict[str, tuple[DownloadFn, str, Path, int]] = {
        "cnpj_empresas": (download_cnpj, urls.cnpj_empresas, raw_dir / "cnpj", t),
        "cnpj_qsa": (download_cnpj, urls.cnpj_qsa, raw_dir / "cnpj_qsa", t),
        "pncp": (download_pncp, urls.pncp_contratos, raw_dir / "pncp", t),
        "ceis": (download_ceis, urls.ceis, raw_dir / "ceis", t),
        "cnep": (download_cnep, urls.cnep, raw_dir / "cnep", t),
        "cepim": (download_cepim, urls.cepim, raw_dir / "cepim", t),
        "servidores": (download_servidores, urls.servidores, raw_dir / "servidores", t),
        "tse": (download_doacoes, urls.tse_doacoes, raw_dir / "tse", t),
    }

    raw_paths: dict[str, Path] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        future_to_name: dict[Future[Path], str] = {
            pool.submit(fn, url, dest, timeout): name for name, (fn, url, dest, timeout) in download_tasks.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            raw_paths[name] = future.result()  # raises on error
            _log(f"  Downloaded: {name}")

    # ---- Phase 2: Parse + validate (sequential, CPU-bound) ----
    _log("Parsing and validating...")

    empresas_df = validate_empresas(parse_empresas(raw_paths["cnpj_empresas"]))
    write_parquet(empresas_df, staging_dir / "empresas.parquet")

    qsa_df = validate_qsa(parse_qsa(raw_paths["cnpj_qsa"]))
    write_parquet(qsa_df, staging_dir / "qsa.parquet")

    contratos_df = validate_contratos(parse_contratos(raw_paths["pncp"]))
    write_parquet(contratos_df, staging_dir / "contratos.parquet")

    sancoes_all = pl.concat(
        [
            parse_ceis(raw_paths["ceis"]),
            parse_cnep(raw_paths["cnep"]),
            parse_cepim(raw_paths["cepim"]),
        ]
    )
    sancoes_df = validate_sancoes(sancoes_all)
    write_parquet(sancoes_df, staging_dir / "sancoes.parquet")

    servidores_df = validate_servidores(parse_servidores(raw_paths["servidores"]))
    write_parquet(servidores_df, staging_dir / "servidores.parquet")

    doacoes_df = validate_doacoes(parse_doacoes(raw_paths["tse"]))
    write_parquet(doacoes_df, staging_dir / "doacoes.parquet")


def _log(message: str) -> None:
    """Simple stdout logger for pipeline progress."""
    sys.stdout.write(f"[pipeline] {message}\n")
    sys.stdout.flush()


if __name__ == "__main__":
    cfg = load_config()
    run_pipeline(cfg)
