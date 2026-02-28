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

from pathlib import Path

import polars as pl

from pipeline.config import PipelineConfig, load_config
from pipeline.log import log
from pipeline.output.build_duckdb import build_duckdb
from pipeline.output.completude import validar_completude
from pipeline.staging.parquet_writer import read_parquet, write_parquet
from pipeline.transform.alertas import detectar_alertas_batch
from pipeline.transform.cruzamentos import enriquecer_socios
from pipeline.transform.grafo_societario import construir_grafo
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
    log("Reading staging parquets...")
    empresas_df = read_parquet(staging_dir / "empresas.parquet")
    qsa_df = read_parquet(staging_dir / "qsa.parquet")
    contratos_df = read_parquet(staging_dir / "contratos.parquet")
    sancoes_df = read_parquet(staging_dir / "sancoes.parquet")
    servidores_df = read_parquet(staging_dir / "servidores.parquet")
    doacoes_df = read_parquet(staging_dir / "doacoes.parquet")

    # Merge RAIS employee counts into empresas (adds qtd_funcionarios column).
    rais_path = staging_dir / "rais.parquet"
    if rais_path.exists():
        rais_df = read_parquet(rais_path)
        empresas_df = _merge_rais_into_empresas(empresas_df, rais_df)
        log(f"  Merged RAIS: qtd_funcionarios column added to {len(empresas_df):,} fornecedores")

    # Merge juntas_comerciais QSA diffs into the base QSA (concat + dedup).
    juntas_path = staging_dir / "juntas_comerciais.parquet"
    if juntas_path.exists():
        juntas_df = read_parquet(juntas_path)
        qsa_df = _merge_qsa_diffs(qsa_df, juntas_df)
        log(f"  Merged QSA diffs: {len(qsa_df):,} total socio rows after merge")

    # Merge Comprasnet contracts into the PNCP contratos (concat + dedup).
    comprasnet_path = staging_dir / "comprasnet.parquet"
    if comprasnet_path.exists():
        comprasnet_df = read_parquet(comprasnet_path)
        contratos_df = _merge_contratos(contratos_df, comprasnet_df)
        log(f"  Merged Comprasnet: {len(contratos_df):,} total contract rows after merge")

    # ---- Transforms ----
    log("Running transforms...")

    # 1. Match servidores against socios (needs cpf_parcial, so run BEFORE HMAC)
    socios_enriched = match_servidor_socio(qsa_df, servidores_df)
    log(f"  Servidor-socio match: {len(socios_enriched):,} rows")

    # 2. HMAC CPFs in enriched socios (if cpf column exists)
    if "cpf_parcial" in socios_enriched.columns:
        socios_enriched = apply_hmac_to_df(socios_enriched, "cpf_parcial", config.cpf_hmac_salt)

    # 3. Cross-reference enrichments
    socios_enriched = enriquecer_socios(socios_enriched, sancoes_df, empresas_df)
    log(f"  Enriched socios: {len(socios_enriched):,} rows")

    # 4–6. Pre-compute scores, alerts, and graph in parallel.
    # ADR: The three computations are fully independent — none reads the output of
    # another, and all three take only the already-materialised DataFrames as input.
    # ThreadPoolExecutor is sufficient (vs ProcessPoolExecutor) because Polars
    # group_by/join operations release the GIL internally.
    from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor

    log("Computing scores, alerts, and graph in parallel...")

    def _compute_scores() -> pl.DataFrame:
        df = calcular_scores_batch(empresas_df, socios_enriched, contratos_df, sancoes_df)
        write_parquet(df, staging_dir / "score_detalhe.parquet")
        return df

    def _compute_alertas() -> pl.DataFrame:
        df = detectar_alertas_batch(empresas_df, socios_enriched, contratos_df, sancoes_df, doacoes_df)
        write_parquet(df, staging_dir / "alertas.parquet")
        return df

    def _compute_grafo() -> tuple[pl.DataFrame, pl.DataFrame]:
        nos, arestas = construir_grafo(socios_enriched, empresas_df)
        write_parquet(nos, staging_dir / "grafo_nos.parquet")
        write_parquet(arestas, staging_dir / "grafo_arestas.parquet")
        return nos, arestas

    with _ThreadPoolExecutor(max_workers=3) as pool:
        score_future = pool.submit(_compute_scores)
        alerta_future = pool.submit(_compute_alertas)
        grafo_future = pool.submit(_compute_grafo)

    scores_df = score_future.result()
    log(f"  Scores: {len(scores_df):,} rows")
    alertas_df = alerta_future.result()
    log(f"  Alertas: {len(alertas_df):,} rows")
    nos_df, arestas_df = grafo_future.result()
    log(f"  Grafo: {len(nos_df):,} nodes, {len(arestas_df):,} edges")

    # 7. Denormalize aggregate columns into empresas (dim_fornecedor)
    log("Denormalizing dim_fornecedor aggregates...")
    empresas_df = _denormalize_fornecedor(empresas_df, scores_df, alertas_df, contratos_df)
    write_parquet(empresas_df, staging_dir / "empresas.parquet")
    log(f"  Denormalized {len(empresas_df):,} fornecedores")

    # 8. Write enriched socios to staging — rename to match dim_socio schema
    socios_for_staging = _prepare_socios_staging(socios_enriched)
    write_parquet(socios_for_staging, staging_dir / "socios.parquet")

    # ---- Validate completude ----
    log("Validating completude...")
    validar_completude(staging_dir)

    # ---- Build DuckDB ----
    log("Building DuckDB...")
    output_path = build_duckdb(staging_dir, config.duckdb_output_path)
    log(f"Done. DuckDB written to: {output_path}")
    return output_path


def _merge_rais_into_empresas(empresas_df: pl.DataFrame, rais_df: pl.DataFrame) -> pl.DataFrame:
    """Join RAIS employee counts into the empresas DataFrame.

    Matches on cnpj_basico (first 8 digits). Fornecedores not found in RAIS
    receive null (not zero — null means "data not available").
    """
    if rais_df.is_empty() or "cnpj_basico" not in empresas_df.columns:
        return empresas_df.with_columns(pl.lit(None, dtype=pl.Int64).alias("qtd_funcionarios"))

    empresas_with_basico = empresas_df.with_columns(pl.col("cnpj_basico").str.zfill(8).alias("_cnpj_basico"))
    rais_slim = rais_df.select(
        [
            pl.col("cnpj_basico").str.zfill(8).alias("_cnpj_basico"),
            pl.col("qtd_funcionarios"),
        ]
    )
    joined = empresas_with_basico.join(rais_slim, on="_cnpj_basico", how="left").drop("_cnpj_basico")
    return joined


def _merge_qsa_diffs(qsa_df: pl.DataFrame, juntas_df: pl.DataFrame) -> pl.DataFrame:
    """Merge Juntas Comerciais QSA diffs into the base Receita Federal QSA.

    Concat and dedup so each (cnpj_basico, nome_socio, data_entrada) triple
    appears at most once.
    """
    if juntas_df.is_empty():
        return qsa_df

    if "data_saida" not in qsa_df.columns:
        qsa_df = qsa_df.with_columns(pl.lit(None, dtype=pl.Date).alias("data_saida"))

    shared_cols = [col for col in qsa_df.columns if col in juntas_df.columns]
    juntas_aligned = juntas_df.select(shared_cols)

    merged = pl.concat([qsa_df, juntas_aligned], how="diagonal")

    dedup_cols = [c for c in ["cnpj_basico", "nome_socio", "data_entrada"] if c in merged.columns]
    if dedup_cols:
        merged = merged.unique(subset=dedup_cols, keep="first", maintain_order=True)

    return merged


def _merge_contratos(contratos_df: pl.DataFrame, comprasnet_df: pl.DataFrame) -> pl.DataFrame:
    """Merge Comprasnet contracts into the PNCP contracts DataFrame.

    Concat and dedup on (cnpj_fornecedor, num_licitacao, data_assinatura).
    """
    if comprasnet_df.is_empty():
        return contratos_df

    merged = pl.concat([contratos_df, comprasnet_df], how="diagonal")

    dedup_cols = [c for c in ["cnpj_fornecedor", "num_licitacao", "data_assinatura"] if c in merged.columns]
    if dedup_cols:
        merged = merged.unique(subset=dedup_cols, keep="first", maintain_order=True)

    if "pk_contrato" in merged.columns:
        n = len(merged)
        merged = merged.with_columns(pl.int_range(1, n + 1, eager=True).alias("pk_contrato"))

    return merged


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
            "pk_socio": pl.int_range(1, n + 1, eager=True),
            "cpf_hmac": cpf_hmac,
            "nome": nome,
            "qualificacao": qualificacao,
            "is_servidor_publico": is_servidor,
            "orgao_lotacao": orgao,
            "is_sancionado": is_sancionado,
            "qtd_empresas_governo": qtd_empresas,
        }
    )


def _denormalize_fornecedor(
    empresas_df: pl.DataFrame,
    scores_df: pl.DataFrame,
    alertas_df: pl.DataFrame,
    contratos_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute denormalized aggregate columns for dim_fornecedor.

    Populates: score_risco, faixa_risco, qtd_alertas, max_severidade,
    total_contratos, valor_total — all derived from the pre-computed
    scores, alerts, and contracts DataFrames.
    """
    pk_col = "pk_fornecedor"

    # --- Score: sum of pesos per fornecedor ---
    if not scores_df.is_empty() and "fk_fornecedor" in scores_df.columns:
        score_agg = scores_df.group_by("fk_fornecedor").agg(pl.col("peso").sum().alias("score_risco"))
    else:
        score_agg = pl.DataFrame(
            {"fk_fornecedor": pl.Series([], dtype=pl.Int64), "score_risco": pl.Series([], dtype=pl.Int64)}
        )

    # --- Alertas: count + max severidade ---
    if not alertas_df.is_empty() and "fk_fornecedor" in alertas_df.columns:
        # Map severidade to a numeric rank for MAX aggregation: GRAVISSIMO=2, GRAVE=1, else 0.
        alertas_ranked = alertas_df.with_columns(
            pl.when(pl.col("severidade") == "GRAVISSIMO")
            .then(2)
            .when(pl.col("severidade") == "GRAVE")
            .then(1)
            .otherwise(0)
            .alias("_sev_rank")
        )
        alerta_agg = alertas_ranked.group_by("fk_fornecedor").agg(
            [
                pl.len().alias("qtd_alertas"),
                pl.col("_sev_rank").max().alias("_max_rank"),
            ]
        )
        # Map rank back to string
        alerta_agg = alerta_agg.with_columns(
            pl.when(pl.col("_max_rank") == 2)
            .then(pl.lit("GRAVISSIMO"))
            .when(pl.col("_max_rank") == 1)
            .then(pl.lit("GRAVE"))
            .otherwise(pl.lit(None))
            .alias("max_severidade")
        ).drop("_max_rank")
    else:
        alerta_agg = pl.DataFrame(
            {
                "fk_fornecedor": pl.Series([], dtype=pl.Int64),
                "qtd_alertas": pl.Series([], dtype=pl.Int64),
                "max_severidade": pl.Series([], dtype=pl.Utf8),
            }
        )

    # --- Contratos: count + total value ---
    if not contratos_df.is_empty() and "fk_fornecedor" in contratos_df.columns:
        contrato_agg = contratos_df.group_by("fk_fornecedor").agg(
            [
                pl.len().alias("total_contratos"),
                pl.col("valor").cast(pl.Float64).sum().alias("valor_total"),
            ]
        )
    else:
        contrato_agg = pl.DataFrame(
            {
                "fk_fornecedor": pl.Series([], dtype=pl.Int64),
                "total_contratos": pl.Series([], dtype=pl.Int64),
                "valor_total": pl.Series([], dtype=pl.Float64),
            }
        )

    # --- Join aggregates onto empresas ---
    result = empresas_df.join(score_agg, left_on=pk_col, right_on="fk_fornecedor", how="left")
    result = result.join(alerta_agg, left_on=pk_col, right_on="fk_fornecedor", how="left")
    result = result.join(contrato_agg, left_on=pk_col, right_on="fk_fornecedor", how="left")

    # Fill nulls with defaults
    result = result.with_columns(
        [
            pl.col("score_risco").fill_null(0).cast(pl.Int16),
            pl.col("qtd_alertas").fill_null(0).cast(pl.Int16),
            pl.col("total_contratos").fill_null(0).cast(pl.Int32),
            pl.col("valor_total").fill_null(0.0),
        ]
    )

    # Compute faixa_risco from score_risco
    result = result.with_columns(
        pl.when(pl.col("score_risco") >= 70)
        .then(pl.lit("Critico"))
        .when(pl.col("score_risco") >= 50)
        .then(pl.lit("Alto"))
        .when(pl.col("score_risco") >= 30)
        .then(pl.lit("Medio"))
        .when(pl.col("score_risco") >= 10)
        .then(pl.lit("Baixo"))
        .otherwise(pl.lit("Minimo"))
        .alias("faixa_risco")
    )

    return result


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
    from pipeline.sources.comprasnet.download import download_comprasnet
    from pipeline.sources.comprasnet.parse import parse_comprasnet
    from pipeline.sources.comprasnet.validate import validate_comprasnet
    from pipeline.sources.juntas_comerciais.download import download_juntas_comerciais
    from pipeline.sources.juntas_comerciais.parse import parse_qsa_diffs
    from pipeline.sources.juntas_comerciais.validate import validate_qsa_diffs
    from pipeline.sources.pncp.download import download_pncp
    from pipeline.sources.pncp.parse import parse_contratos
    from pipeline.sources.pncp.validate import validate_contratos
    from pipeline.sources.rais.download import download_rais
    from pipeline.sources.rais.parse import parse_rais
    from pipeline.sources.rais.validate import validate_rais
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
    log("Downloading all sources in parallel...")
    download_tasks: dict[str, tuple[Callable[[str, Path, int], Path], str, Path, int]] = {
        "cnpj_empresas": (download_cnpj, urls.cnpj_empresas, raw_dir / "cnpj", t),
        "cnpj_qsa": (download_cnpj, urls.cnpj_qsa, raw_dir / "cnpj_qsa", t),
        "pncp": (download_pncp, urls.pncp_contratos, raw_dir / "pncp", t),
        "ceis": (download_ceis, urls.ceis, raw_dir / "ceis", t),
        "cnep": (download_cnep, urls.cnep, raw_dir / "cnep", t),
        "cepim": (download_cepim, urls.cepim, raw_dir / "cepim", t),
        "servidores": (download_servidores, urls.servidores, raw_dir / "servidores", t),
        "tse": (download_doacoes, urls.tse_doacoes, raw_dir / "tse", t),
        "rais": (download_rais, urls.rais, raw_dir / "rais", t),
        "juntas_comerciais": (download_juntas_comerciais, urls.juntas_comerciais, raw_dir / "juntas_comerciais", t),
        "comprasnet": (download_comprasnet, urls.comprasnet_base, raw_dir / "comprasnet", t),
    }

    raw_paths: dict[str, Path] = {}
    with ThreadPoolExecutor(max_workers=len(download_tasks)) as pool:
        future_to_name: dict[Future[Path], str] = {
            pool.submit(fn, url, dest, timeout): name for name, (fn, url, dest, timeout) in download_tasks.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            raw_paths[name] = future.result()  # raises on error
            log(f"  Downloaded: {name}")

    # ---- Phase 2: Parse + validate (parallel — Polars releases the GIL internally) ----
    # ADR: ThreadPoolExecutor gives near-linear speedup here because Polars CSV/JSON
    # parsing releases the GIL, allowing true parallel CPU utilisation across threads.
    # Sancoes is handled as a single task that concats CEIS+CNEP+CEPIM before validate
    # to preserve the validate_sancoes invariant that all three lists are merged first.
    log("Parsing and validating in parallel...")

    def _parse_and_write(parse_fn: Callable[[], pl.DataFrame], output_path: Path) -> int:
        df = parse_fn()
        write_parquet(df, output_path)
        return len(df)

    parse_tasks: dict[str, tuple[Callable[[], pl.DataFrame], Path]] = {
        "empresas": (
            lambda: validate_empresas(parse_empresas(raw_paths["cnpj_empresas"])),
            staging_dir / "empresas.parquet",
        ),
        "qsa": (
            lambda: validate_qsa(parse_qsa(raw_paths["cnpj_qsa"])),
            staging_dir / "qsa.parquet",
        ),
        "contratos": (
            lambda: validate_contratos(parse_contratos(raw_paths["pncp"])),
            staging_dir / "contratos.parquet",
        ),
        "sancoes": (
            lambda: validate_sancoes(
                pl.concat([
                    parse_ceis(raw_paths["ceis"]),
                    parse_cnep(raw_paths["cnep"]),
                    parse_cepim(raw_paths["cepim"]),
                ])
            ),
            staging_dir / "sancoes.parquet",
        ),
        "servidores": (
            lambda: validate_servidores(parse_servidores(raw_paths["servidores"])),
            staging_dir / "servidores.parquet",
        ),
        "doacoes": (
            lambda: validate_doacoes(parse_doacoes(raw_paths["tse"])),
            staging_dir / "doacoes.parquet",
        ),
        "rais": (
            lambda: validate_rais(parse_rais(raw_paths["rais"])),
            staging_dir / "rais.parquet",
        ),
        "juntas_comerciais": (
            lambda: validate_qsa_diffs(parse_qsa_diffs(raw_paths["juntas_comerciais"])),
            staging_dir / "juntas_comerciais.parquet",
        ),
        "comprasnet": (
            lambda: validate_comprasnet(parse_comprasnet(raw_paths["comprasnet"])),
            staging_dir / "comprasnet.parquet",
        ),
    }

    with ThreadPoolExecutor(max_workers=len(parse_tasks)) as pool:
        parse_futures: dict[Future[int], str] = {
            pool.submit(_parse_and_write, fn, path): name
            for name, (fn, path) in parse_tasks.items()
        }
        for future in as_completed(parse_futures):
            name = parse_futures[future]
            count = future.result()  # raises on error, aborting the pipeline
            log(f"  Parsed {name}: {count:,} rows")


if __name__ == "__main__":
    cfg = load_config()
    run_pipeline(cfg)
