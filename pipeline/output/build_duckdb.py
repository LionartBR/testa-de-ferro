# pipeline/output/build_duckdb.py
#
# Atomic DuckDB build: staging Parquet files → final .duckdb artifact.
#
# Design decisions:
#   - Atomicity is guaranteed by writing to a .tmp.duckdb first and only
#     renaming to the final path when the build succeeds. If anything fails,
#     the tmp file is deleted and the previous (or nonexistent) output file is
#     untouched. The operator never serves a partially-built database.
#   - Schema is read from schema.sql at build time (not imported as a module)
#     so the SQL file remains the single source of truth for table structure.
#   - Staging files are loaded via DuckDB's native read_parquet() — this avoids
#     loading large DataFrames into Python memory and lets DuckDB optimise the
#     ingestion directly.
#   - The mapping from staging file names to DuckDB table names is explicit and
#     co-located here. It does not derive from file names automatically — that
#     would silently skip files if a source adds a new staging file without a
#     corresponding mapping update.
#   - Tables without a staging source (dim_tempo, dim_modalidade, bridge tables,
#     fact tables that are computed from multiple sources) are populated by the
#     transform layer before build_duckdb is called, and their parquets are
#     expected in staging_dir under the names listed in STAGING_TO_TABLE.
#
# ADR: Why not use COPY or INSERT INTO SELECT for loading?
#   DuckDB's INSERT INTO ... SELECT * FROM read_parquet(...) is the most
#   direct path. It avoids materialising a Python-side DataFrame and respects
#   the FK constraints already defined in schema.sql. The alternative (Polars
#   to_arrow + conn.register) would require loading all rows into Python RAM.
from __future__ import annotations

from pathlib import Path

import duckdb

from pipeline.log import log

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# ---------------------------------------------------------------------------
# Mapping: staging parquet file stem → DuckDB dimension/fact table name.
#
# Keys are the stems of .parquet files under staging_dir.
# Values are the DuckDB table names as defined in schema.sql.
#
# Order matters: dimension tables must be loaded before fact/bridge tables
# that reference them via foreign keys.
# ---------------------------------------------------------------------------
# ADR: Tables whose staging parquets don't yet exist are silently skipped
# by _load_staging_data. Dimension lookup tables (dim_orgao, dim_tempo,
# dim_modalidade, dim_candidato) and bridge_fornecedor_socio require FK
# resolution logic not yet implemented. They are kept in the mapping so
# they will be auto-loaded when their staging parquets become available.
STAGING_TO_TABLE: dict[str, str] = {
    # Dimension tables (no FK dependencies on other dims)
    "empresas": "dim_fornecedor",
    "orgaos": "dim_orgao",
    "socios": "dim_socio",
    "modalidades": "dim_modalidade",
    "candidatos": "dim_candidato",
    "tempo": "dim_tempo",
    # Bridge table (depends on dim_fornecedor + dim_socio)
    # "qsa": "bridge_fornecedor_socio",  # TODO: needs FK resolution
    # Fact tables (depend on multiple dimensions)
    "contratos": "fato_contrato",
    "doacoes": "fato_doacao",
    "sancoes": "dim_sancao",
    "score_detalhe": "fato_score_detalhe",
    "alertas": "fato_alerta_critico",
}


def build_duckdb(staging_dir: Path, output_path: Path) -> Path:
    """Build the DuckDB database atomically from staging Parquet files.

    Steps:
        1. Create a temporary .duckdb file.
        2. Execute schema.sql to create all tables and indexes.
        3. Load each staging Parquet that exists into its corresponding table.
        4. Close the connection.
        5. Atomically rename the tmp file to output_path.

    If any step raises, the tmp file is deleted and output_path is untouched.

    Args:
        staging_dir:  Directory containing staging ``.parquet`` files.
        output_path:  Desired final path for the DuckDB database.

    Returns:
        The final output_path after a successful atomic rename.

    Raises:
        Any exception from duckdb or the filesystem propagates unchanged after
        cleaning up the tmp file.
    """
    tmp_path = output_path.with_suffix(".tmp.duckdb")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Guard: if a stale tmp file exists from a previous crashed run, remove it.
    if tmp_path.exists():
        tmp_path.unlink()

    try:
        conn = duckdb.connect(str(tmp_path))
        try:
            schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
            conn.execute(schema_sql)
            _load_staging_data(conn, staging_dir)
        finally:
            conn.close()

        # Atomic rename: remove old output if present, then rename tmp → final.
        if output_path.exists():
            output_path.unlink()
        tmp_path.rename(output_path)
        return output_path

    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def _load_staging_data(conn: duckdb.DuckDBPyConnection, staging_dir: Path) -> None:
    """Load all existing staging Parquet files into their DuckDB tables.

    Files that do not exist in staging_dir are silently skipped — this allows
    a partial pipeline run (e.g. only dimension tables built so far) to produce
    a valid, if incomplete, database. The completude check (completude.py) is
    responsible for enforcing that all required files exist before this function
    is called.

    Args:
        conn:        Open DuckDB connection to the database being built.
        staging_dir: Directory containing staging ``.parquet`` files.
    """
    loaded = 0
    for file_stem, table_name in STAGING_TO_TABLE.items():
        parquet_path = staging_dir / f"{file_stem}.parquet"
        if not parquet_path.exists():
            continue

        log(f"  Loading {file_stem} -> {table_name}...")

        # Get the table's column names so we only SELECT matching columns.
        # Staging parquets may carry extra columns used by transforms that
        # are not part of the DuckDB schema (e.g. cnpj_basico).
        # S608 noqa: table_name comes from the internal STAGING_TO_TABLE dict
        # (not from any external input) and posix_path is a local filesystem
        # path — neither value is user-controlled, so injection is not possible.
        table_cols = [
            row[0]
            for row in conn.execute(
                f"SELECT column_name FROM information_schema.columns "  # noqa: S608
                f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            ).fetchall()
        ]

        posix_path = parquet_path.as_posix()

        # Read the parquet schema to find which table columns are available.
        parquet_cols_result = conn.execute(
            f"SELECT name FROM parquet_schema('{posix_path}')"  # noqa: S608
        ).fetchall()
        parquet_cols = {row[0] for row in parquet_cols_result}

        # Select only columns that exist in both the table AND the parquet.
        shared_cols = [c for c in table_cols if c in parquet_cols]
        if not shared_cols:
            continue

        cols_sql = ", ".join(shared_cols)
        conn.execute(
            f"INSERT INTO {table_name} ({cols_sql}) "  # noqa: S608
            f"SELECT {cols_sql} FROM read_parquet('{posix_path}')"
        )
        loaded += 1

    log(f"  DuckDB: {loaded} tables loaded")


def validate_tables(output_path: Path) -> dict[str, int]:
    """Open the finished DuckDB and return row counts per table.

    Useful for smoke-testing the output without a full test suite.

    Args:
        output_path: Path to the completed DuckDB database.

    Returns:
        Mapping of table name → row count for every table in the schema.
    """
    conn = duckdb.connect(str(output_path), read_only=True)
    try:
        tables_result = conn.execute("SHOW TABLES").fetchall()
        counts: dict[str, int] = {}
        for (table_name,) in tables_result:
            # S608 noqa: table_name comes from SHOW TABLES (DuckDB-internal
            # identifiers), not from any external user input.
            row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # noqa: S608
            counts[table_name] = int(row[0]) if row else 0
        return counts
    finally:
        conn.close()
