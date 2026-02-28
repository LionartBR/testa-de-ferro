# pipeline/sources/comprasnet/parse.py
#
# Parse Comprasnet (SIASG) contract CSV into a staging DataFrame matching the
# fato_contrato schema used by the PNCP parser.
#
# Design decisions:
#   - SIASG CSVs use semicolons as separators and Latin-1 encoding, matching
#     the Portal da Transparência convention.
#   - The output schema is intentionally identical to parse_contratos (PNCP)
#     so that the orchestrator can pl.concat both DataFrames and run a single
#     validate + dedup pass, without source-specific branching downstream.
#   - FK columns (fk_fornecedor, fk_orgao, fk_tempo, fk_modalidade) are
#     null placeholders — resolved by the transform layer after dimensions
#     are built, same as PNCP.
#   - SIASG column names vary slightly between yearly extracts. We normalise
#     them to uppercase and use _safe_str to return null when a column is
#     absent rather than raising a KeyError.
#   - Data columns in SIASG arrive as DD/MM/YYYY strings. We parse them to
#     polars Date using that format, with strict=False to handle invalid dates.
#   - pk_contrato is a row-index placeholder; the transform layer re-keys it
#     after merging PNCP and Comprasnet records.
#   - valor is stored as Float64 with comma-to-dot normalisation for the
#     Brazilian decimal separator used in some SIASG exports.
#
# Invariants:
#   - Output columns match fato_contrato staging schema produced by PNCP parser.
#   - cnpj_fornecedor and codigo_orgao are raw strings for FK resolution.
#   - data_assinatura is Date or null — never a raw string.
from __future__ import annotations

from pathlib import Path

import polars as pl


def parse_comprasnet(raw_path: Path) -> pl.DataFrame:
    """Parse a Comprasnet SIASG CSV into a typed contratos staging DataFrame.

    The result has the same column schema as parse_contratos (PNCP), so both
    sources can be concatenated and validated by a single validate_contratos
    call.

    Args:
        raw_path: Path to the raw Comprasnet CSV file (Latin-1, semicolons).

    Returns:
        Polars DataFrame with fato_contrato staging columns; FK columns are null.
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    raw = raw.rename({col: col.strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    def _parse_date_br(col: str) -> pl.Series:
        """Parse a DD/MM/YYYY date column. Returns Date or null."""
        if col in raw.columns:
            return raw[col].str.strip_chars().str.to_date(format="%d/%m/%Y", strict=False)
        return pl.Series(col, [None] * n, dtype=pl.Date)

    def _parse_valor(col: str) -> pl.Series:
        """Parse a currency column with optional comma decimal separator."""
        if col not in raw.columns:
            return pl.Series(col, [None] * n, dtype=pl.Float64)
        return raw[col].str.strip_chars().str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)

    # Map common SIASG column name variants to canonical names.
    cnpj_fornecedor = _safe_str("CNPJ_FORNECEDOR")
    codigo_orgao = _safe_str("CODIGO_ORGAO")
    nome_orgao = _safe_str("NOME_ORGAO")
    num_licitacao = _safe_str("NUM_LICITACAO")
    valor = _parse_valor("VALOR_GLOBAL")
    objeto = _safe_str("OBJETO")
    data_assinatura = _parse_date_br("DATA_ASSINATURA")
    data_vigencia = _parse_date_br("DATA_VIGENCIA")
    modalidade_nome = _safe_str("MODALIDADE")
    modalidade_codigo = _safe_str("CODIGO_MODALIDADE")

    return pl.DataFrame(
        {
            "pk_contrato": list(range(1, n + 1)),
            "num_licitacao": num_licitacao,
            "cnpj_fornecedor": cnpj_fornecedor,
            "codigo_orgao": codigo_orgao,
            "nome_orgao": nome_orgao,
            "poder_orgao": pl.Series("poder_orgao", [None] * n, dtype=pl.Utf8),
            "esfera_orgao": pl.Series("esfera_orgao", [None] * n, dtype=pl.Utf8),
            "modalidade_nome": modalidade_nome,
            "modalidade_codigo": modalidade_codigo,
            "valor": valor,
            "objeto": objeto,
            "data_assinatura": data_assinatura,
            "data_vigencia": data_vigencia,
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
            "fk_orgao": pl.Series("fk_orgao", [None] * n, dtype=pl.Int64),
            "fk_tempo": pl.Series("fk_tempo", [None] * n, dtype=pl.Int64),
            "fk_modalidade": pl.Series("fk_modalidade", [None] * n, dtype=pl.Int64),
        }
    )
