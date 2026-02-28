# pipeline/sources/comprasnet/parse.py
#
# Parse Comprasnet (SIASG) contract CSV into a staging DataFrame matching the
# fato_contrato schema used by the PNCP parser.
#
# Design decisions:
#   - The repositorio.dados.gov.br CSV uses commas as separators and UTF-8
#     encoding, with ISO date format (YYYY-MM-DD).
#   - Column names in the new format are lowercase with underscores. We
#     normalise to uppercase for consistent lookup, then map from the new
#     column names to the canonical staging schema.
#   - The output schema is intentionally identical to parse_contratos (PNCP)
#     so that the orchestrator can pl.concat both DataFrames and run a single
#     validate + dedup pass, without source-specific branching downstream.
#   - FK columns (fk_fornecedor, fk_orgao, fk_tempo, fk_modalidade) are
#     null placeholders — resolved by the transform layer after dimensions
#     are built, same as PNCP.
#   - pk_contrato is a row-index placeholder; the transform layer re-keys it
#     after merging PNCP and Comprasnet records.
#   - valor is stored as Float64 with comma-to-dot normalisation kept for
#     backwards compatibility (the new format uses dot decimals natively,
#     but the replace is a no-op on well-formed values).
#
# ADR: Column renaming from the new repositorio.dados.gov.br format
#
# The old dadosabertos.compras.gov.br format used uppercase column names
# (CNPJ_FORNECEDOR, NUM_LICITACAO, DATA_VIGENCIA, etc.) with semicolons
# and Latin-1 encoding. The new format from repositorio.dados.gov.br uses
# lowercase names (fonecedor_cnpj_cpf_idgener, licitacao_numero, vigencia_fim)
# with commas and UTF-8. We rename new columns to the old canonical names
# after uppercasing so the rest of the parser remains unchanged.
#
# Invariants:
#   - Output columns match fato_contrato staging schema produced by PNCP parser.
#   - cnpj_fornecedor and codigo_orgao are raw strings for FK resolution.
#   - data_assinatura is Date or null — never a raw string.
from __future__ import annotations

from pathlib import Path

import polars as pl

# Mapping from the new repositorio.dados.gov.br column names (uppercased)
# to the canonical names used by the parser.
_COLUMN_RENAME_MAP: dict[str, str] = {
    "FONECEDOR_CNPJ_CPF_IDGENER": "CNPJ_FORNECEDOR",
    "LICITACAO_NUMERO": "NUM_LICITACAO",
    "VIGENCIA_FIM": "DATA_VIGENCIA",
    "ORGAO_CODIGO": "CODIGO_ORGAO",
    "ORGAO_NOME": "NOME_ORGAO",
    "VALOR_GLOBAL": "VALOR_GLOBAL",
    "DATA_ASSINATURA": "DATA_ASSINATURA",
    "MODALIDADE": "MODALIDADE",
    "CODIGO_MODALIDADE": "CODIGO_MODALIDADE",
    "OBJETO": "OBJETO",
    "PODER": "PODER",
    "ESFERA": "ESFERA",
}


def parse_comprasnet(raw_path: Path) -> pl.DataFrame:
    """Parse a Comprasnet SIASG CSV into a typed contratos staging DataFrame.

    The result has the same column schema as parse_contratos (PNCP), so both
    sources can be concatenated and validated by a single validate_contratos
    call.

    Args:
        raw_path: Path to the raw Comprasnet CSV file (UTF-8, comma-delimited).

    Returns:
        Polars DataFrame with fato_contrato staging columns; FK columns are null.
    """
    raw = pl.read_csv(
        raw_path,
        separator=",",
        encoding="utf8",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    # Normalise column names to uppercase and stripped, then apply rename map.
    raw = raw.rename({col: col.strip().upper() for col in raw.columns})
    rename_actual = {old: new for old, new in _COLUMN_RENAME_MAP.items() if old in raw.columns}
    if rename_actual:
        raw = raw.rename(rename_actual)

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    def _parse_date_iso(col: str) -> pl.Series:
        """Parse a YYYY-MM-DD date column. Returns Date or null."""
        if col in raw.columns:
            return raw[col].str.strip_chars().str.to_date(format="%Y-%m-%d", strict=False)
        return pl.Series(col, [None] * n, dtype=pl.Date)

    def _parse_valor(col: str) -> pl.Series:
        """Parse a currency column with optional comma decimal separator."""
        if col not in raw.columns:
            return pl.Series(col, [None] * n, dtype=pl.Float64)
        return raw[col].str.strip_chars().str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)

    # Map columns to canonical names.
    cnpj_fornecedor = _safe_str("CNPJ_FORNECEDOR")
    codigo_orgao = _safe_str("CODIGO_ORGAO")
    nome_orgao = _safe_str("NOME_ORGAO")
    num_licitacao = _safe_str("NUM_LICITACAO")
    valor = _parse_valor("VALOR_GLOBAL")
    objeto = _safe_str("OBJETO")
    data_assinatura = _parse_date_iso("DATA_ASSINATURA")
    data_vigencia = _parse_date_iso("DATA_VIGENCIA")
    modalidade_nome = _safe_str("MODALIDADE")
    modalidade_codigo = _safe_str("CODIGO_MODALIDADE")
    poder_orgao = _safe_str("PODER")
    esfera_orgao = _safe_str("ESFERA")

    return pl.DataFrame(
        {
            "pk_contrato": list(range(1, n + 1)),
            "num_licitacao": num_licitacao,
            "cnpj_fornecedor": cnpj_fornecedor,
            "codigo_orgao": codigo_orgao,
            "nome_orgao": nome_orgao,
            "poder_orgao": poder_orgao,
            "esfera_orgao": esfera_orgao,
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
