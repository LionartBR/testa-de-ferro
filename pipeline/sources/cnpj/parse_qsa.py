# pipeline/sources/cnpj/parse_qsa.py
#
# Parse Receita Federal QSA (Quadro de Sócios e Administradores) CSV.
#
# Design decisions:
#   - QSA CPFs arrive partially masked from Receita ("***222333**"). The
#     visible middle digits are preserved as-is for the match_servidor_socio
#     transform. The cpf_parcial column carries the raw masked string.
#   - QUALIFICACAO_SOCIO is kept as a numeric string (Receita codebook) — it
#     is mapped to a human-readable label in the transform layer once the full
#     codebook is loaded. Storing the code here avoids hard-coding all 70+ codes.
#   - DATA_ENTRADA follows the YYYYMMDD format identical to empresas.
#   - The output targets the bridge_fornecedor_socio staging schema. The
#     pk_socio and fk_fornecedor FK resolution happens in the transform layer
#     after both dim_fornecedor and dim_socio are built.
#
# Invariants:
#   - cnpj_basico is preserved as the raw 8-digit root (not formatted) so the
#     transform layer can JOIN it directly against empresas rows before the
#     full CNPJ is known.
#   - nome_socio is uppercased and stripped for consistent matching.
from __future__ import annotations

from pathlib import Path

import polars as pl


def parse_qsa(raw_path: Path) -> pl.DataFrame:
    """Parse QSA CSV into a sócios staging DataFrame.

    The result carries cnpj_basico (raw, not formatted) as the join key to
    dim_fornecedor, plus socio identity fields used to build dim_socio and
    bridge_fornecedor_socio.

    Args:
        raw_path: Path to the raw QSA CSV file (Latin-1 encoding, semicolons).

    Returns:
        Polars DataFrame with sócio records, one row per (empresa, sócio) pair.
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    # Normalise column names.
    raw = raw.rename({col: col.strip().upper() for col in raw.columns})

    n = len(raw)

    def _safe_str(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    # DATA_ENTRADA: YYYYMMDD → Date (same format as empresas).
    if "DATA_ENTRADA" in raw.columns:
        data_entrada = (
            raw["DATA_ENTRADA"].str.strip_chars().str.to_date(format="%Y%m%d", strict=False).alias("data_entrada")
        )
    else:
        data_entrada = pl.Series("data_entrada", [None] * n, dtype=pl.Date)

    return pl.DataFrame(
        {
            "cnpj_basico": _safe_str("CNPJ_BASICO").str.zfill(8),
            "nome_socio": _safe_str("NOME_SOCIO").str.to_uppercase(),
            "cpf_parcial": _safe_str("CNPJ_CPF_SOCIO"),
            "qualificacao_socio": _safe_str("QUALIFICACAO_SOCIO"),
            "data_entrada": data_entrada,
            "percentual_capital": pl.Series("percentual_capital", [None] * n, dtype=pl.Float64),
        }
    )
