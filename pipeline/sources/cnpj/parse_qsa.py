# pipeline/sources/cnpj/parse_qsa.py
#
# Parse Receita Federal QSA (Quadro de Sócios e Administradores) CSV.
#
# ADR: The Receita Federal Socios CSV has NO header row.
# Layout (11 columns, semicolon-separated, Latin-1):
#   col0: CNPJ_BASICO (8 digits)
#   col1: IDENTIFICADOR_SOCIO (1=PJ, 2=PF, 3=Estrangeiro)
#   col2: NOME_SOCIO
#   col3: CNPJ_CPF_SOCIO (masked: ***222333**)
#   col4: QUALIFICACAO_SOCIO (numeric code)
#   col5: DATA_ENTRADA (YYYYMMDD)
#   col6: PAIS
#   col7: REPRESENTANTE_LEGAL (CPF masked)
#   col8: NOME_REPRESENTANTE
#   col9: QUALIFICACAO_REPRESENTANTE
#   col10: FAIXA_ETARIA
#
# Invariants:
#   - cnpj_basico is preserved as the raw 8-digit root (not formatted).
#   - nome_socio is uppercased and stripped for consistent matching.
from __future__ import annotations

from pathlib import Path

import polars as pl

_QSA_COLUMNS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "cnpj_cpf_socio",
    "qualificacao_socio",
    "data_entrada",
    "pais",
    "representante_legal",
    "nome_representante",
    "qualificacao_representante",
    "faixa_etaria",
]


def parse_qsa(raw_path: Path) -> pl.DataFrame:
    """Parse QSA CSV into a socios staging DataFrame.

    Args:
        raw_path: Path to the raw QSA CSV file (Latin-1 encoding, semicolons,
                  no header, 11 columns).

    Returns:
        Polars DataFrame with socio records, one row per (empresa, socio) pair.
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        has_header=False,
        new_columns=_QSA_COLUMNS,
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    n = len(raw)

    # Parse DATA_ENTRADA from YYYYMMDD string to Date.
    data_entrada = raw["data_entrada"].cast(pl.Utf8).str.strip_chars().str.to_date(format="%Y%m%d", strict=False)

    return pl.DataFrame(
        {
            "cnpj_basico": raw["cnpj_basico"].cast(pl.Utf8).str.strip_chars().str.zfill(8),
            "nome_socio": raw["nome_socio"].cast(pl.Utf8).str.strip_chars().str.to_uppercase(),
            "cpf_parcial": raw["cnpj_cpf_socio"].cast(pl.Utf8).str.strip_chars(),
            "qualificacao_socio": raw["qualificacao_socio"].cast(pl.Utf8).str.strip_chars(),
            "data_entrada": data_entrada,
            "percentual_capital": pl.Series("percentual_capital", [None] * n, dtype=pl.Float64),
        }
    )
