# pipeline/sources/cnpj/parse_empresas.py
#
# Parse Receita Federal empresas CSV into a typed staging DataFrame.
#
# ADR: The Receita Federal distributes CNPJ data across TWO files:
#   - Empresas0.zip: 7 columns (cnpj_basico, razao_social, natureza_juridica,
#     qualificacao_responsavel, capital_social, porte, ente_federativo).
#     NO header row. Semicolon-separated. Latin-1 encoding.
#   - Estabelecimentos0.zip: 30 columns (cnpj_basico, cnpj_ordem, cnpj_dv,
#     identificador_matriz_filial, nome_fantasia, situacao_cadastral, ...,
#     cnae_fiscal, logradouro, municipio, uf, cep, ...).
#     NO header row. Semicolon-separated. Latin-1 encoding.
#
# This parser reads ONLY the Empresas file. The Estabelecimentos data
# (CNPJ completo, endereço, CNAE, situação) is merged in the transform
# layer via juntas_comerciais or a separate Estabelecimentos parse step.
#
# Since the Empresas file only has cnpj_basico (8 digits), the full CNPJ
# is built as XX.XXX.XXX/0001-XX (assuming matriz) until enriched with
# Estabelecimentos data.
#
# Invariants:
#   - Output columns must match dim_fornecedor in schema.sql (minus computed
#     score/alert columns filled by the transform layer).
#   - has_header=False because Receita CSVs have no header row.
from __future__ import annotations

from pathlib import Path

import polars as pl

# Mapping from Receita Federal numeric situacao code to human-readable label.
_SITUACAO_MAP: dict[int, str] = {
    1: "NULA",
    2: "ATIVA",
    3: "SUSPENSA",
    4: "INAPTA",
    8: "BAIXADA",
}

# Column names for the Receita Federal Empresas file (no header in CSV).
_EMPRESAS_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo",
]


def parse_empresas(raw_path: Path) -> pl.DataFrame:
    """Parse Receita Federal empresas CSV into a typed staging DataFrame.

    Args:
        raw_path: Path to the raw Receita Federal CSV file (Latin-1 encoding,
                  no header, semicolon-separated, 7 columns).

    Returns:
        Polars DataFrame with columns matching dim_fornecedor staging schema.
    """
    raw = pl.read_csv(
        raw_path,
        separator=";",
        encoding="latin1",
        has_header=False,
        new_columns=_EMPRESAS_COLUMNS,
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
    )

    n = len(raw)

    # Build cnpj_basico padded to 8 digits.
    basico = raw["cnpj_basico"].cast(pl.Utf8).str.strip_chars().fill_null("00000000").str.zfill(8)

    # Build a placeholder CNPJ: XX.XXX.XXX/0001-00 (matriz assumed).
    # The real CNPJ_ORDEM and CNPJ_DV come from Estabelecimentos.
    cnpj_series = (
        basico.str.slice(0, 2)
        + "."
        + basico.str.slice(2, 3)
        + "."
        + basico.str.slice(5, 3)
        + "/0001-00"
    )

    # Parse capital social: comma -> dot, then cast to Float64.
    capital_series = (
        raw["capital_social"]
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(",", ".")
        .cast(pl.Float64, strict=False)
    )

    return pl.DataFrame(
        {
            "pk_fornecedor": pl.int_range(1, n + 1, eager=True),
            "cnpj": cnpj_series,
            "cnpj_basico": basico,
            "razao_social": raw["razao_social"].cast(pl.Utf8).str.strip_chars(),
            "data_abertura": pl.Series("data_abertura", [None] * n, dtype=pl.Date),
            "capital_social": capital_series,
            "cnae_principal": pl.Series("cnae_principal", [None] * n, dtype=pl.Utf8),
            "cnae_descricao": pl.Series("cnae_descricao", [None] * n, dtype=pl.Utf8),
            "logradouro": pl.Series("logradouro", [None] * n, dtype=pl.Utf8),
            "municipio": pl.Series("municipio", [None] * n, dtype=pl.Utf8),
            "uf": pl.Series("uf", [None] * n, dtype=pl.Utf8),
            "cep": pl.Series("cep", [None] * n, dtype=pl.Utf8),
            "situacao": pl.Series("situacao", [None] * n, dtype=pl.Utf8),
        }
    )
