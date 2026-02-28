# pipeline/sources/servidores/parse.py
#
# Parse Portal da Transparência servidores CSV into a staging DataFrame.
#
# Design decisions:
#   - CPFs arrive in partially masked format: "***.222.333-**". The visible
#     middle digits are extracted by stripping non-digits and taking positions
#     3-8 (0-indexed) of the 11-digit CPF pattern. This is enough for the
#     match_servidor_socio transform to narrow candidates before name matching.
#   - The column naming follows Portal da Transparência's 2024 export format.
#     Column names are normalised to uppercase before any access.
#   - nome is uppercased and stripped for consistent matching against QSA data.
#   - The output columns match what match_servidor_socio.py expects: nome,
#     digitos_visiveis, cargo, orgao_lotacao.
#   - cpf_mascarado is preserved as-is for audit/debugging purposes.
#   - is_servidor_publico is always True here — this file is the definitive
#     list of federal servants.
#
# Invariants:
#   - nome is non-null in every validated row.
#   - digitos_visiveis contains only digits, 6 characters long (positions 3-8
#     of the CPF), or null if the masked format is not recognisable.
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

# Pattern for the Portal da Transparência masked CPF format: ***.DDD.DDD-**
# Captures the 6 visible digits (positions 3-8 of the CPF).
_CPF_MASK_PATTERN = re.compile(r"\*{3}\.(\d{3})\.(\d{3})-\*{2}")


def extrair_digitos_visiveis(cpf_mascarado: str) -> str | None:
    """Extract the 6 visible digits from a masked CPF string.

    The Portal da Transparência masks CPFs as '***.222.333-**', where positions
    4-9 (1-indexed in the formatted string) are visible. This function extracts
    them as a 6-character digit string for matching purposes.

    Args:
        cpf_mascarado: Masked CPF string in '***.DDD.DDD-**' format.

    Returns:
        6 visible digits concatenated (e.g. '222333'), or None if the format
        does not match (e.g. empty string, different masking scheme).
    """
    if not cpf_mascarado:
        return None
    match = _CPF_MASK_PATTERN.match(cpf_mascarado.strip())
    if not match:
        return None
    return match.group(1) + match.group(2)


def parse_servidores(raw_path: Path) -> pl.DataFrame:
    """Parse servidores CSV from Portal da Transparência.

    Extracts the visible CPF digits from the masked column, normalises names,
    and collects cargo and orgao_lotacao for downstream matching.

    Args:
        raw_path: Path to the servidores CSV file (Latin-1, semicolons).

    Returns:
        Polars DataFrame with columns: nome, cpf_mascarado, digitos_visiveis,
        cargo, orgao_lotacao, is_servidor_publico.
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

    # Extract visible CPF digits using the pure function above.
    cpf_raw = _safe_str("CPF")
    digitos_visiveis = cpf_raw.map_elements(
        lambda v: extrair_digitos_visiveis(v) if v is not None else None,
        return_dtype=pl.Utf8,
    ).alias("digitos_visiveis")

    # Normalise nome to uppercase for matching consistency.
    nome = _safe_str("NOME").str.to_uppercase().alias("nome")

    return pl.DataFrame(
        {
            "nome": nome,
            "cpf_mascarado": cpf_raw.alias("cpf_mascarado"),
            "digitos_visiveis": digitos_visiveis,
            "cargo": _safe_str("CARGO_DESCRICAO").alias("cargo"),
            "orgao_lotacao": _safe_str("ORGAO_LOTACAO").alias("orgao_lotacao"),
            "is_servidor_publico": pl.Series(
                "is_servidor_publico", [True] * n, dtype=pl.Boolean
            ),
        }
    )
