# pipeline/sources/tse/parse.py
#
# Parse TSE (Tribunal Superior Eleitoral) prestação de contas CSV into a
# staging DataFrame for fato_doacao.
#
# Design decisions:
#   - TSE uses semicolons and Latin-1 encoding, consistent with other government
#     sources.
#   - The donor (CPF_CNPJ_DOADOR) may be either a CPF (11 digits) or a CNPJ
#     (14 digits). The type is inferred from the digit count after stripping
#     non-digit characters. The classificar_doador function isolates this
#     classification logic for independent testability.
#   - If the donor is a CNPJ (empresa), cnpj_doador is set and cpf_doador is
#     null. If the donor is a CPF (pessoa fisica), cpf_doador is set and
#     cnpj_doador is null. The transform layer resolves which suppliers and
#     socios are linked to each donation.
#   - pk_doacao and FK columns are placeholders, resolved by transform.
#   - valor is parsed as Float64; zero and negative values are dropped at
#     the validate step.
#   - data_receita uses ISO-8601 format (YYYY-MM-DD) from TSE exports.
#   - All column construction uses Polars Series/expressions instead of Python
#     lists (.to_list()) to avoid duplicating data in RAM (~750 MB savings).
#
# Invariants:
#   - tipo_doador is either "CPF" or "CNPJ" or None.
#   - valor > 0 in every validated row.
#   - ano_eleicao is non-null.
from __future__ import annotations

from pathlib import Path

import polars as pl


def classificar_doador(doc: str | None) -> str | None:
    """Classify a donor document as 'CPF' (11 digits) or 'CNPJ' (14 digits).

    Strips all non-digit characters before counting. Documents with any other
    digit count are classified as None (unknown).

    Args:
        doc: Raw donor document string (may include punctuation).

    Returns:
        'CPF' if 11 digits, 'CNPJ' if 14 digits, None otherwise.
    """
    if doc is None:
        return None
    digits = "".join(ch for ch in doc if ch.isdigit())
    if len(digits) == 11:
        return "CPF"
    if len(digits) == 14:
        return "CNPJ"
    return None


def parse_doacoes(raw_path: Path) -> pl.DataFrame:
    """Parse TSE prestacao de contas CSV into a fato_doacao staging DataFrame.

    Args:
        raw_path: Path to the TSE CSV file (Latin-1 encoding, semicolons).

    Returns:
        Polars DataFrame with one row per donation, with FK columns as nulls.
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

    def _safe_series(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    # Classify donor type from raw document string (vectorized).
    doc_series = _safe_series("CPF_CNPJ_DOADOR")
    digits = doc_series.str.replace_all(r"[^\d]", "")
    digit_len = digits.str.len_chars()

    _doador_tmp = pl.DataFrame({"doc": doc_series, "digit_len": digit_len})
    _doador_tmp = _doador_tmp.with_columns(
        [
            pl.when(pl.col("digit_len") == 11)
            .then(pl.lit("CPF"))
            .when(pl.col("digit_len") == 14)
            .then(pl.lit("CNPJ"))
            .otherwise(pl.lit(None))
            .alias("tipo_doador"),
            pl.when(pl.col("digit_len") == 14).then(pl.col("doc")).otherwise(pl.lit(None)).alias("cnpj_doador"),
            pl.when(pl.col("digit_len") == 11).then(pl.col("doc")).otherwise(pl.lit(None)).alias("cpf_doador"),
        ]
    )

    # Parse valor as Float64 (vectorized: comma → dot → cast).
    if "VALOR_RECEITA" in raw.columns:
        valor_series = raw["VALOR_RECEITA"].str.strip_chars().str.replace(",", ".").cast(pl.Float64, strict=False)
    else:
        valor_series = pl.Series("valor", [None] * n, dtype=pl.Float64)

    # Parse data_receita as Date.
    data_series: pl.Series
    if "DATA_RECEITA" in raw.columns:
        data_series = raw["DATA_RECEITA"].str.strip_chars().str.to_date(format="%Y-%m-%d", strict=False)
    else:
        data_series = pl.Series("data_receita", [None] * n, dtype=pl.Date)

    # Extract ano_eleicao as Int64 (vectorized: strip → cast).
    if "ANO_ELEICAO" in raw.columns:
        ano_series = raw["ANO_ELEICAO"].str.strip_chars().cast(pl.Int64, strict=False)
    else:
        ano_series = pl.Series("ano_eleicao", [None] * n, dtype=pl.Int64)

    return pl.DataFrame(
        {
            "pk_doacao": list(range(1, n + 1)),
            "ano_eleicao": ano_series,
            "doc_doador": doc_series,
            "tipo_doador": _doador_tmp["tipo_doador"],
            "cnpj_doador": _doador_tmp["cnpj_doador"],
            "cpf_doador": _doador_tmp["cpf_doador"],
            "nome_doador": _safe_series("NOME_DOADOR"),
            "doc_candidato": _safe_series("CPF_CNPJ_CANDIDATO"),
            "nome_candidato": _safe_series("NOME_CANDIDATO"),
            "partido_candidato": _safe_series("PARTIDO_CANDIDATO"),
            "cargo_candidato": _safe_series("CARGO_CANDIDATO"),
            "uf_candidato": _safe_series("UF_CANDIDATO"),
            "tipo_recurso": _safe_series("TIPO_RECURSO"),
            "valor": valor_series,
            "data_receita": data_series,
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
            "fk_socio": pl.Series("fk_socio", [None] * n, dtype=pl.Int64),
            "fk_candidato": pl.Series("fk_candidato", [None] * n, dtype=pl.Int64),
            "fk_tempo": pl.Series("fk_tempo", [None] * n, dtype=pl.Int64),
        }
    )
