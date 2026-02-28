# pipeline/sources/tse/parse.py
#
# Parse TSE (Tribunal Superior Eleitoral) prestação de contas CSVs into a
# staging DataFrame for fato_doacao.
#
# Design decisions:
#   - TSE uses semicolons and Latin-1 encoding, consistent with other government
#     sources. Columns are quoted with double-quotes.
#   - The TSE ZIP contains per-state receitas_candidatos CSVs. download_doacoes
#     extracts all of them into a directory. This parser reads and concatenates
#     every receitas_candidatos_*.csv found in the directory.
#   - TSE column names use abbreviated prefixes (NR_, NM_, VR_, DT_, AA_, SG_,
#     DS_). The mapping to our staging schema is:
#       NR_CPF_CNPJ_DOADOR  -> doc_doador
#       NM_DOADOR           -> nome_doador
#       VR_RECEITA          -> valor
#       DT_RECEITA          -> data_receita  (DD/MM/YYYY format)
#       AA_ELEICAO          -> ano_eleicao
#       NR_CPF_CANDIDATO    -> doc_candidato
#       NM_CANDIDATO        -> nome_candidato
#       SG_PARTIDO          -> partido_candidato
#       DS_CARGO            -> cargo_candidato
#       SG_UF               -> uf_candidato
#       DS_ORIGEM_RECEITA   -> tipo_recurso
#   - The donor (NR_CPF_CNPJ_DOADOR) may be either a CPF (11 digits) or a CNPJ
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
#   - data_receita uses DD/MM/YYYY format from TSE exports (not ISO-8601).
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


def _read_single_csv(csv_path: Path) -> pl.DataFrame:
    """Read a single TSE receitas_candidatos CSV file.

    Args:
        csv_path: Path to the CSV file (Latin-1, semicolon-delimited, quoted).

    Returns:
        Raw Polars DataFrame with all columns as Utf8.
    """
    return pl.read_csv(
        csv_path,
        separator=";",
        encoding="latin1",
        infer_schema_length=0,
        null_values=["", "NULL"],
        truncate_ragged_lines=True,
        quote_char='"',
    )


def parse_doacoes(raw_path: Path) -> pl.DataFrame:
    """Parse TSE prestacao de contas CSVs into a fato_doacao staging DataFrame.

    Accepts either a directory containing receitas_candidatos_*.csv files
    (the normal case from download_doacoes) or a single CSV file path
    (for testing convenience).

    Args:
        raw_path: Path to a directory of TSE CSVs or a single CSV file.

    Returns:
        Polars DataFrame with one row per donation, with FK columns as nulls.
    """
    if raw_path.is_dir():
        csv_files = sorted(
            f
            for f in raw_path.glob("receitas_candidatos_*.csv")
            if "doador_originario" not in f.name.lower()
            and "_brasil" not in f.name.lower()
        )
        if not csv_files:
            raise FileNotFoundError(
                f"No receitas_candidatos_*.csv files found in {raw_path}"
            )
        frames = [_read_single_csv(f) for f in csv_files]
        raw = pl.concat(frames, how="diagonal_relaxed")
    else:
        raw = _read_single_csv(raw_path)

    # Normalise column names: strip whitespace, quotes, and uppercase.
    raw = raw.rename(
        {col: col.strip().strip('"').upper() for col in raw.columns}
    )

    n = len(raw)

    def _safe_series(col: str) -> pl.Series:
        if col in raw.columns:
            return raw[col].str.strip_chars()
        return pl.Series(col, [None] * n, dtype=pl.Utf8)

    # ADR: TSE column names use abbreviated prefixes (NR_, NM_, VR_, etc.).
    # We map from TSE column names to our staging schema here.

    # Classify donor type from raw document string (vectorized).
    doc_series = _safe_series("NR_CPF_CNPJ_DOADOR")
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
            pl.when(pl.col("digit_len") == 14)
            .then(pl.col("doc"))
            .otherwise(pl.lit(None))
            .alias("cnpj_doador"),
            pl.when(pl.col("digit_len") == 11)
            .then(pl.col("doc"))
            .otherwise(pl.lit(None))
            .alias("cpf_doador"),
        ]
    )

    # Parse valor as Float64 (vectorized: comma -> dot -> cast).
    if "VR_RECEITA" in raw.columns:
        valor_series = (
            raw["VR_RECEITA"]
            .str.strip_chars()
            .str.replace(",", ".")
            .cast(pl.Float64, strict=False)
        )
    else:
        valor_series = pl.Series("valor", [None] * n, dtype=pl.Float64)

    # Parse data_receita as Date (DD/MM/YYYY format from TSE).
    data_series: pl.Series
    if "DT_RECEITA" in raw.columns:
        data_series = (
            raw["DT_RECEITA"]
            .str.strip_chars()
            .str.to_date(format="%d/%m/%Y", strict=False)
        )
    else:
        data_series = pl.Series("data_receita", [None] * n, dtype=pl.Date)

    # Extract ano_eleicao as Int64 (vectorized: strip -> cast).
    if "AA_ELEICAO" in raw.columns:
        ano_series = (
            raw["AA_ELEICAO"].str.strip_chars().cast(pl.Int64, strict=False)
        )
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
            "nome_doador": _safe_series("NM_DOADOR"),
            "doc_candidato": _safe_series("NR_CPF_CANDIDATO"),
            "nome_candidato": _safe_series("NM_CANDIDATO"),
            "partido_candidato": _safe_series("SG_PARTIDO"),
            "cargo_candidato": _safe_series("DS_CARGO"),
            "uf_candidato": _safe_series("SG_UF"),
            "tipo_recurso": _safe_series("DS_ORIGEM_RECEITA"),
            "valor": valor_series,
            "data_receita": data_series,
            "fk_fornecedor": pl.Series(
                "fk_fornecedor", [None] * n, dtype=pl.Int64
            ),
            "fk_socio": pl.Series("fk_socio", [None] * n, dtype=pl.Int64),
            "fk_candidato": pl.Series(
                "fk_candidato", [None] * n, dtype=pl.Int64
            ),
            "fk_tempo": pl.Series("fk_tempo", [None] * n, dtype=pl.Int64),
        }
    )
