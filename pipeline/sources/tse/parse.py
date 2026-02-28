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

    def _safe_str_list(col: str) -> list[str | None]:
        if col not in raw.columns:
            return [None] * n
        return raw[col].str.strip_chars().to_list()

    # Classify donor type from raw document string.
    doc_raw_list: list[str | None] = _safe_str_list("CPF_CNPJ_DOADOR")
    tipo_doador_list: list[str | None] = [classificar_doador(d) for d in doc_raw_list]
    cnpj_doador_list: list[str | None] = [
        d if t == "CNPJ" else None for d, t in zip(doc_raw_list, tipo_doador_list)
    ]
    cpf_doador_list: list[str | None] = [
        d if t == "CPF" else None for d, t in zip(doc_raw_list, tipo_doador_list)
    ]

    # Parse valor as float.
    valor_list: list[float | None] = []
    if "VALOR_RECEITA" in raw.columns:
        for v in raw["VALOR_RECEITA"].to_list():
            if v is None:
                valor_list.append(None)
            else:
                try:
                    valor_list.append(float(str(v).replace(",", ".")))
                except ValueError:
                    valor_list.append(None)
    else:
        valor_list = [None] * n

    # Parse data_receita as Date.
    data_series: pl.Series
    if "DATA_RECEITA" in raw.columns:
        data_series = (
            raw["DATA_RECEITA"]
            .str.strip_chars()
            .str.to_date(format="%Y-%m-%d", strict=False)
        )
    else:
        data_series = pl.Series("data_receita", [None] * n, dtype=pl.Date)

    # Extract ano_eleicao as Int64.
    ano_list: list[int | None] = []
    if "ANO_ELEICAO" in raw.columns:
        for v in raw["ANO_ELEICAO"].to_list():
            if v is None:
                ano_list.append(None)
            else:
                try:
                    ano_list.append(int(v))
                except ValueError:
                    ano_list.append(None)
    else:
        ano_list = [None] * n

    return pl.DataFrame(
        {
            "pk_doacao": list(range(1, n + 1)),
            "ano_eleicao": pl.Series("ano_eleicao", ano_list, dtype=pl.Int64),
            "doc_doador": doc_raw_list,
            "tipo_doador": tipo_doador_list,
            "cnpj_doador": cnpj_doador_list,
            "cpf_doador": cpf_doador_list,
            "nome_doador": _safe_str_list("NOME_DOADOR"),
            "doc_candidato": _safe_str_list("CPF_CNPJ_CANDIDATO"),
            "nome_candidato": _safe_str_list("NOME_CANDIDATO"),
            "partido_candidato": _safe_str_list("PARTIDO_CANDIDATO"),
            "cargo_candidato": _safe_str_list("CARGO_CANDIDATO"),
            "uf_candidato": _safe_str_list("UF_CANDIDATO"),
            "tipo_recurso": _safe_str_list("TIPO_RECURSO"),
            "valor": pl.Series("valor", valor_list, dtype=pl.Float64),
            "data_receita": data_series.rename("data_receita"),
            "fk_fornecedor": pl.Series("fk_fornecedor", [None] * n, dtype=pl.Int64),
            "fk_socio": pl.Series("fk_socio", [None] * n, dtype=pl.Int64),
            "fk_candidato": pl.Series("fk_candidato", [None] * n, dtype=pl.Int64),
            "fk_tempo": pl.Series("fk_tempo", [None] * n, dtype=pl.Int64),
        }
    )
