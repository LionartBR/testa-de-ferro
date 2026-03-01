# tests/pipeline/test_cruzamentos.py
#
# Specification tests for cross-reference enrichments.
from __future__ import annotations

from datetime import date

import polars as pl

from pipeline.transform.cruzamentos import detectar_mesmo_endereco, enriquecer_socios


def _make_empresas(
    cnpj_basicos: list[str],
    cnpjs: list[str],
    logradouros: list[str | None] | None = None,
) -> pl.DataFrame:
    """Helper: build a minimal empresas DataFrame."""
    n = len(cnpj_basicos)
    data: dict[str, list[object]] = {
        "cnpj_basico": cnpj_basicos,
        "cnpj": cnpjs,
        "pk_fornecedor": list(range(1, n + 1)),
    }
    if logradouros is not None:
        data["logradouro"] = logradouros
    return pl.DataFrame(data)


def _make_socios(
    cnpj_basicos: list[str],
    nomes: list[str],
) -> pl.DataFrame:
    """Helper: build a minimal socios DataFrame."""
    return pl.DataFrame(
        {
            "cnpj_basico": cnpj_basicos,
            "nome_socio": nomes,
        }
    )


def _make_sancoes(
    cnpj_basicos: list[str],
    data_fims: list[str | None] | None = None,
) -> pl.DataFrame:
    """Helper: build a minimal sancoes DataFrame using cnpj_basico.

    Args:
        cnpj_basicos: CNPJ roots of sanctioned companies.
        data_fims: Optional end dates (ISO format str or None for active/indefinite).
    """
    n = len(cnpj_basicos)
    data: dict[str, object] = {
        "cnpj_basico": cnpj_basicos,
        "tipo_sancao": ["CEIS"] * n,
        "data_fim": data_fims if data_fims is not None else [None] * n,
    }
    return pl.DataFrame(data).with_columns(pl.col("data_fim").cast(pl.Date))


def test_socio_em_empresa_sancionada_marcado() -> None:
    """is_sancionado is True when the sócio belongs to a sanctioned company."""
    empresas = _make_empresas(
        cnpj_basicos=["11111111", "22222222"],
        cnpjs=["11.111.111/0001-01", "22.222.222/0001-02"],
    )
    # Sócio JOAO appears in company 11111111 (sanctioned).
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["JOAO SILVA"],
    )
    sancoes = _make_sancoes(cnpj_basicos=["11111111"])

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    assert "is_sancionado" in result.columns
    joao_row = result.filter(pl.col("nome_socio") == "JOAO SILVA")
    assert joao_row["is_sancionado"][0] is True


def test_socio_sem_sancao_nao_marcado() -> None:
    """is_sancionado is False for sócios whose companies have no sanction."""
    empresas = _make_empresas(
        cnpj_basicos=["33333333"],
        cnpjs=["33.333.333/0001-03"],
    )
    socios = _make_socios(
        cnpj_basicos=["33333333"],
        nomes=["MARIA OLIVEIRA"],
    )
    # Empty sancoes — no company is sanctioned.
    sancoes = pl.DataFrame(
        {
            "cnpj_basico": pl.Series([], dtype=pl.Utf8),
            "tipo_sancao": pl.Series([], dtype=pl.Utf8),
            "data_fim": pl.Series([], dtype=pl.Date),
        }
    )

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    assert result["is_sancionado"][0] is False


def test_qtd_empresas_governo_contada() -> None:
    """qtd_empresas_governo counts distinct CNPJs each sócio appears in."""
    empresas = _make_empresas(
        cnpj_basicos=["11111111", "22222222", "33333333"],
        cnpjs=["11.111.111/0001-01", "22.222.222/0001-02", "33.333.333/0001-03"],
    )
    # CARLOS appears in 3 companies; ANA appears in 1.
    socios = _make_socios(
        cnpj_basicos=["11111111", "22222222", "33333333", "11111111"],
        nomes=["CARLOS MENDES", "CARLOS MENDES", "CARLOS MENDES", "ANA FARIAS"],
    )
    sancoes = pl.DataFrame(
        {
            "cnpj_basico": pl.Series([], dtype=pl.Utf8),
            "tipo_sancao": pl.Series([], dtype=pl.Utf8),
            "data_fim": pl.Series([], dtype=pl.Date),
        }
    )

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    carlos = result.filter(pl.col("nome_socio") == "CARLOS MENDES")["qtd_empresas_governo"].to_list()
    ana = result.filter(pl.col("nome_socio") == "ANA FARIAS")["qtd_empresas_governo"].to_list()

    assert all(v == 3 for v in carlos)
    assert all(v == 1 for v in ana)


def test_mesmo_endereco_detectado() -> None:
    """Two companies at the same logradouro + numero are returned as a pair."""
    empresas = _make_empresas(
        cnpj_basicos=["11111111", "22222222"],
        cnpjs=["11.111.111/0001-01", "22.222.222/0001-02"],
        logradouros=[
            "RUA DAS FLORES, 123",
            "RUA DAS FLORES, 123",
        ],
    )

    result = detectar_mesmo_endereco(empresas)

    assert len(result) == 1
    assert "cnpj_a" in result.columns
    assert "cnpj_b" in result.columns
    assert "endereco_compartilhado" in result.columns
    # Both CNPJs appear in the pair.
    cnpjs_found = {result["cnpj_a"][0], result["cnpj_b"][0]}
    assert cnpjs_found == {"11.111.111/0001-01", "22.222.222/0001-02"}


def test_sancao_expirada_nao_marca_is_sancionado() -> None:
    """Sanção expirada (data_fim no passado) NÃO marca is_sancionado.

    ADR: "Sanção expirada → indicador SANCAO_HISTORICA (score). Nunca alerta."
    """
    empresas = _make_empresas(
        cnpj_basicos=["11111111", "22222222"],
        cnpjs=["11.111.111/0001-01", "22.222.222/0001-02"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["JOAO SILVA"],
    )
    # Sanção expirada em 2022.
    sancoes = _make_sancoes(
        cnpj_basicos=["11111111"],
        data_fims=["2022-12-31"],
    )

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    joao_row = result.filter(pl.col("nome_socio") == "JOAO SILVA")
    assert joao_row["is_sancionado"][0] is False, (
        "Sanção expirada não deve marcar is_sancionado"
    )


def test_sancao_vigente_marca_is_sancionado() -> None:
    """Sanção vigente (data_fim NULL = indefinida) marca is_sancionado."""
    empresas = _make_empresas(
        cnpj_basicos=["11111111"],
        cnpjs=["11.111.111/0001-01"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["JOAO SILVA"],
    )
    # Sanção sem data_fim = vigente.
    sancoes = _make_sancoes(
        cnpj_basicos=["11111111"],
        data_fims=[None],
    )

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    joao_row = result.filter(pl.col("nome_socio") == "JOAO SILVA")
    assert joao_row["is_sancionado"][0] is True


def test_sancao_vigente_com_data_fim_futura_marca_is_sancionado() -> None:
    """Sanção com data_fim futura (ainda não expirou) marca is_sancionado."""
    empresas = _make_empresas(
        cnpj_basicos=["11111111"],
        cnpjs=["11.111.111/0001-01"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["JOAO SILVA"],
    )
    # Sanção com data_fim no futuro.
    sancoes = _make_sancoes(
        cnpj_basicos=["11111111"],
        data_fims=["2027-06-30"],
    )

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    joao_row = result.filter(pl.col("nome_socio") == "JOAO SILVA")
    assert joao_row["is_sancionado"][0] is True


def test_empresa_com_sancao_expirada_e_vigente_marca_is_sancionado() -> None:
    """Empresa com uma sanção expirada e outra vigente: is_sancionado = True."""
    empresas = _make_empresas(
        cnpj_basicos=["11111111"],
        cnpjs=["11.111.111/0001-01"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["JOAO SILVA"],
    )
    # Duas sanções: uma expirada, outra vigente.
    sancoes = _make_sancoes(
        cnpj_basicos=["11111111", "11111111"],
        data_fims=["2022-12-31", None],
    )

    result = enriquecer_socios(socios, sancoes, empresas, referencia=date(2026, 2, 28))

    joao_row = result.filter(pl.col("nome_socio") == "JOAO SILVA")
    assert joao_row["is_sancionado"][0] is True


def test_mesmo_endereco_ignora_complemento() -> None:
    """Complemento (sala/andar) is ignored — two companies match even if complemento differs."""
    # ADR: match is by logradouro + numero only, without complemento.
    empresas = _make_empresas(
        cnpj_basicos=["44444444", "55555555"],
        cnpjs=["44.444.444/0001-04", "55.555.555/0001-05"],
        logradouros=[
            # The number is the same; complemento text is embedded in the
            # logradouro field as part of real-world Receita data.
            "AV BRASIL 456 SALA 301",
            "AV BRASIL 456 ANDAR 7",
        ],
    )

    result = detectar_mesmo_endereco(empresas)

    # Should detect a match because both share "AV BRASIL|456".
    assert len(result) == 1
