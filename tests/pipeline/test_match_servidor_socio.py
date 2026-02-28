# tests/pipeline/test_match_servidor_socio.py
#
# Specification tests for the sócio × servidor matching transform.
from __future__ import annotations

import polars as pl

from pipeline.transform.match_servidor_socio import match_servidor_socio


def _make_socios(
    nomes: list[str],
    cpf_parciais: list[str | None],
    cpf_hmacs: list[str | None] | None = None,
) -> pl.DataFrame:
    """Helper: build a minimal socios DataFrame for testing."""
    data: dict[str, list[object]] = {
        "cnpj_basico": ["12345678"] * len(nomes),
        "nome_socio": nomes,
        "cpf_parcial": cpf_parciais,
    }
    if cpf_hmacs is not None:
        data["cpf_hmac"] = cpf_hmacs
    return pl.DataFrame(data)


def _make_servidores(
    nomes: list[str],
    digitos: list[str | None],
    orgaos: list[str | None],
) -> pl.DataFrame:
    """Helper: build a minimal servidores DataFrame for testing."""
    return pl.DataFrame(
        {
            "nome": nomes,
            "digitos_visiveis": digitos,
            "orgao_lotacao": orgaos,
            "cargo": ["ANALISTA"] * len(nomes),
        }
    )


def test_match_por_nome_e_digitos() -> None:
    """Socio is matched when both name AND visible CPF digits match a servidor."""
    socios = _make_socios(
        nomes=["JOAO SILVA"],
        # QSA format without separators
        cpf_parciais=["***222333**"],
    )
    servidores = _make_servidores(
        nomes=["JOAO SILVA"],
        digitos=["222333"],
        orgaos=["MINISTERIO DA EDUCACAO"],
    )

    result = match_servidor_socio(socios, servidores)

    assert result["is_servidor_publico"][0] is True
    assert result["orgao_lotacao"][0] == "MINISTERIO DA EDUCACAO"


def test_nao_match_se_nome_diferente() -> None:
    """No match when only digits match but name differs."""
    socios = _make_socios(
        nomes=["JOAO SILVA"],
        cpf_parciais=["***222333**"],
    )
    servidores = _make_servidores(
        nomes=["MARIA OLIVEIRA"],
        digitos=["222333"],
        orgaos=["MINISTERIO DA SAUDE"],
    )

    result = match_servidor_socio(socios, servidores)

    assert result["is_servidor_publico"][0] is False
    assert result["orgao_lotacao"][0] is None


def test_nao_match_se_digitos_diferentes() -> None:
    """No match when only name matches but visible CPF digits differ."""
    socios = _make_socios(
        nomes=["JOAO SILVA"],
        cpf_parciais=["***111222**"],
    )
    servidores = _make_servidores(
        nomes=["JOAO SILVA"],
        digitos=["999888"],
        orgaos=["MINISTERIO DO TRABALHO"],
    )

    result = match_servidor_socio(socios, servidores)

    assert result["is_servidor_publico"][0] is False
    assert result["orgao_lotacao"][0] is None


def test_preserva_hmac_apos_match() -> None:
    """The cpf_hmac column survives the match enrichment unchanged."""
    expected_hmac = "a" * 64
    socios = _make_socios(
        nomes=["ANA COSTA"],
        cpf_parciais=["***444555**"],
        cpf_hmacs=[expected_hmac],
    )
    servidores = _make_servidores(
        nomes=["ANA COSTA"],
        digitos=["444555"],
        orgaos=["TCU"],
    )

    result = match_servidor_socio(socios, servidores)

    assert "cpf_hmac" in result.columns
    assert result["cpf_hmac"][0] == expected_hmac


def test_orgao_lotacao_preenchido_para_match() -> None:
    """orgao_lotacao is filled from the servidores table for matched sócios."""
    socios = _make_socios(
        nomes=["CARLOS PEREIRA", "LUCIA MENDES"],
        cpf_parciais=["***100200**", "***300400**"],
    )
    servidores = _make_servidores(
        nomes=["CARLOS PEREIRA"],
        digitos=["100200"],
        orgaos=["ANATEL"],
    )

    result = match_servidor_socio(socios, servidores)

    # CARLOS PEREIRA matched → has orgao_lotacao.
    carlos_row = result.filter(pl.col("nome_socio") == "CARLOS PEREIRA")
    assert carlos_row["orgao_lotacao"][0] == "ANATEL"
    assert carlos_row["is_servidor_publico"][0] is True

    # LUCIA MENDES not matched → no orgao_lotacao, not a servidor.
    lucia_row = result.filter(pl.col("nome_socio") == "LUCIA MENDES")
    assert lucia_row["orgao_lotacao"][0] is None
    assert lucia_row["is_servidor_publico"][0] is False
