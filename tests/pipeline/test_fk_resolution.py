# tests/pipeline/test_fk_resolution.py
#
# Specification tests for FK resolution in the pipeline transform layer.
#
# These tests verify that staging fact tables (contratos, sancoes, doacoes)
# get their fk_fornecedor populated by joining on CNPJ against empresas.
from __future__ import annotations

import polars as pl

from pipeline.transform.resolve_fks import (
    resolver_fk_contratos,
    resolver_fk_doacoes,
    resolver_fk_sancoes,
)


def _empresas_fixture() -> pl.DataFrame:
    """Minimal empresas DataFrame with pk_fornecedor and cnpj."""
    return pl.DataFrame(
        {
            "pk_fornecedor": [1, 2],
            "cnpj": ["11.222.333/0001-81", "33.000.167/0001-01"],
        }
    )


def test_resolver_fk_contrato_match() -> None:
    """cnpj_fornecedor '11222333000181' matches cnpj '11.222.333/0001-81'."""
    empresas = _empresas_fixture()
    contratos = pl.DataFrame(
        {
            "pk_contrato": [1],
            "cnpj_fornecedor": ["11222333000181"],
            "fk_fornecedor": pl.Series([None], dtype=pl.Int64),
            "valor": [100000.0],
        }
    )

    result = resolver_fk_contratos(contratos, empresas)

    assert result["fk_fornecedor"][0] == 1


def test_resolver_fk_contrato_sem_match_null() -> None:
    """CNPJ not present in empresas leaves fk_fornecedor as NULL."""
    empresas = _empresas_fixture()
    contratos = pl.DataFrame(
        {
            "pk_contrato": [1],
            "cnpj_fornecedor": ["99999999000199"],
            "fk_fornecedor": pl.Series([None], dtype=pl.Int64),
            "valor": [100000.0],
        }
    )

    result = resolver_fk_contratos(contratos, empresas)

    assert result["fk_fornecedor"][0] is None


def test_resolver_fk_sancao_match() -> None:
    """Formatted CNPJ in sancao matches after stripping punctuation."""
    empresas = _empresas_fixture()
    sancoes = pl.DataFrame(
        {
            "pk_sancao": [1],
            "cnpj": ["11.222.333/0001-81"],
            "fk_fornecedor": pl.Series([None], dtype=pl.Int64),
            "data_fim": pl.Series([None], dtype=pl.Date),
        }
    )

    result = resolver_fk_sancoes(sancoes, empresas)

    assert result["fk_fornecedor"][0] == 1


def test_resolver_fk_sancao_cpf_nao_resolve() -> None:
    """Sanction with CPF (11 digits) does not resolve to fk_fornecedor."""
    empresas = _empresas_fixture()
    sancoes = pl.DataFrame(
        {
            "pk_sancao": [1],
            "cnpj": ["12345678901"],
            "fk_fornecedor": pl.Series([None], dtype=pl.Int64),
            "data_fim": pl.Series([None], dtype=pl.Date),
        }
    )

    result = resolver_fk_sancoes(sancoes, empresas)

    assert result["fk_fornecedor"][0] is None


def test_resolver_fk_doacao_match() -> None:
    """cnpj_doador in doacoes resolves to fk_fornecedor."""
    empresas = _empresas_fixture()
    doacoes = pl.DataFrame(
        {
            "pk_doacao": [1],
            "fk_fornecedor": pl.Series([None], dtype=pl.Int64),
            "valor": [15000.0],
            "doc_doador": ["11222333000181"],
            "tipo_doador": ["CNPJ"],
        }
    )

    result = resolver_fk_doacoes(doacoes, empresas)

    assert result["fk_fornecedor"][0] == 1


def test_resolver_fk_preserva_colunas() -> None:
    """Output preserves all original columns from the input DataFrame."""
    empresas = _empresas_fixture()
    contratos = pl.DataFrame(
        {
            "pk_contrato": [1, 2],
            "cnpj_fornecedor": ["11222333000181", "99999999000199"],
            "fk_fornecedor": pl.Series([None, None], dtype=pl.Int64),
            "valor": [100000.0, 200000.0],
            "objeto": ["Servicos de TI", "Compra material"],
            "num_licitacao": ["PE-001/2025", "PE-002/2025"],
        }
    )

    result = resolver_fk_contratos(contratos, empresas)

    assert set(contratos.columns).issubset(set(result.columns))
    assert len(result) == len(contratos)
    assert result["valor"].to_list() == [100000.0, 200000.0]
    assert result["objeto"].to_list() == ["Servicos de TI", "Compra material"]
