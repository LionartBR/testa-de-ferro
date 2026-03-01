# tests/pipeline/test_alertas_batch.py
#
# Specification tests for pipeline alert batch detectors.
#
# Design decisions:
#   - All helper builders produce the minimum columns each detector needs.
#     Extra columns that real staging DataFrames carry are deliberately absent
#     here so that tests document exactly what each detector depends on.
#   - We call the private detector functions directly (not detectar_alertas_batch)
#     so each test is a tight specification of a single rule.
#   - The detectado_em timestamp is pinned to a fixed value to avoid flakiness
#     in assertions that check for its presence.
#   - "cnpj_basico" is zero-padded to 8 chars — matches the normalization that
#     the production detectors apply internally.
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from pipeline.transform.alertas import (
    _rodizio_licitacao_batch,  # type: ignore[attr-defined]  # private API under test
    _socio_sancionado_em_outra_batch,  # type: ignore[attr-defined]  # private API under test
    _testa_de_ferro_batch,  # type: ignore[attr-defined]  # private API under test
)

# Fixed timestamp used across all tests to keep assertions deterministic.
_DETECTADO_EM = datetime(2026, 2, 28, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None)

# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------


def _make_empresas(
    pk_fornecedores: list[int],
    cnpj_basicos: list[str],
    capital_sociais: list[float] | None = None,
    data_aberturas: list[str | None] | None = None,
) -> pl.DataFrame:
    """Build a minimal empresas (dim_fornecedor) staging DataFrame."""
    n = len(pk_fornecedores)
    data: dict[str, object] = {
        "pk_fornecedor": pk_fornecedores,
        "cnpj_basico": cnpj_basicos,
        "capital_social": capital_sociais if capital_sociais is not None else [50000.0] * n,
        "data_abertura": data_aberturas if data_aberturas is not None else [None] * n,
    }
    return pl.DataFrame(data).with_columns(
        pl.col("data_abertura").cast(pl.Date),
    )


def _make_socios(
    cnpj_basicos: list[str],
    nomes: list[str],
    is_servidor_publico: list[bool] | None = None,
    qtd_empresas_governo: list[int] | None = None,
) -> pl.DataFrame:
    """Build a minimal socios (dim_socio) staging DataFrame."""
    n = len(cnpj_basicos)
    return pl.DataFrame(
        {
            "cnpj_basico": cnpj_basicos,
            "nome_socio": nomes,
            "is_servidor_publico": is_servidor_publico if is_servidor_publico is not None else [False] * n,
            "qtd_empresas_governo": qtd_empresas_governo if qtd_empresas_governo is not None else [1] * n,
        }
    )


def _make_contratos(
    fk_fornecedores: list[int],
    num_licitacoes: list[str | None],
    orgao_codigos: list[str] | None = None,
    data_assinaturas: list[str | None] | None = None,
) -> pl.DataFrame:
    """Build a minimal contratos (fato_contrato) staging DataFrame."""
    n = len(fk_fornecedores)
    return pl.DataFrame(
        {
            "fk_fornecedor": fk_fornecedores,
            "num_licitacao": num_licitacoes,
            "codigo_orgao": orgao_codigos if orgao_codigos is not None else ["ORG001"] * n,
            "data_assinatura": data_assinaturas if data_assinaturas is not None else [None] * n,
        }
    ).with_columns(
        pl.col("data_assinatura").cast(pl.Date),
    )


# ---------------------------------------------------------------------------
# _rodizio_licitacao_batch
# ---------------------------------------------------------------------------


def test_rodizio_licitacao_3_licitacoes_em_comum() -> None:
    """Two companies with shared socio that appear in 3+ same bids → GRAVISSIMO for both."""
    # Empresas 1 and 2 share socio "JOSE SILVA".
    empresas = _make_empresas(
        pk_fornecedores=[1, 2],
        cnpj_basicos=["11111111", "22222222"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111", "22222222"],
        nomes=["JOSE SILVA", "JOSE SILVA"],
    )
    # Both companies participate in the same 3 licitações.
    contratos = _make_contratos(
        fk_fornecedores=[1, 2, 1, 2, 1, 2],
        num_licitacoes=["LIC001", "LIC001", "LIC002", "LIC002", "LIC003", "LIC003"],
    )

    rows = _rodizio_licitacao_batch(empresas, socios, contratos, _DETECTADO_EM)

    fk_fornecedores_alertados = {r["fk_fornecedor"] for r in rows}
    assert fk_fornecedores_alertados == {1, 2}, "Both companies in the pair must be alerted"

    for row in rows:
        assert row["tipo_alerta"] == "RODIZIO_LICITACAO"
        assert row["severidade"] == "GRAVISSIMO"
        assert row["fk_socio"] is None, "Company-level alert — fk_socio must be None"


def test_rodizio_licitacao_2_licitacoes_nao_gera_alerta() -> None:
    """Only 2 shared bids (below the threshold of 3) → no alert."""
    empresas = _make_empresas(
        pk_fornecedores=[1, 2],
        cnpj_basicos=["11111111", "22222222"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111", "22222222"],
        nomes=["JOSE SILVA", "JOSE SILVA"],
    )
    # Only 2 shared licitações — below threshold.
    contratos = _make_contratos(
        fk_fornecedores=[1, 2, 1, 2],
        num_licitacoes=["LIC001", "LIC001", "LIC002", "LIC002"],
    )

    rows = _rodizio_licitacao_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "2 shared bids must not trigger the alert (threshold is 3)"


def test_rodizio_licitacao_sem_socio_comum_nao_gera() -> None:
    """Companies with different socios do not trigger alert even if they share bids."""
    empresas = _make_empresas(
        pk_fornecedores=[1, 2],
        cnpj_basicos=["11111111", "22222222"],
    )
    # Each company has a completely different socio.
    socios = _make_socios(
        cnpj_basicos=["11111111", "22222222"],
        nomes=["ANA FARIAS", "CARLOS MENDES"],
    )
    contratos = _make_contratos(
        fk_fornecedores=[1, 2, 1, 2, 1, 2],
        num_licitacoes=["LIC001", "LIC001", "LIC002", "LIC002", "LIC003", "LIC003"],
    )

    rows = _rodizio_licitacao_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "No shared socio → no alert, regardless of bid overlap"


def test_rodizio_licitacao_empresa_aparece_no_maximo_uma_vez() -> None:
    """A single fornecedor appears in at most one alert row even when part of multiple flagged pairs."""
    # Company 1 shares socios with both 2 and 3.
    # Company 2 also shares a socio with 3.
    # Company 1 could appear in two pairs — but must be deduped to one alert row.
    empresas = _make_empresas(
        pk_fornecedores=[1, 2, 3],
        cnpj_basicos=["11111111", "22222222", "33333333"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111", "22222222", "11111111", "33333333", "22222222", "33333333"],
        nomes=["SOCIO_A", "SOCIO_A", "SOCIO_B", "SOCIO_B", "SOCIO_C", "SOCIO_C"],
    )
    # Pair (1,2) shares LIC001/LIC002/LIC003; pair (1,3) shares LIC004/LIC005/LIC006.
    contratos = _make_contratos(
        fk_fornecedores=[1, 2, 1, 2, 1, 2, 1, 3, 1, 3, 1, 3],
        num_licitacoes=[
            "LIC001",
            "LIC001",
            "LIC002",
            "LIC002",
            "LIC003",
            "LIC003",
            "LIC004",
            "LIC004",
            "LIC005",
            "LIC005",
            "LIC006",
            "LIC006",
        ],
    )

    rows = _rodizio_licitacao_batch(empresas, socios, contratos, _DETECTADO_EM)

    fk_counts: dict[int, int] = {}
    for row in rows:
        fk = int(row["fk_fornecedor"])  # type: ignore[arg-type]
        fk_counts[fk] = fk_counts.get(fk, 0) + 1

    for fk, count in fk_counts.items():
        assert count == 1, f"Fornecedor {fk} appeared {count} times — expected at most 1"


def test_rodizio_licitacao_contratos_sem_num_licitacao_ignorados() -> None:
    """Contracts with null num_licitacao are not counted for rodizio detection."""
    empresas = _make_empresas(
        pk_fornecedores=[1, 2],
        cnpj_basicos=["11111111", "22222222"],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111", "22222222"],
        nomes=["JOSE SILVA", "JOSE SILVA"],
    )
    # 3 contracts but num_licitacao is null — these are direct contracts, not bids.
    contratos = _make_contratos(
        fk_fornecedores=[1, 2, 1, 2, 1, 2],
        num_licitacoes=[None, None, None, None, None, None],
    )

    rows = _rodizio_licitacao_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "Null num_licitacao must not be counted as a shared bid"


def test_rodizio_licitacao_dataframe_vazio_nao_crasha() -> None:
    """Empty DataFrames must return empty list without raising."""
    empresas = _make_empresas(pk_fornecedores=[], cnpj_basicos=[])
    socios = _make_socios(cnpj_basicos=[], nomes=[])
    contratos = _make_contratos(fk_fornecedores=[], num_licitacoes=[])

    rows = _rodizio_licitacao_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == []


# ---------------------------------------------------------------------------
# _testa_de_ferro_batch
# ---------------------------------------------------------------------------


def _days_ago(n: int) -> str:
    """Return ISO date string for n days before the pinned test date."""
    base = datetime(2026, 2, 28)
    return (base - timedelta(days=n)).strftime("%Y-%m-%d")


def test_testa_de_ferro_todas_condicoes_verdadeiras() -> None:
    """All 4 conditions met simultaneously → GRAVISSIMO alert."""
    # Capital < R$10k, opened 6 months before first contract, single orgao,
    # socio is servidor publico.
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[_days_ago(365)],  # Opened 1 year ago.
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["MARIA SILVA"],
        is_servidor_publico=[True],
        qtd_empresas_governo=[1],
    )
    # First (and only) contract signed 6 months after company opened → < 12 months.
    contratos = _make_contratos(
        fk_fornecedores=[1],
        num_licitacoes=["LIC001"],
        orgao_codigos=["ORG001"],
        data_assinaturas=[_days_ago(180)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert len(rows) == 1
    assert rows[0]["fk_fornecedor"] == 1
    assert rows[0]["tipo_alerta"] == "TESTA_DE_FERRO"
    assert rows[0]["severidade"] == "GRAVISSIMO"
    assert rows[0]["fk_socio"] is None


def test_testa_de_ferro_capital_alto_nao_gera() -> None:
    """Capital >= R$10k blocks the alert even when all other conditions are met."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[10000.0],  # Exactly at threshold — must NOT trigger.
        data_aberturas=[_days_ago(365)],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["MARIA SILVA"],
        is_servidor_publico=[True],
    )
    contratos = _make_contratos(
        fk_fornecedores=[1],
        num_licitacoes=["LIC001"],
        orgao_codigos=["ORG001"],
        data_assinaturas=[_days_ago(180)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "Capital == R$10k is not below threshold — no alert"


def test_testa_de_ferro_empresa_antiga_nao_gera() -> None:
    """Company opened > 12 months before first contract → condition 2 fails → no alert."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[_days_ago(760)],  # Opened ~2 years ago.
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["MARIA SILVA"],
        is_servidor_publico=[True],
    )
    # Contract signed 6 months ago → gap between opening and contract > 12 months.
    contratos = _make_contratos(
        fk_fornecedores=[1],
        num_licitacoes=["LIC001"],
        orgao_codigos=["ORG001"],
        data_assinaturas=[_days_ago(180)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "Company opened > 12 months before first contract → no alert"


def test_testa_de_ferro_multiplos_orgaos_nao_gera() -> None:
    """Contracts with multiple orgaos → not an exclusive supplier → condition 3 fails → no alert."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[_days_ago(365)],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["MARIA SILVA"],
        is_servidor_publico=[True],
    )
    # Two different orgaos — not exclusive.
    contratos = _make_contratos(
        fk_fornecedores=[1, 1],
        num_licitacoes=["LIC001", "LIC002"],
        orgao_codigos=["ORG001", "ORG002"],
        data_assinaturas=[_days_ago(180), _days_ago(90)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "Contracts with 2 orgaos → not exclusive → no alert"


def test_testa_de_ferro_socio_sem_flag_nao_gera() -> None:
    """Socio is neither servidor nor in 3+ companies → condition 4 fails → no alert."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[_days_ago(365)],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["JOAO COMUM"],
        is_servidor_publico=[False],
        qtd_empresas_governo=[1],  # Below the threshold of 3.
    )
    contratos = _make_contratos(
        fk_fornecedores=[1],
        num_licitacoes=["LIC001"],
        orgao_codigos=["ORG001"],
        data_assinaturas=[_days_ago(180)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "Socio not servidor and not in 3+ companies → condition 4 fails → no alert"


def test_testa_de_ferro_socio_em_3_empresas_ativa_condicao_4() -> None:
    """Socio in 3+ government companies satisfies condition 4 even if not servidor."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[_days_ago(365)],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["CARLOS REINCIDENTE"],
        is_servidor_publico=[False],  # Not a servidor.
        qtd_empresas_governo=[3],  # But appears in 3 government companies.
    )
    contratos = _make_contratos(
        fk_fornecedores=[1],
        num_licitacoes=["LIC001"],
        orgao_codigos=["ORG001"],
        data_assinaturas=[_days_ago(180)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert len(rows) == 1
    assert rows[0]["tipo_alerta"] == "TESTA_DE_FERRO"


def test_testa_de_ferro_sem_contratos_nao_gera() -> None:
    """No contracts at all → conditions 2 and 3 cannot be evaluated → no alert."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[_days_ago(365)],
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["MARIA SILVA"],
        is_servidor_publico=[True],
    )
    contratos = _make_contratos(fk_fornecedores=[], num_licitacoes=[])

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "No contracts → no alert"


def test_testa_de_ferro_empresa_sem_data_abertura_nao_gera() -> None:
    """Company with null data_abertura cannot satisfy condition 2 → no alert."""
    empresas = _make_empresas(
        pk_fornecedores=[1],
        cnpj_basicos=["11111111"],
        capital_sociais=[5000.0],
        data_aberturas=[None],  # Unknown opening date.
    )
    socios = _make_socios(
        cnpj_basicos=["11111111"],
        nomes=["MARIA SILVA"],
        is_servidor_publico=[True],
    )
    contratos = _make_contratos(
        fk_fornecedores=[1],
        num_licitacoes=["LIC001"],
        orgao_codigos=["ORG001"],
        data_assinaturas=[_days_ago(180)],
    )

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == [], "Null data_abertura → cannot verify empresa_recente → no alert"


def test_testa_de_ferro_dataframe_vazio_nao_crasha() -> None:
    """Empty DataFrames must return empty list without raising."""
    empresas = _make_empresas(pk_fornecedores=[], cnpj_basicos=[])
    socios = _make_socios(cnpj_basicos=[], nomes=[])
    contratos = _make_contratos(fk_fornecedores=[], num_licitacoes=[])

    rows = _testa_de_ferro_batch(empresas, socios, contratos, _DETECTADO_EM)

    assert rows == []


# ---------------------------------------------------------------------------
# _socio_sancionado_em_outra_batch
# ---------------------------------------------------------------------------


def _make_socios_sancionado(
    cnpj_basicos: list[str],
    nomes: list[str],
    is_sancionado: list[bool],
    sancionado_cnpj_basicos: list[str | None] | None = None,
    sancionado_razao_socials: list[str | None] | None = None,
) -> pl.DataFrame:
    """Build a socios DataFrame with is_sancionado and enrichment columns."""
    n = len(cnpj_basicos)
    data: dict[str, object] = {
        "cnpj_basico": cnpj_basicos,
        "nome_socio": nomes,
        "is_sancionado": is_sancionado,
    }
    if sancionado_cnpj_basicos is not None:
        data["sancionado_cnpj_basico"] = sancionado_cnpj_basicos
    if sancionado_razao_socials is not None:
        data["sancionado_razao_social"] = sancionado_razao_socials
    return pl.DataFrame(data)


def test_socio_sancionado_em_outra_gera_alerta_grave() -> None:
    """Sócio flagged is_sancionado → alerta GRAVE com evidência da empresa sancionada."""
    empresas = _make_empresas(
        pk_fornecedores=[1, 2],
        cnpj_basicos=["11111111", "22222222"],
    )
    # Sócio JOAO is in company 22222222 but is flagged because company 11111111 is sanctioned.
    socios = _make_socios_sancionado(
        cnpj_basicos=["22222222"],
        nomes=["JOAO SILVA"],
        is_sancionado=[True],
        sancionado_cnpj_basicos=["11111111"],
        sancionado_razao_socials=["EMPRESA SANCIONADA LTDA"],
    )

    rows = _socio_sancionado_em_outra_batch(empresas, socios, _DETECTADO_EM)

    assert len(rows) == 1
    assert rows[0]["tipo_alerta"] == "SOCIO_SANCIONADO_EM_OUTRA"
    assert rows[0]["severidade"] == "GRAVE"
    assert "EMPRESA SANCIONADA LTDA" in str(rows[0]["evidencia"]), (
        "Evidência deve incluir razão social da empresa sancionada"
    )


def test_socio_sancionado_em_outra_evidencia_inclui_cnpj_sancionado() -> None:
    """Evidência do alerta deve incluir o CNPJ da empresa sancionada para rastreabilidade."""
    empresas = _make_empresas(
        pk_fornecedores=[1, 2],
        cnpj_basicos=["11111111", "22222222"],
    )
    socios = _make_socios_sancionado(
        cnpj_basicos=["22222222"],
        nomes=["MARIA OLIVEIRA"],
        is_sancionado=[True],
        sancionado_cnpj_basicos=["11111111"],
        sancionado_razao_socials=["CONSTRUTORA FANTASMA SA"],
    )

    rows = _socio_sancionado_em_outra_batch(empresas, socios, _DETECTADO_EM)

    assert len(rows) == 1
    evidencia = str(rows[0]["evidencia"])
    assert "11111111" in evidencia, "Evidência deve incluir CNPJ da empresa sancionada"
    assert "CONSTRUTORA FANTASMA SA" in evidencia, "Evidência deve incluir razão social"
