# tests/domain/test_grafo_service.py
#
# Specification-tests for GrafoSocietarioService.aplicar_limite.
# All tests are pure: no IO, no database, no mocks.
from api.domain.societario.services import GrafoSocietarioService


def test_sem_truncamento_retorna_tudo_intacto() -> None:
    """Under max_nos: all nodes and edges returned unchanged, foi_truncado=False."""
    nos = [{"id": "a"}, {"id": "b"}]
    arestas = [{"source": "a", "target": "b", "tipo": "socio_de"}]

    result_nos, result_arestas, foi_truncado = GrafoSocietarioService.aplicar_limite(nos, arestas, max_nos=50)

    assert result_nos == nos
    assert result_arestas == arestas
    assert foi_truncado is False


def test_com_truncamento_remove_nos_excedentes() -> None:
    """Over max_nos: only the first max_nos nodes are retained, foi_truncado=True."""
    nos = [{"id": f"n{i}"} for i in range(10)]
    arestas = [{"source": "n0", "target": f"n{i}", "tipo": "x"} for i in range(1, 10)]

    result_nos, _result_arestas, foi_truncado = GrafoSocietarioService.aplicar_limite(nos, arestas, max_nos=5)

    assert len(result_nos) == 5
    assert foi_truncado is True


def test_truncamento_remove_arestas_orfas() -> None:
    """Edges whose source or target was removed are not included in the result."""
    nos = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    arestas = [
        {"source": "a", "target": "b", "tipo": "x"},
        {"source": "a", "target": "c", "tipo": "x"},
    ]

    result_nos, result_arestas, foi_truncado = GrafoSocietarioService.aplicar_limite(nos, arestas, max_nos=2)

    # Retained: a, b — c is removed.
    assert len(result_nos) == 2
    assert foi_truncado is True
    # Only the a→b edge should survive; a→c is orphaned.
    retained_ids = {node["id"] for node in result_nos}
    for edge in result_arestas:
        assert edge["source"] in retained_ids
        assert edge["target"] in retained_ids
    assert len(result_arestas) == 1
    assert result_arestas[0]["target"] == "b"


def test_grafo_vazio_retorna_vazio_sem_truncamento() -> None:
    """Empty input graph: empty result, foi_truncado=False."""
    result_nos, result_arestas, foi_truncado = GrafoSocietarioService.aplicar_limite([], [], max_nos=50)

    assert result_nos == []
    assert result_arestas == []
    assert foi_truncado is False


def test_exatamente_no_limite_nao_trunca() -> None:
    """Exactly at max_nos nodes: no truncation occurs, foi_truncado=False."""
    nos = [{"id": f"n{i}"} for i in range(5)]
    arestas = [{"source": "n0", "target": "n1", "tipo": "x"}]

    result_nos, result_arestas, foi_truncado = GrafoSocietarioService.aplicar_limite(nos, arestas, max_nos=5)

    assert len(result_nos) == 5
    assert foi_truncado is False
    assert result_arestas == arestas
