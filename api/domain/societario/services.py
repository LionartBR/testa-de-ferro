# api/domain/societario/services.py
#
# Pure domain service for corporate graph (grafo societario) operations.
#
# Design decisions:
#   - GrafoSocietarioService contains only pure, stateless logic. No IO, no
#     database access, no async. The shell (DuckDBSocietarioRepo.grafo_2_niveis)
#     is responsible for fetching raw nodes and edges; this service enforces
#     the 50-node visual limit and removes orphan edges that would reference
#     removed nodes.
#   - Nodes and edges are typed as list[dict[str, object]] to remain agnostic
#     of the final DTO shape (GrafoDTO). The service operates on the structural
#     contract ("id" key on nodes, "source"/"target" keys on edges) without
#     importing infrastructure or application DTOs.
#   - Truncation is always from the front of the list (nos[:max_nos]) so that
#     the root node — always inserted first by the repo — is never dropped.
#   - The method returns a 3-tuple (nos, arestas, foi_truncado) so the caller
#     can add a UI hint when the graph was capped.
#
# Invariants:
#   - aplicar_limite is a pure function: same inputs always produce the same
#     output with no side effects.
#   - An edge is included only when BOTH its source and target are present in
#     the truncated node set. Partial edges (dangling references) are removed.
#   - When len(nos) == max_nos, foi_truncado is False — the limit is exclusive.
from __future__ import annotations


class GrafoSocietarioService:
    """Pure domain service for corporate graph operations.

    All methods are static because the service is stateless: there is no
    configuration or injected dependency. Callers instantiate this class only
    for namespacing purposes and may also call methods directly on the class.
    """

    @staticmethod
    def aplicar_limite(
        nos: list[dict[str, object]],
        arestas: list[dict[str, object]],
        max_nos: int = 50,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]], bool]:
        """Truncate graph nodes to max_nos and remove edges to missing nodes.

        Keeps the first max_nos nodes (preserving the root node, which the repo
        always inserts first) and filters out any edge whose source or target is
        no longer in the retained node set.

        Args:
            nos:      List of node dicts. Each dict must have an "id" key whose
                      value is used to match against edge endpoints.
            arestas:  List of edge dicts. Each dict must have "source" and
                      "target" keys whose values correspond to node "id" values.
            max_nos:  Maximum number of nodes to keep. Must be >= 1.

        Returns:
            A 3-tuple of:
              - truncated_nos:    The retained node list (len <= max_nos).
              - filtered_arestas: Edges whose both endpoints are in the retained
                                  node set.
              - foi_truncado:     True if nodes were removed, False otherwise.
        """
        if len(nos) <= max_nos:
            return nos, arestas, False

        truncated_nos = nos[:max_nos]
        valid_ids = {node["id"] for node in truncated_nos}

        filtered_arestas = [edge for edge in arestas if edge["source"] in valid_ids and edge["target"] in valid_ids]

        return truncated_nos, filtered_arestas, True
