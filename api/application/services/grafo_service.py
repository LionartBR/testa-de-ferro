# api/application/services/grafo_service.py
from __future__ import annotations

from api.domain.fornecedor.value_objects import CNPJ
from api.domain.societario.repository import SocietarioRepository

from ..dtos.grafo_dto import ArestaDTO, GrafoDTO, NoDTO


class GrafoService:
    def __init__(self, societario_repo: SocietarioRepository) -> None:
        self._societario_repo = societario_repo

    def obter_grafo(self, cnpj: CNPJ, max_nos: int = 50) -> GrafoDTO:
        nos_raw, arestas_raw = self._societario_repo.grafo_2_niveis(cnpj, max_nos)
        return GrafoDTO(
            nos=[
                NoDTO(
                    id=str(n["id"]),
                    tipo=str(n["tipo"]),
                    label=str(n["label"]),
                    score=int(n["score"]) if n.get("score") is not None else None,  # type: ignore[call-overload]
                    qtd_alertas=int(n["qtd_alertas"]) if n.get("qtd_alertas") is not None else None,  # type: ignore[call-overload]
                )
                for n in nos_raw
            ],
            arestas=[
                ArestaDTO(
                    source=str(a["source"]),
                    target=str(a["target"]),
                    tipo=str(a["tipo"]),
                    label=str(a["label"]) if a.get("label") else None,
                )
                for a in arestas_raw
            ],
            truncado=len(nos_raw) >= max_nos,
        )
