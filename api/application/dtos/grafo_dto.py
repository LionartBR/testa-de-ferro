# api/application/dtos/grafo_dto.py
from pydantic import BaseModel


class NoDTO(BaseModel):
    id: str
    tipo: str            # "empresa" | "socio"
    label: str
    score: int | None = None
    qtd_alertas: int | None = None


class ArestaDTO(BaseModel):
    source: str
    target: str
    tipo: str            # "socio_de"
    label: str | None = None


class GrafoDTO(BaseModel):
    nos: list[NoDTO]
    arestas: list[ArestaDTO]
    truncado: bool = False   # True se > 50 nos
