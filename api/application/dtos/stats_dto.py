# api/application/dtos/stats_dto.py
from pydantic import BaseModel


class FonteMetadataDTO(BaseModel):
    ultima_atualizacao: str | None
    registros: int


class StatsDTO(BaseModel):
    total_fornecedores: int
    total_contratos: int
    total_alertas: int
    fontes: dict[str, FonteMetadataDTO]
