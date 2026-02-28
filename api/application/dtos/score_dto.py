# api/application/dtos/score_dto.py
from pydantic import BaseModel


class IndicadorDTO(BaseModel):
    tipo: str
    peso: int
    descricao: str
    evidencia: str


class ScoreDTO(BaseModel):
    valor: int
    faixa: str
    indicadores: list[IndicadorDTO]
