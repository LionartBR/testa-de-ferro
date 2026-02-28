# api/application/dtos/alerta_dto.py
from pydantic import BaseModel


class AlertaCriticoDTO(BaseModel):
    tipo: str
    severidade: str
    descricao: str
    evidencia: str
