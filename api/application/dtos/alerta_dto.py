# api/application/dtos/alerta_dto.py
from pydantic import BaseModel


class AlertaCriticoDTO(BaseModel):
    tipo: str
    severidade: str
    descricao: str
    evidencia: str


class AlertaFeedItemDTO(BaseModel):
    tipo: str
    severidade: str
    descricao: str
    evidencia: str
    detectado_em: str
    cnpj: str
    razao_social: str
    socio_nome: str | None = None
