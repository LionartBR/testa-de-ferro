# api/application/dtos/contrato_dto.py
from pydantic import BaseModel


class ContratoResumoDTO(BaseModel):
    orgao_codigo: str
    valor: str  # Decimal serializado como string
    data_assinatura: str | None
    objeto: str | None
