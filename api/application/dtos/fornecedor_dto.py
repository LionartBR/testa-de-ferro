# api/application/dtos/fornecedor_dto.py
from pydantic import BaseModel


class SocioDTO(BaseModel):
    nome: str
    qualificacao: str | None
    is_servidor_publico: bool
    orgao_lotacao: str | None


class SancaoDTO(BaseModel):
    tipo: str
    orgao_sancionador: str
    motivo: str
    data_inicio: str
    data_fim: str | None
    vigente: bool


class FornecedorResumoDTO(BaseModel):
    cnpj: str
    razao_social: str
    situacao: str
    score_risco: int
    faixa_risco: str
    qtd_alertas: int
    max_severidade: str | None
    total_contratos: int
    valor_total: str
