# api/domain/societario/entities.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Socio:
    """Socio no dominio de leitura. cpf_hmac ja vem hashado do pipeline.
    Nunca armazena CPF em texto claro."""
    cpf_hmac: str
    nome: str
    qualificacao: str | None = None
    is_servidor_publico: bool = False
    orgao_lotacao: str | None = None
    is_sancionado: bool = False
    qtd_empresas_governo: int = 0


@dataclass(frozen=True)
class VinculoSocietario:
    """Vinculo entre fornecedor e socio com datas e percentual."""
    socio: Socio
    data_entrada: str | None = None
    data_saida: str | None = None
    percentual_capital: Decimal | None = None
