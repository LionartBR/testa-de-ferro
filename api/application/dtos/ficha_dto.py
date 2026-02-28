# api/application/dtos/ficha_dto.py
from __future__ import annotations

from datetime import date

from pydantic import BaseModel

from api.domain.contrato.entities import Contrato
from api.domain.doacao.entities import DoacaoEleitoral
from api.domain.fornecedor.entities import AlertaCritico, Fornecedor
from api.domain.fornecedor.score import ScoreDeRisco
from api.domain.sancao.entities import Sancao
from api.domain.societario.entities import Socio

from .alerta_dto import AlertaCriticoDTO
from .contrato_dto import ContratoResumoDTO
from .fornecedor_dto import DoacaoDTO, SancaoDTO, SocioDTO
from .score_dto import IndicadorDTO, ScoreDTO


class FichaCompletaDTO(BaseModel):
    cnpj: str
    razao_social: str
    situacao: str
    data_abertura: str | None
    capital_social: str | None
    cnae_principal: str | None
    cnae_descricao: str | None
    endereco: dict[str, str] | None
    total_contratos: int
    valor_total_contratos: str
    alertas_criticos: list[AlertaCriticoDTO]
    score: ScoreDTO | None
    socios: list[SocioDTO]
    sancoes: list[SancaoDTO]
    contratos: list[ContratoResumoDTO]
    doacoes: list[DoacaoDTO]
    disclaimer: str = (
        "Dados gerados automaticamente a partir de bases publicas. "
        "Nao constituem acusacao. Correlacao nao implica causalidade."
    )

    @classmethod
    def from_domain(
        cls,
        fornecedor: Fornecedor,
        alertas: list[AlertaCritico],
        score: ScoreDeRisco,
        contratos: list[Contrato],
        socios: list[Socio],
        sancoes: list[Sancao],
        referencia: date,
        doacoes: list[DoacaoEleitoral] | None = None,
    ) -> FichaCompletaDTO:
        return cls(
            cnpj=fornecedor.cnpj.formatado,
            razao_social=fornecedor.razao_social.valor,
            situacao=fornecedor.situacao.value,
            data_abertura=fornecedor.data_abertura.isoformat() if fornecedor.data_abertura else None,
            capital_social=str(fornecedor.capital_social.valor) if fornecedor.capital_social else None,
            cnae_principal=fornecedor.cnae_principal,
            cnae_descricao=fornecedor.cnae_descricao,
            endereco={
                "logradouro": fornecedor.endereco.logradouro,
                "municipio": fornecedor.endereco.municipio,
                "uf": fornecedor.endereco.uf,
                "cep": fornecedor.endereco.cep,
            }
            if fornecedor.endereco
            else None,
            total_contratos=fornecedor.total_contratos,
            valor_total_contratos=str(fornecedor.valor_total_contratos),
            alertas_criticos=[
                AlertaCriticoDTO(
                    tipo=a.tipo.value,
                    severidade=a.severidade.value,
                    descricao=a.descricao,
                    evidencia=a.evidencia,
                )
                for a in alertas
            ],
            score=ScoreDTO(
                valor=score.valor,
                faixa=score.faixa.value,
                indicadores=[
                    IndicadorDTO(
                        tipo=i.tipo.value,
                        peso=i.peso,
                        descricao=i.descricao,
                        evidencia=i.evidencia,
                    )
                    for i in score.indicadores
                ],
            ),
            socios=[
                SocioDTO(
                    nome=s.nome,
                    qualificacao=s.qualificacao,
                    is_servidor_publico=s.is_servidor_publico,
                    orgao_lotacao=s.orgao_lotacao,
                )
                for s in socios
            ],
            sancoes=[
                SancaoDTO(
                    tipo=s.tipo.value,
                    orgao_sancionador=s.orgao_sancionador,
                    motivo=s.motivo,
                    data_inicio=s.data_inicio.isoformat(),
                    data_fim=s.data_fim.isoformat() if s.data_fim else None,
                    vigente=s.vigente(referencia),
                )
                for s in sancoes
            ],
            contratos=[
                ContratoResumoDTO(
                    orgao_codigo=c.orgao_codigo,
                    valor=str(c.valor.valor),
                    data_assinatura=c.data_assinatura.isoformat() if c.data_assinatura else None,
                    objeto=c.objeto,
                )
                for c in contratos
            ],
            doacoes=[
                DoacaoDTO(
                    candidato_nome=d.candidato_nome,
                    candidato_partido=d.candidato_partido,
                    candidato_cargo=d.candidato_cargo,
                    valor=str(d.valor.valor),
                    ano_eleicao=d.ano_eleicao,
                    via_socio=d.socio_cpf_hmac is not None,
                )
                for d in (doacoes or [])
            ],
        )
