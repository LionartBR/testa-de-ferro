# api/application/services/ficha_service.py
from __future__ import annotations

from datetime import date

from api.domain.contrato.repository import ContratoRepository
from api.domain.doacao.repository import DoacaoRepository
from api.domain.fornecedor.repository import FornecedorRepository
from api.domain.fornecedor.value_objects import CNPJ
from api.domain.sancao.repository import SancaoRepository
from api.domain.societario.repository import SocietarioRepository

from ..dtos.ficha_dto import FichaCompletaDTO
from .alerta_service import detectar_alertas
from .score_service import calcular_score_cumulativo


class FichaService:
    """Imperative Shell: orquestra IO (repos) e chama Pure Core (alertas, score)."""

    def __init__(
        self,
        fornecedor_repo: FornecedorRepository,
        contrato_repo: ContratoRepository,
        sancao_repo: SancaoRepository,
        societario_repo: SocietarioRepository,
        doacao_repo: DoacaoRepository | None = None,
    ) -> None:
        self._fornecedor_repo = fornecedor_repo
        self._contrato_repo = contrato_repo
        self._sancao_repo = sancao_repo
        self._societario_repo = societario_repo
        self._doacao_repo = doacao_repo

    def obter_ficha(self, cnpj: CNPJ) -> FichaCompletaDTO | None:
        fornecedor = self._fornecedor_repo.buscar_por_cnpj(cnpj)
        if fornecedor is None:
            return None

        # IO — imperative shell
        socios = self._societario_repo.listar_socios_por_fornecedor(cnpj)
        sancoes = self._sancao_repo.listar_por_fornecedor(cnpj)
        contratos = self._contrato_repo.listar_por_fornecedor(cnpj)
        doacoes = self._doacao_repo.listar_por_fornecedor(cnpj) if self._doacao_repo else []

        referencia = date.today()

        # Pure core — funcoes puras, sem IO
        alertas = detectar_alertas(fornecedor, socios, sancoes, contratos, referencia, doacoes)
        score = calcular_score_cumulativo(fornecedor, socios, sancoes, contratos, referencia)

        return FichaCompletaDTO.from_domain(
            fornecedor, alertas, score, contratos, socios, sancoes, referencia, doacoes,
        )
