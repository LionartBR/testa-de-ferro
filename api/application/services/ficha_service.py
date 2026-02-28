# api/application/services/ficha_service.py
from __future__ import annotations

import uuid
from datetime import date, datetime

from api.domain.contrato.repository import ContratoRepository
from api.domain.doacao.repository import DoacaoRepository
from api.domain.fornecedor.entities import AlertaCritico
from api.domain.fornecedor.enums import Severidade, TipoAlerta
from api.domain.fornecedor.repository import FornecedorRepository
from api.domain.fornecedor.value_objects import CNPJ
from api.domain.sancao.repository import SancaoRepository
from api.domain.societario.repository import SocietarioRepository
from api.infrastructure.repositories.duckdb_alerta_repo import DuckDBAlertaRepo

from ..dtos.ficha_dto import FichaCompletaDTO
from .alerta_service import detectar_alertas
from .score_service import calcular_score_cumulativo

# Alert types that are only computed by the pipeline (not by the API domain service).
_PIPELINE_ONLY_ALERT_TYPES = {TipoAlerta.RODIZIO_LICITACAO, TipoAlerta.TESTA_DE_FERRO}


class FichaService:
    """Imperative Shell: orquestra IO (repos) e chama Pure Core (alertas, score)."""

    def __init__(
        self,
        fornecedor_repo: FornecedorRepository,
        contrato_repo: ContratoRepository,
        sancao_repo: SancaoRepository,
        societario_repo: SocietarioRepository,
        doacao_repo: DoacaoRepository | None = None,
        alerta_repo: DuckDBAlertaRepo | None = None,
    ) -> None:
        self._fornecedor_repo = fornecedor_repo
        self._contrato_repo = contrato_repo
        self._sancao_repo = sancao_repo
        self._societario_repo = societario_repo
        self._doacao_repo = doacao_repo
        self._alerta_repo = alerta_repo

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

        # Merge pre-computed pipeline-only alerts (RODIZIO_LICITACAO, TESTA_DE_FERRO)
        alertas = self._merge_precomputed_alerts(cnpj, alertas)

        return FichaCompletaDTO.from_domain(
            fornecedor,
            alertas,
            score,
            contratos,
            socios,
            sancoes,
            referencia,
            doacoes,
        )

    def _merge_precomputed_alerts(
        self,
        cnpj: CNPJ,
        domain_alertas: list[AlertaCritico],
    ) -> list[AlertaCritico]:
        """Fetch pipeline-only alerts from DB and merge with domain-computed alerts.

        RODIZIO_LICITACAO and TESTA_DE_FERRO are expensive graph-based alerts
        that are pre-computed by the pipeline batch. They cannot be computed
        per-request without the full supplier population.
        """
        if self._alerta_repo is None:
            return domain_alertas

        precomputed = self._alerta_repo.listar_por_fornecedor(cnpj.formatado)
        for row in precomputed:
            tipo_str = str(row["tipo"])
            try:
                tipo = TipoAlerta(tipo_str)
            except ValueError:
                continue
            if tipo not in _PIPELINE_ONLY_ALERT_TYPES:
                continue
            domain_alertas.append(
                AlertaCritico(
                    id=uuid.uuid4(),
                    tipo=tipo,
                    severidade=Severidade(str(row["severidade"])),
                    descricao=str(row["descricao"]),
                    evidencia=str(row["evidencia"]),
                    fornecedor_cnpj=cnpj,
                    detectado_em=datetime.now(),
                )
            )
        return domain_alertas
