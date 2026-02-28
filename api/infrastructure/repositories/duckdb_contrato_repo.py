# api/infrastructure/repositories/duckdb_contrato_repo.py
from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from api.domain.contrato.entities import Contrato
from api.domain.contrato.value_objects import ValorContrato
from api.domain.fornecedor.value_objects import CNPJ


class DuckDBContratoRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def listar_por_fornecedor(self, cnpj: CNPJ) -> list[Contrato]:
        rows = self._conn.execute("""
            SELECT fc.valor, fc.objeto, fc.num_licitacao,
                   fc.data_assinatura, fc.data_vigencia, dorg.codigo
            FROM fato_contrato fc
            JOIN dim_fornecedor df ON fc.fk_fornecedor = df.pk_fornecedor
            JOIN dim_orgao dorg ON fc.fk_orgao = dorg.pk_orgao
            WHERE df.cnpj = ?
        """, [cnpj.formatado]).fetchall()
        return [self._hidratar(r, cnpj) for r in rows]

    def _hidratar(self, row: tuple, cnpj: CNPJ) -> Contrato:  # type: ignore[type-arg]
        return Contrato(
            fornecedor_cnpj=cnpj,
            orgao_codigo=str(row[5]),
            valor=ValorContrato(Decimal(str(row[0]))),
            objeto=str(row[1]) if row[1] else None,
            num_licitacao=str(row[2]) if row[2] else None,
            data_assinatura=row[3] if isinstance(row[3], date) else None,
            data_vigencia=row[4] if isinstance(row[4], date) else None,
        )
