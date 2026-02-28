# api/infrastructure/repositories/duckdb_sancao_repo.py
from __future__ import annotations

from datetime import date

import duckdb

from api.domain.fornecedor.value_objects import CNPJ
from api.domain.sancao.entities import Sancao
from api.domain.sancao.value_objects import TipoSancao


class DuckDBSancaoRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def listar_por_fornecedor(self, cnpj: CNPJ) -> list[Sancao]:
        rows = self._conn.execute(
            """
            SELECT ds.tipo_sancao, ds.orgao_sancionador, ds.motivo,
                   ds.data_inicio, ds.data_fim
            FROM dim_sancao ds
            JOIN dim_fornecedor df ON ds.fk_fornecedor = df.pk_fornecedor
            WHERE df.cnpj = ?
        """,
            [cnpj.formatado],
        ).fetchall()
        return [self._hidratar(r) for r in rows]

    def _hidratar(self, row: tuple) -> Sancao:  # type: ignore[type-arg]
        return Sancao(
            tipo=TipoSancao(str(row[0])),
            orgao_sancionador=str(row[1] or ""),
            motivo=str(row[2] or ""),
            data_inicio=row[3] if isinstance(row[3], date) else date(2000, 1, 1),
            data_fim=row[4] if isinstance(row[4], date) else None,
        )
