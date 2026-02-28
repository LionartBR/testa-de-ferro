# api/infrastructure/repositories/duckdb_societario_repo.py
from __future__ import annotations

import duckdb

from api.domain.fornecedor.value_objects import CNPJ
from api.domain.societario.entities import Socio


class DuckDBSocietarioRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def listar_socios_por_fornecedor(self, cnpj: CNPJ) -> list[Socio]:
        rows = self._conn.execute("""
            SELECT ds.cpf_hmac, ds.nome, ds.qualificacao,
                   ds.is_servidor_publico, ds.orgao_lotacao,
                   ds.is_sancionado, ds.qtd_empresas_governo
            FROM dim_socio ds
            JOIN bridge_fornecedor_socio bfs ON ds.pk_socio = bfs.fk_socio
            JOIN dim_fornecedor df ON bfs.fk_fornecedor = df.pk_fornecedor
            WHERE df.cnpj = ?
        """, [cnpj.formatado]).fetchall()
        return [self._hidratar(r) for r in rows]

    def _hidratar(self, row: tuple) -> Socio:  # type: ignore[type-arg]
        return Socio(
            cpf_hmac=str(row[0]),
            nome=str(row[1]),
            qualificacao=str(row[2]) if row[2] else None,
            is_servidor_publico=bool(row[3]),
            orgao_lotacao=str(row[4]) if row[4] else None,
            is_sancionado=bool(row[5]),
            qtd_empresas_governo=int(row[6]) if row[6] else 0,
        )
