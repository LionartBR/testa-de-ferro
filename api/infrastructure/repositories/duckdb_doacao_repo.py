# api/infrastructure/repositories/duckdb_doacao_repo.py
from __future__ import annotations

from decimal import Decimal

import duckdb

from api.domain.doacao.entities import DoacaoEleitoral
from api.domain.doacao.value_objects import ValorDoacao
from api.domain.fornecedor.value_objects import CNPJ


class DuckDBDoacaoRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def listar_por_fornecedor(self, cnpj: CNPJ) -> list[DoacaoEleitoral]:
        rows = self._conn.execute(
            """
            SELECT dc.nome, dc.partido, dc.cargo,
                   fd.valor, fd.ano_eleicao,
                   ds.cpf_hmac
            FROM fato_doacao fd
            JOIN dim_candidato dc ON fd.fk_candidato = dc.pk_candidato
            LEFT JOIN dim_fornecedor df ON fd.fk_fornecedor = df.pk_fornecedor
            LEFT JOIN dim_socio ds ON fd.fk_socio = ds.pk_socio
            WHERE df.cnpj = ?
               OR ds.pk_socio IN (
                   SELECT bfs.fk_socio
                   FROM bridge_fornecedor_socio bfs
                   JOIN dim_fornecedor df2 ON bfs.fk_fornecedor = df2.pk_fornecedor
                   WHERE df2.cnpj = ?
               )
        """,
            [cnpj.formatado, cnpj.formatado],
        ).fetchall()
        return [self._hidratar(r, cnpj) for r in rows]

    def _hidratar(self, row: tuple, cnpj: CNPJ) -> DoacaoEleitoral:  # type: ignore[type-arg]
        return DoacaoEleitoral(
            fornecedor_cnpj=cnpj,
            socio_cpf_hmac=str(row[5]) if row[5] else None,
            candidato_nome=str(row[0]),
            candidato_partido=str(row[1] or ""),
            candidato_cargo=str(row[2] or ""),
            valor=ValorDoacao(Decimal(str(row[3]))),
            ano_eleicao=int(row[4]),
        )
