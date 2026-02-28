# api/infrastructure/repositories/duckdb_societario_repo.py
from __future__ import annotations

import duckdb

from api.domain.fornecedor.value_objects import CNPJ
from api.domain.societario.entities import Socio


class DuckDBSocietarioRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def listar_socios_por_fornecedor(self, cnpj: CNPJ) -> list[Socio]:
        rows = self._conn.execute(
            """
            SELECT ds.cpf_hmac, ds.nome, ds.qualificacao,
                   ds.is_servidor_publico, ds.orgao_lotacao,
                   ds.is_sancionado, ds.qtd_empresas_governo
            FROM dim_socio ds
            JOIN bridge_fornecedor_socio bfs ON ds.pk_socio = bfs.fk_socio
            JOIN dim_fornecedor df ON bfs.fk_fornecedor = df.pk_fornecedor
            WHERE df.cnpj = ?
        """,
            [cnpj.formatado],
        ).fetchall()
        return [self._hidratar(r) for r in rows]

    def grafo_2_niveis(
        self,
        cnpj: CNPJ,
        max_nos: int = 50,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        """CTE recursiva: nivel 0 = fornecedor, nivel 1 = socios e empresas,
        nivel 2 = socios dessas empresas. Retorna (nos, arestas)."""
        # Buscar fornecedores conectados via socios compartilhados (2 niveis)
        empresa_rows = self._conn.execute(
            """
            WITH RECURSIVE grafo AS (
                SELECT df.pk_fornecedor, df.cnpj, df.razao_social,
                       df.score_risco, df.qtd_alertas, 0 AS nivel
                FROM dim_fornecedor df WHERE df.cnpj = ?
                UNION
                SELECT df2.pk_fornecedor, df2.cnpj, df2.razao_social,
                       df2.score_risco, df2.qtd_alertas, g.nivel + 1
                FROM grafo g
                JOIN bridge_fornecedor_socio bfs1 ON g.pk_fornecedor = bfs1.fk_fornecedor
                JOIN bridge_fornecedor_socio bfs2 ON bfs1.fk_socio = bfs2.fk_socio
                JOIN dim_fornecedor df2 ON bfs2.fk_fornecedor = df2.pk_fornecedor
                WHERE g.nivel < 2 AND df2.pk_fornecedor != g.pk_fornecedor
            )
            SELECT DISTINCT pk_fornecedor, cnpj, razao_social,
                            score_risco, qtd_alertas
            FROM grafo LIMIT ?
        """,
            [cnpj.formatado, max_nos],
        ).fetchall()

        nos: list[dict[str, object]] = []
        arestas: list[dict[str, object]] = []
        pk_set: set[int] = set()

        for row in empresa_rows:
            pk_set.add(int(row[0]))
            nos.append(
                {
                    "id": f"empresa_{row[0]}",
                    "tipo": "empresa",
                    "label": str(row[2]),
                    "score": int(row[3]) if row[3] is not None else None,
                    "qtd_alertas": int(row[4]) if row[4] is not None else None,
                }
            )

        if not pk_set:
            return nos, arestas

        # Buscar socios dessas empresas
        placeholders = ",".join(["?"] * len(pk_set))
        socio_rows = self._conn.execute(
            f"""
            SELECT DISTINCT ds.pk_socio, ds.nome, ds.qualificacao,
                   bfs.fk_fornecedor
            FROM dim_socio ds
            JOIN bridge_fornecedor_socio bfs ON ds.pk_socio = bfs.fk_socio
            WHERE bfs.fk_fornecedor IN ({placeholders})
        """,  # noqa: S608
            list(pk_set),
        ).fetchall()

        socio_ids: set[int] = set()
        for row in socio_rows:
            socio_pk = int(row[0])
            if socio_pk not in socio_ids:
                socio_ids.add(socio_pk)
                nos.append(
                    {
                        "id": f"socio_{row[0]}",
                        "tipo": "socio",
                        "label": str(row[1]),
                        "score": None,
                        "qtd_alertas": None,
                    }
                )
            arestas.append(
                {
                    "source": f"socio_{row[0]}",
                    "target": f"empresa_{row[3]}",
                    "tipo": "socio_de",
                    "label": str(row[2]) if row[2] else None,
                }
            )

        truncado = len(nos) >= max_nos
        if truncado:
            nos = nos[:max_nos]

        return nos, arestas

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
