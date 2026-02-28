# api/infrastructure/repositories/duckdb_alerta_repo.py
from __future__ import annotations

import duckdb


class DuckDBAlertaRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def listar_feed(self, limit: int, offset: int) -> list[dict[str, object]]:
        """JOIN fato_alerta_critico + dim_fornecedor + dim_socio
        ORDER BY detectado_em DESC."""
        rows = self._conn.execute(
            """
            SELECT fac.tipo_alerta, fac.severidade, fac.descricao,
                   fac.evidencia, fac.detectado_em,
                   df.cnpj, df.razao_social,
                   ds.nome AS socio_nome
            FROM fato_alerta_critico fac
            JOIN dim_fornecedor df ON fac.fk_fornecedor = df.pk_fornecedor
            LEFT JOIN dim_socio ds ON fac.fk_socio = ds.pk_socio
            ORDER BY fac.detectado_em DESC
            LIMIT ? OFFSET ?
        """,
            [limit, offset],
        ).fetchall()
        return [self._to_dict(r) for r in rows]

    def listar_por_tipo(self, tipo: str, limit: int, offset: int) -> list[dict[str, object]]:
        """WHERE tipo_alerta = ? ORDER BY detectado_em DESC."""
        rows = self._conn.execute(
            """
            SELECT fac.tipo_alerta, fac.severidade, fac.descricao,
                   fac.evidencia, fac.detectado_em,
                   df.cnpj, df.razao_social,
                   ds.nome AS socio_nome
            FROM fato_alerta_critico fac
            JOIN dim_fornecedor df ON fac.fk_fornecedor = df.pk_fornecedor
            LEFT JOIN dim_socio ds ON fac.fk_socio = ds.pk_socio
            WHERE fac.tipo_alerta = ?
            ORDER BY fac.detectado_em DESC
            LIMIT ? OFFSET ?
        """,
            [tipo, limit, offset],
        ).fetchall()
        return [self._to_dict(r) for r in rows]

    def listar_por_fornecedor(self, cnpj: str) -> list[dict[str, object]]:
        """Pre-computed alerts for a specific fornecedor by CNPJ."""
        rows = self._conn.execute(
            """
            SELECT fac.tipo_alerta, fac.severidade, fac.descricao,
                   fac.evidencia, fac.detectado_em,
                   df.cnpj, df.razao_social,
                   ds.nome AS socio_nome
            FROM fato_alerta_critico fac
            JOIN dim_fornecedor df ON fac.fk_fornecedor = df.pk_fornecedor
            LEFT JOIN dim_socio ds ON fac.fk_socio = ds.pk_socio
            WHERE df.cnpj = ?
            ORDER BY fac.detectado_em DESC
        """,
            [cnpj],
        ).fetchall()
        return [self._to_dict(r) for r in rows]

    def contar(self) -> int:
        """SELECT count(*) FROM fato_alerta_critico."""
        row = self._conn.execute(
            "SELECT count(*) FROM fato_alerta_critico",
        ).fetchone()
        return int(row[0]) if row else 0

    def _to_dict(self, row: tuple) -> dict[str, object]:  # type: ignore[type-arg]
        return {
            "tipo": str(row[0]),
            "severidade": str(row[1]),
            "descricao": str(row[2]),
            "evidencia": str(row[3]),
            "detectado_em": str(row[4]) if row[4] else "",
            "cnpj": str(row[5]),
            "razao_social": str(row[6]),
            "socio_nome": str(row[7]) if row[7] else None,
        }
