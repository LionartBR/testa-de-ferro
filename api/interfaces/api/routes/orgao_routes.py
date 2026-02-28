# api/interfaces/api/routes/orgao_routes.py
from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException

from api.infrastructure.duckdb_connection import get_connection

router = APIRouter()


def _get_conn() -> duckdb.DuckDBPyConnection:
    return get_connection()


@router.get("/orgaos/{codigo}/dashboard")
def get_dashboard_orgao(
    codigo: str,
    conn: duckdb.DuckDBPyConnection = Depends(_get_conn),  # noqa: B008
) -> dict[str, object]:

    # Verificar se orgao existe
    orgao = conn.execute(
        "SELECT nome, sigla FROM dim_orgao WHERE codigo = ?",
        [codigo],
    ).fetchone()
    if orgao is None:
        raise HTTPException(status_code=404, detail="Orgao nao encontrado")

    # Resumo
    resumo = conn.execute(
        """
        SELECT count(*) AS qtd_contratos,
               COALESCE(sum(fc.valor), 0) AS total_contratado,
               count(DISTINCT fc.fk_fornecedor) AS qtd_fornecedores
        FROM fato_contrato fc
        JOIN dim_orgao dorg ON fc.fk_orgao = dorg.pk_orgao
        WHERE dorg.codigo = ?
    """,
        [codigo],
    ).fetchone()

    # Top 10 fornecedores por valor
    top_fornecedores = conn.execute(
        """
        SELECT df.cnpj, df.razao_social, df.score_risco,
               sum(fc.valor) AS valor_total,
               count(*) AS qtd_contratos
        FROM fato_contrato fc
        JOIN dim_fornecedor df ON fc.fk_fornecedor = df.pk_fornecedor
        JOIN dim_orgao dorg ON fc.fk_orgao = dorg.pk_orgao
        WHERE dorg.codigo = ?
        GROUP BY df.cnpj, df.razao_social, df.score_risco
        ORDER BY valor_total DESC
        LIMIT 10
    """,
        [codigo],
    ).fetchall()

    return {
        "orgao": {"nome": str(orgao[0]), "sigla": str(orgao[1]) if orgao[1] else None, "codigo": codigo},
        "qtd_contratos": int(resumo[0]) if resumo else 0,
        "total_contratado": str(resumo[1]) if resumo else "0",
        "qtd_fornecedores": int(resumo[2]) if resumo else 0,
        "top_fornecedores": [
            {
                "cnpj": str(r[0]),
                "razao_social": str(r[1]),
                "score_risco": int(r[2]) if r[2] is not None else 0,
                "valor_total": str(r[3]),
                "qtd_contratos": int(r[4]),
            }
            for r in top_fornecedores
        ],
    }
