# api/infrastructure/repositories/duckdb_fornecedor_repo.py
from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from api.domain.fornecedor.entities import Fornecedor
from api.domain.fornecedor.enums import SituacaoCadastral
from api.domain.fornecedor.value_objects import (
    CNPJ,
    CapitalSocial,
    Endereco,
    RazaoSocial,
)


class DuckDBFornecedorRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def buscar_por_cnpj(self, cnpj: CNPJ) -> Fornecedor | None:
        row = self._conn.execute(
            "SELECT * FROM dim_fornecedor WHERE cnpj = ?",
            [cnpj.formatado],
        ).fetchone()
        if row is None:
            return None
        return self._hidratar(row)

    def ranking_por_score(self, limit: int, offset: int) -> list[Fornecedor]:
        rows = self._conn.execute(
            "SELECT * FROM dim_fornecedor ORDER BY score_risco DESC LIMIT ? OFFSET ?",
            [limit, offset],
        ).fetchall()
        return [self._hidratar(r) for r in rows]

    def buscar_por_nome_ou_cnpj(self, query: str, limit: int) -> list[Fornecedor]:
        rows = self._conn.execute(
            """SELECT * FROM dim_fornecedor
               WHERE cnpj LIKE ? OR razao_social ILIKE ?
               LIMIT ?""",
            [f"%{query}%", f"%{query}%", limit],
        ).fetchall()
        return [self._hidratar(r) for r in rows]

    def contar_total(self) -> int:
        """SELECT count(*) FROM dim_fornecedor."""
        row = self._conn.execute("SELECT count(*) FROM dim_fornecedor").fetchone()
        return int(row[0]) if row else 0

    def _hidratar(self, row: tuple) -> Fornecedor:  # type: ignore[type-arg]
        """Mapeia row do DuckDB para entidade de dominio.
        Colunas: pk(0), cnpj(1), razao_social(2), data_abertura(3),
        capital_social(4), cnae_principal(5), cnae_descricao(6),
        logradouro(7), municipio(8), uf(9), cep(10), situacao(11),
        score_risco(12), faixa_risco(13), qtd_alertas(14), max_severidade(15),
        total_contratos(16), valor_total(17), atualizado_em(18)"""
        return Fornecedor(
            cnpj=CNPJ(str(row[1])),
            razao_social=RazaoSocial(str(row[2])),
            data_abertura=row[3] if isinstance(row[3], date) else None,
            capital_social=CapitalSocial(Decimal(str(row[4]))) if row[4] is not None else None,
            cnae_principal=str(row[5]) if row[5] else None,
            cnae_descricao=str(row[6]) if row[6] else None,
            endereco=Endereco(
                logradouro=str(row[7] or ""),
                municipio=str(row[8] or ""),
                uf=str(row[9] or ""),
                cep=str(row[10] or ""),
            ) if row[7] else None,
            situacao=SituacaoCadastral(str(row[11])) if row[11] else SituacaoCadastral.ATIVA,
            total_contratos=int(row[16]) if row[16] else 0,
            valor_total_contratos=Decimal(str(row[17])) if row[17] else Decimal("0"),
        )
