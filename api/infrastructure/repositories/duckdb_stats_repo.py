# api/infrastructure/repositories/duckdb_stats_repo.py
from __future__ import annotations

import duckdb


class DuckDBStatsRepo:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def obter_stats(self) -> dict[str, object]:
        """Agrega contagens de dim_fornecedor, fato_contrato, fato_alerta_critico."""
        fornecedores = self._contar("dim_fornecedor")
        contratos = self._contar("fato_contrato")
        alertas = self._contar("fato_alerta_critico")
        socios = self._contar("dim_socio")
        sancoes = self._contar("dim_sancao")

        return {
            "total_fornecedores": fornecedores,
            "total_contratos": contratos,
            "total_alertas": alertas,
            "fontes": {
                "fornecedores": {"ultima_atualizacao": None, "registros": fornecedores},
                "contratos": {"ultima_atualizacao": None, "registros": contratos},
                "socios": {"ultima_atualizacao": None, "registros": socios},
                "sancoes": {"ultima_atualizacao": None, "registros": sancoes},
            },
        }

    def _contar(self, tabela: str) -> int:
        # Tabela vem de codigo interno, nao de input do usuario — seguro
        row = self._conn.execute(f"SELECT count(*) FROM {tabela}").fetchone()  # noqa: S608
        return int(row[0]) if row else 0
