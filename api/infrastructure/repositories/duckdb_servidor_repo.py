# api/infrastructure/repositories/duckdb_servidor_repo.py
#
# DuckDB repository for ServidorPublico lookups.
#
# Design decisions:
#   - Servidor data is denormalized into dim_socio after the pipeline runs
#     match_servidor_socio. This avoids a separate dim_servidor table and a
#     join at query time. The trade-off is that a sócio can only be flagged
#     as a servidor if the name+CPF-digit match succeeded during the pipeline.
#   - Lookups are always by cpf_hmac: the API receives a plain CPF from the
#     caller, converts it to an HMAC via hmac_service.hmac_sha256_cpf(), then
#     calls this repo. The repo never sees the raw CPF.
#   - The query filters is_servidor_publico = TRUE so that socios who happen
#     to share a cpf_hmac prefix but are not servers are excluded. In practice
#     cpf_hmac collisions are cryptographically negligible, but the filter
#     documents intent clearly.
#   - LIMIT 1 because cpf_hmac is semantically unique per person: two rows
#     with the same HMAC would mean the same person is listed twice, and we
#     only need one record for the entity.
#
# Invariants:
#   - Never interpolates user input into SQL strings. All parameters use
#     DuckDB's positional placeholder (?).
#   - Returns None (not raising) when no match is found — callers decide
#     how to handle the absence.
from __future__ import annotations

import duckdb

from api.domain.servidor.entities import ServidorPublico


class DuckDBServidorRepo:
    """Concrete implementation of ServidorRepository backed by DuckDB.

    Queries the dim_socio table for rows where is_servidor_publico is TRUE,
    hydrating results into ServidorPublico domain entities.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def buscar_por_cpf_hmac(self, cpf_hmac: str) -> ServidorPublico | None:
        """Return a ServidorPublico matching the given CPF HMAC, or None.

        Args:
            cpf_hmac: 64-character lowercase hex HMAC of the CPF, as produced
                      by hmac_service.hmac_sha256_cpf(). Never a raw CPF.

        Returns:
            ServidorPublico if a servidor match exists in dim_socio, else None.
        """
        row = self._conn.execute(
            """
            SELECT cpf_hmac, nome, qualificacao, orgao_lotacao
            FROM dim_socio
            WHERE cpf_hmac = ? AND is_servidor_publico = TRUE
            LIMIT 1
            """,
            [cpf_hmac],
        ).fetchone()

        if row is None:
            return None

        return ServidorPublico(
            cpf_hmac=str(row[0]),
            nome=str(row[1]),
            cargo=str(row[2]) if row[2] is not None else None,
            orgao_lotacao=str(row[3]) if row[3] is not None else None,
        )
