# pipeline/transform/grafo_societario.py
#
# Build the corporate relationship graph (nodes + edges) for the staging layer.
#
# Design decisions:
#   - The graph is materialised as two Polars DataFrames (nos_df, arestas_df)
#     that mirror the GrafoDTO structure expected by the API's grafo_routes.
#   - Traversal covers up to 2 levels of indirection:
#       Level 0: Pessoa (sócio) → Empresa (fornecedora)
#       Level 1: Empresa → Other Empresas sharing the same sócio
#     This captures the "Pessoa → Holding → Empresa fornecedora" pattern
#     described in the project spec without exploding the graph size.
#   - The max_nos parameter caps the total node count to prevent unbounded
#     graph growth for highly-connected individuals. When the limit is hit,
#     nodes are selected in order of connection degree (highest first).
#   - Node types: "empresa" or "socio". Edge types: "socio_de".
#   - All node IDs are string CNPJs or name-based IDs for sócios (using
#     cpf_hmac when available, otherwise nome_socio as fallback).
#
# ADR: Why not use networkx?
#   This transform runs in the pipeline worker, which should have minimal
#   Python dependencies. Pure Polars operations keep the dependency surface
#   small. The graph traversal logic is simple enough for DataFrame joins.
#
# Invariants:
#   - nos_df columns: id (str), tipo (str: "empresa"|"socio"), label (str).
#   - arestas_df columns: origem (str), destino (str), tipo (str: "socio_de").
#   - No duplicate nodes or edges in the output.
from __future__ import annotations

import polars as pl


def construir_grafo(
    socios_df: pl.DataFrame,
    empresas_df: pl.DataFrame,
    max_nos: int = 50,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build corporate graph nodes and edges for all fornecedores.

    Traversal:
      - Level 0: each sócio IS a sócio of their company (socio → empresa).
      - Level 1: companies that share a sócio with level-0 companies are
        included as sibling nodes (empresa → empresa via shared sócio).

    Args:
        socios_df:   DataFrame with columns: cnpj_basico (str), nome_socio (str),
                     and optionally cpf_hmac (str).
        empresas_df: DataFrame with columns: cnpj_basico (str), cnpj (str),
                     razao_social (str).
        max_nos:     Maximum total nodes to include.  If exceeded, trim by
                     keeping the most-connected nodes first.

    Returns:
        Tuple of (nos_df, arestas_df):
          - nos_df:     Columns: id, tipo, label.
          - arestas_df: Columns: origem, destino, tipo.
    """
    if socios_df.is_empty() or empresas_df.is_empty():
        nos_empty = pl.DataFrame({"id": [], "tipo": [], "label": []})
        arestas_empty = pl.DataFrame({"origem": [], "destino": [], "tipo": []})
        return nos_empty, arestas_empty

    # Normalise empresas: need cnpj_basico → (cnpj, razao_social).
    empresas_lookup = empresas_df.select(
        [
            pl.col("cnpj_basico").str.zfill(8),
            pl.col("cnpj").alias("cnpj_formatado"),
            pl.col("razao_social"),
        ]
    ).unique(subset=["cnpj_basico"])

    # Build the sócio node ID: prefer cpf_hmac, fall back to nome_socio.
    if "cpf_hmac" in socios_df.columns:
        socio_id_expr = pl.col("cpf_hmac").fill_null(pl.col("nome_socio"))
    else:
        socio_id_expr = pl.col("nome_socio")

    socios_with_ids = socios_df.with_columns(
        socio_id_expr.alias("_socio_id"),
        pl.col("cnpj_basico").str.zfill(8).alias("_cnpj_basico_norm"),
    )

    # Join sócios against empresas to get the formatted CNPJ and razao_social.
    socios_joined = socios_with_ids.join(
        empresas_lookup,
        left_on="_cnpj_basico_norm",
        right_on="cnpj_basico",
        how="inner",
    )

    if socios_joined.is_empty():
        nos_empty = pl.DataFrame({"id": [], "tipo": [], "label": []})
        arestas_empty = pl.DataFrame({"origem": [], "destino": [], "tipo": []})
        return nos_empty, arestas_empty

    # ------------------------------------------------------------------
    # Level 0 edges: sócio → empresa
    # ------------------------------------------------------------------
    level0_edges = socios_joined.select(
        [
            pl.col("_socio_id").alias("origem"),
            pl.col("cnpj_formatado").alias("destino"),
            pl.lit("socio_de").alias("tipo"),
        ]
    ).unique()

    # ------------------------------------------------------------------
    # Level 1 edges: empresa → sibling empresa (via shared sócio)
    # Level 1 adds connections between empresas that share a sócio.
    # We find all pairs (empresa_a, empresa_b) where the same _socio_id
    # links both companies.
    # ------------------------------------------------------------------
    self_join = (
        socios_joined.select(
            [
                pl.col("_socio_id"),
                pl.col("cnpj_formatado").alias("cnpj_a"),
            ]
        )
        .join(
            socios_joined.select(
                [
                    pl.col("_socio_id"),
                    pl.col("cnpj_formatado").alias("cnpj_b"),
                ]
            ),
            on="_socio_id",
            how="inner",
        )
        .filter(pl.col("cnpj_a") != pl.col("cnpj_b"))
    )

    # Canonical direction: smaller cnpj is always "origem".
    level1_edges = (
        self_join.with_columns(
            [
                pl.when(pl.col("cnpj_a") < pl.col("cnpj_b"))
                .then(pl.col("cnpj_a"))
                .otherwise(pl.col("cnpj_b"))
                .alias("origem"),
                pl.when(pl.col("cnpj_a") < pl.col("cnpj_b"))
                .then(pl.col("cnpj_b"))
                .otherwise(pl.col("cnpj_a"))
                .alias("destino"),
            ]
        )
        .select(
            [
                pl.col("origem"),
                pl.col("destino"),
                pl.lit("socio_compartilhado").alias("tipo"),
            ]
        )
        .unique()
    )

    all_edges = pl.concat([level0_edges, level1_edges])

    # ------------------------------------------------------------------
    # Build nodes from edges
    # ------------------------------------------------------------------
    # Empresa nodes
    empresa_ids = empresas_lookup.select(
        [
            pl.col("cnpj_formatado").alias("id"),
            pl.lit("empresa").alias("tipo"),
            pl.col("razao_social").alias("label"),
        ]
    ).unique()

    # Sócio nodes
    socio_nodes = socios_joined.select(
        [
            pl.col("_socio_id").alias("id"),
            pl.lit("socio").alias("tipo"),
            pl.col("nome_socio").alias("label"),
        ]
    ).unique()

    all_nodes = pl.concat([empresa_ids, socio_nodes]).unique(subset=["id"])

    # ------------------------------------------------------------------
    # Apply max_nos limit: keep most-connected nodes
    # ------------------------------------------------------------------
    if len(all_nodes) > max_nos:
        # Count how many edges reference each node.
        origem_counts = all_edges.group_by("origem").agg(pl.len().alias("_degree")).rename({"origem": "id"})
        destino_counts = all_edges.group_by("destino").agg(pl.len().alias("_degree")).rename({"destino": "id"})

        degree_df = (
            pl.concat([origem_counts, destino_counts]).group_by("id").agg(pl.col("_degree").sum().alias("_degree"))
        )

        ranked = (
            all_nodes.join(degree_df, on="id", how="left")
            .with_columns(pl.col("_degree").fill_null(0))
            .sort("_degree", descending=True)
            .head(max_nos)
            .drop("_degree")
        )

        kept_ids: set[str] = set(ranked["id"].to_list())
        all_nodes = ranked
        all_edges = all_edges.filter(pl.col("origem").is_in(kept_ids) & pl.col("destino").is_in(kept_ids))

    return (
        all_nodes.select(["id", "tipo", "label"]),
        all_edges.select(["origem", "destino", "tipo"]),
    )
