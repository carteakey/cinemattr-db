"""DuckDB VSS vector store. Schema + similarity query."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Sequence

import duckdb

from filter_parser import ParsedQuery


@dataclass(frozen=True)
class StoreConfig:
    db_path: str
    table: str
    dim: int


def load_store_config() -> StoreConfig:
    db_path = os.getenv("DUCKDB_PATH", "data/cinemattr.duckdb").strip()
    table = os.getenv("VSS_TABLE", "movie_embeddings").strip()
    dim_raw = os.getenv("EMBEDDING_DIM", "").strip()
    if not dim_raw:
        raise RuntimeError("EMBEDDING_DIM is required (e.g. 768, 1536, 3072).")
    return StoreConfig(db_path=db_path, table=table, dim=int(dim_raw))


def connect(config: StoreConfig, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(config.db_path) or ".", exist_ok=True)
    con = duckdb.connect(config.db_path, read_only=read_only)
    con.execute("INSTALL vss;")
    con.execute("LOAD vss;")
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection, config: StoreConfig) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {config.table} (
            title_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            year INTEGER,
            genres VARCHAR[],
            rating DOUBLE,
            plot TEXT,
            embedding FLOAT[{config.dim}]
        );
        """
    )
    # HNSW index. VSS requires explicit index creation for large tables; skip if present.
    con.execute("SET hnsw_enable_experimental_persistence = true;")
    existing = con.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = ?",
        [config.table],
    ).fetchall()
    if not any(row[0].endswith("_hnsw") for row in existing):
        con.execute(
            f"CREATE INDEX {config.table}_hnsw ON {config.table} USING HNSW (embedding) WITH (metric = 'cosine');"
        )


def _build_where(parsed: ParsedQuery) -> tuple[str, list]:
    clauses: list[str] = []
    params: list = []

    if parsed.year_from is not None:
        clauses.append("year >= ?")
        params.append(parsed.year_from)
    if parsed.year_to is not None:
        clauses.append("year <= ?")
        params.append(parsed.year_to)
    if parsed.min_rating is not None:
        clauses.append("rating >= ?")
        params.append(parsed.min_rating)
    if parsed.genres:
        lowered = [g.lower() for g in parsed.genres]
        clauses.append("list_has_any(list_transform(genres, g -> lower(g)), ?)")
        params.append(lowered)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


def search(
    con: duckdb.DuckDBPyConnection,
    config: StoreConfig,
    embedding: Sequence[float],
    parsed: ParsedQuery,
    top_k: int,
) -> list[str]:
    where, params = _build_where(parsed)
    sql = f"""
        SELECT title_id
        FROM {config.table}
        {where}
        ORDER BY array_distance(embedding, ?::FLOAT[{config.dim}])
        LIMIT ?;
    """
    row_params = [*params, list(embedding), top_k]
    return [row[0] for row in con.execute(sql, row_params).fetchall()]
