"""One-shot loader: embed movie plots and write to DuckDB VSS table.

Reads from an input DuckDB database (existing cinemattr data) and writes
embeddings to the VSS target table. Safe to re-run — upserts by title_id.

Usage:
    python api/load_embeddings.py --source db/duckdb/movies.duckdb \
        --table movies --plot-col plot --id-col title_id \
        --batch 64

Env vars required:
    DUCKDB_PATH         target VSS database (output)
    EMBEDDING_DIM       embedding dimension
    EMBEDDING_MODEL     embedding model id
    LLM_BASE_URL/KEY    OR EMBEDDING_BASE_URL/KEY
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable

import duckdb

from embeddings import build_client, embed_texts, iter_batches, load_embedding_config
from vector_store import connect, ensure_schema, load_store_config

logger = logging.getLogger("cinemattr.loader")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _iter_source(
    source_path: str,
    table: str,
    id_col: str,
    plot_col: str,
    title_col: str,
    year_col: str,
    genre_col: str,
    rating_col: str,
) -> Iterable[dict]:
    con = duckdb.connect(source_path, read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT
                {id_col}     AS title_id,
                {title_col}  AS title,
                {year_col}   AS year,
                {genre_col}  AS genres_raw,
                {rating_col} AS rating,
                {plot_col}   AS plot
            FROM {table}
            WHERE {plot_col} IS NOT NULL AND length(trim({plot_col})) > 20;
            """
        ).fetchall()
    finally:
        con.close()

    for row in rows:
        title_id, title, year, genres_raw, rating, plot = row
        if isinstance(genres_raw, list):
            genres = [str(g).strip() for g in genres_raw if str(g).strip()]
        elif isinstance(genres_raw, str) and genres_raw:
            genres = [g.strip() for g in genres_raw.split(",") if g.strip()]
        else:
            genres = []
        yield {
            "title_id": title_id,
            "title": title,
            "year": int(year) if year is not None else None,
            "genres": genres,
            "rating": float(rating) if rating is not None else None,
            "plot": plot,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="Path to source DuckDB database with movies + plots.")
    parser.add_argument("--table", default="movies")
    parser.add_argument("--id-col", default="title_id")
    parser.add_argument("--plot-col", default="plot")
    parser.add_argument("--title-col", default="title")
    parser.add_argument("--year-col", default="year")
    parser.add_argument("--genre-col", default="genres")
    parser.add_argument("--rating-col", default="rating")
    parser.add_argument("--batch", type=int, default=64)
    args = parser.parse_args(argv)

    store_config = load_store_config()
    embedding_config = load_embedding_config()
    if embedding_config.dim is None:
        raise SystemExit("EMBEDDING_DIM must match EMBEDDING_MODEL output dim.")
    if embedding_config.dim != store_config.dim:
        raise SystemExit(
            f"EMBEDDING_DIM ({embedding_config.dim}) does not match store dim ({store_config.dim})."
        )

    client = build_client(embedding_config)
    target = connect(store_config, read_only=False)
    ensure_schema(target, store_config)

    rows = list(_iter_source(
        args.source, args.table, args.id_col, args.plot_col,
        args.title_col, args.year_col, args.genre_col, args.rating_col,
    ))
    logger.info("Loaded %d source rows from %s.%s", len(rows), args.source, args.table)

    total_written = 0
    for batch in iter_batches(iter([r["plot"] for r in rows]), args.batch):
        offset = total_written
        slice_rows = rows[offset : offset + len(batch)]
        texts = [r["plot"] for r in slice_rows]
        vectors = embed_texts(client, embedding_config, texts)

        for row, vector in zip(slice_rows, vectors):
            target.execute(
                f"""
                INSERT OR REPLACE INTO {store_config.table}
                    (title_id, title, year, genres, rating, plot, embedding)
                VALUES (?, ?, ?, ?, ?, ?, ?::FLOAT[{store_config.dim}]);
                """,
                [row["title_id"], row["title"], row["year"], row["genres"], row["rating"], row["plot"], vector],
            )
        total_written += len(slice_rows)
        logger.info("Embedded %d/%d", total_written, len(rows))

    target.close()
    logger.info("Done. Wrote %d rows to %s (%s).", total_written, store_config.db_path, store_config.table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
