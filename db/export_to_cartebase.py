#!/usr/bin/env python3
"""
Export top movies from DuckDB imdb_movies → cartebase SQLite.

Filters: IMDb_rating >= MIN_RATING and ratingCount >= MIN_VOTES.
Ranks by Bayesian score (rating * log10(votes)) so quality + popularity both matter.
Upserts — safe to re-run.

Usage:
  python db/export_to_cartebase.py
  python db/export_to_cartebase.py --limit 5000 --min-rating 7.5 --min-votes 50000
"""
from __future__ import annotations

import argparse
import logging
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cartebase-export")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DUCKDB = str(REPO_ROOT / "db/duckdb/movies.duckdb")
DEFAULT_CARTEBASE = str(Path.home() / "repos/cartebase/cartebase.db")

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS movies (
    id              INTEGER PRIMARY KEY,
    imdb_title_id   TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    year            INTEGER,
    genre           TEXT,
    runtime_min     INTEGER,
    imdb_rating     REAL,
    rating_count    INTEGER,
    directors       TEXT,
    imdb_url        TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(year)",
    "CREATE INDEX IF NOT EXISTS idx_movies_imdb_rating ON movies(imdb_rating)",
    "CREATE INDEX IF NOT EXISTS idx_movies_rating_count ON movies(rating_count)",
]


def fetch_movies(duck_path: str, limit: int, min_rating: float, min_votes: int) -> list[dict]:
    conn = duckdb.connect(duck_path, read_only=True)
    rows = conn.execute(f"""
        SELECT
            imdb_title_id,
            title,
            year,
            genre,
            CASE
                WHEN runtime IS NOT NULL
                THEN TRY_CAST(TRIM(REPLACE(runtime, 'min', '')) AS INTEGER)
            END AS runtime_min,
            CAST(IMDb_rating AS FLOAT)    AS imdb_rating,
            CAST(ratingCount AS INTEGER)  AS rating_count,
            directors,
            'https://www.imdb.com/title/' || imdb_title_id || '/' AS imdb_url
        FROM imdb_movies
        WHERE IMDb_rating >= {min_rating}
          AND ratingCount  >= {min_votes}
        ORDER BY (IMDb_rating * log10(ratingCount)) DESC
        LIMIT {limit}
    """).fetchall()
    cols = ["imdb_title_id", "title", "year", "genre", "runtime_min",
            "imdb_rating", "rating_count", "directors", "imdb_url"]
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


def upsert(sqlite_path: str, movies: list[dict]) -> tuple[int, int]:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    conn = sqlite3.connect(sqlite_path)
    conn.execute(CREATE_TABLE)
    for idx in CREATE_INDEXES:
        conn.execute(idx)

    inserted = updated = 0
    for m in movies:
        existing = conn.execute(
            "SELECT id FROM movies WHERE imdb_title_id = ?", [m["imdb_title_id"]]
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE movies
                SET title=?, year=?, genre=?, runtime_min=?,
                    imdb_rating=?, rating_count=?, directors=?, imdb_url=?,
                    updated_at=?
                WHERE imdb_title_id=?
            """, [m["title"], m["year"], m["genre"], m["runtime_min"],
                  m["imdb_rating"], m["rating_count"], m["directors"], m["imdb_url"],
                  now, m["imdb_title_id"]])
            updated += 1
        else:
            conn.execute("""
                INSERT INTO movies
                    (imdb_title_id, title, year, genre, runtime_min,
                     imdb_rating, rating_count, directors, imdb_url, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, [m["imdb_title_id"], m["title"], m["year"], m["genre"], m["runtime_min"],
                  m["imdb_rating"], m["rating_count"], m["directors"], m["imdb_url"], now, now])
            inserted += 1

    conn.execute("""
        INSERT INTO import_log
            (importer, source_file, source, status, started_at, finished_at,
             rows_inserted, rows_updated)
        VALUES ('export_to_cartebase', 'imdb_movies', 'cinemattr-db', 'success', ?, ?, ?, ?)
    """, [now, now, inserted, updated])

    conn.commit()
    conn.close()
    return inserted, updated


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", default=DEFAULT_DUCKDB)
    parser.add_argument("--cartebase", default=DEFAULT_CARTEBASE)
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--min-rating", type=float, default=7.0)
    parser.add_argument("--min-votes", type=int, default=10000)
    args = parser.parse_args()

    log.info("Fetching top %d movies (rating≥%.1f, votes≥%d) …",
             args.limit, args.min_rating, args.min_votes)
    movies = fetch_movies(args.duckdb, args.limit, args.min_rating, args.min_votes)
    log.info("Fetched %d rows", len(movies))

    if not movies:
        log.warning("No movies matched filters — check imdb_movies is populated")
        return

    log.info("Top 5: %s", [(m["title"], m["year"], m["imdb_rating"]) for m in movies[:5]])

    inserted, updated = upsert(args.cartebase, movies)
    log.info("Done — inserted=%d updated=%d (total=%d)", inserted, updated, inserted + updated)


if __name__ == "__main__":
    main()
