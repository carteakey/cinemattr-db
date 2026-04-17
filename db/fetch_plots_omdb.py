#!/usr/bin/env python3
"""
Fetch movie plot text from OMDB API → imdb_plots table in DuckDB.

OMDB free tier: 1000 req/day. Paid tiers remove limit.
Resumable — skips title_ids already in imdb_plots.
Run daily until all desired movies have plots.

Usage:
  OMDB_API_KEY=xxxxx python db/fetch_plots_omdb.py
  python db/fetch_plots_omdb.py --limit 1000 --min-votes 50000   # top 1k by popularity
  python db/fetch_plots_omdb.py --limit 5000 --delay 0.15        # faster on paid tier
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import duckdb
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("omdb-fetch")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = str(REPO_ROOT / "db/duckdb/movies.duckdb")
OMDB_URL = "https://www.omdbapi.com/"


def fetch_plot(title_id: str, api_key: str) -> tuple[str | None, str | None]:
    """Returns (short_plot, full_plot) or (None, None) on failure."""
    try:
        r = requests.get(OMDB_URL, params={"i": title_id, "plot": "full", "apikey": api_key}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("Response") != "True":
            return None, None
        short = data.get("Plot") or None
        # OMDB returns same field for short/full with plot=full
        return short, short
    except Exception as e:
        log.warning("OMDB error for %s: %s", title_id, e)
        return None, None


def get_candidates(conn: duckdb.DuckDBPyConnection, limit: int, min_votes: int) -> list[str]:
    rows = conn.execute(f"""
        SELECT m.imdb_title_id
        FROM imdb_movies m
        LEFT JOIN imdb_plots p ON p.imdb_title_id = m.imdb_title_id
        WHERE p.imdb_title_id IS NULL
          AND m.ratingCount >= {min_votes}
        ORDER BY (m.IMDb_rating * log10(m.ratingCount)) DESC
        LIMIT {limit}
    """).fetchall()
    return [r[0] for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--api-key", default=os.getenv("OMDB_API_KEY", ""))
    parser.add_argument("--limit", type=int, default=1000, help="Max titles to fetch this run")
    parser.add_argument("--min-votes", type=int, default=10000)
    parser.add_argument("--delay", type=float, default=0.25, help="Seconds between requests (free tier: 0.25)")
    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("Set OMDB_API_KEY env var or pass --api-key")

    conn = duckdb.connect(args.db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS imdb_plots (
            imdb_title_id TEXT PRIMARY KEY,
            summary TEXT,
            plot TEXT
        )
    """)

    candidates = get_candidates(conn, args.limit, args.min_votes)
    log.info("%d titles to fetch (limit=%d, min_votes=%d)", len(candidates), args.limit, args.min_votes)

    fetched = skipped = 0
    for i, title_id in enumerate(candidates, 1):
        short, full = fetch_plot(title_id, args.api_key)
        if full:
            conn.execute("""
                INSERT INTO imdb_plots (imdb_title_id, summary, plot)
                VALUES (?, ?, ?)
                ON CONFLICT (imdb_title_id) DO UPDATE
                SET summary=EXCLUDED.summary, plot=EXCLUDED.plot
            """, [title_id, short, full])
            fetched += 1
        else:
            skipped += 1

        if i % 100 == 0:
            total = conn.execute("SELECT COUNT(*) FROM imdb_plots").fetchone()[0]
            log.info("Progress %d/%d — fetched=%d skipped=%d db_total=%d",
                     i, len(candidates), fetched, skipped, total)

        time.sleep(args.delay)

    total = conn.execute("SELECT COUNT(*) FROM imdb_plots").fetchone()[0]
    conn.close()
    log.info("Done — fetched=%d skipped=%d total_in_db=%d", fetched, skipped, total)


if __name__ == "__main__":
    main()
