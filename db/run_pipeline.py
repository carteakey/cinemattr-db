#!/usr/bin/env python3
"""Refresh Cinemattr's movie corpus and vector index.

Pipeline:
  1. Import current IMDb non-commercial datasets into DuckDB.
  2. Build the normalized ``movie_plots_ld`` table from available OMDb plots.
  3. Embed new/changed plots into the DuckDB VSS store.

Plot enrichment is intentionally a separate resumable command because OMDb quotas vary:
``OMDB_API_KEY=... python db/fetch_plots_omdb.py --limit 1000``.
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = REPO_ROOT / "db"
API_DIR = REPO_ROOT / "api"
DEFAULT_MOVIES_DB = str(DB_DIR / "duckdb/movies.duckdb")
DEFAULT_VSS_DB = str(REPO_ROOT / "data/cinemattr.duckdb")
DEFAULT_IMDB_CACHE = str(DB_DIR / "duckdb/imdb_cache")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")

# Running this file directly otherwise puts ``db/`` first on sys.path, causing the
# ``db/duckdb`` data directory to shadow the installed ``duckdb`` package.
sys.path[0] = str(REPO_ROOT)


def import_imdb(db_path: str, cache_dir: str, start_year: int, end_year: int, force: bool) -> None:
    from db.imdb_datasets_import import import_to_duckdb

    import_to_duckdb(db_path, Path(cache_dir), start_year, end_year, force)


def transform(db_path: str) -> None:
    from db.transform import run

    run(db_path)


def load_vectors(movies_db: str, vss_db: str) -> None:
    env = os.environ.copy()
    env["DUCKDB_PATH"] = vss_db
    command = [
        sys.executable,
        str(API_DIR / "load_embeddings.py"),
        "--source", movies_db,
        "--table", "movie_plots_ld",
        "--id-col", "source",
        "--plot-col", "plot",
        "--title-col", "title",
        "--year-col", "year",
        "--genre-col", "genre",
        "--rating-col", "imdb_rating",
    ]
    subprocess.run(command, env=env, cwd=API_DIR, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh the Cinemattr data and vector stores")
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--db-path", default=DEFAULT_MOVIES_DB)
    parser.add_argument("--vss-db", default=DEFAULT_VSS_DB)
    parser.add_argument("--imdb-cache", default=DEFAULT_IMDB_CACHE)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--skip-imdb-movies", action="store_true")
    parser.add_argument("--skip-transform", action="store_true")
    parser.add_argument("--skip-vss", action="store_true")
    args = parser.parse_args()

    if args.start_year > args.end_year:
        parser.error("--start-year must be less than or equal to --end-year")

    if not args.skip_imdb_movies:
        log.info("Importing IMDb datasets for %d–%d", args.start_year, args.end_year)
        import_imdb(args.db_path, args.imdb_cache, args.start_year, args.end_year, args.force_download)
    if not args.skip_transform:
        log.info("Building movie_plots_ld")
        transform(args.db_path)
    if not args.skip_vss:
        log.info("Updating DuckDB VSS store")
        load_vectors(args.db_path, args.vss_db)

    log.info("Pipeline complete")


if __name__ == "__main__":
    main()
