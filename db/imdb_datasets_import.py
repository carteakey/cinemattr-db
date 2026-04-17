#!/usr/bin/env python3
"""
Import IMDb official datasets into DuckDB.

Replaces the broken web scraper (IMDb redesigned their site).
Uses the free TSV datasets published by IMDb (non-commercial use).

Downloads:
  title.basics.tsv.gz    — title, year, runtime, genres
  title.ratings.tsv.gz   — rating, numVotes
  title.crew.tsv.gz      — directors (nconst IDs)
  name.basics.tsv.gz     — name lookup for nconst IDs

Caches downloads to --cache-dir (default: db/duckdb/imdb_cache/).
Safe to re-run — UPSERT by imdb_title_id.

Usage:
  python db/imdb_datasets_import.py \
    --db db/duckdb/movies.duckdb \
    --start-year 2000 --end-year 2024
"""
from __future__ import annotations

import argparse
import gzip
import logging
import os
import shutil
import urllib.request
from datetime import datetime
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("imdb-import")

IMDB_DATASETS_BASE = "https://datasets.imdb.com/dataset"
DATASETS = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
    "crew": "title.crew.tsv.gz",
    "names": "name.basics.tsv.gz",
}

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CACHE = str(REPO_ROOT / "db/duckdb/imdb_cache")
DEFAULT_DB = str(REPO_ROOT / "db/duckdb/movies.duckdb")


MANUAL_DOWNLOAD_INSTRUCTIONS = """
IMDb dataset download failed. Download these files manually:

  1. Go to: https://developer.imdb.com/non-commercial-datasets/
  2. Download these 4 files into {cache_dir}:
       title.basics.tsv.gz     (~55 MB)
       title.ratings.tsv.gz    (~4 MB)
       title.crew.tsv.gz       (~25 MB)
       name.basics.tsv.gz      (~250 MB)

  mkdir -p {cache_dir}
  cd {cache_dir}
  curl -O https://datasets.imdb.com/dataset/title.basics.tsv.gz
  curl -O https://datasets.imdb.com/dataset/title.ratings.tsv.gz
  curl -O https://datasets.imdb.com/dataset/title.crew.tsv.gz
  curl -O https://datasets.imdb.com/dataset/name.basics.tsv.gz

  Then re-run this script.
"""


def download(url: str, dest: Path) -> None:
    log.info("Downloading %s → %s", url, dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urllib.request.urlopen(url) as resp, open(dest, "wb") as f:
            shutil.copyfileobj(resp, f)
        log.info("Saved %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    except Exception as e:
        if dest.exists():
            dest.unlink()
        raise RuntimeError(
            f"Download failed: {e}\n{MANUAL_DOWNLOAD_INSTRUCTIONS.format(cache_dir=dest.parent)}"
        ) from e


def ensure_dataset(name: str, cache_dir: Path, force: bool = False) -> Path:
    filename = DATASETS[name]
    dest = cache_dir / filename
    if dest.exists() and not force:
        log.info("Using cached %s (%.1f MB)", filename, dest.stat().st_size / 1e6)
        return dest
    download(f"{IMDB_DATASETS_BASE}/{filename}", dest)
    return dest


def decompress(gz_path: Path) -> Path:
    tsv_path = gz_path.with_suffix("")
    if tsv_path.exists():
        return tsv_path
    log.info("Decompressing %s …", gz_path.name)
    with gzip.open(gz_path, "rb") as gz, open(tsv_path, "wb") as tsv:
        shutil.copyfileobj(gz, tsv)
    log.info("Decompressed → %s (%.1f MB)", tsv_path.name, tsv_path.stat().st_size / 1e6)
    return tsv_path


def import_to_duckdb(db_path: str, cache_dir: Path, start_year: int, end_year: int, force_download: bool) -> None:
    basics_gz = ensure_dataset("basics", cache_dir, force_download)
    ratings_gz = ensure_dataset("ratings", cache_dir, force_download)
    crew_gz = ensure_dataset("crew", cache_dir, force_download)
    names_gz = ensure_dataset("names", cache_dir, force_download)

    basics_tsv = str(decompress(basics_gz))
    ratings_tsv = str(decompress(ratings_gz))
    crew_tsv = str(decompress(crew_gz))
    names_tsv = str(decompress(names_gz))

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)

    log.info("Building director name lookup …")
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _name_lookup AS
        SELECT nconst, primaryName
        FROM read_csv(
            $1,
            delim='\t', header=true, nullstr='\\N',
            columns={
                'nconst': 'VARCHAR', 'primaryName': 'VARCHAR',
                'birthYear': 'VARCHAR', 'deathYear': 'VARCHAR',
                'primaryProfession': 'VARCHAR', 'knownForTitles': 'VARCHAR'
            }
        )
    """, [names_tsv])

    log.info("Building crew lookup …")
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _crew AS
        SELECT tconst, directors
        FROM read_csv(
            $1,
            delim='\t', header=true, nullstr='\\N',
            columns={'tconst': 'VARCHAR', 'directors': 'VARCHAR', 'writers': 'VARCHAR'}
        )
        WHERE directors IS NOT NULL
    """, [crew_tsv])

    log.info("Importing movie metadata for %d–%d …", start_year, end_year)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS imdb_movies (
            imdb_title_id TEXT PRIMARY KEY,
            title TEXT, year INTEGER, certificate TEXT,
            genre TEXT, runtime TEXT, description TEXT,
            IMDb_rating NUMERIC, MetaScore NUMERIC,
            ratingCount INTEGER, directors TEXT, stars TEXT
        )
    """)

    conn.execute(f"""
        INSERT INTO imdb_movies
        SELECT
            b.tconst                              AS imdb_title_id,
            b.primaryTitle                        AS title,
            CAST(b.startYear AS INTEGER)          AS year,
            NULL                                  AS certificate,
            b.genres                              AS genre,
            b.runtimeMinutes                      AS runtime,
            NULL                                  AS description,
            CAST(r.averageRating AS FLOAT)        AS IMDb_rating,
            NULL                                  AS MetaScore,
            CAST(r.numVotes AS INTEGER)           AS ratingCount,
            (
                SELECT string_agg(n.primaryName, ',')
                FROM (
                    SELECT UNNEST(str_split(c.directors, ',')) AS nconst_id
                ) d
                JOIN _name_lookup n ON n.nconst = d.nconst_id
            )                                     AS directors,
            NULL                                  AS stars
        FROM read_csv(
            $1,
            delim='\t', header=true, nullstr='\\N',
            columns={{
                'tconst': 'VARCHAR', 'titleType': 'VARCHAR',
                'primaryTitle': 'VARCHAR', 'originalTitle': 'VARCHAR',
                'isAdult': 'VARCHAR', 'startYear': 'VARCHAR',
                'endYear': 'VARCHAR', 'runtimeMinutes': 'VARCHAR',
                'genres': 'VARCHAR'
            }}
        ) b
        JOIN read_csv(
            $2,
            delim='\t', header=true,
            columns={{'tconst': 'VARCHAR', 'averageRating': 'FLOAT', 'numVotes': 'INTEGER'}}
        ) r ON r.tconst = b.tconst
        LEFT JOIN _crew c ON c.tconst = b.tconst
        WHERE b.titleType = 'movie'
          AND b.startYear != '\\N'
          AND b.isAdult = '0'
          AND CAST(b.startYear AS INTEGER) BETWEEN {start_year} AND {end_year}
          AND r.numVotes >= 100
        ON CONFLICT (imdb_title_id) DO UPDATE
            SET IMDb_rating=EXCLUDED.IMDb_rating,
                ratingCount=EXCLUDED.ratingCount,
                directors=EXCLUDED.directors
    """, [basics_tsv, ratings_tsv])

    count = conn.execute("SELECT COUNT(*) FROM imdb_movies").fetchone()[0]
    by_year = conn.execute(
        f"SELECT year, COUNT(*) FROM imdb_movies WHERE year BETWEEN {start_year} AND {end_year} GROUP BY year ORDER BY year DESC LIMIT 5"
    ).fetchall()
    conn.close()

    log.info("imdb_movies: %d total rows", count)
    log.info("Recent years: %s", by_year)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import IMDb official datasets into DuckDB")
    parser.add_argument("--db", default=DEFAULT_DB, help="Target DuckDB path")
    parser.add_argument("--cache-dir", default=DEFAULT_CACHE, help="Directory for downloaded TSV.GZ files")
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--force-download", action="store_true", help="Re-download even if cache exists")
    args = parser.parse_args()

    import_to_duckdb(
        args.db,
        Path(args.cache_dir),
        args.start_year,
        args.end_year,
        args.force_download,
    )
    log.info("Done.")


if __name__ == "__main__":
    main()
