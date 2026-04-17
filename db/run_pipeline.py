#!/usr/bin/env python3
"""
Standalone cinemattr data pipeline (no Airflow / Docker required).

Phases:
  1. Scrape IMDb movie metadata (aiohttp, no proxy)
  2. Scrape Wikipedia plots (mwclient, no API key)
  3. [optional] Scrape IMDb plot summaries (ScrapingAnt proxy, needs SCRAPINGANT_API_KEY)
  4. Run dbt transforms → movie_plots_ld table
  5. Embed plots → DuckDB VSS table (api/load_embeddings.py)

Usage:
  python db/run_pipeline.py --start-year 2015 --end-year 2024
  python db/run_pipeline.py --start-year 1950 --end-year 2026 --pages 10
  python db/run_pipeline.py --skip-wiki --skip-imdb-plots --skip-dbt --skip-vss  # just scrape

Requirements:
  pip install -r db/requirements-pipeline.txt
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_DIR = REPO_ROOT / "db"
SCRAPERS_DIR = DB_DIR / "airflow/dags/scrapers"
DBT_DIR = DB_DIR / "dbt"
DBT_PROFILES_DIR = DBT_DIR / ".dbt"
API_DIR = REPO_ROOT / "api"
DEFAULT_MOVIES_DB = str(DB_DIR / "duckdb/movies.duckdb")
DEFAULT_VSS_DB = str(REPO_ROOT / "data/cinemattr.duckdb")
DEFAULT_IMDB_CACHE = str(DB_DIR / "duckdb/imdb_cache")

sys.path.insert(0, str(SCRAPERS_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------

def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)


def setup_tables(db_path: str) -> None:
    conn = connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS imdb_movies (
            imdb_title_id TEXT PRIMARY KEY,
            title TEXT, year INTEGER, certificate TEXT,
            genre TEXT, runtime TEXT, description TEXT,
            IMDb_rating NUMERIC, MetaScore NUMERIC,
            ratingCount INTEGER, directors TEXT, stars TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS imdb_plots (
            imdb_title_id TEXT PRIMARY KEY,
            summary TEXT, plot TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wiki_plots (
            title TEXT PRIMARY KEY,
            summary TEXT, plot TEXT, external_links TEXT
        )
    """)
    conn.close()
    log.info("Tables ready: %s", db_path)


def upsert_csv(db_path: str, csv_path: str, table: str, pk: str) -> int:
    if not Path(csv_path).exists():
        log.warning("CSV not found: %s", csv_path)
        return 0
    conn = connect(db_path)
    try:
        conn.execute(f"""
            INSERT INTO {table}
            SELECT * FROM read_csv_auto('{csv_path}', header=true)
            ON CONFLICT ({pk}) DO NOTHING
        """)
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()
    return count


# ---------------------------------------------------------------------------
# Scraping phases
# ---------------------------------------------------------------------------

def phase_imdb_movies_scraper(db_path: str, years: range, pages: int, data_dir: Path) -> None:
    """Legacy web scraper — broken as of 2024 (IMDb redesigned their site)."""
    from IMDb_movies import scrape  # type: ignore

    (data_dir / "imdb").mkdir(parents=True, exist_ok=True)
    log.info("=== Phase 1: IMDb movies via web scraper (%d years) ===", len(years))
    for year in years:
        csv = str(data_dir / "imdb" / f"imdb_movies_{year}.csv")
        try:
            result = scrape(year, pages, data_file=csv)
            out = result if isinstance(result, str) else csv
            count = upsert_csv(db_path, out, "imdb_movies", "imdb_title_id")
            log.info("  %d  imdb_movies → %d total", year, count)
        except Exception as e:
            log.error("  %d  imdb_movies FAILED: %s", year, e)


def phase_imdb_movies_datasets(db_path: str, years: range, cache_dir: str, force_download: bool) -> None:
    """Import from official IMDb TSV datasets — reliable, no scraping."""
    import sys
    sys.path.insert(0, str(DB_DIR))
    from imdb_datasets_import import import_to_duckdb  # type: ignore

    log.info("=== Phase 1: IMDb movies via official datasets (%d–%d) ===", years.start, years.stop - 1)
    import_to_duckdb(db_path, Path(cache_dir), years.start, years.stop - 1, force_download)


def phase_wiki_plots(db_path: str, years: range, data_dir: Path) -> None:
    from wiki_plots import scrape  # type: ignore

    (data_dir / "wiki").mkdir(parents=True, exist_ok=True)
    log.info("=== Phase 2: Wikipedia plots (%d years) ===", len(years))
    for year in years:
        csv = str(data_dir / "wiki" / f"wiki_plots_{year}.csv")
        try:
            result = scrape(year, data_file=csv)
            out = result if isinstance(result, str) else csv
            count = upsert_csv(db_path, out, "wiki_plots", "title")
            log.info("  %d  wiki_plots → %d total", year, count)
        except Exception as e:
            log.error("  %d  wiki_plots FAILED: %s", year, e)


def phase_imdb_plots(db_path: str, years: range, data_dir: Path) -> None:
    from IMDb_summary_proxy import scrape  # type: ignore

    scrapingant_key = os.getenv("SCRAPINGANT_API_KEY", "")
    if not scrapingant_key or scrapingant_key.startswith("X"):
        log.warning("SCRAPINGANT_API_KEY not set — skipping IMDb plots proxy phase")
        return

    (data_dir / "imdb_plots").mkdir(parents=True, exist_ok=True)
    log.info("=== Phase 3: IMDb plot summaries (%d years) ===", len(years))
    conn = connect(db_path)
    for year in years:
        csv = str(data_dir / "imdb_plots" / f"imdb_plots_{year}.csv")
        try:
            ids = [
                row[0]
                for row in conn.execute(
                    "SELECT imdb_title_id FROM imdb_movies WHERE year=? AND ratingCount > 1000", [year]
                ).fetchall()
            ]
            if not ids:
                log.info("  %d  no qualifying titles", year)
                continue
            result = scrape(ids, data_file=csv)
            out = result if isinstance(result, str) else csv
            count = upsert_csv(db_path, out, "imdb_plots", "imdb_title_id")
            log.info("  %d  imdb_plots → %d total", year, count)
        except Exception as e:
            log.error("  %d  imdb_plots FAILED: %s", year, e)
    conn.close()


# ---------------------------------------------------------------------------
# Transform phase (dbt)
# ---------------------------------------------------------------------------

def phase_transform(db_path: str) -> None:
    log.info("=== Phase 4: transform → movie_plots_ld ===")
    sys.path.insert(0, str(DB_DIR))
    from transform import run as run_transform  # type: ignore
    run_transform(db_path)


# ---------------------------------------------------------------------------
# VSS embedding phase
# ---------------------------------------------------------------------------

def phase_vss(movies_db: str, vss_db: str) -> None:
    log.info("=== Phase 5: embed plots → DuckDB VSS ===")
    env = os.environ.copy()
    env["DUCKDB_PATH"] = vss_db
    result = subprocess.run(
        [
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
        ],
        env=env,
        cwd=str(API_DIR),
    )
    if result.returncode != 0:
        raise RuntimeError("VSS load failed")
    log.info("VSS load complete → %s", vss_db)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="cinemattr standalone data pipeline")
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument("--pages", type=int, default=10, help="IMDb search pages per year (max 20, scraper mode only)")
    parser.add_argument("--imdb-source", choices=["datasets", "scraper"], default="datasets",
                        help="'datasets' = official IMDb TSV files (recommended); 'scraper' = broken legacy web scraper")
    parser.add_argument("--imdb-cache", default=DEFAULT_IMDB_CACHE, help="Cache dir for downloaded TSV.GZ files")
    parser.add_argument("--force-download", action="store_true", help="Re-download IMDb datasets even if cached")
    parser.add_argument("--db-path", default=DEFAULT_MOVIES_DB, help="DuckDB path for raw + dbt data")
    parser.add_argument("--vss-db", default=DEFAULT_VSS_DB, help="DuckDB VSS output path (for API)")
    parser.add_argument("--skip-imdb-movies", action="store_true")
    parser.add_argument("--skip-wiki", action="store_true")
    parser.add_argument("--skip-imdb-plots", action="store_true")
    parser.add_argument("--skip-transform", action="store_true")
    parser.add_argument("--skip-vss", action="store_true")
    parser.add_argument("--data-dir", default=str(DB_DIR / "airflow/data"), help="Scratch dir for CSVs")
    args = parser.parse_args()

    years = range(args.start_year, args.end_year + 1)
    data_dir = Path(args.data_dir)

    log.info("Repo root: %s", REPO_ROOT)
    log.info("Movies DB: %s", args.db_path)
    log.info("VSS DB:    %s", args.vss_db)
    log.info("Years:     %d–%d (%d total)", args.start_year, args.end_year, len(years))

    setup_tables(args.db_path)

    if not args.skip_imdb_movies:
        if args.imdb_source == "datasets":
            phase_imdb_movies_datasets(args.db_path, years, args.imdb_cache, args.force_download)
        else:
            phase_imdb_movies_scraper(args.db_path, years, args.pages, data_dir)

    if not args.skip_wiki:
        phase_wiki_plots(args.db_path, years, data_dir)

    if not args.skip_imdb_plots:
        phase_imdb_plots(args.db_path, years, data_dir)

    if not args.skip_transform:
        phase_transform(args.db_path)

    if not args.skip_vss:
        phase_vss(args.db_path, args.vss_db)

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
