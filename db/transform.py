#!/usr/bin/env python3
"""
Build movie_plots_ld table from raw DuckDB tables.
Replaces the entire dbt pipeline (no dbt required).

Sources (joined in priority order):
  1. imdb_plots  (OMDB plot text, keyed by imdb_title_id)
  2. wiki_plots  (Wikipedia scrape, needs title/year matching)

Output: movie_plots_ld — normalized, ready for api/load_embeddings.py.

Usage:
  python db/transform.py
  python db/transform.py --db db/duckdb/movies.duckdb
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("transform")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = str(REPO_ROOT / "db/duckdb/movies.duckdb")

def _WIKI_CLEAN(col: str) -> str:
    return (
        f"regexp_replace("
        r"regexp_replace("
        r"regexp_replace("
        r"regexp_replace("
        r"regexp_replace("
        r"regexp_replace("
        r"regexp_replace("
        r"regexp_replace("
        f"    {col},"
        r"    '<!--.*?-->',       '',   'g'),"
        r"    '\[\[(.+?)\]\]',    '\1', 'g'),"
        r"    '<ref>.*?</ref>',   '',   'g'),"
        r"    '<ref .*?/>',       '',   'g'),"
        r"    '<[^>]*>',          '',   'g'),"
        r"    '\{.*?\}',          '',   'g'),"
        r"    '\|',               ' ',  'g'),"
        r"    '\s+',              ' ',  'g')"
    )


SQL_WIKI_CLEAN = f"""
CREATE OR REPLACE TABLE _wiki_clean AS
SELECT
    trim(regexp_replace(regexp_replace(title, '\\(\\d{{4}}\\s\\w+\\)', ''), '\\(film\\)', '')) AS title,
    CASE
        WHEN regexp_extract(
                 regexp_extract(external_links, '\\{{\\{{[Ii][Mm][Dd][Bb] title(.*)\\}}\\}}', 1),
                 '\\d+') <> ''
        THEN regexp_extract(
                 regexp_extract(external_links, '\\{{\\{{[Ii][Mm][Dd][Bb] title(.*)\\}}\\}}', 1),
                 '\\d+')
        WHEN regexp_extract(external_links, 'https://www\\.imdb\\.com/title/tt(\\d+)', 1) <> ''
        THEN regexp_extract(external_links, 'https://www\\.imdb\\.com/title/tt(\\d+)', 1)
        ELSE NULL
    END AS imdb_num,
    CASE
        WHEN regexp_extract(external_links, '\\[\\[Category:(\\d{{4}})', 1) <> ''
        THEN TRY_CAST(regexp_extract(external_links, '\\[\\[Category:(\\d{{4}})', 1) AS INTEGER)
        ELSE NULL
    END AS year,
    {_WIKI_CLEAN('summary')} AS summary,
    {_WIKI_CLEAN('plot')}    AS plot
FROM wiki_plots
WHERE plot NOT IN ('No plot available', ' ', '')
  AND plot IS NOT NULL
"""

SQL_MOVIE_PLOTS_LD = """
CREATE OR REPLACE TABLE movie_plots_ld AS

-- ── Priority 1: OMDB plots (imdb_plots) ─────────────────────────────────────
WITH omdb AS (
    SELECT
        m.imdb_title_id                                           AS source,
        lower(m.title)                                            AS title,
        m.year,
        COALESCE(str_split(lower(m.genre),  ','), ['NA'])         AS genre,
        TRY_CAST(trim(replace(m.runtime, 'min', '')) AS INTEGER)  AS runtime,
        m.ratingCount,
        p.plot                                                    AS plot,
        p.summary                                                 AS summary,
        CAST(m.IMDb_rating AS FLOAT)                              AS imdb_rating,
        COALESCE(str_split(lower(m.directors), ','), ['NA'])      AS directors,
        ['NA']                                                    AS stars
    FROM imdb_movies m
    INNER JOIN imdb_plots p ON p.imdb_title_id = m.imdb_title_id
    WHERE p.plot IS NOT NULL
      AND length(trim(p.plot)) > 20
      AND m.ratingCount > 1000
),

-- ── Priority 2: Wikipedia plots (not already covered by OMDB) ───────────────
wiki AS (
    SELECT
        m.imdb_title_id                                           AS source,
        lower(m.title)                                            AS title,
        m.year,
        COALESCE(str_split(lower(m.genre),  ','), ['NA'])         AS genre,
        TRY_CAST(trim(replace(m.runtime, 'min', '')) AS INTEGER)  AS runtime,
        m.ratingCount,
        w.plot                                                    AS plot,
        w.summary                                                 AS summary,
        CAST(m.IMDb_rating AS FLOAT)                              AS imdb_rating,
        COALESCE(str_split(lower(m.directors), ','), ['NA'])      AS directors,
        ['NA']                                                    AS stars
    FROM imdb_movies m
    INNER JOIN _wiki_clean w
        ON ('tt' || lpad(w.imdb_num, 7, '0') = m.imdb_title_id)
        OR (lower(w.title) = lower(m.title) AND w.year = m.year)
    WHERE m.ratingCount > 1000
      AND w.plot IS NOT NULL
      AND length(trim(w.plot)) > 20
      AND m.imdb_title_id NOT IN (SELECT source FROM omdb)
)

SELECT * FROM omdb
UNION ALL
SELECT * FROM wiki
"""


def run(db_path: str) -> None:
    conn = duckdb.connect(db_path)

    # Ensure base tables exist (no-ops if already present)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS imdb_plots (
            imdb_title_id TEXT PRIMARY KEY, summary TEXT, plot TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wiki_plots (
            title TEXT PRIMARY KEY, summary TEXT, plot TEXT, external_links TEXT
        )
    """)

    wiki_count = conn.execute("SELECT COUNT(*) FROM wiki_plots").fetchone()[0]
    omdb_count = conn.execute("SELECT COUNT(*) FROM imdb_plots").fetchone()[0]
    log.info("wiki_plots=%d  imdb_plots=%d", wiki_count, omdb_count)

    log.info("Building _wiki_clean …")
    conn.execute(SQL_WIKI_CLEAN)
    wiki_clean = conn.execute("SELECT COUNT(*) FROM _wiki_clean").fetchone()[0]
    log.info("_wiki_clean rows: %d", wiki_clean)

    log.info("Building movie_plots_ld …")
    conn.execute(SQL_MOVIE_PLOTS_LD)

    total = conn.execute("SELECT COUNT(*) FROM movie_plots_ld").fetchone()[0]
    by_source = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE source IN (SELECT imdb_title_id FROM imdb_plots)) AS from_omdb,
            COUNT(*) FILTER (WHERE source NOT IN (SELECT imdb_title_id FROM imdb_plots)) AS from_wiki
        FROM movie_plots_ld
    """).fetchone()
    conn.close()

    log.info("movie_plots_ld: %d total (omdb=%d wiki=%d)", total, by_source[0], by_source[1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Build movie_plots_ld without dbt")
    parser.add_argument("--db", default=DEFAULT_DB)
    args = parser.parse_args()
    run(args.db)
    log.info("Done.")


if __name__ == "__main__":
    main()
