import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

CURRENT_YEAR = pendulum.now("UTC").year

"""
This DAG is used to extract and load the IMDB movies dataset.
"""


@dag(
    dag_id="imdb_movies_extract_load",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper", "cinemattr"],
    params={
        "year": Param(CURRENT_YEAR, type="integer", minimum=1900, maximum=CURRENT_YEAR),
        "pages": Param(10, type="integer", minimum=1, maximum=20),
    },
    render_template_as_native_obj=True,
)
def taskflow():
    @task
    def create_movies_temp_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS imdb_movies_temp (imdb_title_id TEXT PRIMARY KEY,
                    title TEXT,
                    year INTEGER,
                    certificate TEXT,
                    genre TEXT,
                    runtime TEXT,
                    description TEXT,
                    IMDb_rating NUMERIC,
                    MetaScore NUMERIC,
                    ratingCount INTEGER,
                    directors TEXT,
                    stars TEXT)"""
        )
        cursor.close()
        conn.close()

    @task
    def create_movies_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS imdb_movies (imdb_title_id TEXT PRIMARY KEY,
                    title TEXT,
                    year INTEGER,
                    certificate TEXT,
                    genre TEXT,
                    runtime TEXT,
                    description TEXT,
                    IMDb_rating NUMERIC,
                    MetaScore NUMERIC,
                    ratingCount INTEGER,
                    directors TEXT,
                    stars TEXT)"""
        )
        cursor.close()
        conn.close()

    @task
    def truncate_movies_temp_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE imdb_movies_temp;")
        cursor.close()
        conn.close()

    @task
    def extract(year, pages):
        from scrapers.IMDb_movies import scrape

        data_file = scrape(year, pages)
        return {"data_file": data_file}

    @task
    def copy_data(files):
        data_file = files["data_file"]
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"""INSERT INTO imdb_movies_temp SELECT * FROM '{data_file}'""")
        cursor.close()
        conn.close()

    @task
    def merge_data():
        query = """
            INSERT INTO imdb_movies
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM imdb_movies_temp
            ) t
            ON CONFLICT (imdb_title_id) DO UPDATE
            SET IMDb_rating=EXCLUDED.IMDb_rating,
            MetaScore=EXCLUDED.MetaScore,
            ratingCount=EXCLUDED.ratingCount;
        """
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.close()

    year = "{{ params.year }}"
    pages = "{{ params.pages }}"

    (
        create_movies_temp_table()
        >> create_movies_table()
        >> truncate_movies_temp_table()
        >> copy_data(extract(year, pages))
        >> merge_data()
    )


dag = taskflow()
