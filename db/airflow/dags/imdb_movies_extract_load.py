from airflow.decorators import dag, task
from airflow.models.param import Param
import pendulum
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

"""
This DAG is used to extract and load the IMDB movies dataset.
"""


@dag(
    dag_id="imdb_movies_extract_load",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper","cinemattr"],
    params={
        "year": Param(2023, type="integer", minimum=1900, maximum=2023),
        "pages": Param(10, type="integer", minimum=0, maximum=20),
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
        from db.airflow.dags.scrapers.IMDb_movies import scrape

        # from scrapers.imdb_proxy import scrape #Use for proxy (when scraping for 100 years)
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
        try:
            duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
            conn = duckdb_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.close()
            conn.close()
            return 0
        except Exception as e:
            return 1

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
