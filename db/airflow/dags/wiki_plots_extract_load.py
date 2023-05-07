from airflow.decorators import dag, task
from airflow.models.param import Param
import pendulum
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
"""
This DAG is used to extract and load the plot section from the Wikipedia page of each movie"
"""
@dag(
    dag_id="wiki_extract_load",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper"],
    params={
        "year": Param(2022, type="integer", minimum=1900, maximum=2023)
    },
    render_template_as_native_obj=True)
def taskflow():

    @task
    def create_wiki_plot_temp_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wiki_plots_temp (
            title TEXT PRIMARY KEY,
            summary TEXT,
            plot TEXT,
            external_links TEXT
        );""")
        cursor.close()
        conn.close()

    @task
    def create_wiki_plot_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS wiki_plots (
            title TEXT PRIMARY KEY,
            summary TEXT,
            plot TEXT,
            external_links TEXT
        );""")
        cursor.close()
        conn.close()


    @task
    def truncate_wiki_plot_temp_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE wiki_plots_temp;")
        cursor.close()
        conn.close()

    @task
    def extract(year):
        from scrapers.wiki_plots import scrape
        data_file = scrape(year)
        return {"data_file": data_file}

    @task
    def copy_data(files):
        data_file=files['data_file']
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"""INSERT INTO wiki_plots_temp SELECT * FROM '{data_file}'""")
        cursor.close()
        conn.close()

    @task
    def merge_data():
        query = """
            INSERT INTO wiki_plots
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM wiki_plots_temp
            ) t
            ON CONFLICT (title) DO UPDATE
            SET summary=EXCLUDED.summary,
            plot=EXCLUDED.plot,
            external_links=EXCLUDED.external_links;
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

    year= "{{ params.year }}"

    (
        create_wiki_plot_temp_table()
        >> create_wiki_plot_table()
        >> truncate_wiki_plot_temp_table()
        >> copy_data(extract(year))
        >> merge_data()
    )


dag = taskflow()
