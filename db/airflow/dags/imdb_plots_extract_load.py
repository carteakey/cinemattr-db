import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from duckdb_provider.hooks.duckdb_hook import DuckDBHook

CURRENT_YEAR = pendulum.now("UTC").year


@dag(
    dag_id="imdb_plots_extract_load",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper", "cinemattr"],
    params={"year": Param(CURRENT_YEAR, type="integer", minimum=1900, maximum=CURRENT_YEAR)},
    render_template_as_native_obj=True,
)
def taskflow():
    @task
    def create_imdb_plot_temp_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS imdb_plots_temp (
            imdb_title_id TEXT PRIMARY KEY,
            summary TEXT,
            plot TEXT
        );"""
        )
        cursor.close()
        conn.close()

    @task
    def create_imdb_plot_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS imdb_plots(
            imdb_title_id TEXT PRIMARY KEY,
            summary TEXT,
            plot TEXT
        );"""
        )
        cursor.close()
        conn.close()

    @task
    def truncate_imdb_plot_temp_table():
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE imdb_plots_temp;")
        cursor.close()
        conn.close()

    @task
    def get_titles(year):
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        query = f"""
        select
            array_slice(imdb_title_id, 3, null)
        from
            main.imdb_movies
        WHERE
            year = {year}
            and ratingCount > 1000
            and imdb_title_id not in (
                SELECT
                    imdb_title_id
                FROM
                    imdb_plots)
        """
        results = cursor.execute(query).fetchall()
        title_ids = [x[0] for x in results]
        cursor.close()
        conn.close()
        return title_ids

    @task.virtualenv(
        system_site_packages=True,
        requirements=["pandas", "beautifulsoup4", "requests"],
    )
    def extract(title_ids):
        import os

        from airflow.models import Variable
        os.environ["SCRAPINGANT_API_KEY"] = Variable.get("SCRAPINGANT_API_KEY")
        from scrapers.IMDb_summary_proxy import scrape

        data_file = scrape(title_ids)
        return {"data_file": data_file}

    @task
    def copy_data(files):
        data_file = files["data_file"]
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO imdb_plots_temp SELECT * FROM read_csv('"""
            + data_file
            + """', delim=',', header=True, columns = {'imdb_title_id': 'TEXT', 'summary': 'TEXT', 'plot': 'TEXT'});"""
        )
        cursor.close()
        conn.close()

    @task
    def merge_data():
        query = """
            INSERT INTO imdb_plots
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM imdb_plots_temp
            ) t
            ON CONFLICT (imdb_title_id) DO UPDATE
            SET summary=EXCLUDED.summary,
            plot=EXCLUDED.plot;
        """
        duckdb_hook = DuckDBHook(duckdb_conn_id="cinemattr-db")
        conn = duckdb_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.close()

    year = "{{ params.year }}"

    title = get_titles(year)
    extracts = extract(title)
    (
        create_imdb_plot_temp_table()
        >> create_imdb_plot_table()
        >> truncate_imdb_plot_temp_table()
        >> title
        >> extracts
        >> copy_data(extracts)
        >> merge_data()
    )


dag = taskflow()
