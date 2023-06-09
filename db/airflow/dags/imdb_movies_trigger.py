import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import chain

"""
This DAG is used to trigger the DAG that extracts and loads the data from the IMDB website. 
The objective is to get a list of movies that we can build our data model from.
"""


@dag(
    dag_id="imdb_movies_trigger",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper",'cinemattr'],
)
def trigger_dag():
    years = range(1950, 2024)

    op_list = [
        TriggerDagRunOperator(
            task_id=f"imdb_movies_trigger_{year}",
            trigger_dag_id="imdb_movies_extract_load",
            conf={"year": year, "pages": 10},
            wait_for_completion=True,
            trigger_rule="all_done",
        )
        for year in years
    ]

    chain(*op_list)


dag = trigger_dag()
