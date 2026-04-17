import os

import pendulum
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

"""
This DAG is used to trigger the DAG that extracts and loads the data from the IMDB website.
The objective is to get a list of movies that we can build our data model from.
"""

CURRENT_YEAR = pendulum.now("UTC").year
START_YEAR = int(os.getenv("CINEMATTR_START_YEAR", "1950"))
END_YEAR = int(os.getenv("CINEMATTR_END_YEAR", str(CURRENT_YEAR)))


@dag(
    dag_id="imdb_movies_trigger",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper", "cinemattr"],
)
def trigger_dag():
    years = range(START_YEAR, END_YEAR + 1)
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
