import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import chain

"""
This DAG is used to trigger the WikiPlots DAG.
"""


@dag(
    dag_id="wiki_plots_trigger",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraper",'cinemattr'],
)
def trigger_dag():
    # years = range(1950, 2024)
    years = [
        2017
    ]
    op_list = [
        TriggerDagRunOperator(
            task_id=f"wiki_plots_trigger_{year}",
            trigger_dag_id="wiki_plots_extract_load",
            conf={"year": year},
            wait_for_completion=True,
            trigger_rule="all_done",
        )
        for year in years
    ]

    chain(*op_list)


dag = trigger_dag()
