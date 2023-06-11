from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
import os

os.environ["DBT_PROFILES_DIR"] = Variable.get("DBT_PROFILES_DIR")
os.environ["DBT_PROJECT_DIR"] = Variable.get("DBT_PROJECT_DIR")


@dag(
    dag_id="dbt_run_trigger",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["transform", "cinemattr"],
    render_template_as_native_obj=True,
)
def taskflow():
    @task.virtualenv(
        task_id="dbt_run",
        requirements=["duckdb==0.7.1", "dbt-core", "dbt-duckdb"],
        system_site_packages=False,
    )
    def trigger():
        import os

        # initialize
        from dbt.cli.main import dbtRunner, dbtRunnerResult

        # initialize
        dbt = dbtRunner()
        # create CLI args as a list of strings
        cli_args = ["run", "--project-dir", os.environ["DBT_PROJECT_DIR"]]

        # run the command
        res: dbtRunnerResult = dbt.invoke(cli_args)

        print(res.success)
        print(res.exception)

        # inspect the results
        for r in res.result:
            print(f"{r.node.name}: {r.status}")

    trigger()


dag = taskflow()
