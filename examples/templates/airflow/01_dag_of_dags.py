"""Pattern 01 - DAG of DAGs (a workflow that calls another workflow).

dagron version (01_dag_of_dags.yaml): a `template` named `etl` is a reusable
sub-DAG (build -> process -> publish); the main DAG `call`s it via `template: etl`
between `prepare` and `notify`. The engine expands the call inline into one flat DAG.

Airflow mapping:
- A dagron *template* (reusable sub-DAG) -> a Python factory function that builds
  a `TaskGroup`. The factory `etl_group(...)` is the analogue of the `etl` template;
  calling it inside the DAG is the analogue of the `run-etl` call task.
- `depends_on` -> the `>>` dependency operator, wiring `prepare >> etl >> notify`.

Caveat: Airflow has no runtime sub-DAG "call"; a TaskGroup is a parse-time grouping
of tasks in the same DAG, which is the faithful flat-expansion analogue of dagron.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def etl_group(dag: DAG) -> TaskGroup:
    """Reusable sub-DAG: build -> process -> publish (mirrors the `etl` template)."""
    with TaskGroup(group_id="etl") as tg:
        build = BashOperator(task_id="build", bash_command="echo build", dag=dag)
        process = BashOperator(task_id="process", bash_command="echo process", dag=dag)
        publish = BashOperator(task_id="publish", bash_command="echo publish", dag=dag)
        build >> process >> publish
    return tg


with DAG(
    dag_id="pattern_01_dag_of_dags",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "sub-dag"],
) as dag:
    prepare = BashOperator(task_id="prepare", bash_command="echo prepare")
    etl = etl_group(dag)
    notify = BashOperator(task_id="notify", bash_command="echo done")

    prepare >> etl >> notify
