"""Pattern 13 - Fan-out where each item runs a multi-step sub-DAG.

dagron version (13_fanout_then_subdag.yaml): fan out over `datasets` and EACH item
runs a full sub-pipeline (download -> validate -> load), then `aggregate` fans in
over every item's sub-DAG exit. datasets=[orders,users,events] -> 3 x (3-step) ->
aggregate.

Airflow mapping (map + DAG-of-DAGs combined):
- "each item runs a multi-step sub-DAG" -> a `@task_group` decorated factory expanded
  with `.expand()` (Airflow 2.6+). Mapped task groups instantiate the whole 3-step
  group ONCE PER dataset - the direct analogue of fanning a template (sub-DAG) out
  over a list.
- Fan-in -> `ingest >> aggregate`; `aggregate` waits for all mapped group instances.

Caveat: mapped task groups require Airflow >= 2.6. The inner steps use @task (which
shell out via BashOperator-equivalent echo through bash); kept to core airflow only.
"""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator

DATASETS = ["orders", "users", "events"]


@task
def download(ds: str) -> str:
    print(f"download {ds}")  # mirrors: echo download {{ ds }}
    return ds


@task
def validate(ds: str) -> str:
    print(f"validate {ds}")  # mirrors: echo validate {{ ds }}
    return ds


@task
def load(ds: str) -> str:
    print(f"load {ds}")  # mirrors: echo load {{ ds }}
    return ds


@task_group
def ingest_one(ds: str):
    """The `ingest-one` sub-DAG: download -> validate -> load (per dataset)."""
    return load(validate(download(ds)))


with DAG(
    dag_id="pattern_13_fanout_then_subdag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "fanout", "sub-dag"],
) as dag:
    # Mapped task group: one full 3-step sub-DAG per dataset (Airflow 2.6+).
    ingest = ingest_one.expand(ds=DATASETS)

    aggregate = BashOperator(
        task_id="aggregate", bash_command="echo aggregate all datasets"
    )

    ingest >> aggregate
