"""Pattern 05 - Dynamic width (withParam from a JSON-array parameter).

dagron version (05_dynamic_shards_withparam.yaml): `with_param: "{{ shards }}"` reads
the fan-out list from a PARAMETER holding a JSON-array string. Override `shards` at
submit time and the graph width changes - data-driven parallelism.

Airflow mapping:
- Data-driven width -> dynamic task mapping over a list produced at run time. The
  faithful "fan out over a run param" analogue is `.expand_kwargs()` fed by an
  upstream task that parses the DAG `param` JSON array (`@task`-style XCom). Because
  this catalog uses ONLY BashOperator + core mapping, we resolve the param list with
  a small upstream PythonOperator that returns the bash commands to map over.

Caveat: in dagron the width is fixed at expansion time; in Airflow mapping over an
XCom list makes the width truly run-time dynamic (closer to with_param's intent).
The default `shards` mirrors the dagron default "[1, 2, 3]".
"""

import json
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def build_commands(**context):
    """Parse the JSON-array `shards` param into one echo command per shard."""
    raw = context["params"]["shards"]
    shard_ids = json.loads(raw)
    return [f"echo ingest shard {sid}" for sid in shard_ids]


with DAG(
    dag_id="pattern_05_dynamic_shards_withparam",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Override `shards` at trigger time to change the fan-out width.
    params={"shards": Param("[1, 2, 3]", type="string")},
    tags=["dagron-pattern", "with-param", "dynamic"],
) as dag:
    plan = PythonOperator(task_id="plan_shards", python_callable=build_commands)

    # Map over the run-time list returned by `plan` (data-driven width).
    ingest = BashOperator.partial(task_id="ingest").expand(
        bash_command=plan.output
    )

    rollup = BashOperator(task_id="rollup", bash_command="echo rollup")

    plan >> ingest >> rollup
