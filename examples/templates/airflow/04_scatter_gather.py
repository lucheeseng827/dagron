"""Pattern 04 - Scatter / gather (map-reduce, fan-out then fan-in).

dagron version (04_scatter_gather.yaml): `split` -> `work` (fanned out 4 ways over
with_items) -> `reduce`. `reduce` `depends_on: [work]`, so it fans IN over all four
expanded copies' exits. Canonical map-reduce / scatter-gather.

Airflow mapping:
- Fan-out -> dynamic task mapping `.expand()` on the `work` task.
- Fan-in -> a single downstream task set with `mapped_work >> reduce`; Airflow waits
  for every mapped instance before `reduce` runs (the gather), matching the dagron
  `depends_on` over the whole fan-out node.

Caveat: `split >> mapped >> reduce` reproduces the exact scatter-gather wiring.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SHARDS = [0, 1, 2, 3]

with DAG(
    dag_id="pattern_04_scatter_gather",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "map-reduce"],
) as dag:
    split = BashOperator(task_id="split", bash_command="echo split input into 4")

    work = BashOperator.partial(task_id="work").expand(
        bash_command=[f"echo partial result for {n}" for n in SHARDS]
    )

    reduce = BashOperator(task_id="reduce", bash_command="echo combine partials")

    # split scatters; reduce gathers (waits for ALL mapped instances).
    split >> work >> reduce
