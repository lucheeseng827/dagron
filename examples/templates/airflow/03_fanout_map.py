"""Pattern 03 - Fan-out / map (withItems).

dagron version (03_fanout_map.yaml): `with_items: [alpha, beta, gamma]` expands the
`handler` call once per element, binding `{{ item }}` into each expansion. The
classic "run the same step over a list" map.

Airflow mapping:
- `with_items` literal fan-out -> dynamic task mapping with `.expand()`. Each mapped
  task instance corresponds to one dagron expansion copy (`process.0`, `process.1`, ...).
- `{{ item }}` -> the mapped value passed positionally via `bash_command` expansion.

Caveat: mapped instances run independently/parallel, exactly like the dagron copies.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

ITEMS = ["alpha", "beta", "gamma"]

with DAG(
    dag_id="pattern_03_fanout_map",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "fanout"],
) as dag:
    # .partial() pins shared config; .expand() creates one task per list item,
    # the direct analogue of dagron's with_items expansion.
    process = BashOperator.partial(
        task_id="process",
        # Mapped task templates {{ item }} of the bash_command list.
    ).expand(bash_command=[f"echo handling shard {item}" for item in ITEMS])
