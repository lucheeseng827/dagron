"""Pattern 10 - Matrix / nested fan-out (2-D map).

dagron version (10_matrix_nested_fanout.yaml): an outer fan-out over `regions` calls
`per-region`, which ITSELF fans out over `services`, producing the region x service
cross-product. regions=[us,eu] x services=[api,web] -> 4 cells.

Airflow mapping:
- A 2-D matrix fan-out -> a single dynamic mapping over the precomputed cross-product
  (`itertools.product`). Mapping over the product of the two lists is the idiomatic
  Airflow way to express nested fan-out; each mapped instance is one (region, svc) cell.

Caveat: Airflow does not nest `.expand()` inside `.expand()`; flattening to the
cartesian product yields the identical set of leaf cells dagron's nested fan-out builds.
"""

from datetime import datetime
from itertools import product

from airflow import DAG
from airflow.operators.bash import BashOperator

REGIONS = ["us", "eu"]
SERVICES = ["api", "web"]

with DAG(
    dag_id="pattern_10_matrix_nested_fanout",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "matrix", "nested-fanout"],
) as dag:
    # Cartesian product = the region x service matrix; one mapped task per cell.
    matrix = BashOperator.partial(task_id="deploy").expand(
        bash_command=[
            f"echo deploy {svc} in {region}"
            for region, svc in product(REGIONS, SERVICES)
        ]
    )
