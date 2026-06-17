"""Pattern 07 - Divide & conquer (binary recursion / tree).

dagron version (07_recursion_divide_conquer.yaml): the `solve` template calls itself
TWICE (left + right, each with `{{ size / 2 }}`) guarded by `when: {{ size }} > 1`,
doing leaf `work` at every level. size=4 -> 2+2 -> 1+1 leaves.

Airflow mapping:
- Binary recursion -> a recursive parse-time builder emitting nested TaskGroups, two
  child groups (`left`, `right`) per level until the base case `size <= 1`.
- `work` at every level -> a BashOperator in each group, with both halves wired after it.

CAVEAT: like pattern 06, this is PARSE-TIME recursion (Airflow can't recurse at run
time). The tree shape is fixed when the file is parsed; `SIZE` controls the depth.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

SIZE = 4  # dagron default `size: "4"`.


def solve(dag: DAG, size: int, label: str):
    """Recursive parse-time builder; base case size <= 1 (mirrors when: size > 1)."""
    with TaskGroup(group_id=f"solve_{label}") as tg:
        work = BashOperator(
            task_id="work",
            bash_command=f"echo solve {label} size {size}",
            dag=dag,
        )
        if size > 1:  # base case terminator
            half = size // 2
            left = solve(dag, half, f"{label}L")
            right = solve(dag, half, f"{label}R")
            work >> [left, right]  # both halves run in parallel
    return tg


with DAG(
    dag_id="pattern_07_recursion_divide_conquer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "recursion", "divide-conquer", "parse-time"],
) as dag:
    solve(dag, SIZE, "n")
