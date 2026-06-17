"""Pattern 06 - Recursion with a base case (countdown).

dagron version (06_recursion_countdown.yaml): the `countdown` template calls ITSELF
with `{{ n - 1 }}` and a `when: {{ n }} > 0` base case, so it terminates at n=0.
n=3 -> tick(3) -> tick(2) -> tick(1) -> tick(0).

Airflow mapping:
- Recursion -> a recursive Python BUILDER that creates nested TaskGroups at PARSE
  time. The base case (`n <= 0`) terminates the recursion, just like the `when` guard.
- `{{ n - 1 }}` decrement -> ordinary `n - 1` in the recursive call.

CAVEAT (important): Airflow cannot self-trigger a DAG recursively at run time, so the
faithful analogue of dagron runtime recursion is PARSE-TIME recursion: the whole
countdown chain is materialized into nested groups when the DAG file is parsed. The
`start` value is therefore fixed in the file (a Param can't change graph shape at run
time the way dagron's expansion does).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

START = 3  # dagron default `start: "3"`; changes the parsed depth.


def countdown(dag: DAG, n: int):
    """Recursive parse-time builder; base case n <= 0 stops the recursion."""
    with TaskGroup(group_id=f"countdown_{n}") as tg:
        tick = BashOperator(task_id="tick", bash_command=f"echo T-minus {n}", dag=dag)
        if n > 0:  # base case: stop at 0 (mirrors when: {{ n }} > 0)
            recurse = countdown(dag, n - 1)  # decrement each level
            tick >> recurse
    return tg


with DAG(
    dag_id="pattern_06_recursion_countdown",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "recursion", "parse-time"],
) as dag:
    countdown(dag, START)
