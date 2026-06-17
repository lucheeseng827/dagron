"""Pattern 11 - Diamond of sub-DAGs.

dagron version (11_diamond_of_subdags.yaml): split -> branch-a, branch-b -> join,
where each branch is itself a multi-task sub-workflow (clean-branch: fetch->clean;
score-branch: fetch->score). `join` fans in over both branches' exits.

Airflow mapping:
- Each branch sub-DAG -> a factory building a `TaskGroup`.
- Diamond wiring -> `split >> [branch_a, branch_b]` then `[branch_a, branch_b] >> join`.
  Airflow resolves group-to-task edges to the group's leaf/root tasks, reproducing the
  boundary rewiring dagron does over multiple exit nodes.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def clean_branch() -> TaskGroup:
    """`clean-branch` template: fetch -> clean."""
    with TaskGroup(group_id="branch_a") as tg:
        fetch = BashOperator(task_id="fetch", bash_command="echo fetch-a")
        clean = BashOperator(task_id="clean", bash_command="echo clean")
        fetch >> clean
    return tg


def score_branch() -> TaskGroup:
    """`score-branch` template: fetch -> score."""
    with TaskGroup(group_id="branch_b") as tg:
        fetch = BashOperator(task_id="fetch", bash_command="echo fetch-b")
        score = BashOperator(task_id="score", bash_command="echo score")
        fetch >> score
    return tg


with DAG(
    dag_id="pattern_11_diamond_of_subdags",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "diamond", "sub-dag"],
) as dag:
    split = BashOperator(task_id="split", bash_command="echo split")
    branch_a = clean_branch()
    branch_b = score_branch()
    join = BashOperator(task_id="join", bash_command="echo join")

    split >> [branch_a, branch_b] >> join
