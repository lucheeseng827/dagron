"""Pattern 08 - Conditional execution (when).

dagron version (08_conditional_branch.yaml): `when:` on a task skips it (and any
sub-DAG it would expand to) when false. `run_tests` gates `test`; `deploy_prod`
gates the `prod-deploy` sub-DAG. Skipped tasks contribute no nodes.

Airflow mapping:
- `when:` guard -> `ShortCircuitOperator` driven by a DAG `param`. When the callable
  returns False, the operator short-circuits and all DOWNSTREAM tasks are skipped -
  the analogue of a dagron-skipped task plus its sub-DAG.
- The optional `prod-deploy` template -> a TaskGroup gated behind its own gate task.

Caveat: ShortCircuitOperator skips the *downstream* subtree, matching dagron's "a
skipped call drops its whole sub-DAG". Defaults mirror dagron: run_tests=true,
deploy_prod=false.
"""

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup


def _gate(param_name):
    def _fn(**context):
        val = context["params"][param_name]
        # Params may arrive as bool or as the string "true"/"false".
        return str(val).strip().lower() == "true"

    return _fn


def prod_deploy_group() -> TaskGroup:
    """Mirrors the `prod-deploy` template (approve -> ship)."""
    with TaskGroup(group_id="prod_deploy") as tg:
        approve = BashOperator(task_id="approve", bash_command="echo approve")
        ship = BashOperator(task_id="ship", bash_command="echo ship to prod")
        approve >> ship
    return tg


with DAG(
    dag_id="pattern_08_conditional_branch",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "run_tests": Param("true", type="string"),
        "deploy_prod": Param("false", type="string"),
    },
    tags=["dagron-pattern", "conditional"],
) as dag:
    build = BashOperator(task_id="build", bash_command="echo build")

    # when: {{ run_tests }} -> short-circuit gate before `test`.
    test_gate = ShortCircuitOperator(
        task_id="test_gate", python_callable=_gate("run_tests")
    )
    test = BashOperator(task_id="test", bash_command="echo test")

    stage = BashOperator(task_id="stage", bash_command="echo deploy staging")

    # when: {{ deploy_prod }} -> short-circuit gate before the prod sub-DAG.
    prod_gate = ShortCircuitOperator(
        task_id="prod_gate", python_callable=_gate("deploy_prod")
    )
    prod = prod_deploy_group()

    build >> test_gate >> test
    build >> stage >> prod_gate >> prod
