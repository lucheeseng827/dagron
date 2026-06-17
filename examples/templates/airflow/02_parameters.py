"""Pattern 02 - Parameterized template (dynamic arguments).

dagron version (02_parameters.yaml): the `deploy` template declares parameters
(`region`, `bucket`) with defaults; two call tasks override them via `arguments`,
whose values are resolved in the CALLER's scope ({{ env }}) before filling the
template's isolated scope ({{ region }}, {{ bucket }}).

Airflow mapping:
- Template parameters -> plain Python function arguments of the factory.
- Caller-scope value `{{ env }}` -> a DAG-level `param`, threaded into the factory
  call. The factory emits a `{{ params.env }}` Jinja reference in the bash command,
  so the value is resolved at run time from the DAG params (overridable per run).
- Each `call` task -> one invocation of the `deploy_group(...)` factory.

Caveat: dagron template scope is *isolated*; in Airflow we emulate that by passing
only the needed values into the factory (the factory sees nothing else).
"""

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def deploy_group(name: str, region: str, bucket_suffix: str) -> TaskGroup:
    """Mirrors the `deploy` template; `bucket` is built from caller-scope `env`."""
    with TaskGroup(group_id=name) as tg:
        # {{ params.env }} is resolved in the caller (DAG) scope at run time.
        BashOperator(
            task_id="push",
            bash_command=(
                f"echo deploy to {region}/{{{{ params.env }}}}-{bucket_suffix}"
            ),
        )
    return tg


with DAG(
    dag_id="pattern_02_parameters",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"env": Param("prod", type="string")},
    tags=["dagron-pattern", "parameters"],
) as dag:
    east = deploy_group("deploy_east", region="us-east-1", bucket_suffix="east")
    west = deploy_group("deploy_west", region="us-west-2", bucket_suffix="west")

    east >> west
