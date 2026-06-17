"""Pattern 09 - Nested templates (composition / multi-level).

dagron version (09_nested_templates.yaml): the `pipeline` template composes the
reusable `stage` template three times (extract -> transform -> load); each `stage`
is prep -> exec. Templates calling templates.

Airflow mapping:
- Template-calls-template -> a factory (`pipeline_group`) that calls another factory
  (`stage_group`). Nesting TaskGroups gives the same compositional structure.
- Parameter threading (`phase`) -> a plain function argument passed down.

Caveat: nested TaskGroups produce nested group_ids (pipeline.stage_extract.prep, ...),
the parse-time analogue of dagron's nested template expansion.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def stage_group(phase: str) -> TaskGroup:
    """Reusable `stage` template: prep -> exec."""
    with TaskGroup(group_id=f"stage_{phase}") as tg:
        prep = BashOperator(task_id="prep", bash_command=f"echo prep {phase}")
        exec_ = BashOperator(task_id="exec", bash_command=f"echo run {phase}")
        prep >> exec_
    return tg


def pipeline_group() -> TaskGroup:
    """`pipeline` template composing three `stage` calls."""
    with TaskGroup(group_id="pipeline") as tg:
        extract = stage_group("extract")
        transform = stage_group("transform")
        load = stage_group("load")
        extract >> transform >> load
    return tg


with DAG(
    dag_id="pattern_09_nested_templates",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "composition", "nested"],
) as dag:
    pipeline_group()
