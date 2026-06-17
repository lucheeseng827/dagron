"""Pattern 12 - Reusable retry/resilience wrapper.

dagron version (12_retry_wrapper.yaml): the `resilient-step` template bundles a
resilience policy (max_attempts=5, retry_delay_secs=2 BASE with exponential backoff,
timeout_secs=30) so every caller (`payment`, `notify`) inherits the same hardened
behaviour without repeating it.

Airflow mapping:
- The wrapper template -> a factory `resilient_step(...)` that stamps the same task
  args onto every BashOperator it builds: `retries`, `retry_delay`, and
  `execution_timeout`.
- dagron's exponential backoff from the base delay -> `retry_exponential_backoff=True`
  with `retry_delay` as the base (Airflow computes delay = base * 2^(attempt-1)),
  matching "2s -> 2s, 4s, 8s, ...".
- max_attempts=5 -> `retries=4` (Airflow counts retries AFTER the first attempt, so
  total attempts = retries + 1 = 5).

Caveat: the policy lives in one factory; change it once and every caller updates,
exactly like the dagron template.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# One place to define the resilience policy (mirrors the template's fields).
MAX_ATTEMPTS = 5
RETRY_BASE = timedelta(seconds=2)
TIMEOUT = timedelta(seconds=30)


def resilient_step(task_id: str, label: str, cmd: str) -> BashOperator:
    """Build a BashOperator with the shared retry/backoff/timeout policy applied."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"echo {label}: {cmd}",
        retries=MAX_ATTEMPTS - 1,  # retries are AFTER the first attempt
        retry_delay=RETRY_BASE,  # base delay for exponential backoff
        retry_exponential_backoff=True,  # 2s -> 4s -> 8s ...
        execution_timeout=TIMEOUT,
    )


with DAG(
    dag_id="pattern_12_retry_wrapper",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dagron-pattern", "retry", "resilience"],
) as dag:
    payment = resilient_step("payment", label="payment", cmd="call payment API")
    notify = resilient_step("notify", label="email", cmd="send receipt")

    payment >> notify
