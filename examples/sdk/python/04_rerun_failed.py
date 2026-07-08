"""Recover a failed run: submit a DAG that fails mid-graph, then cascade-rerun.

`rerun_run` keeps succeeded tasks and re-runs the failure frontier (the failed
task plus everything it blocked) — the fix for a *transient* failure where the
same task succeeds on a later attempt.

To make that observable deterministically on the default engine we simulate a
transient fault with a marker file on the engine host:

* `reset`     removes the marker; it succeeds and is NOT re-run on the rerun,
              so the marker is absent when `transform` runs for the first time.
* `transform` fails the first time (marker absent → touch it + exit 1) and
              succeeds on the rerun (marker is present, left by the first attempt).

    python 04_rerun_failed.py

> Note: passing a `params=` override to `rerun_run` (a fix-forward rerun that
> mutates task input) is gated behind the `enterprise` build feature; the
> default engine rejects it with 400. This example uses a plain cascade rerun,
> which every build supports.
"""
from __future__ import annotations

import uuid

from _config import connect
from dagron import Dag

MARKER = f"/tmp/dagron-rerun-demo.{uuid.uuid4().hex}.marker"


def main() -> None:
    api = connect()

    dag = Dag("sdk-rerun-demo")
    reset = dag.task("reset", command=["sh", "-c", f"rm -f {MARKER}; echo reset"])
    dag.task(
        "transform",
        command=[
            "sh",
            "-c",
            f'if [ -f {MARKER} ]; then echo recovered; '
            f'else touch {MARKER}; echo "transient boom" >&2; exit 1; fi',
        ],
        depends_on=[reset],
        max_attempts=1,  # fail immediately so the rerun, not a retry, recovers it
    )

    run_id = api.submit_run(dag)
    print("submitted run:", run_id)
    run = api.wait_for_run(run_id, timeout=120)
    print("first attempt status:", run["status"])  # expect: failed
    for t in run["tasks"]:
        print(f"  - {t['name']:10s} {t['status']}")

    if run["status"] != "failed":
        print("(expected a failure to demo rerun; nothing to recover)")
        return

    # Cascade-rerun from the failure frontier (no params -> supported by every build).
    result = api.rerun_run(run_id)
    print("rerun reset tasks:", result.get("rerun"))
    recovered = api.wait_for_run(run_id, timeout=120)
    print("after rerun status:", recovered["status"])  # expect: succeeded
    for t in recovered["tasks"]:
        print(f"  - {t['name']:10s} {t['status']}")


if __name__ == "__main__":
    main()
