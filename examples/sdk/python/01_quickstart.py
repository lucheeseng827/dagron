"""Quickstart: build a DAG in code, submit it, wait for it, read a task's logs.

Run against the local stack (compose up) with no arguments:

    python 01_quickstart.py

The DAG is a 3-step ETL — extract -> transform -> load — using plain `echo`
commands so it runs on the local executor without any container images.
"""
from __future__ import annotations

from _config import connect
from dagron import Dag


def main() -> None:
    # 1. Author the DAG. `task()` returns the task name, so you can pass it
    #    straight into a downstream task's `depends_on`.
    dag = Dag("sdk-quickstart")
    extract = dag.task("extract", command=["echo", "extracted 1000 rows"])
    transform = dag.task(
        "transform",
        command=["echo", "transformed rows"],
        depends_on=[extract],
    )
    dag.task("load", command=["echo", "loaded to warehouse"], depends_on=[transform])

    # `to_json()` validates the graph client-side (unique names, known deps,
    # leaf-xor-chain, acyclic) — a bad DAG raises here, before any network call.
    print("spec:", dag.to_json())

    # 2. Connect (login or DAGRON_TOKEN) and submit an ad-hoc run.
    api = connect()
    run_id = api.submit_run(dag)
    print("submitted run:", run_id)

    # 3. Block until the run reaches a terminal state.
    run = api.wait_for_run(run_id, timeout=120)
    print("run status:", run["status"])
    for t in run["tasks"]:
        print(f"  - {t['name']:10s} {t['status']:10s} {(t.get('output') or '').strip()!r}")

    # 4. Pull one task's captured output through the gateway.
    first = run["tasks"][0]
    logs = api.get_task_logs(run_id, first["id"])
    print("logs[", first["name"], "]:", logs.get("output", "").strip())


if __name__ == "__main__":
    main()
