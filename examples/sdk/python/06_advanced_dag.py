"""New in 0.9: the Dag builder's full TaskSpec surface, in one run.

Before 0.9 the Python `Dag`/`task()` builder emitted 12 of the engine's ~35
task fields — no fan-out, no sensor, no approval gate, no sub-workflow
trigger could be authored from Python at all, and a spec that needed one had
to be hand-written as YAML. This example builds one of each:

    process-shard (fan-out) -> settle (sensor) -> sign-off (approval) -> chain-to-child (sub-workflow trigger)

    python 06_advanced_dag.py

Registers a tiny child workflow to chain to, resolves the approval gate
programmatically (a CI job would do this after its own check passes; a human
would do it from the console), and cleans the child workflow up afterward —
safe to re-run.
"""
from __future__ import annotations

import time

from _config import connect
from dagron import Dag, DagronError


def main() -> None:
    api = connect()

    # 1. A tiny child workflow for the parent DAG to chain to. `type: workflow`
    #    resolves its target by *registered name*, so this has to exist first.
    child = Dag("sdk-advanced-child")
    child.task("child-step", command=["echo", "child ran"])
    created_child = False
    try:
        wf = api.create_workflow(
            child, name="sdk-advanced-child", description="SDK example — chained child"
        )
        created_child = True
    except DagronError as e:
        if e.status != 409:
            raise
        wf = next(w for w in api.list_workflows() if w.get("name") == "sdk-advanced-child")
        print("reusing existing child workflow:", wf.get("id"))
    child_id = wf.get("id") or wf.get("workflow_id")

    try:
        # 2. The parent: fan-out -> sensor -> approval gate -> sub-workflow trigger.
        dag = Dag("sdk-advanced-dag", result_from="chain-to-child")

        # Fan-out: one task instance per item, named from `instance_key`. A
        # downstream task that `depends_on` the *base* name ("process-shard")
        # fans back in over every expanded copy, the same as YAML's
        # examples/templates/04_scatter_gather.yaml.
        fanout = dag.task(
            "process-shard",
            command=["sh", "-c", "echo processing shard {{ item }}"],
            with_items=["a", "b", "c"],
            instance_key="{{ item }}",
        )

        # A sensor holds no worker slot while it waits — unlike a task that
        # polls in a loop, this costs nothing until its condition is met.
        settle = dag.sensor("settle", duration="5s", depends_on=[fanout])

        # A human approval gate. Absent a decision within `timeout_secs` it
        # resolves as `on_timeout` ("reject" by default) — a gate fails safe.
        gate = dag.approval("sign-off", depends_on=[settle], timeout_secs=120)

        # Sub-workflow trigger: submits the registered child by name and parks
        # until it is terminal, succeeding or failing with it.
        dag.trigger("chain-to-child", "sdk-advanced-child", depends_on=[gate])

        print("spec:", dag.to_json())
        run_id = api.submit_run(dag)
        print("submitted run:", run_id)

        # 3. Wait for the gate to actually park, then approve it. A real CI job
        #    would call `approve_task`/`reject_task` from its own pass/fail
        #    check instead of polling blind like this.
        gate_task = None
        for _ in range(30):
            run = api.get_run(run_id)
            candidate = next((t for t in run["tasks"] if t["name"] == "sign-off"), None)
            if candidate and candidate["status"] == "awaiting_approval":
                gate_task = candidate
                break
            time.sleep(1)
        if gate_task is None:
            raise RuntimeError("'sign-off' never reached awaiting_approval within 30s")
        api.approve_task(run_id, gate_task["id"])
        print("approved:", gate_task["id"])

        # 4. Block for the rest — the sub-workflow trigger included. One
        #    request, no interval to tune; a wait that times out comes back
        #    `finished: false` rather than raising.
        result = api.wait_run(run_id, timeout_secs=60)
        print("run status:", result["status"], "result:", result.get("result"))
        if result.get("failure"):
            print("failure:", result["failure"]["message"])
    finally:
        if created_child and child_id:
            api.delete_workflow(child_id)
            print("cleaned up child workflow")

    # For reference — not submitted. `pool`/`gang` route to engine replicas or
    # co-scheduled groups that must already exist on your deployment, so this
    # prints the spec rather than running it. See docs/CONFIG.md for
    # RUNNER_CLASSES/pools and the gang-scheduling section.
    reference = Dag("sdk-scheduling-reference")
    reference.task(
        "distributed-step",
        command=["echo", "co-scheduled"],
        pool="gpu-pool",
        priority=10,
        gang=4,
        cache={"key": "{{ params.day }}", "ttl_secs": 3600},
        produces=["dataset://warehouse/daily_rollup"],
    )
    print("\nscheduling fields reference (not submitted):")
    print(reference.to_json())


if __name__ == "__main__":
    main()
