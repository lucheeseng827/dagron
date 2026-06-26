"""Save a reusable workflow, attach a cron schedule, trigger it on demand.

Shows the "first-class workflow" surface (vs. the ad-hoc run in 01): a workflow
is a saved definition you can re-run, schedule, and sync to Git.

    python 02_workflow_and_schedule.py

Cleans up after itself (deletes the schedule + workflow) so it is safe to re-run.
"""
from __future__ import annotations

from _config import connect
from dagron import Dag, DagronError


def main() -> None:
    api = connect()

    dag = Dag("sdk-nightly-rollup")
    prep = dag.task("prepare", command=["echo", "prepare partitions"])
    dag.task("rollup", command=["echo", "daily rollup"], depends_on=[prep])

    # Save it as a reusable workflow (409 if the name already exists).
    created_workflow = False
    created_schedule = False
    wf_id = None
    sched_id = None

    try:
        wf = api.create_workflow(dag, description="SDK example — nightly rollup")
        created_workflow = True
    except DagronError as e:
        if e.status == 409:
            # Already saved from a prior run — find and reuse it.
            wf = next(w for w in api.list_workflows() if w.get("name") == dag.name)
            print("reusing existing workflow:", wf.get("id"))
        else:
            raise
    wf_id = wf.get("id") or wf.get("workflow_id")
    print("workflow:", wf_id)

    try:
        # Attach a cron schedule (6-field: sec min hour dom mon dow — midnight daily).
        sched = api.create_schedule(wf_id, "0 0 0 * * *")
        created_schedule = True
        sched_id = sched.get("id") or sched.get("schedule_id")
        print("scheduled:", sched_id, "->", sched.get("cron_expr"))

        # Trigger it now (don't wait for the cron) and follow the run.
        triggered = api.run_workflow(wf_id)
        print("triggered run:", triggered["run_id"])
        run = api.wait_for_run(triggered["run_id"], timeout=120)
        print("run status:", run["status"])
    finally:
        if created_schedule and sched_id:
            api.delete_schedule(sched_id)
        if created_workflow and wf_id:
            api.delete_workflow(wf_id)
        if created_schedule or created_workflow:
            print("cleaned up schedule + workflow")


if __name__ == "__main__":
    main()
