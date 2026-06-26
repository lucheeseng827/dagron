"""Follow a run live over Server-Sent Events.

dagron's per-run SSE stream emits a lightweight *nudge* on every task-state
change (the event payload is just `{"run_id": ...}`) plus a named `resync` event
if the client falls behind. It's a "something changed — refetch" signal, exactly
how the web UI drives its live run view: on each nudge it refetches the graph.

This example mirrors that: submit a fan-out DAG, and on every nudge refetch the
run and print the current task statuses, stopping once the run is terminal.

    python 03_stream_run.py
"""
from __future__ import annotations

from _config import connect
from dagron import Dag

TERMINAL = {"succeeded", "failed", "cancelled"}


def snapshot(api, run_id: str) -> str:
    """Refetch the run and return a one-line status summary; '' once terminal-printed."""
    run = api.get_run(run_id)
    tasks = " ".join(f"{t['name']}={t['status']}" for t in run["tasks"])
    return f"[{run['status']}] {tasks}"


def main() -> None:
    api = connect()

    # Diamond: root -> (a, b) -> join, with small sleeps so transitions are visible.
    dag = Dag("sdk-stream-demo")
    root = dag.task("root", command=["echo", "start"])
    a = dag.task("branch-a", command=["sh", "-c", "sleep 1; echo a"], depends_on=[root])
    b = dag.task("branch-b", command=["sh", "-c", "sleep 1; echo b"], depends_on=[root])
    dag.task("join", command=["echo", "joined"], depends_on=[a, b])

    run_id = api.submit_run(dag)
    print("streaming run:", run_id)

    # Each item is {"event": <name>, "data": <parsed JSON|str>}. A bounded idle
    # timeout keeps a quiet connection from hanging the example.
    try:
        for ev in api.stream_run(run_id, timeout=30):
            if ev["event"] == "resync":
                print("  (resync — client fell behind, refetching)")
            print("  nudge ->", snapshot(api, run_id))
            if api.get_run(run_id)["status"] in TERMINAL:
                break
    except TimeoutError:
        print("  (stream idle — falling back to a final poll)")

    print("final:", snapshot(api, run_id))


if __name__ == "__main__":
    main()
