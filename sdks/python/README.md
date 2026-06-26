# dagron Python SDK

Author dagron workflows in Python and drive the whole dagron control plane —
trigger runs, manage workflows and schedules, redrive dead letters, wire up
GitOps — without writing REST calls by hand.

- **Zero dependencies.** Standard library only (`json` + `urllib`).
- **Two layers:** `Dag` (a validating spec builder) and `Client` (a typed wrapper
  over the authenticated `dagron-api` gateway).
- **Full coverage.** `v0.2` covers 100% of the current `dagron-api` HTTP surface —
  see [`ROADMAP.md`](ROADMAP.md) for the coverage matrix and what's next.

## Install

```bash
pip install -e .        # from sdks/python
```

## Author a DAG

```python
from dagron import Dag

dag = Dag("etl")
extract = dag.task("extract", image="alpine", command=["echo", "hi"])
dag.task("load", image="alpine", command=["true"], depends_on=[extract])

print(dag.to_json())    # valid dagron input (YAML is a JSON superset)
```

`task()` maps onto the engine's full `TaskSpec` — `image`, `command`, `depends_on`,
plus `input`, `max_attempts`, `retry_delay_secs`, `timeout_secs`, `env`,
`resources`, `service_account`, and `workflow_ref` (chain another saved workflow).
`to_spec()`/`to_json()` validate the graph client-side (unique names, known deps,
leaf-xor-chain, acyclic), mirroring the server so a bad DAG fails fast.

## Drive the control plane

```python
import os
from dagron import Client

api = Client("http://localhost:8080")
api.login("admin@example.com", os.environ["DAGRON_PASSWORD"])   # stores the token
# ...or skip login: Client("http://localhost:8080", token=os.environ["DAGRON_TOKEN"])

# Trigger an ad-hoc run and wait for it to finish.
run_id = api.submit_run(dag)
run = api.wait_for_run(run_id)
print(run["status"])                       # succeeded | failed | cancelled

# Save it as a reusable workflow and schedule it nightly.
wf = api.create_workflow(dag, description="nightly ETL")
api.create_schedule(wf["id"], "0 0 0 * * *")

# Observe.
for ev in api.stream_run(run_id):          # live Server-Sent Events
    print(ev["event"], ev["data"])
```

Every method maps one `dagron-api` endpoint to one call and returns the server's
JSON. Non-2xx responses raise `DagronError(status, message)`:

```python
from dagron import DagronError

try:
    api.submit_run({"name": "x", "tasks": []})
except DagronError as e:
    print(e.status, e.message)             # e.g. 400 "DAG 'x' contains a cycle"
```

### What `Client` covers

`login` · `logout` · `me` · `create_user` · `submit_run` · `list_runs` · `get_run`
· `get_run_graph` · `get_task_logs` · `cancel_run` · `rerun_run` · `resubmit_run`
· `retry_task` · `stream_run` · `wait_for_run` · `list_workflows` · `get_workflow`
· `create_workflow` · `update_workflow` · `delete_workflow` · `run_workflow`
· `sync_workflow_to_git` · `list_schedules` · `create_schedule` · `update_schedule`
· `delete_schedule` · `backfill_schedule` · `list_dead_letters`
· `redrive_dead_letter` · `discard_dead_letter` · `list_git_repos`
· `connect_git_repo` · `sync_git_repo` · `disconnect_git_repo` · `metrics` · `healthz`

## Test

```bash
python -m unittest          # from sdks/python
```

The suite validates the builder and runs `Client` against a threaded in-process
fake gateway (real sockets), so request construction is exercised end-to-end.

## Release

Publishing `dagron-sdk` to PyPI (the `pip` repository) is documented step-by-step
in [`RELEASING.md`](RELEASING.md) — version bump, `python -m build`, a TestPyPI
dry-run, the `twine upload`, tagging, and the move to OIDC Trusted Publishing.
