# dagron Python SDK

Author dagron workflows in Python and drive the whole dagron control plane —
trigger runs, manage workflows and schedules, hold environments and secrets,
redrive dead letters, wire up GitOps — without writing REST calls by hand.

- **Zero dependencies.** Standard library only (`json` + `urllib`).
- **Two layers:** `Dag` (a validating spec builder) and `Client` (a typed wrapper
  over the authenticated `dagron-api` gateway).
- **Full coverage.** `0.9.0` covers the whole `dagron-api` HTTP surface and the
  engine's whole `TaskSpec` — see [`ROADMAP.md`](ROADMAP.md) for the endpoint
  matrix and what's next. The SDK's version tracks the API version it speaks to,
  so `0.9.x` means "covers the 0.9 gateway".

## Install

```bash
pip install dagron-sdk    # or: pip install -e .   (from sdks/python)
```

## Author a DAG

```python
from dagron import Dag

dag = Dag("etl", parameters={"day": "today"}, tags=["nightly"], result_from="load")
extract = dag.task("extract", image="alpine", command=["echo", "{{ day }}"])
dag.task("load", image="alpine", command=["true"], depends_on=[extract])

print(dag.to_json())    # valid dagron input (YAML is a JSON superset)
```

`task()` maps onto the engine's full `TaskSpec`: the basics (`image`, `command`,
`depends_on`, `input`, `env`, `resources`, `service_account`), the retry policy
(`max_attempts`, `retry_delay_secs`, `retry_max_delay_secs`, `retry_on_timeout`,
`retry_budgets`, `timeout_secs`), flow control (`when`, `trigger_rule`, `hook`,
`allow_failure`), fan-out (`with_items`, `with_param`, `instance_key`),
scheduling (`runner_class`, `pool`, `priority`, `gang`) and the rest
(`cache`, `repeat`, `produces`, `isolation`). `Dag(...)` takes the spec-level
block: `parameters`, `tags`, `environment`, `task_defaults`, `run_timeout_secs`,
`max_active_runs`, `result_from`, `budget`, `deadline`, `notify`, `on_datasets`.

Beyond leaf tasks, a task can be a **call** into a template, a **chain** into
another saved workflow, or one of the command-less kinds:

```python
dag = Dag("release")
build = dag.template("build", parameters={"target": "release"})
build.task("compile", command=["make", "{{ target }}"])

dag.task("run-build", template="build", arguments={"target": "debug"})
dag.approval("sign-off", depends_on=["run-build"], timeout_secs=3600)
dag.sensor("settle", duration="5m", depends_on=["sign-off"])
dag.trigger("publish", "publish-artifacts", depends_on=["settle"])
```

`to_spec()`/`to_json()` validate the graph client-side — unique names, known
deps, one kind per task, valid trigger rules and runner classes, resolvable
template calls, a `result_from` that names a real task, acyclicity — mirroring
the server so a bad DAG fails fast instead of costing a 400 round-trip.

## Drive the control plane

```python
import os
from dagron import Client

api = Client("http://localhost:8080")
api.login("admin@example.com", os.environ["DAGRON_PASSWORD"])   # stores the token

# For automation, mint a token once — this response is the only one that ever
# carries it — and put it in the CI job's environment, not a password.
token = api.create_token("ci", expires_in_days=90)["token"]
# Then, in that job: Client.from_env() reads DAGRON_API_URL + DAGRON_TOKEN.
ci = Client("http://localhost:8080", token=token)

# Trigger an ad-hoc run and block on it server-side.
run_id = api.submit_run(dag, parameters={"day": "2026-01-01"}, idempotency_key="etl-2026-01-01")
result = api.wait_run(run_id, timeout_secs=600)
print(result["status"], result["result"])      # succeeded | failed | cancelled
if result["failure"]:
    print(result["failure"]["message"])         # why, without a second call

# Save it as a reusable workflow and schedule it nightly in a real timezone.
wf = api.create_workflow(dag, description="nightly ETL")
api.create_schedule(wf["id"], "0 0 2 * * *", timezone="Europe/Berlin", catchup=True)

# Observe.
for ev in api.stream_events():                 # every run, one connection
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

**Auth & identity** — `login` · `logout` · `me` · `from_env` · `create_user`
· `list_users` · `list_tokens` · `create_token` · `revoke_token`

**Runs** — `submit_run` · `list_runs` · `iter_runs` · `get_run` · `get_run_spec`
· `get_run_graph` · `get_run_logs` · `get_task_logs` · `cancel_run` · `rerun_run`
· `resubmit_run` · `retry_task` · `clear_task` · `approve_task` · `reject_task`
· `list_approvals` · `stream_run` · `stream_events` · `wait_run` · `wait_for_run`
· `set_triage` · `clear_triage` · `archive_run` · `list_archived_runs`
· `get_archived_run`

**Workflows** — `list_workflows` · `get_workflow` · `create_workflow`
· `update_workflow` · `delete_workflow` · `run_workflow` · `list_workflow_runs`
· `list_workflow_versions` · `set_workflow_state` · `apply_bundle`
· `workflow_badge` · `sync_workflow_to_git`

**Schedules & backfills** — `list_schedules` · `create_schedule`
· `update_schedule` · `delete_schedule` · `backfill_schedule` · `create_backfill`
· `list_backfills` · `get_backfill` · `cancel_backfill`

**Environments & settings** — `list_environments` · `create_environment`
· `update_environment` · `delete_environment` · `set_environment_secret`
· `delete_environment_secret` · `get_notification_settings`
· `set_notification_settings` · `test_notifications` · `get_dead_letter_settings`
· `set_dead_letter_settings`

**Data & artifacts** — `list_datasets` · `list_dataset_events` · `put_artifact`
· `get_artifact` · `artifact_exists` · `sync_artifacts`

**Dead letters & GitOps** — `list_dead_letters` · `redrive_dead_letter`
· `discard_dead_letter` · `list_git_repos` · `connect_git_repo`
· `set_git_repo_auth` · `clear_git_repo_auth` · `sync_git_repo`
· `disconnect_git_repo`

**Observability** — `metrics` · `metrics_timeseries` · `search` · `health`
· `healthz` · `readyz`

## Test

```bash
python -m unittest          # from sdks/python
```

The suite validates the builder and runs `Client` against a threaded in-process
fake gateway (real sockets), so request construction is exercised end-to-end.

## Release

`dagron-sdk` is published to PyPI, the Python Package Index. The version is the
single literal in `dagron.__version__`, which `pyproject.toml` reads statically
at build time — bump it there and nowhere else.
