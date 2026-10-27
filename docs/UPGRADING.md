# Upgrading dagron

How to move an installation to a new release, and what each release asks of you
before you do. The general procedure is short; the per-release sections are the
part to read every time.

- [The procedure](#the-procedure) — back up, one engine first, then the rest
- [Upgrading to 0.11.1](#upgrading-to-0111) — from 0.11.0
- [Upgrading to 0.11.0](#upgrading-to-0110) — from 0.10.0
- [Upgrading to 0.10.0](#upgrading-to-0100) — from 0.9.x
- [Upgrading from 0.8.x](#upgrading-from-08x) — the frontend image is gone
- [Rolling back](#rolling-back)

The detail behind every step — what the state is, how migrations work, and what
to do when one fails — is in [`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md).

## The procedure

dagron's only state is its database. Migrations are embedded in the engine and
applied when it starts; they are **forward-only**, so the backup you take first
is the only way back.

1. **Back up.**

   ```sh
   # Postgres
   pg_dump -U dagron -d workflow -Fc -f "workflow-$(date -u +%Y%m%dT%H%M%SZ).dump"
   # SQLite — safe while the daemon runs (WAL mode)
   sqlite3 workflow.db ".backup 'workflow-backup.db'"
   ```

   Never copy a running SQLite `workflow.db` on its own: the `-wal` and `-shm`
   files beside it hold committed data.
2. **Read the section for the version you are moving to**, below. Some releases
   need a setting or a permission in place *before* the new engine starts.
3. **Stop ingestion** — pause schedules, stop whatever submits runs — and let
   running work drain. Work that is still running when an engine stops is not
   lost: its lease expires (30 s by default) and the next engine re-dispatches
   it, so tasks must be safe to run twice.
4. **Upgrade one engine first.** It applies the migrations. Check it exited
   cleanly and that `GET /healthz` on its ops API answers before you touch the
   rest. Two engines starting at once against an unmigrated database both try to
   apply the same migrations; one wins and the other may exit.
5. **Roll the remaining engines, then `dagron-api`, then the GitOps worker**, and
   resume ingestion.

### Compose

The quickstart pins every image to `DAGRON_VERSION`:

```sh
DAGRON_VERSION=0.10.0 docker compose -f compose.quickstart.yaml pull
DAGRON_VERSION=0.10.0 docker compose -f compose.quickstart.yaml up -d
```

(`podman compose` works the same.) A checkout of a newer `compose.quickstart.yaml`
already defaults to its own release.

### Helm

The chart pins matching image tags for every component, but one `helm upgrade`
rolls the engine, `dagron-api` and the GitOps worker Deployments together, and
nothing in the chart waits for the engine's migrations before the others start.
So stage it, the same way as the procedure above. Read the release's section
first: 0.10.0 changes the chart's defaults.

```sh
# 1. The new chart with ONE engine on the new image, everything else still on
#    the old one. Use your current versions for the two old tags.
helm upgrade dagron oci://registry-1.docker.io/mancube/dagron --version 0.10.0   --reuse-values   --set engine.replicas=1   --set dagronApi.image=mancube/dagron-api:0.9.2   --set gitops.image=mancube/dagron-gitops:0.9.2
# 2. Wait for it. The engine applies the migrations before it starts its API,
#    so /healthz answering means they are done. The chart sets no readiness
#    probe, so a finished rollout alone does not. (The Deployment is
#    dagron-engine unless you set fullnameOverride.)
kubectl rollout status deployment/dagron-engine
kubectl port-forward deployment/dagron-engine 8080:8080 &
curl -fsS http://127.0.0.1:8080/healthz && kill %1
# 3. Everything on the new release, at your usual engine count.
helm upgrade dagron oci://registry-1.docker.io/mancube/dagron --version 0.10.0   --reuse-values   --set engine.replicas=<your count>   --set dagronApi.image=mancube/dagron-api:0.10.0   --set gitops.image=mancube/dagron-gitops:0.10.0
```

The chart's default is one engine; if you run one, step 1's `engine.replicas` is
already right. Drop the `gitops.image` lines if the worker is not enabled.

### Binaries

Stop the engine, replace the binary, start it. Same order as above when you run
more than one.

## Upgrading to 0.11.1

From 0.11.0. **No migrations**: the schema of 0.11.1 is 0.11.0's, on SQLite and
Postgres alike, and dagron-ci's schema is unchanged too. Engines, dagron-api and
dagron-ci of the two releases run side by side while you roll them, and rolling
back is pulling the 0.11.0 images again. The procedure is the usual one:

```sh
DAGRON_VERSION=0.11.1 docker compose -f compose.quickstart.yaml pull
DAGRON_VERSION=0.11.1 docker compose -f compose.quickstart.yaml up -d
# or, with dagron-ci: the same two lines against compose.ci.yaml
helm upgrade dagron oci://registry-1.docker.io/mancube/dagron --version 0.11.1 --reuse-values
```

New in 0.11.1 and off until you use it, so nothing to do unless you adopt it:
`dagron-step-sql` on Snowflake, BigQuery and Databricks (`SQL_ENGINE=snowflake`,
`bigquery`, `databricks`), on DuckDB (`SQL_ENGINE=duckdb`), `SQL_MODE=script`,
and `dagron-step-sql parity`; dagron-state's compiled warehouse moves; and
two release binaries, `dagron-ci` and armv7 `dagron`.

### Before you start

| If you… | What changed | Do this first |
|---|---|---|
| run dagron-ci with `CI_FLAKE_ISSUES` set | A job that failed and then passed when a **person reran** the run (`dagron-ci rerun`, a check's Re-run, the console) now counts as a flake, as one the engine's own retry passed always did. A job that passed and was rerun whole, or was cancelled and rerun, does not. So a team that reruns red jobs by hand will see flaky-job issues it did not before | Nothing, if that is what you want: it is the change. If the count now files too many, raise `CI_FLAKE_ISSUES` |
| run dagron-ci against github.com | A GitHub App token request that GitHub answers with a 5xx, or a connection it does not take, is now sent again up to three times inside the mint's 8 s, instead of failing the job at its checkout. A refusal, a rate limit and a timeout are not sent again | Nothing |

## Upgrading to 0.11.0

From 0.10.0. The full list of changes is the [changelog](../CHANGELOG.md); this
is what an upgrade has to act on.

New in 0.11.0 and off until you configure it, so nothing to do unless you adopt
it: dagron-ci (a service of its own, with its own schema; `ci.*` in the chart),
task workspaces (`workspace:`), `DAGRON_CI_EXEC_IMAGE`, the `DAGRON_CHECKOUT_*`
settings, the Docker executor's `DAGRON_DOCKER_MOUNTS`, `DAGRON_DOCKER_WORKDIR`
and `DAGRON_DOCKER_NETWORK`, a task's `image_pull_policy`, a task with no
network or a named allowlist on Kubernetes (`DAGRON_TASK_NETWORK_POLICY`,
`DAGRON_TASK_NETWORK_ALLOWLISTS`; `engine.taskNetworkPolicy` in the chart) or
on Docker, the log store (`DAGRON_LOG_STORE`, and its sweep,
`DAGRON_LOG_SWEEP_SECS`), path caches and workflow artifacts in an object
store (`DAGRON_CACHE_URL`), GitHub's cache service
(`DAGRON_ACTIONS_CACHE_LISTEN`, `DAGRON_ACTIONS_CACHE_URL`), a task's identity
(`DAGRON_OIDC_ISSUER` on dagron-api, `DAGRON_OIDC_REQUEST_URL` on the engine),
`artifact_api: true` (`DAGRON_ARTIFACT_API_URL`), signed provenance
(`DAGRON_PROVENANCE_KEY`), CDEvents (`DAGRON_CDEVENTS_URL`), the image cache
(`DAGRON_REGISTRY_MIRRORS`, `DAGRON_REGISTRY_AUTH_FILE`,
`DAGRON_IMAGE_PULL_SECRETS`), privileged tasks (`DAGRON_TASK_PRIVILEGED`), a
task's own Docker daemon (`DAGRON_DOCKER_DAEMON`), Docker actions built from a
Dockerfile (`DAGRON_DOCKER_BUILD_ACTIONS`), runs that are not trusted
(`DAGRON_UNTRUSTED_ISOLATION_FLOOR`), `DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS`,
`DAGRON_TASK_FS_GROUP` and `DAGRON_TASK_RUN_AS_NON_ROOT`, a task's `services:`
and an `environment:` of its own, the remote runner (`dagron-runner`), agent
tokens, backups (`dagron backup`, the chart's `backup.enabled`), and dagron-api
on SQLite.

**Upgrade the engines before dagron-api, or with it**, as [the
procedure](#the-procedure) has it for every release. It matters more than usual
here. dagron-api of this release writes a record with every run it makes, into
a table the engine's migrations create, and refuses submits until an engine of
this release has started. Its cancel, rerun, retry and clear are the engine's
now, and read columns those migrations add.

### Before you start

| If you… | What changed | Do this first |
|---|---|---|
| run tasks on Kubernetes in a namespace that has a `ResourceQuota` | A pod the apiserver refuses because the namespace is at its quota (`exceeded quota: tenant-quota, …`) is no longer a failed attempt. The task goes back to `ready`, keeps its attempt, and is dispatched again after about 5 s, backing off to about a minute between tries, until a pod is admitted. So `attempt` no longer counts those refusals, and a task that used to fail once `max_attempts` dispatches had been refused now waits, for up to `DAGRON_QUOTA_WAIT_MAX_SECS` (default 6 h), before it fails. `gang:` members under `RUNNER_GANGS` never wait: a refusal still fails the member and cancels its siblings. Wherever a full-quota refusal is still a failure (those members, or `DAGRON_QUOTA_WAIT_MAX_SECS=0`), its text is now `not started: pod refused by namespace ResourceQuota 'tenant-quota' (requested: …, used: …, limited: …)` instead of `create pod '…': … exceeded quota: …`. And a refused dispatch no longer records a sample in `scheduler_task_duration_seconds`, whether or not the task waits | Nothing, if waiting is what you want. If something relies on a refusal failing the task quickly, such as an alert on failed runs or a `one_failed` handler, set `DAGRON_QUOTA_WAIT_MAX_SECS` on every engine (`engine.extraEnv` in the chart) to the longest wait you accept, or to `0` to make a refusal a failed attempt again. Update anything that matches the old failure text, such as a log alert on `exceeded quota` in a task's output: with `0` or for a gang member it has to match the new text, and a task that waits has no failure to match until it passes the bound. `run_timeout_secs` still ends a run whose tasks are waiting. See [`OPERATIONS.md`](OPERATIONS.md#troubleshooting-symptom-first) |
| consume `run.completed` outbox events, the run sink, OpenLineage, `notify` webhooks or Slack messages, or `notify.git` statuses | A cancelled run, and one failed by `run_timeout_secs`, used to change status and report nothing. Both now end fully, once their work has stopped (the row after next): a `run.completed` event (`cancelled`, or `failed` past the deadline), the run sink, OpenLineage `ABORT` for a cancel, the webhooks and Slack, and a commit status (GitHub `error`, GitLab `canceled`) | Make sure consumers accept `cancelled`, and that an alert on failed runs is meant to fire for runs past their deadline too |
| use `notify.git` | A run with a `notify.git` block now posts `pending` ("dagron run in progress") from the moment it exists, through any engine holding a forge token, then its result. Its templates resolve against the parameters the run was created with, so `sha: "{{ commit_sha }}"` picks up a caller's SHA instead of coming out empty | Nothing. A required check now shows pending while its run is in flight |
| cancel runs, and act on what a run reads right afterwards | A cancel no longer reads `cancelled` at once. A task that had not started, or was waiting (an approval, a sensor, a deferred job), is cancelled at once, as before. A task a worker holds is asked to stop and stays `running`, with `stop_requested_at` set, until its container, pod or process is gone. Its run stays `running`, with `stopping_as` saying what it will end as, and ends with the last such task: `run.completed`, the commit status and the run sink follow the work. The same holds for a run past `run_timeout_secs` and for a gang sibling's cancel. `POST /api/runs/{id}/cancel` answers `{cancelled, stopping}`, and the engine's own `POST /runs/{id}/cancel` answers `stopping` too. A run that is stopping holds its `max_active_runs` key, and a rerun or a retry of it is refused until it has ended. A task that was asked to stop takes no other result: one that finishes by itself first is cancelled | Anything that cancels a run and then acts at once on `cancelled` (starts the next run, releases a lock, reads the final status) has to wait for the run to end: poll the run, or take `run.completed`. Nothing if you read the status later |
| run tasks on Docker or Kubernetes | Cancelled work is stopped. A cancelled task's container or pod used to run on to the end of its command; so did the work of a run past its deadline. It is now stopped with the task's grace (`CI_STEP_GRACE_SECS`, 10 s by default) and removed: a container with its volumes, a pod with its env Secret. The Secret's removal is best effort. The delete is tried once, and the pod owns the Secret so that it goes with the pod where the delete fails; a Secret is left only where neither held. An attempt's result reads "stopped and removed" only once the daemon or the apiserver has said the container or the pod is gone, and "what this attempt started may still be running" where it has not. A task's next attempt starts only once what an earlier one left is gone: until then the dispatch waits as a quota wait does and spends no attempt, and is counted and shown with the quota waiters, on Docker too. With `DAGRON_QUOTA_WAIT_MAX_SECS=0`, or for a gang member, that attempt fails instead, reading "an earlier attempt of this task may still be running". Workloads carry a new label, `dagron.dev/fence` | Make sure nothing counts on a cancelled task finishing its command. A task that has to clean up does it when it is told to stop, within the grace. If you set `DAGRON_QUOTA_WAIT_MAX_SECS=0` (the first row), it now fails these dispatches too |
| have a task that writes a great deal | One attempt's log is capped, and so is its output: `DAGRON_LOG_TASK_CAP_BYTES`, 64 MiB by default, for each. A log that reaches it is held there while the task runs, on a line that says so, and once the attempt is over its last `DAGRON_LOG_TAIL_BYTES` (a quarter of the cap by default) are added under a line that says how much was left out: a capped log is up to 1.25 times the cap by default, and twice it at most. A log of a task with secrets also stops once what the task wrote, counted before masking, is past the cap by 64 KiB (or by its longest secret). An output over the cap keeps its end, under a first line that says how much is gone. There was no cap before. The log is also written once a second, not once a line, and a long one waits a second more for each whole MiB its append would rewrite, up to 30 s: a tail of a running task is about a second behind it, and up to that second is lost with the engine, or to a task cancelled before it is written | If a task writes more than 64 MiB in one attempt and something reads all of it, set `DAGRON_LOG_TASK_CAP_BYTES` higher on every engine. `DAGRON_LOG_TAIL_BYTES=0` keeps a capped log's beginning alone |
| read coloured logs through `docker logs` or `kubectl logs` | With `LOG_ANSI` unset, a line is coloured only when stdout is a terminal and `NO_COLOR` is not set. It used to be coloured everywhere, into files, pipes and container logs. This is every binary's log | Set `LOG_ANSI=1` to keep the colour. A parser that stripped the codes has nothing to strip |
| read a run's `trigger`, filter runs by it, or use `not_triggerer` | Every run is made with a trigger record. `trigger` on `GET /api/runs/{id}` is no longer `null` on a run a person submitted. A run that read `manual` only because nothing knew better reads `schedule`, `dataset`, `workflow`, `source` or `api`. And `not_triggerer` compares an approver with the record's `actor`, where it read the run's submitter: a person who starts a workflow can no longer approve such a gate in a workflow it calls, and a backfill a person asks for through `POST /api/schedules/{id}/backfill` is theirs. Runs from before the upgrade are judged as they were | Check anything that filters on `trigger_kind` or `?trigger=manual`, and that the people who approve `not_triggerer` gates are still not the ones who start the run |
| read a run's `status` to tell whether it has started, or run engines of two versions on one datastore | A workflow may now say that a run over its `max_active_runs` waits (`when_full: queue`, or `supersede`), where every such run was refused. Nothing changes for a workflow that does not say so. A run that waits reads `running`, with `queued_at` set and no `admitted_at`, and all of its tasks `pending`: the console and the engine's own page show it as queued. Two columns and a partial index are added to `workflow_runs` (migrations 082 and 083 on Postgres, 060 on SQLite) | Nothing, unless you adopt `when_full`. Then upgrade every engine before a workflow uses it: an older engine lets no waiting run in, and fails one that waits past its `run_timeout_secs` as if it had been running. A reader that must know whether a run has started reads `queued_at` and `admitted_at` |
| rerun, retry or clear tasks through dagron-api | The two APIs now share one implementation. A rerun also resets tasks a rule skipped, with the failed and the cancelled, and `rerun` in its answer counts them. All three clear a reset task's log, its fault verdict and its deferred job's handle. What is replaced is kept in `task_attempts`, and `attempt` in the log and attempts routes counts through the rounds; `attempt` on a run's task rows is unchanged. With `max_active_runs`, a rerun, a retry or a clear that would set an ended run running is refused with `429` while the key is full. A retry sets a cancelled run running again. Each writes an outbox event, `run.rerun` | A client that reads `attempt` from the task log or attempts routes sees larger numbers after a rerun. One that reruns under `max_active_runs` has to take a `429` and try again. Outbox consumers see a new event |
| run a published engine image | The images are built with `otel` now. Each task is given a W3C trace context, a `TRACEPARENT` in its environment, whether or not a collector is named, and the engine logs a line as it does. A task that names its own `TRACEPARENT` keeps it. An `OTEL_EXPORTER_OTLP_ENDPOINT` already in the engine's or dagron-api's environment now turns span delivery on | Nothing, unless a task treats an unexpected `TRACEPARENT` as an error, or that variable is set where spans are not wanted |
| run the engine on Docker (`EXECUTOR=docker`) | Four things. A daemon whose default log driver keeps nothing (`none`, `passthrough`) stops the engine, where every task's log and output were silently empty; other drivers draw a warning at startup that says what they lose. A task's image is pulled when the host does not have it, where the task failed with the daemon's "No such image". A task's stderr lines are no longer logged by the engine as warnings. And `isolation:` is enforced where it was refused, with `DAGRON_TASK_ISOLATION_FLOOR=hardened` taken where it was a startup error | Where the daemon's default driver keeps nothing, set `DAGRON_DOCKER_LOG_DRIVER` (`json-file` keeps each container's log in a file). Update anything that matched the engine's warnings for a task's stderr |
| use "Re-run unchanged", or `POST /api/runs/{id}/resubmit` | A resubmit carries the parameters the run was created with. It used the workflow's defaults | Nothing, if the same parameters are what you meant. A run made before 0.11.0 has none stored and resubmits as before |
| call `dagron --version` from a script | It prints one line (`dagron 0.11.0`) and exits. It used to start an engine, and dagron-api ignored it | Nothing, unless something started an engine that way |
| alert on an engine's `config_fingerprint` drifting across a fleet | Every engine's fingerprint changes with this release | Expect one change as each engine is upgraded |
| run more than one engine on Postgres | The maintenance sweeps (lease recovery, run deadlines and soft deadlines, approval timeouts, the runs that wait for their cap, the time, HTTP and dataset sensors, sub-workflows, fan-outs, dataset triggers and the slow reap of running runs) are run by one engine a window, through a slot in the new table `sweep_slots`, where every engine ran each of them. With three engines the average wait before a shared sweep picks up a due row grows from about a quarter of its interval to about half, and an engine that stalls holding a slot delays that sweep by up to nearly two intervals. Each sweep runs at an interval of its own (`SWEEP_INTERVALS`; every default is the cadence it had), and `SWEEP_INTERVAL_MS` and `REAP_INTERVAL_MS` above a day are read as a day. A due `wait.url` sensor is claimed before its GET, so it is polled once per due time, not once an engine | Nothing, if that latency is acceptable. If you set `SWEEP_INTERVALS`, set it the same on every engine: the engine that takes a slot sets the cadence for the window. `SWEEP_SHARE=off` has every engine run every sweep again. See [`HA.md`](HA.md) |
| grant the engine's and dagron-api's database roles table by table | Two new tables are written by more than the migrations: `sweep_slots`, which the engine takes its sweeps in, and `log_sweeps`, which a trigger writes as a task row is deleted, in whichever process deletes it (only for a task whose log is in `DAGRON_LOG_STORE`) | Grant the engine `SELECT`, `INSERT` and `UPDATE` on `sweep_slots`, and `SELECT`, `INSERT`, `UPDATE` and `DELETE` on `log_sweeps`. Grant dagron-api, whose archive route deletes runs, `SELECT`, `INSERT` and `UPDATE` on `log_sweeps` |
| reach Postgres through a pooler in transaction mode, or start several engines at once | This upgrade builds fourteen indexes concurrently ([Migrations](#migrations)). On a direct connection the first engine to take the migration lock migrates and the others wait without holding a snapshot. The lock is a session's, so through a pooler in transaction mode it keeps no two engines apart, and two that build one index at once deadlock, leaving it INVALID | Run the upgrade with one engine, or give the engines the direct endpoint, until the migrations are done. Keep engines and dagron-api of the old release from starting meanwhile ([below](#when-engines-of-two-versions-run-together)) |
| scrape the engine's `/metrics`, or read dagron-api's `GET /api/metrics` | The datastore gauges are cached. What has not finished (runs `pending` and `running`, tasks still to end, the queue, the dead letters, the waits) for `METRICS_CACHE_SECS` (10 s), and each engine caches its own. The history (`scheduler_runs` and `scheduler_tasks` of the finished statuses, and the runs of the last 24 hours that have finished) for `METRICS_HISTORY_CACHE_SECS` (300 s), served stale while it is read again behind the scrape, so it can be older than that: `scheduler_metrics_history_age_seconds` says how old. A read that does not return is given up, and a scrape that waited for it answers `500`. dagron-api's `GET /api/metrics` is cached likewise (`DAGRON_METRICS_CACHE_SECS`, 2 s; `DAGRON_METRICS_HISTORY_CACHE_SECS`, 300 s), and gains `history_age_secs` | If an alert or a test reads a finished count right after a run ends, set `METRICS_HISTORY_CACHE_SECS=0` (and `DAGRON_METRICS_HISTORY_CACHE_SECS=0`), or `METRICS_CACHE_SECS=0` to read the datastore on every scrape, as before |
| run tasks on Kubernetes with a Role for the engine of your own | The engine keeps one watch of its task pods, where each task asked the API server for its pod twice a second. The watch needs `list` and `watch` on pods; the chart's Role has them. Where the watch is not following the namespace, a task asks as before, and the engine says so once in its log | Grant them, or set `DAGRON_TASK_POD_WATCH=off` for the executor as it was |
| run tasks on Docker or Kubernetes, and read their logs | A task's log is followed as the task writes it. On Docker it no longer depends on the container's log driver (the journal's rate limit, `local`'s 100 MB, rotation); what is read back from the driver once a container has stopped still does, and the engine's warning of such a driver begins `what is read back of a container may not be whole`. On Kubernetes a task has a log, from when its container starts, with why its pod failed, kept from its beginning up to `DAGRON_LOG_TASK_CAP_BYTES`: its log view showed its output's end before | Nothing, unless something reads a Kubernetes task's last lines from its log view: its output keeps them |
| send OTLP from a published image (`OTEL_EXPORTER_OTLP_ENDPOINT`) | The endpoint that is sent spans is now sent log records and metrics too: each event the log filter lets through, with its trace and span, and the engine's metrics, what `GET /metrics` serves, every `OTEL_METRIC_EXPORT_INTERVAL` (60 s). dagron-api sends logs and spans. A binary whose name begins `dagron-` is `service.name` by that name (`dagron-step-sql`), where it was `dagron-dagron-…` | Set `OTEL_LOGS_EXPORTER=none` or `OTEL_METRICS_EXPORTER=none` where that signal is not wanted, or name it an endpoint of its own (`OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT`). Update a query on a `dagron-dagron-…` service |
| implement a `RunSink`, or consume OpenLineage, `notify.git`, `notify.webhook` or `notify.slack` | A run's end-of-run work is done from its `run.completed` event, by a worker of its own for each, off the reconcile loop. Each is retried with backoff (eight tries), where it was tried once; an end superseded before it was done (a cancelled run rerun) is not done; and one older than a day when its part first reaches it is let go. A `RunSink` is called at least once for each end, and may be called twice for one after a crash. The OpenLineage post times out after 15 s, where it had no timeout. A `notify.webhook` post is a CloudEvent in binary mode: its body is the JSON it was, with `ce-*` headers beside it (`DAGRON_EVENTS_SOURCE` names the installation) | Make a `RunSink` take one end twice without harm |
| read `event_outbox` yourself | Each consumer of the outbox is a sink with its own delivery state of each event (`outbox_sinks`, `outbox_deliveries`), and `event_outbox.status` is no longer written. A sink is handed the events written after it was first configured; the webhook also those the outbox still held pending when the upgrade ran. While a sink asks for them, as dagron-ci's reporter does, a trigger writes `task.started`, `task.waiting` and `task.finished` events too | Read delivery state from `outbox_deliveries`, by sink, and expect the task events where dagron-ci runs |
| approve `not_triggerer` gates of runs that are rerun, retried or cleared through dagron-api | The person who put a task back is recorded on it (`task_runs.reset_by`), and dagron-api refuses that person's approval of a gate that asks again (`403`), as it refuses the run's triggerer's; `GET /api/approvals` says so. A run set running again also counts its `run_timeout_secs` deadline and its `deadline` alert from then, where a rerun of a run older than its deadline was failed before any task ran; an alert that fired may fire again | Make sure someone other than whoever reruns a run can approve its gates |
| have a workflow whose `when:` can leave out every task | Such a run had no task and stayed `running`. The reaper, SQLite's and Postgres's, now ends it as succeeded, with its `run.completed` event | Expect those runs to end, and to be reported as any ended run is |
| run the engine on SQLite, with a large file | Migrations 076 to 081 build and rebuild indexes of `task_runs` and `workflow_runs` at start-up, and add triggers to both ([Migrations](#migrations)). A tick looks only at the runs a trigger named, with a slice of the running runs each `REAP_INTERVAL_MS` as the net; `REAP_INTERVAL_MS=0` walks every slice each tick, which is dearer than before on a small file | Start the engine before dagron-api. Run no `ANALYZE` on the file ([`ONE_NODE.md`](ONE_NODE.md)) |
| install the chart on OpenShift | `global.openshift` is `auto`: where the cluster serves `security.openshift.io/v1`, no pod names its own uid or fsGroup, and each is left to the SCC, every pod `runAsNonRoot` under `RuntimeDefault`; the engine gives its task pods the same (`DAGRON_TASK_FS_GROUP`, `DAGRON_TASK_RUN_AS_NON_ROOT`). Off OpenShift every pod renders as it did | Nothing, unless a pod relied on its image's user there: then set `global.openshift=false`. See [`OPENSHIFT.md`](OPENSHIFT.md) |

### What stops a service at start

One of these can stop an engine that ran under 0.10.0: the Docker log driver,
which is in the table above too. The rest are checks on settings that are new
in this release. They matter once you adopt one, and to anyone who ran a build
made before the release, where some of them were taken with a warning.

| Service | It does not start when | What it takes |
|---|---|---|
| engine, on Docker | the daemon's default log driver keeps nothing (`none`, `passthrough`), so that every Docker action's step output would be empty, and so would what a container that failed before its task started said | `DAGRON_DOCKER_LOG_DRIVER=json-file`, or a daemon default that keeps a log. On Podman the driver is not known at start, and the engine says so in its log at the first container instead |
| engine, on Docker | `DAGRON_DOCKER_NETWORK` names a network the daemon does not have, or `host` | A network that exists: `docker network create` it first |
| engine | `DAGRON_CHECKOUT_TOKEN_URL` is `http://` to another host. The engine's secret goes there, and each checkout's credential comes back | `https://`, or `http://` to the engine's own machine (a sidecar, or dagron-ci in the engine's pod) |
| engine, on Kubernetes | `DAGRON_TASK_NETWORK_POLICY=enforced`, and the policy `dagron-task-no-network` is missing, selects other pods or allows anything; or a list named in `DAGRON_TASK_NETWORK_ALLOWLISTS` has no policy that is what the engine takes it for | The chart makes them: `engine.taskNetworkPolicy`, `engine.taskNetworkAllowlists` |
| engine and `dagron-runner`, on Docker | a list named in `DAGRON_TASK_NETWORK_ALLOWLISTS` has no network `dagron-task-allow-<name>`, or its network has a way out or gives the host an address | Make each network `--internal`, with `-o com.docker.network.bridge.inhibit_ipv4=true` |
| engine | `SWEEP_INTERVALS` gives a sweep an interval longer than a day | A day at most for each |
| engine | `DAGRON_OIDC_REQUEST_URL` or `DAGRON_ARTIFACT_API_URL` is plain `http://`, or carries a user. A task sends a credential of its own there, from its own container | `https://`, or, on a closed network where that is accepted, `DAGRON_OIDC_REQUEST_INSECURE=allow` or `DAGRON_ARTIFACT_API_INSECURE=allow` (the engine warns at start) |
| engine | `DAGRON_ACTIONS_CACHE_LISTEN` is set without `DAGRON_ACTIONS_CACHE_URL`, or the other way round; or the URL has a path, carries a user, or is plain `http://` | Both, and an `https://` URL at its host's root, or `DAGRON_ACTIONS_CACHE_INSECURE=allow` |
| engine | `DAGRON_CACHE_URL` names a store that cannot sign URLs | An `s3://`, `gs://` or `az://` store |
| engine | `DAGRON_TASK_PRIVILEGED=allow`, and the engine claims no runner classes of its own (`RUNNER_CLASSES`, without `default`), runs work that is not trusted, or has a floor that drops capabilities, forbids new privileges or names a runtime class or seccomp's default profile; on Kubernetes also without `DAGRON_TASK_NODE_SELECTOR`, or with a namespace whose Pod Security refuses a privileged pod | A dedicated pool for privileged tasks alone |
| engine | `DAGRON_UNTRUSTED_ISOLATION_FLOOR` is set with `EXECUTOR=local`, on Docker without `DAGRON_DOCKER_NETWORK`, or on Kubernetes with a floor that does not say what the pod may reach | Those settings, or no floor: then each task of a run that is not trusted fails with the reason |
| engine, on Kubernetes | `DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS` is malformed, or names one of the engine's own label keys | Labels of the cloud's webhook alone |
| dagron-ci | a cell in `CI_CELLS` is neither `https://` nor `http://` to its own machine, and `CI_CELLS_INSECURE` is not `allow`. Its token for dagron-api goes there | Name what terminates TLS before dagron-api, or, on a closed network where that is accepted, set `CI_CELLS_INSECURE=allow` |
| dagron-ci | `CI_GITHUB_APP_TOKEN_URL` or `CI_URL_FOR_JOBS` is `http://` to another host | `https://`, or `CI_GITHUB_APP_TOKEN_URL_INSECURE=allow` or `CI_URL_FOR_JOBS_INSECURE=allow` on a closed network |
| dagron-ci | `CI_CHECKOUT_TOKEN_SECRET_FILE` is set and `CI_CELLS` names more than one cell: an engine of one cell could ask for the credential of another's run | `CI_CHECKOUT_TOKEN_SECRETS_DIR`, with a secret for each cell |

The chart refuses the same at render, so that a pod does not restart for ever
on it: a cell dagron-ci would not start on, one checkout secret for several
cells, a dagron-api token with no cell named (`ci.apiTokenSecret` needs
`ci.cells`, and the chart makes no cell up), and a fleet (`ci.fleet.labels`)
with no cell and token, no App, no `ci.webhookSecret`, or a label map
dagron-ci would refuse.

### Migrations

The engine applies the migrations at startup.

| Backend | From 0.10.0 |
|---|---|
| SQLite | 051 – 081 |
| Postgres | 066 – 107 |

They add:

- columns on `task_runs`: quota waits, a stop that was asked for, the
  attempts of a task's earlier rounds, who put it back to run (`reset_by`),
  the remote runner's session that holds it (`session_id`), and its log's
  writings (`log_gen`, `log_fence`, `log_writings`; on Postgres `log_seq` as
  well);
- columns on `workflow_runs`: the run's parameters, the stamps for an end and
  a start still to report, what a stopping run will end as, when a run began
  to wait for its cap and when it was let in, and how many times it was rerun
  whole; on SQLite `triggered_by` as well;
- two columns on `task_attempts` (a replaced attempt's status, and when it was
  queued), one on `api_tokens` (`agent`), and on SQLite three on `workflows`
  (`description`, `managed_by`, `managed_md5`);
- new tables: `run_triggers` (what started each run), `environment_policies`,
  `artifacts`, `workflow_artifacts`, `path_caches`, `oidc_requests`,
  `artifact_api_tokens`, `agent_names`, `outbox_sinks`, `outbox_deliveries`,
  `outbox_epoch`, `runner_sessions`, `runner_results`, `log_segments` and
  `log_sweeps`; on Postgres `runners`, `runner_join_tokens` and `sweep_slots`
  too; on SQLite dagron-api's `users` and `git_repos`, `run_wakes` and
  `run_changes` too;
- partial indexes for the sweeps and claims, and the new tables' own indexes;
- triggers, below.

Each new column is nullable or has a constant default, so none rewrites its
table. What does more than add:

- **Triggers.** On both backends a change of a task's status writes
  `task.started`, `task.waiting` or `task.finished` to the outbox, but only
  while a sink asks for them (dagron-ci's reporter); where none does, it
  reads `outbox_sinks` and writes nothing. The deletion of a task row whose
  log is in the log store writes a record to `log_sweeps`, so with a store a
  large purge takes longer: deleting 200,000 task rows on Postgres 16 took
  21.7 s with no trigger, 22.0 s with it and no row marked, and 30.5 s with
  every row marked. On Postgres a task that ends, is made ended, or is taken
  away not ended notifies its run (107): a cancel of 10,000 tasks costs
  18 ms more. On SQLite, triggers on `task_runs` and `workflow_runs` record
  each run a change may have ended (`run_wakes`, 079) and each run a change
  touched, for dagron-api's live stream (`run_changes`, 081, at most 66,560
  rows); 081's cost a cancel of 10,000 tasks 8 to 17 ms.
- **Rows written.** Postgres 088 (SQLite 065) writes `outbox_epoch`'s one row,
  when the migration ran: the webhook is handed the events the outbox still
  held pending then. 098 (SQLite 073) marks `log_writings` on the rows with
  closed log segments, of which an upgrade from 0.10.0 has none:
  `log_segments` is new in it. 106 (SQLite 077) sets every `wait.url` sensor
  parked with no next poll due now.
- **Index builds over large tables.** On SQLite each is built in its
  migration's transaction, at start-up: 076 builds seven indexes of
  `task_runs`; 078 and 079 drop and rebuild six indexes of `workflow_runs` and
  build two, about one scan of the table each (0.55 s together at a million
  runs); 080 builds one (71 ms at a million runs). On a large file, start the
  engine before dagron-api.

On Postgres fourteen indexes are built `CONCURRENTLY`, so that a build blocks
no run's creation or claim: 069, 071, 074, 075, 080, 081, 083, 089, 092 and
101 – 105. Each is alone in a migration of its own, a single statement marked
`-- no-transaction`, as a concurrent build cannot run in a transaction. 089
indexes `event_outbox`, which is append-only and can be large, and 101 – 105
each scan `task_runs` twice and wait out older transactions. On a direct
connection the first engine to take the migration lock migrates while the
others wait for it; through a pooler in transaction mode, upgrade with one
engine ([Before you start](#before-you-start)). A build that is cut short
leaves an INVALID index, which the next start takes for built (`IF NOT
EXISTS`). The engine looks for one as it starts and every 5 minutes
(`scheduler_db_invalid_indexes`), and logs at error level the `REINDEX INDEX
CONCURRENTLY` that rebuilds it; nothing runs that for you.

dagron-ci keeps migrations of its own, in its own schema
(`CI_DATABASE_SCHEMA`), and applies them when it starts. It is new in this
release, so there is nothing of it to upgrade.

### When engines of two versions run together

Mixed versions during a rolling upgrade are safe. A 0.10.0 engine reads a task
waiting on quota as an ordinary `ready` task and dispatches it when it is due.
If that pod is refused too, the 0.10.0 engine fails the attempt the old way. So
until every engine is upgraded, a refusal waits or costs an attempt depending on
which engine dispatched the task.

Likewise for a run's start and end: a run cancelled or timed out by 0.10.0
code reports nothing, as before, and a run created by 0.10.0 code posts no
`pending`. What 0.11.0 code stamps, any 0.11.0 engine reports. Upgrade
dagron-api with the engines, since its cancel now goes through the engine's.

A stop is the engine's too. A 0.10.0 engine does not look for a request to
stop: a task it holds runs on as it did, and it ends no run that is stopping.
A 0.11.0 engine ends the run such an engine leaves. And dagron-api of this
release refuses submits until a 0.11.0 engine has applied its migrations.

A 0.10.0 engine takes no sweep slot and runs every sweep itself, which is
safe: every shared sweep may run on two engines at once. The triggers the
migrations add fire whichever version writes, so on Postgres a task a 0.10.0
engine ends notifies its run too.

Keep 0.10.0 engines and dagron-api from starting while a 0.11.0 engine builds
its indexes (an old pod restarted during the upgrade). A 0.10.0 engine waits
for the migration lock inside a statement, which holds a snapshot that
`CREATE INDEX CONCURRENTLY` waits out, and a 0.10.0 dagron-api adds
`workflow_runs.environment` at every start with an `ALTER TABLE` that takes
the table's lock even where the column is there. Either can deadlock a build:
Postgres fails one of the two, and where it fails the build, its index is left
INVALID and the next engine records the migration as applied. The engine's
check finds such an index ([Migrations](#migrations)).

### Rolling back from 0.11.0

As for any release, rolling back means restoring the backup and starting 0.10.0
against it ([below](#rolling-back)). A 0.10.0 engine started against the
migrated database instead tolerates the new columns, but reports nothing for
an end or a start a 0.11.0 engine stamped and did not yet report, and leaves a
run that a 0.11.0 engine was stopping as `running`: cancel such a run again.

The migrations' tables, indexes and triggers stay, and the triggers go on
firing under 0.10.0. On SQLite `run_wakes` is written as tasks end, and
nothing empties it then. Where `DAGRON_LOG_STORE` was set, a task whose log
went to the store shows only what its row holds (the open part, or notes),
and a task row deleted with a log in the store is still recorded in
`log_sweeps`, for a 0.11.0 engine to sweep. A 0.10.0 engine or dagron-api
started while a 0.11.0 engine builds an index can deadlock the build, as
[above](#when-engines-of-two-versions-run-together).

## Upgrading to 0.10.0

From 0.9.0 – 0.9.3. The full list of changes is the
[changelog](../CHANGELOG.md); this is what an upgrade has to act on.

### Before you start

Work through this list before the first 0.10.0 engine starts. Each row says who
it affects; skip the rows that do not apply to you.

| If you… | What changed | Do this first |
|---|---|---|
| set `service_account:` on any task (IRSA, workload identity) | A task may run only as a ServiceAccount on an allow-list, and the list is **empty by default**, so those tasks are refused | Set `DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS` to the task identities you use. Never list the engine's own ServiceAccount. Tasks without `service_account:` are unaffected |
| run tasks on Kubernetes with environment secrets and wrote the engine's Role yourself | Secrets now reach task pods by reference, through a per-task Secret, not as a literal in the pod spec. The engine needs `secrets` `create`/`patch`/`delete`, and a task fails, naming the permission, if it lacks them | Add those verbs to your Role. The chart's Role already has them. `DAGRON_TASK_SECRET_ENV=inline` restores the old behaviour |
| install with the Helm chart and call the engine's API over HTTP from anything other than dagron's own pods | `networkPolicy.enabled` is now **on by default**: the engine accepts traffic only from `networkPolicy.engine.allowFrom` and the `monitoring` namespace, and Postgres only from the release's own pods | Add those callers to `networkPolicy.engine.allowFrom`, or set `networkPolicy.enabled: false`. The policy only takes effect on a CNI that enforces NetworkPolicy. See [`HARDENING.md`](HARDENING.md) |
| have users who delete workflows, or connect, disconnect or re-key Git repositories, without being in the `admin` group | All of these now require `admin`, and answer `403` otherwise | Put those users in `admin`, or have them retire a workflow (`POST /api/workflows/{id}/state` with `retired`) rather than delete it. An instance with no admin can seed one from `DAGRON_ADMIN_EMAIL` + `DAGRON_ADMIN_PASSWORD` |
| have users in the `viewer` group on the open build | `viewer` is now read-only in every build: every mutation answers `403`. It used to be enforced only in the enterprise build | Move anyone who needs to write out of `viewer` |
| edit Git-synced workflows in the console or through the API | A workflow synced from a repository now answers `409` to `PUT` and `DELETE`, naming the repository; the edit used to be accepted and then silently reverted at the next sync | Change those workflows in the repository. An `admin` can still pass `?force=true` |
| let an agent approve gates through `dagron-mcp` | `dagron_approve_task` is now hidden and refused unless `DAGRON_MCP_ALLOW_APPROVE=1`. Rejecting is unaffected | Set the variable on the MCP server if an agent should approve |
| pin MCP tool definitions in a gateway that hashes annotations | Every dagron tool now declares annotations, so each definition changes once | Re-approve the tool definitions once |
| use `repeat:` on a `template:` call | It is now refused at validation; it used to be dropped silently | Move `repeat:` onto the template's tasks |

### What the engine does at startup

It applies these migrations, all additive:

| Backend | From 0.9.0 – 0.9.2 | From 0.9.3 |
|---|---|---|
| SQLite | 042 – 050 | 047 – 050 |
| Postgres | 053 – 065 | 061 – 065 |

Three of the Postgres ones (054, 058 and 065) build an index with
`CREATE INDEX CONCURRENTLY`, each in a migration of its own, so writers are not
blocked on a large table. A build that is interrupted can leave an index marked
`INVALID`, which a re-run skips because it already exists: drop that index and
start the engine again to rebuild it.

### When engines of two versions run together

Mixed versions during a rolling upgrade are safe for everything that existed in
0.9, with one exception: **`defer:`**. A 0.9 engine does not know the field,
ignores it, and marks a deferred task succeeded while its remote job is still
running. Finish rolling every engine to 0.10.0 before you register any workflow
that uses `defer:`.

### After upgrading

Nothing below is required; it is what 0.10.0 makes available.

- **Remote jobs that hold no worker** — `defer:`, with the `dagron-step-spark`
  and `dagron-step-sql` binaries: [`EXTERNAL_JOBS.md`](EXTERNAL_JOBS.md).
- **Fan-in over several datasets** — `on_datasets: [a, b]` with
  `datasets_mode: all`, now open: [`DATASETS.md`](DATASETS.md).
- **OpenLineage events with inputs and outputs**, when `OPENLINEAGE_URL` is set:
  [`DATASETS.md`](DATASETS.md).
- **New metrics** — per-workflow run time and outcome, `process_*`, signpost
  hits — and eight Grafana dashboards: [`METRICS.md`](METRICS.md) and
  [`examples/monitoring/`](../examples/monitoring/). The `last_run` queries take
  the newest engine's value, so they hold with several engines.
- **`dagron_wait_run` progress** — MCP clients that send a `progressToken` get
  `notifications/progress` while a wait blocks: [`MCP.md`](MCP.md).

### Rolling back from 0.10.0

As for any release, rolling back means restoring the backup and starting 0.9.x
against it ([below](#rolling-back)). One thing is specific to 0.10.0: a task
parked on a remote job (`defer:`) is a row only a 0.10.0 engine knows how to
poll. Starting a 0.9 engine against a database that holds parked tasks, rather
than against the backup, leaves them parked forever, with their remote jobs still
running. Cancel those runs before you go back, or restore the backup as the
procedure says.

## Upgrading from 0.8.x

`mancube/dagron-frontend` is discontinued: **0.8.1 is its last tag**. The console
it served now comes from `dagron-api` itself, on the same port as the API. Drop
the frontend container, and the `frontend.enabled` Helm value. Then follow
[Upgrading to 0.10.0](#upgrading-to-0100), which applies to you too.

## Rolling back

Migrations are forward-only; there are no down migrations. Rolling back is:
restore the pre-upgrade backup, then start the previous release against it.
Everything written after the backup is discarded with it.

If the new engine fails at startup, only the migration that failed is rolled
back. sqlx runs each migration in its own transaction, so the ones before it in
the same startup stay applied, and a concurrent index build runs outside a
transaction altogether (see [above](#what-the-engine-does-at-startup)). The
previous release usually still starts on that schema, because migrations only
add, but that is a way to keep running while you investigate, not a rollback.
To actually go back, restore the pre-upgrade backup.

Step-by-step recovery for the failure cases — a migration that errors partway,
"previously applied but has been modified", a startup that names an unknown
migration — is in [`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md) §4 and §6.
