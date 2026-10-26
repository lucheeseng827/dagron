# Operations runbook

Deploying, upgrading, backing up and debugging a dagron installation. Knobs
live in [`CONFIG.md`](CONFIG.md); endpoints in [`API.md`](API.md); internals in
[`ARCHITECTURE.md`](ARCHITECTURE.md). Backup, upgrade and disaster-recovery
**procedures** — including recovering from a lost database or a migration that
fails mid-upgrade — are in [`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md).

## Deploy

**Full UI stack (local/dev):** `podman compose up --build` (validated with
podman; docker compose works the same) from the module root brings up
`postgres` → `engine` (Postgres+ops build, ops API on 8787, cluster-internal)
→ `dagron-api` (`127.0.0.1:8080`), which serves the console at `/` and the API
under `/api` on that same port. Sign in at
`http://localhost:8080` with the seeded admin (`admin@local` /
`dagron-admin`). Before any non-local deploy, change `DAGRON_JWT_SECRET`
(≥ 32 chars), the admin password, and leave `DAGRON_COOKIE_SECURE` at its
default (`true`). `compose.docker-executor.yaml` is a dev-only override that
runs tasks in containers via the podman socket.

**Single binary:** the default build (`sqlite,ops`) needs no other
infrastructure — `dagron dev` for a resident server with Swagger on
`127.0.0.1:8787/docs`, or `dagron <file.yaml>` for a one-shot run that exits
when the run drains. Container images build from the root `Dockerfile`
(`runtime` target = distroless, `localdev` = debian-slim **with** a shell —
example DAGs that call `echo` only work in `localdev`).

**What makes the process a server vs a one-shot run:** any of `API_ADDR`
(valid), `CRON_CONFIG`, `GC_RETENTION_SECS`, `DB_SCHEDULES=1` keeps the daemon
resident; with none set, a file run drains and exits 0.

## Upgrade / rollback

The procedure, and what each release needs done before it starts, is in
[`UPGRADING.md`](UPGRADING.md). The rules behind it:

- Schema migrations are **embedded and run automatically at startup** (sqlx;
  `crates/dagron-core/migrations*/`). There is no `dagron migrate` command —
  starting an engine is what migrates. They are **forward-only** — no down
  migrations, so rollback = restore the pre-upgrade backup and start the old
  binary. Back up first, always.
- A failed migration is fail-fast, not half-applied: the transaction rolls back,
  the engine exits non-zero, and the database is left where it was.
- Step-by-step upgrade order, and what to do when a migration errors, refuses to
  start (`previously applied but has been modified`), or you have to go
  backwards: [`BACKUP_RECOVERY.md` §4](BACKUP_RECOVERY.md) and §6.
- The stack upgrades in place: stop, swap image/binary, start. Multi-node
  (Postgres) engines coordinate through the DB — leases expire (default 30 s)
  and a surviving node reclaims orphaned tasks, so a rolling restart does not
  lose work (see [`ARCHITECTURE.md` §5.5](ARCHITECTURE.md#55-crash-recovery--orphaned-lease-reclaimed-by-a-survivor)).

## Backup & restore — what IS the state

The database is the **only** state. Executors and workers are stateless;
`dagron-api` keeps nothing outside the same database.

| Backend | Back up | How |
| --- | --- | --- |
| SQLite (default) | The single DB file (default `workflow.db`) **plus** its `-wal`/`-shm` sidecars | Stop the daemon and copy the file, or use `sqlite3 workflow.db ".backup backup.db"` on a live file (WAL mode). Never copy just the `.db` while running. |
| Postgres | The whole `workflow` database — engine tables (`workflow_runs`, `task_runs`, `task_dependencies`, `workflow_definitions`, `dead_letters`, `schedules`, `schedule_backfills`, `event_outbox`, leader election) **and** the `dagron-api`-owned tables (`users`, `git_repos`, `workflows`) | `pg_dump` on a schedule; restore with `pg_restore`/`psql`, then start the engine (it re-applies any newer migrations). |

If you use the artifact store (`DAGRON_ARTIFACT_DIR`) or GitOps workflow dir
(`WORKFLOW_DIR`), those directories are re-creatable inputs, not state — but
back them up if your workflows are not in git.

Three things the table above does **not** cover, and each bites:

- **`DAGRON_ENV_SECRET_KEY` is not in the database.** The ciphertext of every
  stored environment secret is; the key is an env var. Lose it and those secrets
  are unrecoverable — back it up in a secret manager, kept apart from the dumps.
- **A log store is state too.** With `DAGRON_LOG_STORE`, task logs live in
  its directory or bucket, and the engines remove from it what the database no
  longer names: back it up with the database, and never run an engine that
  sweeps it against a copy of the database
  ([`BACKUP_RECOVERY.md` §6.9](BACKUP_RECOVERY.md)).
- **Restore the whole database, never a table subset.** `dagron-api` creates its
  own tables (`users`, `environments`, `environment_secrets`, `git_repos`, …)
  with `CREATE TABLE IF NOT EXISTS`, so a dump missing them restores "cleanly"
  and it recreates them **empty** — every user and secret silently gone.

Ready-made scripts and a restore rehearsal you can run in minutes:
[`../scripts/backup-postgres.sh`](../scripts/backup-postgres.sh),
[`../scripts/restore-postgres.sh`](../scripts/restore-postgres.sh),
[`BACKUP_RECOVERY.md` §5](BACKUP_RECOVERY.md).

## Monitoring

- **Liveness:** `GET /healthz` on both the engine ops API and `dagron-api`
  (no auth, no DB on the latter).
- **Metrics:** Prometheus text on the **engine** at `GET <API_ADDR>/metrics`
  — task counters (dispatched/succeeded/failed/retried), latency histograms,
  per-workflow run counts and run time, live DB gauges (all `scheduler_*`) and
  the engine's own CPU and memory (`process_*`). `dagron-api` re-surfaces JSON
  counts at `GET /api/metrics` for the UI. Every series is documented in the
  [metrics guide](METRICS.md).
- **Dashboards:** a ready-to-run Prometheus + Grafana stack with eight bundled
  dashboards is in [`examples/monitoring/`](../examples/monitoring/): overview,
  workflows by namespace, workflow statistics, run time, jobs, dead letters,
  instance metrics, and health and latency.
- **Alert on:** failed runs trending up; `dead_letters` count > 0 and growing
  (submissions being rejected — redrive or fix them); reconcile-tick histogram
  upper buckets filling (a pegged tick shows up under load); Postgres
  availability; with a log store (`DAGRON_LOG_STORE`),
  `scheduler_log_sweeps_failing > 0` for 30 minutes, or
  `scheduler_log_sweeps_oldest_age_seconds > 7200`: logs nobody reads are
  piling up in it ([`METRICS.md`](METRICS.md)).
- **Capacity:** defaults are `WORKER_COUNT=16` concurrent tasks per node and
  `MAX_INFLIGHT_RUNS=64` active runs (admission-controlled, `429` above it —
  dagron sheds explicitly, it does not silently drop). SQLite is
  single-writer/single-node by design; scale out on Postgres.

## Security posture

- **Two listeners, two trust levels.** The engine ops API (`API_ADDR`) is
  **unauthenticated by design** — bind it to localhost / a cluster-private
  interface only. The public edge is `dagron-api`: self-contained auth
  (argon2 password hashing, HS256 session JWT in an HttpOnly
  `dagron_session` cookie or a Bearer header; sessions default to 7 days).
  `POST /api/users` is admin-group only.
- **Secrets:** `DAGRON_JWT_SECRET` must be ≥ 32 chars (startup-enforced);
  `DATABASE_URL` credentials are redacted before logging; `GITHUB_TOKEN` (git
  sync) is read from env, never stored in the DB.
- **TLS** is out of scope for the binaries — terminate it in front
  (ingress/reverse proxy). The session cookie is `Secure` by default; only
  relax that (`DAGRON_COOKIE_SECURE=false`) for plain-HTTP local dev.
- **CORS on `dagron-api` is currently permissive** (dev posture) — restrict
  to the frontend origin for production exposure.
- **Tasks are arbitrary subprocesses.** Whoever can submit a workflow can run
  commands as the engine's user (`LocalExecutor`) or in containers
  (`docker`/`kubernetes` executors — the isolation boundary to prefer for
  untrusted definitions). Agent access goes through the JWT-gated edge only;
  see [`MCP.md`](MCP.md) for token scoping and sandboxing guidance.
- Vulnerability disclosure: [`SECURITY.md`](../SECURITY.md).

## Troubleshooting (symptom-first)

| You see | Cause | Fix |
| --- | --- | --- |
| `POST /runs` → `429` with `Retry-After` | Admission valve: active runs ≥ `MAX_INFLIGHT_RUNS` (default 64). Deliberate backpressure, not an error. | Wait/retry after the hint, or raise `MAX_INFLIGHT_RUNS`. |
| `dagron-api` exits: `DAGRON_JWT_SECRET must be set and at least 32 characters` | Missing/short secret. | Set a ≥ 32-char secret (compose ships a dev-only one — change it). |
| `dagron-api` exits: `DATABASE_URL must be set` | It is given the engine's datastore and has none. | Point it at the engine's Postgres, or, built for one node, at its SQLite file ([ONE_NODE.md](ONE_NODE.md)). |
| Login succeeds but the browser stays logged out (over `http://`) | `Secure` cookie is not stored on plain HTTP. | `DAGRON_COOKIE_SECURE=false` for local dev only. |
| Every `/api/*` call → `401` | Missing/expired session (default TTL 7 days) or wrong `DAGRON_JWT_SECRET` between mint and verify. | Re-login; for API-only testing mint a token with `scripts/mint-dev-token.mjs` using the *same* secret. |
| First login fails with the seeded admin | `DAGRON_ADMIN_EMAIL`/`DAGRON_ADMIN_PASSWORD` unset at `dagron-api` startup (bootstrap is skipped), or password < 8 chars. | Set both and restart — seeding is idempotent and never resets an existing user. |
| `dagron dev` errors: `requires building with the ops feature` | Lean build without the management API. | Build with default features (or `--features ops`). |
| Startup error: `EXECUTOR=kubernetes requires building with --features kubernetes` | Executor is compile-time gated (no silent downgrade). | Rebuild with the feature, or use `local`/`docker`. |
| Startup error: ``s3://… needs the `archive-s3` feature`` (or `archive-gcs`, `archive-azure`) | The object store behind `GC_ARCHIVE_URL` is compile-time gated (no silent plain purge). The published images have all three; a source build with default features has none. The `configuration resolved` line a binary logs as it starts names what it was built with (`features=`). | Use a published image, or rebuild with the feature. |
| Task killed at ~25 s | Default per-task `timeout_secs` is **25** (sits inside the 30 s lease). | Set `timeout_secs` on the task. |
| Task ran twice after a crash/restart | Lease expiry + reclaim (default 30 s) re-dispatches; version fencing rejects the stale attempt's write. Expected crash-recovery behaviour. | Make tasks idempotent; see [`ARCHITECTURE.md` §5.5](ARCHITECTURE.md#55-crash-recovery--orphaned-lease-reclaimed-by-a-survivor). |
| Tasks sit `ready` with a **Waiting on quota** badge in the console; engine log: `waiting on namespace quota` | Kubernetes executor: the apiserver refused the task's pod because the namespace is at its `ResourceQuota` (`exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4`). Nothing was started, so the task went back to the queue with its attempt intact and is dispatched again after about 5 s, backing off to about a minute between tries. Deliberate backpressure, not an error — until it has waited `DAGRON_QUOTA_WAIT_MAX_SECS` (default 6 h), when the task fails. Waiting tasks are admitted in no particular order: each probes on its own backoff and the first pod create after room frees gets it. Neither `priority` nor how long a task has waited decides who runs next, and a newly ready task probes more often than one that has waited a while. Every probe is an ordinary dispatch, so the engine logs its INFO `dispatching` line for it: about one line a minute per waiting task, so log volume grows with the number of waiters. | Raise the quota, lower the task's `resources`, or cap how many run at once with `POOLS` so fewer are dispatched into a full namespace. Where the order matters, give the tasks a `pool:` and cap it with `POOLS` at or below what the quota fits, so they queue at the claim, which is ordered (`priority`, then due time), instead of at the quota. `scheduler_tasks_waiting_on_quota` and `scheduler_quota_wait_oldest_age_seconds` say how many and for how long ([`METRICS.md`](METRICS.md)). `DAGRON_QUOTA_WAIT_MAX_SECS=0` turns the wait off, and a refusal is a failed attempt ([`CONFIG.md`](CONFIG.md)). |
| One-shot run hangs instead of exiting (or exits when you wanted a server) | Residency is driven by config: `API_ADDR`/`CRON_CONFIG`/`GC_RETENTION_SECS`/`DB_SCHEDULES` keep the process up. | Unset them for one-shot runs; set one (e.g. `API_ADDR`) — or use `dagron dev` — for a server. Note an *invalid* `API_ADDR` logs a warning and disables the API. |
| Log line: `invalid API_ADDR — management API disabled` | Unparseable `host:port`. | Fix the address; the daemon otherwise runs without the ops API. |
| Submissions vanish without a run | They were dead-lettered (unparseable spec immediately; transient `create_run` failures after `DEAD_LETTER_MAX_ATTEMPTS`, default 3). | `GET /api/dead-letters`, fix, `…/redrive`. Alert on the count. |
| Cron/schedules/GC not firing on any node | Not leader, or `cron config invalid — cron disabled` in the log. | Check the leadership lease (one node owns ops loops, lease 30 s) and validate the `CRON_CONFIG` YAML. |
| SSE clients get `event: resync` / `data: lagged` | The per-process broadcast buffer overflowed for a slow client. | Client should refetch run state and reattach — by design, nothing is lost in the DB. |
| `POST /api/workflows/{id}/sync-to-git` → `501` | Git sync unconfigured. | Set `GITHUB_TOKEN` + `GIT_REPO` (optionally `GIT_BASE`, `GIT_PATH_PREFIX`, `GIT_API_BASE`). |
| SQLite: `database is locked` / stalls under write load | SQLite backend is deliberately single-writer (pool of 1, 5 s busy timeout); a second process on the same file contends. | One daemon per SQLite file. For concurrency or multi-node, build with `postgres`. |
| Example DAG fails in the container: command not found | The `runtime` image is distroless — no shell, no `echo`. | Use the `localdev` image target (compose does), or make tasks call real binaries. |
| A task's log or output, or a Docker action's step output, has a hole in it, or lacks its beginning, though the task wrote it; in the task's log: `[dagron] this task's output stopped being followed before it ended`; engine log, at startup or at the first task: `what is read back of a container may not be whole with this log driver`, `the daemon rotates each container's log`, or `the daemon does not wait for its log driver` | **Docker executor:** a task's own log is read from its container's streams as the task writes them, and does not depend on the log driver. What is read back from the daemon once a container has stopped does, and is what that driver kept: a Docker action's step output; what a container that failed before its task started said; a service's last lines; and a task's own log where its container could not be attached to (the engine says so once: `could not attach to a task's container`) or the follow of it was cut (the log says so where what was read back cannot be placed after what was followed, or does not come in time). Each container gets the daemon's own log driver unless `DAGRON_DOCKER_LOG_DRIVER` names one. The journal (Podman's default) drops lines past its rate limit; Docker's `local` keeps 100 MB of a container's log by default; a rotation set on the daemon (`max-size`, `max-file`) keeps the end; a daemon set to `mode=non-blocking` drops what a container writes while its buffer is full, with any driver. **Kubernetes executor:** a task's log is followed from the kubelet as the task writes it, so the kubelet's rotation of a container's log at `containerLogMaxSize` (10Mi unless set) costs it nothing, but for what rotated while a follow of it was cut and opened again (and lines written a moment after the last one read): the kubelet serves only the file it is writing. Whenever the log is opened again, the read once the pod is over included, a line written across a rotation may be in it in part twice or in part not; the engine warns where it sees one (on CRI-O a whole line after such a line may be in it twice, and nothing warns). A task that runs out of time is read once more at its deadline, not followed, before its pod is deleted (for at most 10 s): what the kubelet has of it by then is in its log (on containerd, a line under 16 KiB not yet ended is not yet in the kubelet's file), with what it wrote while that read ran, and nothing later. | Docker and Podman: `DAGRON_DOCKER_LOG_DRIVER=json-file` keeps each container's whole log in a file on the host, for what is read back (`k8s-file` is Podman's own name for it). Nothing bounds that file until the container is removed, so on a host that relies on its driver's bound, raise the bound instead (the journal's `RateLimitBurst`; the daemon's `max-size` and `max-file`). No driver gets back what a daemon in `mode=non-blocking` dropped: set the daemon's `mode` to `blocking`, which ends that loss and leaves what the driver itself keeps as it was. Kubernetes: raise `containerLogMaxSize` in the kubelet's configuration on the nodes tasks run on. In every case an attempt keeps at most `DAGRON_LOG_TASK_CAP_BYTES` of its log's beginning and `DAGRON_LOG_TAIL_BYTES` of its end, and says what it left out between. |
| A task's log has a line in its middle, `[dagron] … of this attempt's output (N bytes and M lines, counted before masking) is left out here; what follows is its end`, after `[dagron] this attempt's log reached 64 MiB (DAGRON_LOG_TASK_CAP_BYTES): what the task writes from here is held back, …`; engine log: `this attempt's log passed its cap; its middle is left out` | The attempt wrote more than its log keeps: its beginning, to `DAGRON_LOG_TASK_CAP_BYTES`, and, once it was over, its end, the last `DAGRON_LOG_TAIL_BYTES` (a quarter of the cap by default). What the line counts is what the task wrote between, before masking. `scheduler_task_logs_capped_total` and `scheduler_task_log_bytes_omitted_total` count such attempts and their bytes. | Expected. To keep more, raise `DAGRON_LOG_TASK_CAP_BYTES` (both are kept per attempt, in the database or the log store) or `DAGRON_LOG_TAIL_BYTES` (no more than the cap); to have a task write less, make it quieter. `DAGRON_LOG_TAIL_BYTES=0` keeps the beginning alone, as before. On a remote runner both are the runner host's settings; dagron-api keeps up to twice its own cap and 1 MiB of a runner's log. |
| A task's log ends on `[dagron] this attempt's log reached … what the task writes from here is held back, and the last … of it (DAGRON_LOG_TAIL_BYTES) is added below when it ends`, with nothing below | The attempt never ended in its executor, and its row took nothing more: its claim went to another (its lease lapsed, or a teardown took it). Or it is still running: the end comes at once when it is over. (A cancelled task's log ends on `… and its end was not kept`.) | Look at the engine's log for the lost claim (`the task's claim was lost`). |
| The log store keeps growing, or `scheduler_log_sweeps_failing` is above 0; engine log: `the log store's GC could not remove some tasks' logs` | A task's logs leave the store only once its row is deleted (retention, an archive, a rerun's or a fan-out's drop) or a later writing of its log begins, and only when an engine with the store removes them: one whose `DAGRON_LOG_SWEEP_SECS` is not `0`. Removals the store refuses (credentials without delete, an object lock, a bucket that moved) are tried again, backing off to six hours. With no retention (`GC_RETENTION_SECS` unset) the store grows as the database does. What was in the store before the migration that added the GC (098; SQLite 073), and what an older engine wrote during a rolling upgrade, is not recorded and stays. | Read what failed: `SELECT task_id, reason, tries, last_error FROM log_sweeps WHERE tries > 0`. Give the engines' credentials delete on the store's prefix (`logs-*`) and check `DAGRON_LOG_SWEEP_SECS` on at least one engine with the store; a record due again is tried at its time, or at once after `UPDATE log_sweeps SET due_at = '1970-01-01T00:00:00+00:00' WHERE tries > 0`. A store given up for good is let go of with `DELETE FROM log_sweeps`. Leftovers with no record are cleared by inserting one a task (`docs/BACKUP_RECOVERY.md`): only what no row names is removed. |
| Engine log: `unrecognized EXECUTOR value, defaulting to local` | Typo in `EXECUTOR`. | Use `local`, `docker`, or `kubernetes`/`k8s`. |
