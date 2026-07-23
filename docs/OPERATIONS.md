# Operations runbook

Deploying, upgrading, backing up and debugging a dagron installation. Knobs
live in [`CONFIG.md`](CONFIG.md); endpoints in [`API.md`](API.md); internals in
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Deploy

**Full UI stack (local/dev):** `podman compose up --build` (validated with
podman; docker compose works the same) from the module root brings up
`postgres` → `engine` (Postgres+ops build, ops API on 8787, cluster-internal)
→ `dagron-api` (`127.0.0.1:8080`) → `frontend` (`127.0.0.1:3000`). Sign in at
`http://localhost:3000` with the seeded admin (`admin@local` /
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

- Schema migrations are **embedded and run automatically at startup** (sqlx;
  `crates/dagron-core/migrations*/`). They are **forward-only** — there are no
  down migrations, so rollback = restore the pre-upgrade backup and start the
  old binary. Back up first, always.
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

## Monitoring

- **Liveness:** `GET /healthz` on both the engine ops API and `dagron-api`
  (no auth, no DB on the latter).
- **Metrics:** Prometheus text on the **engine** at `GET <API_ADDR>/metrics`
  — task counters (dispatched/succeeded/failed/retried), the reconcile-tick
  latency histogram, and live DB gauges (all `scheduler_*`). `dagron-api`
  re-surfaces JSON counts at `GET /api/metrics` for the UI.
- **Dashboard:** a ready-to-run Prometheus + Grafana stack with a bundled
  dashboard is in [`examples/monitoring/`](../examples/monitoring/) — scrapes the
  engine and renders throughput, run/task state, latency percentiles, backlog,
  and DB-pool saturation.
- **Alert on:** failed runs trending up; `dead_letters` count > 0 and growing
  (submissions being rejected — redrive or fix them); reconcile-tick histogram
  upper buckets filling (a pegged tick shows up under load); Postgres
  availability.
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
| `dagron-api` exits: `DATABASE_URL must be set` | The edge is Postgres-only. | Point it at the engine's Postgres. |
| Login succeeds but the browser stays logged out (over `http://`) | `Secure` cookie is not stored on plain HTTP. | `DAGRON_COOKIE_SECURE=false` for local dev only. |
| Every `/api/*` call → `401` | Missing/expired session (default TTL 7 days) or wrong `DAGRON_JWT_SECRET` between mint and verify. | Re-login; for API-only testing mint a token with `scripts/mint-dev-token.mjs` using the *same* secret. |
| First login fails with the seeded admin | `DAGRON_ADMIN_EMAIL`/`DAGRON_ADMIN_PASSWORD` unset at `dagron-api` startup (bootstrap is skipped), or password < 8 chars. | Set both and restart — seeding is idempotent and never resets an existing user. |
| `dagron dev` errors: `requires building with the ops feature` | Lean build without the management API. | Build with default features (or `--features ops`). |
| Startup error: `EXECUTOR=kubernetes requires building with --features kubernetes` | Executor is compile-time gated (no silent downgrade). | Rebuild with the feature, or use `local`/`docker`. |
| Task killed at ~25 s | Default per-task `timeout_secs` is **25** (sits inside the 30 s lease). | Set `timeout_secs` on the task. |
| Task ran twice after a crash/restart | Lease expiry + reclaim (default 30 s) re-dispatches; version fencing rejects the stale attempt's write. Expected crash-recovery behaviour. | Make tasks idempotent; see [`ARCHITECTURE.md` §5.5](ARCHITECTURE.md#55-crash-recovery--orphaned-lease-reclaimed-by-a-survivor). |
| One-shot run hangs instead of exiting (or exits when you wanted a server) | Residency is driven by config: `API_ADDR`/`CRON_CONFIG`/`GC_RETENTION_SECS`/`DB_SCHEDULES` keep the process up. | Unset them for one-shot runs; set one (e.g. `API_ADDR`) — or use `dagron dev` — for a server. Note an *invalid* `API_ADDR` logs a warning and disables the API. |
| Log line: `invalid API_ADDR — management API disabled` | Unparseable `host:port`. | Fix the address; the daemon otherwise runs without the ops API. |
| Submissions vanish without a run | They were dead-lettered (unparseable spec immediately; transient `create_run` failures after `DEAD_LETTER_MAX_ATTEMPTS`, default 3). | `GET /api/dead-letters`, fix, `…/redrive`. Alert on the count. |
| Cron/schedules/GC not firing on any node | Not leader, or `cron config invalid — cron disabled` in the log. | Check the leadership lease (one node owns ops loops, lease 30 s) and validate the `CRON_CONFIG` YAML. |
| SSE clients get `event: resync` / `data: lagged` | The per-process broadcast buffer overflowed for a slow client. | Client should refetch run state and reattach — by design, nothing is lost in the DB. |
| `POST /api/workflows/{id}/sync-to-git` → `501` | Git sync unconfigured. | Set `GITHUB_TOKEN` + `GIT_REPO` (optionally `GIT_BASE`, `GIT_PATH_PREFIX`, `GIT_API_BASE`). |
| SQLite: `database is locked` / stalls under write load | SQLite backend is deliberately single-writer (pool of 1, 5 s busy timeout); a second process on the same file contends. | One daemon per SQLite file. For concurrency or multi-node, build with `postgres`. |
| Example DAG fails in the container: command not found | The `runtime` image is distroless — no shell, no `echo`. | Use the `localdev` image target (compose does), or make tasks call real binaries. |
| Engine log: `unrecognized EXECUTOR value, defaulting to local` | Typo in `EXECUTOR`. | Use `local`, `docker`, or `kubernetes`/`k8s`. |
