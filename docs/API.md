# API reference

dagron exposes **two HTTP surfaces** with a deliberate boundary — the
unauthenticated in-cluster **engine ops API** and the JWT-gated **`dagron-api`
UI edge**. Why there are two, and what is unique to each, is diagrammed in
[`ARCHITECTURE.md` §2a](ARCHITECTURE.md#2a-two-http-surfaces--engine-ops-api-vs-ui-gateway).
This file is the endpoint reference. Sources: the routers in
`crates/dagron-api/src/main.rs` and `crates/dagron-engine/src/api.rs`, and the
handler modules they name — regenerate when those change.

## 1. `dagron-api` — the authenticated UI edge

Postgres-only, stateless, listens on `PORT` (default `8080`).

**Auth:** every route below except `/healthz`, `POST /api/login` and
`POST /api/logout` requires a valid HS256 session JWT, accepted either as the
HttpOnly `dagron_session` cookie (browsers) or `Authorization: Bearer <jwt>`
(API clients). Missing/invalid/expired → `401` (`src/auth.rs`).
`POST /api/users`, `GET /api/users`, `GET /api/audit`, and all three
`/api/settings/notifications*` routes additionally require the `admin` group
(`403` otherwise) — notification defaults hold secret webhook URLs and reroute
every run's notifications, and the test route makes the server POST outbound.

**Errors:** handlers answer `(status, {"error": "<message>"})`; DB failures map
to `500` without leaking internals — except `GET /api/health`, which by design
answers `200` with `db: "error"` so the outage itself is reportable. Request bodies are capped at **1 MiB**.
CORS is currently permissive (dev posture — see
[`OPERATIONS.md`](OPERATIONS.md#security-posture)).

### Auth & session

| Method | Path | Body → Response | Errors |
| --- | --- | --- | --- |
| POST | `/api/login` | `{email, password}` → sets `dagron_session` cookie + `{token}` | `401` bad credentials |
| POST | `/api/logout` | — → clears the cookie | — |
| GET | `/api/me` | — → the session claims `{sub, email, name, groups, exp}` | `401` |
| POST | `/api/users` | `{email, password, name, groups[]}` → `201 {id}` | `403` non-admin, `400` password < 8, `409` duplicate |
| GET | `/api/users` | → `[{id, email, name, groups[], created_at}]` (no hashes) | `403` non-admin |
| GET | `/api/health` | rich health for the UI status widget: `{api, edition, db, scheduler_leader, leader_holder, active_runs, awaiting_approvals, dead_letters}`. Never 500s — a DB outage answers `db: "error"` | `401` |
| GET | `/api/search` | `?q=&limit=` (limit per category, default 8, max 20) → `{query, workflows[], runs[], schedules[]}`. Capped + parameterized (run ids match by prefix, names by substring; LIKE wildcards escaped) — the ⌘K palette backend | `401` |
| GET/POST | `/api/environments` | list / create `{name, description?, variables{}}` → environments with `variables` + `secret_names` (values are **write-only**, never returned) | `409` duplicate, `400` bad name |
| PUT/DELETE | `/api/environments/{id}` | update description/variables (name is immutable — specs reference it) / delete incl. secrets | `404` |
| PUT | `/api/environments/{id}/secrets/{name}` | `{value}` → `204`; encrypted immediately (AES-256-GCM, `DAGRON_ENV_SECRET_KEY`) | `503` no key configured, `404`, `400` |
| DELETE | `/api/environments/{id}/secrets/{name}` | → `204` | `404` |
| GET/PUT | `/api/settings/notifications` | **admin** — instance-wide notification defaults `{slack_enabled, slack_webhook_url, slack_on[], webhook_enabled, webhook_url, webhook_on[]}`, stored in `ui_settings` and merged by the engine into every run's notify dispatch | `403` non-admin, `400` bad URL/event |
| POST | `/api/settings/notifications/test` | **admin** — send a test message to each enabled target in the body; per-target outcome `{slack, webhook}` (never fails the whole call) | `403` non-admin, `400` |

Example:

```bash
TOKEN=$(curl -s http://localhost:8080/api/login \
  -H 'content-type: application/json' \
  -d '{"email":"admin@local","password":"dagron-admin"}' | jq -r .token)
curl -s http://localhost:8080/api/me -H "Authorization: Bearer $TOKEN"
# {"sub":"…","email":"admin@local","name":"Administrator","groups":["admin"],"exp":…}
```

### Runs

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/runs` | `?status=&name=&trigger=&limit=&offset=` (limit default 100, max 500) → `[{id, definition_id, status, created_at, finished_at, name, trigger_kind}]`. `name` filters by workflow, `trigger` by `manual`/`schedule`/`backfill`; `trigger_kind` is derived (schedule stamp / backfill ledger), no schema change |
| POST | `/api/runs` | `{yaml: "<workflow YAML>"}` → `201 {run_id}`; `400` on invalid YAML / cycles / duplicate or unknown task names (validated before anything is persisted) |
| GET | `/api/runs/{id}` | run detail (`{…, name, trigger_kind}`) + `tasks[]` (`{id, name, status, attempt, output, scheduled_at, finished_at}`); `404` |
| GET | `/api/runs/{id}/wait` | `?timeout_secs=` (default 30, clamp 1–600) long-poll to terminal → `{run_id, status, finished, result}` (`result` = the `result_from` task's output on success); `404`; a timed-out wait is `200` with `finished: false` |
| GET | `/api/runs/{id}/graph` | DAG as `{nodes[], edges[]}` for the UI graph view |
| GET | `/api/runs/{id}/tasks/{tid}/logs` | `{task_id, name, status, attempt, output, offset, next_offset, eof}`; `404`. **`?offset=`** returns only the output past that char offset for live tailing — poll with `?offset=next_offset` until `eof` |
| GET | `/api/runs/{id}/stream` | **SSE**: one shared Postgres `LISTEN task_events` fans out to per-run streams; each event is JSON; on broadcast lag the client receives `event: resync` / `data: lagged` and should refetch |
| GET | `/api/events/stream` | **SSE**: account-wide activity — every run's task events (`{run_id}`), unfiltered, off the same shared listener; feeds the UI list pages' live-updates mode; same `resync` contract on lag |
| POST | `/api/runs/{id}/cancel` | → `{cancelled: n}`; `404` |
| POST | `/api/runs/{id}/rerun` | optional `{from?}` → `{run_id, rerun}`; `404`/`409`/`400` |
| POST | `/api/runs/{id}/resubmit` | → `201 {run_id}` (fresh run from the same spec); `404` |
| POST | `/api/runs/{id}/tasks/{tid}/retry` | → `{retried}`; `404`/`409` |
| POST | `/api/runs/{id}/tasks/{tid}/clear` | clear a completed task + its downstream cone (Airflow "Clear, Downstream") → `{run_id, task_id, cleared}`; `404` unknown run/task, `409` task not completed |
| POST | `/api/runs/{id}/tasks/{tid}/approve` | approve a `type: approval` gate → the task succeeds, DAG proceeds → `{run_id, task_id, resolution}`; `404`, `409` not awaiting approval |
| POST | `/api/runs/{id}/tasks/{tid}/reject` | reject a gate → the task fails, `all_success` downstream skips; `404`, `409` |

Control mutations fire `pg_notify('task_events', run_id)` in-transaction, so
the engine wakes immediately (`src/routes/control.rs`).

### Archived runs (history past the hot window)

Runs the archive-before-purge GC moved out of the hot store (see
`docs/CONFIG.md` `GC_ARCHIVE_DIR`/`GC_ARCHIVE_URL` and `ee/STATE_STORE.md`).
The list reads only the `archived_runs` index; the detail endpoint fetches the
run's `dagron.run-archive.v1` JSON document from the archive sink, so
dagron-api must see the same `GC_ARCHIVE_DIR`/`GC_ARCHIVE_URL` env as the
engine (S3 needs the api's `archive-s3` cargo feature).

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/archive/runs` | `?name=&limit=&offset=` (limit default 100, max 500), newest-finished-first → `[{run_id, name, status, created_at, finished_at, archived_at, compacted_at, parquet_path}]` |
| GET | `/api/archive/runs/{id}` | the full archive document (`{format, run, tasks[], outbox_events[], archived: true, index}`); `404` not in the index; **`410`** compacted to Parquet (body carries `parquet_path` — query the analytics dataset instead); `502` sink unreachable/unconfigured |

### Observability & dead letters

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/metrics` | JSON: `{runs_by_status[], tasks_by_status[], dead_letters}` (the Prometheus text endpoint lives on the engine, not here) |
| GET | `/api/metrics/timeseries` | `?days=` (default 14, clamp 1–90) `&name=` → per-day buckets `[{day, succeeded, failed, cancelled, active, avg_duration_secs, max_duration_secs}]` for the Metrics charts / workflow trend |
| GET | `/api/approvals` | every task parked in `awaiting_approval`, oldest first → `[{run_id, task_id, task_name, workflow_name, since}]` (the human-in-the-loop worklist) |
| GET | `/api/dead-letters` | `?limit=` (default 100, max 500) → `[{id, payload, error, source, failures, first_seen_at, last_error_at}]` |
| POST | `/api/dead-letters/{id}/redrive` | → `{run_id, redriven_from}`; `404`/`400` |
| DELETE | `/api/dead-letters/{id}` | → `204`; `404` |

### Workflows, schedules, GitOps

| Method | Path | Notes |
| --- | --- | --- |
| GET/POST | `/api/workflows` | list (enriched with schedule + recent-run digest) / create `{name?, spec, description?}` → `201`; `409` duplicate name |
| GET/PUT/DELETE | `/api/workflows/{id}` | read / update / delete; `404`, `409` |
| POST | `/api/workflows/{id}/run` | → `{run_id, workflow_id}` |
| GET | `/api/workflows/{id}/runs` | `?limit=&offset=` → this workflow's run history (same row shape as `/api/runs`). Runs are matched by definition **name** — the only linkage that exists (each run snapshots its own `workflow_definitions` row, so there is no FK to `workflows`); the list digest uses the same rule. Renaming a workflow therefore starts a fresh history; `404` |
| POST | `/api/workflows/{id}/sync-to-git` | open a PR with the spec → `{pr_url, branch, path}`; `501` until `GITHUB_TOKEN`+`GIT_REPO` are set; `502` on GitHub errors |
| GET/POST | `/api/git-repos` | list / connect `{url, branch?, auto_sync}` → `201`; `400` empty URL, `409` duplicate |
| DELETE | `/api/git-repos/{id}` | `204`; `404` |
| POST | `/api/git-repos/{id}/sync` | sync now → updated repo row |
| GET/POST | `/api/schedules` | `?workflow_id=` / create `{workflow_id, cron_expr, enabled?, catchup?, catchup_window_secs?, catchup_max_runs?}`; `400` bad cron |
| PUT/DELETE | `/api/schedules/{id}` | update / delete; `404` |
| POST | `/api/schedules/{id}/backfill` | **synchronous** backfill: `{from, to, max_runs?}` → `{scheduled, skipped, from, to, run_ids}` (materialized in one call, hard cap 1000) |
| POST | `/api/backfills` | **paced** backfill *job* (AIP-78): `{schedule_id, from, to, max_runs?}` → `201` job row; the engine paces it (job cap 100k). `404` unknown schedule, `400` bad range/cron/spec or no fire-times |
| GET | `/api/backfills` | `?schedule_id=&limit=` → job list (`{id, schedule_id, status, requested, fired, cursor, …}`) |
| GET | `/api/backfills/{id}` | one job for monitoring (`fired`/`requested`/`status`); `404` |
| POST | `/api/backfills/{id}/cancel` | stop pacing a running job → `{id, cancelled}`; `404` unknown, `409` already finished |

## 2. Engine ops API (`--features ops`, bound at `API_ADDR`)

**No authentication** — this surface is designed to stay cluster-private
(localhost / pod-internal). Never expose it publicly; that is what
`dagron-api` is for. Self-describing: OpenAPI 3 at `/openapi.yaml` /
`/openapi.json`, Swagger UI at `/docs`.

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/healthz` | `"ok"`, no DB |
| GET | `/metrics` | **Prometheus text** — process counters (dispatched/succeeded/failed/retried, reconcile-tick histogram) + live DB gauges |
| GET | `/openapi.yaml` · `/openapi.json` · `/docs` | embedded spec + Swagger UI |
| GET | `/runs` | `?status=&limit=` (default 50, clamp 1–1000) |
| POST | `/runs` | **raw YAML body** (not JSON-wrapped) → `201 {run_id}`; `400` invalid DAG; **`429` + `Retry-After`** when `MAX_INFLIGHT_RUNS` is exceeded — the admission backpressure documented in [`ARCHITECTURE.md` §5.6](ARCHITECTURE.md#56-v4-queue-driven-ingestion--admission-backpressure). **`?wait=true`** (with `?timeout_secs=`) makes it a synchronous invocation: `200 {run_id, status, finished, result}` instead of `201` |
| GET | `/runs/{id}` | `{run, tasks}`; `404` |
| GET | `/runs/{id}/wait` | `?timeout_secs=` (default 30, clamp 1–600) long-poll to terminal → `{run_id, status, finished, result}`; `404`; timed-out wait is `200` with `finished: false` |
| GET | `/runs/{id}/tasks/{task_id}/logs` | one task's output for tailing → `{task_id, name, status, attempt, output, offset, next_offset, eof}`; `404`. **`?offset=`** returns only the output past that char offset — poll with `?offset=next_offset` until `eof` |
| POST | `/runs/{id}/cancel` | `{run_id, cancelled: true}`; `409` if not cancellable |
| POST | `/runs/{id}/rerun` | optional `{from?}` → `{run_id, rerun}`; `404`/`409`/`400` |
| POST | `/runs/{id}/tasks/{task_id}/clear` | clear a completed task + its downstream cone → `{run_id, task_id, cleared}`; `404` unknown run/task, `409` task not completed |
| POST | `/runs/{id}/tasks/{task_id}/approve` · `/reject` | resolve a `type: approval` gate → `{run_id, task_id, resolution}`; `404`, `409` not awaiting approval |
| GET | `/dead-letters` | `{dead_letters: […]}` |
| POST | `/dead-letters/{id}/redrive` | `{run_id, redriven_from}`; `404`/`400` |
| DELETE | `/dead-letters/{id}` | `{id, deleted: true}`; `404` |

```bash
# dagron dev (or compose engine) — submit straight YAML, then watch it:
curl -s -X POST localhost:8787/runs --data-binary @examples/simple_dag.yaml
# {"run_id":"…"}
curl -s localhost:8787/runs/<run_id> | jq .run.status
```

## 3. MCP (agent) surface

`dagron-mcp` fronts **`dagron-api`** (never the engine ops API) over the Model
Context Protocol on stdio. Tool catalogue, client config and security model:
[`MCP.md`](MCP.md); the event-call sequence is
[`ARCHITECTURE.md` §5.8](ARCHITECTURE.md#58-mcp-agent-event-call--submit--bounded-sse-event-poll).
