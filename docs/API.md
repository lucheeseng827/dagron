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

**Auth:** every route below except `/healthz`, `/readyz`, `POST /api/login`
and `POST /api/logout` requires a valid HS256 session JWT, accepted either as the
HttpOnly `dagron_session` cookie (browsers) or `Authorization: Bearer <jwt>`
(API clients). Automation may instead present a personal access token (`dgp_`
prefix) in the same `Authorization: Bearer` header; it resolves to its owner's
live permissions. Token *management* (`/api/tokens*`) is the one exception — it
requires a password session and refuses a `dgp_` bearer with `403`, so a leaked
token cannot mint its own replacements. Missing/invalid/expired → `401`
(`src/auth.rs`).
`POST /api/users`, `GET /api/users` and all three
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
| POST | `/api/tokens` | mint a personal access token `{name, expires_in_days?}` → `201 {id, name, prefix, token, expires_at}`; the `token` plaintext is returned **only here** (storage keeps `sha256` only) | `403` presented an API token, `400` empty name / `expires_in_days` ≤ 0 or out of range |
| GET | `/api/tokens` | → the caller's own tokens `[{id, name, prefix, created_at, expires_at, last_used_at, revoked_at}]` (no hashes, no plaintext) | `403` presented an API token |
| DELETE | `/api/tokens/{id}` | revoke → `204` (idempotent — revoking twice is not an error) | `403` presented an API token, `404` unknown / not the caller's |
| GET | `/readyz` | **readiness, no auth**: `200 ready` only when a pooled DB round trip answers inside `DAGRON_READY_TIMEOUT_MS` (default 500 ms); otherwise `503` with the reason. The SSE listener's state is **advisory**: while it is resubscribing the body reads `ready (event listener degraded)` but stays `200` — its failure is fleet-correlated (one shared `DATABASE_LISTEN_URL`), so gating on it would empty the Service rather than reroute; `DAGRON_READY_REQUIRE_LISTENER=1` opts into strict `503` gating. Point the orchestrator's readinessProbe here — `/healthz` stays the bare liveness `200 ok` ([`LOW_LATENCY.md`](LOW_LATENCY.md) R-1) | — |
| GET | `/api/health` | rich health for the UI status widget: `{api, edition, config_fingerprint, event_listener, db, scheduler_leader, leader_holder, active_runs, awaiting_approvals, dead_letters}` — `config_fingerprint` is the fleet-drift hash ([`CONFIG.md`](CONFIG.md#configuration-file--profiles-dagron_config)); `event_listener` (`ok`/`down`) is where a degraded live-event bridge is visible. Never 500s — a DB outage answers `db: "error"` | `401` |
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
| POST | `/api/runs` | `{yaml: "<workflow YAML>", parameters?: {k: v}}` → `201 {run_id}`; `400` on invalid YAML / cycles / duplicate or unknown task names (validated before anything is persisted). `parameters` override the spec's declared `parameters:` defaults; keys the spec never references are ignored. **Optional `Idempotency-Key:` header** — a repeat of the same submit returns the *same* `run_id` with `200` instead of creating a second run, so a retrying client cannot double-submit. `409` if the key was already used for a different body, or if an identical submit is still in flight (retry). An explicitly supplied *empty* `Idempotency-Key` is a `400` (`must not be empty`), so an absent header and an invalid empty one are distinguishable. Keys are printable ASCII, ≤255 chars, scoped to the authenticated caller, and live exactly as long as the run they name |
| GET | `/api/runs/{id}` | run detail (`{…, name, trigger_kind, failure}`) + `tasks[]` (`{id, name, status, attempt, output, scheduled_at, finished_at}`); `404`. **`failure`** summarises why the run broke without a second call — `{task_id, task_name, attempt, failed_tasks, message, truncated}`, or `null` when nothing failed. It names the *earliest-finished* failed task (the likeliest cause rather than a downstream casualty) and `failed_tasks` says how many there were; `message` is the tail of that task's output, clipped to 20 lines / 4 KB with `truncated: true`. A run failed with no failed task — a `run_timeout_secs` overrun cancels its tasks — reports the run's own reason with `task_id: null`. Present as soon as a task fails, so a still-`running` run can carry one |
| GET | `/api/runs/{id}/wait` | `?timeout_secs=` (default 30, clamp 1–600) long-poll to terminal → `{run_id, status, finished, result, failure}` (`result` = the `result_from` task's output on success); `404`; a timed-out wait is `200` with `finished: false`. **`failure`** is the same object `GET /api/runs/{id}` returns, present only when the wait ended on a failure — a synchronous caller gets the reason in the response it is already holding, with no second call. The task rows behind it are read only when there is a failure, so a succeeding wait costs nothing extra |
| GET | `/api/runs/{id}/graph` | DAG as `{nodes[], edges[]}` for the UI graph view |
| GET | `/api/runs/{id}/logs` | **Workflow logs** — every task's output merged into one attributed stream, filtered server-side → `{run_id, tasks[], lines[], total, matched, truncated, eof, filtered, limit}`; `404` unknown run, `400` invalid filter. `?task=`/`?status=` narrow which tasks are read (name or id, repeatable or CSV); the [log filter](#log-filter) then narrows which lines survive. `total`/`matched` are counted **before** the line cap, so a truncated view always says how much it hid |
| GET | `/api/runs/{id}/tasks/{tid}/logs` | `{task_id, name, status, attempt, output, offset, next_offset, eof, total, matched, truncated, filtered, lines?}`; `404`, `400` invalid filter. **`?offset=`** returns only the output past that char offset for live tailing — poll with `?offset=next_offset` until `eof`. The [log filter](#log-filter) applies *within* that slice while `next_offset` keeps counting the raw text, so filtering and tailing compose; `lines` is present only when a filter was asked for |
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

<a id="log-filter"></a>

### Log filter

Both log endpoints — on `dagron-api` and on the engine ops API — accept the same
filter grammar, applied **server-side**. A run's captured output can be hundreds
of megabytes; shipping all of it so the client can grep is not a filter, it's a
download.

The filter is a set of predicates, **all** of which must hold for a line to be
kept. Sending none of them returns unfiltered output (still subject to the line
cap), byte-for-byte as before filtering existed.

| Param | Meaning |
| --- | --- |
| `q` | keep only lines containing this text (repeatable — all terms must match) |
| `exclude` | drop lines containing this text (repeatable — none may match) |
| `regex` | keep only lines matching this regular expression (max 512 bytes) |
| `level` | keep only these levels (repeatable or CSV): `error`/`warn`/`info`/`debug`/`trace`/`plain` |
| `case` | `1` to match case-sensitively; the default is insensitive |
| `context` | also keep N unmatched lines either side of each match (`grep -C`) |
| `limit` | max lines returned (default 2000, hard cap 50000; `0` = default) |
| `tail` | `1` to keep the **last** lines when the cap applies, rather than the first |

```sh
# Every error line in the run, with a line of context either side.
curl -s "$API/api/runs/$RUN/logs?level=error&context=1" | jq -r '.lines[].text'

# Just the extract task, minus healthcheck noise, last 200 lines.
curl -s "$API/api/runs/$RUN/logs?task=extract&exclude=healthz&limit=200&tail=1"
```

Notes that matter when reading a response:

- **Levels are inferred**, not recorded. Task output is whatever the command
  printed, so the level comes from scanning the head of each line. A line that
  never says "error" cannot be found by asking for errors, and `plain` (no
  recognizable level token) is the common case.
- **Line numbers (`n`) are positions in the unfiltered output**, so a filtered
  line can still be located in the raw log.
- **`total` and `matched` are counted before the line cap.** A view that hides
  most of a run always says so, via `matched` vs `total` and `truncated`.
- **Context lines come back with `matched: false`** so a client can dim them —
  context you can't tell apart from a hit overstates what the filter found.
- **An invalid filter is a `400` naming the reason** (uncompilable regex, unknown
  level, non-numeric bound). Ignoring it would return an unfiltered wall of text
  the caller would read as "nothing was filtered out".

The grammar lives in `crates/dagron-logging/src/logfilter.rs` — one parser, so a
filter typed in the console, sent by an SDK, or written into a runbook all mean
the same thing.

### Archived runs (history past the hot window)

Runs the archive-before-purge GC moved out of the hot store (see
`docs/CONFIG.md` `GC_ARCHIVE_DIR`/`GC_ARCHIVE_URL`).
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

### Datasets (lineage & registry — data-aware scheduling)

Read-only views of the dataset registry and its append-only update ledger — the
cross-workflow trail behind `produces:` / `on_datasets:`. Both read off the read
pool and are auth-gated (the engine's own `/datasets` ops surface is
unauthenticated); a dataset is updated by a task's `produces:`, never here.

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/datasets` | `?limit=` (default 100, max 500) → registry, newest-updated first: `[{uri, updated_at, last_run_id, last_task, updates, consumers[]}]`. `consumers` are the `on_datasets:` subscriber workflows a producer wakes (resolved in one extra query, not one per row) |
| GET | `/api/datasets/events` | the lineage ledger, newest first → `[{id, uri, workflow, run_id, task_id, task_name, source, at}]`. **`?uri=`** scopes the trail to one dataset (omitted = the whole ledger); **`?limit=`** default 100, max 500 |

### Workflows, schedules, GitOps

| Method | Path | Notes |
| --- | --- | --- |
| GET/POST | `/api/workflows` | list (enriched with schedule + recent-run digest) / create `{name?, spec, description?}` → `201`; `409` duplicate name. Each row carries `tags: []` — the spec's `tags:` labels, **parsed from the stored spec on read** so they always reflect the current definition. **`?tag=<t>`** returns only workflows carrying that tag |
| GET/PUT/DELETE | `/api/workflows/{id}` | read / update / delete; `404`, `409`. The read also returns the spec's `tags: []`. **PUT** first records the prior definition as a `workflow_versions` row, then overwrites the head. **DELETE** cascades to this workflow's schedules — prefer `state: retired` to stop it without losing them |
| POST | `/api/workflows/{id}/run` | optional `{parameters?: {k: v}}` → **`201`** `{run_id, workflow_id}`; `409` if the workflow is `paused` or `retired` (only `active` starts a run). The body is optional — a request with no `Content-Type` runs the stored spec as-is, exactly as before. **Status changed from `200` to `201`**: this route creates a run and now says so, like `POST /api/runs` and `/resubmit`. Clients that check for 2xx are unaffected; anything asserting `== 200` needs updating |
| GET | `/api/workflows/{id}/versions` | append-only definition history, newest first → `[{id, version, name, spec, created_at, created_by}]`. A version is recorded on create and on every `PUT`, so v1 is the original definition; `workflows.spec` remains the current head; `404` |
| POST | `/api/workflows/{id}/state` | `{state}`, one of `active` / `paused` / `retired` → `{id, state}`. Both non-active states refuse to run (enforced in the scheduler **and** `/run`) and **leave the schedules untouched** — the difference from delete; `retired` also hides from the default listing; `400` unknown state, `404` |
| GET | `/api/workflows/{id}/runs` | `?limit=&offset=` → this workflow's run history (same row shape as `/api/runs`). Runs are matched by definition **name** — the only linkage that exists (each run snapshots its own `workflow_definitions` row, so there is no FK to `workflows`); the list digest uses the same rule. Renaming a workflow therefore starts a fresh history; `404` |
| POST | `/api/workflows/{id}/sync-to-git` | open a PR with the spec → `{pr_url, branch, path}`; `501` until `GITHUB_TOKEN`+`GIT_REPO` are set; `502` on GitHub errors |
| GET/POST | `/api/git-repos` | list / connect `{url, branch?, path?, auto_sync, auth?}` → `201`; `400` empty or unusable URL, `409` duplicate. `url` may be `https://`, `ssh://[user@]host/…` or scp-style `git@host:owner/repo` (an https URL still may not embed credentials). The list also returns `worker_online` and `credentials_configured`. |
| DELETE | `/api/git-repos/{id}` | `204`; `404` |
| POST | `/api/git-repos/{id}/sync` | sync now → updated repo row |
| PUT/DELETE | `/api/git-repos/{id}/auth` | set/rotate or remove this repo's Git credential → updated repo row; `404`; `503` when secret encryption is unconfigured. Body: `{kind: "none"\|"token"\|"ssh", username?, token?, ssh_private_key?, known_hosts?}`. `token` requires an HTTPS URL, `ssh` an SSH one — the mismatch is a `400` rather than a credential that could never be used. **Write-only:** the secret is stored AES-256-GCM encrypted and is never returned; reads get `auth_kind`, `auth_username`, `auth_known_hosts` and a non-secret `auth_hint` (`••••cdef`, or `ssh-ed25519 SHA256:…` — the same fingerprint the forge shows). Passphrase-protected keys are rejected: the worker has no terminal to be prompted at. |
| GET/POST | `/api/schedules` | `?workflow_id=` / create `{workflow_id, cron_expr, enabled?, catchup?, catchup_window_secs?, catchup_max_runs?}`; `400` bad cron |
| PUT/DELETE | `/api/schedules/{id}` | update / delete; `404` |
| POST | `/api/schedules/{id}/backfill` | **synchronous** backfill: `{from, to, max_runs?}` → `{scheduled, skipped, from, to, run_ids}` (materialized in one call, hard cap 1000) |
| POST | `/api/backfills` | **paced** backfill *job* (AIP-78): `{schedule_id, from, to, max_runs?}` → `201` job row; the engine paces it (job cap 100k). `404` unknown schedule, `400` bad range/cron/spec or no fire-times |
| GET | `/api/backfills` | `?schedule_id=&limit=` → job list (`{id, schedule_id, status, requested, fired, cursor, …}`) |
| GET | `/api/backfills/{id}` | one job for monitoring (`fired`/`requested`/`status`); `404` |
| POST | `/api/backfills/{id}/cancel` | stop pacing a running job → `{id, cancelled}`; `404` unknown, `409` already finished |

### Artifacts (encrypted at rest — G-C2)

Programmatic artifact channel; bytes are envelope-encrypted at rest when a KEK
provider is configured (`DAGRON_ENV_KEK_PROVIDER`, see `CONFIG.md`). All routes
require an authenticated session; `503` when `DAGRON_ARTIFACT_DIR` is unset.

| Method | Path | Notes |
| --- | --- | --- |
| PUT | `/api/runs/{run_id}/artifacts/{task}/{name}` | stream the request body into the store (encrypted per-chunk when a KEK is set) → `201` + backend locator. Size-capped by `DAGRON_ARTIFACT_MAX_BYTES`. |
| GET | `/api/runs/{run_id}/artifacts/{task}/{name}` | stream the (decrypted) bytes → `200 application/octet-stream`; `404` if absent (a mid-stream decrypt/IO error aborts the connection) |
| GET | `/api/runs/{run_id}/artifacts/{task}/{name}/exists` | → `{exists: bool}` |
| POST | `/api/artifacts/rotate` | **admin group only.** Re-key every artifact from the previous KEK (`*_OLD` env) to the current one — rewraps the per-object data key, no payload re-encryption → `{rotated: N}`. `403` non-admin, `409` a rotation is already running, `400` no previous KEK configured, `503` artifacts off |

> **Operational caveat — quiesce writes during rotation.** The single-flight lock
> stops two rotations from overlapping, but it does **not** coordinate with normal
> artifact PUTs. Rotation rewraps each object with a read-then-write (`get` → rewrap
> → `put`); a PUT to the *same* key that lands between the `get` and the `put` is
> overwritten by the rewrapped older value (a silent lost update). Pause artifact
> writes (or rotate during a maintenance window) until a store-level compare-and-swap
> closes the window.

## 2. Engine ops API (`--features ops`, bound at `API_ADDR`)

**No authentication** — this surface is designed to stay cluster-private
(localhost / pod-internal). Never expose it publicly; that is what
`dagron-api` is for. Self-describing: OpenAPI 3 at `/openapi.yaml` /
`/openapi.json`, Swagger UI at `/docs`.

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/healthz` | `"ok"`, no DB — liveness only |
| GET | `/readyz` | `"ready"` after a datastore round trip inside the `DAGRON_READY_TIMEOUT_MS` budget (default 500 ms), else `503 datastore unreachable` / `503 datastore probe timed out` — the truthful readiness probe ([`LOW_LATENCY.md`](LOW_LATENCY.md) R-1) |
| GET | `/config` | effective configuration: every registered knob's value (secrets redacted), its source (`env` / `file` / `profile` / `default`), the `DAGRON_CONFIG` file and profile in use, and the fleet fingerprint — the HTTP face of `dagron config` ([`CONFIG.md`](CONFIG.md#configuration-file--profiles-dagron_config)) |
| GET | `/metrics` | **Prometheus text** — process counters (dispatched/succeeded/failed/retried, reconcile-tick histogram) + live DB gauges |
| GET | `/openapi.yaml` · `/openapi.json` · `/docs` | embedded spec + Swagger UI |
| GET | `/runs` | `?status=&limit=` (default 50, clamp 1–1000) |
| POST | `/runs` | **raw YAML body** (not JSON-wrapped) → `201 {run_id}`; `400` invalid DAG; **`429` + `Retry-After`** when `MAX_INFLIGHT_RUNS` is exceeded — the admission backpressure documented in [`ARCHITECTURE.md` §5.6](ARCHITECTURE.md#56-v4-queue-driven-ingestion--admission-backpressure). **`?wait=true`** (with `?timeout_secs=`) makes it a synchronous invocation: `200 {run_id, status, finished, result}` instead of `201` |
| GET | `/runs/{id}` | `{run, tasks}`; `404` |
| GET | `/runs/{id}/wait` | `?timeout_secs=` (default 30, clamp 1–600) long-poll to terminal → `{run_id, status, finished, result}`; `404`; timed-out wait is `200` with `finished: false` |
| GET | `/runs/{id}/logs` | **Workflow logs** — the whole run's output merged, attributed and filtered → `{run_id, tasks[], lines[], total, matched, truncated, eof, filtered, limit}`; `404`, `400` invalid filter. `?task=`/`?status=` scope which tasks are read; the [log filter](#log-filter) scopes which lines survive |
| GET | `/runs/{id}/tasks/{task_id}/logs` | one task's output for tailing → `{task_id, name, status, attempt, output, offset, next_offset, eof, total, matched, truncated, filtered, lines?}`; `404`, `400` invalid filter. **`?offset=`** returns only the output past that char offset — poll with `?offset=next_offset` until `eof`. The [log filter](#log-filter) applies within that slice |
| POST | `/runs/{id}/cancel` | `{run_id, cancelled: true}`; `409` if not cancellable |
| POST | `/runs/{id}/rerun` | optional `{from?}` → `{run_id, rerun}`; `404`/`409`/`400` |
| POST | `/runs/{id}/tasks/{task_id}/clear` | clear a completed task + its downstream cone → `{run_id, task_id, cleared}`; `404` unknown run/task, `409` task not completed |
| POST | `/runs/{id}/tasks/{task_id}/approve` · `/reject` | resolve a `type: approval` gate → `{run_id, task_id, resolution}`; `404`, `409` not awaiting approval |
| POST | `/runs/{id}/tasks/{task_id}/checkpoint` | a **running** task reports its committed checkpoint (`{uri, marker?}`, typically via its injected `DAGRON_RUN_ID`/`DAGRON_TASK_ID`); the pointer survives retries and the next attempt gets `DAGRON_RESUME_FROM` ([AI_WORKLOADS.md](AI_WORKLOADS.md)) → `{run_id, task_id, checkpoint_uri, marker}`; `400` empty uri, `404`, `409` task not running |
| GET | `/dead-letters` | `{dead_letters: […]}` |
| POST | `/dead-letters/{id}/redrive` | `{run_id, redriven_from}`; `404`/`400` |
| DELETE | `/dead-letters/{id}` | `{id, deleted: true}`; `404` |
| GET | `/datasets` | Dataset registry (data-aware scheduling, [DATASETS.md](DATASETS.md)): `?limit=` (default 100, clamp 1–1000) → `{datasets: [{uri, updated_at, last_run_id, last_task, updates}]}`, most recently updated first |
| GET | `/datasets/events` | Lineage ledger — which run/task updated which dataset when: `?uri=` (narrow to one dataset) `&limit=` (default 100, clamp 1–1000) → `{events: [{id, uri, workflow, run_id, task_name, source, at}]}`, newest first. `source` is `task` (a `produces:` success) or `external`. `id` is the monotonic cursor sensors/triggers key off |
| POST | `/datasets/events` | Record an **external** dataset update (a producer outside dagron — CDC, object-store notification): `{uri}` → `{recorded}`. Wakes dataset sensors and fires `on_datasets:` triggers. **dagron Enterprise**; the open build returns `403` with a signpost (its datasets update via `produces:` tasks). `400` invalid URI |

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
