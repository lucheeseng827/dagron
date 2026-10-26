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

**Auth:** every route below except `/healthz`, `/readyz`, `POST /api/login`,
`POST /api/logout` and `GET /api/badges/{name}` (a public SVG status label —
see [Workflows](#workflows-schedules-gitops)) requires a valid HS256 session
JWT, accepted either as the
HttpOnly `dagron_session` cookie (browsers) or `Authorization: Bearer <jwt>`
(API clients). Automation may instead present a personal access token (`dgp_`
prefix) in the same `Authorization: Bearer` header; it resolves to its owner's
live permissions. Token *management* (`/api/tokens*`) is the one exception — it
requires a password session and refuses a `dgp_` bearer with `403`, so a leaked
token cannot mint its own replacements. Missing/invalid/expired → `401`
(`src/auth.rs`).
`POST /api/users`, `GET /api/users`, all three
`/api/settings/notifications*` routes and `PUT /api/settings/dead-letters`
additionally require the `admin` group (`403` otherwise) — notification defaults
hold secret webhook URLs and reroute every run's notifications, the test route
makes the server POST outbound, and the dead-letter retry count is instance-wide
policy. Reading the dead-letter setting needs only a session.
So do `POST`/`DELETE /api/git-repos` and `PUT`/`DELETE /api/git-repos/{id}/auth`:
a connected repository writes and (with `prune`) retires workflows, and its
credential is a stored secret the caller can never read back. Listing the
repositories and `POST /api/git-repos/{id}/sync` stay open to any session — the
first is what the console renders, the second only hurries along the poll
`auto_sync` already performs.
`DELETE /api/workflows/{id}` is admin-only as well: it is the one irreversible
control action, and it cascades to the workflow's schedules. Creating and editing
a workflow stay open (a bad edit is recoverable from `/versions`), and so does
`POST /api/workflows/{id}/state` — `retired` stops a workflow *and keeps its
schedules*, which is the thing an operator reaching for delete usually wanted.
A session in the `viewer` group is refused **every** mutation — `403
{"error": "viewer role is read-only"}` — by middleware, before the request
reaches a handler, in both the open and the enterprise build. `GET` is untouched,
and so are `POST /api/login` and `/api/logout`: a viewer who cannot sign in
cannot read either. The enterprise build additionally records what it let
through, in its audit trail. A user with *no* groups is the console's "operator"
and is not read-only — authoring workflows is that role's purpose.

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
| POST | `/api/tokens` | mint a personal access token `{name, expires_in_days?, agent?}` → `201 {id, name, prefix, token, expires_at, agent?}`; the `token` plaintext is returned **only here** (storage keeps `sha256` only). With `agent` (1 to 64 of `a-z`, `0-9`, `-`), an agent's token: it presents as `agent:<agent>`, with the caller's groups less `admin` and `ci:trigger`, reads, and reruns or cancels only the runs whose trigger's actor is `agent:<agent>`; any other control action is a `403`. An agent's name is its first owner's for good | `403` presented an API token, `400` empty name / `expires_in_days` ≤ 0 or out of range / not an agent's name, `409` the agent's name is another person's |
| GET | `/api/tokens` | → the caller's own tokens `[{id, name, prefix, created_at, expires_at, last_used_at, revoked_at, agent}]` (no hashes, no plaintext) | `403` presented an API token |
| DELETE | `/api/tokens/{id}` | revoke → `204` (idempotent — revoking twice is not an error) | `403` presented an API token, `404` unknown / not the caller's |
| GET | `/readyz` | **readiness, no auth**: `200 ready` only when a pooled DB round trip answers inside `DAGRON_READY_TIMEOUT_MS` (default 500 ms); otherwise `503` with the reason. The SSE listener's state is **advisory**: while it is resubscribing the body reads `ready (event listener degraded)` but stays `200` — its failure is fleet-correlated (one shared `DATABASE_LISTEN_URL`), so gating on it would empty the Service rather than reroute; `DAGRON_READY_REQUIRE_LISTENER=1` opts into strict `503` gating. Point the orchestrator's readinessProbe here — `/healthz` stays the bare liveness `200 ok` ([`LOW_LATENCY.md`](LOW_LATENCY.md) R-1) | — |
| GET | `/api/health` | rich health for the UI status widget: `{api, edition, config_fingerprint, event_listener, log_tails, db, scheduler_leader, leader_holder, active_runs, awaiting_approvals, dead_letters}` — `log_tails` is how many runs this replica's live log relay follows now (one however many watch a run); — `config_fingerprint` is the fleet-drift hash ([`CONFIG.md`](CONFIG.md#configuration-file--profiles-dagron_config)); `event_listener` (`ok`/`down`) is where a degraded live-event bridge is visible. Never 500s — a DB outage answers `db: "error"` | `401` |
| GET | `/api/search` | `?q=&limit=` (limit per category, default 8, max 20) → `{query, workflows[], runs[], schedules[]}`. Capped + parameterized (run ids match by prefix, names by substring; LIKE wildcards escaped) — the ⌘K palette backend | `401` |
| GET/POST | `/api/environments` | list / create `{name, description?, variables{}}` → environments with `variables` + `secret_names` (values are **write-only**, never returned) + `policy` (`null` where there is none) | `409` duplicate, `400` bad name |
| PUT/DELETE | `/api/environments/{id}` | update description/variables (name is immutable — specs reference it) / delete incl. secrets | `404` |
| PUT | `/api/environments/{id}/secrets/{name}` | `{value}` → `204`; encrypted immediately (AES-256-GCM, `DAGRON_ENV_SECRET_KEY`) | `503` no key configured, `404`, `400` |
| DELETE | `/api/environments/{id}/secrets/{name}` | → `204` | `404` |
| PUT/DELETE | `/api/environments/{id}/policy` | which runs may use the environment: `{allowed_trust[], allowed_refs[]?, allowed_pipelines[]?, allowed_identity[]?, approvers[]?}`, the whole policy (`allowed_identity: ["agent"]`: an agent's task in it may hold an identity; `approvers`: a run that is not trusted waits on a gate of these before a task of it uses it; HOWTO §6), → the environment / → `204`, back to no policy, which admits `trusted` runs alone. **Admin only.** A run that names an environment which does not admit it is refused when it is submitted (`403`, with the reason), and its tasks are refused their secrets at dispatch (HOWTO §6) | `403` not an admin, `400` a class that is not one or a list that is set and empty, `404` |
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
| GET | `/api/runs` | `?status=&name=&trigger=&repo=&event=&limit=&offset=` (limit default 100, max 500) → `[{id, definition_id, status, created_at, finished_at, name, trigger_kind, clock_confidence}]`. `name` filters by workflow, `trigger` by kind: `manual`, `api`, `schedule`, `backfill`, `dataset`, `workflow`, `source` or `ci`. `trigger_kind` is the kind of the run's trigger record, which every run is made with (`trigger`, on `GET /api/runs/{id}`). A run from before every run had one has its kind decided when it is read, as it was: a backfill ledger row, a schedule stamp, and `manual` for a run with neither. **`?trigger=ci`** is how a list is narrowed to the runs a CI service submitted. **`?repo=<host/owner/name>`** narrows it to the runs whose trigger record names that repository: two repositories may each have a pipeline of one name, and `name` alone returns both's runs. **`?event=<id>`** narrows it to the runs of one event at its source, by the `event_id` their trigger records carry: a CI event's runs, one for each pipeline it started |
| GET | `/api/runs/sources` | `?limit=` (default 100, max 500) → `[{repo, runs, last_run_at}]`: the repositories that runs' trigger records name, as `host/owner/name`, the one with the latest run first. A run that names no repository is under none. What a repository is listed for is what ran for it: it is no list of what a CI service is connected to |
| GET | `/api/runs/refs` | `?repo=<host/owner/name>&limit=` (default 100, max 500) → `[{ref, pr, sha, actor, event_id, at, runs: [{id, name, status, created_at, finished_at, stopping_as}]}]`: a repository's refs, each with the runs for its **latest commit**: the commit of the latest run on the ref, and the latest run of each workflow for that commit, by name, whichever event each came of (a push's runs, and a later one asked for by hand, are one row). `sha`, `actor` and `event_id` are the latest run's. The ref with the latest run is first. `400` without a `repo`; `[]` for one no run names. A run that was run again is the same run, and reads as it is now |
| POST | `/api/runs` | `{yaml: "<workflow YAML>", parameters?: {k: v}}` → `201 {run_id}`; `400` on invalid YAML / cycles / duplicate or unknown task names, or a `parameters` value the workflow's `param_schema` refuses (`parameter 'environment' must be one of: dev, staging, prod`); `429` at the workflow's `max_active_runs` cap, per `concurrency_key` when it sets one (validated before anything is persisted). A workflow that says a run waits (`when_full: queue`, or `supersede`) answers `201` there instead: the run is made and waits for a turn, reading `running` with `queued_at` set and no `admitted_at`; with `supersede` the runs of its key before it are cancelled. Such a workflow answers `429` only where as many runs wait as may (`DAGRON_MAX_WAITING_RUNS`). `parameters` override the spec's declared `parameters:` defaults; keys the spec never references are ignored. **Optional `Idempotency-Key:` header** — a repeat of the same submit returns the *same* `run_id` with `200` instead of creating a second run, so a retrying client cannot double-submit. `409` if the key was already used for a different body, or if an identical submit is still in flight (retry). An explicitly supplied *empty* `Idempotency-Key` is a `400` (`must not be empty`), so an absent header and an invalid empty one are distinguishable. Keys are printable ASCII, ≤255 chars, scoped to the authenticated caller, and live exactly as long as the run they name. A key is pointed at its run by the transaction that creates the run, so there is never a run made under a key that the key does not name: a submit that dies, or is slower than the five minutes its claim is held for, leaves no run behind for a retry to duplicate. **Optional `trigger`**: what started the run, from a service that submits runs for events — `{kind: "ci", actor?, trust?, forge_host?, repo?, ref?, sha?, base_sha?, pr?, event_id?}`. It is kept with the run (written in the same transaction, never updated), makes the run's `trigger_kind` read `ci`, and is returned as `trigger` on the run's detail with `principal`, which the API sets from the caller it authenticated. **Only a caller in the `ci:trigger` group may send one**: from anyone else the submit is refused with `403` and no run is made, since a list narrowed to CI's runs believes the record. `422` for a `kind` other than `ci`, a `trust` other than `trusted`/`untrusted`/`agent`, or a text field over 512 bytes or with a control character. Give the group to the service's own user (`POST /api/users` with `groups: ["ci:trigger"]`), not to a person. The record is no part of an `Idempotency-Key`'s request: a replay returns the first run, whose record stays the first submit's. **A submit with no `trigger` is recorded as its caller's own**: `{kind: "manual", principal: "user:<caller>", actor: "<caller>"}`, the caller named by email, else by subject |
| GET | `/api/runs/{id}` | run detail (`{…, name, trigger_kind, trigger, failure}`) + `tasks[]` (`{id, name, status, attempt, output, scheduled_at, finished_at, fault_class}`, `fault_class` being what broke a failed attempt as the engine classified it, `null` where nothing did); `404`. **`trigger`** is the run's trigger record: what started it, written in the transaction that made the run and never updated → `{kind, principal, actor, trust, source, forge_host, repo, ref, sha, base_sha, pr, event_id}`, or `null` for a run from before every run had one. `principal` is what submitted the run and `actor` who caused it; `source` is what it came from where that is no repository, and `event_id` the event's id at its source. By `kind`: `manual` from `user:<caller>` (a submit; a resubmit, whose `source` is `resubmit:<run>`; a dead letter's redrive, `dead-letter:<id>`); `ci` from `ci:<service>`, with the repository, ref and commit the service gave; `api` from `ops-api` (the engine's own API, which has no login) or from `plan` (a plan's submission); and from `engine`, for a run the engine started itself: `schedule` (`schedule:<id>` or `cron:<name>`, the fire's time), `backfill` (`schedule:<id>` or `backfill:<job>`, the date; from `user:<caller>` where a person asked for it through `POST /api/schedules/{id}/backfill`), `dataset` (`dataset:<uri>`), `source` (`source:<name>`, the position) and `workflow` (`run:<parent run>/<task>`). A `workflow` run is one a parent's `type: workflow` task started: it keeps the parent's `actor`, `trust`, repository, commit and `event_id`, so that what is read of a run's origin reads the same for a sub-workflow. **`failure`** summarises why the run broke without a second call — `{task_id, task_name, attempt, failed_tasks, message, truncated, fault_class, disposition}`, or `null` when nothing failed. `fault_class` is what broke the task's last attempt, as the engine read it from what the task printed (`user-code`, `storage`, `preemption`, …), and `disposition` what another attempt is worth given that class: `infrastructure` (the same code elsewhere likely succeeds), `application` (another attempt reproduces it), `platform` (it was taken away) or `unknown`; both `null` where nothing classified it. It names the *earliest-finished* failed task (the likeliest cause rather than a downstream casualty) and `failed_tasks` says how many there were; `message` is the tail of that task's output, clipped to 20 lines / 4 KB with `truncated: true`. A run failed with no failed task — a `run_timeout_secs` overrun cancels its tasks — reports the run's own reason with `task_id: null`. Present as soon as a task fails, so a still-`running` run can carry one |
| GET | `/api/runs/{id}/wait` | `?timeout_secs=` (default 30, clamp 1–600) long-poll to terminal → `{run_id, status, finished, result, failure, whole_reruns}` (`result` = the `result_from` task's output on success; `whole_reruns` = how many times the run was rerun whole, `from: all`, by which a reader that counts runs tells a rerun of every job from a rerun of the failed ones); `404`; a timed-out wait is `200` with `finished: false`. **`failure`** is the same object `GET /api/runs/{id}` returns, present only when the wait ended on a failure — a synchronous caller gets the reason in the response it is already holding, with no second call. The task rows behind it are read only when there is a failure, so a succeeding wait costs nothing extra |
| GET | `/api/runs/{id}/spec` | the stored, un-expanded DAG YAML this run was created from → `{yaml, name}`; `404` unknown run or definition. Backs the console's "re-run with changes" editor, and is the only way a caller that submitted ad-hoc YAML can recover what it ran |
| GET | `/api/runs/specs?ids=a,b,c` | the same YAML for **many runs in one call**, grouped by content → `[{yaml, name, run_ids}]`. Every run snapshots its own definition row, so N runs of a workflow nobody edited are N identical specs under N ids — identical specs collapse into one entry listing the runs that used it, which is both the saving and the answer to "did this change?". Groups and `run_ids` follow the requested order (`= ANY` has none). Unknown ids are **absent, not a 404**, so a page containing an archived run still answers for the rest; `400` for no ids or more than 200 |
| GET | `/api/runs/{id}/graph` | DAG as `{nodes[], edges[]}` for the UI graph view |
| GET | `/api/runs/{id}/logs` | **Workflow logs** — every task's output merged into one attributed stream, filtered server-side → `{run_id, tasks[], lines[], total, matched, truncated, eof, filtered, limit}`; `404` unknown run, `400` invalid filter. `?task=`/`?status=` narrow which tasks are read (name or id, repeatable or CSV); the [log filter](#log-filter) then narrows which lines survive. `total`/`matched` are counted **before** the line cap, so a truncated view always says how much it hid |
| GET | `/api/runs/{id}/tasks/{tid}/logs` | `{task_id, name, status, attempt, earlier_attempts, output, offset, next_offset, cursor, eof, total, matched, truncated, filtered, lines?}`; `404`, `400` invalid filter. `cursor` is where this read of the whole log ends, for a stream to go on from (`…/logs/stream?cursor=`, below). `attempt` counts the task's attempts through its life (a rerun or a clear does not set it back), and `earlier_attempts` is how many of them are behind the one shown. `output` here is the task's **log**: stdout and stderr in arrival order, whether the task succeeded or failed (the task's value on the run detail stays stdout, so `when:` never sees a stderr warning); rows written before the log column fall back to the stored output. **`?offset=`** returns only the output past that char offset for live tailing — poll with `?offset=next_offset` until `eof`. The [log filter](#log-filter) applies *within* that slice while `next_offset` keeps counting the raw text, so filtering and tailing compose; `lines` is present only when a filter was asked for. A running task's log is written once a second at most, and a long one less often (a second more for each MiB an append rewrites, up to 30 s), so a tail is about that far behind the task. A task that ended by itself has its whole log by the time it reads as ended; one that was cancelled can lack what it wrote as it was stopped, which its executor had not read yet; one whose engine stopped, or whose claim another took, can lack what waited to be written: its last second, or a long log's longer wait (up to 30 s). A log is kept up to `DAGRON_LOG_TASK_CAP_BYTES` ([`CONFIG.md`](CONFIG.md)) as the task writes it, and one that reached it is held there, on a line that says so, until the attempt is over: it is then its beginning, that line, a line that says how much of what the task wrote was left out (`[dagron] … of this attempt's output (N bytes and M lines, counted before masking) is left out here; what follows is its end`, in the form [`LOG_PIPELINE.md`](LOG_PIPELINE.md) §7.4 gives), and its end, the last `DAGRON_LOG_TAIL_BYTES`. `offset`, `next_offset`, `total`, `matched` and a filter's `n` count the log as it is given, those lines among it; the line between says how many of the task's own lines are not in it |
| GET | `/api/runs/{id}/logs/stream` | **A run's task logs followed live** (SSE; [`LOG_PIPELINE.md`](LOG_PIPELINE.md) §7.3). Each task's log is read from where the stream begins to where it is, and then sent as it is written, with no seam between the two: no line is lost or sent twice. Where it begins: a task's `cursor=<task_id>:<fence>:<gen>:<byte>` (repeatable or CSV, at most 1000; a cursor of a task not in the run is passed over), else `from=start`, `from=end`, or by default the last 256 KiB of the log from a line's start; `tail=1&limit=N` begins at the last `N` lines of that (that match). `task=` (name or id) and `status=` narrow it as the run view (at most 1000 of each); a task `status=` selected once is followed to its end. The [log filter](#log-filter)'s line predicates (`q`, `exclude`, `regex`, `level`, `case`) apply per line; `context`, `limit` without `tail`, `offset` and any other key are `400`. Events, each with JSON data: `start {task_id, task, attempt, cursor, from, size, why}` before a task's first text and at each writing after (`why`: `open`, `attempt` for another attempt, `rewritten` for a log written again from its start, masked; a client replaces what it holds of the task unless the cursor is the one it holds); `log {…, cursor, text}`, or with a filter `{…, lines: [{level, ts?, text}]}`; `end {…, status, complete}` once the attempt is over and sent (`complete: false` where its row was claimed again before all of it could be read); `eof {run_id}` once every followed task is over, with `status=` every task of the run. The stream ends after `eof` alone: any other end is a drop, resumed by cursor. `404` unknown run; `429` with `Retry-After` when the replica follows `DAGRON_LOG_MAX_TAILS` runs already (a run whose followed tasks are all over is not refused for it), or serves `DAGRON_LOG_MAX_STREAMS` streams; keep-alive every 15 s; the stream ends (without `eof`) after 10 minutes at most, at the session's expiry, when its client has taken nothing of it for 60 s, and at shutdown |
| GET | `/api/runs/{id}/tasks/{tid}/logs/stream` | One task's log followed live, as the run's above: every `start`, `log` and `end` carries its cursor as its SSE `id`, and `Last-Event-ID` is taken as a cursor (over a `cursor=` for the task), so an EventSource goes on from where it was by itself. `404` "task not found in this run" |
| GET | `/api/runs/{id}/tasks/{tid}/attempts` | `{task_id, name, current_attempt, attempts[], evicted, filtered}`; `404`, `400` invalid filter. The **iterations the log view can't show**: `task_runs.output` is one column every attempt overwrites, so the row above returns the last pass of a `repeat:` loop and the attempt a retried task finished on. Each entry is `{attempt, reason, status?, scheduled_at?, output, retention_truncated, finished_at, total, matched, truncated, lines?}`, oldest first. `reason` is `iteration` or `failed` for an attempt a worker's own retry superseded, and `rerun`, `retry` or `clear` for one a person replaced: `status` then says how it had ended (`failed`, `cancelled`, `succeeded`, `skipped`), and `scheduled_at` and `finished_at` when. Attempts are numbered through the task's life, so a rerun's first attempt follows the last one before it. `current_attempt` is the one on the row and is deliberately **not** in the list; for a task that was put back and has not started again it is the number its next attempt will have. `evicted` means earlier attempts aged out of `DAGRON_ATTEMPT_LOG_KEEP`. Separate from the row above because that one polls — see [docs/ITERATION-LOGS.md](ITERATION-LOGS.md) |
| GET | `/api/runs/{id}/stream` | **SSE**: one shared Postgres `LISTEN task_events` fans out to per-run streams; each event is JSON; on broadcast lag the client receives `event: resync` / `data: lagged` and should refetch |
| POST | `/api/outbox/ci/claim` | **The engine's outbox, handed to a service** (dagron-ci's reporter): `{limit?, lease_secs?, wait_secs?}` → `{events: [{id, run_id, type, payload, created_at}]}`, in the order they are due, which is the order written for events handed out the first time (one handed out again after its lease comes after newer ones): `task.started`, `task.waiting` (on an approval) and `task.finished` (`{run_id, task_id, name, status, attempt}`), `run.completed`, `run.rerun`, `run.auto_rerun`. Up to `limit` (200; at most 500), leased for `lease_secs` (60; 10 to 300); `wait_secs` (at most 30) holds the claim until there are some. A user in `ci:trigger` alone (`403` for anyone else); each has a sink of its own, by its identity (two deployments sharing one split its events), and is handed the events of the runs it submitted and of no other's. A task's events are written only while such a sink claims, from its first claim to a day after its last, and are deleted a day after they were written |
| POST | `/api/outbox/ci/ack` | `{ids}` → `204`: those events are done with; one not acknowledged is handed out again once its lease is up |
| GET | `/api/events/stream` | **SSE**: account-wide activity — every run's task events (`{run_id}`), unfiltered, off the same shared listener; feeds the UI list pages' live-updates mode; same `resync` contract on lag |
| POST | `/api/runs/{id}/cancel` | → `{cancelled: n, stopping: m}`: the tasks called off, and how many of them a worker still holds. **A cancel asks**: those tasks read `running` with `stop_requested_at` set, and the run `running` with `stopping_as: "cancelled"`, until their container, pod or process is gone; the run ends `cancelled` then, and holds its concurrency key until it has. `m = 0`: it ended at once. `404` |
| POST | `/api/runs/{id}/rerun` | optional `{from?}` → `{run_id, rerun}`: every failed, cancelled and rule-skipped task is put back, and `rerun` is how many. `from` is `failed`, the default, or `all`: the run whole, every task of a run that succeeded, failed or was cancelled, what succeeded included, each put back as a clear puts one back (its checkpoint dropped). A runtime fan-out's instances are deleted and made again from its producer's next output: their kept attempts and logs go with them. An approval gate asks again, from nothing: the decision it asked for last time is kept as its attempt. `404`/`409`/`400`; `409` too for `all` while a task of the run still owes the teardown of a remote job it left running ([EXTERNAL_JOBS.md](EXTERNAL_JOBS.md)), with nothing written, until the engine has torn it down; `429` when the run's `max_active_runs` is reached for its concurrency key, as a new run of the key would be refused. Like retry and clear below, it is the engine's own implementation: the attempts it replaces are kept (`…/attempts`), a task put back is parked again from nothing (what parked it last time, a child run, a time or a dataset, goes with its result), and a `run.rerun` event (`{run_id, by, reset_tasks, task_id?, from?}`, `from` being `all` for a rerun of the run whole) is written to the outbox with it. A run that had ended is held to its `run_timeout_secs` and its `deadline` from when it runs again, and its SLA alert may fire again. The caller is recorded on each task put back (`reset_by`): a `not_triggerer` gate it puts back takes no approval from them |
| POST | `/api/runs/{id}/resubmit` | → `201 {run_id}`: a fresh run from the same spec, created with the parameters this run was created with. A declared `environment:` is read again, as for any new run. The run is its caller's (`trigger.kind` is `manual`), but for its class: a resubmit of a run that was not trusted is a run of that class, for the same repository and commit, whoever asks. `400` or `403` as a submit of that spec and those parameters by a run of that class would answer; `404` |
| POST | `/api/runs/{id}/tasks/{tid}/retry` | → `{retried}`: one failed or cancelled task is put back, and its run set running again if it had ended, failed or cancelled. `404`/`409`; `429` as for a rerun, when the run had ended. The caller is recorded on the task, and its run's deadlines counted again, as for a rerun |
| POST | `/api/runs/{id}/tasks/{tid}/clear` | clear a completed task + its downstream cone (Airflow "Clear, Downstream") → `{run_id, task_id, cleared}`; `404` unknown run/task, `409` task not completed, `429` as for a rerun, when the run had ended. The caller is recorded on each task, and its run's deadlines counted again, as for a rerun |
| POST | `/api/runs/{id}/tasks/{tid}/approve` | approve a `type: approval` gate → the task succeeds, DAG proceeds → `{run_id, task_id, resolution, decided_by, comment?}`; optional body `{comment}` (≤2000 chars, `400` over) is recorded with the authenticated user as the decider; `403` when the gate lists `approvers` and the caller is not among them, or the gate is `not_triggerer` and the caller caused the run: the caller's email or subject is the `actor` of the run's trigger record (who submitted it; whoever a CI service says asked for it, and not the service; its parent's, for a sub-workflow's run). A record with no `actor` (a schedule's fire) bars nobody, and a run from before the record is its submitter's. A `not_triggerer` gate also bars whoever put it back to ask again, by a rerun, a retry or a clear through this API (`reset_by`); a gate with `binds` also needs `digests` in the body (`{"<task>/<name>": "<sha256>"}`, as listed by `GET /api/approvals`): `400` without them, `409` when a bound artifact is missing or has changed since the digest was taken (the message carries the current one), `503` without an artifact store; on success the reviewed digests are written to the artifact `<gate>/approved.sha256` for downstream tasks to verify; the decision is also written to the gate's log (`approved by <who> at <time>`, then `comment: <text>` when there is one), so it reads in `GET /api/runs/{id}/logs` beside the tasks around it; `output` stays the bare `approved`/`rejected` that `when:` reads; `404`, `409` not awaiting approval |
| POST | `/api/runs/{id}/tasks/{tid}/reject` | reject a gate → the task fails, `all_success` downstream skips; the log line reads `rejected by <who> at <time>` plus the optional comment, as for approve; `403` when the caller is not among the gate's `approvers` (the triggerer may still reject); `404`, `409` |
| POST | `/api/runs/{id}/triage` | record what a *human* decided about a failed run: `{state, note?}` → `{run_id, triage_state, triage_note, triaged_at, triaged_by}`; `400` unknown state, `404`. `state` is `acknowledged` (seen, still open), `resolved` (dealt with) or `ignored` (a real failure we accept) — three states rather than one flag, because "we have decided not to care" is not the same answer as "fixed". Re-triaging overwrites; the note is what is worth reading in three months |
| DELETE | `/api/runs/{id}/triage` | clear the decision, putting the run back in the attention queue → the same object with nulls; `404` |

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
engine (a bucket needs the api's matching `archive-*` cargo feature, which
the published image is built with).

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/archive/runs` | `?name=&limit=&offset=` (limit default 100, max 500), newest-finished-first → `[{run_id, name, status, created_at, finished_at, archived_at, compacted_at, parquet_path}]` |
| GET | `/api/archive/runs/{id}` | the full archive document (`{format, run, tasks[], outbox_events[], archived: true, index}`); `404` not in the index; **`410`** compacted to Parquet (body carries `parquet_path` — query the analytics dataset instead); `502` sink unreachable/unconfigured |
| POST | `/api/runs/{id}/archive` | **admin-only, destructive.** Archive one terminal run *now* instead of waiting for the retention window: export the document, verify it landed, index it, purge the run from the hot store → `{run_id, archived: true, purged}`. The run leaves `GET /api/runs` and appears under `/api/archive/runs`. `403` not an admin; `404` not in the hot store (already archived, or never existed); **`409`** the run is not terminal — archiving a live run would purge state the scheduler is still driving; **`501`** no sink configured on dagron-api (without one there is nothing to archive *to*, and the purge would be a delete wearing a kinder word); `502` the sink or the index write failed — the run **stays** in the hot store. Same fail-closed order as the GC sweep: write, index, then purge |

### Observability & dead letters

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/metrics` | JSON: `{runs_by_status[], tasks_by_status[], dead_letters, history_age_secs}` (the Prometheus text endpoint lives on the engine, not here). Cached in two tiers, each on its own: the counts of what has not finished, and the dead letters, for `DAGRON_METRICS_CACHE_SECS` (2 s), and those of the finished statuses for `DAGRON_METRICS_HISTORY_CACHE_SECS` (300 s, or 0 where it is not set and the first is 0); `history_age_secs` is how old the latter are. Only these are read, three counts a tier at most. The first tier is read one read at a time on a replica, the calls that come meanwhile waiting for it, and a failed read, or one given up after 30 s, answers `500` and is never cached; a call that finds the second stale is answered with the last ones, and they are read again behind it, given up after their TTL or 60 s, whichever is longer |
| GET | `/api/metrics/timeseries` | `?days=` (default 14, clamp 1–90) `&name=` → per-day buckets `[{day, succeeded, failed, cancelled, active, avg_duration_secs, max_duration_secs}]` for the Metrics charts / workflow trend |
| GET | `/api/approvals` | every task parked in `awaiting_approval`, oldest first → `[{run_id, task_id, task_name, workflow_name, since, message, show: [{path, url, bound, sha256}], approvers, not_triggerer, can_approve, can_reject}]` (the human-in-the-loop worklist; `message` and `show` are the gate's `approval_message` and `approval_show`, `url` an artifact download link, `bound` marks the artifacts pinned by `binds` and `sha256` is their current digest to send back as `digests`; `approvers`, `not_triggerer` and the caller's own `can_approve` / `can_reject` say who may decide) |
| GET | `/api/dead-letters` | `?limit=` (default 100, max 500) → `[{id, payload, error, source, failures, first_seen_at, last_error_at}]` |
| POST | `/api/dead-letters/{id}/redrive` | → `{run_id, redriven_from}`. **All or nothing:** the run is created and the dead letter deleted in one transaction, so a redrive that fails leaves the row exactly as it was (same `id`, `payload`, `error`, `source`, `failures`, `first_seen_at`, `last_error_at`), and the same `id` can be redriven once the cause is fixed: `400` the payload is not a valid DAG or names an unknown environment, `429` the workflow is at its `max_active_runs` cap, `500` a datastore error. Nothing is re-parked under a new id. `404` no such dead letter: already redriven, discarded, or never parked. Concurrent redrives of one `id` make at most one run, and the others get `404`. Success deletes the row and its failure history; `redriven_from` links the run back to it |
| DELETE | `/api/dead-letters/{id}` | → `204`; `404` |
| GET | `/api/settings/dead-letters` | the instance dead-letter policy → `{max_attempts}`, or `null` when unset (the engine then keeps using its `DEAD_LETTER_MAX_ATTEMPTS` env value) |
| PUT | `/api/settings/dead-letters` | `{max_attempts}` → the stored object; `403` non-admin, `400` `max_attempts` < 1. Takes effect on the next ingestion failure — the engine reads it on the failure path rather than caching it at startup, so there is no restart and no window where the console disagrees with what is running. Only the retry count is settable here: `STREAM_DLQ_PATH` is a path on the engine's own host and stays deployment configuration |

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
| GET/POST | `/api/workflows` | list (enriched with schedule + recent-run digest) / create `{name?, spec, description?}` → `201`; `409` duplicate name. Each row carries `tags: []` — the spec's `tags:` labels, **parsed from the stored spec on read** so they always reflect the current definition. **`?tag=<t>`** returns only workflows carrying that tag. **`?include=ci`** lists, after the stored workflows, the pipelines a CI service compiled and submitted inline: one row for each repository and pipeline name, from the runs whose trigger record says `ci`, with `source: "ci"`, the repository as `description`, the tags of its newest run's definition (the compiler tags each `ci`) and the same run digest. Such a row is **read-only**: its `id` (`ci:<forge>/<repo>:<name>`) is no workflow's, so `/api/workflows/{id}` and its sub-routes answer `404` for it. Its runs are `GET /api/runs?trigger=ci&name=<name>&repo=<its description>`. Without `include=ci` the list is what it always was, so a client that opens every row need not change. `?include=ci&tag=ci` is every pipeline CI compiled |
| GET/PUT/DELETE | `/api/workflows/{id}` | read / update / delete; `404`, `409` (a name clash on PUT, **or** the workflow is git-managed, see the end of this row). **DELETE is admin-only** (`403` otherwise, and the message names `state: retired`) — it is irreversible and cascades to the schedules; read and PUT are open to any session. The read (and the create/update responses) also return the spec's `tags: []`, `parameters: {name: default}` and `param_schema: {name: {required?, enum?, pattern?, description?}}`, so a client can build a run form without parsing YAML. **PUT** first records the prior definition as a `workflow_versions` row, then overwrites the head. **DELETE** cascades to this workflow's schedules — prefer `state: retired` to stop it without losing them. **Git-managed workflows** (synced from a connected repo by `dagron-gitops`): **PUT** and **DELETE** answer `409 {error, repo, sync_to_git}` (the repo URL and `POST /api/workflows/{id}/sync-to-git`) so the change goes through a pull request; an `admin` may pass `?force=true`, and the forced edit counts as the repo's `drift` until the next sync puts git's definition back (the edit stays in `/versions`). `POST /state` and `/run` are unaffected |
| POST | `/api/workflows/bundle` | Apply a **signed workflow bundle** — `{manifest_b64, signature_b64, files: [{path, content_b64}]}` → `200 {bundle, version, digest, provenance, applied: [{id, name, version}]}`. Verification fails closed: `501` when `DAGRON_BUNDLE_PUBKEYS` is unset (there is no unsigned path), `400` on a bad signature, an unlisted or extra file, a hash mismatch, or a spec that does not validate — nothing is persisted unless the whole bundle is good. Applied in one transaction, each spec recording a `workflow_versions` row stamped with the bundle's provenance. An optional `cohort` field (staged rollout) is **not in this build**; this build answers `400` with a signpost and applies the bundle to this deployment immediately. See [docs/BUNDLES.md](BUNDLES.md) |
| POST | `/api/workflows/{id}/run` | optional `{parameters?: {k: v}, trigger?: {…}}` (`trigger` as on `POST /api/runs`, from a member of `ci:trigger` only) → **`201`** `{run_id, workflow_id}`; `409` if the workflow is `paused` or `retired` (only `active` starts a run). Optional `Idempotency-Key`, as on `POST /api/runs`: a repeat with the same workflow and parameters answers **`200`** with the first run, the same key with other parameters is a `409`. The body is optional — a request with no `Content-Type` runs the stored spec as-is, exactly as before. **Status changed from `200` to `201`**: this route creates a run and now says so, like `POST /api/runs` and `/resubmit`. Clients that check for 2xx are unaffected; anything asserting `== 200` needs updating |
| GET | `/api/workflows/{id}/versions` | append-only definition history, newest first → `[{id, version, name, spec, created_at, created_by}]`. A version is recorded on create, on every `PUT`, and on every **git sync that changes the spec** (`created_by = git:<repo>@<rev>`; a signed bundle's is `bundle:…`), so v1 is the original definition; `workflows.spec` remains the current head; `404` |
| POST | `/api/workflows/{id}/state` | `{state}`, one of `active` / `paused` / `retired` → `{id, state}`. Both non-active states refuse to run (enforced in the scheduler **and** `/run`) and **leave the schedules untouched** — the difference from delete; `retired` also hides from the default listing; `400` unknown state, `404` |
| GET | `/api/workflows/{id}/runs` | `?limit=&offset=` → this workflow's run history (same row shape as `/api/runs`). Runs are matched by definition **name** — the only linkage that exists (each run snapshots its own `workflow_definitions` row, so there is no FK to `workflows`); the list digest uses the same rule. Renaming a workflow therefore starts a fresh history; `404` |
| POST | `/api/workflows/{id}/sync-to-git` | open a PR with the spec → `{pr_url, branch, path}`; `501` until `GITHUB_TOKEN`+`GIT_REPO` are set; `502` on GitHub errors. The file lands under `GIT_PATH_PREFIX`; unset, that is the `path` of the connected repo matching `GIT_REPO` (so a merged PR is what the pull loop syncs), else `dags/` |
| GET | `/api/badges/{name}` | **unauthenticated** — a shields-style flat SVG of the named workflow's latest run outcome (`image/svg+xml`, `Cache-Control: no-cache`). Badges are embedded in READMEs and dashboards that cannot send an auth header, so the response reveals only a status label, never the spec; `no runs` (grey) covers both "unknown workflow" and "no runs yet", so the route does not disclose whether a workflow exists. Always `200` |
| GET/POST | `/api/git-repos` | list (any session) / connect (**admin**, `403` otherwise) `{url, branch?, path?, auto_sync, prune?, auth?}` → `201`; `400` empty or unusable URL, `409` duplicate. `url` may be `https://`, `ssh://[user@]host/…` or scp-style `git@host:owner/repo` (an https URL still may not embed credentials). The list also returns `worker_online` and `credentials_configured`. `prune` (default `false`): after a sync with **no file errors**, workflows this repo manages whose file is gone are set to `retired` (history kept; no longer git-managed; the file returning revives them). Each repo row reports `drift`: how many of its workflows were force-edited since the last sync. |
| DELETE | `/api/git-repos/{id}` | **admin group only.** `204`; `403` non-admin, `404` |
| POST | `/api/git-repos/{id}/sync` | sync now → updated repo row. Any session: it asks for the poll `auto_sync` already runs on a timer |
| PUT/DELETE | `/api/git-repos/{id}/auth` | **admin group only** (`403` otherwise). Set/rotate or remove this repo's Git credential → updated repo row; `404`; `503` when secret encryption is unconfigured. Body: `{kind: "none"\|"token"\|"ssh", username?, token?, ssh_private_key?, known_hosts?}`. `token` requires an HTTPS URL, `ssh` an SSH one — the mismatch is a `400` rather than a credential that could never be used. **Write-only:** the secret is stored AES-256-GCM encrypted and is never returned; reads get `auth_kind`, `auth_username`, `auth_known_hosts` and a non-secret `auth_hint` (`••••cdef`, or `ssh-ed25519 SHA256:…` — the same fingerprint the forge shows). Passphrase-protected keys are rejected: the worker has no terminal to be prompted at. |
| GET/POST | `/api/schedules` | `?workflow_id=` / create `{workflow_id, cron_expr, enabled?, catchup?, catchup_window_secs?, catchup_max_runs?}`; `400` bad cron |
| PUT/DELETE | `/api/schedules/{id}` | update / delete; `404` |
| POST | `/api/schedules/{id}/backfill` | **synchronous** backfill: `{from, to, max_runs?}` → `{scheduled, skipped, from, to, run_ids}` (materialized in one call, hard cap 1000) |
| POST | `/api/backfills` | **paced** backfill *job* (AIP-78): `{schedule_id, from, to, max_runs?}` → `201` job row; the engine paces it (job cap 100k). `404` unknown schedule, `400` bad range/cron/spec or no fire-times |
| GET | `/api/backfills` | `?schedule_id=&limit=` → job list (`{id, schedule_id, status, requested, fired, cursor, …}`) |
| GET | `/api/backfills/{id}` | one job for monitoring (`fired`/`requested`/`status`); `404` |
| POST | `/api/backfills/{id}/cancel` | stop pacing a running job → `{id, cancelled}`; `404` unknown, `409` already finished |

### Fleet

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/api/fleet` | The units this deployment manages. Fleet management — unit enrolment and identity, cohorts, staged bundle rollout, selector fan-out — is **not in this build**; this build answers `403` with a signpost naming the edition and the single-unit path (run the engine on the machine and drive it through this API, `SOURCE=dir`, or GitOps sync). The route is present in every build on purpose: a dead `404` at the moment someone acquires a second machine teaches nothing |
| GET | `/api/link` | Whether *this instance* belongs to a fleet, and what its offline licence says. Reads `DAGRON_FLEET_URL` and the presence (never the value) of `DAGRON_FLEET_TOKEN`, plus outbox evidence — last delivered, pending, dead — and verifies `DAGRON_CLOUD_LICENSE` against `DAGRON_CLOUD_LICENSE_PUBLIC_KEY` with the same rule the control plane applies at startup, so a licence this route calls invalid is the reason the plane refuses to boot. Present in every build; the open build answers a signpost, as `/api/fleet` does |
| POST | `/api/link/enrol` | **admin group only.** The one outbound call: `POST {control_plane_url}/units/enrol` with the join token and serial you supply. Redirects are refused (a `30x` would post the join token somewhere the operator did not name), 5 s to connect and 10 s in total, and the plane's own `401` / `402` / `409` sentence is passed through with the status it chose. Returns the unit credential **once**, rendered as env / Helm values / systemd — nothing is stored on the instance, so the operator applies it themselves and restarts. Enterprise build only; the open build answers a signpost |

### Artifacts (encrypted at rest — G-C2)

Programmatic artifact channel; bytes are envelope-encrypted at rest when a KEK
provider is configured (`DAGRON_ENV_KEK_PROVIDER`, see `CONFIG.md`). All routes
require an authenticated session; `503` when `DAGRON_ARTIFACT_DIR` is unset.

**The index.** What is put through this API is recorded: its size, its
sha256, and when it expires. That record is what lists a run's artifacts,
and what deletes one when its time is up. A file a task writes for itself,
into the run's directory or into the bucket with credentials of its own, is
served as before and is not recorded: it is not listed and nothing expires
it, until `POST …/complete` records it.

**Direct transfer.** `POST …/upload` and `GET …/download` answer with a URL
and with `direct`. Where the store can sign one (S3 and what speaks it, GCS,
Azure Blob), the URL is the store's own, signed for one request on the one
object and good for `DAGRON_ARTIFACT_PRESIGN_SECS`: the bytes do not pass
through the API, and are not held to `DAGRON_ARTIFACT_MAX_BYTES`. Whoever holds
such a URL may make that request with no other credential, so it is a secret
for as long as it lasts. Where the store has none to give (a directory, a
store that seals what it keeps, a tiered one), or `DAGRON_ARTIFACT_PRESIGN` is
`off`, the URL is this API's own route and `direct` is `false`: a client is
written for both. A signed URL names the store as the API reaches it, so set
`off` where clients cannot reach that address. A direct upload is one request,
so it is as large as the store takes in one; nothing here splits it into
parts, and nothing counts a quota.

| Method | Path | Notes |
| --- | --- | --- |
| PUT | `/api/runs/{run_id}/artifacts/{task}/{name}` | stream the request body into the store (encrypted per-chunk when a KEK is set) → `201` + backend locator. Size-capped by `DAGRON_ARTIFACT_MAX_BYTES`. The artifact is recorded in the index with its size and sha256. `?retention_days=N` (1 to 3650) asks for how long it is kept: no longer than `DAGRON_ARTIFACT_RETENTION_DAYS` where that is set, and that many days where nothing is asked; `400` for any other value, before a byte is kept |
| GET | `/api/runs/{run_id}/artifacts` | the run's artifacts in the index, by task and name → `{artifacts: [{task, name, size_bytes, sha256, created_at, expires_at}], next}`. `?limit=` (200 unless given, 1000 at most) and `?after=` (the `next` of the page before; `null` when there is no more). `sha256` is `null` for an artifact that went straight to the store, which the API did not read; `expires_at` is `null` for one that is kept. An upload that was begun and is not complete is not listed |
| GET | `/api/runs/{run_id}/workflow-artifacts` | a GitHub workflow's artifacts of the run, which the engine's results service keeps (`actions/upload-artifact`), complete, by name → `{run_id, artifacts: [{id, name, size_bytes, sha256, created_at, expires_at, provenance}]}`; `provenance` is whether the engine signed one |
| GET | `/api/runs/{run_id}/workflow-artifacts/{name}` | that artifact's zip, as its job uploaded it, streamed from the cache store (`DAGRON_CACHE_URL`, as the engine's) → `200 application/zip`; `404` for no such artifact, or one whose bytes have gone; `503` where dagron-api is given no store |
| GET | `/api/runs/{run_id}/workflow-artifacts/{name}/provenance` | that artifact's SLSA v1 provenance, signed by the engine as it was finalized (`DAGRON_PROVENANCE_KEY`): a DSSE envelope of an in-toto statement, `200 application/json` as `<name>.intoto.json`, for `cosign verify-blob-attestation --type slsaprovenance1 --signature` against the zip. `404` for no such artifact, or one kept without provenance |
| GET | `/api/workflow-artifacts/{id}` | the complete workflow artifact its job knows by `id`, with its `run_id`: what dagron-ci's GitHub API door asks, as GitHub names an artifact by its number alone → `404` for none |
| DELETE | `/api/runs/{run_id}/artifacts/{task}/{name}` | delete the artifact's bytes, then its record → `204`; `404` where there was neither; `501` for a store that deletes nothing (a tiered one), which keeps the record too |
| POST | `/api/runs/{run_id}/artifacts/{task}/{name}/upload` | where to send the artifact's bytes. Direct → `{direct: true, method: "PUT", url, headers, expires_in_secs, complete}`: send the bytes to `url` with the `headers` it names and no credential (most stores name none; Azure Blob asks for `x-ms-blob-type`), then `POST` the `complete` route. Relayed → `{direct: false, method: "PUT", url}`, where `url` is the `PUT` route above and takes this API's credentials. A direct upload under a key that held nothing, and never completed, is cleared up after a day |
| POST | `/api/runs/{run_id}/artifacts/{task}/{name}/complete` | record what the store holds under the key, after a direct upload (or for a file a task wrote for itself) → the artifact as the listing has it, with no `sha256`. `?retention_days=N` as for `PUT`. An artifact already complete is answered as it is. `404` where the store holds nothing under the key; `409` for a store that cannot say what an artifact holds without reading it (one that seals what it keeps): send the bytes with `PUT` |
| GET | `/api/runs/{run_id}/artifacts/{task}/{name}/download` | where to fetch the artifact's bytes → `{direct: true, method: "GET", url, headers, expires_in_secs}`, or `{direct: false, method: "GET", url}` with the `GET` route below; `404` if absent |
| GET | `/api/runs/{run_id}/artifacts/{task}/{name}` | stream the (decrypted) bytes → `200 application/octet-stream`; `404` if absent (a mid-stream decrypt/IO error aborts the connection) |
| GET | `/api/runs/{run_id}/artifacts/{task}/{name}/exists` | → `{exists: bool}` |
| POST | `/api/artifacts/rotate` | **admin group only.** Re-key every artifact from the previous KEK (`*_OLD` env) to the current one — rewraps the per-object data key, no payload re-encryption → `{rotated: N}`. `403` non-admin, `409` a rotation is already running, `400` no previous KEK configured, `503` artifacts off |
| POST | `/api/artifacts/sync` | **admin group only.** Drain the tiered artifact store to its remote tier now, under the daily uplink budget → `{moved: N}`. The on-demand path for a unit that has just reconnected; a periodic loop (`DAGRON_ARTIFACT_SYNC_SECS`) does the same on a timer. `{moved: 0}` for a store that is not tiered, `403` non-admin, `409` a store-wide sweep is already running, `503` artifacts off |

> **Operational caveat — quiesce writes during rotation.** The single-flight lock
> stops two rotations from overlapping, but it does **not** coordinate with normal
> artifact PUTs. Rotation rewraps each object with a read-then-write (`get` → rewrap
> → `put`); a PUT to the *same* key that lands between the `get` and the `put` is
> overwritten by the rewrapped older value (a silent lost update). Pause artifact
> writes (or rotate during a maintenance window) until a store-level compare-and-swap
> closes the window. A `DELETE` in that window is undone the same way: the
> rewrapped bytes are written back, with no record of them. The sweep of expired
> artifacts skips its turn while a rotation runs; a `DELETE` through the API
> does not.

### A task's artifacts

No session: the task's own credential, `Authorization: Bearer $DAGRON_ARTIFACTS_TOKEN`, which the engine (or dagron-api, for a remote runner's job) mints for each attempt of a task with `artifact_api: true`, and takes while the task runs. Postgres build. The same store, index, retention and body limit (`DAGRON_ARTIFACT_MAX_BYTES`) as a person's `PUT /api/runs/{run_id}/artifacts/…`, and the store an approval's `binds` is checked against.

| Method & path | |
|---|---|
| `GET /task/v1/artifacts/{task}/{name}` | the bytes of an artifact of the credential's run, of any of its tasks. `404` where there is none |
| `PUT /task/v1/artifacts/{task}/{name}` | writes one, under the credential's own task alone: `403` for another's, so that no task writes what another's approval binds, nor the digests an approval publishes. `201` |

Both: `401` with no credential, one that is no task's or one past its time; `403` once its task is not running; `503` with no artifact store.

### OIDC issuer (task identity)

No session. Served where `DAGRON_OIDC_ISSUER` is set; 404 otherwise.

| Method & path | |
|---|---|
| `GET {issuer path}/.well-known/openid-configuration` | the discovery document: `issuer`, `jwks_uri`, RS256 |
| `GET {issuer path}/.well-known/jwks` | every key's public half (`kty`, `kid`, `n`, `e`), a KMS's among them where `DAGRON_OIDC_SIGN_CMD` signs (enterprise) |
| `GET /oidc/token?audience=…` | with `Authorization: Bearer <DAGRON_OIDC_REQUEST_TOKEN>`: `{"value": "<jwt>"}`, as GitHub's request endpoint answers. 401 with no credential, one that is no task's or one past its time; 403 when its task is not running, the audience is not one of the task's, or the run is not trusted (an agent's is, where its environment's policy has `allowed_identity`) |

### Remote runners

Their management, under `/api`. Errors as the rest of `/api`.

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/api/runners/join-tokens` | **admin, signed in** (an API token is refused, as minting one is). `{"labels": [...], "max_uses": 1, "ttl_secs": 86400}`: the labels the runners it enrols may serve (at least one), how many it may enrol (1 to 1000) and how long it lives (60 s to 30 days). Returns the token (`dgrj_…`) **once** |
| GET | `/api/runners/join-tokens` | **admin.** Every join token, without its secret: labels, uses, expiry, revocation |
| POST | `/api/runners/join-tokens/{id}/revoke` | **admin.** It enrols no one more |
| GET | `/api/runners` | Each runner (`runner:<id>`): its labels, OS, architecture, capacity, release, when it was last seen, and its sessions, each with its lease, its secrets mode, whether it drains, and the tasks it holds |
| POST | `/api/runners/{id}/drain` | **admin.** Its live sessions take nothing more and end once what they run is over; the runner exits once drained. `{"draining": <sessions>}` |
| POST | `/api/runners/{id}/revoke` | **admin.** Its credential is refused from now on, its sessions end at once, and the engine's session sweep takes their tasks back to `ready` |
| GET | `/api/runners/queue?labels=a,b` | For each label (unset: every one a runner serves): its ready tasks, those sessions hold, and the live sessions that serve it with their capacity together |

The protocol a runner speaks, `/runner/v1`, is outside `/api`, takes a
runner's credential (`Authorization: Bearer dgr_…`) and answers RFC 9457
problem documents: see the [runner's README](../crates/dagron-runner/README.md#the-protocol).

| Method & path | |
|---|---|
| `GET /runner/v1` | No credential. `{"protocols": [0], "min_version": …}` |
| `POST /runner/v1/register` | `{join_token, name, labels, os, arch, capacity, version}` → `201 {runner_id, token, labels}`; the credential is shown once. 401 for a join token that is unknown, revoked, expired or used up |
| `POST /runner/v1/sessions` | `{labels, capacity, version, protocols, secrets: server\|home, os, arch}` → `201 {session_id, protocol, labels, capacity, lease_secs, heartbeat_secs, lease_expires_at}`. 426 where no protocol is shared, or the release is older than `DAGRON_RUNNER_MIN_VERSION` |
| `POST /runner/v1/sessions/{id}/claim?wait=30` | Long-polls (at most 55 s) for up to the session's free capacity of ready tasks of its labels: `{tasks: [job], drain}`. A task protocol 0 cannot carry is failed with why, and not given out. A job whose task asks for an identity carries `identity: {token, audience}`, its request credential minted for the attempt (a dagron-api that is no issuer fails the task). 410 for a session that is gone |
| `POST /runner/v1/sessions/{id}/heartbeat` | `{running: [{task_id, fence}]}` → `{lease_expires_at, cancel: [task_id], drain}`. One statement renews the session and every task it holds. A task the session holds that it has not named for a minute is given back. 410 for a session that is gone |
| `POST /runner/v1/tasks/{id}/logs/{seq}` | `{session_id, fence, text, reset}` → 204. An attempt's segments are numbered 0, 1, 2, … and the numbers only rise; segment 0, and one with `reset: true` (a seal, the whole log redacted as one piece), replace the log. A segment numbered at or below the last written under that fence is written already, sent again or held up on its way: 204, and nothing is written. 409 where the claim is not the session's under that fence, or the log would pass what dagron-api keeps of one: twice `DAGRON_LOG_TASK_CAP_BYTES` and 1 MiB, its beginning, its end and dagron's lines (a runner's log is dead past it, and its result is said all the same) |
| `POST /runner/v1/tasks/{id}/result` | `{session_id, fence, outcome: succeeded\|failed\|timed_out\|backpressure\|cancelled, exit_code, output}` → `{recorded: true}`, kept for an engine to apply; the same word again is `duplicate`. 409 for a stale fence |
| `DELETE /runner/v1/sessions/{id}` | Drain: `200 {"ended": true}` once it holds nothing, `202 {"draining": true, "running": n}` until then |
| `GET /runner/v1/queue?labels=` | The queue, for labels the runner serves (the autoscaler's) |

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
| GET | `/metrics` | **Prometheus text** — process counters and histograms, per-workflow run series, live DB gauges and `process_*`; see [METRICS.md](METRICS.md) |
| GET | `/openapi.yaml` · `/openapi.json` · `/docs` | embedded spec + Swagger UI |
| GET | `/runs` | `?status=&limit=` (default 50, clamp 1–1000) |
| POST | `/runs` | **raw YAML body** (not JSON-wrapped) → `201 {run_id}`; `400` invalid DAG; **`429` + `Retry-After`** when `MAX_INFLIGHT_RUNS` is exceeded — the admission backpressure documented in [`ARCHITECTURE.md` §5.6](ARCHITECTURE.md#56-v4-queue-driven-ingestion--admission-backpressure). **`?wait=true`** (with `?timeout_secs=`) makes it a synchronous invocation: `200 {run_id, status, finished, result}` instead of `201` |
| GET | `/runs/{id}` | `{run, tasks, trigger}`; `404`. `trigger` is the run's trigger record, with the fields `GET /api/runs/{id}` gives it, or `null` for a run from before every run had one |
| GET | `/runs/{id}/wait` | `?timeout_secs=` (default 30, clamp 1–600) long-poll to terminal → `{run_id, status, finished, result}`; `404`; timed-out wait is `200` with `finished: false` |
| GET | `/runs/{id}/logs` | **Workflow logs** — the whole run's output merged, attributed and filtered → `{run_id, tasks[], lines[], total, matched, truncated, eof, filtered, limit}`; `404`, `400` invalid filter. `?task=`/`?status=` scope which tasks are read; the [log filter](#log-filter) scopes which lines survive |
| GET | `/runs/{id}/tasks/{task_id}/logs` | one task's output for tailing → `{task_id, name, status, attempt, output, offset, next_offset, eof, total, matched, truncated, filtered, lines?}`; `404`, `400` invalid filter. **`?offset=`** returns only the output past that char offset — poll with `?offset=next_offset` until `eof`. The [log filter](#log-filter) applies within that slice. A running task's log is written once a second at most, and a long one less often (a second more for each MiB an append rewrites, up to 30 s), so a tail is about that far behind the task. A task that ended by itself has its whole log by the time it reads as ended; one that was cancelled can lack what it wrote as it was stopped, which its executor had not read yet; one whose engine stopped, or whose claim another took, can lack what waited to be written: its last second, or a long log's longer wait (up to 30 s). A log is kept up to `DAGRON_LOG_TASK_CAP_BYTES` ([`CONFIG.md`](CONFIG.md)) as the task writes it, and one that reached it is held there, on a line that says so, until the attempt is over: it is then its beginning, that line, a line that says how much of what the task wrote was left out (`[dagron] … of this attempt's output (N bytes and M lines, counted before masking) is left out here; what follows is its end`, in the form [`LOG_PIPELINE.md`](LOG_PIPELINE.md) §7.4 gives), and its end, the last `DAGRON_LOG_TAIL_BYTES`. `offset`, `next_offset`, `total`, `matched` and a filter's `n` count the log as it is given, those lines among it; the line between says how many of the task's own lines are not in it |
| POST | `/runs/{id}/cancel` | `{run_id, cancelled: true, stopping: m}`: `m` tasks a worker holds were asked to stop, and the run reads `running` (`stopping_as: "cancelled"`) until their work is gone; `409` if not cancellable. A run that waits for its workflow's cap (`queued_at` set, no `admitted_at`) is cancelled at once |
| POST | `/runs/{id}/rerun` | optional `{from?}` → `{run_id, rerun}`: `from` is `failed`, the default, or `all`, the run whole, one that succeeded too; `404`/`409`/`400`, and `409` for `all` while a task of the run still owes the teardown of a remote job; `429` when the run's concurrency key is full. The same implementation `dagron-api`'s route calls |
| POST | `/runs/{id}/tasks/{task_id}/retry` | put one failed or cancelled task back → `{run_id, task_id, retried}`; `404` unknown run/task, `409` task not failed or cancelled, `429` as for a rerun, when the run had ended |
| POST | `/runs/{id}/tasks/{task_id}/clear` | clear a completed task + its downstream cone → `{run_id, task_id, cleared}`; `404` unknown run/task, `409` task not completed, `429` as for a rerun, when the run had ended |
| POST | `/runs/{id}/tasks/{task_id}/approve` · `/reject` | resolve a `type: approval` gate → `{run_id, task_id, resolution, comment?}`; optional body `{comment}` is stored as `decision_comment` and written to the gate's log with the verdict (`approved by unknown at <time>`, since this API has no login to name a decider). This API is unauthenticated, so there is no `decided_by`, and it answers `403` for a gate it cannot check, pointing at the gateway: `approvers` blocks both decisions, `not_triggerer` and `binds` block approving (rejecting still works), and a stored spec it cannot read is refused too; `404`, `409` not awaiting approval |
| POST | `/runs/{id}/tasks/{task_id}/checkpoint` | a **running** task reports its committed checkpoint (`{uri, marker?}`, typically via its injected `DAGRON_RUN_ID`/`DAGRON_TASK_ID`); the pointer survives retries and the next attempt gets `DAGRON_RESUME_FROM` ([AI_WORKLOADS.md](AI_WORKLOADS.md)) → `{run_id, task_id, checkpoint_uri, marker}`; `400` empty uri, `404`, `409` task not running |
| GET | `/dead-letters` | `{dead_letters: […]}` |
| POST | `/dead-letters/{id}/redrive` | `{run_id, redriven_from}`; `404`/`400`; `409` a concurrent redrive claimed it first. **All or nothing**, like `dagron-api`'s route: the run is created and the dead letter deleted in one transaction, so a redrive that fails leaves the row exactly as it was, same `id` and history. A capacity refusal answers `503` (the workflow's `max_active_runs` cap) or `507` (the free-disk floor) with `Retry-After: 1` and `dead_letter_id`, which is the id you sent: retry it. Any other failure is a `500` and also keeps the row |
| DELETE | `/dead-letters/{id}` | `{id, deleted: true}`; `404` |
| GET | `/datasets` | Dataset registry (data-aware scheduling, [DATASETS.md](DATASETS.md)): `?limit=` (default 100, clamp 1–1000) → `{datasets: [{uri, updated_at, last_run_id, last_task, updates}]}`, most recently updated first |
| GET | `/datasets/events` | Lineage ledger — which run/task updated which dataset when: `?uri=` (narrow to one dataset) `&limit=` (default 100, clamp 1–1000) → `{events: [{id, uri, workflow, run_id, task_name, source, at}]}`, newest first. `source` is `task` (a `produces:` success) or `external`. `id` is the monotonic cursor sensors/triggers key off |
| POST | `/datasets/events` | Record an **external** dataset update (a producer outside dagron — CDC, object-store notification): `{uri}` → `{recorded}`. Wakes dataset sensors and fires `on_datasets:` triggers. **not implemented here**; this build returns `403` with a signpost (its datasets update via `produces:` tasks). `400` invalid URI |

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

## 4. `dagron-ci` (CI, bound at `CI_LISTEN`)

The CI sidecar ([`crates/dagron-ci`](../crates/dagron-ci/README.md)). The
receivers authenticate each request by its signature; the run API sends the
caller's dagron-api credential to the dagron-api of the repository's cell,
which says who the caller is (`GET /api/me`); the probes are open.
Errors are RFC 9457 problem documents (`application/problem+json`). Its event
workers are a client of dagron-api: they run each repository's bound workflow
through `POST /api/workflows/{id}/run`, under an `Idempotency-Key`. Its poll
sweep is a client of each forge: git's ref listing over HTTPS, and on GitHub
`GET /repos/{owner}/{repo}/pulls`.

| Method | Path | Auth | Notes |
|---|---|---|---|
| POST | `/hooks/github` | `X-Hub-Signature-256` | 202 `{"event_id"}` once queued; `{"duplicate": true}` for a delivery id seen before; `{"ignored": …}` for events that cannot lead to a run; `{"pong": true}` for `ping`. With `CI_GITHUB_CHECKS=on`, a `check_run` delivery whose action is `rerequested` (a check's "Re-run") reruns the check run's engine run from its failed jobs, and a `check_suite` one (a commit's "Re-run all checks") each run of the commit that was reported as check runs, with dagron-ci's own token: 202 `{"rerun": […], "left": […]}`, or `{"ignored": …}` where nothing was rerun, and 503 where the run's cell could not be asked. With `CI_GITHUB_FLEET_FILE`, a `workflow_job` delivery for a job whose labels the fleet serves is written down for it, and a runner is started for a job that waits: 202 `{"fleet_job", "class", "state"}`; a job that asks for no label of the fleet's is `{"ignored": …}`. 401 bad signature, 400 unreadable, 404 repository not connected or no secret configured. Only a new event writes anything. |
| POST | `/ci/v1/runs` | dagron-api session or API token; not a viewer | `{"repo": "github.com/acme/widgets", "ref": "refs/heads/main", "sha"?: …, "inputs"?: {…}, "pipeline"?: "deploy"}` queues a dispatch (the repository is matched without regard to case), which runs the pipeline it names (a file in `.dagron/ci`), or every pipeline at the commit whose `on:` takes `dispatch` (or the bound workflow when it has none), and answers 202 with the run. Without `sha`, the ref is read from the forge and its commit pinned. With it, the commit is one that is on the ref: the ref's own, or one it has behind it. `Idempotency-Key`, scoped to the caller: a repeat answers 200 with the first run, another request under the key 409. 400 invalid, 401, 403 a viewer, 404 repository not connected, 422 no such ref or a commit that is not on the ref, 502 forge unreadable, 503 dagron-api unreachable. |
| GET | `/ci/v1/runs/{id}` | dagron-api session or API token | Any event, by id, or by the id of an engine run it submitted (the one a commit status links to), which reads the event with that run alone: its repository, kind, ref, commit, source, trust, actor, state, reason, attempts, and the engine runs it submitted (`engine_runs: [{id, cell, pipeline, status}]`, oldest first; `pipeline` is the file, or `workflow:ID` for the bound workflow; `status` is `submitted` while dagron-ci follows the run, then how it ended, and `submitted` again once it runs again). 401, 404. |
| POST | `/ci/v1/runs/{id}/rerun` | dagron-api session or API token; not a viewer | Runs an event's failed and cancelled runs again, each from its failed jobs (`?failed=true`, the default), or with `?failed=false` each of its runs that is over, one that succeeded too, whole (`{"from": "all"}` to its cell). The cell's dagron-api is asked for each (`POST /api/runs/{id}/rerun`) with the caller's credential, and dagron-ci's record of the run re-opens with the answer, so its checks, its checkouts' credentials and its place under a build's limit follow it. 200 `{id, rerun: [{id, cell, pipeline, jobs}], left: [{id, pipeline, why}]}`. 400 a `failed` that is neither `true` nor `false`, 401, 403 a viewer, an agent, or a caller dagron-api does not let rerun, 404 no such event or no dagron-api token configured, 409 nothing to rerun (each run says why, among them a cell whose dagron-api runs no rerun of every job), 503 dagron-api unreachable. |
| POST | `/ci/v1/runs/{id}/cancel` | dagron-api session or API token; not a viewer | Stops an event's runs. Each is asked of its cell's dagron-api (`POST /api/runs/{id}/cancel`) with the caller's credential, whatever dagron-ci's record of it says, and dagron-ci's record follows the run to its end. An event that still waits to be dispatched is withdrawn: it ends `skipped`, with who cancelled it. 200 `{id, event, cancelled: [{id, cell, pipeline, jobs, stopping}], left: [{id, pipeline, why}]}`, where `event` is `withdrawn`, `dispatching` (a worker is on it, and a run it submits after this is not cancelled: ask again) or `settled`. With an engine run's id, that run alone is stopped and its event is not withdrawn. 401; 403 a viewer, an agent, or dagron-api's own refusal; 404; 409 nothing to cancel, with `Retry-After` when a worker is dispatching the event; 503 dagron-api could not be asked. |
| GET | `/ci/v1/runs/{id}/logs` | dagron-api session or API token | What an event's runs wrote, or the one run an engine run's id names. dagron-ci keeps no log: each run's is read from its cell's dagron-api (`GET /api/runs/{id}/logs`) with the caller's credential. `?job=` (repeatable), `status=`, `q=`, `limit=` and `tail=` narrow it. 200 `{id, state, reason, runs: [{id, cell, pipeline, status, logs \| error}]}`, `logs` being dagron-api's own answer. **`follow=true`**: one run's log followed live, its cell's `GET /api/runs/{id}/logs/stream` as the caller, passed on as it comes (`text/event-stream`; `from=` and `cursor=` say where it begins); 409 for an event that has submitted no run yet, 400 for one of several (with their ids, each to be followed by its own), and what dagron-api answers instead of a stream answered so (429 with its `Retry-After`). It ends when the run's log does, or when dagron-ci stops. Any other parameter is a 400, as is whatever dagron-api refuses. 401; 404; when no run's log could be read, dagron-api's 401 or 403, or 503 if it could not be asked. |
| GET | `/ci/v1/me` | dagron-api session or API token | Who the `default` cell's dagron-api says presents the credential (`GET /api/me`), and what a run they ask for is recorded as: `{sub, email, groups, trust}`, `trust` being `trusted`, or `agent` for a subject `agent:<name>`. 401 no credential or one dagron-api refused, 503 dagron-api could not be asked, 404 no cell is configured. `dagron-ci login` asks it before it keeps a token. |
| GET | `/ci/v1/repos` | dagron-api session or API token of an admin; not an agent | The repositories dagron-ci serves: `{repos: [{id, repo, forge, cell, workflow, poll_secs, polled_at, poll_error}]}`. The caller is who the `default` cell's dagron-api says the credential is (`GET /api/me`), in its `admin` group. 401 no credential or one dagron-api refused, 403 not an admin, or an agent; a `PUT` or `DELETE` by a viewer is 403 too, admin or not. |
| GET | `/ci/v1/repos/{host}/{owner}/{name}` | the same | One repository, connected or not: `{id, repo, forge, cell, workflow, connected, disconnected_at, changes: [{at, actor, action, set}]}`, its changes oldest first, each with who made it. 400 not a repository's name, 404 never connected. |
| PUT | `/ci/v1/repos/{host}/{owner}/{name}` | the same | Connects the repository, or changes one that is connected. The body may be empty; it takes `forge` (`github` by default, and only when connecting), `cell`, `workflow` (an id or `null`) and `poll_secs` (0, 30 to 86400, or `null`), and a key that is absent leaves a connected repository's setting as it is. What is so already is not set again, and not recorded. 201 `{…, action: "connected" \| "reconnected"}`, 200 `{…, action: "changed" \| "unchanged"}`. A repository that was disconnected comes back as the one it was, set to what this request names. 400 an unknown key, a cell that is not configured or a forge for a repository connected already; 409 connected by another request meanwhile. The change is recorded with the caller. |
| DELETE | `/ci/v1/repos/{host}/{owner}/{name}` | the same | Disconnects it: 200 `{id, repo, action: "disconnected"}`, 404 not connected. Its events and runs stay to be read; it takes no more deliveries, run requests or sweeps, its events that waited end `skipped`, and no checkout credential is minted for it. A run already going is not stopped. The change is recorded with the caller. |
| GET | `/healthz` | — | `ok` |
| GET | `/readyz` | — | `ready`, or 503 when the database does not answer within 1 s |
| GET | `/metrics` | — | Prometheus text: deliveries by outcome, queue depth, age of the oldest pending event, workers, events finished by outcome, poll sweeps by outcome and the events they queued, run API requests by outcome, records re-opened because their run was seen running again |
