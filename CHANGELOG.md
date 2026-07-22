# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor = breaking).

## [Unreleased]

### Added
- **Live-updating UI with a global pause toggle** — the Runs, Workflows, and
  Overview pages now refresh in realtime off a new account-wide SSE endpoint
  (`GET /api/events/stream`, fanned out from the same shared Postgres
  `LISTEN task_events` as the per-run stream), replacing Overview's fixed 5s
  poll with event-driven refetches (debounced 400ms, flushed at least every 2s
  during bursts; a slow 30s poll keeps GitOps sync state and next-fire times
  fresh). A **Live/Paused pill** on each page header (and the run detail
  header) toggles one persisted preference (`localStorage`, synced across tabs
  and pages): paused holds no streams open and does zero background reads —
  data loads once, with a ⟳ manual-refresh button — for sessions where live
  reads are too costly; resuming (or an SSE reconnect) refetches to catch up.
  Hidden tabs cost nothing either: streams close on tab hide and reopen on
  re-show (with a catch-up refetch), and Overview's slow poll skips ticks
  while hidden.
- **DAG layout directions** (frontend) — the DAG graph (Submit live preview and
  the run viewer) now offers three layouts via a ↓/→/↘ segmented control on the
  canvas: vertical (top→bottom, the old default), horizontal (left→right, with
  edges leaving node sides), and a diagonal cascade (ranks step down-and-right
  like a staircase). The choice persists in localStorage and follows the user
  across views.
- **Resizable editor/preview split** (frontend) — the border between the YAML
  editor and the live DAG preview on Submit (and the rerun dialog) is now a
  draggable divider: slide it to grow either pane (clamped 20–80%), double-click
  to reset to even, and the ratio persists across sessions.
- **Offline UI assets (no CDN)** — the frontend self-hosts the Monaco editor
  (staged into `public/monaco/vs` at build, no `cdn.jsdelivr.net`) and the engine
  serves vendored Swagger UI assets at `/docs`, so no browser asset needs a CDN.
- **Native GCS + Azure Blob archive backends** (multi-cloud) — the GC archive
  sink, `dagron archive-compact`, and the `dagron-api` archive-history reads now
  speak `gs://` (feature `archive-gcs`) and `az://`/`azure://` (`archive-azure`)
  natively, alongside `s3://` (`archive-s3`, which also covers S3-compatible
  MinIO/Ceph). `GC_ARCHIVE_URL`'s scheme selects the backend; credentials come
  from each backend's standard env (`AWS_*`/`GOOGLE_*`/`AZURE_*`). URL→store
  dispatch is centralized in a small `objstore` module in both crates; a scheme
  whose backend feature isn't compiled in is a hard startup error, never a
  silent plain purge.
- **Archive history reads** — the archive-before-purge GC now upserts an
  `archived_runs` index row (run_id, name, status, timestamps; SQLite
  migration 020 / Postgres 024) before purging, and `dagron-api` gains
  `GET /api/archive/runs` (index-only list, filter by `name`, paged) and
  `GET /api/archive/runs/{id}` (fetches the run's `dagron.run-archive.v1`
  document from `GC_ARCHIVE_DIR`/`GC_ARCHIVE_URL`; the api's `archive-s3`
  feature mirrors the engine's). Purge now fails closed on an index-write
  failure too — an archived-but-unlisted run would be invisible history.
- **Parquet compaction** (`dagron archive-compact`, cargo feature
  `archive-parquet`) — a bounded, CronJob-shaped sweep that folds archived
  `run-*.json` documents older than `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS`
  (default 30) into a date-partitioned Parquet dataset
  (`compact/tasks/dt=<date>/part-<uuid>.parquet`, one row per task with run
  columns denormalized), stamps `archived_runs.compacted_at`/`parquet_path`,
  and deletes source documents **only after** the part file verifiably
  landed (at-least-once across crashes — dedup on `(run_id, task_id)` when
  querying). Works over both the dir sink and, with `archive-s3`, the S3
  sink. A compacted run's detail endpoint answers 410 Gone with the
  `parquet_path`.
- **Split LISTEN DSN** (`DATABASE_LISTEN_URL`) — the engine's reconcile-loop
  `Waker` and dagron-api's shared SSE listener connect their session-scoped
  `LISTEN` to this URL when set (the direct Postgres endpoint), while
  `DATABASE_URL` may point at transaction-mode PgBouncer — which cannot serve
  `LISTEN`. Unset = the listener shares the pool config, exactly as before.
  Unlocks pooled query traffic on shared state-store cells.
- **S3-native GC archive sink** (`GC_ARCHIVE_URL=s3://bucket[/prefix]`,
  cargo feature `archive-s3`) — archive-before-purge without the intermediary
  volume: each expired run's `dagron.run-archive.v1` document is one atomic S3
  `PUT` (credentials/region/endpoint from standard `AWS_*` env), and only
  verified PUTs are purged. Setting the URL without the feature is a startup
  error (same contract as `EXECUTOR=kubernetes` without `kubernetes`), and
  `GC_ARCHIVE_DIR` local archiving is unchanged.
- **Runner-class routing** (`runner_class`) — segment the scheduler fleet by
  workload shape. A task (or a whole DAG via a spec-level default) may name a
  **runner class** (`[a-z0-9_-]{1,64}`, default `default`); the class is
  persisted on `task_runs` (SQLite migration 019 / Postgres 022, with a
  class-scoped partial ready-index) and a scheduler started with
  `RUNNER_CLASSES=etl,pulse` claims **only** those classes
  (`db::claim_ready_classes`, both backends — same `SKIP LOCKED`/CAS + lease +
  fence contract). Unset `RUNNER_CLASSES` claims every class, so existing
  single-pool deployments are unchanged. Template expansion substitutes
  `{{ param }}`s in `runner_class` like `docker_image`; invalid names fail
  validation at submit (spec) or startup (env). SDKs:
  `Dag(name, runner_class=...)` / `task(..., runner_class=...)` (Python),
  `new Dag(name, {runnerClass})` / `task(..., {runnerClass})` (TypeScript).
- **`DB_MAX_CONNECTIONS`** — the Postgres pool size (previously hard-coded 8)
  is env-tunable (min 2), so many lean engines can share one pooled state
  cluster.
- **Archive-before-purge retention GC** (`GC_ARCHIVE_DIR`) — with an archive
  directory configured, the leadership-gated GC sweep first exports each
  expired terminal run as a self-contained JSON document
  (`dagron.run-archive.v1`: run + definition + task rows + outbox events),
  written atomically (tmp → fsync → rename), and purges **only** the runs
  whose export verifiably landed (`db::archivable_runs` +
  `db::purge_runs_by_id`, both backends). A failed write keeps the run in the
  hot store; re-archiving after a crash is idempotent. Unset, GC behaves
  exactly as before.
- **Per-class backlog gauges + stale-ready alert** — `/metrics` now exposes
  `scheduler_ready_tasks_by_class{runner_class=…}` and
  `scheduler_ready_oldest_age_seconds{runner_class=…}`, and a leadership-gated
  loop WARNs when a class's oldest ready task waits longer than
  `READY_AGE_ALERT_SECS` (default 300 s; `0` disables;
  `READY_AGE_CHECK_INTERVAL_SECS` tunes the cadence). Catches the
  segmentation footgun where every scheduler's `RUNNER_CLASSES` excludes a
  class and its tasks age silently.

### Fixed
- **Approval-gate timeouts in lean builds** — `db::resolve_approval` /
  `db::resolve_expired_approvals` were `ops`-gated while the reconcile loop
  calls the sweep unconditionally, so an engine built without the `ops`
  feature failed to compile (and, conceptually, a lean engine would never
  fail-safe an expired gate). Both are now available in every build.
- **SDKs cover the 0.3.0 API.** The Python SDK (`dagron` 0.3.0) gains `approve_task`
  / `reject_task` for `type: approval` gates, the durable backfill-job methods
  (`create_backfill` / `list_backfills` / `get_backfill` / `cancel_backfill` over
  `/api/backfills`), and an optional `path` on `connect_git_repo`. The TypeScript
  SDK (`@dagron/sdk` 0.3.0) gains a full `Client` class (it previously shipped only
  the `Dag` builder) mirroring the Python client's whole surface, including the same
  new 0.3.0 methods and an SSE `streamRun` / `waitForRun`. (The TS package version
  jumps `0.1.1 → 0.3.0`, skipping `0.2.0`, to line up with the dagron-api / Python
  SDK version — no `0.2.0` TS release was lost.)

## [0.3.0] - 2026-07-08

### Added
- **Human approval tasks** (`type: approval`) — a task can now be a **human
  gate**: when its dependencies are
  satisfied it parks in a new `awaiting_approval` status (never claimed by a
  worker, so it needs no command) and the run waits until an operator approves or
  rejects it. `POST /runs/{id}/tasks/{task_id}/approve` (→ the task succeeds and
  the DAG proceeds) and `.../reject` (→ it fails, so `all_success` downstream
  skips), mirrored on the UI edge (`POST /api/runs/:id/tasks/:tid/approve|reject`).
  A gate may set `approval_timeout_secs` with `approval_on_timeout: approve|reject`
  (default **reject** — a gate fails safe): a reconcile-loop sweep auto-resolves
  an expired gate. Reuses the trigger-rule dependency model (approve decrements
  dependents like any success; reject like any failure). New `awaiting_approval`
  task status + `task_runs.is_approval` / `approval_timeout_secs` /
  `approval_on_timeout` columns (SQLite migration 018 — a table rebuild to widen
  the status CHECK — Postgres 021). Named approvers/groups, notifications, and
  audit build on this primitive behind the `enterprise` feature.
- **Backfill as a first-class API object** — a
  date-range backfill is now a durable, listable, monitorable, cancellable
  *job* the scheduler **paces**, instead of the synchronous capped
  `POST /schedules/:id/backfill` that materializes a whole window in one call.
  `POST /api/backfills` (`{schedule_id, from, to, max_runs?}`) snapshots the
  schedule's cron + timezone + workflow spec into a `backfills` row; the engine's
  leadership-gated pacer fires a bounded number of the range's fire-times per
  tick (default 20, `BACKFILL_PACE_PER_TICK`), advancing a cursor, so a large
  backfill drips into the cluster over many ticks rather than stampeding it —
  and the paced job can cover far more than the synchronous endpoint's 1000-run
  cap (job cap 100k). `GET /api/backfills` (list, `?schedule_id=`),
  `GET /api/backfills/:id` (monitor `fired`/`requested`/`status`), and
  `POST /api/backfills/:id/cancel` (stop pacing) round out the lifecycle. Runs
  are still deduped through the shared `schedule_backfills` ledger (a job never
  double-runs a slot a manual/auto backfill already materialized) and each
  backfilled run gets its logical date as `{{ scheduled_time }}`. New `backfills`
  table (SQLite migration 017, Postgres 020).
- **Live log tailing** — a running task's output is now visible *as it runs*,
  not only after it exits. `LocalExecutor` streams each stdout line to the
  datastore mid-run (fence-guarded, so a stale attempt can't corrupt a re-run;
  secrets are masked per-chunk like the final output, #8), and the task-logs
  endpoints gained an `?offset=` tail: `GET /runs/{id}/tasks/{task_id}/logs`
  (engine ops) and `GET /api/runs/:id/tasks/:tid/logs` (UI edge) return only the
  output past a character offset plus `next_offset` (resume point) and `eof`
  (task terminal), so a client polls with `?offset=next_offset` until `eof`.
  Offsets are Unicode-scalar counts (never split a multibyte character). No
  schema change — appends reuse the existing `task_runs.output` column. Docker
  surfaces its captured output through the same tail path (true mid-run
  `follow: true` is a follow-up); Kubernetes is unchanged (output at completion).
- **Synchronous invocation + run results** (`result_from`) — makes dagron
  callable as a durable
  function. A workflow can name the task whose output *is* the run's result with
  `result_from: <task>`; on success the engine copies that task's output into
  `workflow_runs.output`. Two ways to get it back synchronously: `POST /runs?
  wait=true` blocks until the run is terminal and returns `{run_id, status,
  finished, result}` (200, not 201) instead of just the id; `GET /runs/{id}/wait`
  long-polls an already-submitted run to completion. Both take `?timeout_secs=`
  (default 30, clamp 1–600); a timed-out wait returns `finished: false` with the
  live status so the caller re-polls (not an error). Mirrored on the `dagron-api`
  UI edge (`GET /api/runs/:id/wait` + `result_from` on submit). New nullable
  `workflow_runs.result_from` column (SQLite migration 016, Postgres 019);
  `result_from` must name a real, non-hook task (rejected at parse time).
- **Clear task + downstream** — a new
  recovery verb that re-runs a *single completed task together with everything
  that transitively depends on it*, without re-running the whole DAG or waiting
  for a failure. `POST /runs/{id}/tasks/{task_id}/clear` (engine ops) and
  `POST /api/runs/:id/tasks/:tid/clear` (UI edge) reset the target and its
  downstream cone from any terminal state (`succeeded`/`failed`/`skipped`/
  `cancelled`) back to `pending`, recompute each reset task's `remaining_deps`,
  bump `version` to fence stale workers, and re-arm a finished run so the
  reconcile loop resumes — while ancestors and unrelated branches stay intact.
  Use it to pick up a fixed input on a *green* node (which `rerun-from-failed`
  can't reach, since that only resets the failure frontier). 404 for an unknown
  run/task, 409 if the task is still running/pending. Reuses the existing
  fencing + `remaining_deps` model, so no schema change.
- **Deadline alerts** (`deadline: { in: 45m }`) — a soft
  SLA on a run: when a still-running run passes it, the engine emits a
  `run.deadline_exceeded` event to the transactional outbox (drained by the
  outbox delivery worker for webhook/Slack) and bumps `scheduler_deadline_alerts_total`,
  **without** cancelling — unlike `run_timeout_secs`, which fails the run.
  Fire-once and winner-take-all across schedulers. New `alert_deadline_at` /
  `alert_fired_at` columns (SQLite migration 015, Postgres 018); a shared
  duration parser accepts `45m` / `2h` / `90s` / `1d` / bare seconds.
- **Lifecycle hooks + `allow_failure`** — `hook: on_exit` makes a task a
  finalizer that runs once every
  non-hook task is terminal, `hook: on_failure` runs it only when the run is
  failing; both are auto-wired to depend on all other tasks with the matching
  trigger rule (`all_done` / `one_failed`), so no `depends_on` is needed.
  `allow_failure: true` lets an optional/best-effort task fail without failing
  the run (the task still records `failed`). New `task_runs.allow_failure`
  column (SQLite migration 014, Postgres 017); `reap_completed_runs` ignores
  allow-failure tasks when deciding the run status.
- **Secret env references** (`value_from`) — a task env var can pull its value
  from an external secret instead of storing it inline, so a credential never
  lands in the workflow spec or the datastore:
  `env: [{ name: DB_PASSWORD, value_from: { secret: prod-db-password } }]`.
  Resolved at dispatch from `DAGRON_SECRET_<NAME>` (process env) or a file
  `<DAGRON_SECRETS_DIR>/<NAME>` (the SOPS / External-Secrets / k8s-secret mount
  convention); a missing secret fails the task rather than running it empty.
  Resolved `value_from` values are always masked in output (regardless of the
  var's name), building on the #8 redactor. (Vault/cloud secret backends are
  a follow-up behind the same seam.)
- **Task trigger rules** (`trigger_rule:`) — a task can now run
  based on its dependencies' *outcomes*, not just their success: `all_success`
  (default), `all_done` (cleanup joins), `one_failed` / `all_failed` (failure
  handlers), `none_failed`. The scheduler's dependency model was generalized so
  every terminal transition (success/failure/skip) decrements dependents, and
  `advance_ready_tasks` evaluates each task's rule once its deps are all terminal
  → `ready` or `skipped` (with skips cascading). New `task_runs.trigger_rule`
  column (SQLite migration 013, Postgres 016). **Behavior change:** a task
  skipped because an upstream failed now shows as `skipped` (was `cancelled`);
  `cancelled` now means only an operator cancel or a run-deadline sweep. The run
  is still `failed` if any task failed, and `rerun-from-failed` re-runs the
  failed + skipped frontier.
- **Secret masking in task output.** Sensitive task-env values are now masked to
  `***` before a task's output is stored or logged, so a task that echoes a
  credential (or a library that prints one in a stack trace) no longer leaks it
  into the datastore or UI. On by default: any task env var whose **name** matches
  a sensitive pattern (`TOKEN`/`PASSWORD`/`SECRET`/`KEY`/… — overridable via
  `DAGRON_SENSITIVE_ENV_PATTERNS`, empty to disable), plus the values of any
  engine-process vars named in `DAGRON_REDACT_ENV` (e.g. `DATABASE_URL`). Masking
  is applied centrally in the worker (covers local/Docker/Kubernetes backends)
  and to the local executor's live stderr log; only values ≥ 4 chars are masked.
- **Forge feedback — commit statuses + run badges.** A workflow can declare a
  `notify.git` block (`provider: github|gitlab`, `repo`, `sha`, optional
  `context`/`target_url`, all `{{ param }}`-templated); when the run finishes the
  engine posts a success/failure commit status to the forge, so a dagron run
  shows up as a green/red check on the commit that triggered it. Best-effort and
  off by default — active only when `GITHUB_TOKEN`/`GITLAB_TOKEN` is set
  (`GITHUB_API_BASE`/`GITLAB_API_BASE` override for GHE/self-managed) — and a
  forge being down never affects run execution (mirrors the OpenLineage
  emitter). New `dagron-forge` crate holds the `ForgeClient` + GitHub/GitLab
  request builders. Plus a public, unauthenticated **run badge**:
  `GET /api/badges/:name` returns a shields-style SVG of a workflow's latest run
  status for embedding in a README.
- **GitOps pull sync (Git → datastore).** The GitOps page's **Sync** action now
  performs a real reconcile instead of just flipping UI state: it shallow-clones
  the registered branch, validates every `*.yaml`/`*.yml` under the repo's
  configured `path` (default `dagron/`) through the same parser the submit path
  uses, and upserts each valid workflow into the `workflows` table keyed by name
  — the Git → datastore *pull* half of GitOps, with no CRDs required. The
  fetched commit (`rev`), synced count, and per-file validation errors are
  recorded on the repo row; one bad file doesn't block the good ones. `POST
  /api/git-repos` gains an optional `path`; private repos clone with a token from
  `DAGRON_GIT_TOKEN`/`GITHUB_TOKEN` (injected only into `https://` URLs and
  redacted from errors); only `https/http/git/ssh/file` URL schemes are accepted.
  (An `auto_sync` background poller is the remaining follow-up; the flag is
  stored.)
- **`dagron-plan` — workflow diff for pull requests.** A new binary crate
  (`crates/dagron-plan`, depends on `dagron-core` only) that resolves two specs
  through the real parse → expand → validate pipeline and reports what would
  actually change: added/removed/changed leaf tasks with field-level diffs
  (command, deps, image, env, retries, timeouts), run-timeout changes, and a
  Mermaid graph of the resulting DAG with added/changed tasks flagged. Because
  it diffs the *resolved* DAG, two different YAML spellings of the same fan-out
  show as no change. `dagron-plan <base.yaml> <head.yaml>` or
  `dagron-plan --git <base>..<head> <path>` (shells `git show`); `git diff`-style
  exit codes with `--exit-code` (2 when the plan is non-empty) for a CI drift
  gate. Pairs with `dagron validate` to gate merges.
- **Cron `when` gate + `stopStrategy`** — two
  optional per-schedule expressions, both reusing the task-level `when:`
  evaluator:
  - **`when`**: a per-fire conditional gate for conditions cron can't express
    (e.g. `"{{ day }} == {{ days_in_month }}"` = last day of month,
    `"{{ weekday }} <= 5"` = weekdays only). Evaluated against the scheduled
    time's calendar fields in the schedule's timezone; a false result skips the
    fire (only `next_fire_at` advances). Supported on both file-cron config
    entries (`when:`) and UI schedules (`when_expr`). Skips counted in
    `scheduler_schedule_gated_total`.
  - **`stopStrategy`** (`stop_expr`, UI schedules): a comparison over the
    schedule's run outcome counts — `{{ succeeded }}` / `{{ failed }}` /
    `{{ total }}` — evaluated before each fire; when true the schedule
    auto-stops (disabled, with `stopped_at`/`stop_reason` surfaced via the API).
    Re-enabling clears the stop record. Counted in
    `scheduler_schedules_stopped_total`.
  New `schedules.when_expr/stop_expr/stopped_at/stop_reason` columns and a
  `workflow_runs.schedule_id` stamp for outcome counting (SQLite migration 012,
  Postgres 015).
- **Timezone-aware cron schedules** — a schedule now carries an IANA `timezone`
  (e.g. `America/New_York`); its cron expression is evaluated in that zone so a
  "02:00 daily" job keeps firing at 02:00 wall-clock across DST transitions.
  Threaded through the file-cron
  config (`timezone:` per entry), the DB-schedule loop, the manual + automatic
  backfill catch-up, the `dagron-api` schedule drawer (`timezone` field on
  create/update, validated → 400 on an unknown zone), and the operator's
  `CronWorkflow` CRD (`spec.timezone`). New `schedules.timezone` column
  (SQLite migration 011, Postgres 014), `DEFAULT 'UTC'` so existing rows are
  unchanged. The tz-aware next-fire computation is one shared helper
  (`dagron-engine::schedule_time`) mirrored by the API.
- **`dagron validate <file|dir>... [--json]`** — offline workflow lint through
  the exact parse → template-expansion → graph-validation pipeline every submit
  path uses. Directories are walked recursively; `--json` emits one object per
  file for CI; exits non-zero on any invalid spec. (A pre-merge GitOps check.)
- **Run-level timeout** — `run_timeout_secs` on the workflow spec. The engine's
  deadline sweep marks an overdue run `failed`, cancels its remaining tasks
  (fence-guarded against late executor writes), and counts it in the new
  `scheduler_runs_deadline_exceeded_total` metric. New nullable
  `workflow_runs.deadline_at` column (SQLite migration 010, Postgres 013).
- **Retry backoff cap** — `retry_max_delay_secs` on a task clamps the
  exponential backoff to a ceiling.
- **Named fan-out instances** — `instance_key: "{{ item.region }}"` on a
  `with_items`/`with_param` task names each expanded instance `<task>.<label>`
  instead of `<task>.<index>`. Labels are
  sanitized to `[A-Za-z0-9_-]` and must be unique within the fan-out.
- **`{{ scheduled_time }}` parameter** — every time-originated run (file cron,
  DB schedules, automatic backfill catch-up) receives its *nominal* fire time as
  an RFC-3339 workflow parameter, so tasks can reference their logical date
  (the data-interval idiom; a backfilled run processes *its* interval,
  not "now").
- The Python and TypeScript SDKs (`sdks/`) now ship in the distribution, so
  the `examples/sdk/` scripts resolve against the bundled SDK out of the box.
- Runnable SDK examples under `examples/sdk/` (Python + TypeScript) that drive a
  live `dagron-api`: quickstart, workflow+schedule, live SSE streaming, and
  cascade-rerun recovery, with a README covering setup and env config.
- Initial open-source cut of the dagron engine.

### Fixed
- **TypeScript SDK `Dag.submit()`** posted the raw spec to `POST /api/runs`; the
  gateway expects `{"yaml": "<spec>"}` and rejected it with `422 missing field
  yaml`. It now wraps the spec and returns the parsed `run_id` (`@dagron/sdk`
  0.1.0 → 0.1.1). Mirrors the Python SDK's v0.2 fix.
- `Executor` trait + `LocalExecutor` (subprocess) reference backend.
- `WorkflowSource` trait + `FileSource` and `ChannelSource` reference sources.
- In-memory `run_dag` scheduler: dependency-driven concurrency, retries with
  exponential backoff, and downstream skip-on-failure.
- `dagron run <file.yaml>` CLI and a bundled example DAG.
