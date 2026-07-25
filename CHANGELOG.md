# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor = breaking).

## [Unreleased]

### Fixed
- **The OSS release gate can build the mirror cut again.** It ran
  `cargo build --workspace`, which this workspace cannot satisfy *by design*:
  `dagron-core` compiles exactly one sqlx backend, the engine resolves `sqlite`,
  and `dagron-api`/`dagron-gitops` resolve `postgres` — `--workspace` unifies the
  features, enables both, and hits the `compile_error!`. It only started failing
  now because `dagron-api` gained its `dagron-core` dependency in the run-submit
  fix, after the last release. The gate (and the two docs that told you to run
  the same command) now builds the two feature worlds separately, which is how
  the images are built anyway. Every member is covered exactly once.
- **Mirror provenance scrub.** A dry run of the OSS sync — staging the include
  set exactly as the workflow does, from tracked files only — found leaks the
  fail-closed marker scan cannot see, because it matches tokens rather than
  meaning. The `examples/gitops/` Argo CD manifests pointed their `repoURL` and
  source paths at a repository that is not this one, so anyone applying them
  publicly got an auth failure against something they cannot see; the Python
  SDK's `Roadmap` URL did the same on PyPI. Both now point here, with
  repo-relative paths. `ARCHITECTURE.md`, `DLQ.md` and the backfill/monitoring/
  sdk examples told readers to `cd` into a directory this repository does not
  have. And ~16 files cited internal design documents a public reader cannot
  open — the explanations stay, the dangling citations are gone, along with a
  workflow comment that enumerated closed add-on images by name.

## [0.5.0] - 2026-07-26

### Added
- **The Helm chart can deploy the GitOps worker** (`gitops.enabled`, off by
  default — most installs never connect a repository). Until now the chart had no
  template for it, so a Helm install could register repos in the console and have
  nothing polling them. One replica by design: each would poll every repo against
  the same rows. Runs read-only-root like its siblings, with an emptyDir mounted
  at **`/tmp`** — the worker clones into `std::env::temp_dir()`, so mounting the
  scratch anywhere else leaves every sync failing on a read-only filesystem.
  Listed in the Artifact Hub image annotation, and `helm install` now says so when
  the worker is absent instead of leaving you to wonder why nothing syncs.

### Fixed
- **`docs/DATASETS.md` can be mirrored.** It linked a **private**
  a private internal roadmap doc and twice used the edition abbreviation the mirror's own
  fail-closed tripwire rejects, so it could never be published, while three mirrored
  files (CHANGELOG, `API.md`, `CONFIG.md`) linked to it. Scrubbed and included;
  the closed connector name it advertised is gone too.

### Fixed
- **The OSS mirror stops publishing dead documentation links.** `docs/OPERATIONS.md`
  is mirrored and had grown links to `BACKUP_RECOVERY.md`, `BACKUP_AWS.md` and the
  two `scripts/*.sh` backup helpers — none of which were in the `.ossync` include
  set, so every one of them 404'd on the public repo. `BACKUP_RECOVERY.md` and the
  scripts are now mirrored; `BACKUP_AWS.md` stays out (it walks through this repo's
  terraform, which is not published) and is referenced without a link. A link-closure
  check over the whole mirrored set — every relative link in every included file,
  resolved against the include list — is what found them, and the remaining
  offenders are all pre-existing.
- **An Enterprise console no longer refuses specs the Enterprise engine accepts.**
  `dagron-api`'s `enterprise` feature did not forward to `dagron-core` (it pinned
  `features = ["postgres", "ops"]`), so every dagron-core gate stayed shut in an
  Enterprise build of the API. The visible symptom: a workflow using multi-dataset
  composition (`on_datasets: [a, b]` / `datasets_mode:`) submitted through the
  console was rejected with the open-build signpost — telling a customer to buy the
  edition they were already running — while the identical spec sent to the engine's
  ops API was accepted. Same cluster, two answers. A test now pins the console's
  answer to the engine's in both build shapes; it fails without the forwarding.
- **The GitOps worker is actually buildable and published.** #721 split GitOps
  into its own `dagron-gitops` crate + image, but the crate was never added to
  the workspace `members` and nothing path-depends on it — so
  `cargo check -p dagron-gitops` answered *"package ID specification
  `dagron-gitops` did not match any packages"*, and any cargo invocation quietly
  dropped it from `Cargo.lock`. The release workflow's image matrix listed four
  images and not this one, so a tag would have shipped the feature with no image
  to run it. Both fixed, plus a Docker Hub overview for the new public repo.

### Changed
- **Envelope encryption (BYOK/KMS) and encrypted artifacts at rest are
  Enterprise.** Drawn before 0.5.0 ships, deliberately: the KEK layer has never
  appeared in a tagged OSS release (`dagron-crypto/src/lib.rs` was 121 lines at
  `dagron-oss-v0.4.3` with no `KeyProvider`; it is 1468 now), so this is a line
  drawn rather than a takeback.

  **Open in every build, unchanged:** environment secrets encrypted with
  AES-256-GCM under `DAGRON_ENV_SECRET_KEY` — the `v1:` format the compose stack
  and [docs/HOWTO.md](docs/HOWTO.md) §5 use — and the plain artifact store.
  **Enterprise:** KEK providers (AWS KMS / GCP KMS / Azure Key Vault / the
  command seam), envelope-wrapped data keys, artifact encryption at rest, and the
  store-wide rotation sweep (`POST /api/artifacts/rotate`, a route now absent from
  open builds).

  Enforced in one place — `dagron_crypto::build_provider`, the funnel every KEK
  consumer reaches through (env-secret `v2:` mode, artifact-at-rest, rotation) —
  so the line cannot drift apart across three subsystems. An open build handed a
  `DAGRON_ENV_KEK_PROVIDER` **errors with a signpost naming the edition and the
  open alternative**, rather than returning "no provider" and quietly encrypting
  with a single env-var key: silently downgrading a deployment that asked for KMS
  is not something anyone should discover from a ciphertext dump.

### Fixed
- **dagron-api: Argon2 password hashing no longer runs on the async runtime, and
  is bounded.** Measured on the compose stack: RSS went 67 MB → 187 MB after 20
  logins → 227 MB after 60, then plateaued; 30 concurrent SSE clients moved it
  3 MB, so streaming was never the cost. `Argon2::default()` is Argon2id with
  `m_cost = 19 MiB`, and glibc raises its mmap threshold after repeated
  allocations that size and keeps one arena per thread that ever hashed — with
  hashing done inline on tokio workers, that was ~19 MiB × worker-count resident
  *and* a worker pinned for the ~50–100 ms each hash takes, stalling every other
  request on that thread. All hashing now goes through one `pwhash` module: a
  semaphore (`DAGRON_PW_HASH_CONCURRENCY`, default 2 → ~38 MiB ceiling) taken
  *before* the work is handed to `spawn_blocking`. Parameters are unchanged —
  19 MiB is the OWASP floor and lowering it would weaken the hashes.

  The residency itself is glibc's dynamic mmap threshold: it grows as it sees
  repeated frees that size, after which the scratch comes off the heap and is
  never returned. `compose.yaml` pins `MALLOC_MMAP_THRESHOLD_=131072` (plus
  `MALLOC_ARENA_MAX=2` for the idle baseline). Measured over 30 logins:

  | | baseline | after |
  |---|---|---|
  | before | 67 MB | 227 MB |
  | arena cap only | 5.6 MB | 166 MB |
  | + threshold pinned | 5.6 MB | **5.9 MB** |
- **dagron-api: `POST /api/login` is rate limited** (`DAGRON_LOGIN_RATE_LIMIT`,
  default 30 per `DAGRON_LOGIN_RATE_WINDOW_SECS`=60, per client; `0` disables).
  It was the one unauthenticated route that spends ~19 MiB and ~100 ms of CPU per
  call — and spends it even for an email that doesn't exist, because the
  no-such-user path deliberately runs the same verify to avoid leaking which
  accounts are registered. That made it free memory/CPU amplification for anyone
  who could reach the port. Keyed on the socket peer (unforgeable) by default;
  `DAGRON_TRUST_PROXY_HEADERS=true` switches to the first `X-Forwarded-For` hop
  for deployments actually behind a proxy, which is what the console's `/api`
  rewrite is (set in `compose.yaml`). In-memory and per replica: a cost ceiling
  and a guessing brake, not a distributed lockout policy.

### Added
- **Point-in-time recovery on AWS** (`docs/BACKUP_AWS.md`, not mirrored — it references this repo's terraform) —
  the follow-on to the runbook's "nightly dumps are a floor, not a plan". Three
  concrete shapes with working config: RDS/Aurora managed PITR (terraform, and
  the `backup_retention_period = 0` footgun), CloudNativePG on EKS shipping WAL +
  base backups to S3 over IRSA, and pgBackRest on EC2. Includes the archiver
  heartbeat to alert on (`pg_stat_archiver.failed_count` — a permissions mistake
  in the verification drill produced `archived=6 failed=3` while the database
  accepted writes normally), and the dagron-specific steps after a restore:
  repoint `DATABASE_URL` and start one engine first, keep old
  `DAGRON_ENV_SECRET_KEY` versions for at least the PITR window (restored
  ciphertext predates a rotation and the current key will not decrypt it), expect
  the artifact store and GC archive to be inconsistent with the restored
  database, and cancel unsafe runs before the engine reclaims stale leases. The
  full PITR loop was executed end to end (base backup → data → target time →
  TRUNCATE → recover): recovery stopped before the offending transaction and the
  100 rows came back.
- **Backup / migration / disaster-recovery runbook**
  ([docs/BACKUP_RECOVERY.md](docs/BACKUP_RECOVERY.md)) plus
  `scripts/backup-postgres.sh` and `scripts/restore-postgres.sh`. Covers what is
  state (the database **and** `DAGRON_ENV_SECRET_KEY`, which lives outside it —
  lose the key and stored secrets are unrecoverable ciphertext), how migrations
  actually behave (embedded, applied at engine startup, no `dagron migrate`
  command, forward-only, two sets sharing one `_sqlx_migrations` ledger via
  `ignore_missing`), the production upgrade order, a restore rehearsal, and
  eight recovery playbooks. Every claim was executed against a live stack:
  dump/restore round-tripped identical counts (25 runs / 54 tasks / 42
  migrations), a tampered checksum produced
  `Error: migration 40 was previously applied but has been modified` with exit 1
  and an untouched database, and an injected version-900 row was absorbed by
  `ignore_missing` rather than blocking startup. Also documents the trap that
  dagron-api recreates its own tables empty, so a partial restore loses every
  user and secret without erroring.
- **Docs: where a long script lives** ([docs/HOWTO.md](docs/HOWTO.md) §6 +
  [examples/scripts/](examples/scripts/README.md)). A task's `command:` is argv
  and there is no `script:` field or file-include, which left "my script is 400
  lines" unanswered. Four worked patterns — bake into the image, absolute path on
  the engine host, fetch at run start, or the body in an env var — with the two
  facts that decide between them: only `command`/`env`/`docker_image` reach the
  task process (`input:` does not), and no executor mounts host paths, so
  `DAGRON_ARTIFACTS` passes files between host tasks but cannot carry a script
  into a container. All four validate through `dagron validate`; the env-var one
  runs with no setup.
- **Console: template calls (`templates:` / `template:`) are first-class.** The
  engine has expanded template calls since day one, but dagron-api's own spec
  mirror knew only `command` and `workflow_ref`, so a task calling a template was
  a 400 (`needs a command or a workflow_ref`) on **every** save/submit path —
  `examples/templates/01_dag_of_dags.yaml` could not be stored through the API at
  all, let alone opened in the editor. `TaskSpecInput` now carries
  `template`/`arguments` and `DagSpecInput` carries `templates`, with save-time
  validation the engine can't do lazily: template names unique and non-empty, no
  empty template, every call resolves to a declared template (spec-local, so a
  typo is caught on Save, not at run time), each template's own task list checked
  as a sub-graph, and a task is exactly one of leaf / call / chain. Two
  silent-drop paths are now explicit errors instead: a `workflow_ref` **inside** a
  template (the chain expander only walks top-level tasks) and a `workflow_ref`
  **to** a workflow that declares its own `templates:` (inlining copies tasks,
  not templates).
- **Console: the visual editor edits template calls.** A `template:` task renders
  as a dashed sub-DAG node (`⧉ etl · 3 tasks`); its panel has a **Calls template**
  picker over the declared templates and an **Arguments** row per parameter the
  chosen template declares (defaults as placeholders, unknown-but-present
  arguments kept and flagged). Leaf-only knobs (command, image, retries, timeout,
  trigger rule) are hidden on a call, which runs no command of its own. New
  **“DAG of DAGs (template call)”** starter and **Blocks → Control flow → “Sub-DAG
  (template call)”**, which scaffolds the `templates:` entry and its call together
  — a call is only valid alongside the template it names.

### Changed
- **The visual editor refuses to silently degrade.** The canvas draws one node
  per task, which is a lie for a spec using `with_items:`/`with_param:` (one node,
  N tasks), `hook:` (edges to every task, undrawn), `gang:`, `type: wait`/
  `workflow` (command-less kinds whose panel offered a command box), `cache:` or
  `resources:`. Loading such a spec now **disables the Visual tab** — "this
  workflow uses features the visual editor can't edit — use the YAML tab" — and
  lists the responsible fields. The rule is an allowlist, so a field the editor
  has never heard of (a new engine feature, a typo) locks the tab rather than
  becoming quietly editable; everything the block palette emits stays editable.
  Nothing is dropped either way — the spec is untouched and fully editable as
  YAML. Renaming a task in the visual editor now also follows `result_from:`,
  which otherwise saved a dangling reference.
- **Datasets: data-aware scheduling** (Airflow Datasets / Dagster asset
  sensors — the #27 dataset-sensor follow-on plus the round-2 `dataset://`
  stretch slice, as one subsystem; [docs/DATASETS.md](docs/DATASETS.md)). Three
  pieces on a shared ledger:
  - **`produces: [<uri>, …]`** on a command task — on success the engine
    upserts each URI in the new `datasets` registry and appends a
    `dataset_events` lineage row (producing workflow/run/task/time): the
    cross-workflow update trail, queryable via `GET /datasets` and
    `GET /datasets/events?uri=…`. URIs template per fan-out instance. Recording
    is fence-guarded (a reclaimed task's late result can't fabricate lineage)
    and best-effort (a ledger failure never fails the run).
  - **`wait: { dataset: <uri> }`** — a dataset sensor on the #27 park
    machinery (running + NULL lease, no worker slot, **no new status**): new
    `task_runs.wait_dataset` + `wait_dataset_cursor` columns hold the ledger's
    high-water mark at park time, so only an update recorded *after* the park
    resolves it (fresh data, never history). Joins `for`/`until`/`url` in the
    exactly-one-of rule.
  - **`on_datasets: [<uri>]`** on a registered workflow — dataset-triggered
    scheduling: a sweep (~5 s, `DATASET_TRIGGERS=0` opts out) syncs
    subscriptions into the new `dataset_triggers` table and fires a run when a
    subscribed dataset updates, injecting `{{ trigger_dataset }}`. Firing is a
    **CAS cursor advance** — HA-safe with no leadership, exactly one scheduler
    wins each fire; registration never fires on history; rapid updates coalesce
    into one run; a fire refused at `max_active_runs` rolls its cursor back and
    retries. SQLite migrations 032–033, Postgres 039–040; metrics
    `scheduler_dataset_updates_total` / `scheduler_dataset_fires_total`.
  - **Open-core split** (per the open-core split): the single-team loop —
    produce, lineage, sensor, single-dataset trigger — is fully open.
    **Multi-dataset composition** (`on_datasets: [a, b, …]` +
    `datasets_mode: any|all` fan-in) and **external dataset events**
    (`POST /datasets/events`, for CDC/object-store producers outside dagron)
    ship with dagron Enterprise; the open build answers both with signpost
    errors naming the edition and the OSS workaround (the SOURCE-connector
    funnel pattern).
- **HTTP wait sensor: `wait.url`** (parity fast-win #27 follow-on — Airflow
  `HttpSensor`) — a task `{ type: wait, wait: { url: <http(s)-endpoint> } }` parks
  holding **no worker slot** and the engine GETs the endpoint every
  `WAIT_POLL_SECS` (default 15 s), succeeding the task on the first `2xx` so its
  dependents advance. Reuses the #27 park mechanics: the parked task stays
  `running` with `claimed_by`/`lease_expires_at` NULL and the endpoint in a new
  `task_runs.wait_url` column with a `next_poll_at` throttle (SQLite migration
  031, Postgres 038) — so the claim scan skips it and lease recovery leaves it
  alone, needing no new status. A non-2xx / errored poll re-parks it for the next
  window; the per-request timeout keeps a hung endpoint from stalling the tick.
  Bounded by the run's own `run_timeout_secs`. `for` / `until` / `url` are
  mutually exclusive (exactly one). The `url` may template (`{{ params.* }}`).
  (A fourth form, `wait: { dataset: … }`, ships in the datasets entry above, so
  the exactly-one-of rule is over four fields, not three.)
- **OpenTelemetry OTLP span exporter** (parity fast-win #28 follow-on) — with an
  `otel`-feature build **and** `OTEL_EXPORTER_OTLP_ENDPOINT` set, `dagron-logging`
  now installs an OTLP **HTTP/protobuf** span exporter + a `tracing-opentelemetry`
  bridge layer, so the engine's own `tracing` spans are delivered to a collector.
  Each dispatched task is wrapped in a `task.dispatch` span (dependency-free,
  always on under `otel`) so there's a span per task to export. Transport is
  HTTP/protobuf over a blocking `reqwest`/rustls client — the gRPC/tonic
  transport is deliberately not pulled in; endpoint/headers/timeouts come from the
  standard `OTEL_EXPORTER_OTLP_*` env vars. Off by default in every sense: no
  opentelemetry dependency without the feature, and no exporter without the
  endpoint (spans still propagate, just aren't exported). Dashboards/sampling/
  retention remain observability's per `SCOPE.md`.
- **OpenTelemetry trace-context propagation** (parity fast-win #28 — Dagster
  #12353 / Prefect #9271 / Airflow external-trace #69633) — built with the new
  `otel` Cargo feature, the engine injects a fresh W3C `traceparent` into every
  dispatched task's env (`TRACEPARENT`), so the task's own OpenTelemetry
  instrumentation joins an external trace (external-trace embedding). The trace
  id is logged on dispatch for log↔trace correlation. Dependency-free and off by
  default — a default build never sets `TRACEPARENT`, unchanged behavior. The
  OTLP span exporter (span **delivery**) ships as the follow-on above.
- **Deferrable wait sensor: `type: wait`** (parity fast-win #27 — Airflow
  deferrable time sensors / Argo suspend-with-duration) — a task
  `{ type: wait, wait: { for: <duration> } }` (or `{ until: <rfc3339> }`) parks
  holding **no worker slot** until its deadline, then succeeds and its dependents
  advance. `for` is a relative duration (`30s`/`5m`/`2h`) anchored when the task
  is reached; `until` is an absolute instant. Reuses the #23 park mechanics: the
  parked task stays `running` with `claimed_by`/`lease_expires_at` NULL and its
  resume time in a new `task_runs.wake_at` column (SQLite migration 030, Postgres
  037) — so the claim scan skips it and lease recovery leaves it alone, needing
  no new status. A reconcile sweep resolves due sensors each tick (idempotent,
  HA-safe); a deadline already in the past resolves at once. The HTTP (`url`)
  sensor ships as a follow-on above; dataset sensors remain a follow-on.
- **Sub-workflow trigger: `type: workflow`** (parity fast-win #23 — Airflow
  TriggerDagRunOperator / Argo #6922 workflow-of-workflows) — a task
  `{ type: workflow, workflow: <name> }` submits the named **registered**
  workflow as a child run when reached, then parks until the child is terminal:
  the parent succeeds if the child succeeded and fails if it failed/cancelled,
  after which its dependents advance. The parked task stays `running` with
  `claimed_by`/`lease_expires_at` NULL and the child id in a new
  `task_runs.sub_run_id` column (SQLite migration 029, Postgres 036) — so the
  claim scan never re-picks it and lease recovery never reclaims it, needing **no
  new status value and no status-CHECK rebuild**. A reconcile sweep resolves
  parked triggers each tick (idempotent, HA-safe). If the child is at its own
  `max_active_runs` cap the trigger is released back to `ready` and retries; an
  unknown or unparseable child fails the task. Nesting is bounded by
  `SUBWORKFLOW_MAX_DEPTH` (default 8) — a workflow that names itself, directly
  or around a cycle, fails the offending task instead of spawning child runs
  without end (see **Security** below); the global `max_inflight_runs`
  admission valve caps concurrent depth on top of that.
- **Workflow tags** (parity fast-win #26 — Airflow #16432 colored tags / #24464
  folder view, Dagster #14530) — a DAG may declare `tags: [..]` (each
  `[A-Za-z0-9_.-]`, ≤64 chars) to organize and filter workflows. The registry
  surfaces them on `GET /api/workflows` and `GET /api/workflows/:id`, and
  `GET /api/workflows?tag=<t>` returns only workflows carrying that tag. Tags are
  **parsed from the stored spec on read** — no denormalized column to keep in
  sync, so they always reflect the current definition — and validated when the
  engine loads the spec. The **Workflows UI** renders per-tag colored chips
  (deterministic color per tag) in the table and board views; clicking a chip
  filters the list to that tag, and tags join the text search. Purely
  organizational; the engine ignores them.
- **Result memoization: `cache`** (parity fast-win #22 — Argo memoization /
  Prefect task caching) — a task may declare `cache: { key, max_age_secs? }`. A
  successful run stores its output keyed by `(workflow, task, resolved key)`; a
  later task whose key matches reuses that output and **skips execution
  entirely** — no worker, no secrets, no artifacts — then its dependents advance
  as for a normal success. The `key` is a template resolved at expansion, so
  `{{ scheduled_time }}` / `{{ params.* }}` make repeated and backfilled runs hit
  the cache (reproducible backfills). `max_age_secs` expires stale entries so the
  task re-runs and refreshes. New `task_memo` table (SQLite 028, Postgres 035);
  the memo write reuses the per-success spec parse the `repeat` operator already
  does, so uncached tasks pay nothing; `scheduler_cache_hits_total` metric. No
  behavior change without a `cache:` block.
- **Named concurrency pools: `pool`** (parity fast-win #21, second slice —
  Airflow #13975 "multiple pools" / Argo Kueue) — a task may name a `pool:`, and
  a scheduler claims a pooled task only while fewer than the pool's capacity are
  already running in it (capacities from the `POOLS` env, e.g. `POOLS=etl:4,db:2`).
  An over-budget task simply waits in `ready` until a slot frees — no run is
  dropped and there is no new run state — because the ready-claim itself is the
  enforcement point (`task_runs.pool`; SQLite migration 027, Postgres 033 column
  / 034 `CREATE INDEX CONCURRENTLY`). **When `POOLS` is unset the claim path is
  byte-for-byte unchanged** (zero cost/risk for deployments not using pools). On
  Postgres, unpooled/uncapped tasks keep the lock-free `FOR UPDATE SKIP LOCKED`
  fast path; only capped-pool claims serialize, under a global advisory lock, so
  concurrent controllers can't over-commit a pool's slots (SQLite is single-writer,
  so its one write-tx claim is already race-free). DAG-wide default via
  `task_defaults.pool`; fan-out instances inherit the parent's pool; names are
  validated `[a-z0-9_-]{1,64}`. Together with `max_active_runs` (run-level), this
  completes #21's concurrency-control story: per-run and per-task-pool limits.
- **Per-workflow concurrency limit: `max_active_runs`** (parity fast-win #21,
  first slice — Argo #12757 workflow concurrency control / Prefect deployment
  concurrency limits) — a DAG may set `max_active_runs: <n>` to cap how many of
  its runs (matched by workflow name) are `running` at once. `create_run`
  enforces it at the single choke point every fire path flows through, so the
  cap holds for API submits, cron/DB schedules, backfills, catch-up, queue
  sources, and dead-letter redrive alike — with **no change to the hot task-claim
  path**. Over the cap, `create_run` returns a typed `MaxActiveRunsReached`: the
  API answers **429 + Retry-After** (not 500), a queue source **requeues** the
  message instead of dead-lettering it, and schedule/backfill fires are held back
  (backfill releases its slot and retries when capacity frees). The count filters
  the small set of `running` runs and PK-joins their definitions — no schema
  change, no migration. `None`/`0` = unlimited (unchanged behavior). Caps
  concurrent *runs*, not per-task concurrency (named task pools are a follow-on).
- **Cause-conditional retry: `retry_on_timeout`** (parity fast-win #24 —
  Airflow #9232) — a task may set `retry_on_timeout: false` so that a run killed
  by its `timeout_secs` deadline **fails immediately** instead of burning the
  rest of its `max_attempts` (a deadline kill usually recurs, so the retries just
  delay the failure). Timeout-only: a non-zero exit or a backend error still
  retries under the normal attempts rule. Every executor backend (local, Docker,
  Kubernetes) now aborts a deadline via a typed `TimeoutError` that the worker
  downcasts, so the reconcile loop distinguishes a timeout from any other
  failure; the terminal-fail log records `timed_out` / `not_retried_due_to_timeout`.
  DAG-wide default via `task_defaults.retry_on_timeout` (a task's own value wins).
  Default `true` = unchanged behavior. Pure logic — no migration.
- **Task dispatch priority** (parity fast-win #25 — Airflow `priority_weight` /
  Argo Kueue analog) — a task may declare `priority: <int>` (default `0`); among
  the tasks that are `ready` at the same moment a scheduler claims higher
  priority first (`claim_ready` now orders `priority DESC, scheduled_at`), so a
  latency-sensitive branch jumps a deep backlog of low-priority work. A pure
  tiebreak: it never lets a task run before its dependencies, and it persists on
  the row so a retry / lease recovery keeps its place. A DAG-wide default is
  settable via `task_defaults.priority` (a task's own non-zero value wins);
  fan-out instances inherit their parent's priority. `task_runs.priority`
  (SQLite migration 026 + partial ready index; Postgres 031 column / 032
  `CREATE INDEX CONCURRENTLY`), threaded through both DB backends. No behavior
  change for existing/unprioritized workflows.
- **Gang co-scheduling schema (`gang: {size: N}`)** — a task expands at run
  creation into N member rows (`<name>.<rank>`, shared gang id), dependents
  wait for every member, and members receive `DAGRON_GANG_ID` /
  `DAGRON_GANG_RANK` / `DAGRON_GANG_SIZE` for rendezvous — in **any** build, so
  a member always knows its rank however it was scheduled. Validation enforces
  leaf, single-attempt gang tasks (die-together retry is a unit-level rerun).
  The all-or-nothing gang claimer (`RUNNER_GANGS=1` — claim a gang only when
  every member is ready and capacity fits it whole, cancel a failed member's
  siblings) ships with the dagron Enterprise scheduler; without it, members
  schedule as ordinary tasks. Placement sweeps leave gang members in place.
- **Per-partition range leases + sharded streams (multi-consumer)** — a
  `source_partitions` lease table with claim/renew/release primitives (both
  backends) lets N engines split one logical stream, one consumer per
  partition, heartbeat-renewed and rebalanced when a consumer dies. Committed
  positions namespace per shard (`<source>/<partition>` in `source_offsets`)
  via the new `PendingPosition.substream`, so exactly-once holds per
  partition. Reference consumer: point `STREAM_PATH` at a **directory** and
  each `*.ndjson` file becomes a leased shard (`STREAM_SUFFIX`,
  `STREAM_MAX_PARTITIONS`) — the broker-free consumer group.
- **Exactly-once streaming ingestion (transactional source offsets)** — a
  source's resumable coordinate (byte offset, broker offset, replication LSN)
  now commits **in the same datastore transaction** as the run — or dead
  letter — it accounts for (`source_offsets` table; `create_run_with_offset`
  / `record_dead_letter_with_offset`), and the ingest actor hands the
  committed cursor back to the source at startup. A crash-replay of the whole
  stream creates zero duplicate runs and zero duplicate dead letters. Sources
  opt in via two new defaulted `WorkflowSource` methods (`pending_position`,
  `set_committed_position`) — existing sources are untouched; `SOURCE=stream`
  implements them (its offset file remains as a trailing mirror).
- **Cloud artifact stores: S3 / GCS / Azure Blob** — `DAGRON_ARTIFACT_URL=
  s3://bucket/prefix` (or `gs://`, `az://`; S3-compatible MinIO/Ceph via
  `AWS_ENDPOINT_URL`) selects an `object_store`-backed `ArtifactStore`
  (features `s3`/`gcs`/`azure` on `dagron-artifact`, forwarded by the
  `archive-*` features on the API binary). Same sanitized
  `<run>/<task>/<name>` layout as the local store, bounded-memory multipart
  `put_stream` (aborted on failure), streaming `get_stream`, listing (so
  KEK rotation sweeps buckets too), and transparent composition with the
  BYOK/KMS `EncryptedStore` — ciphertext in the bucket. The engine injects
  per-task `DAGRON_ARTIFACTS_URL` / `DAGRON_CHECKPOINT_URL` so checkpoints
  written on one machine resume on any other that can reach the bucket
  (multi-cloud checkpoint resume); a URL in a build without the matching
  backend feature errors loudly instead of falling back to local.
- **Streaming ingestion built in: `SOURCE=stream`** — follow an append-only
  NDJSON event file or named pipe, one workflow submission per line, with
  at-least-once delivery off a durable byte-offset checkpoint (committed only
  after the run is created; delete the offset file to replay, or
  `STREAM_FOLLOW=false` to drain a backlog and exit). Poison lines are
  dead-lettered (durable row + a `.dlq` NDJSON mirror) instead of wedging the
  stream. Selecting a managed connector kind (`kafka`/`nats`/`sqs`/`redis`/
  `events`) now errors with an accurate signpost to the dagron Enterprise
  connector suite and the built-in alternatives. Guide: `docs/STREAMING.md`;
  runnable case studies: `examples/streaming/`.
- **Long-running tasks: lease heartbeat** — workers renew a running task's
  lease every 10 s (claim triple–guarded), so a task may run for hours under
  the same short-lease crash recovery; a worker that dies stops heartbeating
  and its task is reclaimed exactly as before. Losing the claim aborts the
  local execution (the reclaimer owns the task). Opt out with
  `TASK_LEASE_HEARTBEAT=false`.
- **Checkpoint-aware resume for retries** — a running task reports its
  committed checkpoint (`POST /runs/{id}/tasks/{task_id}/checkpoint` with its
  injected `DAGRON_RUN_ID`/`DAGRON_TASK_ID`, or the
  `$DAGRON_CHECKPOINT_DIR/latest` file convention); the pointer survives
  retries and *Rerun failed*, is cleared by *Clear task*, and the next attempt
  is dispatched with `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER` — resume
  from epoch N, not epoch 0. Tasks also now receive their identity
  (`DAGRON_RUN_ID`, `DAGRON_TASK`, `DAGRON_TASK_ID`) in env. Guide:
  `docs/AI_WORKLOADS.md`; runnable case studies: `examples/ai/`.
- **GPU resource sugar** — `resources: { gpu: { count: N, resource: … } }`
  expands to the Kubernetes extended-resource limit
  (default `nvidia.com/gpu`); an explicit `limits` entry for the same key
  wins. Validation rejects `count: 0`.

### Security
- **Sub-workflow nesting is bounded (`SUBWORKFLOW_MAX_DEPTH`, default 8).** A
  `type: workflow` task that names its own workflow — directly or around a cycle
  — spawned child runs without end, each leaving a parked parent row behind.
  There is no recursion on a stack here, so nothing failed: it was an unbounded
  run factory that a single spec could start. A trigger at or past the cap now
  fails that task with a message naming the depth. Depth is read by walking
  `task_runs.sub_run_id` up from the triggering task's own run — the parentage
  the parking column already records — so no `parent_run_id` column and no
  backfill are needed; SQLite migration 034 / Postgres 041 add the partial index
  that makes each hop a lookup (and also covers the reconcile sweep's existing
  forward read). The walk stops at the cap, so the check is O(depth) and
  terminates even if the column ever describes a cycle.
- **`wait.url` sensors no longer follow redirects.** A `wait.url` poll is issued
  by the **scheduler**, not the task sandbox, so an external endpoint answering
  `302 http://169.254.169.254/…` could previously use the scheduler as a deputy
  to reach addresses a task pod may not. The scheme check at validation time
  cannot see a redirect target; the client now declines to follow one, and a 3xx
  simply reads as "not ready" and re-parks. A failed client build is now a
  startup error rather than a silent fall back to a default (redirect-following,
  untimed) client.
- **Optional `WAIT_URL_DENY_PRIVATE=1`** restricts `wait.url` polls to
  globally-routable addresses — private/loopback/link-local (incl. the cloud
  metadata address), CGNAT, multicast and reserved ranges are refused across
  IPv4, IPv6 and IPv4-mapped IPv6. **Off by default on purpose:** the common
  `wait.url` *is* an internal address (`http://svc.default.svc/ready`), so a
  deny-by-default would break the primary use case to harden the case where the
  scheduler's network position is genuinely wider than the executor's
  (`EXECUTOR=kubernetes` with a differentiated NetworkPolicy). Enforcement is
  split so neither half is bypassable: IP-literal hosts are checked before the
  request (a literal never reaches a resolver), and names are filtered **inside
  the client's DNS resolver**, so the addresses dialed are exactly the ones that
  passed the filter — no check-to-connect window for a DNS rebind. Proxies are
  bypassed (`no_proxy`) while the policy is on: `HTTP_PROXY`/`HTTPS_PROXY` would
  otherwise mean the resolver only ever judges the *proxy's* address while the
  proxy dials the blocked target. A refused URL logs a WARN and re-parks as "not
  ready" rather than failing the task, so relaxing the policy resolves parked
  sensors in place. Editing the workflow spec does not: the endpoint is
  materialized into `task_runs.wait_url` when the task row is created, so a spec
  fix applies to the next run, not to a task already parked on the old URL.

## [0.4.0] - 2026-07-23

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

## [0.4.1] - 2026-07-23

### Added
- **Artifact Hub-ready OSS Helm chart** — chart annotations/metadata so the
  published OCI chart is listed and browsable.

## [0.4.2] - 2026-07-23

### Added
- **Task-oriented [`HOWTO.md`](docs/HOWTO.md)**, and the reference docs
  (`API`/`CONFIG`/`OPERATIONS`/`MCP`) added to the OSS mirror.
- Artifact Hub badge on the README; the Grafana dashboard screenshot committed
  (the repo-wide `*.png` ignore had silently dropped it).

### Fixed
- **Frontend image CVEs** — dependency bumps and a distroless runtime.
- Open-core framing scrubbed from the mirrored docs.

## [0.4.3] - 2026-07-23

### Added
- **`mancube/dagron-mcp`** — the OSS MCP server image (eight stdio tools over the
  same JWT-gated API the console uses).
- Product landing site (S3 + CloudFront + Route53) and Artifact Hub repo metadata.

### Fixed
- **CRITICAL CVE in the frontend image** — runtime moved to a continuously
  patched base.

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
