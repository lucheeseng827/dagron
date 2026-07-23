# Configuration reference — every knob in one place

dagron has **no CLI flag parser** — the binaries take a couple of positional
arguments and read everything else from the environment. Compile-time Cargo
features select the storage backend and optional subsystems. Sources of truth:
`crates/dagron-engine/src/lib.rs` (engine env reads),
`crates/dagron-api/src/main.rs` + `src/routes/{login,gitsync}.rs` (UI-edge env
reads), `crates/dagron-logging/src/lib.rs`, `crates/dagron-mcp/src/lib.rs`,
and the workspace `Cargo.toml` feature lists. Regenerate this file when those
change.

## Invocation (positional arguments)

```text
dagron [dev] [DAG_PATH] [DB_TARGET]
dagron validate <file|dir>... [--json]
dagron archive-compact [DB_TARGET]
```

| Arg | Default | Meaning |
| --- | --- | --- |
| `dev` (literal) | — | Zero-infra local quickstart: SQLite + the management API/Swagger on `127.0.0.1:8787` (sets `API_ADDR` if unset), stays resident. With no DAG file present it starts idle and waits for `POST /runs`. Requires the (default) `ops` feature. |
| `validate` (literal) | — | Offline spec lint: parses, template-expands, and graph-validates each `*.yaml`/`*.yml` (directories walked recursively; hidden dirs skipped) through the same pipeline every submit path uses. `--json` emits one JSON object per file; exits non-zero if any file fails. No datastore, no server, works in every build. |
| `DAG_PATH` | `examples/simple_dag.yaml` | Workflow YAML for the `file` source. **There is no `run` subcommand** — the first positional *is* the DAG path. |
| `DB_TARGET` | `workflow.db` (sqlite build) / `$DATABASE_URL` → `postgres://localhost/workflow` (postgres build) | SQLite file path, or Postgres connection string. |

Other binaries: `dagron-import argo <workflow.yaml>` (exactly two args; only
the `argo` importer exists — prints a dagron DAG YAML to stdout), `dagron-mcp`
(no args; JSON-RPC over stdio — see [`MCP.md`](MCP.md)), and `dagron-plan`
(diff two specs for a PR — `dagron-plan <base.yaml> <head.yaml>` or
`dagron-plan --git <base>..<head> <path>`; prints markdown + a Mermaid graph,
`--exit-code` returns `2` when the plan has changes for a CI gate).

## Cargo features (compile-time selection)

| Feature | Default? | Effect |
| --- | --- | --- |
| `sqlite` | yes | Embedded single-writer SQLite datastore. Exactly one of `sqlite`/`postgres` must be enabled — both or neither is a `compile_error!` (`dagron-core/src/db.rs`). |
| `postgres` | no | Postgres datastore: `LISTEN/NOTIFY` wake, `FOR UPDATE SKIP LOCKED` multi-worker claim. Required by HA and by the UI stack (`FEATURES: postgres,ops` in `compose.yaml`). |
| `ops` | yes | The engine management API (`API_ADDR`), cron, retention GC, DB schedules, leadership. |
| `kubernetes` | no | `EXECUTOR=kubernetes` (KubeExecutor). Without it that value is a startup error, never a silent downgrade. |
| `archive-s3` | no | Cloud GC archive sink over S3 (`GC_ARCHIVE_URL=s3://…`, incl. S3-compatible MinIO/Ceph via `AWS_ENDPOINT_URL`). A `GC_ARCHIVE_URL` scheme whose backend feature is absent is a startup error, never a silent downgrade — same contract as `kubernetes`. Implies `ops`. |
| `archive-gcs` | no | Google Cloud Storage archive sink (`GC_ARCHIVE_URL=gs://…`; credentials from `GOOGLE_*` env). Implies `ops`. |
| `archive-azure` | no | Azure Blob Storage archive sink (`GC_ARCHIVE_URL=az://…` or `azure://…`; credentials from `AZURE_*` env). Implies `ops`. |
| `archive-parquet` | no | `dagron archive-compact` — fold archived `run-*.json` documents into the date-partitioned Parquet dataset (`compact/tasks/dt=…/`). Heavy (arrow+parquet), hence its own feature; combine with a cloud backend (`archive-s3`/`-gcs`/`-azure`) to compact a cloud archive. Implies `ops`. |

## Engine (`dagron` binary) environment

All read in `crates/dagron-engine/src/lib.rs` unless noted.

| Variable | Type / values | Default | What it does |
| --- | --- | --- | --- |
| `EXECUTOR` | `local` \| `docker` \| `kubernetes`/`k8s` | `local` | Task execution backend. Unrecognized values warn and fall back to `local`; `kubernetes` without the feature is a startup error. |
| `WORKER_COUNT` | usize | `16` (min 1) | Worker-pool size = max concurrently running tasks. |
| `SOURCE` | `file` | `file` | Workflow ingestion source. The OSS build supports `file`; queue backends (`redis`/`sqs`/`kafka`/`nats`) are not compiled in and selecting one errors at startup (`dagron-source/src/source.rs`). |
| `MAX_INFLIGHT_RUNS` | i64 | `64` (min 1) | Admission valve: cap on simultaneously active runs; overflow stays buffered at the source. The ops API answers `429` + `Retry-After` above it (`0` disables the API-side cap). |
| `DEAD_LETTER_MAX_ATTEMPTS` | i64 | `3` (min 1) | Transient `create_run` failures retried before a submission is dead-lettered (parse failures dead-letter immediately). |
| `DATABASE_URL` | conn string | `postgres://localhost/workflow` | Postgres builds only; positional `DB_TARGET` wins. Redacted before logging. |
| `API_ADDR` | `host:port` | unset = ops API **disabled** (`dagron dev` sets `127.0.0.1:8787`) | Bind address of the engine's unauthenticated ops API; also keeps the process resident. Invalid values warn and disable the API. |
| `DOCKER_IMAGE` | image ref | `alpine:latest` | Default image for `EXECUTOR=docker` (also k8s fallback). |
| `K8S_IMAGE` | image ref | `$DOCKER_IMAGE` → `alpine:latest` | Image for KubeExecutor. |
| `K8S_NAMESPACE` | string | `default` | KubeExecutor namespace. |
| `RUNNER_CLASSES` | comma list | unset = claim **every** class | Runner segmentation: restrict this scheduler to claiming tasks whose `runner_class` is in the list (e.g. `etl,pulse`). Names validated like the spec field (`[a-z0-9_-]{1,64}`) — a typo is a startup error, not an unclaimable task class. Unset keeps the single-pool behavior. |
| `DB_MAX_CONNECTIONS` | u32 ≥ 2 | `8` | Postgres pool size (read in `dagron-core/src/db/postgres.rs`). Lower it (2–3) for lean engines sharing a pooled state cluster; min 2 keeps claim tx + listener from deadlocking. SQLite ignores it (pinned to 1 by design). |
| `DATABASE_LISTEN_URL` | postgres conn string | unset = listener shares the pool config | Split-DSN seam for shared state cells: the reconcile loop's `LISTEN` session connects here (the **direct** Postgres endpoint) while `DATABASE_URL` may point at PgBouncer transaction pooling — which cannot serve a session-scoped `LISTEN`. Postgres builds only. |
| `CRON_CONFIG` | path | unset = cron off | Cron schedule YAML (below). Leadership-gated; keeps the process resident. |
| `GC_RETENTION_SECS` | i64 > 0 | unset = GC off | Retention window for the run/task GC. Leadership-gated; resident. |
| `GC_INTERVAL_SECS` | u64 | `3600` | GC sweep interval. |
| `GC_ARCHIVE_DIR` | path | unset = plain purge | Archive-before-purge: the GC sweep exports each expired terminal run as a self-contained `dagron.run-archive.v1` JSON file (`run-<id>.json`: run + definition + tasks + outbox events; atomic tmp→fsync→rename) and purges **only** verified exports. Point it at an object-store-synced volume. |
| `GC_ARCHIVE_URL` | `s3://` \| `gs://` \| `az://` \| `azure://` `bucket[/prefix]` | unset | Cloud archive-before-purge (**requires the matching cargo feature** — `archive-s3` / `archive-gcs` / `archive-azure`; a scheme without its feature is a startup error, never a silent plain purge). Same document/purge contract as `GC_ARCHIVE_DIR`, but each run is one atomic object `PUT`; credentials/region/endpoint from the backend's standard env (`AWS_*` — incl. `AWS_ENDPOINT_URL` for MinIO — / `GOOGLE_*` / `AZURE_*`). Wins over `GC_ARCHIVE_DIR`. |
| `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS` | i64 | `30` | `dagron archive-compact` only: documents younger than this stay **individually retrievable** (`/api/archive/runs/{id}`); older ones fold into the Parquet dataset and become analytics-only. `0` compacts everything eligible. |
| `READY_AGE_ALERT_SECS` | i64 | `300` (`0` = off) | Stale-ready (unclaimable-class) alert: WARN when a runner class's oldest `ready` task has waited longer than this — catches a class no live scheduler serves. Leadership-gated; runs in any resident daemon. Same signal exported as `scheduler_ready_oldest_age_seconds{runner_class=…}`. |
| `READY_AGE_CHECK_INTERVAL_SECS` | u64 | `60` | How often the stale-ready alert loop checks. |
| `DB_SCHEDULES` | `1`/`true` | off | Fire DB-backed UI schedules (the ones `dagron-api` manages). Leadership-gated; resident. |
| `LEADER_LEASE_SECS` | i64 > 0 | `30` | Leadership lease for cron/GC/schedules (exactly-one-node guarantee). |
| `WORKFLOW_DIR` | path | `/workflows` | GitOps seed target: inside the container image, bundled examples are copied here on first start when empty. |
| `OPENLINEAGE_URL` | URL | unset = off | Emit an OpenLineage RunEvent per finalized run (`dagron-lineage`); best-effort. |
| `OPENLINEAGE_NAMESPACE` | string | `dagron` | OpenLineage namespace. |
| `DAGRON_ARTIFACT_DIR` | path | unset = off | Local artifact store root; each task gets its run's shared dir injected as `DAGRON_ARTIFACTS` (`dagron-artifact`). |
| `DAGRON_SENSITIVE_ENV_PATTERNS` | comma list | `SECRET,TOKEN,PASSWORD,PASSWD,PWD,CREDENTIAL,APIKEY,ACCESS_KEY,PRIVATE_KEY` | Task env var **name** substrings (case-insensitive) whose values are masked to `***` in task output/logs (secret masking, #8). Set empty to disable name-based masking. |
| `DAGRON_REDACT_ENV` | comma list | unset | Engine-process env var **names** whose values are always masked in task output (e.g. `DATABASE_URL`), on top of the name-pattern matching above. |
| `DAGRON_SECRET_<NAME>` | string | unset | Value for a task env `value_from: { secret: <name> }` reference (#9); `<name>` uppercased with non-alphanumerics → `_`. Resolved at dispatch; masked in output. |
| `DAGRON_SECRETS_DIR` | path | unset | Directory of secret files (one per secret, filename = secret name) for `value_from` refs — the SOPS / External-Secrets / k8s-secret mount convention. Checked after `DAGRON_SECRET_<NAME>`. |
| `DAGRON_ENV_SECRET_KEY` | string | unset = env-secret store off | Encryption key for **UI-managed environment secrets** (AES-256-GCM): 32 bytes of base64 used verbatim, any other string hashed to key length. Must be set identically on **both** dagron-api (encrypts on write) and the engine (decrypts at dispatch). The environment store is checked **before** `DAGRON_SECRET_<NAME>` / `DAGRON_SECRETS_DIR` for runs with an `environment:`. Helm: `envSecrets.*`; compose: the `x-env-secret-key` anchor. |
| `GITHUB_TOKEN` / `GITLAB_TOKEN` | token | unset = forge feedback off | Enables `notify.git` commit statuses (#14). `GITHUB_API_BASE` / `GITLAB_API_BASE` override the API base for GHE / self-managed GitLab. |
| `DAGRON_GIT_TOKEN` | token | unset | Token injected into `https://` clone URLs for the GitOps pull sync (#12); falls back to `GITHUB_TOKEN`. Injected **only for trusted forge hosts** (see `DAGRON_GIT_TRUSTED_HOSTS`) and redacted from any error output. |
| `DAGRON_GIT_TRUSTED_HOSTS` | comma-list | `github.com,gitlab.com` | Extra hosts (and their subdomains) the GitOps token may be sent to — add your GHE / self-managed GitLab host. A repo on any other host is cloned without the token. |
| `DAGRON_GIT_ALLOW_INSECURE` | bool | `false` | Allow `http://`, `git://`, and `file://` clone URLs for the GitOps sync. Off by default (only `https://` / `ssh://`) to avoid plaintext fetches, SSRF, and local-path reads; set `1` for `file://` in tests / air-gapped dev. |

## S3-compatible object storage (MinIO / Ceph) — air-gapped archive tier

`GC_ARCHIVE_URL`, the `dagron-api` archive endpoints, and `dagron archive-compact`
all build their S3 client from the standard `AWS_*` env (`object_store`), so an
on-prem MinIO/Ceph target needs **no code change** — only these variables:

| Variable | Example | What it does |
| --- | --- | --- |
| `AWS_ENDPOINT_URL` | `https://minio.storage.svc:9000` | Point the S3 client at MinIO/Ceph instead of AWS. Required for any non-AWS endpoint. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | Static credentials (on-prem has no IAM/IRSA). |
| `AWS_REGION` | `us-east-1` | A dummy region; some clients require it even for MinIO. |
| `AWS_ALLOW_HTTP` | `true` | Permit a plain-`http://` internal endpoint (omit for TLS). |

Path-style addressing (what MinIO/Ceph expect) is the `object_store` default — no
flag needed. For a TLS endpoint with a private CA use the CA-trust variables
below; **never** `AWS_ALLOW_INVALID_CERTIFICATES` in production.

## CA trust (private/internal certificates) — all HTTPS clients

Every HTTPS client — S3/MinIO, forge status, webhooks, OIDC/JWKS, the LLM
gateway — trusts the **system certificate store** (`rustls` native roots), so an
internal/corporate CA is honored once made available. Equivalent options:

| Variable | Example | What it does |
| --- | --- | --- |
| `SSL_CERT_FILE` | `/etc/dagron/ca-bundle.pem` | A CA bundle file trusted by every client. Simplest for a mounted Secret. |
| `SSL_CERT_DIR` | `/etc/ssl/certs` | A directory of CA certs (OpenSSL hashed layout). |

Or bake the CA into the image trust store (`update-ca-certificates`). No rebuild
of dagron is required — only the bundle.

## `dagron-api` (authenticated UI edge) environment

Read in `crates/dagron-api/src/main.rs` and `src/routes/{login,gitsync}.rs`.
Postgres-only service (SSE needs `LISTEN/NOTIFY`).

| Variable | Type | Default | What it does |
| --- | --- | --- | --- |
| `DATABASE_URL` | postgres conn string | **required** (startup error) | The same database the engine writes. |
| `DATABASE_LISTEN_URL` | postgres conn string | unset = SSE listener shares the pool config | Direct (non-PgBouncer) endpoint for the shared `task_events` SSE listener — same split-DSN seam as the engine (see the engine table). |
| `GC_ARCHIVE_DIR` / `GC_ARCHIVE_URL` | path / `s3://` \| `gs://` \| `az://` \| `azure://`… | unset = `/api/archive/runs/{id}` answers 502 | Where the `/api/archive` endpoints fetch archived run documents — the **same values the engine's GC uses**. A cloud URL needs dagron-api built with the matching feature (`archive-s3` / `archive-gcs` / `archive-azure`). The list endpoint reads only the `archived_runs` index and needs neither. |
| `DAGRON_JWT_SECRET` | string, **≥ 32 chars** | **required** (startup error if unset/short) | HS256 key that signs and validates session JWTs. |
| `PORT` | u16 | `8080` | Listen port. |
| `DAGRON_COOKIE_SECURE` | bool | `true` | `Secure` flag on the `dagron_session` cookie. Set `false`/`0`/`no` only for plain-HTTP local dev — a Secure cookie is not stored over `http://`. |
| `DAGRON_SESSION_TTL_SECS` | i64 > 0 | `604800` (7 days) | Session/JWT lifetime. |
| `DAGRON_ADMIN_EMAIL` / `DAGRON_ADMIN_PASSWORD` | string / string ≥ 8 | unset = no bootstrap | Idempotently seed a first admin at startup (never resets an existing user). |
| `DAGRON_ADMIN_NAME` | string | `Administrator` | Seeded admin display name. |
| `GITHUB_TOKEN` / `GIT_REPO` | PAT / `owner/name` | unset → `POST …/sync-to-git` answers `501` | Enable workflow → Git PR sync. |
| `GIT_BASE` | branch | `main` | Base branch for sync PRs. |
| `GIT_PATH_PREFIX` | path prefix | `dags/` | Where synced specs are committed. |
| `GIT_API_BASE` | URL | `https://api.github.com` | GitHub API root (GHE). |

## `dagron-mcp` environment

| Variable | Default | What it does |
| --- | --- | --- |
| `DAGRON_API_URL` | `http://localhost:8080` | The `dagron-api` edge the MCP adapter calls. |
| `DAGRON_MCP_TOKEN` | unset | Session JWT sent as `Authorization: Bearer` (mint one via login or `scripts/mint-dev-token.mjs`). |

## Logging (all binaries, via `dagron-logging`)

`RUST_LOG` (full tracing filter, wins over everything), `LOG_LEVEL` (default
`info`), `LOG_FORMAT` (`full`|`compact`|`pretty`|`json`, default `full`),
`LOG_TARGET` (default `1`), `LOG_THREAD_IDS` (`0`), `LOG_THREAD_NAMES` (`0`),
`LOG_LINE` (`0`), `LOG_SPAN_EVENTS` (default `none`), `LOG_ANSI` (auto;
forced off for `json`). The authoritative table is the doc comment at the top
of `crates/dagron-logging/src/lib.rs`.

## Cron config file (`CRON_CONFIG`)

```yaml
# crates/dagron-engine/src/cron.rs — RawConfig / RawEntry
schedules:
  - name: nightly-etl        # entry name (for logs)
    cron: "0 0 2 * * *"      # 6- or 7-field cron expression
    dag: examples/etl_demo.yaml   # path to the DAG YAML to submit
    timezone: America/New_York    # optional IANA zone (default UTC); DST-safe
    when: "{{ weekday }} <= 5"     # optional per-fire gate (weekdays only)
```

`timezone` (optional, default `UTC`) is the IANA zone the `cron` expression is
evaluated in — a `0 0 2 * * *` job with `timezone: America/New_York` fires at
02:00 New York wall-clock all year, so its UTC instant shifts by an hour across
DST. An unknown zone fails the whole cron config at load (fail-fast). The same
`timezone` field exists on UI-managed schedules (`POST/PUT /api/schedules`).

`when` (optional) is a per-fire conditional gate for conditions cron syntax
can't express: the fire is skipped when it evaluates false (only the next fire
time advances). It is one `LHS OP RHS` comparison (`== != <= >= < >`) over the
scheduled time's calendar fields, evaluated in the schedule's timezone:
`{{ hour }}` (0–23), `{{ minute }}`, `{{ day }}` (1–31), `{{ month }}` (1–12),
`{{ weekday }}` (1=Mon … 7=Sun), `{{ day_of_year }}`, `{{ days_in_month }}`.
Examples: `"{{ weekday }} <= 5"` (weekdays only), `"{{ day }} == {{ days_in_month }}"`
(last day of month), `"{{ hour }} != 3"` (skip the 03:00 fire). A malformed
gate fires anyway and logs a warning — a typo never silently stops a schedule.

UI-managed schedules additionally support a **`stopStrategy`** (`stop_expr`):
a comparison over this schedule's run outcome counts — `{{ succeeded }}`,
`{{ failed }}`, `{{ total }}` — evaluated before each fire; when true the
schedule auto-stops (disabled, with `stopped_at`/`stop_reason` surfaced in the
UI). Examples: `"{{ succeeded }} >= 1"` (run once), `"{{ failed }} >= 3"` (give
up after three failures). Re-enabling the schedule clears the stop record.
(`stop_expr` is a DB-schedule feature; the file-cron config supports `when` only.)

## Workflow YAML (per-task knobs)

The DAG format is documented in the [README](../README.md#workflow-format);
per-task fields (`dagron-core/src/dag.rs`, `TaskSpec`): `command` (argv list),
`depends_on`, `env`, `max_attempts` (default `1` = no retries),
`retry_delay_secs` (default `0`; actual delay `retry_delay_secs × 2^(attempt−1)`),
`timeout_secs` (default **25 s**, chosen to sit inside the 30 s task lease —
`dagron-executor/src/executor.rs`), plus executor extras (`docker_image`,
`resources`, `service_account`).

## Data formats & compatibility

- **State schema** = embedded sqlx migrations, applied automatically at
  startup: `crates/dagron-core/migrations/` (SQLite, 001–009) and
  `migrations_pg/` (Postgres, 001–012). Forward-only — there are no down
  migrations, so **back up before upgrading** (see
  [`OPERATIONS.md`](OPERATIONS.md#backup--restore--what-is-the-state)). `dagron-api` additionally
  ensures its own `users`/`git_repos` tables and the additive
  `workflows.description` column at boot, idempotently.
- **Workflow YAML** is validated before anything runs (duplicate names,
  unknown deps, cycles). Unknown persisted task specs are failed individually
  ("poison row"), never crash-loop the daemon.
- **No wire-format versioning** on the HTTP APIs today; the engine ops API is
  self-describing via `/openapi.yaml`.
