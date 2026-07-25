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
| `dev` (literal) | — | Zero-infra local quickstart: SQLite + the management API/Swagger on `127.0.0.1:8787` (sets `API_ADDR` if unset), stays resident. With no DAG file present it starts idle and waits for `POST /runs`. Requires the (default) `ops` feature. **`dev` consumes the first positional, so the others shift right — `dagron dev [DAG_PATH] [DB_TARGET]`.** `dagron dev foo.db` therefore reads `foo.db` as the *workflow* and still writes to `workflow.db`; the datastore is the **third** token (`dagron dev my.yaml my.db`), or — on a Postgres build only — `$DATABASE_URL` when that token is absent. The startup line prints the datastore actually in use; check it when a run seems to vanish. |
| `validate` (literal) | — | Offline spec lint: parses, template-expands, and graph-validates each `*.yaml`/`*.yml` (directories walked recursively; hidden dirs skipped) through the same pipeline every submit path uses. `--json` emits one JSON object per file; exits non-zero if any file fails. No datastore, no server, works in every build. |
| `DAG_PATH` | `examples/simple_dag.yaml` | Workflow YAML for the `file` source. **There is no `run` subcommand** — the first positional *is* the DAG path (the *second* under `dagron dev`, which consumes one token itself). |
| `DB_TARGET` | `workflow.db` (sqlite build) / `$DATABASE_URL` → `postgres://localhost/workflow` (postgres build) | SQLite file path, or Postgres connection string. Second positional — third under `dagron dev`. |

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
| `SOURCE` | `file` \| `stream` | `file` | Workflow ingestion source. `file` = one-shot DAG file; `stream` = follow an NDJSON event file / named pipe ([docs/STREAMING.md](STREAMING.md)). Managed broker connectors (`redis`/`sqs`/`kafka`/`nats`/`events`) are part of dagron Enterprise and error at startup here with a pointer (`dagron-source/src/source.rs`). |
| `STREAM_PATH` | path | — (required for `SOURCE=stream`) | NDJSON file or FIFO to follow; one workflow spec per line. A **directory** switches to sharded multi-consumer mode: each `*.ndjson` file is a partition, split across engines via per-partition leases (`source_partitions`), each shard with its own exactly-once cursor. |
| `STREAM_MODE` | `auto` \| `file` \| `sharded` | `auto` | How `STREAM_PATH` is interpreted. `auto` inspects the path at startup (dir → sharded, file/FIFO → single-stream) and **errors when the path does not exist** — mode is fixed at startup, so it is never guessed. `file` waits for a single stream file to appear; `sharded` requires the shard directory. |
| `STREAM_SUFFIX` | string | `.ndjson` | Sharded mode: which files in the directory are shards. |
| `STREAM_MAX_PARTITIONS` | i64 | unlimited | Sharded mode: max shards one engine holds — cap it so capacity spreads across consumers instead of one engine hoarding every shard. |
| `STREAM_FOLLOW` | bool | `true` | `false` = drain the file's backlog then exhaust (batch replay); `true` = keep following for new lines. |
| `STREAM_POLL_MS` | u64 (ms) | `500` | Poll interval while waiting at end-of-file. |
| `STREAM_OFFSET_PATH` | path | `<STREAM_PATH>.offset` | Committed-offset checkpoint (written atomically on ack; delete to replay). |
| `STREAM_DLQ_PATH` | path | `<STREAM_PATH>.dlq` | Poison-line mirror (NDJSON), alongside the durable `dead_letters` rows. |
| `TASK_LEASE_HEARTBEAT` | bool | `true` | Workers renew a running task's lease every 10 s (+30 s), so long tasks (training, consumers) are never reclaimed mid-run. `false` restores the old finish-inside-one-lease behaviour. |
| `RUNNER_GANGS` | `1`/`true` | off | Gang co-scheduling ([docs/AI_WORKLOADS.md](AI_WORKLOADS.md)): claim `gang:` tasks all-or-nothing and cancel a failed member's siblings. Requires a scheduler built with the `enterprise` feature; inert otherwise. Composes with `POOLS` and `priority`: gang members inherit the task's `pool`, and a pooled gang is claimed only when its pool can seat **every** member at once (never partially, never over the cap); ordinary claims on this path keep the same priority ordering. |
| `MAX_INFLIGHT_RUNS` | i64 | `64` (min 1) | Admission valve: cap on simultaneously active runs; overflow stays buffered at the source. The ops API answers `429` + `Retry-After` above it (`0` disables the API-side cap). |
| `DEAD_LETTER_MAX_ATTEMPTS` | i64 | `3` (min 1) | Transient `create_run` failures retried before a submission is dead-lettered (parse failures dead-letter immediately). |
| `DATABASE_URL` | conn string | `postgres://localhost/workflow` | Postgres builds only; positional `DB_TARGET` wins. Redacted before logging. |
| `API_ADDR` | `host:port` | unset = ops API **disabled** (`dagron dev` sets `127.0.0.1:8787`) | Bind address of the engine's unauthenticated ops API; also keeps the process resident. Invalid values warn and disable the API. |
| `DOCKER_IMAGE` | image ref | `alpine:latest` | Default image for `EXECUTOR=docker` (also k8s fallback). |
| `K8S_IMAGE` | image ref | `$DOCKER_IMAGE` → `alpine:latest` | Image for KubeExecutor. |
| `K8S_NAMESPACE` | string | `default` | KubeExecutor namespace. |
| `RUNNER_CLASSES` | comma list | unset = claim **every** class | Runner segmentation: restrict this scheduler to claiming tasks whose `runner_class` is in the list (e.g. `etl,pulse`). Names validated like the spec field (`[a-z0-9_-]{1,64}`) — a typo is a startup error, not an unclaimable task class. Unset keeps the single-pool behavior. |
| `POOLS` | `name:slots` comma list | unset = no pools | Named concurrency pools (#21): capacity per pool, e.g. `POOLS=etl:4,db:2`. A task's `pool:` draws a slot; the claim runs at most `slots` tasks of a pool at once, holding the rest in `ready` until one frees (no run dropped). Names validated like `runner_class`; a non-positive/unparseable slot count is a startup error. On Postgres, pooled claims serialize via a global advisory lock (the unpooled fast path stays lock-free); an unpooled or unconfigured-pool task is unlimited. Keep the value identical across HA replicas. |
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
| `WAIT_POLL_SECS` | u64 > 0 | `15` | Poll interval for `type: wait` HTTP sensors (`wait.url`, #27 follow-on): a parked sensor is GETed at most once per interval and succeeds on the first `2xx`. **Redirects are not followed** — the sensor reads the origin's own status, and following a 3xx would let an external URL pivot the *scheduler's* network position toward internal/metadata addresses; a 3xx simply reads as "not ready". Note `wait.url` polls run from the **scheduler**, not the task sandbox: treat workflow authorship as trusted with respect to the scheduler's network reachability, or set `WAIT_URL_DENY_PRIVATE` below. Time/dataset sensors ignore this. |
| `SUBWORKFLOW_MAX_DEPTH` | i64 > 0 | `8` | How deep `type: workflow` tasks may nest. A workflow that names itself — directly or through a cycle of workflows — would otherwise spawn child runs without end, each leaving a parked parent row behind; there is no stack to overflow, so nothing stops it on its own. A trigger at or past the cap **fails that task** with a message naming the depth, leaving the rest of the run to proceed under normal failure handling. Depth is read by walking `task_runs.sub_run_id` up from the triggering task's own run (no `parent_run_id` column; SQLite migration 034 / Postgres 041 index it), and the walk stops at the cap — so the check costs at most `SUBWORKFLOW_MAX_DEPTH` indexed lookups, on sub-workflow dispatch only. |
| `WAIT_URL_DENY_PRIVATE` | `1`/`true`/`on`/`yes` = on | off | Restrict `wait.url` polls to **globally-routable** addresses. Off by default because the common `wait.url` *is* an internal address (`http://svc.default.svc/ready`) — turn it on when you run untrusted workflow specs and the scheduler's network position is wider than a task pod's (realistically `EXECUTOR=kubernetes` with a differentiated NetworkPolicy). When on, private/loopback/link-local (incl. `169.254.169.254`), `0.0.0.0/8`, CGNAT, multicast and reserved ranges are refused, for IPv4, IPv6, and IPv4-mapped IPv6 alike. Enforced in two places: IP-literal hosts are checked before the request (a literal never reaches a resolver — and the check parses the URL with the *same* parser reqwest uses, so legacy spellings like `http://2130706433/` and `http://127.1/` are normalized before they are judged), and hostnames are filtered **inside the client's DNS resolver**, so the addresses dialed are the ones that passed — a DNS rebind has no check-to-connect window. Proxies are also bypassed while the policy is on: with `HTTP_PROXY`/`HTTPS_PROXY` set, the resolver would only ever judge the *proxy's* address while the proxy dialed the blocked target. A refused URL logs a WARN and re-parks as "not ready"; it does not fail the task, so turning the policy back off resolves parked sensors in place. Editing the spec does not — `wait.url` is materialized into `task_runs.wait_url` when the task row is created, so a spec fix applies to the next run, not to an already-parked task. |
| `DATASET_TRIGGERS` | `0`/`false` = off | on | Dataset-triggered scheduling sweep ([docs/DATASETS.md](DATASETS.md)): sync `on_datasets:` subscriptions from the workflow registry and fire runs when subscribed datasets update (~5 s cadence; HA-safe CAS claims, so every scheduler may sweep). Disabling only stops this scheduler's sweeping — `produces:` recording and dataset sensors are run-local and stay on. |
| `DB_SCHEDULES` | `1`/`true` | off | Fire DB-backed UI schedules (the ones `dagron-api` manages). Leadership-gated; resident. |
| `LEADER_LEASE_SECS` | i64 > 0 | `30` | Leadership lease for cron/GC/schedules (exactly-one-node guarantee). |
| `WORKFLOW_DIR` | path | `/workflows` | GitOps seed target: inside the container image, bundled examples are copied here on first start when empty. |
| `OPENLINEAGE_URL` | URL | unset = off | Emit an OpenLineage RunEvent per finalized run (`dagron-lineage`); best-effort. |
| `OPENLINEAGE_NAMESPACE` | string | `dagron` | OpenLineage namespace. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | URL | unset = export off | With an `otel`-feature build, enables OTLP (HTTP/protobuf) span export to this collector (#28 follow-on); other `OTEL_EXPORTER_OTLP_*` vars tune headers/timeouts. Ignored without `--features otel`. See [OpenTelemetry](#opentelemetry---features-otel). |
| `DAGRON_ARTIFACT_DIR` | path | unset = off | Local artifact store root; each task gets its run's shared dir injected as `DAGRON_ARTIFACTS`, plus a per-task `DAGRON_CHECKPOINT_DIR` under it for checkpoint-aware resume (`dagron-artifact`, [docs/AI_WORKLOADS.md](AI_WORKLOADS.md)). |
| `DAGRON_ARTIFACT_URL` | `s3://` \| `gs://` \| `az://` bucket/prefix | unset = off | Cloud artifact/checkpoint location. The engine injects per-run/per-task URLs (`DAGRON_ARTIFACTS_URL`, `DAGRON_CHECKPOINT_URL`) — tasks reach the bucket with their own tooling — and the artifact API serves the same bucket via the `dagron-artifact` cloud backend (features `s3`/`gcs`/`azure`; credentials from the standard `AWS_*`/`GOOGLE_*`/`AZURE_*` env; takes precedence over `DAGRON_ARTIFACT_DIR` for API reads/writes). |
| *(injected into tasks)* | — | — | The engine sets these **in every task's environment** at dispatch (they are not read from the operator's environment): `DAGRON_RUN_ID` / `DAGRON_TASK` / `DAGRON_TASK_ID` (task identity, e.g. for `POST /runs/{id}/tasks/{task_id}/checkpoint`); `DAGRON_ARTIFACTS` + `DAGRON_CHECKPOINT_DIR` (when the local artifact store is on); `DAGRON_ARTIFACTS_URL` + `DAGRON_CHECKPOINT_URL` (when `DAGRON_ARTIFACT_URL` is set); and, on retry attempts of a task that reported a checkpoint, `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER` ([docs/AI_WORKLOADS.md](AI_WORKLOADS.md)). |
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
| `DAGRON_PW_HASH_CONCURRENCY` | usize ≥ 1 | `2` | How many Argon2id hashes may run at once. Each needs **~19 MiB** of scratch (the OWASP-floor `m_cost`), so this is the memory ceiling for password work: permits × 19 MiB. Hashing runs on the blocking pool, never on an async worker. Raise it on a box with memory to spare and many concurrent logins. |
| `DAGRON_LOGIN_RATE_LIMIT` | u32 | `30` | Max `POST /api/login` attempts per client per window; `0` disables. The one unauthenticated route that costs an Argon2 verify per call (including for unknown emails, by design). Per replica and in memory — a cost ceiling and a guessing brake, not a distributed lockout policy. |
| `DAGRON_LOGIN_RATE_WINDOW_SECS` | u64 > 0 | `60` | Length of that window. |
| `DAGRON_TRUST_PROXY_HEADERS` | bool | `false` | Key the login rate limit off the first `X-Forwarded-For` hop instead of the socket peer. **Only** set this when something in front actually rewrites the header (the console's `/api` proxy, an ingress): a directly-reachable dagron-api lets any caller forge it and mint a fresh budget per request. Left off, every request behind a proxy shares one bucket — still a ceiling, just a global one. |
| `MALLOC_MMAP_THRESHOLD_` | bytes | unset (glibc auto-tunes) | Not read by dagron-api — glibc reads it. glibc raises its mmap threshold as it sees repeated frees of Argon2-sized blocks, after which that scratch comes off the heap and is never returned to the OS. Pinning it (`131072`) keeps every hash an mmap, released on free. Measured over 30 logins: unset 67 → 227 MB; pinned 5.6 → 5.9 MB. Set in `compose.yaml`. |
| `MALLOC_ARENA_MAX` | usize | unset (glibc default) | Also glibc's. `2` caps per-thread arenas, which lowers the idle baseline (67 → 5.6 MB here). It does **not** fix the post-hash plateau on its own — that needs the threshold above. |
| `GITHUB_TOKEN` / `GIT_REPO` | PAT / `owner/name` | unset → `POST …/sync-to-git` answers `501` | Enable workflow → Git PR sync. |
| `GIT_BASE` | branch | `main` | Base branch for sync PRs. |
| `GIT_PATH_PREFIX` | path prefix | `dags/` | Where synced specs are committed. |
| `GIT_API_BASE` | URL | `https://api.github.com` | GitHub API root (GHE). |

## Artifact encryption at rest (envelope / BYOK-KMS) & key rotation

> **Enterprise.** Envelope mode — data keys wrapped by a KEK you control — ships
> with dagron Enterprise. An open build that is handed a `DAGRON_ENV_KEK_PROVIDER`
> **refuses to start the path with a signpost** rather than quietly falling back,
> because silently downgrading a deployment that asked for KMS to a single env-var
> key is not a surprise anyone should find in a ciphertext dump.
>
> **Open in every build:** environment secrets encrypted with AES-256-GCM under
> `DAGRON_ENV_SECRET_KEY` (the `v1:` format — see [`HOWTO.md` §5](HOWTO.md)), and
> the plain artifact store (`DAGRON_ARTIFACT_DIR` / `DAGRON_ARTIFACT_URL`). Only
> the KEK layer above them is paid.

Read by `dagron-crypto` (used by `dagron-api` on write and `dagron-engine` on
decrypt). With a KEK provider set, artifacts (and DB secrets) are envelope-
encrypted — a fresh per-object data key wrapped by the KEK — so the operator never
holds the key that protects the data.

| Variable | Type | Default | What it does |
| --- | --- | --- | --- |
| `DAGRON_ARTIFACT_MAX_BYTES` | usize | `134217728` (128 MiB) | Max artifact PUT body (tower body limit on the artifact routes). |
| `DAGRON_ENV_KEK_PROVIDER` | `local`\|`command`\|`awskms`\|`gcpkms`\|`azurekv`\|`none` | unset = off (legacy `v1` single-key path) | Selects the KEK provider that wraps/unwraps data keys. `awskms`/`gcpkms`/`azurekv` require the crate built with `kms-aws`/`kms-gcp`/`kms-azure`. |
| `DAGRON_ENV_KEK` | 32-byte base64 or passphrase | required for `local` | The local KEK. |
| `DAGRON_ENV_KMS_ID` | string | `kms` | Provider id recorded in the ciphertext (must not contain `:`). |
| `DAGRON_ENV_KMS_WRAP_CMD` / `_UNWRAP_CMD` | command | required for `command` | Wrap/unwrap seam to any KMS/HSM/Vault (base64 stdin→stdout). |
| `DAGRON_ENV_KMS_TIMEOUT_SECS` | u64 > 0 | `30` | Per-call timeout for the `command` provider (kills a wedged wrapper). |
| `DAGRON_ENV_KMS_KEY_ID` | string | required for `awskms`/`gcpkms` | Cloud CMK resource id. |
| `DAGRON_ENV_KMS_VAULT_URL` | url | required for `azurekv` | Azure Key Vault URL. |
| `DAGRON_ENV_KMS_KEY_VERSION` | string | unset = latest | Azure Key Vault key version (optional). |
| `DAGRON_ENV_KEK_PROVIDER_OLD` **+ the matching `*_OLD` vars** | same shapes as above | unset | The **retiring** KEK, read by `POST /api/artifacts/rotate` to re-key every artifact onto the current KEK. Set `DAGRON_ENV_KEK_PROVIDER_OLD` plus whichever the old provider's kind needs: `DAGRON_ENV_KEK_OLD`, `DAGRON_ENV_KMS_ID_OLD`, `DAGRON_ENV_KMS_KEY_ID_OLD`, `DAGRON_ENV_KMS_VAULT_URL_OLD`, `DAGRON_ENV_KMS_KEY_VERSION_OLD`, `DAGRON_ENV_KMS_WRAP_CMD_OLD`/`_UNWRAP_CMD_OLD`. |

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
`retry_max_delay_secs` (backoff ceiling), `retry_on_timeout` (default `true`; set
`false` so a task killed by its `timeout_secs` deadline fails at once instead of
burning its remaining `max_attempts` — timeout-only, other failures still retry),
`timeout_secs` (default **25 s**, chosen to sit inside the 30 s task lease —
`dagron-executor/src/executor.rs`),
`priority` (default `0`; dispatch order among simultaneously-`ready` tasks —
higher is claimed first, `ORDER BY priority DESC, scheduled_at`; a pure tiebreak
that never overrides dependencies, and persists across retries),
`pool` (named concurrency pool — a scheduler claims a pooled task only while
fewer than the pool's capacity are running, capacities set via the `POOLS` env;
an over-budget task waits in `ready`, no run is dropped; unpooled/uncapped =
unlimited; `[a-z0-9_-]`, ≤64 chars),
`type: workflow` + `workflow: <name>` (sub-workflow trigger — submits the named
registered workflow as a child run and parks this task until the child is
terminal, succeeding/failing with it; no command),
`type: wait` + `wait: { for: <duration> | until: <rfc3339> | url: <http(s)> | dataset: <uri> }`
(deferrable sensor — parks holding **no worker slot**; `for`/`until` are a time
sensor that succeeds at the deadline (`for` relative and anchored when reached,
`until` absolute), `url` is an HTTP sensor the engine GETs every `WAIT_POLL_SECS`
(default 15 s) and succeeds on the first `2xx` — polled by the scheduler, without
following redirects, and optionally restricted to public addresses via
`WAIT_URL_DENY_PRIVATE`, `dataset` is a dataset sensor
that succeeds when the named dataset records an update **after** the park
([docs/DATASETS.md](DATASETS.md)); exactly one of the four; no command),
`cache: { key, max_age_secs? }` (result memoization — a successful run stores its
output under `(workflow, task, resolved key)`; a later task with the same key
reuses it and skips execution; `key` templates against params/`scheduled_time`,
so backfills are reproducible; `max_age_secs` expires stale entries),
`produces: [<dataset-uri>, …]` (datasets this task updates on success — recorded
in the registry + lineage ledger, waking dataset sensors and firing
`on_datasets:` workflows; command tasks only; URIs template per instance —
[docs/DATASETS.md](DATASETS.md)), plus
executor extras (`docker_image`,
`resources`, `service_account`, `runner_class`). A `task_defaults:` block sets
DAG-wide defaults for `max_attempts`, `retry_delay_secs`, `retry_max_delay_secs`,
`retry_on_timeout`, `timeout_secs`, `docker_image`, `runner_class`, `priority`,
`pool`, and `env` (a task's own value always wins).

DAG-level fields (`DagSpec`): `run_timeout_secs` (hard run deadline → cancel),
`deadline` (soft SLA → alert), `max_active_runs` (default unlimited; the max number
of runs of this workflow — by name — that may be `running` at once; further fires
are held back with a `MaxActiveRunsReached` error → the API returns **429**, queue
submissions requeue, and schedule/backfill fires wait for a slot), `result_from`,
`runner_class`, `environment`, `notify`, `templates`, `parameters`, and
`tags` (organizational labels, `[A-Za-z0-9_.-]` ≤64 chars each — the workflow
registry surfaces them on `GET /api/workflows` and filters with `?tag=<t>`; the
engine ignores them), and `on_datasets` (dataset triggers: fire a run of this
**registered** workflow when a subscribed dataset records an update, with the
trigger injected as `{{ trigger_dataset }}`; the open build subscribes **one**
dataset — multi-dataset composition with `datasets_mode: any|all` ships with
dagron Enterprise; [docs/DATASETS.md](DATASETS.md)).

## Data formats & compatibility

- **State schema** = embedded sqlx migrations, applied automatically at
  startup: `crates/dagron-core/migrations/` (SQLite, 001–033) and
  `migrations_pg/` (Postgres, 001–040). Forward-only — there are no down
  migrations, so **back up before upgrading** (see
  [`OPERATIONS.md`](OPERATIONS.md#backup--restore--what-is-the-state)). `dagron-api` additionally
  ensures its own `users`/`git_repos` tables and the additive
  `workflows.description` column at boot, idempotently.
- **Workflow YAML** is validated before anything runs (duplicate names,
  unknown deps, cycles). Unknown persisted task specs are failed individually
  ("poison row"), never crash-loop the daemon.
- **No wire-format versioning** on the HTTP APIs today; the engine ops API is
  self-describing via `/openapi.yaml`.

## OpenTelemetry (`--features otel`)

Build the engine with the `otel` Cargo feature for OpenTelemetry integration
(#28). Two things switch on, both **off by default** (a default build has no
opentelemetry dependency and unchanged behavior):

1. **Trace-context propagation** — a fresh W3C `traceparent` is injected into
   every dispatched task's environment (`TRACEPARENT`) and each dispatched task
   is wrapped in a `task.dispatch` span, so a task's own OpenTelemetry
   instrumentation joins an external trace (external-trace embedding). Active
   whenever the feature is built in.
2. **OTLP span export** (follow-on) — when `OTEL_EXPORTER_OTLP_ENDPOINT` is
   **also** set, an OTLP **HTTP/protobuf** span exporter + a
   `tracing-opentelemetry` bridge layer are installed by `dagron-logging`, so the
   engine's own `tracing` spans (the per-task `task.dispatch` span and anything
   nested) are delivered to a collector. The transport is HTTP/protobuf over a
   blocking `reqwest`/rustls client — the gRPC/tonic transport is deliberately
   not pulled in. Unset endpoint ⇒ exporter stays off (spans propagate but aren't
   exported).

Endpoint, headers, and timeouts come from the **standard
`OTEL_EXPORTER_OTLP_*` env vars** (e.g. `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_HEADERS`), so delivery is tuned per-deployment without a
rebuild; the service name is reported as `dagron-<service>`. The transport is fixed to
HTTP/protobuf — `OTEL_EXPORTER_OTLP_PROTOCOL` is **not** honored (the gRPC/tonic
stack is deliberately not compiled in). Dashboards,
sampling policy, and retention remain observability's per [`SCOPE.md`](SCOPE.md).
