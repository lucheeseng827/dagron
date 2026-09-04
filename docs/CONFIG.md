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
dagron config [--json]
dagron validate <file|dir>... [--json]
dagron archive-compact [DB_TARGET]
```

| Arg | Default | Meaning |
| --- | --- | --- |
| `dev` (literal) | — | Zero-infra local quickstart: SQLite + the management API/Swagger on `127.0.0.1:8787` (sets `API_ADDR` if unset), stays resident. With no DAG file present it starts idle and waits for `POST /runs`. Requires the (default) `ops` feature. **`dev` consumes the first positional, so the others shift right — `dagron dev [DAG_PATH] [DB_TARGET]`.** `dagron dev foo.db` therefore reads `foo.db` as the *workflow* and still writes to `workflow.db`; the datastore is the **third** token (`dagron dev my.yaml my.db`), or — on a Postgres build only — `$DATABASE_URL` when that token is absent. The startup line prints the datastore actually in use; check it when a run seems to vanish. |
| `config` (literal) | — | Print every knob's effective value, its source (env / file / profile / default), and the fleet fingerprint — exactly what a daemon started in this environment would run with. `--json` for machines. |
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
| `mqtt` | no | `SOURCE=mqtt` — the open MQTT ingestion source, for plant floors, gateways and robot fleets. Without it that value is a startup error naming the rebuild, never a silent downgrade. Links `rumqttc` with the same rustls/ring stack the rest of the binary uses, so it adds no second crypto provider. |
| `archive-s3` | no | Cloud GC archive sink over S3 (`GC_ARCHIVE_URL=s3://…`, incl. S3-compatible MinIO/Ceph via `AWS_ENDPOINT_URL`). A `GC_ARCHIVE_URL` scheme whose backend feature is absent is a startup error, never a silent downgrade — same contract as `kubernetes`. Implies `ops`. |
| `archive-gcs` | no | Google Cloud Storage archive sink (`GC_ARCHIVE_URL=gs://…`; credentials from `GOOGLE_*` env). Implies `ops`. |
| `archive-azure` | no | Azure Blob Storage archive sink (`GC_ARCHIVE_URL=az://…` or `azure://…`; credentials from `AZURE_*` env). Implies `ops`. |
| `archive-parquet` | no | `dagron archive-compact` — fold archived `run-*.json` documents into the date-partitioned Parquet dataset (`compact/tasks/dt=…/`). Heavy (arrow+parquet), hence its own feature; combine with a cloud backend (`archive-s3`/`-gcs`/`-azure`) to compact a cloud archive. Implies `ops`. |

## Engine (`dagron` binary) environment

All read in `crates/dagron-engine/src/lib.rs` unless noted.

| Variable | Type / values | Default | What it does |
| --- | --- | --- | --- |
| `DAGRON_CONFIG` | path to YAML | unset | Configuration **file** layered UNDER the environment — see [the section below](#configuration-file--profiles-dagron_config). |
| `DAGRON_CONFIG_NO_WARN` | set/unset | unset | Silence the startup typo scan (an env var that looks like a dagron knob but isn't one draws a warning). |
| `EXECUTOR` | `local` \| `docker` \| `kubernetes`/`k8s` | `local` | Task execution backend. Unrecognized values warn and fall back to `local`; `kubernetes` without the feature is a startup error. |
| `WORKER_COUNT` | usize | `16` (min 1) | Worker-pool size = max concurrently running tasks. |
| `POLL_INTERVAL_MS` | u64 (ms) | `500` (floor 10) | Reconcile-loop timer bound — the longest the loop may sleep with work outstanding. The loop also wakes the moment a local task completes and (Postgres) on any `LISTEN/NOTIFY` event, so this timer is a safety net there; on SQLite it additionally paces time-based retries and parked sensors. Part of the low-latency profile ([docs/LOW_LATENCY.md](LOW_LATENCY.md)). |
| `SWEEP_INTERVAL_MS` | u64 (ms) | = `POLL_INTERVAL_MS` (floor 10) | Cadence of the maintenance sweeps (expired-lease recovery, run deadlines, SLA alerts, approval expiry, sub-workflow / wait-sensor / dataset reconciliation). At the default this matches the old every-tick behaviour; profiles that shrink `POLL_INTERVAL_MS` set this back to ~`500` so faster ticks don't multiply sweep load. Sweeps only run when a tick runs, so the effective cadence is at least the time between ticks. |
| `LEASE_SECS` | i64 | `30` (floor 3) | Claim-time task lease window, shared by both backends and by the heartbeat's renewals. A crashed scheduler's tasks wait out this window before any peer reclaims them, so shortening it shortens crash recovery; the worker heartbeat renews every ⌊lease/3⌋ s (floor 1 s), keeping the two-missed-renewals headroom at any setting. With `TASK_LEASE_HEARTBEAT=false`, tasks must finish inside one window — keep it above your longest `timeout_secs`. |
| `MQTT_URL` | `mqtt://host[:port]` \| `mqtts://…` | `mqtt://127.0.0.1:1883` | `SOURCE=mqtt` only: the broker. `mqtts://` enables TLS with the platform root store (default port 8883, plain 1883). |
| `MQTT_TOPIC` | topic filter | `dagron/workflows` | Topic to subscribe to; MQTT wildcards (`+`, `#`) are allowed, so one unit can serve a whole cell. Each message payload is a workflow spec (YAML, or JSON — YAML is a superset). |
| `MQTT_CLIENT_ID` | string | `dagron-<uuid>` | MQTT client id. **Set a stable one per unit**: it is what makes offline redelivery possible, and it is what `MQTT_CLEAN_SESSION` defaults from. |
| `MQTT_QOS` | `0` \| `1` \| `2` | `1` | Subscription QoS. `0` cannot redeliver, so a message lost in a disconnect is gone; `1`/`2` are the durable choices. |
| `MQTT_USERNAME` | string | — | Broker username, when the broker authenticates. |
| `MQTT_PASSWORD` | string | — | Broker password. Never printed by `dagron config` (redacted like every other secret knob), and not trimmed — a trailing space may be part of the credential. |
| `MQTT_CLEAN_SESSION` | bool | `false` when `MQTT_CLIENT_ID` is set, else `true` | `false` keeps a persistent broker session, so QoS-1/2 messages published while the unit was offline are redelivered on reconnect — the whole point on a duty-cycled link. That only works if the broker sees the same client id again, so the default follows `MQTT_CLIENT_ID`: name one and the session resumes; leave it generated and the source starts clean rather than orphaning a session (and the messages queued against it) on every restart. `false` without a stable id is a startup error. |
| `MQTT_KEEPALIVE_SECS` | u64 (s) | `30` | PING interval. `0` disables keepalive, which is what a link that is metered by the byte usually wants. |
| `MQTT_DLQ_TOPIC` | topic | — (datastore `dead_letters` only) | Where a poison message is mirrored, in addition to the durable `dead_letters` row. It **must not** be matched by `MQTT_TOPIC`, or the source would consume its own dead letters and re-park them forever; that is a startup error, not a warning. The mirrored payload is truncated past 256 KiB (`truncated: true` on the envelope) — an envelope too large to encode would drop the connection and stall the subscription, and the durable row is the record that matters. |
| `MQTT_POSITION_FIELD` | field name | — (at-least-once) | Opt in to exactly-once. Names a top-level field in the payload carrying a per-topic monotonic id **the producer assigns in order** (Sparkplug's `seq`, a device's own counter — *not* a CloudEvents `id` or a UUID, which are unique but unordered, so a cursor built from one would skip messages); the source commits it with the run it creates and skips — acking, never re-running — anything at or below the committed cursor. Unset, delivery is at-least-once. |
| `SOURCE` | `file` \| `dir` \| `stream` \| `mqtt` | `file` | Workflow ingestion source. `file` = one-shot DAG file (emits once at startup, then drains); `dir` = **watch `WORKFLOW_DIR` for YAML** — a file added later runs, an edited file is re-submitted, and the process stays up because a watched directory is never exhausted; `stream` = follow an NDJSON event file / named pipe ([docs/STREAMING.md](STREAMING.md)). `mqtt` = subscribe to a broker topic and turn each message into a run (`--features mqtt`, see the `MQTT_*` rows). Managed broker connectors (`redis`/`sqs`/`kafka`/`nats`/`events`) and the fleet plane (`fleet`) are not in this build and error at startup with a pointer rather than starting a source you did not ask for (`dagron-source/src/source.rs`). |
| `DIR_POLL_MS` | u64 (ms) | `2000` (floor 100) | `SOURCE=dir` only: how often `WORKFLOW_DIR` is re-scanned. A poll, not a watch — inotify does not fire on the mounts this is for (Docker/Podman bind mounts from a Windows or macOS host, NFS/SMB shares). One `readdir` plus a `stat` per file per scan. Each file's (mtime, length) is committed with the run it becomes (`source_offsets`, keyed `dir/<file>`), so a restart re-runs nothing that already ran. |
| `STREAM_PATH` | path | — (required for `SOURCE=stream`) | NDJSON file or FIFO to follow; one workflow spec per line. A **directory** switches to sharded multi-consumer mode: each `*.ndjson` file is a partition, split across engines via per-partition leases (`source_partitions`), each shard with its own exactly-once cursor. |
| `STREAM_MODE` | `auto` \| `file` \| `sharded` | `auto` | How `STREAM_PATH` is interpreted. `auto` inspects the path at startup (dir → sharded, file/FIFO → single-stream) and **errors when the path does not exist** — mode is fixed at startup, so it is never guessed. `file` waits for a single stream file to appear; `sharded` requires the shard directory. |
| `STREAM_SUFFIX` | string | `.ndjson` | Sharded mode: which files in the directory are shards. |
| `STREAM_MAX_PARTITIONS` | i64 | unlimited | Sharded mode: max shards one engine holds — cap it so capacity spreads across consumers instead of one engine hoarding every shard. |
| `STREAM_FOLLOW` | bool | `true` | `false` = drain the file's backlog then exhaust (batch replay); `true` = keep following for new lines. |
| `STREAM_POLL_MS` | u64 (ms) | `500` | Poll interval while waiting at end-of-file. |
| `STREAM_OFFSET_PATH` | path | `<STREAM_PATH>.offset` | Committed-offset checkpoint (written atomically on ack; delete to replay). |
| `STREAM_DLQ_PATH` | path | `<STREAM_PATH>.dlq` | Poison-line mirror (NDJSON), alongside the durable `dead_letters` rows. |
| `TASK_LEASE_HEARTBEAT` | bool | `true` | Workers renew a running task's lease every ⌊`LEASE_SECS`/3⌋ s (+`LEASE_SECS`, so 10 s +30 s at defaults) while it executes, so long tasks (training, consumers) are never reclaimed mid-run. `false` restores the old finish-inside-one-lease behaviour. |
| `RUNNER_GANGS` | `1`/`true` | off | Gang co-scheduling ([docs/AI_WORKLOADS.md](AI_WORKLOADS.md)): claim `gang:` tasks all-or-nothing and cancel a failed member's siblings. Requires a scheduler built with the `enterprise` feature; inert otherwise. Composes with `POOLS` and `priority`: gang members inherit the task's `pool`, and a pooled gang is claimed only when its pool can seat **every** member at once (never partially, never over the cap); ordinary claims on this path keep the same priority ordering. |
| `DAGRON_PRESSURE_FILE` | path | — (no gate) | Back-pressure gate for constrained hosts: while this file exists the engine claims **no new tasks** (a file whose first token is `0`, `false`, `off` or `resume` counts as absent, so an agent can flip it without deleting it). In-flight tasks finish and every other loop keeps running; only claiming stops. A thermal, battery or maintenance daemon owns the file. Note a one-shot `dagron <file>.yaml` will not exit while the gate is closed — its runs are still active. |
| `DAGRON_MIN_FREE_BYTES` | u64 (bytes) | `0` (off) | Free-space floor on the SQLite datastore's filesystem. Below it, **new run creation is refused** rather than risking a half-written datastore: the ops API answers `507` + `Retry-After`, and the ingest source is throttled instead of dead-lettering the payload. Only the embedded backend enforces it; with Postgres the disk belongs to the database server. If the probe itself fails, the floor fails open with one warning. The `edge` profile sets 64 MiB. |
| `DAGRON_CLOCK_CHECK_SECS` | u64 (s) | `30` (0 disables) | How often the wall clock is compared against the monotonic clock to detect a step. |
| `DAGRON_CLOCK_STEP_TOLERANCE_MS` | u64 (ms) | `1000` | A wall-vs-monotonic disagreement above this is a step: the engine records `drifted` on runs it is executing and stamps new ones the same way. |
| `DAGRON_CLOCK_SYNC_FILE` | path | — (no positive evidence) | A file whose existence means the clock is disciplined (e.g. `/run/systemd/timesync/synchronized`). Present ⇒ runs are stamped `synced`; absent ⇒ `unknown`, which is honest rather than optimistic. Re-checked every tick. |
| `MAX_INFLIGHT_RUNS` | i64 | `64` | Admission valve: cap on simultaneously active runs; overflow stays buffered at the source. The ops API answers `429` + `Retry-After` above it. **`0` disables the cap** on both admission paths (the API check and the ingest actor's throttle); a negative value is normalized to `0`. |
| `MAX_INFLIGHT_TASKS` | i64 | `0` (off) | Second admission dimension, on TASKS. Runs are the wrong unit on their own: a run of 100,000 tasks and a run of four both count as one against `MAX_INFLIGHT_RUNS`, so a fleet comfortably under the run cap can be far past what the scheduler and datastore carry. Counts `task_runs` in `pending`/`ready`/`running` (a task parked on an approval is not pressure and is excluded). Only queried when set, so the default path pays no extra round trip. |
| `DAGRON_MAX_TASKS_PER_RUN` | usize | compiled ceiling (100000) | Per-run cap on the EXPANDED graph, enforced as a budget during expansion so a fan-out blow-up fails loudly instead of exhausting memory. May only LOWER the compiled ceiling — a larger value is ignored, because this knob exists to tighten a bound that prevents an OOM, not to widen it. |
| `DEAD_LETTER_MAX_ATTEMPTS` | i64 | `3` (min 1) | Transient `create_run` failures retried before a submission is dead-lettered (parse failures dead-letter immediately). |
| `DATABASE_URL` | conn string | `postgres://localhost/workflow` | Postgres builds only; positional `DB_TARGET` wins. Redacted before logging. |
| `API_ADDR` | `host:port` | unset = ops API **disabled** (`dagron dev` sets `127.0.0.1:8787`) | Bind address of the engine's unauthenticated ops API; also keeps the process resident. Invalid values warn and disable the API. |
| `DAGRON_READY_TIMEOUT_MS` | u64 ≥ 50 | `500` | Budget for the ops API's `/readyz` datastore ping — 503 `datastore probe timed out` past it, so a wedged pool never hangs the probe past the kubelet's `timeoutSeconds`. Same knob (name and semantics) as dagron-api's. |
| `DAGRON_CONSOLE` | `off`/`false`/`0`/`no` = unmount | unset = console served | Serves the operator console at `/` and `/console` on the ops API. Opt **out**, not in: the API those pages drive is on that socket either way, so unmounting the UI hides it and closes nothing. An ops API you need closed needs a network boundary — see `API_ADDR` above. |
| `DOCKER_IMAGE` | image ref | `alpine:latest` | Default image for `EXECUTOR=docker` (also k8s fallback). |
| `K8S_IMAGE` | image ref | `$DOCKER_IMAGE` → `alpine:latest` | Image for KubeExecutor. |
| `K8S_NAMESPACE` | string | `default` | KubeExecutor namespace. |
| `DAGRON_MAX_TASK_TIMEOUT_SECS` | u64 > 0 | unset = **no ceiling** | Upper bound on any single task's wall clock, applied by every executor (local, docker, k8s). A task's own `timeout_secs` is a *request*: it is clamped to this, and so is the 25 s default. Unset changes nothing, which is right for a self-host — it is your hardware. A multi-tenant install wants it set, because plan quotas cap tasks per day and runs per month while nothing capped how long one task may run, leaving worst-case compute per plan unbounded. A value that is not a positive integer (including `0`, which would time out every task instantly) is ignored with a warning rather than silently becoming a ceiling that is not one. |
| `DAGRON_TASK_RUN_AS_USER` | uid | unset | KubeExecutor: stamp `runAsUser` (and `runAsNonRoot` when > 0) onto every task pod. Unset leaves the pod's user to the image, which is what happened before this existed — a task image's final `USER root` ran as root. |
| `DAGRON_TASK_READ_ONLY_ROOT_FS` | `1`/`true` = on | off | KubeExecutor: `readOnlyRootFilesystem` + `allowPrivilegeEscalation: false` on task containers. |
| `DAGRON_TASK_DROP_ALL_CAPABILITIES` | `1`/`true` = on | off | KubeExecutor: `capabilities.drop: [ALL]` on task containers. |
| `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT` | `1`/`true` = on | off | KubeExecutor: `seccompProfile: RuntimeDefault`. |
| `DAGRON_TASK_ACTIVE_DEADLINE_SECS` | u64 > 0 | unset | KubeExecutor: `activeDeadlineSeconds` on task pods. Without it a hung task holds a node slot until the run-level timeout notices. |
| `DAGRON_TASK_RUNTIME_CLASS` | string | unset | KubeExecutor: `runtimeClassName` (e.g. `gvisor`) — kernel-surface isolation for untrusted task images. |
| `DAGRON_TASK_NODE_SELECTOR` | `k=v,k=v` | unset | KubeExecutor: pin task pods to specific nodes, e.g. an untrusted-workload pool. |
| `DAGRON_TASK_AUTOMOUNT_SA_TOKEN` | `1`/`true` = on | **off** | KubeExecutor: mount a ServiceAccount token into task pods that did NOT declare `service_account:`. **This is the one default that changed**: such a task never asked for an identity, and on an IRSA cluster the token it was being handed is an IAM credential given to arbitrary task code. Set it to restore the old behaviour. Tasks that DO declare `service_account:` are unaffected and keep their token. |
| `RUNNER_CLASSES` | comma list | unset = claim **every** class | Runner segmentation: restrict this scheduler to claiming tasks whose `runner_class` is in the list (e.g. `etl,pulse`). Names validated like the spec field (`[a-z0-9_-]{1,64}`) — a typo is a startup error, not an unclaimable task class. Unset keeps the single-pool behavior. |
| `POOLS` | `name:slots` comma list | unset = no pools | Named concurrency pools (#21): capacity per pool, e.g. `POOLS=etl:4,db:2`. A task's `pool:` draws a slot; the claim runs at most `slots` tasks of a pool at once, holding the rest in `ready` until one frees (no run dropped). Names validated like `runner_class`; a non-positive/unparseable slot count is a startup error. On Postgres, pooled claims serialize via a global advisory lock (the unpooled fast path stays lock-free); an unpooled or unconfigured-pool task is unlimited. Keep the value identical across HA replicas. |
| `DB_MAX_CONNECTIONS` | u32 ≥ 2 | `8` | Postgres pool size (read in `dagron-core/src/db/postgres.rs`). Lower it (2–3) for lean engines sharing a pooled state cluster; min 2 keeps claim tx + listener from deadlocking. SQLite ignores it (pinned to 1 by design). |
| `DATABASE_LISTEN_URL` | postgres conn string | unset = listener shares the pool config | Split-DSN seam for shared state cells: the reconcile loop's `LISTEN` session connects here (the **direct** Postgres endpoint) while `DATABASE_URL` may point at PgBouncer transaction pooling — which cannot serve a session-scoped `LISTEN`. Postgres builds only. |
| `CRON_CONFIG` | path | unset = cron off | Cron schedule YAML (below). Leadership-gated; keeps the process resident. |
| `GC_RETENTION_SECS` | i64 > 0 | unset = GC off | Retention window for the run/task GC. Leadership-gated; resident. |
| `GC_INTERVAL_SECS` | u64 | `3600` | GC sweep interval. |
| `GC_ARCHIVE_DIR` | path | unset = plain purge | Archive-before-purge: the GC sweep exports each expired terminal run as a self-contained `dagron.run-archive.v1` JSON file (`run-<id>.json`: run + definition + tasks + outbox events; atomic tmp→fsync→rename) and purges **only** verified exports. Point it at an object-store-synced volume. **Set it on `dagron-api` too** — the same path/bucket — or the console cannot read archived runs back, and `POST /api/runs/{id}/archive` answers `501` rather than archiving. |
| `GC_ARCHIVE_URL` | `s3://` \| `gs://` \| `az://` \| `azure://` `bucket[/prefix]` | unset | Cloud archive-before-purge (**requires the matching cargo feature** — `archive-s3` / `archive-gcs` / `archive-azure`; a scheme without its feature is a startup error, never a silent plain purge). Same document/purge contract as `GC_ARCHIVE_DIR`, but each run is one atomic object `PUT`; credentials/region/endpoint from the backend's standard env (`AWS_*` — incl. `AWS_ENDPOINT_URL` for MinIO — / `GOOGLE_*` / `AZURE_*`). Wins over `GC_ARCHIVE_DIR`. **Set it on `dagron-api` too** — the same bucket/prefix, and that binary built with the matching feature — or `POST /api/runs/{id}/archive` answers `501`, and the console cannot read archived runs back. |
| `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS` | i64 | `30` | `dagron archive-compact` only: documents younger than this stay **individually retrievable** (`/api/archive/runs/{id}`); older ones fold into the Parquet dataset and become analytics-only. `0` compacts everything eligible. |
| `READY_AGE_ALERT_SECS` | i64 | `300` (`0` = off) | Stale-ready (unclaimable-class) alert: WARN when a runner class's oldest `ready` task has waited longer than this — catches a class no live scheduler serves. Leadership-gated; runs in any resident daemon. Same signal exported as `scheduler_ready_oldest_age_seconds{runner_class=…}`. |
| `READY_AGE_CHECK_INTERVAL_SECS` | u64 | `60` | How often the stale-ready alert loop checks. |
| `WAIT_POLL_SECS` | u64 > 0 | `15` | Poll interval for `type: wait` HTTP sensors (`wait.url`, #27 follow-on): a parked sensor is GETed at most once per interval and succeeds on the first `2xx`. **Redirects are not followed** — the sensor reads the origin's own status, and following a 3xx would let an external URL pivot the *scheduler's* network position toward internal/metadata addresses; a 3xx simply reads as "not ready". Note `wait.url` polls run from the **scheduler**, not the task sandbox: treat workflow authorship as trusted with respect to the scheduler's network reachability, or set `WAIT_URL_DENY_PRIVATE` below. Time/dataset sensors ignore this. |
| `SUBWORKFLOW_MAX_DEPTH` | i64 > 0 | `8` | How deep `type: workflow` tasks may nest. **Depth is not breadth**: a `repeat:` on a trigger creates one child run per iteration at the *same* depth, so this cap does not bound a loop — `repeat.max_iterations` does, and it is required. See [Loops over sub-workflows](#loops-over-sub-workflows). A workflow that names itself — directly or through a cycle of workflows — would otherwise spawn child runs without end, each leaving a parked parent row behind; there is no stack to overflow, so nothing stops it on its own. A trigger at or past the cap **fails that task** with a message naming the depth, leaving the rest of the run to proceed under normal failure handling. Depth is read by walking `task_runs.sub_run_id` up from the triggering task's own run (no `parent_run_id` column; SQLite migration 034 / Postgres 041 index it), and the walk stops at the cap — so the check costs at most `SUBWORKFLOW_MAX_DEPTH` indexed lookups, on sub-workflow dispatch only. |
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
| `DAGRON_ARTIFACT_TIER` | bool | — (tiering off) | Turn the artifact store into a **tiered** one: every write lands locally first and is uploaded to `DAGRON_ARTIFACT_URL` later, under a budget. Needs both `DAGRON_ARTIFACT_DIR` and `DAGRON_ARTIFACT_URL` — asking for tiering with only one of them is a startup error, never a silent downgrade. For units that capture data on a metered or intermittent link. |
| `DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY` | u64 (bytes) | — (unlimited) | Daily uplink budget for the tiered store, counted in UTC days and persisted, so a restart does not hand the unit a fresh allowance. An artifact larger than the whole budget is skipped with a warning rather than blocking everything queued behind it. |
| `DAGRON_BUNDLE_PUBKEYS` | comma-separated keys | — (bundles refused) | Trusted ed25519 public keys (32 bytes, hex or base64) for **signed workflow bundles** ([docs/BUNDLES.md](BUNDLES.md)). Verification fails closed: unset means every bundle is refused, and there is no unsigned path. |
| *(injected into tasks)* | — | — | The engine sets these **in every task's environment** at dispatch (they are not read from the operator's environment): `DAGRON_RUN_ID` / `DAGRON_TASK` / `DAGRON_TASK_ID` (task identity, e.g. for `POST /runs/{id}/tasks/{task_id}/checkpoint`); `DAGRON_ARTIFACTS` + `DAGRON_CHECKPOINT_DIR` (when the local artifact store is on); `DAGRON_ARTIFACTS_URL` + `DAGRON_CHECKPOINT_URL` (when `DAGRON_ARTIFACT_URL` is set); and, on retry attempts of a task that reported a checkpoint, `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER` ([docs/AI_WORKLOADS.md](AI_WORKLOADS.md)). |
| `DAGRON_SENSITIVE_ENV_PATTERNS` | comma list | `SECRET,TOKEN,PASSWORD,PASSWD,PWD,CREDENTIAL,APIKEY,ACCESS_KEY,PRIVATE_KEY` | Task env var **name** substrings (case-insensitive) whose values are masked to `***` in task output/logs (secret masking, #8). Set empty to disable name-based masking. |
| `DAGRON_REDACT_ENV` | comma list | unset | Engine-process env var **names** whose values are always masked in task output (e.g. `DATABASE_URL`), on top of the name-pattern matching above. |
| `DAGRON_SECRET_<NAME>` | string | unset | Value for a task env `value_from: { secret: <name> }` reference (#9); `<name>` uppercased with non-alphanumerics → `_`. Resolved at dispatch; masked in output. |
| `DAGRON_SECRETS_DIR` | path | unset | Directory of secret files (one per secret, filename = secret name) for `value_from` refs — the SOPS / External-Secrets / k8s-secret mount convention. Checked after `DAGRON_SECRET_<NAME>`. |
| `DAGRON_ENV_SECRET_KEY` | string | unset = env-secret store off | Encryption key for **UI-managed environment secrets** *and* **per-repository GitOps credentials** (AES-256-GCM): 32 bytes of base64 used verbatim, any other string hashed to key length. Must be set identically on dagron-api (encrypts on write), the engine (decrypts secrets at dispatch), and the `dagron-gitops` worker (decrypts Git credentials at clone). The environment store is checked **before** `DAGRON_SECRET_<NAME>` / `DAGRON_SECRETS_DIR` for runs with an `environment:`. Helm: `envSecrets.*`; compose: the `x-env-secret-key` anchor. |
| `GITHUB_TOKEN` / `GITLAB_TOKEN` | token | unset = forge feedback off | Enables `notify.git` commit statuses (#14). `GITHUB_API_BASE` / `GITLAB_API_BASE` override the API base for GHE / self-managed GitLab. |
| `DAGRON_GIT_TOKEN` | token | unset | **Fallback** token for the GitOps pull sync (#12), used by repos with no credential of their own; falls back to `GITHUB_TOKEN`. Sent **only to trusted forge hosts** (see `DAGRON_GIT_TRUSTED_HOSTS`) and redacted from any error output. Read by the `dagron-gitops` worker. Prefer a **per-repository** credential (console → GitOps → Credential, or `PUT /api/git-repos/{id}/auth`): it is scoped to one repo, rotatable without a redeploy, and can be an SSH key. |
| `DAGRON_GIT_TRUSTED_HOSTS` | comma-list | unset | Extra hosts (and their subdomains) the **fallback** token may be sent to — add your GHE / self-managed GitLab host. Built-ins (`github.com`, `gitlab.com`, `bitbucket.org` + `*.github.com`, `*.gitlab.com`) always apply. A repo on any other host is cloned without the token — unless it has a per-repository credential, which is not host-filtered because it was bound to that repo deliberately. |
| `DAGRON_GIT_SSH_STRICT` | bool | `false` | Refuse to sync an SSH repository whose credential has no `known_hosts` entries, instead of accepting whatever host key answers. Off by default so repos connected without host keys keep syncing; turn it on to require the forge's host key be pinned. Read by the `dagron-gitops` worker. |
| `DAGRON_GIT_ALLOW_INSECURE` | bool | `false` | Allow `http://`, `git://`, and `file://` clone URLs for the GitOps sync. Off by default (only `https://` / `ssh://`) to avoid plaintext fetches, SSRF, and local-path reads; set `1` for `file://` in tests / air-gapped dev. |

## Configuration file & profiles (`DAGRON_CONFIG`)

The engine's knobs can come from a **reviewed YAML file** instead of loose env
vars (docs/LOW_LATENCY.md §5). Keys are the exact env var names; an optional
`profile:` applies a named preset underneath. Precedence, highest first:
**environment → file → profile → compiled default** — an explicit env var
always wins, so a container override still works.

```yaml
# /etc/dagron/dagron.yaml   (DAGRON_CONFIG=/etc/dagron/dagron.yaml)
profile: low-latency        # POLL_INTERVAL_MS=25 · SWEEP_INTERVAL_MS=500 · LEASE_SECS=5
WORKER_COUNT: 64
MAX_INFLIGHT_RUNS: 256
```

| Profile | Presets | Intent |
| --- | --- | --- |
| `low-latency` | `POLL_INTERVAL_MS=25`, `SWEEP_INTERVAL_MS=500`, `LEASE_SECS=5` | The trading-desk engine tuning ([docs/LOW_LATENCY.md §6](LOW_LATENCY.md)); pair with `RUNNER_CLASSES` pulse segmentation and a same-AZ Postgres. |
| `edge` | `WORKER_COUNT=2`, `POLL_INTERVAL_MS=1000`, `SWEEP_INTERVAL_MS=5000`, `MAX_INFLIGHT_RUNS=4`, `MAX_INFLIGHT_TASKS=64`, `DAGRON_MIN_FREE_BYTES=67108864` | Constrained gateways, robots and vehicles ([docs/EDGE_PROFILE.md](EDGE_PROFILE.md)): few workers, slow ticks, small in-flight caps, and a 64 MiB free-disk floor so a full flash device refuses new runs instead of corrupting its datastore. |
| `throughput` | *(none — stock defaults)* | Declares the intent in a reviewed file without changing anything. |

An unknown key or unknown profile in the file is a **startup error** (a
reviewed file has no excuse for a typo); an env var that merely *looks* like a
knob warns instead. Introspection: `dagron config [--json]` prints every
knob's effective value (secrets redacted) with its source
(`env`/`file`/`profile`/`default`) and the **fleet fingerprint**; the ops API
serves the same at `GET /config`, and startup logs the fingerprint — two
replicas with the same fingerprint run the same knob values. Deliberately no
hot reload: settings are boot-immutable (the audit story); the short runtime
allow-list stays proposed (LOW_LATENCY S-5).

Some `DAGRON_*` families belong to *other* dagron components that legitimately
share a shell with the engine — the API edge's `DAGRON_JWT_*`, the MCP adapter's
`DAGRON_MCP_*`, and `DAGRON_FLEET_*`, which an optional sidecar worker reads and
the engine never does. Those are registered as foreign and the scan stays quiet
about them; they are documented by the component that owns them, not here.

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
| `DAGRON_DB_MAX_CONNECTIONS` | u32 ≥ 1 | `8` | Postgres pool ceiling (was hard-coded). |
| `DAGRON_DB_MIN_CONNECTIONS` | u32 | `0` | Warm floor: the pool keeps this many connections open, and startup acquires them **eagerly** before the process reports Ready — market open never pays TLS + auth handshakes on the first requests (docs/LOW_LATENCY.md R-2). |
| `DAGRON_DB_ACQUIRE_TIMEOUT_MS` | u64 ≥ 50 | `10000` | How long a request may queue for a pooled connection. The low-latency profile sets ~`250`: a saturated pool should fail fast, not hold callers for ten seconds. |
| `DAGRON_DB_TEST_BEFORE_ACQUIRE` | bool | `true` | sqlx's per-checkout liveness round trip. `false` removes one round trip per checkout and lets the query itself surface a dead connection. |
| `DAGRON_READY_TIMEOUT_MS` | u64 ≥ 50 | `500` | `/readyz`'s own budget for the pooled `SELECT 1`, independent of the acquire timeout so the probe never outlasts the kubelet's patience — keep any probe `timeoutSeconds` **above** this budget so the app answers 503-with-reason instead of the kubelet cutting the probe. `/readyz` answers 503 on a failed acquire or a failed query; `/healthz` stays the bare liveness probe. (The engine's ops API reads the same knob for its `/readyz` datastore ping.) |
| `DAGRON_READY_REQUIRE_LISTENER` | bool | `false` | Make `/readyz` also 503 while the SSE listener is not subscribed. Off by default: the listener watches **one shared** direct endpoint (`DATABASE_LISTEN_URL`), so its failure hits every replica at once — gating on it cannot reroute, it can only empty the Service while login/listing/submit still work. Degraded live events are visible as `event_listener` in `GET /api/health` and in `/readyz`'s body. Opt in only with per-replica listen endpoints, where eviction genuinely reroutes. |
| `DAGRON_SHUTDOWN_DRAIN_MS` | u64 | `15000` | Bound on the graceful-shutdown drain: after SIGTERM/ctrl-c, in-flight requests get this long to finish before remaining connections (idle keep-alives, open SSE streams, `/wait` long-polls — which never end on their own) are closed. Keep it under the pod's `terminationGracePeriodSeconds` (default 30 s) so the orderly close always beats the SIGKILL. |
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
| `DAGRON_GIT_ALLOW_INSECURE` | bool | `false` | Permit `http`/`git`/`file` clone transports for registered GitOps repos (tests / air-gapped dev only — off so a server-side clone can't be pointed at plaintext, an internal host, or a local path). |
| `DAGRON_ARTIFACT_MAX_BYTES` | usize | `134217728` (128 MiB) | Body cap for artifact PUTs (separately from the 1 MiB core cap). |
| `DAGRON_ARTIFACT_SYNC_SECS` | u64 (s) | `60` (0 disables) | How often the tiered artifact store drains to its remote tier (`DAGRON_ARTIFACT_TIER`). A no-op for every other store, so the loop is harmless when tiering is off; `POST /api/artifacts/sync` is the on-demand path for a unit that has just docked. |
| `DAGRON_AUDIT_SINK_URL` / `DAGRON_AUDIT_SINK_TOKEN` | URL / bearer | unset = local audit table only | Read where the `enterprise` feature is on: forward each audit record to a central sink (fire-and-forget). |

Every knob above is registered in `crates/dagron-api/src/config.rs`: startup
logs each explicitly-set value (secrets redacted) plus a **configuration
fingerprint** (also in `GET /api/health` as `config_fingerprint`) so a fleet
can alert when one replica's settings drift from the reviewed deployment.

## Artifact encryption at rest (envelope / BYOK-KMS) & key rotation

> **Not in this build.** Envelope mode — data keys wrapped by a KEK you
> control — is not implemented here. A build handed a `DAGRON_ENV_KEK_PROVIDER`
> **refuses to start the path with a signpost** rather than quietly falling back,
> because silently downgrading a deployment that asked for KMS to a single env-var
> key is not a surprise anyone should find in a ciphertext dump.
>
> **Present in this build:** environment secrets encrypted with AES-256-GCM
> under `DAGRON_ENV_SECRET_KEY` (the `v1:` format — see
> [`HOWTO.md` §5](HOWTO.md)), and the plain artifact store
> (`DAGRON_ARTIFACT_DIR` / `DAGRON_ARTIFACT_URL`). Only the KEK layer above
> them is missing.

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
`retry_budgets: { <fault-class>: <attempts>, … }` (per-fault-class attempt
budgets — how many attempts this task gets *given what broke*, so an ECC error
and a NaN loss do not draw from the same three. Resolved most-specific first:
this map's entry for the class that occurred, then the class's disposition
default (infrastructure **5**, platform **3**, application **1**), then
`max_attempts`. That fallback is **not a ceiling over the classified cases**:
failures classify from their own output with no opt-in, so a task that never
sets this field still takes the disposition default when its failure matches a
class — `max_attempts: 3` runs 5 times for a `gpu-ecc` and once for a
`nan-loss`. That is the feature working, but it does move attempt counts on
existing workflows; name a class here to pin it back to a number you choose.
`max_attempts` applies in two cases: a failure that classified as nothing, and
one whose class carries the *unknown* disposition (`nccl-timeout`, `unknown`),
whose default budget is `0` on purpose — an uncorroborated collective timeout is
a symptom, so it declines to set a policy and leaves `max_attempts` deciding.
`0` means the attempt that just ran was the last one; `retry_on_timeout: false` still applies first and still wins.
Keys are validated at parse — an unknown class is an error, not an inert
policy — and the vocabulary is `dagron-autopsy --explain`
([docs/HPC_AUTOPSY.md](HPC_AUTOPSY.md))),
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
`retry_on_timeout`, `retry_budgets`, `timeout_secs`, `docker_image`,
`runner_class`, `priority`, `pool`, and `env` (a task's own value always wins —
and `retry_budgets` merges **per class**, so a task that overrides one class
still inherits the rest rather than silently dropping them).

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
trigger injected as `{{ trigger_dataset }}`; this build subscribes **one**
dataset — multi-dataset composition with `datasets_mode: any|all` is not in
this build; [docs/DATASETS.md](DATASETS.md)).

## Data formats & compatibility

- **State schema** = embedded sqlx migrations, applied automatically at
  startup: `crates/dagron-core/migrations/` (SQLite, 001–040) and
  `migrations_pg/` (Postgres, 001–051). Forward-only — there are no down
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
stack is deliberately not compiled in). Dashboards, sampling policy, and
retention are left to the observability stack you export to.

<a id="loops-over-sub-workflows"></a>

## Loops over sub-workflows

A `repeat:` on a `type: workflow` task runs the child workflow once per
iteration — one **child run per turn**. That is the point (each turn is
separately inspectable, retryable and attributable), and it is also the thing to
size before turning it on.

**What bounds it, in order:**

| Control | Bounds | Notes |
|---|---|---|
| `repeat.max_iterations` | turns per loop | **Required**, and exhausting it *fails* the task. This is the only hard stop on the number of child runs one submission creates. |
| `max_active_runs` on the child workflow | concurrent runs of that workflow | Across all loops. A second conversation waits rather than doubling the load. |
| `budget: { tasks: N }` on the child | how big one turn may get | Per run, so it caps a turn, **not** the conversation. |
| `MAX_INFLIGHT_RUNS` / `MAX_INFLIGHT_TASKS` | the whole engine | The backstop when everything else is set too high. |

**What does *not* bound it:**

- `SUBWORKFLOW_MAX_DEPTH` — every iteration is a sibling at the same depth, not a
  deeper nesting. The cap catches a workflow that names itself; it does not
  notice one that runs forty times.
- `budget:` on the **parent** — a budget counts the tasks that run creates, and
  the child runs are their own runs with their own tasks. A parent whose only
  task is the trigger is one task, however many turns it drives.

The practical sizing question is `max_iterations × tasks-per-turn`. A 40-turn
loop over a 6-task turn is 240 tasks and 40 runs from a single submission —
which is fine if that is what you meant, and is the first thing to look at when
a queue fills up unexpectedly.

Delay between turns is `repeat.delay_secs`. Set it above zero when a turn polls
something that needs time to change; leaving it at zero when each turn is real
work is correct and costs nothing.
