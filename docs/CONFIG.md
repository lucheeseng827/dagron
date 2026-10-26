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
<any dagron binary> --version
```

**`--version`** (or `-V`), alone, is answered by every dagron binary with
one line: the build's own name and the release it was built for
(`dagron 0.11.0`). Nothing is read and nothing is started for it. The release
is what the build was told (`APP_VERSION`, which a release sets for each
image and each binary it publishes), and not a crate's version: the crates of
a release are not numbered with it. A build no release made says `dev`.

| Arg | Default | Meaning |
| --- | --- | --- |
| `dev` (literal) | — | Zero-infra local quickstart: SQLite + the management API/Swagger on `127.0.0.1:8787` (sets `API_ADDR` if unset), stays resident. With no DAG file present it starts idle and waits for `POST /runs`. Requires the (default) `ops` feature. **`dev` consumes the first positional, so the others shift right — `dagron dev [DAG_PATH] [DB_TARGET]`.** `dagron dev foo.db` therefore reads `foo.db` as the *workflow* and still writes to `workflow.db`; the datastore is the **third** token (`dagron dev my.yaml my.db`), or — on a Postgres build only — `$DATABASE_URL` when that token is absent. The startup line prints the datastore actually in use; check it when a run seems to vanish. |
| `config` (literal) | — | Print every knob's effective value, its source (env / file / profile / default), and the fleet fingerprint — exactly what a daemon started in this environment would run with. `--json` for machines. |
| `validate` (literal) | — | Offline spec lint: parses, template-expands, and graph-validates each `*.yaml`/`*.yml` (directories walked recursively; hidden dirs skipped) through the same pipeline every submit path uses. `--json` emits one JSON object per file; exits non-zero if any file fails. No datastore, no server, works in every build. |
| `DAG_PATH` | `examples/simple_dag.yaml` | Workflow YAML for the `file` source. **There is no `run` subcommand** — the first positional *is* the DAG path (the *second* under `dagron dev`, which consumes one token itself). A resident server (the management API, cron, DB schedules or GC on) given no DAG path, where the default is not there either, as in every image, starts with no initial run, as `dagron dev` does without its file, instead of warning of a file it never asked for. A path that was given and is missing still warns. |
| `DB_TARGET` | `workflow.db` (sqlite build) / `$DATABASE_URL` → `postgres://localhost/workflow` (postgres build) | SQLite file path, or Postgres connection string. Second positional — third under `dagron dev`. |

Other binaries: `dagron-import argo <workflow.yaml>` (prints a dagron DAG
YAML to stdout) and `dagron-import github <workflow.yml> [--name NAME]
[--image LABEL=IMAGE]...` (prints a dagron-ci pipeline under a report of
what was not carried over: [`CI_MIGRATING.md`](CI_MIGRATING.md); `gitlab`
and `jenkins` too, and `dagron-ci import` runs the same three importers
over a checkout and checks what they write), `dagron-mcp`
(no args; JSON-RPC over stdio — see [`MCP.md`](MCP.md)), and `dagron-plan`
(diff two specs for a PR — `dagron-plan <base.yaml> <head.yaml>` or
`dagron-plan --git <base>..<head> <path>`; prints markdown + a Mermaid graph,
`--exit-code` returns `2` when the plan has changes for a CI gate).

## Cargo features (compile-time selection)

| Feature | Default? | Effect |
| --- | --- | --- |
| `sqlite` | yes | Embedded single-writer SQLite datastore. Exactly one of `sqlite`/`postgres` must be enabled — both or neither is a `compile_error!` (`dagron-core/src/db.rs`). |
| `postgres` | no | Postgres datastore: `LISTEN/NOTIFY` wake, `FOR UPDATE SKIP LOCKED` multi-worker claim. Required by HA and by the UI stack (`FEATURES: postgres,ops` in `compose.yaml`). The Postgres-backed tests run against Postgres 14, 15, 16 and 17; an older one is not tried. |
| `ops` | yes | The engine management API (`API_ADDR`), cron, retention GC, DB schedules, leadership. |
| `kubernetes` | no | `EXECUTOR=kubernetes` (KubeExecutor). Without it that value is a startup error, never a silent downgrade. |
| `mqtt` | no | `SOURCE=mqtt` — the open MQTT ingestion source, for plant floors, gateways and robot fleets. Without it that value is a startup error naming the rebuild, never a silent downgrade. Links `rumqttc` with the same rustls/ring stack the rest of the binary uses, so it adds no second crypto provider. |
| `otel` | no | OpenTelemetry: a W3C trace context in each task's environment, and OTLP export of spans, logs and the engine's metrics once `OTEL_EXPORTER_OTLP_ENDPOINT` names a collector. See [OpenTelemetry](#opentelemetry---features-otel). |
| `archive-s3` | no | Cloud GC archive sink over S3 (`GC_ARCHIVE_URL=s3://…`, incl. S3-compatible MinIO/Ceph via `AWS_ENDPOINT_URL`). A `GC_ARCHIVE_URL` scheme whose backend feature is absent is a startup error, never a silent downgrade — same contract as `kubernetes`. Implies `ops`. |
| `archive-gcs` | no | Google Cloud Storage archive sink (`GC_ARCHIVE_URL=gs://…`; credentials from `GOOGLE_*` env). Implies `ops`. |
| `archive-azure` | no | Azure Blob Storage archive sink (`GC_ARCHIVE_URL=az://…` or `azure://…`; credentials from `AZURE_*` env). Implies `ops`. |
| `archive-parquet` | no | `dagron archive-compact` — fold archived `run-*.json` documents into the date-partitioned Parquet dataset (`compact/tasks/dt=…/`). Heavy (arrow+parquet), hence its own feature; combine with a cloud backend (`archive-s3`/`-gcs`/`-azure`) to compact a cloud archive. Implies `ops`. |

**What the published images are built with.** "Default?" above is a source
build's. The `dagron-engine` and `dagron-engine-localdev` images are built with
`postgres,ops,kubernetes,otel,archive-s3,archive-gcs,archive-azure`, and the
`dagron-api` image with its own `otel,archive-s3,archive-gcs,archive-azure`: the same
three object stores, for archived runs and for the artifact store, and span
export under the same variable. `mqtt` and `archive-parquet` are in no published
image. This holds for the images of releases after 0.11.0. Those of 0.11.0 and
earlier were built with `postgres,ops,kubernetes` (the engine) and with no
features (`dagron-api`), and name no `features=` as they start.

Each of the two binaries names what it was built with as it starts, on its
`configuration resolved` line (`features=…`). A setting that needs a part the
build lacks is refused when it is read, so this is how to know what an image
can do before asking it to.

## Engine (`dagron` binary) environment

All read in `crates/dagron-engine/src/lib.rs` unless noted.

| Variable | Type / values | Default | What it does |
| --- | --- | --- | --- |
| `DAGRON_CONFIG` | path to YAML | unset | Configuration **file** layered UNDER the environment — see [the section below](#configuration-file--profiles-dagron_config). |
| `DAGRON_CONFIG_NO_WARN` | set/unset | unset | Silence the startup typo scan (an env var that looks like a dagron knob but isn't one draws a warning). Kubernetes' service links draw none: the variables the kubelet sets for each Service in the pod's namespace, `<SERVICE>_SERVICE_HOST` and its family (`DAGRON_ENGINE_PORT_8080_TCP` and the rest, from a `dagron-engine` Service), in their exact shapes and only beside their `_SERVICE_HOST`. |
| `EXECUTOR` | `local` \| `docker` \| `kubernetes`/`k8s` | `local` | Task execution backend. Unrecognized values warn and fall back to `local`; `kubernetes` without the feature is a startup error. On `docker`, the executor pulls `DAGRON_CI_EXEC_IMAGE` and `DAGRON_CHECKOUT_IMAGE` when it starts, and a task's `docker_image` before each attempt when the daemon does not hold it; a task's `image_pull_policy` (`if_not_present`, the default there; `always`; `never`) says otherwise, and a pull that fails fails the attempt with what the daemon said. It pulls through `DAGRON_REGISTRY_MIRRORS` where one is set, and with the credentials of `DAGRON_REGISTRY_AUTH_FILE` for a trusted run alone; without that file a private image must be on the daemon already. A run that is not trusted has its own images pulled, as no one, whatever their `image_pull_policy`. On `kubernetes` the kubelet pulls, under the pod's `imagePullPolicy`, which is the task's `image_pull_policy` when it states one. |
| `WORKER_COUNT` | usize | `16` (min 1) | Worker-pool size = max concurrently running tasks. |
| `POLL_INTERVAL_MS` | u64 (ms) | `500` (floor 10) | Reconcile-loop timer bound — the longest the loop may sleep with work outstanding. The loop also wakes the moment a local task completes and (Postgres) on any `LISTEN/NOTIFY` event, so this timer is a safety net there; on SQLite it additionally paces time-based retries and parked sensors. Part of the low-latency profile ([docs/LOW_LATENCY.md](LOW_LATENCY.md)). |
| `SWEEP_INTERVAL_MS` | u64 (ms) | = `POLL_INTERVAL_MS` (floor 10, at most a day) | The interval of each maintenance sweep (expired-lease recovery, run deadlines, SLA alerts, approval expiry, sub-workflow / wait-sensor / dataset reconciliation) that `SWEEP_INTERVALS` does not name and that has no cadence of its own (below). At the default this matches the old every-tick behaviour; profiles that shrink `POLL_INTERVAL_MS` set this back to ~`500` so faster ticks don't multiply sweep load. A sweep is looked at on every tick and runs once its interval has passed, so its effective cadence is at least the time between ticks. |
| `SWEEP_INTERVALS` | `name:ms,…` | unset | Each sweep's own interval, in milliseconds (floor 10, and at most a day, 86400000), as `POOLS` is written: `waits:250,approvals:5000`. A name that is no sweep's, a value that is no whole number or is above a day, or a sweep named twice stops the start, and says the names. The sweeps, their scopes and their defaults are in the table under [Sweeps](#sweeps) below; `orphans` and `reap` have settings of their own, and are not named here. An installation sweep is run by one engine a window where `SWEEP_SHARE` is on, and that engine's interval is the installation's for the window: **set this the same on every engine** (the fleet fingerprint, `GET /config`, shows one that differs). Each sweep's interval, scope and where it was set are logged at start (`sweep cadence`) and served as `scheduler_sweep_interval_seconds` ([docs/METRICS.md](METRICS.md#sweeps)). |
| `SWEEP_SHARE` | `on` \| `off` | `on` | Whether the engines of one installation share its sweeps. `on`: a tick asks, in one statement, for the slots (`sweep_slots`, Postgres) of the installation's sweeps that are due by its own clock, and runs those it took and its own; the rest wait a whole interval, so each installation sweep runs once a window for the whole fleet, not once an engine. A slot is taken for nine tenths of the sweep's interval on the database's clock. A slot due later than a claim would set it is taken as well: after the database's clock steps back, every slot is due that much later, and none would be taken until the clock caught up; it is also how an engine with a shorter interval for a sweep runs it early, where one with a longer interval took it last (another reason to set `SWEEP_INTERVALS` the same everywhere). A pass that read a full batch (256 rows) hands its slot back, and the sweep is due again at once on the engine that ran it, which asks for the slot at its next tick: when its loop next wakes (a notification, one of its own tasks ending, or `POLL_INTERVAL_MS`), which on a quiet engine can be most of an interval away. An engine that asks before then takes the next batch; the engines told the slot was taken in that window still wait a whole interval. The first tick after a start runs every sweep whatever the slots say. With N engines, the average wait before a due row is picked up grows from about interval/(N+1) to about interval/2. The worst case is about one interval and the time between ticks while the engine that took the slot runs its pass; an engine that stalls after its claim delays that sweep by up to nearly two intervals, because its slot is held for nine tenths of one and the engines told it was taken wait a whole interval before they ask again. `off`: every engine runs every sweep, as before: the rollback. Unset or empty is `on`; `true`, `1` and `yes` are `on` too, and `false`, `0` and `no` are `off`, in any case. Any other value stops the start. SQLite is one engine, and has no slots. An engine from before `SWEEP_SHARE` takes no slot and sweeps every window itself, which is safe: every shared sweep may run on two engines at once. |
| `REAP_INTERVAL_MS` | u64 (ms) | `5000` (`0`: every tick looks at every running run; at most a day) | How often the reconcile loop takes a slice of the running runs (500 of them, in turn) and ends those whose tasks are all over. On Postgres a tick looks for ended runs among the runs a notification named, by their keys, and this slow sweep is the net under a notification nobody took: a change made with none, or one sent while a listener had no connection. A task's end always notifies its run, by a trigger on `task_runs` (`migrations_pg/107`). On Postgres `0` is the loop as it was: every tick joins every running run to all its tasks. A replica that has just started, or whose listener lost its connection, looks at every run once whatever this is. Where `SWEEP_SHARE` is on, one engine an interval takes the slice, from the run the engine before it got to (its slot's cursor), so the running runs are walked once an installation, not once an engine. On SQLite the file's triggers record each run a change may have ended (`run_wakes`), a tick looks at those by their keys, and this is the interval of the slice under them, as on Postgres. On SQLite `0` is not the loop as it was: every tick walks every slice of the running runs, 500 at a time, which is dearer than the join it replaced on a small file and on the enterprise build (about 21 ms a tick with 1,000 running, where the join took 8.5 ms with 10,000 runs kept, and 7 ms on the enterprise build, on one machine), and the triggers still write each wake and each tick still takes them. |
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
| `DAGRON_ACTIONS_ARTIFACT_MAX_BYTES` | bytes | `5368709120` (5 GiB) | The largest artifact a workflow's `actions/upload-artifact` may upload to the engine's results service (`DAGRON_ACTIONS_CACHE_LISTEN`). A larger one is refused as it arrives. |
| `DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS` | days, 1 to 3650 | `90` | The longest the results service keeps a workflow's artifact; a job's `retention-days` may ask for less. The engine deletes an artifact that has expired, with its bytes, every hour. [CI_RUNNERS.md](CI_RUNNERS.md#a-workflows-artifacts). |
| `DAGRON_ACTIONS_CACHE_DIR` | path | the temporary directory's `dagron-actions-cache` | Where the cache service keeps an upload until the job finalizes it: room for the largest entry (`DAGRON_CACHE_MAX_ENTRY_BYTES`), times the saves at once. |
| `DAGRON_ACTIONS_CACHE_INSECURE` | `allow` | unset = the service's URL is `https://` | `allow` lets `DAGRON_ACTIONS_CACHE_URL` be plain `http://`, for a closed network where a job's cache credential may cross in the clear (a test rig's). The engine warns at start that it does. |
| `DAGRON_ACTIONS_CACHE_LISTEN` | `host:port` | unset = no cache service | Where the engine answers a workflow job's `actions/cache`: GitHub's cache service, version 2, over the path caches' store and index; and its `actions/upload-artifact` and `actions/download-artifact` v4, GitHub's artifact service, in the same store. Set with `DAGRON_ACTIONS_CACHE_URL`, and needs `DAGRON_CACHE_URL`: the engine does not start with one and not the others. [CI_RUNNERS.md](CI_RUNNERS.md#a-workflows-actionscache). |
| `DAGRON_ACTIONS_CACHE_URL` | `https://` URL, no path | unset = no cache service | The cache service's URL as a job's network reaches this engine, given to a job as `ACTIONS_RESULTS_URL`: a host's root, since the artifact actions keep only its origin. Each engine has its own: an upload is kept by the engine it was made to. |
| `DAGRON_ADMISSION_FILE` | path | — (admission open) | **Run-admission** gate, the mirror image of `DAGRON_PRESSURE_FILE` below: while this file exists the engine refuses to admit **new runs** — `POST /runs` answers `503` with `Retry-After: 60`, and cron/schedule fires are skipped (not queued). Work already admitted is untouched: in-flight runs finish, their tasks are still claimed and dispatched, and sub-workflows they spawn are still created. Same file grammar as the pressure gate (a body of `0`, `false`, `off` or `resume` counts as absent, so a writer can flip it without deleting it); an unreadable file counts as **closed**, an absent one as open. Refusals are counted by `scheduler_admission_refused_gate_total`. Use it for a maintenance window, or let an external agent own it. |
| `DAGRON_PRESSURE_FILE` | path | — (no gate) | Back-pressure gate for constrained hosts: while this file exists the engine claims **no new tasks** (a file whose first token is `0`, `false`, `off` or `resume` counts as absent, so an agent can flip it without deleting it). In-flight tasks finish and every other loop keeps running; only claiming stops. A thermal, battery or maintenance daemon owns the file. Note a one-shot `dagron <file>.yaml` will not exit while the gate is closed — its runs are still active. |
| `DAGRON_PROVENANCE_KEY` | path | unset = artifacts are kept without provenance | An ECDSA P-256 private key, PEM, PKCS#8 (`openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256`): each workflow artifact the results service finalizes is made its run's with its SLSA v1 provenance, signed with it. Verify with its public half (`openssl pkey -pubout`) and `cosign verify-blob-attestation --key … --type slsaprovenance1 --insecure-ignore-tlog`: nothing is written to a public log, so it signs air-gapped. An artifact whose provenance cannot be signed is refused; a key that cannot be read stops the start. [CI_RUNNERS.md](CI_RUNNERS.md#a-workflows-artifacts). |
| `DAGRON_PROVENANCE_SIGN_CMD` / `DAGRON_PROVENANCE_PUBLIC_KEY` | command / path | unset | Enterprise, in place of `DAGRON_PROVENANCE_KEY`: a KMS key signs. The command is given the SHA-256 of what is signed, base64, on stdin, and writes the ASN.1 DER ECDSA P-256 signature, base64, on stdout (`aws kms sign --message-type DIGEST --signing-algorithm ECDSA_SHA_256`, say), within `DAGRON_ENV_KMS_TIMEOUT_SECS`. The public key is the KMS key's public half, PEM: each signature is checked against it before it is kept. An open build is refused at start. |
| `DAGRON_PROVENANCE_BUILDER_ID` | URI | `https://dagron.dev/builders/dagron-engine` | The provenance's `builder.id`: what a verifier's policy names as the builder it trusts. Set it to a URI of this installation's. |
| `DAGRON_MIN_FREE_BYTES` | u64 (bytes) | `0` (off) | Free-space floor on the SQLite datastore's filesystem. Below it, **new run creation is refused** rather than risking a half-written datastore: the ops API answers `507` + `Retry-After`, and the ingest source is throttled instead of dead-lettering the payload. Only the embedded backend enforces it; with Postgres the disk belongs to the database server. If the probe itself fails, the floor fails open with one warning. The `edge` profile sets 64 MiB. |
| `DAGRON_ATTEMPT_LOG_BYTES` | usize (bytes) | `4096` (0 = off) | Tail of each **superseded** attempt's output kept in `task_attempts`, so a `repeat:` loop's log shows every iteration and a retried task shows the attempts that explain the failure — not just the last one. `task_runs.output` is a single column every attempt overwrites, which is why only one was ever readable. A **tail**, because the thing being multiplied is large: an attempt's output is kept up to `DAGRON_LOG_TASK_CAP_BYTES` (64 MiB by default), `max_iterations` is a `u32`, and `GC_RETENTION_SECS` is unset (GC off) by default. Capped at 64 KiB — the knob tightens the bound or loosens it within a compiled limit, never past it. **`0` disables retention entirely**: no rows, no extra transaction work, byte-for-byte the previous consumption. See [docs/ITERATION-LOGS.md](ITERATION-LOGS.md) for the measurements. |
| `DAGRON_ATTEMPT_LOG_KEEP` | usize | `50` (0 = unlimited) | Superseded attempts kept per task; the oldest fall out first, and the read API reports `evicted` so a history that lost its beginning says so. With `DAGRON_ATTEMPT_LOG_BYTES` this bounds the whole feature at `iterating_tasks × keep × bytes` — a measured **228 KiB** for a 1000-iteration loop, and **zero** for a task that neither loops nor retries. `0` removes the ceiling and is for a debugging session, not a deployment — the rows then grow without bound, though a single read still returns at most the newest 200. |
| `DAGRON_LOG_TASK_CAP_BYTES` | usize (bytes) > 0 | `67108864` (64 MiB) | How much is kept of one attempt of a task: of its **log**, and of its **output**, each. Both are values in the scheduling database, and nothing bounded either: a task that wrote without end filled the scheduler's disk. A log that reaches the cap keeps its **beginning**, to the cap, and its **end**: while the task runs it is held at the cap, on a line that says so (`[dagron] this attempt's log reached 64 MiB (DAGRON_LOG_TASK_CAP_BYTES): what the task writes from here is held back, and the last 16 MiB of it (DAGRON_LOG_TAIL_BYTES) is added below when it ends`), and what the task writes after that is counted by the executor and not queued for the engine's loop; once the attempt is over, the last `DAGRON_LOG_TAIL_BYTES` of what the task wrote are added, from a line's start where one is within 64 KiB and never from inside a secret, masked as one text, under a line that says how much was left out between, in bytes and lines of what the task wrote, counted before masking (`[dagron] 1.2 GiB of this attempt's output (1288490189 bytes and 3456789 lines, counted before masking) is left out here; what follows is its end`; its form is in `docs/LOG_PIPELINE.md` §7.4). A follower sees nothing past the cap until the attempt ends, and then its end at once. An attempt that no executor sealed (a cancel, an error) says that its end was not kept (`… is left out here, and its end was not kept`); one whose claim went to another (a lease that lapsed, a teardown) is given no end, its row being another's, and its log ends on the line it was held at. An output over the cap keeps its **end**, under a first line that says how much of its beginning is gone, so that what reads an output (`result_from`, `when:`, `with_output_of:`) is not handed a cut text as if it were whole. The cap counts what the task wrote: the line a log is held at, the line that says what was left out and the end after it, the first line of a cut output, and any line dagron adds to a log itself, such as why the attempt failed, come on top of it, and a capped log is given them all the same. A value that is not a positive integer is the default, with a warning in the engine's log as it starts. The cap bounds what is stored and what waits in the engine, and what the Docker and Kubernetes executors hold of a running task's output: of its beginning, the cap and a few hundred KiB more, for its log; of its end, up to twice the cap and a few hundred KiB, for its output; more of each with a secret over 64 KiB (the local executor holds all of it). Where a task has secrets, its log is also held once what the task wrote, counted before it is masked, is past the cap by 64 KiB, or by its longest secret, on a line that says so (`[dagron] this attempt's output, counted before masking, passed 64 MiB (DAGRON_LOG_TASK_CAP_BYTES): what the task writes from here is held back, …`): masks shorten a text, and a log of a long secret printed again and again would reach the cap only far past it, while an executor held all of that for the log's last masking. That masking, of the whole, ends where the log stopped, on the line that says so; where it writes the log again (a secret cut in two between the pieces the log was sent in), the log's end is in the text it writes. Either way the log stops at a byte of what the task wrote, clear of a secret, so the line that says what was left out is exact. Beside it, and with no setting: a running task's log is written to its row once a second at most, its lines together, and a long one less often (a second more for each whole MiB an append rewrites, up to 30 s; a `file://` log store's appends rewrite nothing), so a tail is about that far behind the task, and a task that ends by itself has its log written before its status changes. Up to that wait of a log is lost with the engine, and to a task cancelled before it is written; and an append the database refuses is logged by the engine and loses the lines it carried, as one line's did. |
| `DAGRON_LOG_TAIL_BYTES` | usize (bytes) | a quarter of `DAGRON_LOG_TASK_CAP_BYTES` (16 MiB at its default) | How much of the **end** of a log past its cap is kept, on top of the cap (above), in bytes of what the task wrote. No more than the cap: a larger value is the cap, with a warning in the engine's log as it starts. `0` keeps a log's beginning alone, as before the tail was kept, on the line it was then held at (`… and stops here`). A capped log is then at most its cap, this, and dagron's lines: 1.25 times the cap by default, twice it at most. Read by the engine's executors, and by each `dagron-runner` host for the tasks it runs; dagron-api does not read it, and keeps a runner's log up to twice the cap and 1 MiB whatever it is. |
| `DAGRON_LOG_STORE` | `file:///DIR`, or `s3://`, `gs://`, `az://` | unset (each log in its task row, as before) | Where task logs are kept out of the scheduling database. A task attempt's log is closed segments, each a zstd object of about `DAGRON_LOG_SEGMENT_BYTES` of text with a row in `log_segments`, and an open part. With `file:///DIR` both are files under `DIR`, which the engine and dagron-api share (a volume), and nothing of the log goes through the database but a row for each closed segment. With an object store the closed segments are objects (the engine's cloud credentials, as for `DAGRON_CACHE_URL`), and the open part is the task row's `log`, as a whole log was, never larger than a segment: a closed segment's text leaves the row in the transaction that records it, so a reader sees each line once. The engine and dagron-api must name the same store, and so must every engine: what nothing reads any more leaves the store (`DAGRON_LOG_SWEEP_SECS`), a deleted task's logs about a minute after its row and an attempt's or a writing's as soon as a later one begins, removed by whichever engine takes the record of it, from its own store. dagron-api removes nothing. A log written before it was set is read from its row as before; a remote runner's log stays in its row. A setting that cannot be used stops the process. |
| `DAGRON_LOG_SEGMENT_BYTES` | 4096 to 1073741824 | `8388608` (8 MiB) | How much text a log segment holds before it closes (`DAGRON_LOG_STORE`). |
| `DAGRON_LOG_SWEEP_SECS` | u64 (s) | `30` (0 = this engine removes nothing) | How often this engine's GC of the log store sweeps it (`docs/LOG_PIPELINE.md` §7.4). It takes what is recorded in `log_sweeps` (a task row deleted, by the retention GC, an archive, a rerun or by hand; a log begun again over a writing kept before) and removes from the store what no task row names: a superseded writing at once, the reset waking it, and a deleted task's a minute after its row went. Every engine with a store sweeps, the leader or not; a record is taken by one engine at a time, for ten minutes. A record is let go once a look finds nothing left, and looked at again 15 minutes after one that removed something; a removal that fails is tried again, a minute on and up to six hours. `0`: this engine removes nothing, and what is recorded waits for an engine that does. Without `DAGRON_LOG_STORE` it does nothing, and says at its start if records wait. A value that is no number stops the process. |
| `DAGRON_CLOCK_CHECK_SECS` | u64 (s) | `30` (0 disables) | How often the wall clock is compared against the monotonic clock to detect a step. |
| `DAGRON_CLOCK_STEP_TOLERANCE_MS` | u64 (ms) | `1000` | A wall-vs-monotonic disagreement above this is a step: the engine records `drifted` on runs it is executing and stamps new ones the same way. |
| `DAGRON_CLOCK_SYNC_FILE` | path | — (no positive evidence) | A file whose existence means the clock is disciplined (e.g. `/run/systemd/timesync/synchronized`). Present ⇒ runs are stamped `synced`; absent ⇒ `unknown`, which is honest rather than optimistic. Re-checked every tick. |
| `MAX_INFLIGHT_RUNS` | i64 | `64` | Admission valve: cap on simultaneously active runs; overflow stays buffered at the source. The ops API answers `429` + `Retry-After` above it. **`0` disables the cap** on both admission paths (the API check and the ingest actor's throttle); a negative value is normalized to `0`. |
| `MAX_INFLIGHT_TASKS` | i64 | `0` (off) | Second admission dimension, on TASKS. Runs are the wrong unit on their own: a run of 100,000 tasks and a run of four both count as one against `MAX_INFLIGHT_RUNS`, so a fleet comfortably under the run cap can be far past what the scheduler and datastore carry. Counts `task_runs` in `pending`/`ready`/`running` (a task parked on an approval is not pressure and is excluded). Only queried when set, so the default path pays no extra round trip. |
| `METRICS_CACHE_SECS` | u64 (s) | `10` (`0` = off) | How long `GET /metrics` may serve the datastore gauges of what has not finished from a cache: `scheduler_runs` and `scheduler_tasks` of the statuses still to end (`pending`, `running`; and a task's `ready`, `awaiting_approval`), the runs of the last 24 hours that are still `pending` or `running` (`scheduler_workflow_recent_runs`, `scheduler_environment_recent_runs`), `scheduler_queue_depth`, the dead letters, the quota and log-sweep waits, the runs that wait and the ready backlog by class. The counts of the finished statuses, and the recent runs that have finished, are the history's (`METRICS_HISTORY_CACHE_SECS`). Process counters, `scheduler_db_pool_*` and the other engine state gauges stay live, so only the datastore gauges lag: these by at most this long, the history by its own. Each engine caches on its own, so two engines on one installation can disagree on these for up to this long, and on the history for up to its lag (see [docs/METRICS.md](METRICS.md)). One read at a time: scrapes that come as the entry expires wait for it. A read that has not returned after 30 s is given up, so that a connection left half open cannot hold every scrape behind it. A failed read, or one given up, is never served from the cache: `/metrics` answers `500` as before. **`0` reads the datastore on every scrape**, which is what a test or load harness that changes state and then scrapes needs: where `METRICS_HISTORY_CACHE_SECS` is not set, it is then `0` as well, so `METRICS_CACHE_SECS=0` alone still reads every datastore gauge on every scrape, as it did before the history had a cache of its own. Set it below your scrape interval to make this cache a no-op. |
| `METRICS_HISTORY_CACHE_SECS` | u64 (s) | `300`; `0` where `METRICS_CACHE_SECS` is `0` (`0` = with every read of the rest) | How long `GET /metrics` may serve the history from a cache: `scheduler_runs` and `scheduler_tasks` of the finished statuses, and the runs of the last 24 hours that have finished (`scheduler_workflow_recent_runs`, `scheduler_environment_recent_runs`). They grow with every run, and reading them was most of what a scrape cost. A scrape that finds the history stale is served the last one at once, and the history is read again behind it, one read at a time, so a slow read of it never holds up a scrape, a single Prometheus's or the OTLP export's included, and a scrape that times out does not take the read with it. Until a read of it has succeeded, a scrape waits for one: the first after a start, and each after a first read that failed or was given up. That read runs on its own too: a scrape that times out while it waits leaves it to be cached for the next. A read behind a scrape that fails is logged, and the last history is served until one succeeds. A read of the history that has not returned after this TTL or 60 s, whichever is longer, is given up: behind a scrape, as a failed read; for a scrape that waits for it, or where this is `0`, as the scrape's `500`. So the history served can be as old as this TTL, plus the time until the scrape that finds it stale, plus the read that scrape starts, and older while reads of it fail; `scheduler_metrics_history_age_seconds` says how old it is, and a sum over every status moves at each refresh of it. Not set (or not a whole number), it is `300`, or `0` where `METRICS_CACHE_SECS` is `0`; set, it is its own whatever `METRICS_CACHE_SECS` is. `0` reads it with every read of the rest, as before. The OTLP export shares the cache. |
| `DAGRON_MAX_TASKS_PER_RUN` | usize | compiled ceiling (100000) | Per-run cap on the EXPANDED graph, enforced as a budget during expansion so a fan-out blow-up fails loudly instead of exhausting memory. May only LOWER the compiled ceiling — a larger value is ignored, because this knob exists to tighten a bound that prevents an OOM, not to widen it. |
| `DAGRON_MAX_WAITING_RUNS` | i64 | `100` (floor 1) | How many runs may wait for one workflow's `max_active_runs`, or for one `concurrency_key` of it, where the workflow says a run waits (`when_full: queue`, `supersede`). A run past it is refused as one is by a workflow that does not queue: `429` from the API, and a schedule's fire held back. It is what keeps a schedule that fires faster than its runs end from filling the datastore with runs that wait. Read by whatever makes the run, so set it the same on the engine and on dagron-api. The runs that wait are `scheduler_runs_waiting` ([docs/METRICS.md](METRICS.md)). |
| `DEAD_LETTER_MAX_ATTEMPTS` | i64 | `3` (min 1) | Transient `create_run` failures retried before a submission is dead-lettered (parse failures dead-letter immediately). |
| `DATABASE_URL` | conn string | `postgres://localhost/workflow` | Postgres builds only; positional `DB_TARGET` wins. Redacted before logging. |
| `API_ADDR` | `host:port` | unset = ops API **disabled** (`dagron dev` sets `127.0.0.1:8787`) | Bind address of the engine's unauthenticated ops API; also keeps the process resident. Invalid values warn and disable the API. |
| `DAGRON_READY_TIMEOUT_MS` | u64 ≥ 50 | `500` | Budget for the ops API's `/readyz` datastore ping — 503 `datastore probe timed out` past it, so a wedged pool never hangs the probe past the kubelet's `timeoutSeconds`. Same knob (name and semantics) as dagron-api's. |
| `DAGRON_CONSOLE` | `off`/`false`/`0`/`no` = unmount | unset = console served | Serves the operator console at `/` and `/console` on the ops API. Opt **out**, not in: the API those pages drive is on that socket either way, so unmounting the UI hides it and closes nothing. An ops API you need closed needs a network boundary — see `API_ADDR` above. |
| `DOCKER_IMAGE` | image ref | `alpine:latest` | Default image for `EXECUTOR=docker` (also k8s fallback). |
| `DAGRON_DOCKER_MOUNTS` | `SOURCE:TARGET[:ro\|rw]`, comma separated | unset (none) | `EXECUTOR=docker` only: mounts every task container gets, written as `docker run -v` writes one. SOURCE is an absolute host path, bound as it is (a missing one is Docker's error, not a directory it creates), or a volume's name, which Docker creates on first use; TARGET is an absolute path, each at most once. The operator's, never a workflow's: a workflow that could name a host path could read the host. Checked at startup, and a bad entry is a startup error, since every task gets them. `dagron-ci run` sets it to mount the working tree at its own path. |
| `DAGRON_DOCKER_WORKDIR` | absolute path | unset (the image's) | `EXECUTOR=docker` only: the directory every task container starts in. A task with a `workspace:` works here too, and no workspace volume is made for it: the operator chose where tasks work (so does a mount at `/workspace`). `dagron-ci run` keeps the working tree in every task this way. |
| `DAGRON_DOCKER_DAEMON` | `dind` | unset (none) | `EXECUTOR=docker` only: a task that asks for a Docker daemon (`docker_daemon: true`, `docs/HOWTO.md`) is given one of its own, a container of `DAGRON_DOCKER_DAEMON_IMAGE` beside it as a service named `docker`. **That container is privileged** (unless `DAGRON_DOCKER_DAEMON_RUNTIME` names Sysbox's runtime): it can do on the engine's host what root can, and the task that asked reaches it, so set this only where every trusted run's author may; for a CI service, list the repositories whose jobs may have one (dagron-ci's `CI_GITHUB_DOCKER`). It is not held to the task's isolation, nor to `DAGRON_TASK_ISOLATION_FLOOR`: through it the task can start a privileged container too. A run that is not trusted is never given one, and an engine that also runs such work (`DAGRON_UNTRUSTED_ISOLATION_FLOOR`) does not start: run the two on engines of their own. Unset, a task that asks for one is failed with the reason. Set with another executor, or to anything but `dind`, the engine does not start. |
| `DAGRON_DOCKER_DAEMON_RUNTIME` | a runtime's name, `sysbox-runc` | unset (privileged) | The container runtime a task's Docker daemon is run on, **unprivileged**: [Sysbox](https://github.com/nestybox/sysbox)'s `sysbox-runc` gives the daemon's container a user namespace of its own, in which its root is no user of the host's, and what a daemon needs inside it, without `--privileged`. The engine's host has to have it installed and registered with its Docker daemon, which is asked as the engine starts: a daemon that has no such runtime stops the engine, saying which it has. Only Sysbox's runtime is taken (a name beginning `sysbox`): runc, crun or another that gives a container no user namespace of its own could not run a daemon unprivileged, and the engine does not start with one. Needs `DAGRON_DOCKER_DAEMON=dind`. Unset, the daemon's container is privileged on the host's default runtime. Podman has no Sysbox. `dagron-ci run --docker-daemon` passes this variable on to the engine it starts. |
| `DAGRON_DOCKER_DAEMON_IMAGE` | image ref | `docker.io/library/docker:27-dind` | The image a task's Docker daemon is run from, pulled once if the host lacks it, whatever the task's `image_pull_policy`. Any image of Docker's `dind` variant: it is run as `dockerd`, listening without TLS on `127.0.0.1:2375` alone, the loopback address only the task and its services share. The image runs privileged, or as root in a user namespace of its own on Sysbox: pin it by digest (`docker.io/library/docker@sha256:…`) to run the one you checked. |
| `DAGRON_DOCKER_LOG_DRIVER` | a log driver's name | unset (the daemon's own) | `EXECUTOR=docker` only: the log driver every container the executor makes is given: a task's, the ones that install its tools and fetch its checkout and its actions, its services' and its steps' (a Docker action's). A task's own log and output are read from its container's streams as it writes them, and do not depend on the driver. What the daemon gives back of a container once it has exited does, and is what that driver kept: a Docker action's step output; what a container that failed before its task started said (the one that installs its tools, its checkout's, its actions' fetch); a service's last lines; and a task's own log where its container could not be attached to, or the follow of it was cut. Unset, a container names none and gets the daemon's own default, which is the host's choice: Podman's is the journal, which drops lines past its rate limit (10,000 in 30 seconds by default, one limit for all of a user's containers), and Docker's `local` keeps 100 MB of a container's log by default; with either, a container that writes more loses part of what is read back of it, and nothing says so. **`json-file` keeps it all**: a file on the daemon's host, which the daemon reads back whole (Podman takes the name for its own file driver, `k8s-file`). It is not the default because nothing bounds that file but the daemon's own options for the driver, until the container is removed when its task ends: on a host whose own driver is bounded, a task that writes without end could then fill the disk. Whichever is in use is judged at startup: a driver that keeps no log to read back (`none`, `passthrough`) stops the engine, and one that may lose part of a log draws a warning that says what and names this setting. A daemon need not say which driver its containers get, and Podman's does not through this API: there the driver is read off the first container the executor makes and judged then, too late to stop the engine, so a driver that keeps nothing is an error in its log. The first container is also read for a rotation the daemon gives the driver (`max-size`, `max-file`), which a daemon does not otherwise say: a rotated file keeps the end of a long log and loses its beginning, and the engine warns of it once, with the bound. And for `mode=non-blocking`, in which the daemon does not wait for the driver and drops what a container writes while its buffer (`max-buffer-size`) is full: a container that writes fast loses lines from anywhere in what is read back of it, whichever driver keeps the rest, and the engine warns of that once too. |
| `DAGRON_DOCKER_BUILD_ACTIONS` | `allow`, or unset | unset (none is built) | `EXECUTOR=docker` only: whether a trusted run's step may have its image built from a Dockerfile on the executor's daemon, for a GitHub workflow's Docker action whose `runs.image` is one ([CI_GITHUB.md](CI_GITHUB.md#a-docker-action)). A build runs what its Dockerfile says on the daemon, with the daemon's network and none of a job's confinement but its memory, so the engine asks for one only for a trusted run: a run that is not trusted has none built, whatever is set here. The images are kept on the host, named `dagron-ci-action:<digest>` and labelled `dagron.ci.step-image`, for the next step that uses one; remove them when the host needs the room. Unset, such a step fails, and says so. |
| `DAGRON_DOCKER_NETWORK` | a network's name, or `none` | unset (the daemon's default) | `EXECUTOR=docker` only: the network every container the executor makes is on: a task's, and the ones that install its tools and fetch its checkout. Unset, a container names none and is on the daemon's default network, beside every other container on the host that named none, and with whatever route off the host that network has. **Name one made for jobs**, and what a job can reach is what that network reaches: `docker network create --internal jobs` makes one with no route off the host, for jobs that fetch nothing but their checkout from a forge on the same network. The network is the operator's to make, and is asked of the daemon at startup: one it does not have stops the engine, since every task would fail where its container is made. `none` gives no container a network, the checkout's among them, so a task with a checkout fails. `host` is refused, and so is another container's network (`container:NAME`): a job on the host's network reaches what the engine's host does. A task that declares `isolation: { network: none }` has no network in its own container whatever is set here; what is run for it first keeps this one. |
| `DAGRON_REGISTRY_MIRRORS` | `REGISTRY=MIRROR[/PATH]`, comma or newline separated; `*` for every other registry | unset (each image from its own registry) | `EXECUTOR=docker` and `kubernetes`: the registry each image is pulled from in place of its own. `docker.io=mirror.local:5000/hub` pulls `alpine:3.20` as `mirror.local:5000/hub/library/alpine:3.20`; `*=mirror.local:5000` pulls every other registry's image with that registry's name in the path (`ghcr.io/o/i` as `mirror.local:5000/ghcr.io/o/i`), which is an install with a mirror and nothing else. Nothing falls back to the image's own registry. A task's, a service's and a step's image go through it, and so do `DAGRON_CI_EXEC_IMAGE` and `DAGRON_CHECKOUT_IMAGE`; an image already on a mirror is left as it is. A mirror that cannot be read stops the engine at startup. |
| `DAGRON_REGISTRY_AUTH_FILE` | path | unset (images pulled with no credential) | `EXECUTOR=docker` only: a Docker `config.json` whose `"auths"` (`auth`, or `username` and `password`, or `identitytoken`) the engine pulls with, each registry's own (after the mirror). The credential goes to the daemon with the pull, never to a task: for a trusted run's pulls, and for the engine's own startup pulls of `DAGRON_CI_EXEC_IMAGE` and `DAGRON_CHECKOUT_IMAGE` (before any run; one that cannot be had stops the engine). A run that is not trusted pulls as no one, and pulls its own images whatever their `image_pull_policy`, so it can name no private image the engine may read, whether from the registry or from what the host holds. A file that names a credential helper (`credsStore`, `credHelpers`) stops the engine: it runs none. |
| `DAGRON_IMAGE_PULL_SECRETS` | Secret names, comma separated | unset (a pod names none) | `EXECUTOR=kubernetes` only: the pull secrets (of the task namespace) a trusted run's task pod names in `imagePullSecrets`. A run that is not trusted names none, and its task's and services' containers are `imagePullPolicy: Always`, so the kubelet asks the registry, with no credential, rather than start a private image a trusted pod left on the node. Admission adds a pod's ServiceAccount's own `imagePullSecrets`, which the engine cannot take away: the ServiceAccount such a pod runs as (the namespace's `default`, unless one is allowed) must carry none. |
| `DAGRON_CI_EXEC_IMAGE` | image ref | unset (no init container) | `EXECUTOR=docker` and `kubernetes`: an image whose entrypoint is `dagron-ci-exec` (the published `dagron-ci-exec` image is one). Each task with a `workspace:` then starts with an init container from it, which runs `install /dagron/bin` into a volume of its own; the task mounts that volume read-only at `/dagron/bin`, where a compiled dagron-ci task looks for its step runner first. On Docker the image is pulled at startup if the daemon lacks it, and one that cannot be had stops the engine; a `DAGRON_DOCKER_MOUNTS` target under `/dagron/bin` is refused beside it. On Docker the init container also makes the task's workspace volume, which starts as a copy of the image's `/workspace`, so the image needs one open to every user (`mkdir -m 0777 /workspace`; the published image has it) for a task that runs as non-root to write in its workspace. Unset, the workspace made for a task on Docker is a volume of the task's own container, which its checkout's and its steps' containers mount by name: the task's container is removed only after them, and is otherwise killed and left, with its volume and the task's variables in its settings, for the task's next attempt, a stop's teardown, or the fleet sweep. A task that has ended has no next attempt: where the sweep does not run (`DAGRON_INSTALLATION` unset), it stays until removed by hand, after the containers the engine's warning names. An init container that fails fails its task with the init container's own words. A task whose workspace names `tools: [node]` (dagron-ci's, for a job that uses an action) is given the Node.js beside the step runner too, `install /dagron/bin --with node`: the published image holds it, some 130 MB, and one that does not fails such a task. |
| `DAGRON_CHECKOUT_IMAGE` | image ref | unset (a task with a checkout fails on Docker and Kubernetes) | `EXECUTOR=docker` and `kubernetes`: an image with `git` and `sh` (`alpine/git`), which fetches a workspace's `checkout:` (a repository and a commit) into it before the task runs: on Kubernetes an init container after `DAGRON_CI_EXEC_IMAGE`'s, on Docker a container of its own, the only one with the repository's credential, which is seen removed before the task starts: one that is not fails the attempt, and is left, the credential in its settings, until the task's next dispatch, a stop's teardown or the fleet sweep removes it, or by hand for a task that has ended where the sweep does not run (the credential is revoked once the attempt is over, in the background, and lives until it expires where that revoke fails). It fetches that one commit by its id, at depth 1 and without tags, and checks it out detached; a fetch that fails fails the attempt with git's words. On Docker the image is pulled at startup if the daemon lacks it, and one that cannot be had stops the engine. The local executor runs the engine machine's own `git`. The task's environment marks `/workspace` a safe directory for git, since the checkout may be another user's than the task's. A workspace's `actions:` are fetched with the same image, each at its ref with no credential but its own where it asks for one (`DAGRON_CHECKOUT_TOKEN_URL`), into a volume the task reads at `/dagron/actions` (`DAGRON_ACTIONS_DIR`): on Kubernetes an init container after the checkout's, on Docker a container of its own. |
| `DAGRON_CHECKOUT_PATCH_INSECURE` | `allow` | unset (a patch's link is `https://`, or `http://` to this machine) | A patched checkout (a run on a patch bundle) fetches its patch from the link `DAGRON_CHECKOUT_TOKEN_URL` gives the attempt. That link is a bearer's, so the engine takes one over plain `http://` to another host only with `allow`, on a closed network where that is accepted; else the attempt fails. |
| `DAGRON_CHECKOUT_TOKEN_URL` | `https://` URL | unset (every checkout fetched without a credential) | Where each attempt's checkout credential comes from: before an attempt whose workspace has a checkout, the engine `POST`s `{repository, commit, run_id, task_id, attempt}` and gets `200 {username, password}` (HTTP basic, which git sends as a header) or `204` for none; once the attempt is over it `DELETE`s the credential, so the service revokes it. A workspace action that asks for one (`actions[].credential`) is asked for the same way, with `"action": true`, its repository, and its ref for `commit`: its credential reaches that action's fetch alone, and only that repository's URL. Any other answer fails the attempt with the service's words. dagron-ci's `/ci/v1/checkout-tokens` is one (crates/dagron-ci/README.md, "The checkout"). The credential reaches git's environment only, never the task's, its config or a URL; on Kubernetes it goes in the task's Secret (`DAGRON_TASK_SECRET_ENV`), read by the checkout's init container alone; it is redacted from the task's log, and sent to a repository over `https://` only (or to this machine). The service itself is `https://`, or `http://` to this machine: the engine stops at startup on one over plain http to another host, since its secret goes there and every credential comes back from there. A redirect from it is not followed: it fails the attempt like any other answer. Over plain http, to this machine, it is asked directly: a proxy the environment names (`HTTP_PROXY`, `ALL_PROXY`) is not used, since it would be handed the secret. Needs `DAGRON_CHECKOUT_TOKEN_SECRET_FILE`. |
| `DAGRON_CHECKOUT_TOKEN_SECRET_FILE` | path | unset | The file holding the bearer secret the engine presents to `DAGRON_CHECKOUT_TOKEN_URL`, one token. Each needs the other: one set alone stops the engine at startup, as does an empty or unreadable file. |
| `DAGRON_LOCAL_WORKDIR` | absolute path to a directory | unset (the engine's) | `EXECUTOR=local` only: the directory every task process starts in, and the workspace of a task with a `workspace:`. Unset, such a task gets a new directory under the temporary directory for each attempt, removed after it. Checked at startup: a missing directory would fail every task at spawn. `dagron-ci run --executor local` sets it to the working tree. |
| `DAGRON_WORKSPACE_PVC_FROM` | Kubernetes quantity | unset (always an `emptyDir`) | KubeExecutor: from this size, a task's workspace (`workspace: { size: … }`) is a volume of its own, a generic ephemeral volume that asks for that much storage, rather than an `emptyDir` on the node's disk with `size` as its `sizeLimit`. `0` gives every sized workspace a volume. A workspace with no size is always an `emptyDir`. |
| `DAGRON_WORKSPACE_STORAGE_CLASS` | StorageClass name | unset (the cluster's default) | KubeExecutor: the StorageClass of those volumes. Needs `DAGRON_WORKSPACE_PVC_FROM`, or it would do nothing, and is refused at startup without it. |
| `K8S_IMAGE` | image ref | `$DOCKER_IMAGE` → `alpine:latest` | Image for KubeExecutor. |
| `K8S_NAMESPACE` | string | `default` | KubeExecutor namespace. |
| `DAGRON_INSTALLATION` | string (label-safe) | — (**fleet sweep off**) | Names *which* dagron installation this engine is. Stamped on every pod/container as `dagron.dev/installation`, and it is what **arms the fleet sweep**: the periodic pass that deletes workloads whose task is no longer live. Off without it, and that is not a conservative default but the only safe one — `dagron.dev/managed-by=dagron` says *some* dagron made a workload, not *which*, so a sweep scoped on that alone would read a second installation's pods in a shared namespace, fail to find their task ids in **this** engine's datastore, and delete someone else's running work. dagron cannot infer the value (a namespace can hold two installs, one install can span namespaces, a pod carries no pointer to the database that made it). Must be a valid label value — 1–63 of `[A-Za-z0-9]`, `-`, `_`, `.`, starting and ending alphanumeric; anything else contributes **no** label at all rather than a truncated one that could collide with another installation sharing its prefix. Unrelated to the per-dispatch reap, which selects on a task id and keeps working either way. |
| `DAGRON_ORPHAN_SWEEP_SECS` | u64 ≥ 30 | `300` | Gap between fleet sweeps. Deliberately slow: the sweep lists every workload carrying this installation's labels, an apiserver/daemon call whose cost scales with the *cluster* rather than with this engine's work, and what it catches — a workload whose scheduler died — does not accumulate quickly. Every **eligible candidate** is judged each pass, where eligible means labelled for this installation, older than `DAGRON_ORPHAN_MIN_AGE_SECS`, carrying a readable `dagron.dev/task-id`, and — on Kubernetes — carrying the uid the delete will be bound to; anything else is skipped rather than judged, because a workload that cannot prove it is stale is not deleted. Among those candidates nothing is truncated — the liveness query is chunked instead, because an apiserver lists in a stable order and capping the candidates would re-examine the same live ones forever while never reaching the leftovers behind them. What is capped is the **deletes**: at most 200 per pass, the rest next time. A datastore error skips the pass's deletes entirely rather than acting on a partial answer. Ignored without `DAGRON_INSTALLATION`. |
| `DAGRON_ORPHAN_MIN_AGE_SECS` | u64 ≥ 60 | `600` | Age below which a workload is never judged by the fleet sweep. A correctness floor, not a tuning knob: the datastore and the apiserver share no transaction, so the sweep lists first and asks which tasks are live second — a listed workload therefore predates the answer — and this covers the clock skew and replication lag that ordering alone does not. Generous deliberately; waiting costs a leftover living a few more minutes, being wrong costs live work. |
| `DAGRON_MAX_TASK_TIMEOUT_SECS` | u64 > 0 | unset = **no ceiling** | Upper bound on any single task's wall clock, applied by every executor (local, docker, k8s). A task's own `timeout_secs` is a *request*: it is clamped to this, and so is the 25 s default. Unset changes nothing, which is right for a self-host — it is your hardware. A multi-tenant install wants it set, because plan quotas cap tasks per day and runs per month while nothing capped how long one task may run, leaving worst-case compute per plan unbounded. A value that is not a positive integer (including `0`, which would time out every task instantly) is ignored with a warning rather than silently becoming a ceiling that is not one. |
| `DAGRON_LOCAL_COMMAND_ALLOWLIST` | comma-separated programs | unset = **any command** | `EXECUTOR=local` only: the programs a task of this pool may run. A pool is an engine process with `RUNNER_CLASSES` set, so a process-wide setting is per-pool. Matching is on the **resolved file**, not the word the task wrote — a bare name is searched on `PATH`, anything with a `/` is a path, and both are canonicalised before comparison, so a planted `dagron-build` on a task-supplied `PATH`, an absolute path to a different file, and a `./` spelling are all refused. The permitted program is then spawned by its canonical path, so the exec does no lookup of its own. An entry that resolves to nothing permits nothing, and an allowlist where nothing resolved refuses every task — the safe direction for a typo, and loud rather than silent. A refusal is a task failure classified as a configuration fault, so it is not retried. **It bounds which program runs, not what that program does.** |
| `DAGRON_LOCAL_ENV_PASSTHROUGH` | comma-separated variable names | unset = **inherit everything** | `EXECUTOR=local` only: which of the pool's own environment variables reach a task. Set, the subprocess environment is cleared and rebuilt from a fixed baseline (`PATH`, `HOME`, `USER`, `LOGNAME`, `SHELL`, `TMPDIR`, `TZ`, `TERM`, `LANG`, `LC_ALL`, `LC_CTYPE`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `CURL_CA_BUNDLE`, `REQUESTS_CA_BUNDLE`, and the proxy variables in both cases), plus exactly the names listed here, plus the task's own `env:`. A task whose `env:` names one of the listed variables — or `PATH` — is **refused** rather than quietly overruling the deployment. Unset, a task inherits the engine's whole environment, which is what every local pool did before this existed and why a `build`-class task could read the pool's datastore credential. Note this is a different thing from `DAGRON_REDACT_ENV`, which masks values in stored *output* and cannot stop a task reading one. |
| `DAGRON_TASK_RUN_AS_USER` | uid | unset | KubeExecutor: stamp `runAsUser` (and `runAsNonRoot` when > 0) onto every task pod. Unset leaves the pod's user to the image, which is what happened before this existed — a task image's final `USER root` ran as root. |
| `DAGRON_TASK_READ_ONLY_ROOT_FS` | `1`/`true` = on | off | KubeExecutor: `readOnlyRootFilesystem` + `allowPrivilegeEscalation: false` on task containers. |
| `DAGRON_TASK_DROP_ALL_CAPABILITIES` | `1`/`true` = on | off | KubeExecutor: `capabilities.drop: [ALL]` on task containers. |
| `DAGRON_TASK_RUN_AS_NON_ROOT` | `1`/`true` = on | off | KubeExecutor: `runAsNonRoot: true` on every task pod without naming a uid, so a task whose image runs as root is refused rather than run. For a platform that gives the uid itself: OpenShift's restricted SCCs refuse a pod that names one, and the chart sets this there ([docs/OPENSHIFT.md](OPENSHIFT.md)). |
| `DAGRON_TASK_FS_GROUP` | group number, or `none` | `65532` | KubeExecutor: the `fsGroup` a task pod is given where the executor gives one (a workspace that is its own volume, `DAGRON_WORKSPACE_PVC_FROM`, whose root is its provisioner's). `none` gives none: the platform does, as OpenShift's restricted SCCs do from the namespace's range, and refuse a pod that names another. The engine refuses to start on anything else. |
| `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT` | `1`/`true` = on | off | KubeExecutor: `seccompProfile: RuntimeDefault`. |
| `DAGRON_TASK_ACTIVE_DEADLINE_SECS` | u64 > 0 | unset | KubeExecutor: `activeDeadlineSeconds` on task pods. Without it a hung task holds a node slot until the run-level timeout notices. |
| `DAGRON_TASK_RUNTIME_CLASS` | string | unset | KubeExecutor: `runtimeClassName` (e.g. `gvisor`) — kernel-surface isolation for untrusted task images. |
| `DAGRON_TASK_NODE_SELECTOR` | `k=v,k=v` | unset | KubeExecutor: pin task pods to specific nodes, e.g. an untrusted-workload pool. |
| `DAGRON_TASK_TOLERATIONS` | `key[=value][:effect]` comma list | unset | KubeExecutor: tolerations on task pods, so they can land on a tainted pool (the node selector alone leaves them Pending there). `key=value` tolerates that value (`Equal`); a bare `key` any value (`Exists`); `:NoSchedule`, `:PreferNoSchedule` or `:NoExecute` limits it to one effect. An empty key or value is refused, since an empty key would tolerate every taint, and so is one the apiserver would refuse in a toleration: a key must be a label key (an optional DNS-subdomain prefix and `/`, then at most 63 letters, digits, `-`, `_` or `.`, a letter or digit at each end) and a value a label value. A malformed value stops the engine at startup. |
| `DAGRON_TASK_PRIVILEGED` | `allow`, or unset | unset (a task that asks is refused) | **A task that asks to be privileged** (`privileged: true`) has its own container privileged: root on its host, with every device and capability (`docs/HOWTO.md`, "A privileged task"). For a **dedicated pool** alone: the engine does not start with it unless it claims the classes named in `RUNNER_CLASSES` and not `default`, runs no work that is not trusted (`DAGRON_UNTRUSTED_ISOLATION_FLOOR` unset), has a floor that does not drop capabilities, ask for no new privileges, name a runtime class or seccomp's default profile, and is not `EXECUTOR=local`. On Kubernetes, also: task pods go to nodes of the pool's own (`DAGRON_TASK_NODE_SELECTOR`), nothing set for every task pod contradicts privilege (`DAGRON_TASK_DROP_ALL_CAPABILITIES`, `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT`, `DAGRON_TASK_RUNTIME_CLASS`), and the task namespace admits a privileged pod, asked of the API server as a dry run (Pod Security: `pod-security.kubernetes.io/enforce=privileged` on a namespace of the pool's own). A task held to what contradicts privilege by its own `isolation` is refused at dispatch. Unset, a task that asks is failed once, with the reason. |
| `DAGRON_TASK_PRIORITY_CLASS` | PriorityClass name | unset | KubeExecutor: `priorityClassName` on task pods, e.g. so that under node pressure task pods are preempted before the engine is. Each pod is also labelled `dagron.dev/priority-class` with the name, when it fits a label (at most 63 characters), so the pods one class admitted can be selected by label, e.g. to stop the leftover tasks of a tier a workspace has left. |
| `DAGRON_TASK_ISOLATION_FLOOR` | `hardened`, or `key=value` comma/newline list | unset = no floor | The trust envelope **no task may go below**, whatever its workflow declares. Every knob above it is process-wide — one envelope for every task this scheduler dispatches — which is the wrong shape when the engine runs other people's code. A task declares its own envelope with `isolation:` (see `docs/HOWTO.md`); this floor is raised over it field by field, taking the stronger value and never the weaker, so an author may harden a task **beyond** the floor and never **under** it. A named `runtime_class` in the floor is *pinned*, not compared: which sandbox runtimes a node has is an operator fact. Parsed at **boot** — an unknown key or a floor this executor cannot enforce is a startup failure, because a floor discovered to be invalid at dispatch was not in force for anything dispatched before it. Keys: `runtime_class`, `seccomp` (`runtime_default`/`unconfined`), `read_only_root_fs`, `no_new_privileges`, `drop_all_capabilities`, `run_as_non_root`, `run_as_user`, `service_account_token`, `network` (`none`/`default`/`allow:<name>`; a floor's allowlist is pinned as its runtime class is, and `none` is over any). `hardened` is the preset: seccomp `runtime_default`, read-only root, no new privileges, all capabilities dropped, non-root, no service-account token; it leaves the network alone. **Which executor enforces what:** the Kubernetes executor every key, `network` only where its operator has said the cluster enforces NetworkPolicy (`DAGRON_TASK_NETWORK_POLICY`), since no field of a pod gives it (and a floor of `network=none` there refuses every task with a `checkout:`, which is every dagron-ci job: on Kubernetes a checkout is fetched in the task's own pod); the Docker executor every key but `runtime_class`, which names a handler on a cluster's nodes, and `network=allow:<name>` only for a list its operator named (`DAGRON_TASK_NETWORK_ALLOWLISTS`); the local executor none, since a subprocess runs as the engine does. On Docker `run_as_non_root` is held as a kubelet holds it: a task that names no `run_as_user` is refused unless its image's `USER` is a number other than 0, and `run_as_user` is the task's container's alone (the containers that install its tools and fetch its checkout run as their own images say). |
| `DAGRON_UNTRUSTED_ISOLATION_FLOOR` | `hardened`, or `key=value` comma/newline list | unset = no task of such a run is run | The floor for the tasks of a run that is **not trusted** (`docs/HOWTO.md`, "Runs that are not trusted"): a pull request from a fork, as a CI service records it. Written as `DAGRON_TASK_ISOLATION_FLOOR` is, and applied over it to those tasks alone. Unset, the engine runs none of them: each is failed where it would have been dispatched, with the reason. Set, the engine starts only where it can hold such a task: not with `EXECUTOR=local`; on Docker only with `DAGRON_DOCKER_NETWORK`; on Kubernetes only with `network=none` or `network=allow:<name>` in this floor. `service_account_token=false` is always part of it. |
| `DAGRON_UNTRUSTED_IMAGES` | comma-separated patterns | unset = any image | The images a task of a run that is not trusted may name: `docker.io/library/*,ghcr.io/acme/**`. `*` is any run of characters up to a `/`, `**` any run of characters, `?` one character. An image is matched as the task wrote it. A task that names another is failed where it would have been dispatched; one that names none runs in the engine's own default. On Docker a private image is one the host already holds, so without a list such a task may name it. |
| `DAGRON_UNTRUSTED_LIMITS` | `memory=…,pids=…,cpus=…,timeout=…,attempts=…,tasks=…,services=…`, any of them | `memory=4g,pids=2048,cpus=2,timeout=3600,attempts=3,tasks=256,services=4` | What a task of a run that is not trusted is held to. `timeout` (seconds, 1 to 604800): a task's longest, and what one that names none has. `attempts` (1 to 100): the most a task is tried. `tasks` (1 to 100000): the most a run may have; one with more has none of them run, and such a run is the size it was made. `services` (0 to 8): the most a task may have beside it; one with more is not run. A task's own numbers stand where they are less. `memory`, `pids`, `cpus`: each container made for such a task, its services' among them, `EXECUTOR=docker` only; memory takes `k`, `m` or `g` and is given no swap beyond it, and `cpus` may be a fraction. On Kubernetes a pod's limits are its own `resources` and its namespace's. None can be turned off, and an unknown key stops start-up. Such a task is also given none of `DAGRON_DOCKER_MOUNTS` and not `DAGRON_DOCKER_WORKDIR`, and draws from the pool `untrusted` (`POOLS=untrusted:N` caps it). |
| `DAGRON_TASK_POD_WATCH` | `on`, `off` | `on` | KubeExecutor: how a task learns that its pod has ended. `on`: the engine keeps one watch of the namespace's task pods (those labelled `app=module-54-scheduler`), and a task is told of its own pod's changes: no request is made for a pod while the watch tells of it. `off`: each task asks the API server for its pod twice a second for as long as it runs, as every task did before. The watch needs `list` and `watch` on pods, which the chart's Role has. Where it cannot follow (it is refused, its connection goes quiet for 15 s), it says so once in the log and each task asks for its own pod until it follows again; a task also asks until the watch has first seen its pod. Any other value stops the engine at startup. |
| `DAGRON_TASK_SECRET_ENV` | `secret`, `inline` | `secret` | KubeExecutor: how a task's secret env vars (`value_from`) reach its pod. `secret` creates a per-task Kubernetes Secret, owned by the pod and deleted with it, and references it with `secretKeyRef`, so the value is **not** in the Pod spec that `get pods` / `describe` / audit logs show. Needs `create`, `patch`, `delete` on `secrets` in the task namespace (the chart's engine Role grants them); if the Secret cannot be created the task **fails** naming that permission, never falling back to plaintext. `inline` puts the value in the pod spec as before, for clusters where the engine has no Secret RBAC. Anything else is refused at startup. The Docker and local executors are unaffected (see docs/HOWTO.md §6). |
| `DAGRON_TASK_AUTOMOUNT_SA_TOKEN` | `1`/`true` = on | **off** | KubeExecutor: mount a ServiceAccount token into task pods that did NOT declare `service_account:`. **This is the one default that changed**: such a task never asked for an identity, and on an IRSA cluster the token it was being handed is an IAM credential given to arbitrary task code. Set it to restore the old behaviour. Tasks that DO declare `service_account:` are unaffected and keep their token. |
| `DAGRON_TASK_NETWORK_ALLOWLISTS` | names, comma-separated | unset (a task that names an allowlist is refused) | **The allowlists a task may name** with `isolation: { network: allow:<name> }`. A task that names one reaches what that list lets it, and nothing else; it picks a name and cannot write a rule. **DockerExecutor:** each name is a network of the operator's on the daemon, `dagron-task-allow-<name>`, made `--internal` (no route out) and with no address of the host's on it (`docker network create --internal -o com.docker.network.bridge.inhibit_ipv4=true dagron-task-allow-<name>`: a bridge the host has an address on is a way to whatever the host serves there), with on it what the list allows. The engine reads each as it starts and does not start where one is missing, not internal, or a bridge the host has an address on (or IPv6, or of another driver), and reads it again before each task held to it: one removed since has its task refused before anything is made. The task's container, its services, its steps and its daemon are on that network alone; its checkout, tools and actions are fetched first, on `DAGRON_DOCKER_NETWORK`. **Either executor:** a list by hostname is a proxy of the operator's, named by the label (Docker network) or annotation (NetworkPolicy) `dagron.dev/proxy`: `http://` or `https://`, a host and a port, no user and no path. A task held to it is told `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`, `https_proxy`, and `NO_PROXY`/`no_proxy` for itself and its services, each unless it set it itself; its services, which are on the list's network (Docker) or under the pod's policy (Kubernetes), are told the same under their own settings, and so on Kubernetes are its checkout's and actions' fetches. **KubeExecutor:** each name is a NetworkPolicy of the operator's in the task namespace, `dagron-task-allow-<name>`: the pods labelled `dagron.dev/network=allow-<name>`, nothing in, and out only its egress rules (the chart makes them from `engine.taskNetworkAllowlists`). Needs `DAGRON_TASK_NETWORK_POLICY=enforced`, and all that is said there holds: the engine reads each policy as it starts and does not start where one is missing, selects other pods, lets something in, or has an egress rule with no `to`; it refuses beside another policy that gives such a pod more; and the task starts behind a gate (`dagron-ci-exec wait-allowlist`, from `DAGRON_CI_EXEC_IMAGE`). That gate asks the cluster's API server alone, so **an allowlist must not allow the API server**: one that did would have its tasks fail unstarted. A name is 1 to 57 characters of `[a-z0-9-]`. A task that names a list not given here is refused, and so is one on the local executor. |
| `DAGRON_TASK_NETWORK_POLICY` | `enforced` | unset (a task that asks for no network is refused) | KubeExecutor: the operator's word that **this cluster's CNI enforces NetworkPolicy**, which is what `isolation: { network: none }` needs on Kubernetes. Nothing in the API says whether a CNI enforces a policy or stores it and does nothing, so it is the operator's to say. With it, a task that asks for no network gets a pod labelled `dagron.dev/network=none`, which the NetworkPolicy `dagron-task-no-network` of the task namespace allows nothing in and nothing out, name resolution included. The policy is the operator's to apply (the chart makes it with `engine.taskNetworkPolicy=true`); the engine makes no policy, reads the namespace's policies as it starts and again before each such task (it needs `get` and `list` on `networkpolicies`), and does not start where this one is missing, selects other pods, or allows anything. **Nor where another policy gives some of it back:** a pod has what *any* NetworkPolicy that selects it allows, so a namespace's "every pod may reach the name server" would leave such a task resolving names. The engine refuses beside a policy that selects the task's pod and has a rule in either direction, and names it; keep such a policy from these pods with `{key: dagron.dev/network, operator: DoesNotExist}` in its `podSelector`'s `matchExpressions`. **The task is not started on that word alone.** Its pod gets an init container, last, from `DAGRON_CI_EXEC_IMAGE` (so this needs that set), which asks for a connection at the cluster's API server and at the pod's name servers and goes on only once none is made at any of them three times running; a pod that still has a network after 30 s fails, and its task with it, unstarted. Silence is not told apart from an outage: were the API server and every name server down at once before the pod's network was taken, the gate would let it through. Not every CNI holds a pod from its first instant, and this is what makes the difference: on k3s, whose own controller applies a policy a second or two after a pod starts, an unheld task reached the API server as it began. The wait costs such a task a few seconds. A task with a `checkout:` cannot ask for no network on Kubernetes: its checkout is fetched by an init container of the same pod. Unset, such a task is refused, as it always was. |
| `DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS` | comma-separated names | **unset = none** | KubeExecutor: the ServiceAccounts a task may name with `service_account:`. A task pod runs in the executor's namespace beside the engine, whose own ServiceAccount can create pods and read Secrets (the chart's engine Role), so letting a workflow-authored task run as an arbitrary ServiceAccount is a privilege escalation. Unset, **any `service_account:` is refused** and the task fails naming this variable. `service_account:` is the IRSA seam, so an install that uses it lists here the task identities it created for that purpose — **never** the engine's own ServiceAccount, or any with pod-create / Secret RBAC. A task that names none is unaffected (it runs as `default`, whose token is withheld unless `DAGRON_TASK_AUTOMOUNT_SA_TOKEN` is set — or a task's `isolation.service_account_token: true` asks for it, which the isolation floor governs). |
| `DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS` | `key=value` comma list | unset | KubeExecutor: labels put on every task pod that runs as a ServiceAccount (`service_account:`), and on no other. For a cloud whose workload identity is given by a webhook that acts only on labelled pods: on AKS, `azure.workload.identity/use=true`, without which a task running as its identity's ServiceAccount gets no token. Each key must be a label key and each value a label value, and none may be the engine's own (`app`, `dagron.dev/*`), by which its task pods are selected. A malformed value stops the engine at startup. |
| `DAGRON_QUOTA_WAIT_MAX_SECS` | u64 (s) | `21600` (0 = off) | KubeExecutor: how long a task may wait on a full namespace `ResourceQuota` (read in `dagron-engine/src/quota_wait.rs`). A pod the apiserver refuses because the namespace is at its quota (`exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4`) started nothing, so it is back-pressure rather than a failed attempt: the task goes back to `ready`, its attempt is given back, and it is dispatched again about 5 s later, then at doubling intervals up to about a minute, until a pod is admitted. This knob bounds one such wait. The clock starts at the first refusal and is stored on the task row, so for a task at rest in the queue it survives a restart or another replica taking the task over. A task whose probe was in flight when its engine stopped starts a new wait when its lease is recovered (`LEASE_SECS`), and, as with any lease recovery, that dispatch's attempt is not given back. Past the bound the task **fails for good**, whatever its `max_attempts`, and its output carries the last refusal. Only a refusal that waiting can cure qualifies: a request larger than the quota's hard limit, and every other pod-create error, fails the attempt. `gang:` members under `RUNNER_GANGS` never wait — a refusal fails the member and cancels its siblings. **`0` turns the wait off**, and a refusal is a failed attempt. Where a full-quota refusal is a failure (a gang member, or `0`), its text is `not started: pod refused by namespace ResourceQuota '<quota>' (requested: …, used: …, limited: …)`, not the `create pod '…': …` line other pod-create errors carry; and a dispatch refused by a full quota records no sample in the task-duration histogram, whether or not the task waits. Set it the same on every replica: the one that receives the refusal applies its own value. A waiting task is an ordinary queued task: it counts toward `MAX_INFLIGHT_TASKS` and its run toward `MAX_INFLIGHT_RUNS`, it holds no `POOLS` slot, and `run_timeout_secs` still cancels it. Waiting tasks are admitted in no particular order, not by `priority` and not by time waited: see the quota row in [docs/OPERATIONS.md](OPERATIONS.md#troubleshooting-symptom-first). Counted by `scheduler_quota_refusals_total` and `scheduler_tasks_waiting_on_quota` ([docs/METRICS.md](METRICS.md)). |
| `RUNNER_CLASSES` | comma list | unset = claim **every** class | Runner segmentation: restrict this scheduler to claiming tasks whose `runner_class` is in the list (e.g. `etl,pulse`). Names validated like the spec field (`[a-z0-9_-]{1,64}`) — a typo is a startup error, not an unclaimable task class. Unset keeps the single-pool behavior. |
| `POOLS` | `name:slots` comma list | unset = no pools | Named concurrency pools (#21): capacity per pool, e.g. `POOLS=etl:4,db:2`. A task's `pool:` draws a slot; the claim runs at most `slots` tasks of a pool at once, holding the rest in `ready` until one frees (no run dropped). Names validated like `runner_class`; a non-positive/unparseable slot count is a startup error. On Postgres, pooled claims serialize via a global advisory lock (the unpooled fast path stays lock-free); an unpooled or unconfigured-pool task is unlimited. Keep the value identical across HA replicas. |
| `DB_MAX_CONNECTIONS` | u32 ≥ 2 | `8` | Postgres pool size (read in `dagron-core/src/db/postgres.rs`). Lower it (2–3) for lean engines sharing a pooled state cluster; min 2 keeps claim tx + listener from deadlocking. SQLite ignores it (pinned to 1 by design). **Connection budget:** one engine holds this pool, and with `DATABASE_LISTEN_URL` one direct session more for its `LISTEN` (without it, the `LISTEN` is one of the pool's). A database's connection limit (`ALTER DATABASE … CONNECTION LIMIT`, or the role's) has to cover every process on it at once: each engine's and each `dagron-api`'s pool and `LISTEN`, a `dagron-ci`'s pool (`CI_DATABASE_MAX_CONNECTIONS`; it holds no `LISTEN`), the pods of a rolling update (each old pod keeps its pool and `LISTEN` until it exits, and Kubernetes does not count a terminating pod against its surge, so count two pods for each replica), and a few sessions for an administrator. Beyond it, a new connection is refused, not queued. |
| `DATABASE_LISTEN_URL` | postgres conn string | unset = listener shares the pool config | Split-DSN seam for shared state cells: the reconcile loop's `LISTEN` session connects here (the **direct** Postgres endpoint) while `DATABASE_URL` may point at PgBouncer transaction pooling — which cannot serve a session-scoped `LISTEN`. Postgres builds only. |
| `CRON_CONFIG` | path | unset = cron off | Cron schedule YAML (below). Leadership-gated; keeps the process resident. |
| `GC_RETENTION_SECS` | i64 > 0 | unset = GC off | Retention window for the run/task GC. Leadership-gated; resident. A purged run's task logs leave the log store too (`DAGRON_LOG_STORE`), about a minute after its rows: with no retention, the store grows as the database does. |
| `GC_INTERVAL_SECS` | u64 | `3600` | GC sweep interval. |
| `GC_ARCHIVE_DIR` | path | unset = plain purge | Archive-before-purge: the GC sweep exports each expired terminal run as a self-contained `dagron.run-archive.v1` JSON file (`run-<id>.json`: run + definition + tasks + outbox events; atomic tmp→fsync→rename) and purges **only** verified exports. Point it at an object-store-synced volume. **Set it on `dagron-api` too** — the same path/bucket — or the console cannot read archived runs back, and `POST /api/runs/{id}/archive` answers `501` rather than archiving. |
| `GC_ARCHIVE_URL` | `s3://` \| `gs://` \| `az://` \| `azure://` `bucket[/prefix]` | unset | Cloud archive-before-purge (**requires the matching cargo feature** — `archive-s3` / `archive-gcs` / `archive-azure`, all of which the published images have; a scheme without its feature is a startup error, never a silent plain purge). Same document/purge contract as `GC_ARCHIVE_DIR`, but each run is one atomic object `PUT`; credentials/region/endpoint from the backend's standard env (`AWS_*` — incl. `AWS_ENDPOINT_URL` for MinIO — / `GOOGLE_*` / `AZURE_*`). Wins over `GC_ARCHIVE_DIR`. **Set it on `dagron-api` too** — the same bucket/prefix, and that binary built with the matching feature, as its published image is — or `POST /api/runs/{id}/archive` answers `501`, and the console cannot read archived runs back. |
| `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS` | i64 | `30` | `dagron archive-compact` only: documents younger than this stay **individually retrievable** (`/api/archive/runs/{id}`); older ones fold into the Parquet dataset and become analytics-only. `0` compacts everything eligible. |
| `READY_AGE_ALERT_SECS` | i64 | `300` (`0` = off) | Stale-ready (unclaimable-class) alert: WARN when a runner class's oldest `ready` task has waited longer than this — catches a class no live scheduler serves. Leadership-gated; runs in any resident daemon. Same signal exported as `scheduler_ready_oldest_age_seconds{runner_class=…}`. |
| `READY_AGE_CHECK_INTERVAL_SECS` | u64 | `60` | How often the stale-ready alert loop checks. |
| `WAIT_POLL_SECS` | u64 > 0 | `15` | Poll interval for `type: wait` HTTP sensors (`wait.url`, #27 follow-on): a parked sensor is GETed at most once per interval and succeeds on the first `2xx`. A due sensor is claimed before it is GETed, so however many engines sweep, it is GETed once per interval; a claim lasts this long or 11 s, whichever is more (longer than the GET's timeout), and an engine that dies mid-GET leaves the sensor due again when it lapses. **Redirects are not followed** — the sensor reads the origin's own status, and following a 3xx would let an external URL pivot the *scheduler's* network position toward internal/metadata addresses; a 3xx simply reads as "not ready". Note `wait.url` polls run from the **scheduler**, not the task sandbox: treat workflow authorship as trusted with respect to the scheduler's network reachability, or set `WAIT_URL_DENY_PRIVATE` below. Time/dataset sensors ignore this. |
| `SUBWORKFLOW_MAX_DEPTH` | i64 > 0 | `8` | How deep `type: workflow` tasks may nest. **Depth is not breadth**: a `repeat:` on a trigger creates one child run per iteration at the *same* depth, so this cap does not bound a loop — `repeat.max_iterations` does, and it is required. See [Loops over sub-workflows](#loops-over-sub-workflows). A workflow that names itself — directly or through a cycle of workflows — would otherwise spawn child runs without end, each leaving a parked parent row behind; there is no stack to overflow, so nothing stops it on its own. A trigger at or past the cap **fails that task** with a message naming the depth, leaving the rest of the run to proceed under normal failure handling. Depth is read by walking `task_runs.sub_run_id` up from the triggering task's own run (no `parent_run_id` column; SQLite migration 034 / Postgres 041 index it), and the walk stops at the cap — so the check costs at most `SUBWORKFLOW_MAX_DEPTH` indexed lookups, on sub-workflow dispatch only. |
| `WAIT_URL_DENY_PRIVATE` | `1`/`true`/`on`/`yes` = on | off | Restrict `wait.url` polls to **globally-routable** addresses. Off by default because the common `wait.url` *is* an internal address (`http://svc.default.svc/ready`) — turn it on when you run untrusted workflow specs and the scheduler's network position is wider than a task pod's (realistically `EXECUTOR=kubernetes` with a differentiated NetworkPolicy). When on, private/loopback/link-local (incl. `169.254.169.254`), `0.0.0.0/8`, CGNAT, multicast and reserved ranges are refused, for IPv4, IPv6, and IPv4-mapped IPv6 alike. Enforced in two places: IP-literal hosts are checked before the request (a literal never reaches a resolver — and the check parses the URL with the *same* parser reqwest uses, so legacy spellings like `http://2130706433/` and `http://127.1/` are normalized before they are judged), and hostnames are filtered **inside the client's DNS resolver**, so the addresses dialed are the ones that passed — a DNS rebind has no check-to-connect window. Proxies are also bypassed while the policy is on: with `HTTP_PROXY`/`HTTPS_PROXY` set, the resolver would only ever judge the *proxy's* address while the proxy dialed the blocked target. A refused URL logs a WARN and re-parks as "not ready"; it does not fail the task, so turning the policy back off resolves parked sensors in place. Editing the spec does not — `wait.url` is materialized into `task_runs.wait_url` when the task row is created, so a spec fix applies to the next run, not to an already-parked task. |
| `DATASET_TRIGGERS` | `0`/`false` = off | on | Dataset-triggered scheduling sweep ([docs/DATASETS.md](DATASETS.md)): sync `on_datasets:` subscriptions from the workflow registry and fire runs when subscribed datasets update (every 5 s, `dataset_triggers` in `SWEEP_INTERVALS`; HA-safe CAS claims, and one engine a window where `SWEEP_SHARE` is on; a workflow that drops its `on_datasets:` has its subscriptions cleared by whichever engine sweeps next). A spec this engine cannot read (in a rolling upgrade, one a newer engine reads) is neither synced nor cleared here, and a fire of it is handed back for an engine that reads it; the first hand-back of each such workflow is logged as a warning, with the reason, once a process, and where no engine reads the spec (a broken one) its fires are handed back on every pass until it is fixed. A pass that could not list the workflows fires nothing. Disabling only stops this scheduler's sweeping — `produces:` recording and dataset sensors are run-local and stay on. |
| `DB_SCHEDULES` | `1`/`true` | off | Fire DB-backed UI schedules (the ones `dagron-api` manages). Leadership-gated; resident. |
| `LEADER_LEASE_SECS` | i64 > 0 | `30` | Leadership lease for cron/GC/schedules (exactly-one-node guarantee). |
| `WORKFLOW_DIR` | path | `/workflows` | GitOps seed target: inside the container image, bundled examples are copied here on first start when empty. |
| `OPENLINEAGE_URL` | URL | unset = off | Emit an OpenLineage RunEvent per finalized run (`dagron-lineage`), naming the run's input and output datasets ([DATASETS.md](DATASETS.md)); best-effort. |
| `OPENLINEAGE_NAMESPACE` | string | `dagron` | OpenLineage namespace. |
| `DAGRON_CDEVENTS_URL` | URL | unset = off | Send each run's end as CDEvents v0.5.1 (CloudEvents in HTTP binary mode): a `taskrun.finished` for each task that ran, then the run's `pipelinerun.finished`, through the outbox's `engine.cdevents` sink, retried with backoff. An event's `context.id` is stable across retries. |
| `DAGRON_CDEVENTS_SOURCE` | URI reference | `dagron` | The CDEvents' `source`, the engine they are from. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | URL | unset = export off | With an `otel`-feature build, which the published engine, dagron-api and dagron-ci images are, enables OTLP (HTTP/protobuf) export to this collector: spans (#28 follow-on; dagron-ci opens none yet), log records and, from the engine and dagron-ci, their metrics (what `/metrics` serves, sent as OTLP sums, gauges and histograms). A signal's own `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` sends that signal alone, and `OTEL_<SIGNAL>_EXPORTER=none` keeps it home; other `OTEL_EXPORTER_OTLP_*` vars tune headers/timeouts, and `OTEL_METRIC_EXPORT_INTERVAL` how often metrics go (60000 ms). Ignored without `--features otel`. See [OpenTelemetry](#opentelemetry---features-otel). |
| `DAGRON_EVENTS_SOURCE` | URI reference | `/dagron` | The `source` of the CloudEvents a `notify.webhook` sends, which a receiver that hears from more than one installation tells them apart by. See [CloudEvents](#cloudevents). |
| `DAGRON_ARTIFACT_API_INSECURE` | `allow` | unset = the URL is `https://` | `allow` lets `DAGRON_ARTIFACT_API_URL` be plain `http://`, for a closed network where a task's credential may cross in the clear. The engine warns at start that it does. |
| `DAGRON_ARTIFACT_API_URL` | `https://` URL | unset = a task that asks for the artifact API is refused | Where a task with `artifact_api: true` reaches dagron-api's `/task/v1/artifacts`, as the task's network reaches it: for a task that does not see `DAGRON_ARTIFACT_DIR` (in a container, a pod). The engine gives such a task this URL as `DAGRON_ARTIFACTS_API` and a credential of the attempt's own, `DAGRON_ARTIFACTS_TOKEN`, good while the task runs, with which it reads its run's artifacts and writes its own, in dagron-api's store, the one an approval's `binds` is checked against ([API.md](API.md#a-tasks-artifacts)). `https://`, as `DAGRON_OIDC_REQUEST_URL` is. |
| `DAGRON_ARTIFACT_DIR` | path | unset = off | Local artifact store root; each task gets its run's shared dir injected as `DAGRON_ARTIFACTS`, plus a per-task `DAGRON_CHECKPOINT_DIR` under it for checkpoint-aware resume (`dagron-artifact`, [docs/AI_WORKLOADS.md](AI_WORKLOADS.md)). |
| `DAGRON_ARTIFACT_URL` | `s3://` \| `gs://` \| `az://` bucket/prefix | unset = off | Cloud artifact/checkpoint location. The engine injects per-run/per-task URLs (`DAGRON_ARTIFACTS_URL`, `DAGRON_CHECKPOINT_URL`) — tasks reach the bucket with their own tooling — and the artifact API serves the same bucket via the `dagron-artifact` cloud backend (features `s3`/`gcs`/`azure`, which dagron-api's `archive-*` features turn on and its published image has; credentials from the standard `AWS_*`/`GOOGLE_*`/`AZURE_*` env; takes precedence over `DAGRON_ARTIFACT_DIR` for API reads/writes). |
| `DAGRON_CACHE_URL` | `s3://` \| `gs://` \| `az://` bucket/prefix | unset = off | The store a task's `workspace.cache` is kept in. The engine signs URLs for it as it dispatches such a task and moves no bytes itself; a store that cannot sign is refused at start. Same backends and credentials as `DAGRON_ARTIFACT_URL`, compiled in by the engine's `archive-s3`/`archive-gcs`/`archive-azure` features. Also `DAGRON_CACHE_QUOTA_BYTES` (10 GiB a repository), `DAGRON_CACHE_UNUSED_DAYS` (7), `DAGRON_CACHE_MAX_ENTRY_BYTES` (4 GiB), `DAGRON_CACHE_URL_SECS` (6 h, for a task with no timeout) and `DAGRON_CACHE_SWEEP_SECS` (30): [CI_RUNNERS.md](CI_RUNNERS.md#caches). dagron-api is given it too, with the store's own variables, to stream a GitHub workflow's artifacts (`GET /api/runs/{run_id}/workflow-artifacts/{name}`); without it, they are listed and not downloaded. |
| `DAGRON_OIDC_REQUEST_INSECURE` | `allow` | unset = the request URL is `https://` | `allow` lets `DAGRON_OIDC_REQUEST_URL` be plain `http://`, for a closed network where a task's request credential may cross in the clear (a test rig's). The engine warns at start that it does. |
| `DAGRON_OIDC_REQUEST_URL` | `https://` URL | unset = a task that asks for an identity is refused | Where a task with `identity:` asks dagron-api's issuer for tokens (its `/oidc/token`, as the task's network reaches it). The engine gives such a task this URL, a request credential of its own (`DAGRON_OIDC_REQUEST_TOKEN`, good while the task runs) and its first audience (`DAGRON_OIDC_AUDIENCE`). It is `https://`: the task sends the credential there from its own container, so a plain `http://` URL crosses a network whatever host it names, and the engine does not start with one but where `DAGRON_OIDC_REQUEST_INSECURE=allow`. |
| `DAGRON_ARTIFACT_TIER` | bool | — (tiering off) | Turn the artifact store into a **tiered** one: every write lands locally first and is uploaded to `DAGRON_ARTIFACT_URL` later, under a budget. Needs both `DAGRON_ARTIFACT_DIR` and `DAGRON_ARTIFACT_URL` — asking for tiering with only one of them is a startup error, never a silent downgrade. For units that capture data on a metered or intermittent link. |
| `DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY` | u64 (bytes) | — (unlimited) | Daily uplink budget for the tiered store, counted in UTC days and persisted, so a restart does not hand the unit a fresh allowance. An artifact larger than the whole budget is skipped with a warning rather than blocking everything queued behind it. |
| `DAGRON_BUNDLE_PUBKEYS` | comma-separated keys | — (bundles refused) | Trusted ed25519 public keys (32 bytes, hex or base64) for **signed workflow bundles** ([docs/BUNDLES.md](BUNDLES.md)). Verification fails closed: unset means every bundle is refused, and there is no unsigned path. |
| *(injected into tasks)* | — | — | The engine sets these **in every task's environment** at dispatch (they are not read from the operator's environment): `DAGRON_RUN_ID` / `DAGRON_TASK` / `DAGRON_TASK_ID` (task identity, e.g. for `POST /runs/{id}/tasks/{task_id}/checkpoint`); `DAGRON_ARTIFACTS` + `DAGRON_CHECKPOINT_DIR` (when the local artifact store is on); `DAGRON_ARTIFACTS_URL` + `DAGRON_CHECKPOINT_URL` (when `DAGRON_ARTIFACT_URL` is set); `DAGRON_CACHE` (the signed URLs of a task's `workspace.cache`, when `DAGRON_CACHE_URL` is set); `DAGRON_ARTIFACTS_API` + `DAGRON_ARTIFACTS_TOKEN` (for a task with `artifact_api: true`, when `DAGRON_ARTIFACT_API_URL` is set); `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_SERVICE_V2` and `ACTIONS_RUNTIME_TOKEN` (the cache service and a credential for it, for a task with a workspace of a run with a repository, when `DAGRON_ACTIONS_CACHE_LISTEN` is set); and, on retry attempts of a task that reported a checkpoint, `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER` ([docs/AI_WORKLOADS.md](AI_WORKLOADS.md)). An engine built with `otel`, as the published images are, also sets `TRACEPARENT`, unless the task's workflow names one of its own ([OpenTelemetry](#opentelemetry---features-otel)). |
| `DAGRON_SENSITIVE_ENV_PATTERNS` | comma list | `SECRET,TOKEN,PASSWORD,PASSWD,PWD,CREDENTIAL,APIKEY,ACCESS_KEY,PRIVATE_KEY` | Task env var **name** substrings (case-insensitive) whose values are masked to `***` in task output/logs (secret masking, #8). Set empty to disable name-based masking. In a secret of several lines, the value of a `key: value`, `key = value` or `KEY=value` line whose key matches (its quotes off, `-` read as `_`), or is one that holds a secret in such files (`auth`, `passphrase`, `identitytoken`, `…_key_data`, or one of those after a `_`), is masked on its own too, where it is of 8 characters or more; not with this set empty. |
| `DAGRON_REDACT_ENV` | comma list | unset | Engine-process env var **names** whose values are always masked in task output (e.g. `DATABASE_URL`), on top of the name-pattern matching above. |
| `DAGRON_SECRET_<NAME>` | string | unset | Value for a task env `value_from: { secret: <name> }` reference (#9); `<name>` uppercased with non-alphanumerics → `_`. Resolved at dispatch; masked in output. Read for `trusted` runs alone: this store belongs to no environment, so no policy speaks for it, and a run of another trust class has what an environment that admits it holds and nothing else (HOWTO §6). |
| `DAGRON_SECRETS_DIR` | path | unset | Directory of secret files (one per secret, filename = secret name) for `value_from` refs — the SOPS / External-Secrets / k8s-secret mount convention. Checked after `DAGRON_SECRET_<NAME>`, and for `trusted` runs alone, as that is. |
| `DAGRON_ENV_SECRET_KEY` | string | unset = env-secret store off | Encryption key for **UI-managed environment secrets** *and* **per-repository GitOps credentials** (AES-256-GCM): 32 bytes of base64 used verbatim, any other string hashed to key length. Must be set identically on dagron-api (encrypts on write), the engine (decrypts secrets at dispatch), and the `dagron-gitops` worker (decrypts Git credentials at clone). The environment store is checked **before** `DAGRON_SECRET_<NAME>` / `DAGRON_SECRETS_DIR` for runs with an `environment:`. Helm: `envSecrets.*`; compose: the `x-env-secret-key` anchor. |
| `GITHUB_TOKEN` / `GITLAB_TOKEN` | token | unset = forge feedback off | Enables `notify.git` commit statuses (#14): `pending` when a run starts, its result when it ends. Set it on the engine, which posts them all, including for runs submitted through dagron-api. `GITHUB_API_BASE` / `GITLAB_API_BASE` override the API base for GHE / self-managed GitLab. |
| `DAGRON_GIT_TOKEN` | token | unset | **Fallback** token for the GitOps pull sync (#12), used by repos with no credential of their own; falls back to `GITHUB_TOKEN`. Sent **only to trusted forge hosts** (see `DAGRON_GIT_TRUSTED_HOSTS`) and redacted from any error output. Read by the `dagron-gitops` worker. Prefer a **per-repository** credential (console → GitOps → Credential, or `PUT /api/git-repos/{id}/auth`): it is scoped to one repo, rotatable without a redeploy, and can be an SSH key. |
| `DAGRON_GIT_TRUSTED_HOSTS` | comma-list | unset | Extra hosts (and their subdomains) the **fallback** token may be sent to — add your GHE / self-managed GitLab host. Built-ins (`github.com`, `gitlab.com`, `bitbucket.org` + `*.github.com`, `*.gitlab.com`) always apply. A repo on any other host is cloned without the token — unless it has a per-repository credential, which is not host-filtered because it was bound to that repo deliberately. |
| `DAGRON_GIT_SSH_STRICT` | bool | `false` | Refuse to sync an SSH repository whose credential has no `known_hosts` entries, instead of accepting whatever host key answers. Off by default so repos connected without host keys keep syncing; turn it on to require the forge's host key be pinned. Read by the `dagron-gitops` worker. |
| `DAGRON_GIT_ALLOW_INSECURE` | bool | `false` | Allow `http://`, `git://`, and `file://` clone URLs for the GitOps sync. Off by default (only `https://` / `ssh://`) to avoid plaintext fetches, SSRF, and local-path reads; set `1` for `file://` in tests / air-gapped dev. |

### Sweeps

The reconcile loop's maintenance sweeps, the names `SWEEP_INTERVALS` takes
and the `sweep` label of their metrics ([docs/METRICS.md](METRICS.md#sweeps)).
An **installation** sweep acts on the datastore as a whole, and with
`SWEEP_SHARE` on one engine a window runs it; an **engine** sweep acts on what
is this engine's own, or each due row of it is claimed, and every engine runs
it.

| Sweep | Scope | Default interval | What it does |
|---|---|---|---|
| `orphans` | engine | `DAGRON_ORPHAN_SWEEP_SECS` (300 s) | Deletes the workloads of tasks no longer live (with `DAGRON_INSTALLATION` alone). Not in `SWEEP_INTERVALS`. |
| `leases` | installation | `SWEEP_INTERVAL_MS` | Takes back the tasks whose lease has lapsed. |
| `sessions` | installation | `SWEEP_INTERVAL_MS` | Takes back the tasks of remote runners' sessions that have lapsed. |
| `stops` | engine | `SWEEP_INTERVAL_MS` | Stops the work of this engine's tasks that were asked to stop. |
| `quota_summary` | engine | 60000 | Logs the tasks waiting on a namespace quota (off with `DAGRON_QUOTA_WAIT_MAX_SECS=0`). |
| `deadlines` | installation | `SWEEP_INTERVAL_MS` | Fails the runs past their `run_timeout_secs`. |
| `stopped_runs` | installation | `SWEEP_INTERVAL_MS` | Ends the runs asked to stop that have nothing left stopping. |
| `admissions` | installation | `SWEEP_INTERVAL_MS` | Lets in the runs that wait for their workflow's cap. |
| `alerts` | installation | `SWEEP_INTERVAL_MS` | Sends the soft-deadline alerts. |
| `approvals` | installation | `SWEEP_INTERVAL_MS` | Times out the approval gates past their timeout. |
| `subworkflows` | installation | `SWEEP_INTERVAL_MS` | Resolves the triggers whose child run has ended. |
| `fanouts` | installation | `SWEEP_INTERVAL_MS` | Expands and joins the runtime fan-outs. |
| `waits` | installation | `SWEEP_INTERVAL_MS` | Resolves the time sensors that are due. |
| `url_waits` | engine | `SWEEP_INTERVAL_MS` | Polls the HTTP sensors that are due, each claimed, so it is polled once whichever engine polls it. |
| `external_polls` | engine | `SWEEP_INTERVAL_MS` | Polls the deferred external jobs that are due, each claimed. |
| `external_cancels` | engine | `SWEEP_INTERVAL_MS` | Tears down the remote jobs that tasks which ended (a cancel, a deadline) left running, each claimed. |
| `dataset_waits` | installation | `SWEEP_INTERVAL_MS` | Resolves the dataset sensors whose dataset has a new update. |
| `dataset_triggers` | installation | 5000 | Syncs the `on_datasets:` subscriptions and fires their runs (off with `DATASET_TRIGGERS=0`). |
| `teardowns` | engine | `SWEEP_INTERVAL_MS` | Claims the stopping tasks nobody is watching, for their teardown, as this engine has room. |
| `gangs` | engine | 60000 | Warns of a gang larger than this engine's pool (with gangs alone). |
| `reap` | installation | `REAP_INTERVAL_MS` (5000) | Takes a slice of the running runs and ends those whose tasks are all over: the net under the runs a notification (Postgres) or the file's triggers (SQLite) named. Not in `SWEEP_INTERVALS`. |

`SWEEP_INTERVAL_MS` is `POLL_INTERVAL_MS` unless it is set. A sweep runs on
a tick once its interval has passed, so its cadence is at least the time
between ticks.

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
A build that puts settings of its own around the engine tells the scan their
names (the `env_names` seam), and they are not warned of either.

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
A Postgres service by default (SSE rides `LISTEN/NOTIFY`). Built
`--no-default-features --features sqlite`, it is one process beside one engine
on the same SQLite file, serving sign-in, tokens, workflows, runs, logs and
live updates, and `501` for the rest: [ONE_NODE.md](ONE_NODE.md). The pool and
listener rows below are the Postgres build's.

| Variable | Type | Default | What it does |
| --- | --- | --- | --- |
| `DATABASE_URL` | postgres conn string; `sqlite://<path>` or a path in the `sqlite` build | **required** (startup error) | The same database the engine writes. |
| `DAGRON_SQLITE_POLL_MS` | u64 ≥ 50 | `500` | The `sqlite` build only: how often live updates read, in place of `LISTEN/NOTIFY`, the runs the file's triggers wrote to `run_changes` since the last read (a run made, a run's status, a task made, a task's status or attempt, a task not over taken away), past a cursor this process keeps, by their key. At its start, and when its cursor is more than 65,536 changes behind, it reads every run that has not finished or finished in the last 30 s instead, and sends each. An update reaches the console up to this long after the engine writes it. |
| `DATABASE_LISTEN_URL` | postgres conn string | unset = SSE listener shares the pool config | Direct (non-PgBouncer) endpoint for the shared `task_events` SSE listener — same split-DSN seam as the engine (see the engine table). |
| `GC_ARCHIVE_DIR` / `GC_ARCHIVE_URL` | path / `s3://` \| `gs://` \| `az://` \| `azure://`… | unset = `/api/archive/runs/{id}` answers 502 | Where the `/api/archive` endpoints fetch archived run documents — the **same values the engine's GC uses**. A cloud URL needs dagron-api built with the matching feature (`archive-s3` / `archive-gcs` / `archive-azure`), as the published image is. The list endpoint reads only the `archived_runs` index and needs neither. |
| `DAGRON_JWT_SECRET` | string, **≥ 32 chars** | **required** (startup error if unset/short) | HS256 key that signs and validates session JWTs. |
| `PORT` | u16 | `8080` | Listen port. |
| `DAGRON_DB_MAX_CONNECTIONS` | u32 ≥ 1 | `8` | Postgres pool ceiling (was hard-coded). At least 2 without `DATABASE_LISTEN_URL`: the SSE listener then holds one of the pool's connections for as long as it listens, and a pool of 1 would leave none for requests. With it, the listener's is one direct session beside the pool. See the connection budget under the engine's `DB_MAX_CONNECTIONS`. |
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
| `DAGRON_MAX_WAITING_RUNS` | i64 | `100` (floor 1) | How many runs may wait for one workflow's `max_active_runs`, or one `concurrency_key` of it (`when_full: queue`, `supersede`); a submit past it answers `429`. The engine's knob of the same name, read here because a run is made here too: set it the same on both. |
| `DAGRON_METRICS_CACHE_SECS` | u64 (s) | `2` (`0` = off) | How long `GET /api/metrics` may serve its counts of what has not finished (runs and tasks in the statuses still to end, and the dead letters) from a cache. Its default is 2 s, not the engine's 10 s, because a person watches these: shorter than the console's poll of its Metrics page (every 5 s), so each poll shows counts no older than 2 s, while the polls of every Metrics page open on this replica in that time share one read (three counts: the runs and the tasks by status, and the dead letters), where each poll read them before. One read at a time; a failed one, or one given up after 30 s, is never cached, and answers `500`. **`0` reads them on every call**, and, where `DAGRON_METRICS_HISTORY_CACHE_SECS` is not set, the finished statuses too. Per replica. The engine's `/metrics` has its own (`METRICS_CACHE_SECS`). |
| `DAGRON_METRICS_HISTORY_CACHE_SECS` | u64 (s) | `300`; `0` where `DAGRON_METRICS_CACHE_SECS` is `0` (`0` = with every read of the rest) | How long `GET /api/metrics` may serve its counts of the finished statuses, which grow with every run, from a cache: as the engine's `METRICS_HISTORY_CACHE_SECS`, read the same way and given up after the same time. Its answer's `history_age_secs` says how old they are. A request that finds them stale is answered with the last ones at once, and they are read again behind it, one read at a time, so they can be as old as this TTL, plus the time until the request that finds them stale, plus the read it starts, and older while reads of them fail. Not set, it is `300`, or `0` where `DAGRON_METRICS_CACHE_SECS` is `0`; set, it is its own. |
| `DAGRON_TRUST_PROXY_HEADERS` | bool | `false` | Key the login rate limit off the first `X-Forwarded-For` hop instead of the socket peer. **Only** set this when something in front actually rewrites the header (the console's `/api` proxy, an ingress): a directly-reachable dagron-api lets any caller forge it and mint a fresh budget per request. Left off, every request behind a proxy shares one bucket — still a ceiling, just a global one. |
| `MALLOC_MMAP_THRESHOLD_` | bytes | unset (glibc auto-tunes) | Not read by dagron-api — glibc reads it. glibc raises its mmap threshold as it sees repeated frees of Argon2-sized blocks, after which that scratch comes off the heap and is never returned to the OS. Pinning it (`131072`) keeps every hash an mmap, released on free. Measured over 30 logins: unset 67 → 227 MB; pinned 5.6 → 5.9 MB. Set in `compose.yaml`. |
| `MALLOC_ARENA_MAX` | usize | unset (glibc default) | Also glibc's. `2` caps per-thread arenas, which lowers the idle baseline (67 → 5.6 MB here). It does **not** fix the post-hash plateau on its own — that needs the threshold above. |
| `GITHUB_TOKEN` / `GIT_REPO` | PAT / `owner/name` | unset → `POST …/sync-to-git` answers `501` | Enable workflow → Git PR sync. |
| `GIT_BASE` | branch | `main` | Base branch for sync PRs. |
| `GIT_PATH_PREFIX` | path prefix | the connected repo's `path`, else `dags/` | Where synced specs are committed. Unset, it is the `path` of the connected repository matching `GIT_REPO`, so a merged PR lands where the pull loop watches. |
| `GIT_API_BASE` | URL | `https://api.github.com` | GitHub API root (GHE). |
| `DAGRON_GIT_ALLOW_INSECURE` | bool | `false` | Permit `http`/`git`/`file` clone transports for registered GitOps repos (tests / air-gapped dev only — off so a server-side clone can't be pointed at plaintext, an internal host, or a local path). |
| `DAGRON_ARTIFACT_MAX_BYTES` | usize | `134217728` (128 MiB) | Body cap for artifact PUTs (separately from the 1 MiB core cap). |
| `DAGRON_ARTIFACT_PRESIGN` | `on` \| `off` | `on` | Whether `POST …/upload` and `GET …/download` answer with a URL the artifact store signs, where it can sign one (S3 and what speaks it, GCS, Azure Blob), so that the bytes go straight to the store. `off`: every transfer is relayed through the API. A directory, a store that seals what it keeps and a tiered store are relayed whatever this says. Set `off` where clients cannot reach the store at the address the API uses for it. Any other value stops the start. |
| `DAGRON_ARTIFACT_PRESIGN_SECS` | u64 (s), 60 to 604800 | `900` | How long a signed URL is good for. Whoever holds one may make its one request until then. |
| `DAGRON_ARTIFACT_RETENTION_DAYS` | days, 0 to 3650 | unset = artifacts are kept | How long an artifact recorded in the index is kept, and the most a `?retention_days=` may ask for. Unset or `0`: an artifact is kept unless its upload asked otherwise. It is read when an artifact is recorded: changing it does not change when artifacts already recorded expire. A value that is no such number stops the start. |
| `DAGRON_ARTIFACT_SWEEP_SECS` | u64 (s), 0 to 86400 | `300` (0 disables) | How often expired artifacts are deleted, bytes then record, and direct uploads that never completed are cleared up. Replicas sweep at once without meeting. A store that deletes nothing (a tiered one) says so once in the log, and nothing of it is swept. |
| `DAGRON_ARTIFACT_SYNC_SECS` | u64 (s) | `60` (0 disables) | How often the tiered artifact store drains to its remote tier (`DAGRON_ARTIFACT_TIER`). A no-op for every other store, so the loop is harmless when tiering is off; `POST /api/artifacts/sync` is the on-demand path for a unit that has just docked. |
| `DAGRON_OIDC_ISSUER` | `https://` URL | unset = no issuer | The OIDC issuer for tasks' identities: `{issuer}/.well-known/openid-configuration` and `{issuer}/.well-known/jwks` are served at its path, and `GET /oidc/token` signs a token for a task's request credential. Needs `DAGRON_OIDC_KEYS`; `DAGRON_OIDC_TOKEN_SECS` (600, 60 to 3600) is how long a token lasts. [CI_RUNNERS.md](CI_RUNNERS.md#a-jobs-identity). |
| `DAGRON_OIDC_KEYS` | path | — | A PEM RSA private key, or a directory of `*.pem`: every key is published, the one whose name sorts last signs. A public key alone (PKCS#8 or PKCS#1) is published and signs nothing, unless it is the newest and `DAGRON_OIDC_SIGN_CMD` signs with it. |
| `DAGRON_OIDC_SIGN_CMD` | command | unset = the newest key is a private key | Enterprise: the newest key is a KMS's, and this signs with it. Given the SHA-256 of a token's signing input, base64, on stdin, it writes the RSASSA-PKCS1-v1_5 signature, base64, on stdout (`aws kms sign --message-type DIGEST --signing-algorithm RSASSA_PKCS1_V1_5_SHA_256`, say), within `DAGRON_ENV_KMS_TIMEOUT_SECS`. Each signature is checked against the newest key's public half before a token is given. An open build is refused at start. |
| `DAGRON_AUDIT_SINK_URL` / `DAGRON_AUDIT_SINK_TOKEN` | URL / bearer | unset = local audit table only | Read where the `enterprise` feature is on: forward each audit record to a central sink (fire-and-forget). |
| `DAGRON_SESSION_LEASE_SECS` | seconds, ≥ 15 | `90` | How long a remote runner's session holds its tasks without a heartbeat; a runner heartbeats every third of it. Once it lapses an engine's sweep takes the session's tasks back to `ready`. Set it the same on every replica. |
| `DAGRON_RUNNER_MIN_VERSION` | `MAJOR.MINOR.PATCH` | unset = any release | The oldest `dagron-runner` release a session is opened for; an older one, or a build with no release (`dev`), is answered `426`. A value that is no release stops the start. |
| `DAGRON_RUNNER_CLAIM_RECHECK_MS` | ms, ≥ 100 | `1000` | How often a runner's waiting claim looks for work it was not told of: a task an engine's tick makes ready sends no notification. Each look is a read; a claim is made only when there is work. |
| `DAGRON_RUNNER_BODY_LIMIT` | bytes, ≥ 65536 | `16777216` (16 MiB) | The most a runner's request may carry (a log segment, a result with the end of a task's output). |
| `DAGRON_LOG_TASK_CAP_BYTES` | bytes | `67108864` (64 MiB) | The engine's knob, read here for a remote runner's task: the most of its output that is kept, and, twice it and 1 MiB, of its log (its beginning, of the cap, and its end, which a runner's host keeps no more than the cap of, `DAGRON_LOG_TAIL_BYTES`); a segment past that is answered `409`. Set it to the engines' and the runners' cap, or more. |
| `DAGRON_LOG_STORE` | `file:///DIR`, or `s3://`, `gs://`, `az://` | unset (each log in its task row, as before) | dagron-api: where it reads task logs from, the engine's own setting. A task attempt's log is closed segments, each a zstd object of about `DAGRON_LOG_SEGMENT_BYTES` of text with a row in `log_segments`, and an open part. With `file:///DIR` both are files under `DIR`, which the engine and dagron-api share (a volume), and nothing of the log goes through the database but a row for each closed segment. With an object store the closed segments are objects (the engine's cloud credentials, as for `DAGRON_CACHE_URL`), and the open part is the task row's `log`, as a whole log was, never larger than a segment: a closed segment's text leaves the row in the transaction that records it, so a reader sees each line once. The engine and dagron-api must name the same store. dagron-api removes nothing from it: the engines do, once nothing reads a log (`DAGRON_LOG_SWEEP_SECS`), and its archive route's purge records what they are to remove. A log written before it was set is read from its row as before; a remote runner's log stays in its row. A setting that cannot be used stops the process. |
| `DAGRON_LOG_SEGMENT_BYTES` | 4096 to 1073741824 | `8388608` (8 MiB) | How much text a log segment holds before it closes (`DAGRON_LOG_STORE`). |
| `DAGRON_LOG_MAX_TAILS` | ≥ 1 | `256` | The live log relay (`GET /api/runs/{id}/logs/stream`): how many runs this replica follows at once. A run is followed once, however many streams watch it; a stream of one more is answered `429` with `Retry-After`, unless a run nobody watches any more can make room. Each followed run is a statement a pass (a second, or sooner where its notification comes). Per replica. |
| `DAGRON_LOG_MAX_STREAMS` | ≥ 1 | `2048` | How many log streams this replica serves at once, of watched runs and of runs that are over alike; one more is answered `429` with `Retry-After`. A stream lasts 10 minutes at most (and to its session's expiry), and one whose client takes nothing of it for 60 s ends: each is resumed from its cursors, its caller asked who they are again. Per replica. |
| `DAGRON_LOG_STREAM_POLL_MS` | ms, ≥ 50 | `1000` | How often the relay reads the runs it follows when nothing tells of them: a `file://` store's growth and the SQLite build's tell nobody. On Postgres a log append to a row-kept log, and a task's end, wake it at once. |
| `DAGRON_LOG_STREAM_LINGER_MS` | ms | `5000` | How long a run nobody follows any more stays followed, so a page loaded again finds it so. |
| `DAGRON_LOG_RELAY_BYTES` | bytes, ≥ 1048576 | `67108864` (64 MiB) | The most text the relay's channels hold at once, for the whole process: past it a pass sends where the logs got to, and each stream reads the text itself. A stream that falls behind is read from the store at its own pace. |
| `POOLS` | `name:slots,…` | unset = no pools | The engine's pools, read here because a remote runner's claims count against them as an engine's do: set it the same on both. A value the engine would refuse stops the start. |

Every knob above is registered in `crates/dagron-api/src/config.rs`: startup
logs each explicitly-set value (secrets redacted) plus a **configuration
fingerprint** (also in `GET /api/health` as `config_fingerprint`) so a fleet
can alert when one replica's settings drift from the reviewed deployment.

### Fleet link & offline licence (read by `GET /api/link`)

Read in `crates/dagron-api/src/routes/{link,link_ee}.rs`. **The API reports what
its own environment says.** In a deployment where the engine and the API get
different env blocks, an engine that is linked shows here as not linked — the
page is reading this process, not the machine.

Both routes are registered in every build, and both are admin-only. Built
without the `enterprise` feature they answer `403` with a signpost naming the
single-instance path, rather than a `404` — the moment someone asks is the
moment they have a second instance to manage.

None of these are set by enrolling. Enrolment is **generate, then apply**:

1. `POST /api/link/enrol` makes the one outbound call and returns the unit
   credential **once**, rendered as an env block, Helm values, and a systemd
   drop-in. It stores nothing and never hot-reloads the uplink.
2. Set `DAGRON_FLEET_URL` and `DAGRON_FLEET_TOKEN` where **both** the engine and
   the API read their environment — per the note above, an env block applied to
   only one of them leaves a linked engine reporting as unlinked — then restart
   the engine.
3. `GET /api/link` reads the result back: the URL, whether the token is set, and
   whether the outbox is actually draining.

There is no unlink call. Revoke the unit where it was enrolled, unset the two
variables, restart.

| Variable | Type | Default | What it does |
| --- | --- | --- | --- |
| `DAGRON_FLEET_URL` | `http(s)://` origin | unset = not linked | The control plane this instance reports to. Parsed, not prefix-matched — the same rule the enrol call applies, so a URL that enrols is a URL the unit will accept later. |
| `DAGRON_FLEET_TOKEN` | string | unset | The unit credential minted by `/units/enrol`. `GET /api/link` reports only **whether** it is set; the value is never returned or logged. |
| `DAGRON_UNIT_SERIAL` | string | unset, then `HOSTNAME`, then empty | Prefills the serial field on the enrol form — a hint only, never a decision. The serial that is actually sent is the one submitted, and it is what gets validated (1–64 of `[A-Za-z0-9-_.:]`, matching the plane) before the join token is spent. |
| `DAGRON_CLOUD_LICENSE` | `<payload>.<sig>` ed25519 token | unset = absent | The offline licence. Verified signature-first against the public key below, then expiry; the payload is only parsed once the signature holds. Takes precedence over `_FILE`. |
| `DAGRON_CLOUD_LICENSE_FILE` | path | unset | File form of the above, for a mounted Secret. Read only when `DAGRON_CLOUD_LICENSE` is unset; an empty or unreadable file reports as present-but-invalid with the reason, not as absent. |
| `DAGRON_CLOUD_LICENSE_PUBLIC_KEY` | ed25519 key, hex or base64 (std or url-safe), 32 bytes | unset = licence cannot be verified | The vendor key. Without it a present licence is reported as unverifiable rather than valid. |
| `DAGRON_CLOUD_LICENSE_REQUIRED` | `1` \| `true` \| `yes` \| `on` = on | `false` | Mirrors the control plane's own switch, and is only *reported* by this route — reading it here changes nothing on the instance. It is what lets the page say whether an invalid licence is merely noted or is the reason the plane will not start. |

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

## `dagron-runner` environment

The remote runner. Every setting is an environment variable; the full
reference, with the executor's own variables, is
[its README](../crates/dagron-runner/README.md#settings).

| Variable | Default | What it does |
| --- | --- | --- |
| `DAGRON_RUNNER_URL` | **required** | dagron-api's base URL: `https://`, or `http://` to this machine. |
| `DAGRON_RUNNER_JOIN_TOKEN_FILE` | unset | The one-time join token, for the first start: the runner registers, and keeps its credential. |
| `DAGRON_RUNNER_TOKEN_FILE` | `$DAGRON_RUNNER_STATE_DIR/credential` | The runner's credential (`dgr_…`): written at registration, or given (a Secret). |
| `DAGRON_RUNNER_STATE_DIR` | `~/.dagron-runner` | Where the credential is kept when no file is named for it. |
| `DAGRON_RUNNER_NAME` | the host's name | How the runner is listed. |
| `DAGRON_RUNNER_LABELS` | all it was registered for | The runner classes this process takes tasks of. |
| `DAGRON_RUNNER_CAPACITY` | `1` | Tasks at once, 1 to 256. |
| `DAGRON_RUNNER_EXECUTOR` | `docker` | `local`, `docker` or `k8s` (`k8s` needs `--features kubernetes`). |
| `DAGRON_RUNNER_SECRETS` | `server` | `server`: dagron-api sends a task's secrets from its environment. `home`: it sends their names, and the runner reads them from its own store (`DAGRON_SECRET_<NAME>`, `DAGRON_SECRETS_DIR`). |
| `DAGRON_RUNNER_DRAIN_TIMEOUT_SECS` | unset = as long as the tasks run | How long a drain waits before it stops the running tasks. |
| `DAGRON_RUNNER_INSECURE` | unset | `allow`: plain `http://` to another host. |
| `DAGRON_RUNNER_OIDC_REQUEST_URL` | `$DAGRON_RUNNER_URL/oidc/token` | Where a task that asks for an identity trades its request credential for tokens, as the task's network reaches dagron-api. |
| `DAGRON_RUNNER_ARTIFACTS_URL` | `$DAGRON_RUNNER_URL/task/v1/artifacts` | Where a task that asks for its run's artifacts (`artifact_api: true`) reaches dagron-api's door, as the task's network reaches it: given to the task as `DAGRON_ARTIFACTS_API`, with its attempt's credential as `DAGRON_ARTIFACTS_TOKEN`, good while it runs, for reading its run's artifacts and writing its own task's. |
| `DAGRON_TASK_ISOLATION_FLOOR` | unset | As the engine's: the floor every task run here is raised to, refused where the executor cannot hold it. |
| `DAGRON_LOG_TASK_CAP_BYTES` | `67108864` (64 MiB) | As the engine's: how much of a task's log is sent as it runs, and of its output. No more than dagron-api's own. |
| `DAGRON_LOG_TAIL_BYTES` | a quarter of the cap | As the engine's: how much of the end of a log past its cap is sent once the task is over. |
| `DAGRON_SCALE_TARGETS` | — | `dagron-runner scale`: `label=deployment` or `label=namespace/deployment`, comma-separated. With `DAGRON_SCALE_MIN` (0), `_MAX` (10), `_PER_REPLICA` (1), `_INTERVAL_SECS` (15), `_DOWN_AFTER_SECS` (300). |

## `dagron-mcp` environment

| Variable | Default | What it does |
| --- | --- | --- |
| `DAGRON_API_URL` | `http://localhost:8080` | The `dagron-api` edge the MCP adapter calls. |
| `DAGRON_MCP_TOKEN` | unset | Session JWT sent as `Authorization: Bearer` (mint one via login or `scripts/mint-dev-token.mjs`). |

## `dagron-ci` environment

The CI sidecar ([`crates/dagron-ci`](../crates/dagron-ci/README.md)): it
queues forge events, from webhooks and from polling each repository, and runs
each trusted one's pipelines through dagron-api.
Read once at start; a value that is set but wrong stops the process.

| Variable | Default | What it does |
| --- | --- | --- |
| `CI_AGENT_RUNS` | `refuse` | What a run an agent asks for through the run API gets: a caller whose dagron-api subject is `agent:<name>`, as an agent's token presents (`POST /api/tokens` with `agent`). `refuse`: it is queued and skipped, and the agent reruns and cancels nothing. `untrusted`: it runs as an `agent` run, confined as an untrusted run is, on a cell whose dagron-api takes dagron-ci's trigger record (the `ci:trigger` group); and the agent reruns and cancels the events it asked for, and no other. |
| `CI_API_TOKEN_FILE` | unset (required with `CI_CELLS`) | File holding the dagron-api API token dagron-ci runs workflows with. |
| `CI_CELLS` | unset | `name=url` pairs, comma-separated: the dagron-api each cell's runs go to, such as `default=https://dagron-api.example.com`. Each URL is `https://`, or `http://` to this machine (a sidecar that carries it on): dagron-ci's token for dagron-api is sent there, and the credential of each caller of the run API, so a cell over plain http to another host stops start-up. Unset, events stay queued and nothing runs them. |
| `CI_CELLS_INSECURE` | unset | `allow`: a cell in `CI_CELLS` may be plain `http://` to another host, on a closed network where that is accepted, such as a dagron-api beside dagron-ci behind a network policy. `CI_API_TOKEN_FILE`'s token, and each caller's credential, cross that network unencrypted. |
| `CI_CHECKOUT_TOKEN_SECRETS_DIR` | unset | Directory holding one file per cell, named as the cell (a Kubernetes Secret with a key per cell, mounted), with the secret that cell's engines present: an engine holding it is answered for its own cell's runs alone. A file for a cell `CI_CELLS` does not name, or one secret for two cells stops start-up. |
| `CI_CHECKOUT_TOKEN_SECRET_FILE` | unset (`POST /ci/v1/checkout-tokens` answers 404, unless `CI_CHECKOUT_TOKEN_SECRETS_DIR` is set) | File holding the secret engines present as a bearer to ask for a checkout's credential, for a run on any cell: their `DAGRON_CHECKOUT_TOKEN_SECRET_FILE` holds the same. An empty file stops start-up. So does this secret where `CI_CELLS` names more than one cell: it answers for every cell, so an engine of one could ask for the credential of another's run. Give each cell its own in `CI_CHECKOUT_TOKEN_SECRETS_DIR`. |
| `CI_DATABASE_MAX_CONNECTIONS` | `8` | Pool size for `CI_DATABASE_URL`: all dagron-ci holds on it (it holds no `LISTEN`). On the engine's own database, count it in that database's connection budget (the engine's `DB_MAX_CONNECTIONS`). |
| `CI_DATABASE_SCHEMA` | `dagron_ci` | Postgres schema for dagron-ci's tables and its migration history, created on first start if the role may. Lets dagron-ci share dagron-api's database without sharing its migrations. |
| `CI_DATABASE_URL` | — (required) | `postgres://…` or `sqlite://…`: the event queue. |
| `CI_DEFAULT_TIMEOUT_SECS` | `3600` | The timeout of a compiled pipeline's tasks when the pipeline sets none (1 to 604800). `dagron-ci compile` and `dagron-ci run` read it as well. |
| `CI_EVENT_LEASE_SECS` | `60` | How long a worker holds an event before another may take it over (5 to 3600). A paths filter's shallow fetch is given a third of it, and 20 s at most. |
| `CI_EVENT_MAX_ATTEMPTS` | `8` | Attempts at an event before it is left `failed` (1 to 100). |
| `CI_EVENT_WORKERS` | `8` | Event workers in this replica (0 to 256); `0` runs the receivers alone. |
| `CI_FLAKE_DAYS` | `14` | How many days back a job's flakes are counted for `CI_FLAKE_ISSUES`, and kept (1 to 90). |
| `CI_FLAKE_ISSUES` | `off` | Whether a job that flakes is filed an issue, and at how many flakes. A number N (1 to 1000) keeps each job of a trusted run that passed only after an attempt of it failed, and files an issue of a job that did so N times within `CI_FLAKE_DAYS`, written again in place as it flakes again; a person closes it, and its flakes are counted from then. With the App's token for `issues: write` alone on a github.com repository it is installed on, or the token `CI_GITLAB_TOKENS_DIR` keeps for a GitLab host. dagron-ci does not start with it set and neither. |
| `CI_FLEET_ENVIRONMENT` | `dagron-ci-fleet` | The environment, in each cell's dagron-api, that a fleet runner's just-in-time configuration is kept in as a secret while the runner is out, and deleted from after. dagron-ci makes it where a cell has none of the name. Its secrets are an environment's: the cell's dagron-api needs `DAGRON_ENV_SECRET_KEY` to keep one. |
| `CI_FORK_APPROVAL` | `first-time` | With `CI_FORK_PRS=untrusted`: which such pull requests wait for an approval before any pipeline of them is submitted. `first-time`: those whose author has, for all the forge says, no commit in the repository and no place on it. `always`: every one. `never`: none. The approval is asked for each head commit. |
| `CI_FORK_APPROVAL_TIMEOUT_SECS` | `604800` | How long that approval waits before it is taken as refused (60 to 7776000). |
| `CI_FORK_APPROVERS` | — | Who may give it, comma-separated, as a workflow's `approvers:` takes them: an email, or `group:<name>`. Required with `CI_FORK_PRS=untrusted` unless `CI_FORK_APPROVAL=never`: an approval with no approvers is one anybody signed in may give. |
| `CI_FORK_PRS` | `refuse` | What a pull request from a fork or a non-member gets. `refuse`: it is not run. Where dagron-ci posts statuses (a github.com repository, with the App installed on it or `CI_GITHUB_TOKEN_FILE` set), its head commit is given an `error` status that says so; elsewhere the event is skipped and nothing is posted. `untrusted`: it is run, as an untrusted run (`docs/CI_PIPELINES.md`, "Pull requests from forks"), on a cell whose dagron-api takes dagron-ci's record of what started a run (the `ci:trigger` group) and whose engine has a floor for such runs (`DAGRON_UNTRUSTED_ISOLATION_FLOOR`). On a cell that takes no record it is not run, and its head says so; on an engine with no floor each of its jobs fails with the reason. |
| `CI_GITHUB_API_URL` | `https://api.github.com` | Where github.com's REST API is reached: an egress proxy or a cache in front of it. `https://`, or `http://` to this machine (a sidecar), since `CI_GITHUB_TOKEN_FILE`'s token goes there. Git's ref listing still goes to github.com. |
| `CI_GITHUB_APP_ID` | unset (checkouts take no credential) | The GitHub App dagron-ci acts as on github.com. Each checkout's credential is minted as it: an installation token for the run's one repository, `contents: read`, revoked when the attempt ends. dagron-ci's own calls for a repository the App is installed on are made as it too: a token for that repository alone (contents and pull requests, read; commit statuses, write), used for fifty minutes and minted again. A positive number; set with `CI_GITHUB_APP_KEY_FILE`, or with `CI_GITHUB_APP_TOKEN_URL` where another service holds the key, as one without the other stops start-up. |
| `CI_GITHUB_APP_KEY_FILE` | unset (set with `CI_GITHUB_APP_ID`) | File holding that App's private key, PEM, which signs its requests to GitHub. Read and checked at start: a key that is not an RSA key stops start-up. |
| `CI_GITHUB_APP_TOKEN_SECRET_FILE` | unset (set with `CI_GITHUB_APP_TOKEN_URL`) | File holding the secret dagron-ci asks `CI_GITHUB_APP_TOKEN_URL` for the App's tokens under, as a bearer token. Read at start. |
| `CI_GITHUB_APP_TOKEN_URL` | unset (dagron-ci holds the App's key) | Where the App's installation tokens are asked for, in place of `CI_GITHUB_APP_KEY_FILE`: a service that holds the App's key, so that one App serving many dagron-ci instances has its key in one place. `POST {repository, permissions, one_repository}` is answered with GitHub's own `{token, expires_at}`, or 404 where the App is not installed for the asker; dagron-ci then signs nothing and holds no key. `https://`, or `http://` to this machine; one or the other of the two with `CI_GITHUB_APP_ID`, as both, or neither, stops start-up. |
| `CI_GITHUB_APP_TOKEN_URL_INSECURE` | unset | `allow`: `CI_GITHUB_APP_TOKEN_URL` may be plain `http://` to another host, on a closed network where that is accepted. Its secret, and every token it gives, cross that network unencrypted. |
| `CI_GITHUB_CHECKS` | `off` | Whether a job is reported as a check run. `on` reports each job of a repository the App is installed on, with `checks: write`, as a check run and not a commit status, under the name its status had, and takes a check's "Re-run", and a commit's "Re-run all checks", as reruns. A job's test report (`report:`) and its steps' summaries go on its check run. It needs `CI_GITHUB_APP_ID`, and dagron-ci does not start with it on and no App. A repository the App is not installed on, or was not given the permission for, keeps its commit statuses. |
| `CI_GITHUB_PR_COMMENT` | `off` | Whether a pull request is given a comment that says how its runs went. `on` keeps one comment on each pull request of a repository the App is installed on with `pull requests: write`: each pipeline's jobs for the pull request's latest commit, how each ended, and what a job said of its tests. It is written again in place as the runs change, at most once in twenty seconds, with a token of the App's for that permission alone. dagron-ci does not start with it `on` and no `CI_GITHUB_APP_ID`. |
| `CI_GITHUB_FLEET_FILE` | unset (no job of GitHub Actions' own is served) | File holding the fleet's label map, YAML: `labels:`, each with the `image` its runner is run from and, where said, a `runner_class`, `pool`, `priority`, `timeout_secs`, `command` and the labels it goes `with`. A job of GitHub Actions' own whose `runs-on` asks for one of them is given GitHub's own runner, started as a dagron run. Read and checked at start: dagron-ci does not start on a file it cannot read, or with one set and no `CI_GITHUB_APP_ID`, or with one set and no cell to start a runner in (`CI_CELLS`, `CI_API_TOKEN_FILE`). With one set and no `CI_GITHUB_WEBHOOK_SECRET_FILE` it starts, and warns: a job is heard of by its delivery alone, so that replica serves only the jobs another replica takes. On the chart the map is the value `ci.fleet.labels`. The App needs **Administration: Read and write** on the repository and the **Workflow job** event. |
| `CI_GITHUB_TOKEN_FILE` | unset | File holding a GitHub token for the github.com repositories the App is not installed on or cannot have a token for, or for all of them without `CI_GITHUB_APP_ID`: refs and pull requests for the poll sweep, and each event's pipelines and changed files for the workers (contents and pull requests, read). Each run's commit statuses are posted with it too, which needs `statuses: write`; a repository it may not post to records why, once per run. Sent to github.com only (or `CI_GITHUB_API_URL`). Unset, a repository without the App is read anonymously, which a public one allows inside GitHub's 60 requests an hour, and gets no statuses. |
| `CI_GITHUB_WEBHOOK_SECRET_FILE` | unset | File holding the secret `POST /hooks/github` checks `X-Hub-Signature-256` with. Unset, the receiver answers 404. |
| `CI_GITHUB_WORKFLOWS` | `off` | Whether a repository's `.github/workflows` files are read. `on` runs the ones a repository lists in its `.dagron/ci.yaml` (`github.workflows`), as pipelines, at level 1 of the GitHub Actions front-end ([`CI_GITHUB.md`](CI_GITHUB.md)). `off` reads none, and asks no repository for a `.dagron/ci.yaml`. |
| `CI_GITLAB_MR_NOTES` | `off` | Whether a GitLab merge request is given a note that says how the runs of its latest commit went. `on` keeps one on each merge request of a project on a host `CI_GITLAB_TOKENS_DIR` keeps a token for, written with it, and written again in place as its runs change. Needs a token. |
| `CI_GITLAB_PIPELINES` | `off` | Whether a GitLab project's `.gitlab-ci.yml` is read. `on` runs the one a project names in its `.dagron/ci.yaml` (`gitlab.pipeline`), with the files it includes from the project ([`CI_GITLAB.md`](CI_GITLAB.md)). `off` reads none. |
| `CI_GITLAB_TOKENS_DIR` | unset | Directory of GitLab access tokens, a file per host, named as the host (`gitlab.com`), each with the `api` scope: that host's projects are read with it, statuses posted, notes written, and each checkout's project access token minted. Each is sent to its own host alone, and never to a job. Unset, GitLab is read without credentials and given no statuses. |
| `CI_GITLAB_WEBHOOK_SECRET_FILE` | unset | File holding the secret token `POST /hooks/gitlab` compares `X-Gitlab-Token` with. Unset, the GitLab receiver answers 404. |
| `CI_LISTEN` | `0.0.0.0:8090` | HTTP listen address. |
| `CI_OBJECT_STORE_URL` | unset | dagron-ci's object store: `s3://`, `gs://`, `az://` (credentials from the environment, as the engine's stores read them) or `file:///path`. A webhook payload over 64 KiB is kept there; and a day past `CI_RECORD_DAYS` is written there as zstd segments, read back before the day leaves the database. Unset, such a payload is not kept, and a day past `CI_RECORD_DAYS` is dropped unwritten. |
| `CI_POLL_SECS` | `300` | How often the poll sweep reads a repository that sets no interval of its own (`dagron-ci repo set … --poll-secs N`): 30 to 86400 seconds, or `0` for never. |
| `CI_RECORD_DAYS` | unset | How many whole days before today dagron-ci's records are kept in the database (1 to 3650). Each hour the older days are written to `CI_OBJECT_STORE_URL`, where it is set, and dropped: a partition of `ci_events` a day on Postgres and the rows on SQLite, with their runs' rows, run API requests, report log, deliveries and stored payloads. Unset, every record is kept. |
| `CI_RUN_URL` | unset (statuses link nowhere) | The page a commit status links to: an `http(s)` URL in which `{run_id}` and `{cell}` are filled in with the run's, such as `https://dagron.example.com/runs/{run_id}`. Another `{…}`, or a run id in the host, is refused at start. |
| `CI_SEGMENT_DAYS` | unset | How many days after its day a segment of records in `CI_OBJECT_STORE_URL` is kept (1 to 36500). Unset, segments are kept. |
| `CI_URL_FOR_JOBS` | unset (no patch bundles) | Where a job reaches dagron-ci: an `https://` URL with no path, or `http://` to this machine, or with `CI_URL_FOR_JOBS_INSECURE`. A patched checkout fetches its bundle there (`/ci/v1/bundles/{id}/content`), from a link of an hour the checkout credential door gives the attempt. With `CI_OBJECT_STORE_URL` and the checkout door's secret, `POST /ci/v1/bundles` takes bundles (crates/dagron-ci/README.md, "Patch bundles"). |
| `CI_URL_FOR_JOBS_INSECURE` | unset (https only, or http:// to this machine) | `allow`: `CI_URL_FOR_JOBS` may be plain `http://` to another host, on a closed network where that is accepted. A link to a bundle is a bearer's: whoever sees it in passing reads the change until it expires. |

Inside a task, the steps' runner,
[`dagron-ci-exec`](../crates/dagron-ci-exec/README.md), reads two settings of
its own from the task's environment: `CI_RESULT_FILE` (where each step's
outcome is written, as well as logged) and `CI_STEP_GRACE_SECS` (`10`: how
long a step asked to stop, at its timeout or a cancelled task, has before
SIGKILL).

## Logging (all binaries, via `dagron-logging`)

`RUST_LOG` (full tracing filter, wins over everything), `LOG_LEVEL` (default
`info`), `LOG_FORMAT` (`full`|`compact`|`pretty`|`json`, default `full`),
`LOG_TARGET` (default `1`), `LOG_THREAD_IDS` (`0`), `LOG_THREAD_NAMES` (`0`),
`LOG_LINE` (`0`), `LOG_SPAN_EVENTS` (default `none`), `LOG_ANSI` (forces
colour on or off; never on for `json`). With `LOG_ANSI` unset, lines are
coloured only when stdout is a terminal and `NO_COLOR` is not set (or is
empty, as <https://no-color.org> has it): a file, a
pipe or a container's log gets none. The authoritative table is the doc
comment at the top of `crates/dagron-logging/src/lib.rs`.

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
time advances). It is an `LHS OP RHS` comparison (`== != <= >= < >`), or an
`in [..]` / `not in [..]` membership, joinable with `and` / `or`, over the
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
`defer: { kind, poll_secs?, max_wait_secs?, cost?, http? }` (**external job**
— the task's `command` is the *submit*: it starts work on a system dagron does
not own, prints `dagron::handle=<id>` on stdout and exits 0, and the engine parks
the row on that handle holding **no worker slot** until a reconcile sweep
resolves it. `kind` routes to a poller — a registered `ExternalPoller` gets
first refusal, then the built-in `http` transport. `poll_secs` defaults to 30,
floor 1. `max_wait_secs` bounds the **remote job**; unset means the run's own
deadline is the only bound, which is deliberate — the right ceiling is a
property of the job, and guessing one would fail exactly the long workloads
this exists for; watch `scheduler_external_parked` for a row with no ceiling
that is not moving. `cost` (default 1) is what one submission of this task is
worth, in whatever unit the author picked — the engine never learns what it
means, it only sums it against `budget.external_cost` below.
`http: { url, headers?, succeed_when, fail_when?, error_from? }` polls a status
endpoint, templating `{{ handle }}` into the URL
and matching dotted JSON paths — `fail_when` is evaluated first. **Three
different timeouts, deliberately separate:** `timeout_secs` bounds the submit
(default 25 s), `defer.max_wait_secs` bounds the job, `run_timeout_secs` bounds
the run — [docs/EXTERNAL_JOBS.md](EXTERNAL_JOBS.md)),
`produces: [<dataset-uri>, …]` (datasets this task updates on success — recorded
in the registry + lineage ledger, waking dataset sensors and firing
`on_datasets:` workflows; command tasks only; URIs template per instance —
[docs/DATASETS.md](DATASETS.md); a **deferred** task records these when its
remote job finishes, in the reconcile sweep, not when the submit returns), plus
executor extras (`docker_image`, `image_pull_policy`,
`resources`, `service_account`, `runner_class`). A `task_defaults:` block sets
DAG-wide defaults for `max_attempts`, `retry_delay_secs`, `retry_max_delay_secs`,
`retry_on_timeout`, `retry_budgets`, `timeout_secs`, `docker_image`,
`image_pull_policy` (which reaches a template's tasks, and is refused on a `template:` call itself), `runner_class`, `priority`, `pool`, and `env` (a task's own value always wins —
and `retry_budgets` merges **per class**, so a task that overrides one class
still inherits the rest rather than silently dropping them).

DAG-level fields (`DagSpec`): `run_timeout_secs` (hard run deadline → cancel),
`deadline` (soft SLA → alert),
`budget: { tasks?, external_cost?, external_cost_attribution?, tokens? }` (ceilings on how
big one run may get, enforced **at creation** — after expansion both numbers are
exact, so a run that would break a ceiling never starts instead of being killed
with work already spent; over the API a **400**. `tasks` caps the task **rows**
the run would create, not the spec entries an author typed, so a `with_items`
fan-out or a `gang:` counts as the many rows it becomes. `external_cost` caps the
summed `defer.cost` of the run's deferred tasks (gang-aware for the same reason);
these are numbers the *author* declared, never anything dagron measured, so the
arithmetic is exact and claims nothing about real spend — there is deliberately
no `spend:` field. `external_cost_attribution` asks for reconciliation of a
vendor's actual invoice back to the run, and is a validation error in this build
rather than a silently dropped key — [docs/EXTERNAL_JOBS.md](EXTERNAL_JOBS.md).
`tokens` caps the model tokens a run and the runs it starts spend, held at
**dispatch** by each task's `tokens:` reservation, and is likewise a validation
error in this build, where nothing keeps the meter; a task's own
`dagron::tokens=<n>` report line is taken out of its output in every build —
[docs/AI_WORKLOADS.md](AI_WORKLOADS.md#what-a-model-call-spent)),
`max_active_runs` (default unlimited; the max number of runs of this workflow —
by name — that may be `running` at once; further fires are held back with a
`MaxActiveRunsReached` error → the API returns **429**, queue submissions
requeue, and schedule/backfill fires wait for a slot),
`concurrency_key` (templated with the workflow's parameters, e.g.
`"{{ stack }}"`; requires `max_active_runs`) narrows that cap to runs sharing the
resolved key, so `max_active_runs: 1` allows one run per stack rather than one
per workflow, and the 429 names the key;
`when_full` (requires `max_active_runs`) says what becomes of a run that
arrives at that cap: `refuse` (what a workflow that says nothing gets, as
above), `queue` or `supersede`. With `queue` the run is **made, and waits**:
the submit answers `201`, the run reads `running` with `queued_at` set and no
`admitted_at`, none of its tasks advances, and it is let in when a run before
it has ended, oldest first, as many as the cap has room for. With `supersede`
(which needs `max_active_runs: 1`) the run is made and **every run of its key
before it is cancelled**, the ones that wait as the ones that run; it is let
in once what they had running has stopped, which for a run nothing was running
for is at once. A superseded run ends `cancelled`, with its `run.completed`
event, and its `output` says which run took its place. A run that waits is a
run: it is cancelled as any other, it counts toward `MAX_INFLIGHT_RUNS`, and
its `run_timeout_secs` is counted from when it is let in (a `deadline:` alert
is counted from when it was made). At most `DAGRON_MAX_WAITING_RUNS` runs wait
for one workflow, or one key of it; a run past that is refused as with
`refuse`. A rerun, a retry or a clear of a run that has ended does not wait
and takes nobody's place: while its key is full it is refused. A run that is
not trusted (a pull request from a fork) takes the place of runs of its own
source alone, the same repository and pull request or ref, since its key is
its author's to write; `param_schema` (rules per top-level
parameter: `required`, `enum`, `pattern` matched against the whole value,
`description`; every key must also be declared under `parameters:`) is checked
when a run is triggered and refuses a bad value with a 400 naming the parameter
before any `{{ }}` substitution — saving a workflow does not demand `required`
values, and an empty value skips `enum`/`pattern`,
`result_from`,
`runner_class`, `environment`, `notify`, `templates`, `parameters`, and
`tags` (organizational labels, `[A-Za-z0-9_.-]` ≤64 chars each — the workflow
registry surfaces them on `GET /api/workflows` and filters with `?tag=<t>`; the
engine ignores them), and `on_datasets` (dataset triggers: fire a run of this
**registered** workflow when a subscribed dataset records an update, with the
trigger injected as `{{ trigger_dataset }}`; subscribe to one dataset, or to
several composed with `datasets_mode: any|all` — `all` fires only once every
upstream has refreshed since the last fire; [docs/DATASETS.md](DATASETS.md)).

## Data formats & compatibility

- **State schema** = embedded sqlx migrations, applied automatically at
  startup: `crates/dagron-core/migrations/` (SQLite, 001–056) and
  `migrations_pg/` (Postgres, 001–076). Forward-only — there are no down
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

The `otel` Cargo feature is OpenTelemetry integration (#28). A source build
with default features leaves it off, and has no opentelemetry dependency and
unchanged behavior. **The published engine and dagron-api images are built
with it.** In the engine it switches two things on:

1. **Trace-context propagation** — a fresh W3C `traceparent` is injected into
   every dispatched task's environment (`TRACEPARENT`) and each dispatched task
   is wrapped in a `task.dispatch` span, so a task's own OpenTelemetry
   instrumentation joins an external trace (external-trace embedding). Active
   whenever the feature is built in, with or without a collector. A task
   whose workflow gives it a `TRACEPARENT` of its own keeps that one: the
   engine then adds none.
2. **OTLP span export** (follow-on) — when `OTEL_EXPORTER_OTLP_ENDPOINT` is
   **also** set, an OTLP **HTTP/protobuf** span exporter + a
   `tracing-opentelemetry` bridge layer are installed by `dagron-logging`, so the
   engine's own `tracing` spans (the per-task `task.dispatch` span and anything
   nested) are delivered to a collector. The transport is HTTP/protobuf over a
   blocking `reqwest`/rustls client — the gRPC/tonic transport is deliberately
   not pulled in. Unset endpoint ⇒ exporter stays off (spans propagate but aren't
   exported).
3. **OTLP log export** — with the same endpoint, every event the log
   filter lets through (`RUST_LOG` / `LOG_LEVEL`) also goes to the collector
   as an OTLP log record: its level, its message and fields, and the trace and
   span it was logged in, so a record links to its trace. The lines on stdout
   are unchanged. The exporter's own HTTP stack (`hyper`, `reqwest`, `h2`,
   `opentelemetry`, …) is never exported, or sending a record would log a
   record to send.
4. **OTLP metric export** — the engine also sends what `GET /metrics`
   serves, every `OTEL_METRIC_EXPORT_INTERVAL` (60000 ms), as an OTLP metrics
   request: a counter is a monotonic cumulative sum, a gauge a gauge, a
   histogram a histogram with the same bounds, each series' labels its
   attributes. It reads the same exposition a scrape does, through the one
   gauge cache the API's `/metrics` reads (`METRICS_CACHE_SECS`,
   `METRICS_HISTORY_CACHE_SECS`), so the collector and a Prometheus scrape
   see the same numbers and an export adds no read of its own within the
   TTLs, and it sends whether or not `API_ADDR` serves the API.
   A collector slower than the interval loses the older of two expositions,
   never the newer. A read of the exposition that has not ended after the
   interval or 30 s, whichever is longer, is given up with a warning, and
   that export is skipped; the next goes on at its time.

Each signal goes where `OTEL_EXPORTER_OTLP_ENDPOINT` says, with its path
(`/v1/traces`, `/v1/logs`, `/v1/metrics`), or where its own
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `_LOGS_ENDPOINT` / `_METRICS_ENDPOINT`
says, as it is; and not at all when its `OTEL_TRACES_EXPORTER` /
`OTEL_LOGS_EXPORTER` / `OTEL_METRICS_EXPORTER` is `none`. All three carry one
resource: `service.name` = `dagron-<service>` (`dagron-controller`, `dagron-api`), or the binary's own name where it begins `dagron-` (`dagron-ci`), and `OTEL_RESOURCE_ATTRIBUTES`.

Endpoint, headers, and timeouts come from the **standard
`OTEL_EXPORTER_OTLP_*` env vars** (e.g. `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_HEADERS`), so delivery is tuned per-deployment without a
rebuild; the service name is reported as above. The transport is fixed to
HTTP/protobuf — `OTEL_EXPORTER_OTLP_PROTOCOL` is **not** honored (the gRPC/tonic
stack is deliberately not compiled in). Dashboards, sampling policy, and
retention are left to the observability stack you export to.

A collector that cannot be reached does not stop a run: the exporter logs an
error for a batch it could not send, and the work goes on.

**dagron-api** has an `otel` feature of its own and reads the same variables.
It sends its log records as the engine does, and no metrics: it serves no
`/metrics`. It sends the spans it opens, which today are its HTTP requests, opened at
`debug`. So at the default log level it sends nothing: name a collector and
set `RUST_LOG=info,tower_http=debug` to have each request sent as a span (which
also logs each request). A request's span carries its method, its path with
the query string, and the HTTP version, and no header: send it where you
would send that log line. What it holds when it is stopped is sent as it
shuts down. It hands no trace context to anything.

<a id="cloudevents"></a>

## CloudEvents

Every webhook dagron sends is a [CloudEvent](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
(1.0). A `notify.webhook` (the workflow's or the instance's, from the
console's Notifications settings) goes in the HTTP binding's **binary** content
mode: the body is the JSON it always was, and the event's attributes ride in
headers, so a receiver that knows nothing of CloudEvents reads what it read
before and one built on a CloudEvents SDK reads an event.

| Header | Value |
|---|---|
| `ce-specversion` | `1.0` |
| `ce-id` | one per occurrence: the spec's target and the instance's default get the same id |
| `ce-source` | `DAGRON_EVENTS_SOURCE`, `/dagron` when unset |
| `ce-type` | `dev.dagron.notify.<event>`: `succeeded`, `failed`, `cancelled`, `deadline_exceeded`; the settings' test sends `dev.dagron.notify.test` |
| `ce-subject` | the run's id |
| `ce-time` | the payload's `at` |
| `content-type` | `application/json` |

A Slack target gets Slack's own shape and no CloudEvents headers. The
enterprise outbox's webhook (`DAGRON_WEBHOOK_URL`) sends CloudEvents too, its
types `dev.dagron.run.completed` and the outbox's other kinds, its `ce-id` the
outbox row's (a retry is the same event), and can send the structured mode
instead (`DAGRON_WEBHOOK_CONTENT_MODE=structured`: the body
`application/cloudevents+json`, the payload under `data`); its
`x-dagron-signature` is over the body that is sent.

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
