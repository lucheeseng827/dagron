//! dagron engine — the reconcile-loop daemon as a reusable library.
//!
//! [`run`] is the whole scheduler: config from env, executor + worker pool + db
//! pool + ingest actor, the ops surface, and the multi-run reconcile loop. The
//! `dagron` binary is a thin shell over it; alternate builds differ only in the
//! [`Seams`] they pass (built-in vs. extra sources; no-op vs. active
//! run-lifecycle hooks).

pub mod hooks;
pub use hooks::Seams;

// ── ops surface (feature `ops`) — the axum management API + the leadership-gated
// cron / GC / DB-schedule loops. They wire the library crates together rather than
// belonging to any one, so they live in the engine alongside the run loop.
#[cfg(feature = "ops")]
mod api;
#[cfg(feature = "enterprise")]
mod backfill;
// First-class paced backfill jobs (#18) — driven from the schedule loop.
#[cfg(feature = "ops")]
mod backfill_jobs;
#[cfg(feature = "ops")]
mod cron;
#[cfg(feature = "ops")]
mod gc;
#[cfg(feature = "ops")]
// Environment integration: `{{ env.* }}` template params at run creation +
// DB-backed secret resolution at dispatch.
mod environments;
mod leadership;
// Outbound run notifications (`notify.webhook` / `notify.slack`), fired on run
// finalization and soft-deadline breach. Best-effort, like forge feedback.
mod notify;
#[cfg(feature = "ops")]
mod schedule;
// Timezone-aware cron fire-time helper shared by cron/schedule/backfill loops.
#[cfg(feature = "ops")]
mod schedule_time;
// Unclaimable-class alarm: warns when a runner class's ready backlog ages
// because no live scheduler serves it (runner segmentation).
#[cfg(feature = "ops")]
mod stale_ready;
// Cloud archive URL → object_store dispatch (s3/gs/az), shared by the GC sink
// and the Parquet compactor.
#[cfg(feature = "archive-cloud")]
mod objstore;
// `dagron archive-compact` — fold archived run documents into Parquet
// (the analytics tier of the hot/cold split).
#[cfg(feature = "archive-parquet")]
mod archive_compact;
// Offline spec validation (`dagron validate`) — pure dagron-core, no ops needed.
mod validate;

// Network policy for `wait.url` sensors: the opt-in `WAIT_URL_DENY_PRIVATE`
// guard that keeps a scheduler-issued poll from reaching addresses the task
// pod could not (see the module docs for the threat model).
mod wait_url;

// The engine logic now lives in three library crates. Re-alias them to the module
// paths the wiring below (and the ops modules) already use — `db::`, `dag::`,
// `executor::`, `source::`, … — so the split is pure plumbing: no call site moved.
// (A private `use` in the crate root is visible to every descendant module, so
// `crate::db` inside api.rs/cron.rs/… keeps resolving here.)
use dagron_core::{dag, db, metrics};
// Used only by the ops modules (api.rs) via `crate::models`.
#[cfg(feature = "ops")]
use dagron_core::models;
#[cfg(feature = "kubernetes")]
use dagron_executor::kube_executor;
use dagron_executor::{docker_executor, executor, worker};
use dagron_source::{ingest, source};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::info;

use ractor::Actor;

use executor::{ExecContext, LocalExecutor};
use ingest::{IngestActor, IngestArgs};
use metrics::Metrics;
use worker::{DispatchPayload, WorkerPool};

/// Build a fresh W3C `traceparent` header value for a task, plus its trace id
/// (OpenTelemetry integration, #28). Format: `00-<16-byte trace id>-<8-byte span
/// id>-01` — version `00`, `01` = sampled. Injected into a dispatched task's env
/// as `TRACEPARENT` so the task's own OpenTelemetry instrumentation joins this
/// trace (external-trace embedding). The ids come from v4 UUIDs, so the trace id
/// is never the all-zero value W3C forbids.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
fn new_traceparent() -> (String, String) {
    let trace_id = uuid::Uuid::new_v4().simple().to_string(); // 32 hex = 16 bytes
    let span_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_string(); // 16 hex = 8 bytes
    (format!("00-{trace_id}-{span_id}-01"), trace_id)
}

/// How far each worker heartbeat pushes a running task's lease. Matches the
/// claim-time lease window (30 s): with a 10 s renew interval, a healthy task's
/// lease is always 20–30 s in the future, and a dead worker's task is reclaimed
/// on the same timeline as before heartbeats existed.
const TASK_LEASE_EXTEND_SECS: i64 = 30;

/// The worker heartbeat over the datastore — renews the claim lease while a
/// task executes, guarded by the claim triple so a reclaimed task can never be
/// resurrected. This is what lets one task run for hours (training, stream
/// consumers) on the same short-lease crash-recovery machinery.
struct DbLeaseKeeper {
    pool: db::Pool,
}

#[async_trait::async_trait]
impl worker::LeaseKeeper for DbLeaseKeeper {
    async fn renew(&self, task_id: &str, worker_id: &str, fence: i64) -> Result<bool> {
        db::renew_task_lease(&self.pool, task_id, worker_id, fence, TASK_LEASE_EXTEND_SECS).await
    }
}

/// Max parked `wait.url` sensors probed in a single reconcile tick. Bounds how
/// much outbound HTTP one tick can take on; the rest roll to the next tick.
const WAIT_URL_BATCH: i64 = 32;

/// Record a task's `produces:` dataset updates after a **fenced** success.
///
/// Shared by the two paths that can succeed a producer task: the ordinary worker
/// result and a memoization cache hit. A cache hit still records — `produces:` is
/// a postcondition ("after this task succeeds, the dataset is current"), and
/// staying silent would leave a downstream sensor or `on_datasets:` consumer
/// parked forever whenever the producer happens to hit its cache. Best-effort:
/// a ledger failure is logged, never fatal to the run.
async fn record_produces(
    pool: &db::Pool,
    metrics: &Metrics,
    workflow: &str,
    task_id: &str,
    task_name: &str,
    produces: &[String],
) {
    if produces.is_empty() {
        return;
    }
    match db::record_dataset_updates(pool, workflow, task_id, task_name, produces).await {
        Ok(()) => {
            metrics.inc_dataset_updates();
            info!(%task_id, datasets = produces.len(), "dataset update(s) recorded");
        }
        Err(e) => tracing::warn!(
            error = %e, %task_id,
            "dataset update record failed (best-effort)"
        ),
    }
}

/// Redact secrets from a connection target before logging: mask sensitive
/// query-string params (`password`, `token`, …) and strip URL userinfo
/// (`user:pass@`) so nothing from `DATABASE_URL` reaches the logs. SQLite file
/// paths have neither and pass through unchanged.
fn redact_conn(target: &str) -> String {
    const SENSITIVE: [&str; 6] =
        ["password", "passwd", "pwd", "secret", "token", "access_token"];
    // Mask sensitive query params first (e.g. `…?sslmode=require&password=hunter2`).
    let scrubbed = match target.split_once('?') {
        Some((base, query)) => {
            let masked = query
                .split('&')
                .map(|kv| match kv.split_once('=') {
                    Some((k, _)) if SENSITIVE.contains(&k.to_ascii_lowercase().as_str()) => {
                        format!("{k}=<redacted>")
                    }
                    _ => kv.to_string(),
                })
                .collect::<Vec<_>>()
                .join("&");
            format!("{base}?{masked}")
        }
        None => target.to_string(),
    };
    // Strip URL userinfo.
    if let Some(scheme_end) = scrubbed.find("://") {
        let (scheme, rest) = scrubbed.split_at(scheme_end + 3);
        if let Some(at) = rest.find('@') {
            return format!("{scheme}<redacted>@{}", &rest[at + 1..]);
        }
    }
    scrubbed
}

/// Resolve the `MAX_INFLIGHT_RUNS` admission cap, defaulting to 64.
///
/// **`0` disables the cap** — the contract the Helm chart and its `values.yaml`
/// have always documented. Both admission sites read it that way: `api.rs` gates
/// its `POST /runs` check on `max_inflight_runs > 0`, and the ingest actor's
/// throttle is gated the same way. Anything below zero is a nonsense cap rather
/// than a distinct mode, so it normalizes to `0` (disabled) instead of silently
/// meaning something else.
///
/// This used to clamp with `.max(1)`, which made the documented `0` unreachable
/// and turned "disable the cap" into "cap at one active run" — a near-total
/// stall for a deployment that set it.
fn parse_max_inflight_runs(raw: Option<String>) -> i64 {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(64)
        .max(0)
}

/// Seed the GitOps workflow directory with the image's bundled examples on first
/// start (when the dir is empty). Ported from the old `docker-entrypoint.sh` so
/// the container can run on a shell-less, lightweight base (distroless). Best
/// effort: a no-op when the bundled examples aren't present (local dev) and
/// non-fatal on any I/O error — never block the daemon from starting.
///
/// `WORKFLOW_DIR` (env, default `/workflows`) overrides the target directory —
/// point it at a mounted volume to manage workflows via GitOps. Both `.yaml` and
/// `.yml` example files are seeded.
fn seed_workflow_dir() {
    let target = std::env::var("WORKFLOW_DIR").unwrap_or_else(|_| "/workflows".to_string());
    let examples = std::path::Path::new("/etc/dagron/examples");
    // Only relevant inside the image (examples baked in); skip otherwise.
    if !examples.is_dir() {
        return;
    }
    let target = std::path::Path::new(&target);
    if let Err(e) = std::fs::create_dir_all(target) {
        tracing::warn!(dir = %target.display(), error = %e, "could not create workflow dir");
        return;
    }
    // Seed only when empty (GitOps init); never clobber a managed volume.
    let empty = std::fs::read_dir(target)
        .map(|mut d| d.next().is_none())
        .unwrap_or(false);
    if !empty {
        return;
    }
    let mut count = 0u32;
    if let Ok(entries) = std::fs::read_dir(examples) {
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|s| s.to_str());
            if matches!(ext, Some("yaml") | Some("yml")) {
                if let Some(name) = path.file_name() {
                    match std::fs::copy(&path, target.join(name)) {
                        Ok(_) => count += 1,
                        Err(e) => tracing::warn!(file = %path.display(), error = %e, "seed copy failed"),
                    }
                }
            }
        }
    }
    if count > 0 {
        info!(dir = %target.display(), count, "initialized workflow dir with bundled examples");
    }
}

/// Post a terminal commit status for a finalized run, if its spec declares a
/// `notify.git` target. Best-effort: any failure (spec missing, no notify block,
/// forge error) is logged and swallowed so run execution is never affected. The
/// target's `{{ param }}` fields are resolved against the spec's `parameters`
/// (e.g. `sha: "{{ commit_sha }}"` from the CI caller's submitted parameters).
async fn post_forge_status(
    forge: &dagron_forge::ForgeClient,
    pool: &db::Pool,
    run_id: &str,
    status: &str,
) {
    let Some(yaml) = db::spec_for_run(pool, run_id).await.ok().flatten() else {
        return;
    };
    let spec: dag::DagSpec = match serde_yaml::from_str(&yaml) {
        Ok(s) => s,
        Err(_) => return, // a spec this build can't parse; nothing to notify
    };
    let Some(git) = spec.notify.and_then(|n| n.git) else {
        return; // no notify.git block — the common case
    };
    // Resolve templated fields against the workflow parameters.
    let sub = |s: &str| dagron_core::expand::substitute(s, &spec.parameters);
    let target = dagron_forge::GitTarget {
        provider: git.provider,
        repo: sub(&git.repo),
        sha: sub(&git.sha),
        context: git.context.as_deref().map(sub).unwrap_or_else(|| "dagron".to_string()),
        target_url: git.target_url.as_deref().map(sub),
    };
    if target.sha.is_empty() || target.sha.contains("{{") {
        tracing::warn!(%run_id, "notify.git sha did not resolve (missing parameter?) — skipping forge status");
        return;
    }
    let state = dagron_forge::CommitState::from_run_status(status);
    if let Err(e) = forge.post_status(&target, state).await {
        tracing::warn!(error = %e, %run_id, "forge commit status post failed");
    }
}

/// Run the dagron scheduler daemon to completion (or until killed). `seams`
/// selects the extension behaviour; pass `Seams::default()` for the built-in
/// configuration.
pub async fn run(seams: Seams) -> Result<()> {
    // Tunable, structured logging for the workflow controller (and the worker
    // pool it spawns). Verbosity/format are env-driven — see the shared
    // `dagron_logging` crate for the full knob list (RUST_LOG / LOG_LEVEL /
    // LOG_FORMAT / …).
    dagron_logging::init("controller");

    let args: Vec<String> = std::env::args().collect();

    // `dagron validate <file|dir>... [--json]` — offline spec lint through the
    // same parse → expand → graph-validate pipeline every submit path uses.
    // Handled before any daemon setup — and before `seed_workflow_dir` — so the
    // subcommand stays side-effect free (no datastore, no executor, no server,
    // no file seeding).
    // Both this and `archive-compact` below flush the exporter before returning:
    // logging (and, in an `otel` build, the batch span exporter) is already up by
    // the time we get here, so a bare `return` would drop everything buffered
    // since the last export interval — which for a subcommand that lives a
    // second or two is the entire run.
    if args.get(1).map(String::as_str) == Some("validate") {
        let result = validate::run_cli(&args[2..]);
        dagron_logging::shutdown();
        return result;
    }

    // `dagron archive-compact [db_target]` — one bounded sweep folding archived
    // run documents into the Parquet dataset (k8s CronJob shape). Like
    // `validate`, handled before daemon setup; unlike it, it needs the sink env
    // (GC_ARCHIVE_DIR / GC_ARCHIVE_URL) and optionally a datastore to stamp
    // `archived_runs`. Feature-gated: without `archive-parquet` the subcommand
    // is a clear startup error, never a silent no-op.
    if args.get(1).map(String::as_str) == Some("archive-compact") {
        #[cfg(feature = "archive-parquet")]
        {
            let result = archive_compact::run_cli(&args[2..]).await;
            dagron_logging::shutdown();
            return result;
        }
        #[cfg(not(feature = "archive-parquet"))]
        {
            dagron_logging::shutdown();
            anyhow::bail!(
                "`dagron archive-compact` requires building with `--features archive-parquet`"
            );
        }
    }

    // GitOps init: seed /workflows from the image's bundled examples if empty.
    // (Was docker-entrypoint.sh; in-binary now so the image needs no shell.)
    seed_workflow_dir();

    // `dagron dev` (QW2) — curated zero-infra local quickstart: SQLite + the
    // management API/Swagger on a fixed local port, resident so the API (and any
    // schedules/cron) stay up. Positional args shift by one (`dev [dag] [db]`);
    // env still wins, so power users override any default. Edition 2021 → set_var
    // is safe; do it before the rest of main reads the environment.
    let dev_mode = args.get(1).map(|s| s == "dev").unwrap_or(false);
    // `dagron dev` is the API/UI quickstart; without the `ops` feature there is no
    // management server to keep the process resident, so refuse rather than start a
    // daemon that advertises an API and then exits.
    #[cfg(not(feature = "ops"))]
    if dev_mode {
        anyhow::bail!("`dagron dev` requires building with the `ops` feature (the management API)");
    }
    if dev_mode && std::env::var_os("API_ADDR").is_none() {
        std::env::set_var("API_ADDR", "127.0.0.1:8787");
    }

    // Positional args, skipping the `dev` subcommand token when present.
    let pos_offset = if dev_mode { 2 } else { 1 };
    let dag_path = args
        .get(pos_offset)
        .map(String::as_str)
        .unwrap_or("examples/simple_dag.yaml");

    // Datastore target. SQLite takes a file path (defaulting to a local file);
    // Postgres takes a connection string (else $DATABASE_URL).
    let db_target: String = {
        #[cfg(feature = "postgres")]
        {
            args.get(pos_offset + 1)
                .cloned()
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .unwrap_or_else(|| "postgres://localhost/workflow".to_string())
        }
        #[cfg(feature = "sqlite")]
        {
            args.get(pos_offset + 1).cloned().unwrap_or_else(|| "workflow.db".to_string())
        }
    };

    if dev_mode {
        let api_addr = std::env::var("API_ADDR").unwrap_or_default();
        info!(
            "dagron dev — local quickstart: datastore {}, management API + Swagger UI on \
             http://{api_addr}/docs (override with API_ADDR)",
            redact_conn(&db_target)
        );
    }
    let worker_id = format!("worker-{}", uuid::Uuid::new_v4());

    // Executor backend: EXECUTOR=local|docker (default: local)
    let executor_kind = std::env::var("EXECUTOR").unwrap_or_else(|_| "local".to_string());
    let worker_count: usize = std::env::var("WORKER_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
        .max(1); // guard against WORKER_COUNT=0 stalling the scheduler

    // Runner segmentation: RUNNER_CLASSES=etl,pulse restricts this scheduler to
    // claiming tasks in those classes, so a pool of replicas becomes a dedicated
    // runner for one workload shape. Unset/empty =
    // claim every class — the unsegmented default. Names are validated with the
    // same rule as the spec side so a typo fails at startup, not as an
    // unclaimable-forever task class.
    let runner_classes: Vec<String> = std::env::var("RUNNER_CLASSES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    for class in &runner_classes {
        dag::validate_runner_class(class)
            .map_err(|e| anyhow::anyhow!("invalid RUNNER_CLASSES entry: {e}"))?;
    }

    // Named concurrency pools (#21): POOLS=etl:4,db:2 caps how many tasks in each
    // pool may run at once. A task's `pool:` draws a slot; when a pool is full its
    // tasks wait in `ready` until one frees (no run is dropped). Unset/empty = no
    // pools, and the claim path is unchanged. Names are validated like the spec
    // side; a non-positive or unparseable slot count is a startup error, not a
    // silently-ignored limit.
    let pool_caps: std::collections::BTreeMap<String, i64> = {
        let raw = std::env::var("POOLS").unwrap_or_default();
        let mut m = std::collections::BTreeMap::new();
        for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (name, slots) = entry.split_once(':').ok_or_else(|| {
                anyhow::anyhow!("invalid POOLS entry '{entry}': expected name:slots")
            })?;
            let name = name.trim();
            dag::validate_pool(name).map_err(|e| anyhow::anyhow!("invalid POOLS entry: {e}"))?;
            let slots: i64 = slots
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid POOLS slots for '{name}': '{slots}'"))?;
            if slots < 1 {
                anyhow::bail!("POOLS slots for '{name}' must be >= 1, got {slots}");
            }
            // Every other malformed POOLS input is a hard startup error; a
            // silently-overwritten duplicate would be the odd one out.
            if let Some(prev) = m.insert(name.to_string(), slots) {
                anyhow::bail!(
                    "duplicate POOLS entry for '{name}' (slots {prev} then {slots}) — declare each pool once"
                );
            }
        }
        m
    };

    // Gang co-scheduling (enterprise scheduler + RUNNER_GANGS=1): claim gangs
    // all-or-nothing before filling leftover capacity with ordinary tasks, and
    // cancel a failed member's siblings (die-together). Compiled out of the
    // default build via the `enterprise` feature; inert unless opted in.
    let gangs_on = cfg!(feature = "enterprise")
        && std::env::var("RUNNER_GANGS")
            .map(|v| matches!(v.trim(), "1" | "true" | "yes"))
            .unwrap_or(false);
    if gangs_on {
        info!("gang co-scheduling enabled (RUNNER_GANGS) — all-or-nothing gang claims");
    }

    // Ingestion source: SOURCE=file|stream (default: file; `stream` follows an
    // NDJSON file or named pipe with a durable offset checkpoint). Managed
    // broker connectors (kafka/nats/sqs/redis) and the events gateway register
    // through Seams.source_factory. MAX_INFLIGHT_RUNS caps how many runs
    // may be active at once — the admission valve that lets the scheduler absorb
    // a large influx by leaving the overflow buffered in the queue.
    let source_kind = std::env::var("SOURCE").unwrap_or_else(|_| "file".to_string());
    let max_inflight_runs: i64 = parse_max_inflight_runs(std::env::var("MAX_INFLIGHT_RUNS").ok());
    // How many times a transient create_run failure is retried (nacked) before a
    // submission is dead-lettered. Parse failures dead-letter immediately.
    let dead_letter_max_attempts: i64 = std::env::var("DEAD_LETTER_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1);

    info!(%worker_id, %dag_path, db = %redact_conn(&db_target), %executor_kind, worker_count, %source_kind, max_inflight_runs, runner_classes = ?runner_classes, pools = ?pool_caps, "scheduler starting");

    // rustls 0.23 needs a process-level CryptoProvider before the kube client
    // opens TLS to the apiserver (KubeExecutor); install it once at startup.
    #[cfg(feature = "kubernetes")]
    {
        dagron_executor::install_crypto_provider();
    }

    let executor: Arc<dyn executor::Executor> = match executor_kind.as_str() {
        "docker" => {
            let image = std::env::var("DOCKER_IMAGE").unwrap_or_else(|_| "alpine:latest".to_string());
            info!(%image, "using DockerExecutor");
            Arc::new(docker_executor::DockerExecutor::connect(image).await?)
        }
        "kubernetes" | "k8s" => {
            // Cluster-gated: only compiled with `--features kubernetes`. Without
            // it, fail clearly rather than silently downgrading to local.
            #[cfg(feature = "kubernetes")]
            {
                let image = std::env::var("K8S_IMAGE")
                    .or_else(|_| std::env::var("DOCKER_IMAGE"))
                    .unwrap_or_else(|_| "alpine:latest".to_string());
                let namespace =
                    std::env::var("K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
                info!(%image, %namespace, "using KubeExecutor");
                Arc::new(kube_executor::KubeExecutor::connect(image, namespace).await?)
            }
            #[cfg(not(feature = "kubernetes"))]
            {
                anyhow::bail!(
                    "EXECUTOR=kubernetes requires building with `--features kubernetes`"
                )
            }
        }
        "local" => {
            info!("using LocalExecutor");
            Arc::new(LocalExecutor)
        }
        _ => {
            tracing::warn!(%executor_kind, "unrecognized EXECUTOR value, defaulting to local");
            Arc::new(LocalExecutor)
        }
    };

    // Process metrics (counters + latency histograms); the management API renders
    // these alongside live datastore gauges at GET /metrics. Created before the
    // worker pool so workers can record per-task durations into it.
    let metrics = Arc::new(Metrics::new());

    let pool = db::init_pool(&db_target).await?;

    // Lease heartbeat: workers renew a running task's lease every few seconds,
    // so a task may run for hours under the short claim-time lease — the
    // long-task keystone (training jobs, stream consumers). A task whose worker
    // dies stops heartbeating and is reclaimed by the expired-lease sweep
    // exactly as before. TASK_LEASE_HEARTBEAT=false restores the old
    // finish-inside-one-lease behaviour.
    let heartbeat_on = std::env::var("TASK_LEASE_HEARTBEAT")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let lease_keeper: Option<Arc<dyn worker::LeaseKeeper>> = if heartbeat_on {
        Some(Arc::new(DbLeaseKeeper { pool: pool.clone() }))
    } else {
        info!("task lease heartbeat disabled (TASK_LEASE_HEARTBEAT) — tasks must finish inside one lease window");
        None
    };

    let workers =
        WorkerPool::with_lease_keeper(worker_count, executor, Arc::clone(&metrics), lease_keeper)
            .await?;
    info!(size = workers.size(), "worker pool ready");

    // When an ops time-source / API is active the process must stay up after the
    // workflow source drains (future cron/schedule fires, live API). One-shot
    // file runs with no ops time-source still exit cleanly. Always-declared so the
    // drain check below compiles regardless of the `ops` feature.
    #[allow(unused_mut)]
    let mut stay_resident = false;

    // ── v5/v6 ops: management API + leadership-gated cron and retention GC ──
    // Compiled only with the (default) `ops` feature; a lean build drops this
    // block along with axum/cron. Each piece is also opt-in via env, so even the
    // full build stays a plain scheduler until configured. Cron and GC must run
    // on exactly one node, so they share a leadership lease (acquired only when
    // at least one of them is enabled).
    #[cfg(feature = "ops")]
    {
        let cron_config = std::env::var("CRON_CONFIG").ok();
        let gc_retention_secs: Option<i64> = std::env::var("GC_RETENTION_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0);
        // DB-backed UI schedules are opt-in (server mode): enabling them keeps the
        // daemon resident so future fires happen, which would otherwise stop a
        // one-shot `dagron file.yaml` from exiting after its run drains.
        let db_schedules_on = std::env::var("DB_SCHEDULES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        // Automatic backfill & self-healing (behind the `enterprise` feature):
        // opt-in via AUTO_BACKFILL=1. Like DB schedules it is a resident
        // time-source, so a
        // one-shot run does not exit while the loop is armed to heal future gaps.
        #[cfg(feature = "enterprise")]
        let auto_backfill_cfg = backfill::Config::from_env();

        // API_ADDR only makes this a server if it actually parses and the API
        // spawns. If it's set but invalid the API is disabled, so it must not
        // count toward stay_resident — otherwise a one-shot run would hang with
        // no API to drain.
        let mut api_on = false;
        if let Ok(addr_raw) = std::env::var("API_ADDR") {
            match addr_raw.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    api_on = true;
                    let state = api::ApiState {
                        pool: pool.clone(),
                        metrics: Arc::clone(&metrics),
                        max_inflight_runs,
                    };
                    tokio::spawn(async move {
                        if let Err(e) = api::serve(addr, state).await {
                            tracing::error!(error = %e, "management API stopped");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!(addr = %addr_raw, error = %e, "invalid API_ADDR — management API disabled")
                }
            }
        }
        // Any of these makes the process a long-running server, not a one-shot run.
        stay_resident = api_on
            || cron_config.is_some()
            || gc_retention_secs.is_some()
            || db_schedules_on;
        #[cfg(feature = "enterprise")]
        { stay_resident = stay_resident || auto_backfill_cfg.is_some(); }

        let needs_leadership = cron_config.is_some()
            || gc_retention_secs.is_some()
            || db_schedules_on
            // #18: pace backfill jobs in any resident daemon (they can be created
            // via the API whenever the server is up, independent of DB_SCHEDULES).
            || stay_resident;
        #[cfg(feature = "enterprise")]
        let needs_leadership = needs_leadership || auto_backfill_cfg.is_some();
        if needs_leadership {
            let lease_secs: i64 = std::env::var("LEADER_LEASE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&n| n > 0)
                .unwrap_or(30);
            let is_leader =
                leadership::spawn(pool.clone(), "ops".to_string(), worker_id.clone(), lease_secs);

            // DB-backed UI schedules — leadership-gated firing of first-class workflows.
            if db_schedules_on {
                let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                tokio::spawn(async move { schedule::run(p, l, m).await });
            }

            // First-class paced backfill jobs (#18) — leadership-gated, always on in
            // a resident daemon so an API-created job is paced to completion.
            {
                let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                tokio::spawn(async move { backfill_jobs::run(p, l, m).await });
            }

            // Automatic backfill & self-healing (behind the `enterprise` feature) —
            // leadership-gated catch-up of missed fires + auto-rerun of failed runs,
            // republishing schedule lag / incomplete-run state as metrics.
            #[cfg(feature = "enterprise")]
            if let Some(cfg) = auto_backfill_cfg {
                let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                tokio::spawn(async move { backfill::run(p, cfg, l, m).await });
            }

            if let Some(path) = cron_config {
                match cron::load(&path).await {
                    Ok(entries) => {
                        let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                        tokio::spawn(async move { cron::run(p, entries, l, m).await });
                    }
                    Err(e) => tracing::error!(%path, error = %e, "cron config invalid — cron disabled"),
                }
            }

            if let Some(retention) = gc_retention_secs {
                let interval: u64 = std::env::var("GC_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3600);
                // Archive-before-purge (the hot/cold split):
                // GC_ARCHIVE_URL (s3://…, feature archive-s3) or GC_ARCHIVE_DIR
                // — expired runs are exported to the sink and only verified
                // exports are purged. A misconfigured sink is a startup error:
                // never fall back to purging unarchived history.
                let archive = gc::ArchiveSink::from_env()?;
                let (p, l) = (pool.clone(), Arc::clone(&is_leader));
                tokio::spawn(async move { gc::run(p, retention, interval, l, archive).await });
            }

            // Stale-ready (unclaimable-class) alert — on by default in any
            // resident daemon; READY_AGE_ALERT_SECS=0 disables.
            let ready_alert_secs: i64 = std::env::var("READY_AGE_ALERT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300);
            if ready_alert_secs > 0 {
                let check_interval: u64 = std::env::var("READY_AGE_CHECK_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60);
                let (p, l) = (pool.clone(), Arc::clone(&is_leader));
                tokio::spawn(async move {
                    stale_ready::run(p, ready_alert_secs, check_interval, l).await
                });
            }
        }
    }

    // Reconcile-loop waker. On Postgres this is a LISTEN/NOTIFY listener that
    // wakes the loop the instant any worker changes task readiness; on SQLite it
    // degrades to the fixed-interval timer.
    let mut waker = db::Waker::connect(&pool).await?;

    // Build the configured ingestion source and start the ractor ingest actor.
    // It pulls workflow submissions and calls db::create_run for each, applying
    // MAX_INFLIGHT_RUNS admission backpressure. For the default `file` source it
    // emits one DAG then drains; for queue sources it streams indefinitely.
    // In `dagron dev` with no DAG file present, start with no initial run — just
    // serve the API/UI — instead of letting the file source error-loop on a
    // missing path. Submit work via the API once the server is up.
    let wf_source: Box<dyn source::WorkflowSource> = if dev_mode
        && source_kind == "file"
        && !std::path::Path::new(dag_path).exists()
    {
        info!(
            dag = %dag_path,
            "dagron dev — no DAG file found; starting with no initial run (submit via POST /runs)"
        );
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        drop(tx); // immediately drained → no initial run; the API keeps the daemon resident
        Box::new(source::ChannelSource::new(rx))
    } else {
        source::build_pooled(&source_kind, dag_path, &pool, seams.source_factory.as_deref())
            .await?
    };
    let exhausted = Arc::new(AtomicBool::new(false));
    let (_ingest_ref, _ingest_handle) = IngestActor::spawn(
        Some("ingest".to_string()),
        IngestActor,
        IngestArgs {
            pool: pool.clone(),
            source: wf_source,
            max_inflight_runs,
            exhausted: Arc::clone(&exhausted),
            metrics: Arc::clone(&metrics),
            source_name: source_kind.clone(),
            max_validation_attempts: dead_letter_max_attempts,
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("spawn ingest actor: {e}"))?;

    let (tx, mut rx) = mpsc::unbounded_channel::<worker::TaskResult>();

    // Live-log stream (#17): workers push incremental output chunks here as tasks
    // run; the loop drains them each tick and appends to the task's stored output
    // for tailing. Separate from the result channel so partial output is visible
    // before the task's terminal result arrives.
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<dagron_executor::executor::LogChunk>();

    let poll_interval = std::time::Duration::from_millis(500);

    // Simple counter: how many tasks are currently in-flight inside the worker pool.
    let mut in_flight: usize = 0;

    // Optional OpenLineage emitter (data lineage parity). Off unless
    // OPENLINEAGE_URL is set; emits a terminal RunEvent per finalized run.
    let lineage = dagron_lineage::OpenLineageClient::from_env();
    if lineage.is_some() {
        info!("OpenLineage emit enabled");
    }

    // Optional forge feedback (GitHub/GitLab commit statuses). Active when a
    // GITHUB_TOKEN / GITLAB_TOKEN is set; a run whose spec carries a `notify.git`
    // block posts its terminal status on finalization. Best-effort, like lineage.
    let forge = dagron_forge::ForgeClient::from_env();
    if forge.is_some() {
        info!("forge feedback enabled (notify.git commit statuses)");
    }

    // Optional artifact store for passing files between tasks. When
    // DAGRON_ARTIFACT_DIR is set, each dispatched task gets a per-run shared dir
    // via `DAGRON_ARTIFACTS` so tasks in a run can pass files. Off otherwise.
    let artifact_store = dagron_artifact::LocalFsStore::from_env();
    if artifact_store.is_some() {
        info!("artifact store enabled (DAGRON_ARTIFACTS injected per task)");
    }

    // HTTP wait-sensor polling (#27 follow-on): parked `wait.url` tasks are GETed
    // every WAIT_POLL_SECS (default 15 s) and succeed on the first 2xx. The client
    // is built once; a short per-request timeout keeps a hung endpoint from
    // stalling the reconcile tick.
    // How deep `type: workflow` may nest (#23). A workflow that names itself —
    // or a cycle of workflows — otherwise spawns child runs without end, each
    // leaving a parked parent behind; the cap turns that into one clear task
    // failure. 8 is well past any legitimate composition depth.
    let subworkflow_max_depth: i64 = std::env::var("SUBWORKFLOW_MAX_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&d| d > 0)
        .unwrap_or(8);

    let wait_poll_secs: u64 = std::env::var("WAIT_POLL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&s| s > 0)
        .unwrap_or(15);
    // Redirects are disabled: the sensor only needs the origin's own status, and
    // following redirects would let an innocuous-looking external `wait.url`
    // pivot the *scheduler's* network position to loopback / link-local / cloud
    // metadata (a scheme-prefix check cannot see a redirect target). A 3xx simply
    // reads as "not ready" and re-parks.
    let mut wait_http_builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10));
    // Opt-in for operators who run untrusted specs: refuse to poll anything that
    // is not globally routable, enforced in the resolver the client dials with
    // so a DNS rebind has no window to exploit. Off by default — the common
    // `wait.url` *is* an in-cluster address. See `wait_url` for the threat model.
    let wait_url_deny_private = wait_url::deny_private_enabled();
    if wait_url_deny_private {
        wait_http_builder = wait_http_builder
            .dns_resolver(std::sync::Arc::new(wait_url::PublicOnlyResolver))
            // reqwest honours HTTP_PROXY / HTTPS_PROXY / ALL_PROXY by default.
            // With a proxy in play the resolver only ever sees the *proxy's*
            // host — the blocked target travels in the request line and the
            // proxy dials it — so the whole policy would rest on the proxy's
            // own egress rules. Under this policy the scheduler goes direct.
            .no_proxy();
        info!("wait.url sensors restricted to globally-routable addresses, proxies bypassed (WAIT_URL_DENY_PRIVATE)");
    }
    let wait_http = wait_http_builder
        .build()
        // A failed build would silently hand back a *default* client — redirects
        // enabled, no timeout — discarding the very policy above. Fail startup.
        .map_err(|e| anyhow::anyhow!("could not build the wait-sensor HTTP client: {e}"))?;

    // Dataset-triggered scheduling (Airflow Datasets / Dagster asset sensors):
    // registered workflows with `on_datasets:` fire when their subscribed
    // datasets record new updates. On by default; DATASET_TRIGGERS=0 opts a
    // scheduler out of sweeping (sensors and `produces:` recording stay on —
    // they are run-local, not trigger machinery). The sweep itself is throttled
    // below; `dataset_trigger_sweep` is its last-run instant.
    let dataset_triggers_on = std::env::var("DATASET_TRIGGERS")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"))
        .unwrap_or(true);
    let mut dataset_trigger_sweep: Option<std::time::Instant> = None;
    // Workflows this scheduler has synced subscription rows for, so a spec that
    // later drops `on_datasets:` is cleared once instead of every sweep.
    let mut subscribed_workflows: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    // Workflows already signposted for Enterprise-only dataset composition —
    // warn once each, not twelve times a minute forever.
    let mut warned_dataset_composition: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    // Cloud artifact location (DAGRON_ARTIFACT_URL — s3://, gs://, az://): the
    // engine does no cloud I/O at dispatch; it injects per-run/per-task *URLs*
    // (`DAGRON_ARTIFACTS_URL`, `DAGRON_CHECKPOINT_URL`) and tasks reach the
    // bucket with their own tooling. Resume pointers for cloud checkpoints flow
    // through the checkpoint report route (any URI), so this composes with
    // checkpoint-aware resume across machines and clouds — the substrate the
    // artifact API also serves via `dagron_artifact::store_from_env`.
    let artifact_url = std::env::var("DAGRON_ARTIFACT_URL")
        .ok()
        .map(|u| u.trim().trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty());
    if let Some(url) = &artifact_url {
        info!(%url, "cloud artifact location enabled (DAGRON_ARTIFACTS_URL / DAGRON_CHECKPOINT_URL injected per task)");
    }

    info!("reconcile loop running (multi-run, queue-driven daemon)");

    // Oversized-gang alarm state (gangs_on only): slow-cadence check + a
    // warned set so each impossible gang logs exactly once per process.
    let mut oversized_gang_check: Option<std::time::Instant> = None;
    let mut warned_oversized_gangs: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    loop {
        // Tick timer — the recover→advance→dispatch→collect→reap span. A tick
        // pegging the CPU (a load-test finding) shows up as this histogram's
        // upper buckets filling. Excludes the wait below (idle, not work).
        let tick_start = std::time::Instant::now();

        // ── Step 1: crash recovery ──────────────────────────────────────────
        let recovered = db::recover_expired_leases(&pool).await?;
        if recovered > 0 {
            info!(recovered, "reclaimed expired leases");
        }

        // ── Step 1b: run-level deadlines ────────────────────────────────────
        // Fail any run past its `run_timeout_secs` budget and cancel its
        // remaining tasks. Idempotent, so every scheduler may sweep; an executor
        // finishing after the sweep is rejected by the fence guard.
        for run_id in db::cancel_overdue_runs(&pool).await? {
            tracing::warn!(%run_id, "run deadline exceeded (run_timeout_secs) — run failed, tasks cancelled");
            metrics.inc_runs_deadline_exceeded();
        }

        // ── Step 1c: soft SLA deadline alerts ───────────────────────────────
        // Emit a `run.deadline_exceeded` outbox event (once) for a run past its
        // `deadline` — the run keeps running. Fire-once + winner-take-all in SQL.
        for run_id in db::fire_deadline_alerts(&pool).await? {
            tracing::warn!(%run_id, "run exceeded its soft deadline — SLA alert emitted");
            metrics.inc_deadline_alerts();
            // Push the SLA breach to any notify.webhook / notify.slack targets
            // (fire-once is guaranteed by fire_deadline_alerts above). Spawned
            // so a slow target can't stall the reconcile tick.
            {
                let pool = pool.clone();
                tokio::spawn(async move {
                    notify::notify_run_event(&pool, &run_id, "deadline_exceeded").await;
                });
            }
        }

        // ── Step 1d: expire human approval gates (#19) ──────────────────────
        // Auto-resolve any `awaiting_approval` task past its `approval_timeout_secs`
        // per its `approval_on_timeout` default. Idempotent (guarded resolve), so
        // every scheduler may sweep.
        for (task_id, approved) in db::resolve_expired_approvals(&pool).await? {
            tracing::info!(%task_id, approved, "approval gate timed out — auto-resolved");
        }

        // Resolve any `type: workflow` trigger whose child run has finished (#23):
        // the parent succeeds/fails with the child and its dependents advance.
        // Idempotent (guarded resolve), so every scheduler may sweep.
        for (task_id, succeeded) in db::reconcile_subworkflows(&pool).await? {
            tracing::info!(%task_id, succeeded, "sub-workflow finished — resolved trigger task");
        }

        // Resolve any deferred `type: wait` sensor whose deadline has passed (#27):
        // the task succeeds and its dependents advance. Idempotent, HA-safe.
        for task_id in db::reconcile_waits(&pool).await? {
            tracing::info!(%task_id, "wait sensor elapsed — resolved");
        }

        // Poll any parked `wait.url` HTTP sensor (#27 follow-on) that is due: a 2xx
        // resolves the task (dependents advance); anything else re-parks it for the
        // next WAIT_POLL_SECS window. The parked-shape guards keep this idempotent
        // and HA-safe. A hung endpoint is bounded by the client's request timeout.
        // Probe the due batch CONCURRENTLY. Polled serially, N parked sensors
        // pointed at a black-holed endpoint would hold the tick for N × the
        // request timeout — the per-request timeout bounds one poll, not the
        // tick — stalling claim, dispatch, log drain, and run reaping behind
        // them. `WAIT_URL_BATCH` additionally caps the batch in SQL, so the work
        // one tick can take on is bounded from both ends; anything not polled
        // this tick is simply picked up by the next.
        let due = db::due_url_waits(&pool, WAIT_URL_BATCH).await?;
        if !due.is_empty() {
            let mut probes = tokio::task::JoinSet::new();
            for (task_id, url) in due {
                let client = wait_http.clone(); // Client is an Arc handle — cheap
                let deny_private = wait_url_deny_private;
                probes.spawn(async move {
                    // An IP-literal host never reaches the resolver, so the
                    // deny-private policy has to be applied here as well. This
                    // reads as "not ready" and re-parks rather than failing the
                    // task: the warning is the signal, and relaxing the policy
                    // resolves the parked sensor in place. (Editing the spec
                    // would not — `wait_url` was materialized onto this row when
                    // the task was created, so a spec fix lands on the next run.)
                    if deny_private && wait_url::literal_host_blocked(&url) {
                        tracing::warn!(
                            %task_id, %url,
                            "wait.url names a non-global address — refusing to poll (WAIT_URL_DENY_PRIVATE)"
                        );
                        return (task_id, url, false);
                    }
                    let ready = match client.get(&url).send().await {
                        Ok(resp) => resp.status().is_success(),
                        Err(e) => {
                            tracing::debug!(%task_id, %url, error = %e, "http wait sensor poll errored");
                            false
                        }
                    };
                    (task_id, url, ready)
                });
            }
            while let Some(joined) = probes.join_next().await {
                let Ok((task_id, url, ready)) = joined else { continue }; // task panicked
                if ready {
                    if db::resolve_url_wait(&pool, &task_id).await? {
                        tracing::info!(%task_id, %url, "http wait sensor endpoint ready (2xx) — resolved");
                    }
                } else {
                    let next_poll = (chrono::Utc::now()
                        + chrono::TimeDelta::seconds(wait_poll_secs as i64))
                    .to_rfc3339();
                    db::repark_url_wait(&pool, &task_id, &next_poll).await?;
                }
            }
        }

        // Resolve any parked `wait.dataset` sensor whose dataset recorded an
        // update after the park: the task succeeds and its dependents advance.
        // Idempotent, HA-safe (guarded UPDATE).
        for (task_id, uri) in db::reconcile_dataset_waits(&pool).await? {
            tracing::info!(%task_id, dataset = %uri, "dataset sensor saw a new update — resolved");
        }

        // Dataset-triggered scheduling: sync `on_datasets:` subscriptions from
        // the workflow registry, then claim-and-fire triggers whose datasets
        // recorded new updates. HA-safe with no leadership — claiming is a CAS
        // cursor advance, so exactly one scheduler creates each run; every
        // scheduler may sweep. Throttled: registry parsing every tick would be
        // wasted work at a 500 ms cadence.
        if dataset_triggers_on
            && dataset_trigger_sweep
                .is_none_or(|t: std::time::Instant| t.elapsed().as_secs() >= 5)
        {
            dataset_trigger_sweep = Some(std::time::Instant::now());

            // 1. Sync subscriptions for workflows that declare any. A spec with
            //    no `on_datasets:` is skipped entirely rather than issuing a
            //    per-workflow DELETE every sweep — with a few hundred registered
            //    workflows that would be a few hundred pointless writes a minute,
            //    forever. `subscribed` remembers who we synced, so a workflow that
            //    *drops* its `on_datasets:` still gets its rows cleared exactly
            //    once (and the orphan prune below is the backstop for the rest).
            //    Multi-dataset / all-of composition is the Enterprise data-aware
            //    scheduler — an open build skips such specs with a signpost
            //    instead of silently subscribing to a subset, warning once per
            //    workflow rather than on every sweep.
            match db::list_registered_workflows(&pool).await {
                Ok(wfs) => {
                    let mut still_subscribed: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    for (name, spec) in wfs {
                        match dag::dataset_subscriptions(&spec) {
                            Some((uris, mode)) => {
                                if !cfg!(feature = "enterprise")
                                    && (uris.len() > 1 || mode == "all")
                                {
                                    if warned_dataset_composition.insert(name.clone()) {
                                        tracing::warn!(
                                            workflow = %name, datasets = uris.len(), mode = %mode,
                                            "multi-dataset trigger composition ships with dagron Enterprise — subscription skipped (docs/DATASETS.md#enterprise)"
                                        );
                                    }
                                    if subscribed_workflows.contains(&name) {
                                        let _ = db::sync_dataset_triggers(&pool, &name, &[], "any").await;
                                    }
                                    continue;
                                }
                                warned_dataset_composition.remove(&name);
                                still_subscribed.insert(name.clone());
                                if let Err(e) =
                                    db::sync_dataset_triggers(&pool, &name, &uris, &mode).await
                                {
                                    tracing::warn!(workflow = %name, error = %e, "dataset trigger sync failed");
                                    still_subscribed.remove(&name);
                                }
                            }
                            None => {
                                // Only clear when this scheduler previously synced
                                // rows for it — otherwise this is a no-op workflow.
                                if subscribed_workflows.contains(&name) {
                                    let _ = db::sync_dataset_triggers(&pool, &name, &[], "any").await;
                                }
                                warned_dataset_composition.remove(&name);
                            }
                        }
                    }
                    subscribed_workflows = still_subscribed;
                }
                Err(e) => tracing::warn!(error = %e, "dataset trigger sweep: workflow listing failed"),
            }
            if let Err(e) = db::prune_dataset_triggers(&pool).await {
                tracing::warn!(error = %e, "dataset trigger prune failed");
            }

            // 2. Claim + fire. The triggering dataset is injected as the
            //    `{{ trigger_dataset }}` parameter so the fired run can reference
            //    what woke it. A fire refused at the workflow's max_active_runs
            //    cap rolls its cursors back and retries on a later sweep; a spec
            //    that no longer parses keeps its advanced cursor (skipping the
            //    fire) so a broken spec can't warn-loop forever.
            for fire in db::claim_due_dataset_triggers(&pool).await? {
                let Some(spec_yaml) =
                    db::workflow_spec_by_name(&pool, &fire.workflow_name).await?
                else {
                    continue; // deregistered between sync and claim — prune gets it
                };
                let mut params = std::collections::BTreeMap::new();
                params.insert("trigger_dataset".to_string(), fire.trigger_uri.clone());
                let parsed = match environments::template_params(&pool, &spec_yaml).await {
                    Ok(extra) => {
                        params.extend(extra);
                        dag::DagGraph::from_yaml_with_params(&spec_yaml, &params)
                    }
                    Err(e) => Err(e),
                };
                match parsed {
                    Ok(child_dag) => match db::create_run(&pool, &child_dag, &spec_yaml).await {
                        Ok(run_id) => {
                            metrics.inc_runs_created();
                            metrics.inc_dataset_fires();
                            if let Err(e) =
                                db::stamp_dataset_trigger_fired(&pool, &fire.workflow_name, &run_id)
                                    .await
                            {
                                tracing::warn!(workflow = %fire.workflow_name, error = %e, "dataset trigger stamp failed");
                            }
                            info!(
                                workflow = %fire.workflow_name, %run_id, dataset = %fire.trigger_uri,
                                "dataset trigger fired run"
                            );
                        }
                        Err(e)
                            if e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>()
                                .is_some() =>
                        {
                            db::unclaim_dataset_trigger(&pool, &fire.workflow_name, &fire.advanced)
                                .await?;
                            info!(workflow = %fire.workflow_name, "dataset fire at max_active_runs — will retry");
                        }
                        Err(e) => {
                            db::unclaim_dataset_trigger(&pool, &fire.workflow_name, &fire.advanced)
                                .await?;
                            tracing::warn!(workflow = %fire.workflow_name, error = %e, "dataset fire create_run failed — will retry");
                        }
                    },
                    Err(e) => {
                        tracing::warn!(workflow = %fire.workflow_name, error = %e, "dataset-triggered workflow no longer parses — fire skipped");
                    }
                }
            }
        }

        // ── Step 2: unblock tasks whose deps just completed ─────────────────
        db::advance_ready_tasks(&pool).await?;

        // ── Step 3: claim and dispatch ──────────────────────────────────────
        // Oversized-gang alarm: a gang bigger than this scheduler's whole pool
        // can never be claimed HERE and would sit ready silently if no larger
        // peer exists. Warn loudly, once per gang, on a slow cadence (a peer
        // with a bigger pool may still claim it, so this warns rather than
        // fails — but it must never be silent).
        if gangs_on
            && oversized_gang_check
                .map_or(true, |t: std::time::Instant| t.elapsed().as_secs() >= 60)
        {
            oversized_gang_check = Some(std::time::Instant::now());
            match db::oversized_ready_gangs(&pool, workers.size() as i64).await {
                Ok(gangs) => {
                    for (gang_id, size) in gangs {
                        if warned_oversized_gangs.insert(gang_id.clone()) {
                            tracing::warn!(
                                %gang_id,
                                gang_size = size,
                                pool_size = workers.size(),
                                "gang needs more slots than this scheduler's whole worker pool — it cannot be claimed here; raise WORKER_COUNT, lower gang.size, or run a larger scheduler"
                            );
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "oversized-gang check failed"),
            }
        }

        let capacity = workers.size().saturating_sub(in_flight);
        if capacity > 0 {
            // Both claim paths take the same `pool_caps` (#21) and order by
            // priority (#25), so gang co-scheduling never bypasses either.
            let claimed = if gangs_on {
                // Gangs first (all-or-nothing, and only into a pool that can seat
                // the whole gang), then fill leftover capacity with ordinary
                // tasks — never claiming a gang member solo.
                let mut claimed = db::claim_ready_gang(
                    &pool,
                    &worker_id,
                    capacity as i64,
                    &runner_classes,
                    &pool_caps,
                )
                .await?;
                let remaining = capacity as i64 - claimed.len() as i64;
                if remaining > 0 {
                    claimed.extend(
                        db::claim_ready_classes_nongang(
                            &pool,
                            &worker_id,
                            remaining,
                            &runner_classes,
                            &pool_caps,
                        )
                        .await?,
                    );
                }
                claimed
            } else {
                db::claim_ready_classes(
                    &pool,
                    &worker_id,
                    capacity as i64,
                    &runner_classes,
                    &pool_caps,
                )
                .await?
            };
            for task in claimed {
                let (mut ctx, max_attempts, retry_delay_secs, retry_max_delay_secs, retry_on_timeout, cache, sub_workflow, wait, gang_member, produces) = match &task.input {
                    Some(json) => match serde_json::from_str::<dag::TaskSpec>(json) {
                        Ok(spec) => {
                            // Sub-workflow trigger target (#23) and wait-sensor
                            // config (#27), captured before `spec`'s fields are
                            // moved into the exec context below.
                            let sub_workflow = if spec.task_type.as_deref() == Some("workflow") {
                                spec.workflow.clone()
                            } else {
                                None
                            };
                            let wait = if spec.task_type.as_deref() == Some("wait") {
                                spec.wait.clone()
                            } else {
                                None
                            };
                            // Start with the declared env, then append any top-level
                            // string keys from `input` so parameterized reruns
                            // (deep-merged `params`) visibly change task behavior
                            // without requiring the workflow author to thread each
                            // param through an explicit `env:` entry.
                            #[cfg(not(feature = "enterprise"))]
                            let env = spec.env;
                            #[cfg(feature = "enterprise")]
                            let mut env = spec.env;
                            // Behind the `enterprise` feature: merge top-level string keys
                            // from `input` as env vars so parameterized reruns
                            // (params deep-merge) visibly change
                            // task behavior without threading each param through an explicit
                            // `env:` entry. `spec.env` is authoritative: skip any key whose
                            // uppercased form already appears there, and reject names with
                            // characters outside [A-Z0-9_] to prevent injection of reserved
                            // names (PATH, HOME, etc.).
                            #[cfg(feature = "enterprise")]
                            if let Some(serde_json::Value::Object(map)) = &spec.input {
                                let declared: std::collections::HashSet<String> =
                                    env.iter().map(|e| e.name.clone()).collect();
                                for (k, v) in map {
                                    if let Some(s) = v.as_str() {
                                        let name = k.to_uppercase();
                                        if declared.contains(&name)
                                            || !name.chars().all(|c| c == '_' || c.is_ascii_alphanumeric())
                                        {
                                            continue;
                                        }
                                        env.push(dag::EnvVar { name, value: s.to_string(), value_from: None });
                                    }
                                }
                            }
                            (
                                ExecContext {
                                    command: spec.command,
                                    timeout_secs: spec.timeout_secs,
                                    docker_image: spec.docker_image,
                                    env,
                                    resources: spec.resources,
                                    service_account: spec.service_account,
                                    // Wired per-attempt by the worker from `log_tx`.
                                    log_sink: None,
                                },
                                spec.max_attempts,
                                spec.retry_delay_secs,
                                spec.retry_max_delay_secs,
                                spec.retry_on_timeout.unwrap_or(true),
                                spec.cache,
                                sub_workflow,
                                wait,
                                // Carried out of the parse so the gang rendezvous
                                // env below needs no second deserialization.
                                spec.gang_member,
                                // Carried so a cache hit can record the same
                                // dataset updates a normal success would.
                                spec.produces,
                            )
                        }
                        Err(e) => {
                            // Poison row: a persisted spec this build can't parse.
                            // Failing the whole loop here would crash-loop the
                            // daemon every time the lease is recovered, so fail
                            // just this task terminally and carry on.
                            tracing::error!(
                                task = %task.name, task_id = %task.id, error = %e,
                                "unparseable task spec — marking task failed"
                            );
                            db::mark_task_failed(
                                &pool,
                                &task.id,
                                &worker_id,
                                task.version.saturating_add(1),
                                Some(format!("unparseable task spec: {e}")),
                            )
                            .await?;
                            continue;
                        }
                    },
                    None => (ExecContext::new(vec!["true".to_string()], None, None), 1, 0, None, true, None, None, None, None, Vec::new()),
                };

                // Deferrable wait sensor (#27): park this task with no worker held
                // and let a reconcile sweep resolve it. A `wait.url` HTTP sensor
                // (#27 follow-on) parks on the endpoint and the poll sweep GETs it;
                // a `wait.dataset` sensor parks on the lineage ledger's current
                // high-water mark and resolves on the next recorded update; a
                // time sensor parks on its resume deadline.
                if let Some(w) = &wait {
                    let fence = task.version.saturating_add(1);
                    let now = chrono::Utc::now();
                    if let Some(ds) = &w.dataset {
                        db::park_wait_dataset(&pool, &task.id, fence, ds.trim()).await?;
                        info!(task = %task.name, task_id = %task.id, dataset = %ds.trim(), "dataset sensor deferred — parked with no worker until the dataset next updates");
                        continue;
                    }
                    if let Some(url) = &w.url {
                        // Park immediately due for its first poll on the next tick.
                        db::park_wait_url(&pool, &task.id, fence, url.trim(), &now.to_rfc3339()).await?;
                        info!(task = %task.name, task_id = %task.id, url = %url.trim(), "http wait sensor deferred — parked with no worker until the endpoint returns 2xx");
                        continue;
                    }
                    // Spec validation normally rejects an unparseable deadline,
                    // but a row persisted by a different build lands here — and
                    // defaulting to "already elapsed" would silently succeed the
                    // sensor and unblock the whole downstream DAG. Fail instead.
                    let deadline: Result<chrono::DateTime<chrono::Utc>, String> =
                        if let Some(dur) = &w.wait_for {
                            dag::parse_duration_secs(dur)
                                .map(|secs| {
                                    now + chrono::TimeDelta::seconds(secs.min(i64::MAX as u64) as i64)
                                })
                                .map_err(|e| format!("invalid wait.for '{dur}': {e}"))
                        } else if let Some(until) = &w.until {
                            chrono::DateTime::parse_from_rfc3339(until)
                                .map(|t| t.with_timezone(&chrono::Utc))
                                .map_err(|e| format!("invalid wait.until '{until}': {e}"))
                        } else {
                            Ok(now)
                        };
                    let wake_dt = match deadline {
                        Ok(dt) => dt,
                        Err(msg) => {
                            tracing::error!(task = %task.name, task_id = %task.id, %msg, "unreadable wait deadline — failing task");
                            db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(msg)).await?;
                            continue;
                        }
                    };
                    if wake_dt <= now {
                        db::mark_task_succeeded(&pool, &task.id, &worker_id, fence, Some("wait elapsed".into())).await?;
                    } else {
                        db::park_wait(&pool, &task.id, fence, &wake_dt.to_rfc3339()).await?;
                        info!(task = %task.name, task_id = %task.id, wake_at = %wake_dt.to_rfc3339(), "wait sensor deferred — parked with no worker until the deadline");
                    }
                    continue;
                }

                // Sub-workflow trigger (#23): submit the named registered workflow
                // as a child run and park this task until the child is terminal —
                // no worker is dispatched. The parent succeeds/fails with the child.
                if let Some(child_name) = &sub_workflow {
                    let fence = task.version.saturating_add(1);
                    // Recursion guard: nothing stops a workflow from naming
                    // itself (directly or through a cycle of workflows), and
                    // each hop creates a real run whose parked parent holds a
                    // row — so an unguarded self-reference is an unbounded run
                    // factory, not a stack overflow that fails loudly. Depth is
                    // read by walking `sub_run_id` up from this task's own run;
                    // the walk stops at the cap, so the check costs at most
                    // `SUBWORKFLOW_MAX_DEPTH` lookups on an indexed column.
                    let depth = db::sub_workflow_depth(&pool, &task.run_id, subworkflow_max_depth)
                        .await
                        .unwrap_or(0);
                    if depth >= subworkflow_max_depth {
                        tracing::error!(
                            task = %task.name, workflow = %child_name, depth,
                            max = subworkflow_max_depth,
                            "sub-workflow nesting hit SUBWORKFLOW_MAX_DEPTH — refusing to trigger"
                        );
                        db::mark_task_failed(
                            &pool,
                            &task.id,
                            &worker_id,
                            fence,
                            Some(format!(
                                "sub-workflow nesting depth {depth} reached SUBWORKFLOW_MAX_DEPTH ({subworkflow_max_depth}) — refusing to trigger '{child_name}' (recursive workflow?)"
                            )),
                        )
                        .await?;
                        continue;
                    }
                    match db::workflow_spec_by_name(&pool, child_name).await? {
                        None => {
                            tracing::error!(task = %task.name, workflow = %child_name, "type: workflow names an unknown workflow — failing task");
                            db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(format!("unknown workflow '{child_name}'"))).await?;
                        }
                        Some(child_yaml) => match dag::DagGraph::from_yaml(&child_yaml) {
                            Err(e) => {
                                tracing::error!(task = %task.name, workflow = %child_name, error = %e, "child workflow spec no longer parses — failing task");
                                db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(format!("child workflow '{child_name}' invalid: {e}"))).await?;
                            }
                            Ok(child_dag) => match db::create_run(&pool, &child_dag, &child_yaml).await {
                                Ok(child_run) => {
                                    db::park_subworkflow(&pool, &task.id, fence, &child_run).await?;
                                    info!(task = %task.name, task_id = %task.id, workflow = %child_name, %child_run, "sub-workflow triggered — parking until it finishes");
                                }
                                Err(e)
                                    if e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>().is_some() =>
                                {
                                    // Child at its concurrency cap — release the
                                    // task back to ready and retry on a later tick.
                                    db::release_subworkflow_task(&pool, &task.id, fence).await?;
                                    info!(task = %task.name, workflow = %child_name, "child workflow at max_active_runs — will retry");
                                }
                                Err(e) => {
                                    db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(format!("failed to start child workflow '{child_name}': {e}"))).await?;
                                }
                            },
                        },
                    }
                    continue;
                }

                // Memoization (#22): a cache hit resolves the task from a prior
                // run's output with no worker, secrets, or artifacts — then the
                // dependents advance exactly as for a normal success.
                if let Some(cache) = &cache {
                    let wf_name =
                        db::workflow_name_for_run(&pool, &task.run_id).await?.unwrap_or_default();
                    if let Some(cached) = db::memo_lookup(
                        &pool,
                        &wf_name,
                        &task.name,
                        &cache.key,
                        cache.max_age_secs,
                    )
                    .await?
                    {
                        info!(task = %task.name, task_id = %task.id, key = %cache.key, "cache hit — reusing memoized output");
                        metrics.inc_cache_hits();
                        // `_cached` stamps `cache_hit` on the row: a hit is
                        // otherwise a normal `succeeded` with a reused output,
                        // and the log line + counter above are not attached to
                        // the run anyone is looking at.
                        let marked = db::mark_task_succeeded_cached(
                            &pool,
                            &task.id,
                            &worker_id,
                            task.version.saturating_add(1),
                            Some(cached),
                        )
                        .await?;
                        // A cache hit is a success, so it records the task's
                        // `produces:` datasets exactly as a real execution would —
                        // otherwise a downstream sensor or `on_datasets:` consumer
                        // would park forever whenever the producer hits its cache.
                        if marked {
                            record_produces(
                                &pool,
                                &metrics,
                                &wf_name,
                                &task.id,
                                &task.name,
                                &produces,
                            )
                            .await;
                        }
                        continue;
                    }
                }

                // Gang rendezvous env: members learn their gang, rank, and size
                // (torchrun-style rendezvous wires MASTER_ADDR off rank 0 — on
                // Kubernetes, a headless Service per gang_id). Not gated on
                // `gangs_on`: the spec, the expansion into member rows, and this
                // env are the open programming model — only the all-or-nothing
                // *claimer* is Enterprise. A member row carries its rank however
                // it was scheduled, so ranks must be readable in either build.
                if let Some(member) = gang_member {
                    for (name, value) in [
                        ("DAGRON_GANG_ID", member.id),
                        ("DAGRON_GANG_RANK", member.rank.to_string()),
                        ("DAGRON_GANG_SIZE", member.size.to_string()),
                    ] {
                        ctx.env.push(dag::EnvVar {
                            name: name.to_string(),
                            value,
                            value_from: None,
                        });
                    }
                }

                // Task identity env: lets a task talk about itself to the
                // management API — report a checkpoint, tail a sibling's logs —
                // without the workflow author threading ids through params.
                for (name, value) in [
                    ("DAGRON_RUN_ID", task.run_id.clone()),
                    ("DAGRON_TASK", task.name.clone()),
                    ("DAGRON_TASK_ID", task.id.clone()),
                ] {
                    ctx.env.push(dag::EnvVar {
                        name: name.to_string(),
                        value,
                        value_from: None,
                    });
                }

                // Artifact passing: give this task the run's shared artifact dir,
                // plus a per-task checkpoint dir under it (the file-convention
                // half of checkpoint-aware resume — see below).
                let mut checkpoint_dir: Option<std::path::PathBuf> = None;
                if let Some(store) = &artifact_store {
                    match store.prepare_run_dir(&task.run_id).await {
                        Ok(dir) => {
                            let ck = std::path::Path::new(&dir)
                                .join(".checkpoints")
                                .join(&task.name);
                            match tokio::fs::create_dir_all(&ck).await {
                                Ok(()) => {
                                    ctx.env.push(dag::EnvVar {
                                        name: "DAGRON_CHECKPOINT_DIR".to_string(),
                                        value: ck.to_string_lossy().into_owned(),
                                        value_from: None,
                                    });
                                    checkpoint_dir = Some(ck);
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, run = %task.run_id, "could not prepare checkpoint dir")
                                }
                            }
                            ctx.env.push(dag::EnvVar {
                                name: "DAGRON_ARTIFACTS".to_string(),
                                value: dir,
                                value_from: None,
                            });
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, run = %task.run_id, "could not prepare artifact dir")
                        }
                    }
                }

                // Cloud artifact/checkpoint URLs: pure string composition (no
                // I/O), matching the cloud store's sanitized layout so a task's
                // uploads land where the artifact API reads them.
                if let Some(url) = &artifact_url {
                    let run_seg = dagron_artifact::sanitize_component(&task.run_id);
                    let task_seg = dagron_artifact::sanitize_component(&task.name);
                    for (name, value) in [
                        ("DAGRON_ARTIFACTS_URL", format!("{url}/{run_seg}")),
                        (
                            "DAGRON_CHECKPOINT_URL",
                            format!("{url}/{run_seg}/.checkpoints/{task_seg}"),
                        ),
                    ] {
                        ctx.env.push(dag::EnvVar {
                            name: name.to_string(),
                            value,
                            value_from: None,
                        });
                    }
                }

                // Checkpoint-aware resume: a retry attempt is handed the last
                // committed checkpoint instead of restarting from zero. The
                // pointer comes from the datastore (reported via
                // POST …/tasks/{id}/checkpoint), falling back to the
                // `<checkpoint_dir>/latest` file convention for setups that run
                // without the management API. dagron owns pointer durability;
                // what is inside the checkpoint belongs to the task.
                if task.attempt > 0 {
                    let mut resume: Option<(String, Option<String>)> = None;
                    match db::task_checkpoint(&pool, &task.id).await {
                        Ok(found @ Some(_)) => resume = found,
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, task = %task.name, "checkpoint lookup failed — dispatching without resume pointer")
                        }
                    }
                    if resume.is_none() {
                        if let Some(ck) = &checkpoint_dir {
                            if let Ok(uri) = tokio::fs::read_to_string(ck.join("latest")).await {
                                let uri = uri.trim().to_string();
                                if !uri.is_empty() {
                                    resume = Some((uri, None));
                                }
                            }
                        }
                    }
                    if let Some((uri, marker)) = resume {
                        info!(task = %task.name, attempt = task.attempt + 1, resume_from = %uri, "dispatching with checkpoint resume");
                        ctx.env.push(dag::EnvVar {
                            name: "DAGRON_RESUME_FROM".to_string(),
                            value: uri,
                            value_from: None,
                        });
                        if let Some(marker) = marker {
                            ctx.env.push(dag::EnvVar {
                                name: "DAGRON_RESUME_MARKER".to_string(),
                                value: marker,
                                value_from: None,
                            });
                        }
                    }
                }

                // Resolve `value_from` secret refs into concrete env values just
                // before dispatch (#9): the run's environment secret store
                // first (DB, decrypted), then process env / secrets dir. A
                // missing secret fails the task rather than running it with an
                // empty credential.
                if let Err(e) =
                    environments::resolve_secrets(&pool, &task.run_id, &mut ctx.env).await
                {
                    tracing::error!(task = %task.name, task_id = %task.id, error = %e, "secret resolution failed — marking task failed");
                    db::mark_task_failed(
                        &pool,
                        &task.id,
                        &worker_id,
                        task.version.saturating_add(1),
                        Some(format!("secret resolution failed: {e}")),
                    )
                    .await?;
                    continue;
                }

                // Per-task dispatch span (#28): groups the trace-context
                // propagation and the enqueue under one `tracing` span, so an OTLP
                // exporter (`--features otel` + `OTEL_EXPORTER_OTLP_ENDPOINT`)
                // renders a span per dispatched task. Entered only around this
                // synchronous section — no `.await` occurs inside the closure, so
                // the span opens and closes cleanly on this thread.
                let dispatch_span = tracing::info_span!(
                    "task.dispatch",
                    task = %task.name,
                    task_id = %task.id,
                    run_id = %task.run_id,
                    attempt = task.attempt + 1,
                );
                dispatch_span.in_scope(|| -> anyhow::Result<()> {
                    // OpenTelemetry (#28): hand this task a W3C trace context in
                    // the standard `TRACEPARENT` carrier, so its own
                    // instrumentation joins the trace rather than starting an
                    // island. Prefer the **active** `task.dispatch` span's
                    // context, injected through the global propagator — that is
                    // what makes the task's spans children of the span the OTLP
                    // exporter ships. When no `tracing-opentelemetry` layer is
                    // installed (an `otel` build without
                    // `OTEL_EXPORTER_OTLP_ENDPOINT`) the context is empty and the
                    // propagator injects nothing, so fall back to a freshly
                    // generated context — that build still gets a valid, unique
                    // traceparent, exactly as before. Off entirely without
                    // `--features otel`.
                    #[cfg(feature = "otel")]
                    {
                        use tracing_opentelemetry::OpenTelemetrySpanExt;
                        let mut carrier = std::collections::HashMap::<String, String>::new();
                        let cx = tracing::Span::current().context();
                        opentelemetry::global::get_text_map_propagator(|p| {
                            p.inject_context(&cx, &mut carrier)
                        });
                        let (traceparent, trace_id, linked) = match carrier.remove("traceparent") {
                            Some(tp) => {
                                // "00-<32-hex trace>-<16-hex span>-<flags>"
                                let id = tp.split('-').nth(1).unwrap_or_default().to_string();
                                (tp, id, true)
                            }
                            None => {
                                let (tp, id) = new_traceparent();
                                (tp, id, false)
                            }
                        };
                        ctx.env.push(dag::EnvVar {
                            name: "TRACEPARENT".to_string(),
                            value: traceparent,
                            value_from: None,
                        });
                        info!(task = %task.name, task_id = %task.id, trace_id = %trace_id, linked, "trace context propagated to task");
                    }

                    info!(
                        task = %task.name,
                        attempt = task.attempt + 1,
                        max_attempts,
                        // Log only the program, not full argv — args may carry secrets.
                        cmd = %ctx.command.first().map(String::as_str).unwrap_or("<empty>"),
                        "dispatching"
                    );

                    workers.dispatch(DispatchPayload {
                        task_id: task.id.clone(),
                        worker_id: worker_id.clone(),
                        ctx,
                        attempt: task.attempt,
                        max_attempts,
                        retry_delay_secs,
                        retry_max_delay_secs,
                        retry_on_timeout,
                        // Post-claim version is the fencing token (claim_ready bumped
                        // version from task.version to task.version + 1). saturating_add
                        // guards the theoretical i64 overflow without a debug panic.
                        fence: task.version.saturating_add(1),
                        result_tx: tx.clone(),
                        log_tx: Some(log_tx.clone()),
                    })?;
                    metrics.inc_dispatched();
                    in_flight += 1;
                    Ok(())
                })?;
            }
        }

        // ── Step 3b: drain live-log chunks (#17) ────────────────────────────
        // Append incremental output from still-running tasks so the API/UI can
        // tail it before the task exits. Fence-guarded, so a stale attempt's late
        // chunk can't corrupt a re-run; the first chunk of an attempt resets any
        // prior-attempt output. Best-effort: a failed append is logged, not fatal.
        while let Ok(chunk) = log_rx.try_recv() {
            if let Err(e) =
                db::append_task_output(&pool, &chunk.task_id, chunk.fence, &chunk.chunk, chunk.first)
                    .await
            {
                tracing::warn!(task_id = %chunk.task_id, error = %e, "live-log append failed");
            }
        }

        // ── Step 4: collect finished tasks ──────────────────────────────────
        while let Ok(result) = rx.try_recv() {
            in_flight = in_flight.saturating_sub(1);

            if result.success {
                // Loop operator (`repeat:`): a successful iteration only counts
                // as task success once `until` holds. Otherwise the task is
                // re-queued (reusing the retry machinery: fence-guarded ready +
                // scheduled_at delay; `attempt` doubles as the iteration count),
                // and after max_iterations the loop fails loudly — a condition
                // that never came true is an error, not a success.
                // Parse the task spec once here; `repeat` (below) and `cache`
                // (the memo write after success) both read from it, so a
                // non-repeating, uncached task costs no extra work.
                let task_spec: Option<dag::TaskSpec> =
                    match db::task_input_json(&pool, &result.task_id).await {
                        Ok(Some(json)) => serde_json::from_str::<dag::TaskSpec>(&json).ok(),
                        _ => None,
                    };
                let repeat = task_spec.as_ref().and_then(|s| s.repeat.clone());
                if let Some(rep) = repeat {
                    let iteration = result.attempt + 1; // the iteration that just ran
                    let output = result.output.clone().unwrap_or_default();
                    let mut ctx = std::collections::BTreeMap::new();
                    ctx.insert("output".to_string(), output.trim().to_string());
                    ctx.insert("attempt".to_string(), iteration.to_string());
                    let done = dagron_core::expand::eval_when(&dagron_core::expand::substitute(
                        &rep.until, &ctx,
                    ));
                    match done {
                        Ok(true) => {} // condition met — fall through to success
                        Ok(false) if iteration < rep.max_iterations as i64 => {
                            let delay = i64::try_from(rep.delay_secs).unwrap_or(i64::MAX);
                            let retry_at =
                                (chrono::Utc::now() + chrono::TimeDelta::seconds(delay)).to_rfc3339();
                            info!(
                                task_id = %result.task_id,
                                iteration,
                                max_iterations = rep.max_iterations,
                                delay_secs = rep.delay_secs,
                                "repeat.until not yet satisfied — re-queueing iteration"
                            );
                            db::retry_task(
                                &pool,
                                &result.task_id,
                                &result.worker_id,
                                result.fence,
                                result.output,
                                retry_at,
                            )
                            .await?;
                            continue;
                        }
                        Ok(false) => {
                            info!(task_id = %result.task_id, iteration, "repeat.until never satisfied — failing task");
                            metrics.inc_failed();
                            seams.meter.on_task_completed(false).await;
                            db::mark_task_failed(
                                &pool,
                                &result.task_id,
                                &result.worker_id,
                                result.fence,
                                Some(format!(
                                    "repeat.until '{}' not satisfied after {} iterations; last output:\n{}",
                                    rep.until, iteration, output
                                )),
                            )
                            .await?;
                            continue;
                        }
                        Err(e) => {
                            metrics.inc_failed();
                            seams.meter.on_task_completed(false).await;
                            db::mark_task_failed(
                                &pool,
                                &result.task_id,
                                &result.worker_id,
                                result.fence,
                                Some(format!("repeat.until '{}' failed to evaluate: {e}", rep.until)),
                            )
                            .await?;
                            continue;
                        }
                    }
                }

                info!(task_id = %result.task_id, "task succeeded");
                metrics.inc_succeeded();
                seams.meter.on_task_completed(true).await; // extension seam (usage accounting)
                // Keep the output for the memo write below — `mark_task_succeeded`
                // consumes `result.output`, and the memo must not be written until
                // that fenced mutation has actually landed.
                let memo_output = task_spec
                    .as_ref()
                    .filter(|s| s.cache.is_some())
                    .map(|_| result.output.clone().unwrap_or_default());
                let marked = db::mark_task_succeeded(
                    &pool,
                    &result.task_id,
                    &result.worker_id,
                    result.fence,
                    result.output,
                )
                .await?;
                // Post-success side effects — the memoization write (#22) and the
                // `produces:` dataset ledger — run **only when the fence held**.
                // A reclaimed attempt's late result gets `marked == false`, and
                // must neither poison the cache with a stale output nor fabricate
                // lineage. Both are best-effort: a failure here never fails the
                // run. Only tasks that use a feature pay for the name lookup.
                if marked {
                    let needs_wf = task_spec
                        .as_ref()
                        .is_some_and(|s| s.cache.is_some() || !s.produces.is_empty());
                    let wf = if needs_wf {
                        db::workflow_name_for_task(&pool, &result.task_id)
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or_default()
                    } else {
                        String::new()
                    };
                    if let (Some(spec), Some(cache), Some(output)) = (
                        task_spec.as_ref(),
                        task_spec.as_ref().and_then(|s| s.cache.as_ref()),
                        memo_output,
                    ) {
                        if let Err(e) =
                            db::memo_store(&pool, &wf, &spec.name, &cache.key, &output).await
                        {
                            tracing::warn!(error = %e, task_id = %result.task_id, "memo store failed (best-effort)");
                        }
                    }
                    if let Some(spec) = task_spec.as_ref() {
                        record_produces(
                            &pool,
                            &metrics,
                            &wf,
                            &result.task_id,
                            &spec.name,
                            &spec.produces,
                        )
                        .await;
                    }
                }
            } else {
                // attempt + 1 = the attempt number that just ran (claim_ready increments
                // the counter in the DB, but the snapshot we received is pre-claim).
                let actual_attempt = result.attempt + 1;
                // Normally retry while attempts remain, but a `timeout_secs`
                // deadline kill on a task with `retry_on_timeout: false` fails at
                // once — a timeout usually recurs (#24).
                if dagron_core::models::should_retry_failed(
                    actual_attempt,
                    result.max_attempts,
                    result.timed_out,
                    result.retry_on_timeout,
                ) {
                    // Exponential backoff: base * 2^(attempt-1), capped at 2^10 doublings
                    // and clamped to the spec's optional retry_max_delay_secs ceiling.
                    let shift = (actual_attempt as u32).saturating_sub(1).min(10);
                    let mut delay_secs = result.retry_delay_secs.saturating_mul(1u64 << shift);
                    if let Some(cap) = result.retry_max_delay_secs {
                        delay_secs = delay_secs.min(cap);
                    }
                    let delay_i64 = i64::try_from(delay_secs).unwrap_or(i64::MAX);
                    let retry_at = (chrono::Utc::now()
                        + chrono::TimeDelta::seconds(delay_i64))
                    .to_rfc3339();
                    info!(
                        task_id = %result.task_id,
                        attempt = actual_attempt,
                        max_attempts = result.max_attempts,
                        retry_in_secs = delay_secs,
                        "task failed — scheduling retry"
                    );
                    metrics.inc_retried();
                    db::retry_task(
                        &pool,
                        &result.task_id,
                        &result.worker_id,
                        result.fence,
                        result.output,
                        retry_at,
                    )
                    .await?;
                } else {
                    // Terminal failure. Either attempts are exhausted, or the task
                    // timed out and opted out of timeout retries (#24) — record
                    // which so an operator can tell a budget-exhausted task from a
                    // deliberate no-retry-on-timeout one.
                    let not_retried_due_to_timeout = result.timed_out && !result.retry_on_timeout;
                    info!(
                        task_id = %result.task_id,
                        attempt = actual_attempt,
                        max_attempts = result.max_attempts,
                        timed_out = result.timed_out,
                        not_retried_due_to_timeout,
                        "task failed — not retrying"
                    );
                    metrics.inc_failed();
                    seams.meter.on_task_completed(false).await; // extension seam (usage accounting)
                    db::mark_task_failed(
                        &pool,
                        &result.task_id,
                        &result.worker_id,
                        result.fence,
                        result.output,
                    )
                    .await?;
                    // Die-together: a failed gang member takes its siblings
                    // with it (their heartbeats lose the fenced claim and
                    // abort). The gang retries as a unit via run-level rerun.
                    if gangs_on {
                        match db::cancel_gang_siblings(&pool, &result.task_id).await {
                            Ok(0) => {}
                            Ok(n) => info!(task_id = %result.task_id, cancelled = n, "gang member failed — siblings cancelled"),
                            Err(e) => tracing::warn!(error = %e, "gang sibling cancel failed"),
                        }
                    }
                }
            }
        }

        // ── Step 5: finalize any runs whose tasks are all terminal ──────────
        for (run_id, status) in db::reap_completed_runs(&pool).await? {
            info!(%run_id, %status, "run complete");
            // Extension seam: no-op by default; an alternate build may emit the run event.
            seams.run_sink.on_run_completed(&run_id, &status.to_string()).await;
            // OpenLineage: emit the terminal RunEvent (best-effort — a lineage
            // backend being down never affects run execution).
            if let Some(ol) = &lineage {
                let job = db::workflow_name_for_run(&pool, &run_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| run_id.clone());
                let failed = status.to_string() == "failed";
                if let Err(e) = ol.emit_run_completed(&run_id, &job, failed).await {
                    tracing::warn!(error = %e, %run_id, "OpenLineage emit failed");
                }
            }
            // Forge feedback: if the run's spec has a `notify.git` block, post the
            // terminal commit status (best-effort — a forge being down never
            // affects run execution).
            if let Some(forge) = &forge {
                post_forge_status(forge, &pool, &run_id, &status.to_string()).await;
            }
            // Operator notifications: push the terminal status to any
            // notify.webhook / notify.slack targets in the run's spec. Spawned
            // so up to four sequential HTTP posts (each with a 10s timeout)
            // can't stall task dispatch for every other run in this tick.
            {
                let pool = pool.clone();
                let run_id = run_id.clone();
                let status = status.to_string();
                tokio::spawn(async move {
                    notify::notify_run_event(&pool, &run_id, &status).await;
                });
            }
        }

        // ── Step 6: drain-mode shutdown ─────────────────────────────────────
        // Only one-shot sources (file) ever set `exhausted`; streaming queue
        // sources keep it false, so this daemon runs until killed. Once the
        // source is drained, no task is in flight, and no run is still active,
        // there is nothing left to do — exit cleanly. UNLESS an ops time-source
        // (cron / DB schedules) or the management API is active: then the process
        // is a long-running server and must stay up for future fires.
        if !stay_resident && exhausted.load(Ordering::SeqCst) && in_flight == 0 {
            let active = db::count_active_runs(&pool).await?;
            if active == 0 {
                info!("all runs drained — scheduler exiting");
                break;
            }
        }

        metrics.observe_reconcile_tick(tick_start.elapsed().as_secs_f64());

        // Wake on the next task-readiness event (Postgres) or after the poll
        // interval (SQLite / safety net for time-based retries), whichever first.
        waker.wait(poll_interval).await?;
    }

    pool.close().await;
    // Flush any spans the OTLP batch exporter is still holding (no-op unless the
    // `otel` feature is built in and an endpoint was configured).
    dagron_logging::shutdown();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{new_traceparent, parse_max_inflight_runs};

    /// `MAX_INFLIGHT_RUNS` contract, as the Helm chart and `values.yaml`
    /// document it: default 64, an explicit number is honoured verbatim, and
    /// `0` **disables** the cap rather than clamping to a cap of one.
    #[test]
    fn max_inflight_runs_zero_disables_the_cap() {
        let cap = |s: &str| parse_max_inflight_runs(Some(s.to_string()));
        assert_eq!(parse_max_inflight_runs(None), 64, "unset → default");
        assert_eq!(cap("200"), 200);
        assert_eq!(cap(" 200 "), 200, "whitespace tolerated");
        assert_eq!(cap("1"), 1, "a cap of one is still a cap");
        assert_eq!(cap("0"), 0, "0 disables — never clamped to 1");
        assert_eq!(cap("-5"), 0, "negative normalizes to disabled");
        assert_eq!(cap("banana"), 64, "unparseable → default");
        assert_eq!(cap(""), 64, "empty → default");
    }

    /// A generated traceparent is a valid W3C header: version 00, 32-hex trace
    /// id, 16-hex span id, sampled flag — and the trace id is never all-zero.
    #[test]
    fn traceparent_is_valid_w3c() {
        let (tp, trace_id) = new_traceparent();
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4, "traceparent has four fields: {tp}");
        assert_eq!(parts[0], "00", "version 00");
        assert_eq!(parts[1].len(), 32, "trace id is 16 bytes");
        assert_eq!(parts[2].len(), 16, "span id is 8 bytes");
        assert_eq!(parts[3], "01", "sampled");
        assert_eq!(parts[1], trace_id, "returned trace id matches the header");
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(parts[1], "0".repeat(32), "trace id is never the forbidden all-zero");
        // Two calls produce distinct traces.
        assert_ne!(new_traceparent().0, tp);
    }
}
