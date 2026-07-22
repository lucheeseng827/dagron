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
    if args.get(1).map(String::as_str) == Some("validate") {
        return validate::run_cli(&args[2..]);
    }

    // `dagron archive-compact [db_target]` — one bounded sweep folding archived
    // run documents into the Parquet dataset (k8s CronJob shape). Like
    // `validate`, handled before daemon setup; unlike it, it needs the sink env
    // (GC_ARCHIVE_DIR / GC_ARCHIVE_URL) and optionally a datastore to stamp
    // `archived_runs`. Feature-gated: without `archive-parquet` the subcommand
    // is a clear startup error, never a silent no-op.
    if args.get(1).map(String::as_str) == Some("archive-compact") {
        #[cfg(feature = "archive-parquet")]
        return archive_compact::run_cli(&args[2..]).await;
        #[cfg(not(feature = "archive-parquet"))]
        anyhow::bail!("`dagron archive-compact` requires building with `--features archive-parquet`");
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
    // runner for one workload shape (ee/RUNNER_SEGMENTATION.md). Unset/empty =
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

    // Ingestion source: SOURCE=file|redis|sqs|kafka (default: file). Queue
    // backends require their Cargo feature. MAX_INFLIGHT_RUNS caps how many runs
    // may be active at once — the admission valve that lets the scheduler absorb
    // a large influx by leaving the overflow buffered in the queue.
    let source_kind = std::env::var("SOURCE").unwrap_or_else(|_| "file".to_string());
    let max_inflight_runs: i64 = std::env::var("MAX_INFLIGHT_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
        .max(1);
    // How many times a transient create_run failure is retried (nacked) before a
    // submission is dead-lettered. Parse failures dead-letter immediately.
    let dead_letter_max_attempts: i64 = std::env::var("DEAD_LETTER_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1);

    info!(%worker_id, %dag_path, db = %redact_conn(&db_target), %executor_kind, worker_count, %source_kind, max_inflight_runs, runner_classes = ?runner_classes, "scheduler starting");

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

    let workers = WorkerPool::new(worker_count, executor, Arc::clone(&metrics)).await?;
    info!(size = workers.size(), "worker pool ready");

    let pool = db::init_pool(&db_target).await?;

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
                // Archive-before-purge (ee/STATE_STORE.md hot/cold split):
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
        source::build_with(&source_kind, dag_path, seams.source_factory.as_deref()).await?
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

    info!("reconcile loop running (multi-run, queue-driven daemon)");

    loop {
        // Tick timer — the recover→advance→dispatch→collect→reap span. A tick
        // pegging the CPU (the LOADTEST.md finding) shows up as this histogram's
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

        // ── Step 2: unblock tasks whose deps just completed ─────────────────
        db::advance_ready_tasks(&pool).await?;

        // ── Step 3: claim and dispatch ──────────────────────────────────────
        let capacity = workers.size().saturating_sub(in_flight);
        if capacity > 0 {
            let claimed =
                db::claim_ready_classes(&pool, &worker_id, capacity as i64, &runner_classes)
                    .await?;
            for task in claimed {
                let (mut ctx, max_attempts, retry_delay_secs, retry_max_delay_secs) = match &task.input {
                    Some(json) => match serde_json::from_str::<dag::TaskSpec>(json) {
                        Ok(spec) => {
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
                    None => (ExecContext::new(vec!["true".to_string()], None, None), 1, 0, None),
                };

                // Artifact passing: give this task the run's shared artifact dir.
                if let Some(store) = &artifact_store {
                    match store.prepare_run_dir(&task.run_id).await {
                        Ok(dir) => ctx.env.push(dag::EnvVar {
                            name: "DAGRON_ARTIFACTS".to_string(),
                            value: dir,
                            value_from: None,
                        }),
                        Err(e) => {
                            tracing::warn!(error = %e, run = %task.run_id, "could not prepare artifact dir")
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
                    // Post-claim version is the fencing token (claim_ready bumped
                    // version from task.version to task.version + 1). saturating_add
                    // guards the theoretical i64 overflow without a debug panic.
                    fence: task.version.saturating_add(1),
                    result_tx: tx.clone(),
                    log_tx: Some(log_tx.clone()),
                })?;
                metrics.inc_dispatched();
                in_flight += 1;
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
                let repeat = match db::task_input_json(&pool, &result.task_id).await {
                    Ok(Some(json)) => serde_json::from_str::<dag::TaskSpec>(&json)
                        .ok()
                        .and_then(|s| s.repeat),
                    _ => None,
                };
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
                db::mark_task_succeeded(
                    &pool,
                    &result.task_id,
                    &result.worker_id,
                    result.fence,
                    result.output,
                )
                .await?;
            } else {
                // attempt + 1 = the attempt number that just ran (claim_ready increments
                // the counter in the DB, but the snapshot we received is pre-claim).
                let actual_attempt = result.attempt + 1;
                if actual_attempt < result.max_attempts as i64 {
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
                    info!(
                        task_id = %result.task_id,
                        attempt = actual_attempt,
                        max_attempts = result.max_attempts,
                        "task failed — max attempts reached"
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
    Ok(())
}
