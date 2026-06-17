//! dagron engine — the reconcile-loop daemon as a reusable library.
//!
//! [`run`] is the whole scheduler: config from env, executor + worker pool + db
//! pool + ingest actor, the ops surface, and the multi-run reconcile loop. Both
//! the OSS `dagron` binary and the downstream binary are thin shells over it,
//! differing only in the [`Seams`] they pass (built-in vs. extra sources; no-op
//! vs. active run-lifecycle hooks).

pub mod hooks;
pub use hooks::Seams;

// ── ops surface (feature `ops`) — the axum management API + the leadership-gated
// cron / GC / DB-schedule loops. They wire the library crates together rather than
// belonging to any one, so they live in the engine alongside the run loop.
#[cfg(feature = "ops")]
mod api;
#[cfg(feature = "ops")]
mod cron;
#[cfg(feature = "ops")]
mod gc;
#[cfg(feature = "ops")]
mod leadership;
#[cfg(feature = "ops")]
mod schedule;

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

/// Run the dagron scheduler daemon to completion (or until killed). `seams`
/// selects the edition behaviour; pass `Seams::default()` for the OSS engine.
pub async fn run(seams: Seams) -> Result<()> {
    // Tunable, SaaS-ready logging for the workflow controller (and the worker
    // pool it spawns). Verbosity/format are env-driven — see the shared
    // `dagron_logging` crate for the full knob list (RUST_LOG / LOG_LEVEL /
    // LOG_FORMAT / …).
    dagron_logging::init("controller");

    // GitOps init: seed /workflows from the image's bundled examples if empty.
    // (Was docker-entrypoint.sh; in-binary now so the image needs no shell.)
    seed_workflow_dir();

    let args: Vec<String> = std::env::args().collect();

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

    info!(%worker_id, %dag_path, db = %redact_conn(&db_target), %executor_kind, worker_count, %source_kind, max_inflight_runs, "scheduler starting");

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
        stay_resident =
            api_on || cron_config.is_some() || gc_retention_secs.is_some() || db_schedules_on;

        if cron_config.is_some() || gc_retention_secs.is_some() || db_schedules_on {
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
                let (p, l) = (pool.clone(), Arc::clone(&is_leader));
                tokio::spawn(async move { gc::run(p, retention, interval, l).await });
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

    let poll_interval = std::time::Duration::from_millis(500);

    // Simple counter: how many tasks are currently in-flight inside the worker pool.
    let mut in_flight: usize = 0;

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

        // ── Step 2: unblock tasks whose deps just completed ─────────────────
        db::advance_ready_tasks(&pool).await?;

        // ── Step 3: claim and dispatch ──────────────────────────────────────
        let capacity = workers.size().saturating_sub(in_flight);
        if capacity > 0 {
            let claimed = db::claim_ready(&pool, &worker_id, capacity as i64).await?;
            for task in claimed {
                let (ctx, max_attempts, retry_delay_secs) = match &task.input {
                    Some(json) => match serde_json::from_str::<dag::TaskSpec>(json) {
                        Ok(spec) => (
                            ExecContext {
                                command: spec.command,
                                timeout_secs: spec.timeout_secs,
                                docker_image: spec.docker_image,
                                env: spec.env,
                                resources: spec.resources,
                                service_account: spec.service_account,
                            },
                            spec.max_attempts,
                            spec.retry_delay_secs,
                        ),
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
                    None => (ExecContext::new(vec!["true".to_string()], None, None), 1, 0),
                };

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
                    // Post-claim version is the fencing token (claim_ready bumped
                    // version from task.version to task.version + 1). saturating_add
                    // guards the theoretical i64 overflow without a debug panic.
                    fence: task.version.saturating_add(1),
                    result_tx: tx.clone(),
                })?;
                metrics.inc_dispatched();
                in_flight += 1;
            }
        }

        // ── Step 4: collect finished tasks ──────────────────────────────────
        while let Ok(result) = rx.try_recv() {
            in_flight = in_flight.saturating_sub(1);

            if result.success {
                info!(task_id = %result.task_id, "task succeeded");
                metrics.inc_succeeded();
                seams.meter.on_task_completed(true).await; // edition seam (usage accounting)
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
                    // Exponential backoff: base * 2^(attempt-1), capped at 2^10 doublings.
                    let shift = (actual_attempt as u32).saturating_sub(1).min(10);
                    let delay_secs = result.retry_delay_secs.saturating_mul(1u64 << shift);
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
                    seams.meter.on_task_completed(false).await; // edition seam (usage accounting)
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
            // Edition seam: OSS no-op; downstream editions may emit the run event.
            seams.run_sink.on_run_completed(&run_id, &status.to_string()).await;
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
