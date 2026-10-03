//! Process metrics + Prometheus exposition (v5).
//!
//! Two kinds of signal feed `GET /metrics`:
//!
//! * **Process-lifetime counters** ([`Metrics`]) — monotonic totals (`runs
//!   created`, `tasks dispatched/succeeded/failed/retried`) accumulated by this
//!   scheduler since boot. Plain atomics, incremented on the hot path.
//! * **Datastore gauges** ([`MetricsSnapshot`](crate::models::MetricsSnapshot)) —
//!   live run/task counts grouped by status, read fresh from the DB per scrape.
//!   The datastore is the source of truth, so these survive a restart and reflect
//!   the whole cluster, not just this process.
//!
//! [`Metrics::render`] formats both into the Prometheus text exposition format —
//! no extra registry crate, just the handful of series this scheduler emits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[cfg(feature = "ops")]
use std::fmt::Write as _;

#[cfg(feature = "ops")]
use crate::models::MetricsSnapshot;

/// A gate this build refuses with a signpost, counted when it is hit
/// (`scheduler_signpost_hits_total{gate}`).
///
/// Only the gates that refuse a request while the engine keeps running are
/// here. A gate met at startup (`SOURCE=fleet`, a managed connector kind, a KMS
/// key provider) stops the process, so there is no `/metrics` left to read its
/// count from; and dagron-api's gates run in a process that serves no
/// Prometheus endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignpostGate {
    /// `budget.external_cost_attribution` in a workflow spec.
    ExternalCostAttribution,
    /// `defer.connection:` on a task.
    DeferConnection,
    /// `POST /datasets/events` on the engine API.
    ExternalDatasetEvents,
}

impl SignpostGate {
    pub const ALL: [SignpostGate; 3] = [
        SignpostGate::ExternalCostAttribution,
        SignpostGate::DeferConnection,
        SignpostGate::ExternalDatasetEvents,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SignpostGate::ExternalCostAttribution => "external_cost_attribution",
            SignpostGate::DeferConnection => "defer_connection",
            SignpostGate::ExternalDatasetEvents => "external_dataset_events",
        }
    }
}

/// Signpost hits, per [`SignpostGate`], for the whole process.
///
/// Process-wide rather than on [`Metrics`], because two of the three gates are
/// spec validation in this crate: a pure function that has no `Metrics` handle
/// and runs wherever a spec is parsed. Every process counts its own; only the
/// engine renders them.
static SIGNPOST_HITS: [AtomicU64; SignpostGate::ALL.len()] =
    [const { AtomicU64::new(0) }; SignpostGate::ALL.len()];

/// Count one hit of a signpost gate. Call it where the refusal is made, once
/// per refusal.
pub fn record_signpost_hit(gate: SignpostGate) {
    if let Some(i) = SignpostGate::ALL.iter().position(|g| *g == gate) {
        SIGNPOST_HITS[i].fetch_add(1, Ordering::Relaxed);
    }
}

/// Hits so far for one gate, in this process.
pub fn signpost_hits(gate: SignpostGate) -> u64 {
    SignpostGate::ALL
        .iter()
        .position(|g| *g == gate)
        .map_or(0, |i| SIGNPOST_HITS[i].load(Ordering::Relaxed))
}

/// Upper bounds (seconds) for the latency histograms. Spans sub-millisecond
/// scheduling latencies through minutes-long ETL tasks so one bucket set serves
/// `reconcile_tick`, `task_duration`, and the dispatch-path histograms. A
/// `+Inf` bucket is appended at render. The sub-5 ms bounds exist for the
/// low-latency profile (docs/LOW_LATENCY.md A-5): hop-level SLOs are single-digit
/// milliseconds, which the old 5 ms floor could not resolve.
const DURATION_BUCKETS: &[f64] = &[
    0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Bucket bounds for the claim batch-size histogram — counts, not seconds.
/// Sized to worker-pool scales (`WORKER_COUNT` defaults 16, profiles run 64+).
const BATCH_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0];

/// Upper bounds (seconds) for whole-run durations. A run is a pipeline, not a
/// task: a CI build or an ETL chain runs for minutes to hours, which
/// [`DURATION_BUCKETS`]' 300 s ceiling would flatten into `+Inf`.
const RUN_DURATION_BUCKETS: &[f64] = &[
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0, 1800.0, 3600.0, 7200.0, 14400.0,
];

/// Max distinct `workflow` label values per series family. Workflow names come
/// from submitted specs, so the cap is what stops a client that names every run
/// differently from minting a series per run; the rest fold into
/// `workflow="other"`.
const WORKFLOW_SERIES_CAP: usize = 50;

/// Max distinct `environment` and dead-letter `source` label values.
#[cfg(feature = "ops")]
const SMALL_SERIES_CAP: usize = 20;

/// The label value the tail beyond a series cap is folded into.
const OTHER: &str = "other";

/// Max distinct `runner_class` label values exported per scrape. The class
/// comes from workflow specs (only syntax-validated), so without a cap a
/// submitter minting a class per run would mint a Prometheus series per run;
/// classes beyond the cap fold into `runner_class="other"`. Generous next to
/// any sane operator taxonomy (a handful of pools).
#[cfg(feature = "ops")]
const READY_CLASS_SERIES_CAP: usize = 20;

/// A fixed-bucket Prometheus histogram backed by plain atomics — no registry
/// crate, matching the hand-rolled exposition the rest of this module uses.
///
/// Each `observe` bumps the smallest bucket whose bound it falls under (buckets
/// are made cumulative at render), the observation count, and a microsecond sum
/// (integer atomic; divided back to seconds in the exposition).
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    /// One counter per bound (non-cumulative); render emits the running total.
    buckets: Vec<AtomicU64>,
    /// Observations above the largest bound (the implicit `+Inf` bucket delta).
    overflow: AtomicU64,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: bounds.iter().map(|_| AtomicU64::new(0)).collect(),
            overflow: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one observation in seconds. Cheap relaxed atomics — the hot path
    /// (every dispatched task, every reconcile tick) must not contend.
    pub fn observe(&self, secs: f64) {
        let secs = if secs.is_finite() && secs > 0.0 { secs } else { 0.0 };
        match self.bounds.iter().position(|&b| secs <= b) {
            Some(i) => self.buckets[i].fetch_add(1, Ordering::Relaxed),
            None => self.overflow.fetch_add(1, Ordering::Relaxed),
        };
        self.sum_micros.fetch_add((secs * 1_000_000.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Append this histogram's series to `out` in the Prometheus text format.
    #[cfg(feature = "ops")]
    fn render_into(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0u64;
        for (i, &bound) in self.bounds.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        cumulative += self.overflow.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
        let sum = self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {}", self.count.load(Ordering::Relaxed));
    }
}

/// Point-in-time datastore connection-pool stats, read per `/metrics` scrape and
/// rendered as saturation gauges (point 5: "DB pool saturation").
#[cfg(feature = "ops")]
pub struct DbPoolStats {
    pub connections: u32,
    pub idle: u32,
    pub max: u32,
}

/// Finished-run totals for one workflow, split by outcome so a success ratio
/// and a mean duration per outcome both fall out of the same two series.
#[derive(Debug, Default, Clone)]
struct WorkflowRunStats {
    succeeded: u64,
    succeeded_secs: f64,
    failed: u64,
    failed_secs: f64,
    last_secs: f64,
    // Fractional, so two engines finishing the same workflow within one second
    // still order: dashboards pick the engine whose last run is newest.
    last_finished_unix: f64,
    last_succeeded: bool,
}

/// Monotonic process-lifetime counters. Cheap relaxed atomics — exactness across
/// threads is unnecessary for counters that only ever grow.
#[derive(Debug)]
pub struct Metrics {
    // Read only by the ops `/metrics` renderer; still set in a lean build.
    #[cfg_attr(not(feature = "ops"), allow(dead_code))]
    pub started: Instant,
    pub runs_created: AtomicU64,
    pub tasks_dispatched: AtomicU64,
    pub tasks_succeeded: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub tasks_retried: AtomicU64,
    pub dead_letters: AtomicU64,
    /// Runs failed by the run-level deadline sweep (spec `run_timeout_secs`).
    pub runs_deadline_exceeded: AtomicU64,
    /// Soft SLA deadline alerts emitted (spec `deadline`) — run kept running.
    pub deadline_alerts: AtomicU64,
    /// Tasks resolved from the memoization cache without executing (#22).
    pub cache_hits: AtomicU64,
    /// Remote jobs this engine gave up trying to tear down (`defer:`).
    ///
    /// The one number that says "a cancelled run left something running on
    /// someone's cluster and we stopped trying". It exists because the
    /// alternative to a visible leak is a silent one: teardown is best-effort
    /// by nature — a job we cannot reach is a job we cannot stop — and the
    /// honest response to that is a counter and a log line naming the handle,
    /// not a pretence that the cluster is idle.
    pub external_orphans: AtomicU64,
    /// Workloads (pods, containers) the fleet sweep deleted because the task
    /// that owned them was no longer live.
    ///
    /// Worth alerting on if it is anything but near-zero in steady state. Every
    /// increment is a workload that outlived its task, which means a scheduler
    /// died between creating it and finishing it — the failure the per-dispatch
    /// reap cannot see, because that reap only ever runs when the *same task*
    /// is dispatched again.
    pub orphan_workloads_reaped: AtomicU64,
    /// Failed attempts by [`crate::fault::FaultClass`], indexed by the class's
    /// position in `FaultClass::ALL`.
    ///
    /// A fixed array rather than a map because the taxonomy is closed and
    /// small (23): no lock, no allocation, and — the reason that matters — no
    /// unbounded label cardinality on a `/metrics` scrape, which is how a
    /// per-error-string counter takes down a Prometheus.
    ///
    /// A **count of failed attempts**, not GPU-hours. Costing the failures needs
    /// each job's GPU count and elapsed time, which this counter does not carry
    /// — that is `JobAutopsy::gpu_hours_lost` on the record side, and a fleet
    /// ledger on the aggregate side. What this answers is the question that
    /// comes before the cost one: *what is breaking, and is it ours or the
    /// hardware's.*
    pub task_faults: [AtomicU64; crate::fault::FaultClass::ALL.len()],
    /// Schedule fires skipped by a `when:` gate evaluating false.
    pub schedule_gated: AtomicU64,
    /// Schedules auto-stopped by a `stopStrategy` expression.
    pub schedules_stopped: AtomicU64,
    /// Dataset updates recorded (`produces:` successes + external events).
    pub dataset_updates: AtomicU64,
    /// Runs fired by dataset triggers (`on_datasets:`).
    pub dataset_fires: AtomicU64,
    // ── Constrained-host gates + clock discipline (the edge profile) ────────
    /// Runs refused by the SQLite free-disk floor (`DAGRON_MIN_FREE_BYTES`).
    /// Bumped by whichever admission path caught the typed refusal — the
    /// ingest actor's nack, the ops API's 507 — since the datastore that
    /// raises it holds no metrics handle.
    pub admission_refused_disk: AtomicU64,
    /// Runs refused because DAGRON_ADMISSION_FILE was closed.
    pub admission_refused_gate: AtomicU64,
    /// `1` while the pressure file (`DAGRON_PRESSURE_FILE`) is holding new
    /// claims at zero, else `0`. A state gauge like the catch-up gauges:
    /// the reconcile loop is the single writer, re-publishing its verdict
    /// every tick.
    pub claims_paused: AtomicU64,
    /// Wall-clock steps the clock detector caught — wall and monotonic
    /// clocks disagreeing over one interval by more than
    /// `DAGRON_CLOCK_STEP_TOLERANCE_MS`. Each one re-stamped the runs in
    /// flight `drifted`.
    pub clock_steps: AtomicU64,
    /// Runs created by the auto-backfill catch-up sweep (QW3 auto-catchup). A schedule that
    /// missed fires while the scheduler was down has them materialized here.
    #[cfg(feature = "enterprise")]
    pub catchup_runs: AtomicU64,
    /// Terminally-failed runs the self-healing loop re-armed from their failure
    /// frontier (QW3-catchup auto-rerun of incomplete workflows).
    #[cfg(feature = "enterprise")]
    pub auto_reruns: AtomicU64,
    /// Task wall-time (claim→finish), the headroom-dominating signal real ETL
    /// tasks have but the no-op load test never exercised.
    pub task_duration: Histogram,
    /// Reconcile-loop tick duration — the CPU-pegging signal load testing surfaced.
    pub reconcile_tick: Histogram,
    // ── Dispatch-path latency (docs/LOW_LATENCY.md A-5) ─────────────────────
    /// Became-claimable → handed to a worker. `scheduled_at` is stamped when a
    /// task flips `pending → ready` (and set to the due time on retries), so
    /// this is the scheduler's own queueing latency: claim wait + tick pacing +
    /// per-task dispatch prep. The SLO metric for the low-latency profile.
    pub dispatch_latency: Histogram,
    /// Executor finished → reconcile loop drained the result. Measures the
    /// wake-on-completion path directly: before it existed this sat at
    /// "remainder of the poll interval" (up to 500 ms) for every task.
    pub result_wait: Histogram,
    /// Tasks claimed per non-empty claim call — batch-shape signal for tuning
    /// `WORKER_COUNT` against the ready backlog. Unit: tasks, not seconds.
    pub claim_batch: Histogram,
    /// Whole-run wall time (created→finished) for runs this engine finalized.
    pub run_duration: Histogram,
    /// Per-workflow finished-run totals and the last run's duration. A lock,
    /// not atomics: it is taken once per finished *run*, never per task.
    workflow_runs: std::sync::Mutex<std::collections::BTreeMap<String, WorkflowRunStats>>,
    // ── QW3 auto-catchup self-healing state gauges ──────────────────────────────────
    // Unlike the counters above (monotonic, bumped on the hot path) these are
    // *current state* re-published by the auto-backfill loop on every sweep: the
    // loop is the single writer, `render` reads the last value. They are the
    // signals an alerting rule scrapes to *trigger* eventing (alert on lag →
    // webhook / redrive) — the "register workflow + data state as metrics" goal.
    /// Catch-up schedules whose oldest missed fire is still outstanding.
    #[cfg(feature = "enterprise")]
    pub overdue_schedules: AtomicU64,
    /// Largest catch-up lag, in seconds, across all catch-up schedules.
    #[cfg(feature = "enterprise")]
    pub schedule_lag_seconds: AtomicU64,
    /// Runs still `running` past the stall SLA (suspected-incomplete workflows).
    #[cfg(feature = "enterprise")]
    pub incomplete_runs: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            runs_created: AtomicU64::new(0),
            tasks_dispatched: AtomicU64::new(0),
            tasks_succeeded: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            tasks_retried: AtomicU64::new(0),
            dead_letters: AtomicU64::new(0),
            runs_deadline_exceeded: AtomicU64::new(0),
            deadline_alerts: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            external_orphans: AtomicU64::new(0),
            orphan_workloads_reaped: AtomicU64::new(0),
            task_faults: std::array::from_fn(|_| AtomicU64::new(0)),
            schedule_gated: AtomicU64::new(0),
            schedules_stopped: AtomicU64::new(0),
            dataset_updates: AtomicU64::new(0),
            dataset_fires: AtomicU64::new(0),
            admission_refused_disk: AtomicU64::new(0),
            admission_refused_gate: AtomicU64::new(0),
            claims_paused: AtomicU64::new(0),
            clock_steps: AtomicU64::new(0),
            #[cfg(feature = "enterprise")]
            catchup_runs: AtomicU64::new(0),
            #[cfg(feature = "enterprise")]
            auto_reruns: AtomicU64::new(0),
            task_duration: Histogram::new(DURATION_BUCKETS),
            reconcile_tick: Histogram::new(DURATION_BUCKETS),
            dispatch_latency: Histogram::new(DURATION_BUCKETS),
            result_wait: Histogram::new(DURATION_BUCKETS),
            claim_batch: Histogram::new(BATCH_BUCKETS),
            run_duration: Histogram::new(RUN_DURATION_BUCKETS),
            workflow_runs: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            #[cfg(feature = "enterprise")]
            overdue_schedules: AtomicU64::new(0),
            #[cfg(feature = "enterprise")]
            schedule_lag_seconds: AtomicU64::new(0),
            #[cfg(feature = "enterprise")]
            incomplete_runs: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_runs_created(&self) {
        Self::bump(&self.runs_created);
    }
    pub fn inc_dispatched(&self) {
        Self::bump(&self.tasks_dispatched);
    }
    pub fn inc_succeeded(&self) {
        Self::bump(&self.tasks_succeeded);
    }
    pub fn inc_failed(&self) {
        Self::bump(&self.tasks_failed);
    }
    pub fn inc_retried(&self) {
        Self::bump(&self.tasks_retried);
    }
    /// One failed attempt attributed to `class`. Counted whether or not the
    /// attempt is retried — the question "what is breaking" is asked separately
    /// from "what did we give up on".
    pub fn inc_fault(&self, class: crate::fault::FaultClass) {
        if let Some(i) = crate::fault::FaultClass::ALL.iter().position(|c| *c == class) {
            Self::bump(&self.task_faults[i]);
        }
    }
    pub fn inc_dead_letters(&self) {
        Self::bump(&self.dead_letters);
    }
    /// One run failed by the run-level deadline sweep (spec `run_timeout_secs`).
    pub fn inc_runs_deadline_exceeded(&self) {
        Self::bump(&self.runs_deadline_exceeded);
    }
    /// One soft SLA deadline alert emitted (spec `deadline`).
    pub fn inc_deadline_alerts(&self) {
        Self::bump(&self.deadline_alerts);
    }
    /// One schedule fire skipped by a `when:` gate.
    pub fn inc_schedule_gated(&self) {
        Self::bump(&self.schedule_gated);
    }
    pub fn inc_cache_hits(&self) {
        Self::bump(&self.cache_hits);
    }
    /// One remote job abandoned: teardown was owed and could not be delivered.
    pub fn inc_external_orphans(&self) {
        Self::bump(&self.external_orphans);
    }
    /// One leftover workload deleted by the fleet sweep.
    pub fn inc_orphan_workloads_reaped(&self) {
        Self::bump(&self.orphan_workloads_reaped);
    }
    /// One schedule auto-stopped by a `stopStrategy` expression.
    pub fn inc_schedules_stopped(&self) {
        Self::bump(&self.schedules_stopped);
    }
    /// One dataset update recorded (a `produces:` success or an external event).
    pub fn inc_dataset_updates(&self) {
        Self::bump(&self.dataset_updates);
    }
    /// One run fired by a dataset trigger (`on_datasets:`).
    pub fn inc_dataset_fires(&self) {
        Self::bump(&self.dataset_fires);
    }
    /// One run refused because the admission gate (`DAGRON_ADMISSION_FILE`)
    /// was closed or unreadable.
    pub fn inc_admission_refused_gate(&self) {
        Self::bump(&self.admission_refused_gate);
    }
    /// One run refused by the free-disk floor (`DAGRON_MIN_FREE_BYTES`).
    pub fn inc_admission_refused_disk(&self) {
        Self::bump(&self.admission_refused_disk);
    }
    /// Re-publish the pressure gate's verdict for this tick (the reconcile
    /// loop is the single writer).
    pub fn set_claims_paused(&self, paused: bool) {
        self.claims_paused.store(u64::from(paused), Ordering::Relaxed);
    }
    /// One wall-clock step caught by the clock detector.
    pub fn inc_clock_steps(&self) {
        Self::bump(&self.clock_steps);
    }
    /// One run materialized by the auto-backfill catch-up sweep (QW3 auto-catchup).
    #[cfg(feature = "enterprise")]
    pub fn inc_catchup_runs(&self) {
        Self::bump(&self.catchup_runs);
    }
    /// One failed run re-armed by the self-healing auto-rerun loop (QW3 auto-catchup).
    #[cfg(feature = "enterprise")]
    pub fn inc_auto_reruns(&self) {
        Self::bump(&self.auto_reruns);
    }

    /// Re-publish the QW3 auto-catchup self-healing state gauges. Called once per sweep by the
    /// auto-backfill loop (the single writer); `render` reads these back. Storing
    /// them as atomics — rather than re-querying the DB per `/metrics` scrape —
    /// keeps the scrape cheap and decouples the alerting signal from scrape timing.
    #[cfg(feature = "enterprise")]
    pub fn set_backfill_state(&self, overdue_schedules: u64, max_lag_secs: u64, incomplete_runs: u64) {
        self.overdue_schedules.store(overdue_schedules, Ordering::Relaxed);
        self.schedule_lag_seconds.store(max_lag_secs, Ordering::Relaxed);
        self.incomplete_runs.store(incomplete_runs, Ordering::Relaxed);
    }

    /// Record a completed task's wall time (claim→finish), in seconds.
    pub fn observe_task_duration(&self, secs: f64) {
        self.task_duration.observe(secs);
    }

    /// Record one reconcile-loop tick duration, in seconds.
    pub fn observe_reconcile_tick(&self, secs: f64) {
        self.reconcile_tick.observe(secs);
    }

    /// Record one task's became-claimable → dispatched latency, in seconds.
    pub fn observe_dispatch_latency(&self, secs: f64) {
        self.dispatch_latency.observe(secs);
    }

    /// Record one result's executor-finished → drained-by-the-loop wait, in seconds.
    pub fn observe_result_wait(&self, secs: f64) {
        self.result_wait.observe(secs);
    }

    /// Record the size of one non-empty claim batch (unit: tasks).
    pub fn observe_claim_batch(&self, claimed: usize) {
        self.claim_batch.observe(claimed as f64);
    }

    /// Record one run this engine finalized: its workflow, whether it
    /// succeeded, and its wall time from creation to the terminal state.
    ///
    /// Only the first [`WORKFLOW_SERIES_CAP`] distinct workflow names get their
    /// own entry; later ones are counted under `other`.
    pub fn observe_run_finished(&self, workflow: &str, succeeded: bool, secs: f64) {
        let secs = if secs.is_finite() && secs > 0.0 { secs } else { 0.0 };
        self.run_duration.observe(secs);
        // A panic while holding this lock leaves counts, not invariants.
        let mut map = self.workflow_runs.lock().unwrap_or_else(|e| e.into_inner());
        let key = if map.contains_key(workflow) || map.len() < WORKFLOW_SERIES_CAP {
            workflow
        } else {
            OTHER
        };
        let w = map.entry(key.to_string()).or_default();
        if succeeded {
            w.succeeded += 1;
            w.succeeded_secs += secs;
        } else {
            w.failed += 1;
            w.failed_secs += secs;
        }
        w.last_secs = secs;
        w.last_finished_unix = chrono::Utc::now().timestamp_millis() as f64 / 1000.0;
        w.last_succeeded = succeeded;
    }

    /// Render the Prometheus text exposition format (version 0.0.4) for the
    /// process counters plus the datastore gauges in `snap`. Gated to the `ops`
    /// feature — the only caller is the management API's `/metrics` endpoint.
    ///
    /// Per-class series are capped at [`READY_CLASS_SERIES_CAP`]; see the
    /// ready-by-class block below.
    ///
    /// `pool` is the live datastore connection-pool saturation, read per scrape.
    #[cfg(feature = "ops")]
    pub fn render(&self, snap: &MetricsSnapshot, pool: Option<&DbPoolStats>) -> String {
        let mut out = String::with_capacity(2048);

        let counters: [(&str, &str, u64); 18] = [
            ("scheduler_runs_created_total", "Runs created by this scheduler since boot.",
             self.runs_created.load(Ordering::Relaxed)),
            ("scheduler_tasks_dispatched_total", "Tasks dispatched to the worker pool.",
             self.tasks_dispatched.load(Ordering::Relaxed)),
            ("scheduler_tasks_succeeded_total", "Tasks that completed successfully.",
             self.tasks_succeeded.load(Ordering::Relaxed)),
            ("scheduler_tasks_failed_total", "Tasks that exhausted retries and failed.",
             self.tasks_failed.load(Ordering::Relaxed)),
            ("scheduler_tasks_retried_total", "Task attempts rescheduled for retry.",
             self.tasks_retried.load(Ordering::Relaxed)),
            ("scheduler_dead_letters_total", "Poison submissions parked in the dead-letter store.",
             self.dead_letters.load(Ordering::Relaxed)),
            ("scheduler_runs_deadline_exceeded_total", "Runs failed by the run-level deadline sweep (run_timeout_secs).",
             self.runs_deadline_exceeded.load(Ordering::Relaxed)),
            ("scheduler_deadline_alerts_total", "Soft SLA deadline alerts emitted (deadline).",
             self.deadline_alerts.load(Ordering::Relaxed)),
            ("scheduler_cache_hits_total", "Tasks resolved from the memoization cache without executing.",
             self.cache_hits.load(Ordering::Relaxed)),
            ("scheduler_external_orphans_total", "Remote jobs (defer:) this engine gave up tearing down — each one may still be running and consuming cluster-hours.",
             self.external_orphans.load(Ordering::Relaxed)),
            ("scheduler_orphan_workloads_reaped_total", "Pods/containers deleted by the fleet sweep because the task that owned them was no longer live — each one outlived the scheduler that created it.",
             self.orphan_workloads_reaped.load(Ordering::Relaxed)),
            ("scheduler_schedule_gated_total", "Schedule fires skipped by a when: gate.",
             self.schedule_gated.load(Ordering::Relaxed)),
            ("scheduler_schedules_stopped_total", "Schedules auto-stopped by a stopStrategy expression.",
             self.schedules_stopped.load(Ordering::Relaxed)),
            ("scheduler_dataset_updates_total", "Dataset updates recorded (produces: successes + external events).",
             self.dataset_updates.load(Ordering::Relaxed)),
            ("scheduler_dataset_fires_total", "Runs fired by dataset triggers (on_datasets:).",
             self.dataset_fires.load(Ordering::Relaxed)),
            ("scheduler_admission_refused_disk_total", "Runs refused by the free-disk floor (DAGRON_MIN_FREE_BYTES).",
             self.admission_refused_disk.load(Ordering::Relaxed)),
            ("scheduler_admission_refused_gate_total", "Runs refused because the admission gate (DAGRON_ADMISSION_FILE) was closed.",
             self.admission_refused_gate.load(Ordering::Relaxed)),
            ("scheduler_clock_steps_total", "Wall-clock steps caught by the clock detector (wall vs monotonic past DAGRON_CLOCK_STEP_TOLERANCE_MS).",
             self.clock_steps.load(Ordering::Relaxed)),
        ];
        for (name, help, value) in counters {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        }
        #[cfg(feature = "enterprise")]
        {
            let ee_counters: [(&str, &str, u64); 2] = [
                ("scheduler_catchup_runs_total", "Runs materialized by the auto-backfill catch-up sweep.",
                 self.catchup_runs.load(Ordering::Relaxed)),
                ("scheduler_auto_reruns_total", "Failed runs re-armed by the self-healing auto-rerun loop.",
                 self.auto_reruns.load(Ordering::Relaxed)),
            ];
            for (name, help, value) in ee_counters {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} counter");
                let _ = writeln!(out, "{name} {value}");
            }
        }

        // Fault attribution. Two series off one array: the class (what broke)
        // and the disposition (whether another attempt was worth anything).
        // The disposition roll-up is emitted rather than left to a recording
        // rule because it is the series the cost dashboard actually plots, and
        // a `sum by` over 23 classes is exactly the kind of mapping that drifts
        // from the code that defines it.
        //
        // Zero-valued classes are emitted too: a counter that only appears
        // after the first occurrence cannot be alerted on with `increase()`
        // over a window that starts before it existed.
        let _ = writeln!(out, "# HELP scheduler_task_faults_total Failed task attempts by attributed fault class.");
        let _ = writeln!(out, "# TYPE scheduler_task_faults_total counter");
        let mut by_disposition: std::collections::BTreeMap<&'static str, u64> =
            std::collections::BTreeMap::new();
        for (i, class) in crate::fault::FaultClass::ALL.iter().enumerate() {
            let v = self.task_faults[i].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "scheduler_task_faults_total{{class=\"{}\",disposition=\"{}\"}} {v}",
                class.as_str(),
                class.disposition().as_str()
            );
            *by_disposition.entry(class.disposition().as_str()).or_default() += v;
        }
        let _ = writeln!(out, "# HELP scheduler_task_faults_by_disposition_total Failed task attempts by fault disposition (infrastructure / application / platform / unknown).");
        let _ = writeln!(out, "# TYPE scheduler_task_faults_by_disposition_total counter");
        for (disposition, v) in &by_disposition {
            let _ = writeln!(
                out,
                "scheduler_task_faults_by_disposition_total{{disposition=\"{disposition}\"}} {v}"
            );
        }

        // Requests refused with a signpost, by gate. Every gate is emitted, at
        // zero too, for the same reason as the fault classes above.
        let _ = writeln!(out, "# HELP scheduler_signpost_hits_total Requests this build refused with a signpost to what it does not include, by gate.");
        let _ = writeln!(out, "# TYPE scheduler_signpost_hits_total counter");
        for gate in SignpostGate::ALL {
            let _ = writeln!(
                out,
                "scheduler_signpost_hits_total{{gate=\"{}\"}} {}",
                gate.as_str(),
                signpost_hits(gate)
            );
        }

        // Datastore gauges (whole-cluster truth, read per scrape).
        let _ = writeln!(out, "# HELP scheduler_runs Workflow runs grouped by status.");
        let _ = writeln!(out, "# TYPE scheduler_runs gauge");
        for (status, count) in &snap.runs_by_status {
            let _ = writeln!(out, "scheduler_runs{{status=\"{status}\"}} {count}");
        }
        let _ = writeln!(out, "# HELP scheduler_tasks Task runs grouped by status.");
        let _ = writeln!(out, "# TYPE scheduler_tasks gauge");
        let mut queue_depth: i64 = 0;
        for (status, count) in &snap.tasks_by_status {
            let _ = writeln!(out, "scheduler_tasks{{status=\"{status}\"}} {count}");
            // "ready" tasks are the dispatch backlog — surfaced below as a
            // first-class queue-depth gauge (point 5).
            if status == "ready" {
                queue_depth = *count;
            }
        }

        // Queue depth as a first-class gauge: the backlog whose growth rate the
        // recommended alert rules watch.
        let _ = writeln!(out, "# HELP scheduler_queue_depth Ready tasks awaiting dispatch (backlog).");
        let _ = writeln!(out, "# TYPE scheduler_queue_depth gauge");
        let _ = writeln!(out, "scheduler_queue_depth {queue_depth}");

        // Per-runner-class backlog (runner segmentation). The age gauge is the
        // unclaimable-class alarm signal: a class no live scheduler serves
        // (every pool restricted away from it) only ever grows here.
        //
        // Cardinality is bounded even though `runner_class` comes from
        // workflow specs (a submitter could mint a new class per run): only
        // the READY_CLASS_SERIES_CAP busiest classes get their own series; the
        // tail is folded into runner_class="other" (count summed, age = the
        // tail's max, so an unserved class still raises the alarm from inside
        // the bucket).
        if !snap.ready_by_class.is_empty() {
            let now = chrono::Utc::now();
            let mut classes: Vec<_> = snap.ready_by_class.iter().collect();
            classes.sort_by(|a, b| b.count.cmp(&a.count).then(a.runner_class.cmp(&b.runner_class)));
            let (head, tail) = classes.split_at(classes.len().min(READY_CLASS_SERIES_CAP));
            let tail_count: i64 = tail.iter().map(|b| b.count).sum();
            let tail_age: i64 = tail.iter().map(|b| b.oldest_age_secs(now)).max().unwrap_or(0);

            let _ = writeln!(out, "# HELP scheduler_ready_tasks_by_class Ready tasks awaiting dispatch, per runner class (top classes; tail bucketed as \"other\").");
            let _ = writeln!(out, "# TYPE scheduler_ready_tasks_by_class gauge");
            for b in head {
                let _ = writeln!(out, "scheduler_ready_tasks_by_class{{runner_class=\"{}\"}} {}", b.runner_class, b.count);
            }
            if !tail.is_empty() {
                let _ = writeln!(out, "scheduler_ready_tasks_by_class{{runner_class=\"other\"}} {tail_count}");
            }
            let _ = writeln!(out, "# HELP scheduler_ready_oldest_age_seconds Age of the oldest ready task, per runner class (top classes; tail bucketed as \"other\").");
            let _ = writeln!(out, "# TYPE scheduler_ready_oldest_age_seconds gauge");
            for b in head {
                let _ = writeln!(out, "scheduler_ready_oldest_age_seconds{{runner_class=\"{}\"}} {}", b.runner_class, b.oldest_age_secs(now));
            }
            if !tail.is_empty() {
                let _ = writeln!(out, "scheduler_ready_oldest_age_seconds{{runner_class=\"other\"}} {tail_age}");
            }
        }

        let _ = writeln!(out, "# HELP scheduler_dead_letters Dead-letter rows currently parked.");
        let _ = writeln!(out, "# TYPE scheduler_dead_letters gauge");
        let _ = writeln!(out, "scheduler_dead_letters {}", snap.dead_letters);

        // Constrained-host gate + clock discipline (the edge profile). The
        // pause gauge is the reconcile loop's last verdict on the pressure
        // file; the confidence gauge is read live from `crate::clock` — its
        // one writer is the engine's detector — so a scrape can never lag a
        // published change.
        let _ = writeln!(out, "# HELP scheduler_claims_paused 1 while DAGRON_PRESSURE_FILE is holding new task claims at zero, else 0.");
        let _ = writeln!(out, "# TYPE scheduler_claims_paused gauge");
        let _ = writeln!(out, "scheduler_claims_paused {}", self.claims_paused.load(Ordering::Relaxed));
        let _ = writeln!(out, "# HELP scheduler_clock_confidence Wall-clock confidence stamped on new runs: 0 synced, 1 drifted, 2 unknown (alert on > 0).");
        let _ = writeln!(out, "# TYPE scheduler_clock_confidence gauge");
        let _ = writeln!(out, "scheduler_clock_confidence {}", crate::clock::current().confidence.gauge());

        // QW3 auto-catchup self-healing state gauges — republished by the
        // auto-backfill loop each sweep. Alerting on `scheduler_schedule_lag_seconds`
        // or `scheduler_incomplete_runs` is the intended trigger for downstream
        // eventing (redrive, page, webhook).
        #[cfg(feature = "enterprise")]
        {
            let _ = writeln!(out, "# HELP scheduler_overdue_schedules Catch-up schedules with an outstanding missed fire.");
            let _ = writeln!(out, "# TYPE scheduler_overdue_schedules gauge");
            let _ = writeln!(out, "scheduler_overdue_schedules {}", self.overdue_schedules.load(Ordering::Relaxed));
            let _ = writeln!(out, "# HELP scheduler_schedule_lag_seconds Largest catch-up lag across schedules (oldest outstanding miss).");
            let _ = writeln!(out, "# TYPE scheduler_schedule_lag_seconds gauge");
            let _ = writeln!(out, "scheduler_schedule_lag_seconds {}", self.schedule_lag_seconds.load(Ordering::Relaxed));
            let _ = writeln!(out, "# HELP scheduler_incomplete_runs Runs still running past the stall SLA (suspected incomplete).");
            let _ = writeln!(out, "# TYPE scheduler_incomplete_runs gauge");
            let _ = writeln!(out, "scheduler_incomplete_runs {}", self.incomplete_runs.load(Ordering::Relaxed));
        }

        let _ = writeln!(out, "# HELP scheduler_uptime_seconds Seconds since this scheduler booted.");
        let _ = writeln!(out, "# TYPE scheduler_uptime_seconds gauge");
        let _ = writeln!(out, "scheduler_uptime_seconds {}", self.started.elapsed().as_secs());

        // Latency histograms — the workload signals the no-op load test lacked.
        self.task_duration.render_into(
            &mut out,
            "scheduler_task_duration_seconds",
            "Task wall time from claim to finish.",
        );
        self.reconcile_tick.render_into(
            &mut out,
            "scheduler_reconcile_tick_seconds",
            "Reconcile-loop tick duration (recover→advance→dispatch→collect→reap).",
        );
        self.dispatch_latency.render_into(
            &mut out,
            "scheduler_dispatch_latency_seconds",
            "Task became-claimable (scheduled_at) to handed-to-a-worker.",
        );
        self.result_wait.render_into(
            &mut out,
            "scheduler_result_wait_seconds",
            "Executor finished to result drained by the reconcile loop.",
        );
        self.claim_batch.render_into(
            &mut out,
            "scheduler_claim_batch_size",
            "Tasks claimed per non-empty claim call (unit: tasks).",
        );

        self.run_duration.render_into(
            &mut out,
            "scheduler_run_duration_seconds",
            "Run wall time from creation to a terminal state, for runs this engine finalized.",
        );

        // Per-workflow finished runs. A summary with no quantiles: the sum and
        // the count are what a mean and a success ratio need, and a histogram
        // per workflow would be buckets × workflows series.
        {
            let map = self.workflow_runs.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let name = "scheduler_workflow_run_duration_seconds";
            let _ = writeln!(out, "# HELP {name} Run wall time by workflow and outcome, for runs this engine finalized (sum and count only).");
            let _ = writeln!(out, "# TYPE {name} summary");
            for (workflow, w) in &map {
                let wf = escape_label(workflow);
                for (status, count, secs) in [
                    ("succeeded", w.succeeded, w.succeeded_secs),
                    ("failed", w.failed, w.failed_secs),
                ] {
                    let _ = writeln!(out, "{name}_sum{{workflow=\"{wf}\",status=\"{status}\"}} {secs}");
                    let _ = writeln!(out, "{name}_count{{workflow=\"{wf}\",status=\"{status}\"}} {count}");
                }
            }
            let _ = writeln!(out, "# HELP scheduler_workflow_last_run_duration_seconds Wall time of the workflow's most recently finished run.");
            let _ = writeln!(out, "# TYPE scheduler_workflow_last_run_duration_seconds gauge");
            for (workflow, w) in &map {
                let _ = writeln!(out, "scheduler_workflow_last_run_duration_seconds{{workflow=\"{}\"}} {}", escape_label(workflow), w.last_secs);
            }
            let _ = writeln!(out, "# HELP scheduler_workflow_last_run_finished_timestamp_seconds Unix time the workflow's most recent run finished.");
            let _ = writeln!(out, "# TYPE scheduler_workflow_last_run_finished_timestamp_seconds gauge");
            for (workflow, w) in &map {
                let _ = writeln!(out, "scheduler_workflow_last_run_finished_timestamp_seconds{{workflow=\"{}\"}} {}", escape_label(workflow), w.last_finished_unix);
            }
            let _ = writeln!(out, "# HELP scheduler_workflow_last_run_success 1 if the workflow's most recently finished run succeeded, 0 if it failed.");
            let _ = writeln!(out, "# TYPE scheduler_workflow_last_run_success gauge");
            for (workflow, w) in &map {
                let _ = writeln!(out, "scheduler_workflow_last_run_success{{workflow=\"{}\"}} {}", escape_label(workflow), u8::from(w.last_succeeded));
            }
        }

        // Datastore views by workflow and environment (whole-cluster truth).
        {
            use std::collections::BTreeMap;
            let mut by_workflow: BTreeMap<(String, String), i64> = BTreeMap::new();
            let mut by_env: BTreeMap<(String, String), i64> = BTreeMap::new();
            for r in &snap.recent_runs {
                *by_workflow.entry((r.workflow.clone(), r.status.clone())).or_default() += r.count;
                // Parentheses cannot appear in an environment name, so the
                // placeholder can never collide with a real one.
                let env = r.environment.clone().unwrap_or_else(|| "(none)".to_string());
                *by_env.entry((env, r.status.clone())).or_default() += r.count;
            }
            let _ = writeln!(out, "# HELP scheduler_workflow_recent_runs Runs created in the last 24 hours, by workflow and current status (busiest workflows; the rest summed as \"other\").");
            let _ = writeln!(out, "# TYPE scheduler_workflow_recent_runs gauge");
            for ((workflow, status), count) in fold_tail(by_workflow, WORKFLOW_SERIES_CAP) {
                let _ = writeln!(out, "scheduler_workflow_recent_runs{{workflow=\"{}\",status=\"{}\"}} {count}", escape_label(&workflow), escape_label(&status));
            }
            let _ = writeln!(out, "# HELP scheduler_environment_recent_runs Runs created in the last 24 hours, by environment and current status.");
            let _ = writeln!(out, "# TYPE scheduler_environment_recent_runs gauge");
            for ((env, status), count) in fold_tail(by_env, SMALL_SERIES_CAP) {
                let _ = writeln!(out, "scheduler_environment_recent_runs{{environment=\"{}\",status=\"{}\"}} {count}", escape_label(&env), escape_label(&status));
            }

            let mut tasks: BTreeMap<(String, String), i64> = BTreeMap::new();
            for (workflow, status, count) in &snap.active_tasks {
                *tasks.entry((workflow.clone(), status.clone())).or_default() += count;
            }
            let _ = writeln!(out, "# HELP scheduler_workflow_active_tasks Tasks that have not finished, by workflow and status (pending, ready, running, awaiting_approval).");
            let _ = writeln!(out, "# TYPE scheduler_workflow_active_tasks gauge");
            for ((workflow, status), count) in fold_tail(tasks, WORKFLOW_SERIES_CAP) {
                let _ = writeln!(out, "scheduler_workflow_active_tasks{{workflow=\"{}\",status=\"{}\"}} {count}", escape_label(&workflow), escape_label(&status));
            }

            // Dead letters by source. The age is the tail's maximum, so a
            // source folded into "other" still raises the alarm from there.
            let now = chrono::Utc::now();
            let mut sources: Vec<_> = snap.dead_letters_by_source.iter().collect();
            sources.sort_by(|a, b| b.count.cmp(&a.count).then(a.source.cmp(&b.source)));
            let (head, tail) = sources.split_at(sources.len().min(SMALL_SERIES_CAP));
            let _ = writeln!(out, "# HELP scheduler_dead_letters_by_source Dead-letter rows currently parked, by the source that produced them.");
            let _ = writeln!(out, "# TYPE scheduler_dead_letters_by_source gauge");
            for d in head {
                let _ = writeln!(out, "scheduler_dead_letters_by_source{{source=\"{}\"}} {}", escape_label(&d.source), d.count);
            }
            if !tail.is_empty() {
                let _ = writeln!(out, "scheduler_dead_letters_by_source{{source=\"{OTHER}\"}} {}", tail.iter().map(|d| d.count).sum::<i64>());
            }
            let _ = writeln!(out, "# HELP scheduler_dead_letters_oldest_age_seconds Age of the oldest parked dead letter, by source.");
            let _ = writeln!(out, "# TYPE scheduler_dead_letters_oldest_age_seconds gauge");
            for d in head {
                let _ = writeln!(out, "scheduler_dead_letters_oldest_age_seconds{{source=\"{}\"}} {}", escape_label(&d.source), d.oldest_age_secs(now));
            }
            if !tail.is_empty() {
                let _ = writeln!(out, "scheduler_dead_letters_oldest_age_seconds{{source=\"{OTHER}\"}} {}", tail.iter().map(|d| d.oldest_age_secs(now)).max().unwrap_or(0));
            }
        }

        // The process itself. `process_*` follows the names every
        // Prometheus client library uses, so stock process dashboards and
        // alerts work unchanged; it is read from /proc and so is Linux-only.
        if let Some(p) = process::read() {
            p.render_into(&mut out);
        }

        // DB connection-pool saturation (in-use / idle / max).
        if let Some(p) = pool {
            let in_use = p.connections.saturating_sub(p.idle);
            let _ = writeln!(out, "# HELP scheduler_db_pool_connections Open datastore connections.");
            let _ = writeln!(out, "# TYPE scheduler_db_pool_connections gauge");
            let _ = writeln!(out, "scheduler_db_pool_connections {}", p.connections);
            let _ = writeln!(out, "# HELP scheduler_db_pool_in_use Datastore connections currently checked out.");
            let _ = writeln!(out, "# TYPE scheduler_db_pool_in_use gauge");
            let _ = writeln!(out, "scheduler_db_pool_in_use {in_use}");
            let _ = writeln!(out, "# HELP scheduler_db_pool_max Configured datastore pool ceiling.");
            let _ = writeln!(out, "# TYPE scheduler_db_pool_max gauge");
            let _ = writeln!(out, "scheduler_db_pool_max {}", p.max);
        }

        out
    }
}

/// Escape a label value for the text exposition format: backslash, double
/// quote and newline are the three characters it gives meaning to.
#[cfg(feature = "ops")]
fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// Keep the `cap` keys with the largest totals and sum every other key's cells
/// into [`OTHER`]. Cells are `(key, status) → count`; ties break by name so the
/// set of series is stable from one scrape to the next.
#[cfg(feature = "ops")]
fn fold_tail(
    cells: std::collections::BTreeMap<(String, String), i64>,
    cap: usize,
) -> std::collections::BTreeMap<(String, String), i64> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut totals: BTreeMap<&str, i64> = BTreeMap::new();
    for ((key, _), count) in &cells {
        *totals.entry(key.as_str()).or_default() += count;
    }
    if totals.len() <= cap {
        return cells;
    }
    let mut ranked: Vec<(&str, i64)> = totals.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let keep: BTreeSet<String> = ranked.iter().take(cap).map(|(k, _)| k.to_string()).collect();
    let mut out: BTreeMap<(String, String), i64> = BTreeMap::new();
    for ((key, status), count) in cells {
        let key = if keep.contains(&key) { key } else { OTHER.to_string() };
        *out.entry((key, status)).or_default() += count;
    }
    out
}

/// The engine process's own resource use, in the standard `process_*` names.
#[cfg(feature = "ops")]
mod process {
    use std::fmt::Write as _;

    #[derive(Debug, PartialEq)]
    pub(super) struct ProcessStats {
        pub cpu_seconds: f64,
        pub resident_bytes: u64,
        pub virtual_bytes: u64,
        pub threads: u64,
        pub start_time_seconds: f64,
        pub open_fds: Option<u64>,
        pub max_fds: Option<u64>,
    }

    /// Parse `/proc/<pid>/stat`. The command name is the one field that may
    /// contain spaces or parentheses, so fields are counted from the *last*
    /// `)`; `ticks` is the kernel's clock ticks per second, `page` its page
    /// size, and `boot_time` the Unix time the host booted.
    #[cfg_attr(not(any(test, target_os = "linux")), allow(dead_code))]
    pub(super) fn parse_stat(stat: &str, ticks: f64, page: u64, boot_time: f64) -> Option<ProcessStats> {
        let rest = &stat[stat.rfind(')')? + 1..];
        // `rest` starts at field 3 (state), so field N is at index N - 3.
        let f: Vec<&str> = rest.split_whitespace().collect();
        let num = |field: usize| f.get(field - 3)?.parse::<u64>().ok();
        Some(ProcessStats {
            cpu_seconds: (num(14)? + num(15)?) as f64 / ticks,
            threads: num(20)?,
            start_time_seconds: boot_time + num(22)? as f64 / ticks,
            virtual_bytes: num(23)?,
            resident_bytes: num(24)? * page,
            open_fds: None,
            max_fds: None,
        })
    }

    #[cfg(target_os = "linux")]
    pub(super) fn read() -> Option<ProcessStats> {
        // SAFETY: sysconf takes no pointers and has no preconditions.
        let (ticks, page) = unsafe { (libc::sysconf(libc::_SC_CLK_TCK), libc::sysconf(libc::_SC_PAGESIZE)) };
        if ticks <= 0 || page <= 0 {
            return None;
        }
        let boot_time = std::fs::read_to_string("/proc/stat")
            .ok()?
            .lines()
            .find_map(|l| l.strip_prefix("btime "))?
            .trim()
            .parse::<f64>()
            .ok()?;
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        let mut p = parse_stat(&stat, ticks as f64, page as u64, boot_time)?;
        p.open_fds = std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count() as u64);
        p.max_fds = std::fs::read_to_string("/proc/self/limits").ok().and_then(|l| {
            l.lines()
                .find_map(|l| l.strip_prefix("Max open files"))?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        });
        Some(p)
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn read() -> Option<ProcessStats> {
        None
    }

    impl ProcessStats {
        pub(super) fn render_into(&self, out: &mut String) {
            let mut one = |name: &str, kind: &str, help: &str, value: String| {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} {kind}");
                let _ = writeln!(out, "{name} {value}");
            };
            one("process_cpu_seconds_total", "counter", "Total user and system CPU time spent by the engine process, in seconds.", self.cpu_seconds.to_string());
            one("process_resident_memory_bytes", "gauge", "Resident memory of the engine process, in bytes.", self.resident_bytes.to_string());
            one("process_virtual_memory_bytes", "gauge", "Virtual memory of the engine process, in bytes.", self.virtual_bytes.to_string());
            one("process_threads", "gauge", "Operating-system threads in the engine process.", self.threads.to_string());
            one("process_start_time_seconds", "gauge", "Unix time the engine process started.", self.start_time_seconds.to_string());
            if let Some(n) = self.open_fds {
                one("process_open_fds", "gauge", "File descriptors the engine process has open.", n.to_string());
            }
            if let Some(n) = self.max_fds {
                one("process_max_fds", "gauge", "The engine process's open file descriptor limit.", n.to_string());
            }
        }
    }
}

#[cfg(all(test, feature = "ops"))]
mod tests {
    use super::*;

    #[test]
    fn render_emits_counters_and_gauges() {
        let m = Metrics::new();
        m.inc_runs_created();
        m.inc_succeeded();
        m.inc_succeeded();
        m.inc_dead_letters();
        m.observe_task_duration(0.3);
        m.observe_reconcile_tick(0.002);
        m.observe_dispatch_latency(0.0004);
        m.observe_result_wait(0.0009);
        m.observe_claim_batch(12);
        let snap = MetricsSnapshot {
            runs_by_status: vec![("running".into(), 2), ("succeeded".into(), 5)],
            tasks_by_status: vec![("succeeded".into(), 10), ("ready".into(), 7)],
            dead_letters: 3,
            ready_by_class: vec![crate::models::ReadyClassBacklog {
                runner_class: "etl".into(),
                count: 7,
                oldest_scheduled_at: Some(
                    (chrono::Utc::now() - chrono::TimeDelta::seconds(120)).to_rfc3339(),
                ),
            }],
            ..Default::default()
        };
        let pool = DbPoolStats { connections: 5, idle: 2, max: 10 };
        let text = m.render(&snap, Some(&pool));
        // Per-class backlog gauges (runner segmentation / unclaimable-class alarm).
        assert!(text.contains("scheduler_ready_tasks_by_class{runner_class=\"etl\"} 7"));
        let age_line = text
            .lines()
            .find(|l| l.starts_with("scheduler_ready_oldest_age_seconds{runner_class=\"etl\"}"))
            .expect("per-class age gauge present");
        let age: i64 = age_line.rsplit(' ').next().unwrap().parse().unwrap();
        assert!((115..=130).contains(&age), "age ~120s, got {age}");
        assert!(text.contains("scheduler_runs_created_total 1"));
        assert!(text.contains("scheduler_tasks_succeeded_total 2"));
        assert!(text.contains("scheduler_dead_letters_total 1"));
        assert!(text.contains("scheduler_runs{status=\"running\"} 2"));
        assert!(text.contains("scheduler_tasks{status=\"succeeded\"} 10"));
        assert!(text.contains("scheduler_dead_letters 3"));
        assert!(text.contains("scheduler_uptime_seconds"));
        // Queue depth derived from the "ready" task gauge.
        assert!(text.contains("scheduler_queue_depth 7"));
        // Histograms: bucket/sum/count series present and the observation counted.
        assert!(text.contains("scheduler_task_duration_seconds_bucket{le=\"+Inf\"} 1"));
        assert!(text.contains("scheduler_task_duration_seconds_count 1"));
        assert!(text.contains("scheduler_reconcile_tick_seconds_count 1"));
        // Dispatch-path histograms (LOW_LATENCY A-5): a 400 µs dispatch latency
        // must resolve below the old 5 ms floor — the sub-millisecond buckets
        // are the point of these series.
        assert!(text.contains("scheduler_dispatch_latency_seconds_bucket{le=\"0.0005\"} 1"));
        assert!(text.contains("scheduler_dispatch_latency_seconds_count 1"));
        assert!(text.contains("scheduler_result_wait_seconds_bucket{le=\"0.001\"} 1"));
        assert!(text.contains("scheduler_result_wait_seconds_count 1"));
        // Claim batch of 12 lands in the le="16" count bucket.
        assert!(text.contains("scheduler_claim_batch_size_bucket{le=\"16\"} 1"));
        assert!(text.contains("scheduler_claim_batch_size_count 1"));
        // DB pool saturation: in_use = connections - idle.
        assert!(text.contains("scheduler_db_pool_connections 5"));
        assert!(text.contains("scheduler_db_pool_in_use 3"));
        assert!(text.contains("scheduler_db_pool_max 10"));
    }

    /// The per-class series cap: with more classes than READY_CLASS_SERIES_CAP,
    /// only the busiest get their own series and the rest fold into
    /// `runner_class="other"` (count summed, age = tail max). `other` cannot
    /// collide with a real class — `dag::validate_runner_class` reserves it.
    #[test]
    fn ready_class_series_are_capped_with_other_tail() {
        let m = Metrics::new();
        let now = chrono::Utc::now();
        // cap + 2 classes: class-00 (busiest, count 102) … class-21 (count 81).
        // The two least-busy (class-20: 82, class-21: 81) fold into the tail;
        // class-21 carries the tail's oldest task (~500 s).
        let ready_by_class: Vec<crate::models::ReadyClassBacklog> = (0..READY_CLASS_SERIES_CAP + 2)
            .map(|i| crate::models::ReadyClassBacklog {
                runner_class: format!("class-{i:02}"),
                count: (READY_CLASS_SERIES_CAP + 2 - i) as i64 + 80,
                oldest_scheduled_at: Some(
                    (now - chrono::TimeDelta::seconds(if i == READY_CLASS_SERIES_CAP + 1 { 500 } else { 60 }))
                        .to_rfc3339(),
                ),
            })
            .collect();
        let snap = MetricsSnapshot {
            runs_by_status: vec![],
            tasks_by_status: vec![],
            dead_letters: 0,
            ready_by_class,
            ..Default::default()
        };
        let text = m.render(&snap, None);

        let class_lines: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("scheduler_ready_tasks_by_class{"))
            .collect();
        assert_eq!(class_lines.len(), READY_CLASS_SERIES_CAP + 1, "cap + one 'other' bucket");
        assert!(text.contains("scheduler_ready_tasks_by_class{runner_class=\"class-00\"} 102"));
        assert!(
            !text.contains("runner_class=\"class-20\"") && !text.contains("runner_class=\"class-21\""),
            "tail classes must not get their own series"
        );
        // Tail: 82 + 81 summed; age = the tail's max (~500 s).
        assert!(text.contains("scheduler_ready_tasks_by_class{runner_class=\"other\"} 163"));
        let age_line = text
            .lines()
            .find(|l| l.starts_with("scheduler_ready_oldest_age_seconds{runner_class=\"other\"}"))
            .expect("tail age series present");
        let age: i64 = age_line.rsplit(' ').next().unwrap().parse().unwrap();
        assert!((495..=510).contains(&age), "tail age = max of folded classes, got {age}");
    }

    /// The constrained-host and clock series render: the pause gauge follows
    /// the loop's verdict, disk-floor refusals and clock steps count, and the
    /// confidence gauge reads whatever `clock` last published — live, not a
    /// copy that could lag.
    #[test]
    fn edge_gates_and_clock_confidence_render() {
        let _guard = crate::clock::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let m = Metrics::new();
        let snap = MetricsSnapshot::default();
        m.set_claims_paused(true);
        m.inc_admission_refused_disk();
        m.inc_clock_steps();
        m.inc_clock_steps();
        crate::clock::publish(crate::clock::ClockStatus::drifted(-2_500, "step"));
        let text = m.render(&snap, None);
        assert!(text.contains("scheduler_claims_paused 1"), "{text}");
        assert!(text.contains("scheduler_admission_refused_disk_total 1"), "{text}");
        assert!(text.contains("scheduler_clock_steps_total 2"), "{text}");
        assert!(text.contains("scheduler_clock_confidence 1"), "{text}");

        m.set_claims_paused(false);
        crate::clock::publish(crate::clock::ClockStatus::synced("sync-file"));
        let text = m.render(&snap, None);
        assert!(text.contains("scheduler_claims_paused 0"), "{text}");
        assert!(text.contains("scheduler_clock_confidence 0"), "{text}");
        crate::clock::publish(crate::clock::ClockStatus::unknown());
        assert!(m.render(&snap, None).contains("scheduler_clock_confidence 2"));
    }

    /// `observe` lands a value in the correct cumulative bucket and ignores
    /// non-finite inputs without corrupting the count.
    #[test]
    fn histogram_buckets_and_count() {
        let h = Histogram::new(DURATION_BUCKETS);
        h.observe(0.3); // falls in the le="0.5" bucket
        h.observe(1000.0); // overflow (+Inf only)
        h.observe(f64::NAN); // clamped to 0.0, still counted
        assert_eq!(h.count.load(Ordering::Relaxed), 3);
    }

    /// Finished runs feed the run histogram and the per-workflow summary and
    /// last-run gauges; the datastore views render by workflow, environment
    /// and dead-letter source, with hostile label values escaped.
    #[test]
    fn workflow_and_dead_letter_views_render() {
        let m = Metrics::new();
        m.observe_run_finished("ci/build", true, 90.0);
        m.observe_run_finished("ci/build", false, 30.0);
        m.observe_run_finished("ci/build", true, 120.0);
        let now = chrono::Utc::now();
        let snap = MetricsSnapshot {
            recent_runs: vec![
                crate::models::RecentRuns { workflow: "ci/build".into(), environment: Some("prod".into()), status: "succeeded".into(), count: 4 },
                crate::models::RecentRuns { workflow: "ci/build".into(), environment: None, status: "succeeded".into(), count: 1 },
                crate::models::RecentRuns { workflow: "a\"b\\c".into(), environment: None, status: "running".into(), count: 2 },
            ],
            active_tasks: vec![("ci/build".into(), "running".into(), 3)],
            dead_letters_by_source: vec![crate::models::DeadLetterSource {
                source: "kafka".into(),
                count: 5,
                oldest_first_seen_at: Some((now - chrono::TimeDelta::seconds(600)).to_rfc3339()),
            }],
            ..Default::default()
        };
        let text = m.render(&snap, None);
        assert!(text.contains("scheduler_run_duration_seconds_count 3"), "{text}");
        assert!(text.contains("scheduler_run_duration_seconds_bucket{le=\"120\"} 3"), "{text}");
        assert!(text.contains("# TYPE scheduler_workflow_run_duration_seconds summary"));
        assert!(text.contains("scheduler_workflow_run_duration_seconds_sum{workflow=\"ci/build\",status=\"succeeded\"} 210"), "{text}");
        assert!(text.contains("scheduler_workflow_run_duration_seconds_count{workflow=\"ci/build\",status=\"succeeded\"} 2"));
        assert!(text.contains("scheduler_workflow_run_duration_seconds_count{workflow=\"ci/build\",status=\"failed\"} 1"));
        assert!(text.contains("scheduler_workflow_last_run_duration_seconds{workflow=\"ci/build\"} 120"));
        assert!(text.contains("scheduler_workflow_last_run_success{workflow=\"ci/build\"} 1"));
        // Two environments' cells for one workflow sum into one workflow series.
        assert!(text.contains("scheduler_workflow_recent_runs{workflow=\"ci/build\",status=\"succeeded\"} 5"), "{text}");
        assert!(text.contains("scheduler_workflow_recent_runs{workflow=\"a\\\"b\\\\c\",status=\"running\"} 2"), "{text}");
        assert!(text.contains("scheduler_environment_recent_runs{environment=\"prod\",status=\"succeeded\"} 4"));
        assert!(text.contains("scheduler_environment_recent_runs{environment=\"(none)\",status=\"succeeded\"} 1"));
        assert!(text.contains("scheduler_workflow_active_tasks{workflow=\"ci/build\",status=\"running\"} 3"));
        assert!(text.contains("scheduler_dead_letters_by_source{source=\"kafka\"} 5"));
        let age_line = text
            .lines()
            .find(|l| l.starts_with("scheduler_dead_letters_oldest_age_seconds{source=\"kafka\"}"))
            .expect("dead-letter age present");
        let age: i64 = age_line.rsplit(' ').next().unwrap().parse().unwrap();
        assert!((595..=610).contains(&age), "age ~600s, got {age}");
    }

    /// A client that names every run differently gets `other`, not a series
    /// per run: in the process map and in the datastore views alike.
    #[test]
    fn workflow_series_are_capped() {
        let m = Metrics::new();
        for i in 0..WORKFLOW_SERIES_CAP + 5 {
            m.observe_run_finished(&format!("wf-{i:03}"), true, 1.0);
        }
        let recent_runs = (0..WORKFLOW_SERIES_CAP + 5)
            .map(|i| crate::models::RecentRuns {
                workflow: format!("wf-{i:03}"),
                environment: None,
                status: "succeeded".into(),
                // wf-000 is the busiest; the last five are the tail.
                count: (WORKFLOW_SERIES_CAP + 5 - i) as i64,
            })
            .collect();
        let text = m.render(&MetricsSnapshot { recent_runs, ..Default::default() }, None);
        let series = |prefix: &str| text.lines().filter(|l| l.starts_with(prefix)).count();
        assert_eq!(series("scheduler_workflow_last_run_duration_seconds{"), WORKFLOW_SERIES_CAP + 1);
        assert!(text.contains("scheduler_workflow_run_duration_seconds_count{workflow=\"other\",status=\"succeeded\"} 5"), "{text}");
        assert_eq!(series("scheduler_workflow_recent_runs{"), WORKFLOW_SERIES_CAP + 1);
        // The five least-busy workflows (counts 5..1) fold into one cell.
        assert!(text.contains("scheduler_workflow_recent_runs{workflow=\"other\",status=\"succeeded\"} 15"), "{text}");
    }

    /// `/proc/<pid>/stat` is parsed from the last `)`, so a command name with
    /// spaces and parentheses cannot shift the fields.
    #[test]
    fn process_stat_parses_past_a_hostile_command_name() {
        let stat = "42 (dag ron) x) S 1 42 42 0 -1 4194560 100 0 0 0 250 150 0 0 20 0 9 0 5000 104857600 2560 18446744073709551615";
        let p = process::parse_stat(stat, 100.0, 4096, 1_000.0).expect("parses");
        assert_eq!(p.cpu_seconds, 4.0);
        assert_eq!(p.threads, 9);
        assert_eq!(p.start_time_seconds, 1_050.0);
        assert_eq!(p.virtual_bytes, 104_857_600);
        assert_eq!(p.resident_bytes, 2560 * 4096);
        assert!(process::parse_stat("garbage", 100.0, 4096, 0.0).is_none());
    }

    #[test]
    fn fault_counters_render_both_series_and_never_grow_cardinality() {
        use crate::fault::FaultClass;
        let m = Metrics::new();
        m.inc_fault(FaultClass::GpuEcc);
        m.inc_fault(FaultClass::GpuEcc);
        m.inc_fault(FaultClass::NanLoss);
        m.inc_fault(FaultClass::NcclTimeout);
        let snap = MetricsSnapshot {
            runs_by_status: vec![],
            tasks_by_status: vec![],
            dead_letters: 0,
            ready_by_class: vec![],
            ..Default::default()
        };
        let text = m.render(&snap, None);

        assert!(text.contains("scheduler_task_faults_total{class=\"gpu-ecc\",disposition=\"infrastructure\"} 2"), "{text}");
        assert!(text.contains("scheduler_task_faults_total{class=\"nan-loss\",disposition=\"application\"} 1"));
        // An uncorroborated collective timeout must not land in the infra
        // bucket — that bucket is what a fault-aware retry policy acts on.
        assert!(text.contains("scheduler_task_faults_total{class=\"nccl-timeout\",disposition=\"unknown\"} 1"));
        assert!(text.contains("scheduler_task_faults_by_disposition_total{disposition=\"infrastructure\"} 2"));
        // Every signpost gate is a series from the first scrape, hit or not.
        // The values are process-wide and other tests hit the gates, so this
        // asserts the series and not a count.
        for gate in SignpostGate::ALL {
            let prefix = format!("scheduler_signpost_hits_total{{gate=\"{}\"}} ", gate.as_str());
            assert!(text.lines().any(|l| l.starts_with(&prefix)), "missing {prefix}: {text}");
        }
        assert!(text.contains("scheduler_task_faults_by_disposition_total{disposition=\"application\"} 1"));
        assert!(text.contains("scheduler_task_faults_by_disposition_total{disposition=\"unknown\"} 1"));

        // Closed taxonomy: exactly one series per class, present at zero, so
        // increase() over a window predating the first fault still works.
        let series = text
            .lines()
            .filter(|l| l.starts_with("scheduler_task_faults_total{"))
            .count();
        assert_eq!(series, FaultClass::ALL.len());
        assert!(text.contains("scheduler_task_faults_total{class=\"storage\",disposition=\"infrastructure\"} 0"));
    }
}
