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

/// Upper bounds (seconds) for the latency histograms. Spans sub-millisecond
/// reconcile ticks through minutes-long ETL tasks so one bucket set serves both
/// `reconcile_tick` and `task_duration`. A `+Inf` bucket is appended at render.
const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

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
    /// Schedule fires skipped by a `when:` gate evaluating false.
    pub schedule_gated: AtomicU64,
    /// Schedules auto-stopped by a `stopStrategy` expression.
    pub schedules_stopped: AtomicU64,
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
    /// Reconcile-loop tick duration — the CPU-pegging signal from LOADTEST.md.
    pub reconcile_tick: Histogram,
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
            schedule_gated: AtomicU64::new(0),
            schedules_stopped: AtomicU64::new(0),
            #[cfg(feature = "enterprise")]
            catchup_runs: AtomicU64::new(0),
            #[cfg(feature = "enterprise")]
            auto_reruns: AtomicU64::new(0),
            task_duration: Histogram::new(DURATION_BUCKETS),
            reconcile_tick: Histogram::new(DURATION_BUCKETS),
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
    /// One schedule auto-stopped by a `stopStrategy` expression.
    pub fn inc_schedules_stopped(&self) {
        Self::bump(&self.schedules_stopped);
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

        let counters: [(&str, &str, u64); 10] = [
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
            ("scheduler_schedule_gated_total", "Schedule fires skipped by a when: gate.",
             self.schedule_gated.load(Ordering::Relaxed)),
            ("scheduler_schedules_stopped_total", "Schedules auto-stopped by a stopStrategy expression.",
             self.schedules_stopped.load(Ordering::Relaxed)),
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
        // LOADTEST.md alert rules watch.
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
}
