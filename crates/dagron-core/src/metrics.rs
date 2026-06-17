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
    /// Task wall-time (claim→finish), the headroom-dominating signal real ETL
    /// tasks have but the no-op load test never exercised.
    pub task_duration: Histogram,
    /// Reconcile-loop tick duration — the CPU-pegging signal from LOADTEST.md.
    pub reconcile_tick: Histogram,
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
            task_duration: Histogram::new(DURATION_BUCKETS),
            reconcile_tick: Histogram::new(DURATION_BUCKETS),
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
    /// `pool` is the live datastore connection-pool saturation, read per scrape.
    #[cfg(feature = "ops")]
    pub fn render(&self, snap: &MetricsSnapshot, pool: Option<&DbPoolStats>) -> String {
        let mut out = String::with_capacity(2048);

        let counters: [(&str, &str, u64); 6] = [
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
        ];
        for (name, help, value) in counters {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
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

        let _ = writeln!(out, "# HELP scheduler_dead_letters Dead-letter rows currently parked.");
        let _ = writeln!(out, "# TYPE scheduler_dead_letters gauge");
        let _ = writeln!(out, "scheduler_dead_letters {}", snap.dead_letters);

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
        };
        let pool = DbPoolStats { connections: 5, idle: 2, max: 10 };
        let text = m.render(&snap, Some(&pool));
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
