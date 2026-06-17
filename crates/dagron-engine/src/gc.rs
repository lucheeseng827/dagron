//! Retention GC (v6).
//!
//! The datastore is the source of truth, which means it also accumulates every
//! run's history forever unless something reclaims it. This loop enforces a
//! retention window: terminal runs (`succeeded`/`failed`/`cancelled`) whose
//! `finished_at` is older than `retention` are purged with their tasks,
//! dependency edges, and now-orphaned definitions in one transaction
//! ([`db::gc_old_runs`]).
//!
//! Like cron, GC is **singleton-gated** — only the [leadership](crate::leadership)
//! holder sweeps, so N schedulers don't all race the same deletes. It is safe if
//! they did (the deletes are idempotent), but gating keeps the work to one node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::db;

/// Run the GC sweep loop until the process exits. `retention_secs` is the age
/// past `finished_at` after which a terminal run is eligible for deletion;
/// `interval_secs` is how often to sweep.
pub async fn run(
    pool: db::Pool,
    retention_secs: i64,
    interval_secs: u64,
    is_leader: Arc<AtomicBool>,
) {
    // A non-positive retention would make the cutoff `now` or a *future* time,
    // deleting all (or recent) terminal runs. Refuse to run rather than risk
    // mass deletion. (main.rs already filters to > 0; this is the backstop.)
    if retention_secs <= 0 {
        warn!(retention_secs, "retention GC disabled: retention_secs must be > 0");
        return;
    }
    let interval = Duration::from_secs(interval_secs.max(1));
    info!(retention_secs, interval_secs, "retention GC loop running");
    loop {
        tokio::time::sleep(interval).await;
        if !is_leader.load(Ordering::SeqCst) {
            continue;
        }
        let cutoff = (chrono::Utc::now() - chrono::TimeDelta::seconds(retention_secs)).to_rfc3339();
        match db::gc_old_runs(&pool, &cutoff).await {
            Ok(0) => {}
            Ok(n) => info!(deleted = n, %cutoff, "retention GC purged old runs"),
            Err(e) => warn!(error = %e, "retention GC sweep failed"),
        }
    }
}
