//! Stale-ready (unclaimable-class) alert.
//!
//! Runner segmentation makes a new silent failure possible: with every
//! scheduler restricted via `RUNNER_CLASSES`, a task in a class **no live pool
//! serves** just sits `ready` forever — nothing errors, nothing retries, the
//! run simply never finishes. (Any *unrestricted* scheduler drains every class,
//! which is why an unsegmented deployment can't hit this.)
//!
//! This loop watches the per-class ready backlog
//! ([`db::ready_backlog_by_class`]) and WARNs — once per interval, per class —
//! when a class's oldest ready task has waited longer than
//! `READY_AGE_ALERT_SECS`. The same signal is exported continuously as the
//! `scheduler_ready_oldest_age_seconds{runner_class=…}` gauge for real
//! alerting; the log line is the in-band backstop for fleets nobody scrapes.
//!
//! Leadership-gated like cron/GC — one warner per cluster, not one per replica.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::db;

/// Run the stale-ready watch loop until the process exits. `alert_secs` is the
/// oldest-ready age past which a class is warned about; `interval_secs` is how
/// often to check.
pub async fn run(
    pool: db::Pool,
    alert_secs: i64,
    interval_secs: u64,
    is_leader: Arc<AtomicBool>,
) {
    if alert_secs <= 0 {
        return;
    }
    let interval = Duration::from_secs(interval_secs.max(1));
    info!(alert_secs, interval_secs, "stale-ready alert loop running");
    loop {
        tokio::time::sleep(interval).await;
        if !is_leader.load(Ordering::SeqCst) {
            continue;
        }
        match db::ready_backlog_by_class(&pool).await {
            Ok(backlog) => {
                let now = chrono::Utc::now();
                for b in backlog {
                    let age = b.oldest_age_secs(now);
                    if age > alert_secs {
                        warn!(
                            runner_class = %b.runner_class,
                            ready = b.count,
                            oldest_age_secs = age,
                            "ready tasks are not being claimed — is any scheduler serving \
                             this runner class? (RUNNER_CLASSES on every pool excludes it, \
                             or the pool serving it is down)"
                        );
                    }
                }
            }
            Err(e) => warn!(error = %e, "stale-ready check failed"),
        }
    }
}
