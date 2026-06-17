//! Leadership singleton (v5).
//!
//! Cron firing and retention GC must happen on **exactly one** scheduler at a
//! time, even with N identical processes against one datastore. Rather than a
//! coordinator or leader-election protocol, this reuses the system's core idea —
//! *the lease row is the truth*: [`db::try_acquire_leadership`] holds one
//! `leader_election` row per role, valid for a bounded lease. The holder renews
//! on a timer; if it dies, the lease lapses and the next renewing peer takes the
//! role. No heartbeat table, no consensus — the same pattern that recovers
//! orphaned task leases.
//!
//! [`spawn`] runs that renew loop on a background task and publishes the current
//! status through a shared [`AtomicBool`] that the cron and GC loops read each
//! tick. Losing the DB (or the race) simply clears the flag, so a partitioned
//! scheduler stops firing cron / GC rather than acting as a second leader.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::db;

/// Spawn the leadership renew loop for `role`. Returns the flag that mirrors
/// "do I currently hold this role?" — cron/GC gate their actions on it.
///
/// The lease is renewed every `lease_secs / 2` (at least once a second) so a live
/// holder never lets its lease lapse between ticks, while a dead holder's role
/// frees up within `lease_secs`.
pub fn spawn(pool: db::Pool, role: String, holder: String, lease_secs: i64) -> Arc<AtomicBool> {
    let is_leader = Arc::new(AtomicBool::new(false));
    // A non-positive lease is meaningless (it would be born expired). Don't spawn
    // the renew loop — this node simply never becomes leader, so cron/GC stay off
    // here rather than acting on a bad lease. (main.rs already filters to > 0.)
    if lease_secs <= 0 {
        warn!(%role, lease_secs, "leadership disabled: lease_secs must be > 0");
        return is_leader;
    }
    let flag = Arc::clone(&is_leader);
    let renew = Duration::from_secs(((lease_secs / 2).max(1)) as u64);

    tokio::spawn(async move {
        loop {
            match db::try_acquire_leadership(&pool, &role, &holder, lease_secs).await {
                Ok(true) => {
                    if !flag.swap(true, Ordering::SeqCst) {
                        info!(%role, %holder, "acquired leadership");
                    }
                }
                Ok(false) => {
                    if flag.swap(false, Ordering::SeqCst) {
                        info!(%role, "lost leadership");
                    }
                }
                Err(e) => {
                    // A DB blip must not leave us acting as leader on stale belief.
                    if flag.swap(false, Ordering::SeqCst) {
                        warn!(%role, error = %e, "leadership renew failed — standing down");
                    }
                }
            }
            tokio::time::sleep(renew).await;
        }
    });

    is_leader
}
