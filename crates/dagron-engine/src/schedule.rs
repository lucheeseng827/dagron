//! DB-backed workflow schedules (v7 UI).
//!
//! The file-based [cron](crate::cron) loop fires DAG files from a static config.
//! This loop fires **first-class workflows** scheduled through the UI: rows in the
//! `schedules` table (managed by dagron-api) pair a workflow with a cron
//! expression. Each tick the leadership holder selects due rows, submits the
//! workflow's spec via [`db::create_run`], and advances `next_fire_at`.
//!
//! **Singleton + shared state.** Unlike file cron (which tracks `next` per
//! process), `next_fire_at` lives in the DB. Only the leader fires *and* advances
//! it, so N schedulers never double-fire — there is no per-process bookkeeping to
//! drift. Opt out with `DB_SCHEDULES=0`.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
use tracing::{info, warn};

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;

/// How often the loop wakes to check for due schedules.
const TICK: Duration = Duration::from_secs(1);

/// Park a schedule far out when its cron has no further fire time.
const FAR_FUTURE_DAYS: i64 = 36_500;

/// Drive the DB-schedule loop until the process exits. Fires only while
/// `is_leader` is set; `next_fire_at` is shared DB state, so a follower simply
/// does nothing (no local bookkeeping to keep warm).
pub async fn run(pool: db::Pool, is_leader: Arc<AtomicBool>, metrics: Arc<Metrics>) {
    info!("DB schedule loop running");
    loop {
        tokio::time::sleep(TICK).await;
        if !is_leader.load(Ordering::SeqCst) {
            continue;
        }
        let now = Utc::now();
        let now_s = now.to_rfc3339();

        let due = match db::claim_due_schedules(&pool, &now_s).await {
            Ok(d) => d,
            Err(e) => {
                warn!(error = %e, "schedule sweep query failed");
                continue;
            }
        };

        for s in due {
            // Fire: submit the workflow's spec as a run (validated at save time,
            // but re-check so a bad edit can't panic the loop).
            match DagGraph::from_yaml(&s.spec) {
                Ok(dag) => match db::create_run(&pool, &dag, &s.spec).await {
                    Ok(run_id) => {
                        metrics.inc_runs_created();
                        info!(schedule = %s.id, %run_id, name = %dag.spec.name, "schedule fired run");
                    }
                    Err(e) => warn!(schedule = %s.id, error = %e, "schedule create_run failed"),
                },
                Err(e) => warn!(schedule = %s.id, error = %e, "scheduled workflow no longer parses"),
            }

            // Advance to the next fire time regardless of fire outcome (the next
            // fire is independent), so a transient failure doesn't wedge the row.
            let next = Schedule::from_str(&s.cron_expr)
                .ok()
                .and_then(|sch| sch.after(&now).next());
            let next_s = next
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| (now + chrono::TimeDelta::days(FAR_FUTURE_DAYS)).to_rfc3339());
            if let Err(e) = db::advance_schedule(&pool, &s.id, &next_s, &now_s).await {
                warn!(schedule = %s.id, error = %e, "advance_schedule failed");
            }
        }
    }
}
