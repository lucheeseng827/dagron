//! First-class backfill jobs (fast-win #18).
//!
//! The synchronous `POST /schedules/:id/backfill` materializes a whole window in
//! one capped call. A *backfill job* (`backfills` table, created via
//! `POST /api/backfills`) is instead a durable, listable, cancellable object that
//! this module **paces**: [`pace`] runs once per schedule-loop tick (so it is
//! leadership-gated for free) and fires only a bounded number of the job's due
//! fire-times, advancing the job's `cursor`. A large range therefore drips into
//! the cluster over many ticks instead of stampeding it, and an operator can
//! watch progress (`fired`/`requested`) or cancel mid-flight.
//!
//! Slots are still deduped through the existing `schedule_backfills` ledger (keyed
//! by `schedule_id` + logical date), so a job never double-runs a fire-time a
//! manual backfill or the auto-catch-up loop already materialized — and a
//! dedup-skipped slot does *not* count against the per-tick pace, so the cursor
//! keeps marching instead of re-selecting the same claimed page.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cron::Schedule;
use serde_json::json;
use tracing::{info, warn};

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;
use crate::schedule_time::parse_tz_or_utc;

/// How often the pacing loop wakes.
const TICK: Duration = Duration::from_secs(1);

/// Drive the backfill-job pacing loop until the process exits. Paces only while
/// `is_leader` is set (a follower idles); the dedup ledger makes a stray
/// double-tick safe, but gating keeps the work on one node. Runs in any resident
/// daemon so a job created via the API is always paced.
pub async fn run(pool: db::Pool, is_leader: Arc<AtomicBool>, metrics: Arc<Metrics>) {
    info!("backfill-job pacing loop running");
    loop {
        tokio::time::sleep(TICK).await;
        if !is_leader.load(Ordering::SeqCst) {
            continue;
        }
        pace(&pool, &metrics, Utc::now()).await;
    }
}

/// Runs a single job may fire per tick — the pacing rate. With the 1 s
/// schedule-loop tick this caps a job at ~this many runs/second, so a wide range
/// fills gradually. Overridable via `BACKFILL_PACE_PER_TICK`.
const DEFAULT_PACE_PER_TICK: usize = 20;

/// Absolute per-job ceiling on fire-times enumerated in one tick, so a
/// mis-snapshotted cron can't spin the iterator unbounded before the pace/range
/// checks bite.
const ENUMERATE_HARD_CAP: usize = 10_000;

fn pace_per_tick() -> usize {
    std::env::var("BACKFILL_PACE_PER_TICK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PACE_PER_TICK)
}

/// Pace every active backfill job by one tick. Called from the (leadership-gated)
/// schedule loop, so only the leader fires — the dedup ledger makes a stray
/// double-tick safe, but gating keeps the work on one node. Best-effort per job:
/// one job's error is logged and does not stop the others.
pub async fn pace(pool: &db::Pool, metrics: &Metrics, now: DateTime<Utc>) {
    let jobs = match db::list_active_backfills(pool).await {
        Ok(j) => j,
        Err(e) => {
            warn!(error = %e, "backfill-job list failed");
            return;
        }
    };
    if jobs.is_empty() {
        return;
    }
    let pace = pace_per_tick();
    let now_s = now.to_rfc3339();
    for job in jobs {
        if let Err(e) = pace_one(pool, metrics, &job, pace, &now_s).await {
            warn!(backfill = %job.id, error = %e, "backfill-job pacing failed");
        }
    }
}

/// Fire up to `pace` due runs for one job, advance its cursor, and complete it
/// when the range is exhausted or `max_runs` is reached.
async fn pace_one(
    pool: &db::Pool,
    metrics: &Metrics,
    job: &crate::models::BackfillJob,
    pace: usize,
    now_s: &str,
) -> anyhow::Result<()> {
    let sched = match Schedule::from_str(&job.cron_expr) {
        Ok(s) => s,
        Err(e) => {
            // A snapshot that no longer parses can never make progress — finish it
            // so the loop stops re-selecting it every tick.
            warn!(backfill = %job.id, cron = %job.cron_expr, error = %e, "backfill-job cron invalid — completing");
            db::complete_backfill(pool, &job.id, now_s).await?;
            return Ok(());
        }
    };
    let tz = parse_tz_or_utc(&job.timezone);
    let cursor = DateTime::parse_from_rfc3339(&job.cursor)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("bad cursor '{}': {e}", job.cursor))?;
    let range_to = DateTime::parse_from_rfc3339(&job.range_to)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("bad range_to '{}': {e}", job.range_to))?;

    // How many more runs this job may fire in total, and this tick.
    let remaining_total = (job.max_runs - job.fired).max(0) as usize;
    if remaining_total == 0 {
        db::complete_backfill(pool, &job.id, now_s).await?;
        return Ok(());
    }
    let tick_cap = pace.min(remaining_total);

    let mut fired_this_tick = 0usize;
    let mut new_cursor = job.cursor.clone();
    let mut exhausted = false;

    // Enumerate fire-times strictly after the cursor, in the job's timezone.
    for fire in sched
        .after(&cursor.with_timezone(&tz))
        .map(|d| d.with_timezone(&Utc))
        .take(ENUMERATE_HARD_CAP)
    {
        if fire > range_to {
            exhausted = true; // no more fire-times fall inside the range
            break;
        }
        // Advance the cursor past every fire we *consider*, so a dedup-skipped
        // (already-materialized) slot doesn't get re-examined next tick. Keep the
        // prior cursor so a released (failed) fire can be retried next tick.
        let previous_cursor = new_cursor.clone();
        new_cursor = fire.to_rfc3339();
        let logical = new_cursor.clone();

        // Dedup gate (shared with manual backfill + catch-up). A skip does not
        // count against the tick's pace.
        if !db::claim_backfill_slot(pool, &job.schedule_id, &logical, now_s).await? {
            continue;
        }

        // Inject the fire's nominal time as {{ scheduled_time }} so the run
        // processes *its* interval. Re-validate per fire so a bad snapshot can't
        // panic the loop; on a parse error release the slot and complete the job.
        let mut params = std::collections::BTreeMap::new();
        params.insert("scheduled_time".to_string(), logical.clone());
        // Environment resolution errors split two ways: a *deleted* environment
        // is permanent (complete the job, like a spec that no longer parses),
        // but a transient DB failure must NOT complete it — release the slot,
        // rewind the cursor, and retry this fire next tick.
        let parsed = match crate::environments::template_params(pool, &job.spec).await {
            Ok(extra) => {
                params.extend(extra);
                DagGraph::from_yaml_with_params(&job.spec, &params)
            }
            Err(e) if e.downcast_ref::<crate::environments::UnknownEnvironment>().is_some() => {
                Err(e)
            }
            Err(e) => {
                db::release_backfill_slot(pool, &job.schedule_id, &logical).await?;
                warn!(backfill = %job.id, logical_date = %logical, error = %e, "backfill-job environment resolution failed; retrying next tick");
                new_cursor = previous_cursor;
                break;
            }
        };
        let dag = match parsed {
            Ok(d) => d,
            Err(e) => {
                db::release_backfill_slot(pool, &job.schedule_id, &logical).await?;
                warn!(backfill = %job.id, error = %e, "backfill-job spec no longer parses — completing");
                exhausted = true;
                break;
            }
        };
        match db::create_run(pool, &dag, &job.spec).await {
            Ok(run_id) => {
                db::record_backfill_run(pool, &job.schedule_id, &logical, &run_id).await?;
                metrics.inc_runs_created();
                fired_this_tick += 1;
                let payload = json!({
                    "run_id": run_id,
                    "backfill_id": job.id,
                    "schedule_id": job.schedule_id,
                    "logical_date": logical,
                    "reason": "backfill_job",
                })
                .to_string();
                if let Err(e) = db::enqueue_outbox_event(pool, &run_id, "backfill.job", &payload).await {
                    warn!(%run_id, error = %e, "backfill-job outbox enqueue failed");
                }
            }
            Err(e) => {
                // Release so the slot stays reclaimable, and rewind the cursor so
                // the next tick retries this fire instead of skipping past it.
                db::release_backfill_slot(pool, &job.schedule_id, &logical).await?;
                warn!(backfill = %job.id, logical_date = %logical, error = %e, "backfill-job create_run failed; slot released");
                new_cursor = previous_cursor;
                break;
            }
        }

        if fired_this_tick >= tick_cap {
            break; // paced this tick (or hit the remaining-total cap)
        }
    }

    let total_fired = job.fired + fired_this_tick as i64;
    db::advance_backfill(pool, &job.id, &new_cursor, total_fired, now_s).await?;

    // Complete when the range holds no more fires or the run cap is reached.
    if exhausted || total_fired >= job.max_runs {
        db::complete_backfill(pool, &job.id, now_s).await?;
        info!(backfill = %job.id, fired = total_fired, requested = job.requested, "backfill job completed");
    } else if fired_this_tick > 0 {
        info!(backfill = %job.id, fired = total_fired, requested = job.requested, "backfill job progress");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::BackfillJob;

    async fn temp_pool() -> (db::Pool, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("m54_bfjob_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    /// Seed the parent chain the `schedule_backfills` FK requires
    /// (workflow → schedule) so `claim_backfill_slot` can insert.
    async fn seed_schedule(pool: &db::Pool, schedule_id: &str, spec: &str) {
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ('wf-1', 'bf', ?, 't', 't')")
            .bind(spec)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO schedules (id, workflow_id, cron_expr, created_at, updated_at) VALUES (?, 'wf-1', '0 * * * * *', 't', 't')")
            .bind(schedule_id)
            .execute(pool)
            .await
            .unwrap();
    }

    fn job(now: &str) -> BackfillJob {
        BackfillJob {
            id: uuid::Uuid::new_v4().to_string(),
            schedule_id: "sched-1".to_string(),
            cron_expr: "0 * * * * *".to_string(), // every minute at :00
            timezone: "UTC".to_string(),
            spec: "name: bf\ntasks:\n  - name: a\n    command: [\"true\"]\n".to_string(),
            range_from: "2026-01-01T00:00:00+00:00".to_string(),
            range_to: "2026-01-01T00:05:00+00:00".to_string(),
            cursor: "2026-01-01T00:00:00+00:00".to_string(),
            status: "running".to_string(),
            max_runs: 5,
            requested: 5,
            fired: 0,
            created_at: now.to_string(),
            updated_at: now.to_string(),
        }
    }

    /// The pacer fires at most `pace` runs per tick, advancing the cursor, and
    /// completes the job once the range is exhausted — 5 per-minute fires in a
    /// 5-minute range, drained 2 → 2 → 1 across three ticks.
    #[tokio::test]
    async fn paces_a_range_across_ticks_then_completes() {
        let (pool, path) = temp_pool().await;
        let metrics = Metrics::new();
        let now = "2026-02-01T00:00:00+00:00";
        let j = job(now);
        seed_schedule(&pool, &j.schedule_id, &j.spec).await;
        db::create_backfill(&pool, &j).await.unwrap();

        // Tick 1: pace=2 → fires 00:01, 00:02; cursor advances; still running.
        let cur = db::get_backfill(&pool, &j.id).await.unwrap().unwrap();
        pace_one(&pool, &metrics, &cur, 2, now).await.unwrap();
        let after1 = db::get_backfill(&pool, &j.id).await.unwrap().unwrap();
        assert_eq!(after1.fired, 2, "two fired this tick");
        assert_eq!(after1.status, "running");
        assert_eq!(after1.cursor, "2026-01-01T00:02:00+00:00");

        // Tick 2: fires 00:03, 00:04.
        pace_one(&pool, &metrics, &after1, 2, now).await.unwrap();
        let after2 = db::get_backfill(&pool, &j.id).await.unwrap().unwrap();
        assert_eq!(after2.fired, 4);
        assert_eq!(after2.status, "running");

        // Tick 3: fires 00:05 (last in range), next fire 00:06 > range_to → complete.
        pace_one(&pool, &metrics, &after2, 2, now).await.unwrap();
        let after3 = db::get_backfill(&pool, &j.id).await.unwrap().unwrap();
        assert_eq!(after3.fired, 5, "all five materialized");
        assert_eq!(after3.status, "completed");

        // Exactly five runs were created, and the dedup ledger holds five slots.
        let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(runs, 5);
        let slots: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM schedule_backfills WHERE schedule_id = 'sched-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(slots, 5);

        // Idempotent: pacing the completed job's list is a no-op (list is empty).
        assert!(db::list_active_backfills(&pool).await.unwrap().is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A cancelled job stops pacing: `cancel_backfill` flips it and the active-list
    /// no longer returns it, so `pace` skips it. Cancel is idempotent/guarded.
    #[tokio::test]
    async fn cancel_stops_pacing() {
        let (pool, path) = temp_pool().await;
        let now = "2026-02-01T00:00:00+00:00";
        let j = job(now);
        seed_schedule(&pool, &j.schedule_id, &j.spec).await;
        db::create_backfill(&pool, &j).await.unwrap();

        assert!(db::cancel_backfill(&pool, &j.id, now).await.unwrap(), "running → cancelled");
        let after = db::get_backfill(&pool, &j.id).await.unwrap().unwrap();
        assert_eq!(after.status, "cancelled");
        assert!(db::list_active_backfills(&pool).await.unwrap().is_empty(), "cancelled job not paced");
        // Second cancel is a no-op (already terminal).
        assert!(!db::cancel_backfill(&pool, &j.id, now).await.unwrap());
        // Unknown id → false.
        assert!(!db::cancel_backfill(&pool, "nope", now).await.unwrap());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
