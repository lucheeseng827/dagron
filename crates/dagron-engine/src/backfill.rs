//! Automatic backfill & self-healing reconciler (QW3 auto-catchup).
//!
//! The QW3 backfill is *operator-driven*: a human calls
//! `POST /api/schedules/:id/backfill` with an explicit window. This loop makes
//! backfill **autonomous** — the engine detects and heals the gaps a long-running
//! scheduler accumulates, and republishes that state as metrics so an alerting
//! rule can *trigger* downstream eventing. Three cases, escalating in blast radius
//! and so in caution:
//!
//! 1. **Missed schedule fires → catch-up.** For each `catchup`-opted schedule the
//!    loop enumerates the cron fire-times between its lower bound — the later of
//!    `last_fired_at` and `now - window`, so a long gap never replays unbounded
//!    history — and `now`, materializing each through the `schedule_backfills`
//!    dedup ledger so a re-sweep (or the manual endpoint) never double-runs a slot.
//!    This heals the gap since the last recorded fire: the canonical case is a
//!    schedule **paused then re-enabled** (its `last_fired_at` is frozen at the
//!    pause, a classic catch-up foot-gun), and a **catchup-only**
//!    deployment (`DB_SCHEDULES` off) where this loop is the sole firing path.
//!    (When the normal schedule loop co-runs it advances `last_fired_at` to the
//!    present on resume — "resume from present", `schedule.rs` — so a pure process
//!    downtime leaves no `last_fired_at` gap for catch-up to see; routing normal
//!    fires through the ledger to also heal that is the durable-queue beta below.)
//! 2. **Failed runs → auto-rerun.** A terminally-`failed` run is re-armed from its
//!    failure frontier (the existing [`db::rerun_from_failed`]), bounded by a
//!    per-run attempt cap + cooldown (the `run_reruns` ledger) so a
//!    deterministically-failing DAG is not re-armed forever.
//! 3. **Stalled runs → surface.** A run still `running` past the stall SLA is
//!    *counted* into the `scheduler_incomplete_runs` gauge — a signal, not an
//!    auto-action: force-restarting a maybe-still-progressing run is unsafe, so
//!    QW3-catchup leaves the decision to an alert/operator.
//!
//! **Singleton-gated.** Like [cron](crate::cron) / [GC](crate::gc) /
//! [schedule](crate::schedule), only the [leadership](crate::leadership) holder
//! sweeps — the dedup ledger + attempt cap make a double-sweep *safe*, but gating
//! keeps the work on one node. Every action lands a row in the transactional
//! `event_outbox`, so catch-up and auto-rerun are observable by the same drain
//! worker that ships `run.completed`.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use cron::Schedule;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;
use crate::schedule_time::parse_tz_or_utc;

/// Absolute ceiling on runs one catch-up sweep materializes for a single
/// schedule, regardless of the per-schedule or env cap — the backstop that stops
/// a months-wide gap (or a misconfigured 1-second cron) from stampeding the
/// cluster in one tick. Mirrors the API backfill's `BACKFILL_HARD_CAP`.
const CATCHUP_HARD_CAP: usize = 1000;

/// Cap on failed runs re-armed per sweep, so a flood of failures is healed over
/// several ticks rather than all at once (each rerun re-enqueues a whole DAG).
const RERUN_BATCH: i64 = 100;

/// Tunables for the auto-backfill loop, read from the environment once at start.
/// Returns `None` (loop disabled) unless `AUTO_BACKFILL` is truthy.
#[derive(Debug, Clone)]
pub struct Config {
    /// How often the loop sweeps.
    pub interval: Duration,
    /// Default catch-up look-back when a schedule sets no `catchup_window_secs`.
    pub default_window_secs: i64,
    /// Default per-sweep run cap when a schedule sets no `catchup_max_runs`.
    pub default_max_runs: i64,
    /// Whether to auto-rerun terminally-failed runs (case 2).
    pub rerun_failed: bool,
    /// Per-run cap on automatic reruns before a run is left for an operator.
    pub rerun_max_attempts: i64,
    /// Minimum gap between automatic reruns of the same run.
    pub rerun_cooldown_secs: i64,
    /// Age past `created_at` after which a still-`running` run counts as stalled.
    pub stall_secs: i64,
}

impl Config {
    /// Build from env, or `None` when `AUTO_BACKFILL` is unset/false. Numeric
    /// knobs fall back to safe defaults; non-positive values are floored so a
    /// fat-fingered `0` cannot wedge the loop or disable a bound.
    pub fn from_env() -> Option<Self> {
        let on = std::env::var("AUTO_BACKFILL")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !on {
            return None;
        }
        let env_i64 = |k: &str, default: i64| -> i64 {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        };
        Some(Self {
            interval: Duration::from_secs(env_i64("AUTO_BACKFILL_INTERVAL_SECS", 60).max(1) as u64),
            default_window_secs: env_i64("CATCHUP_DEFAULT_WINDOW_SECS", 86_400).max(0),
            default_max_runs: env_i64("CATCHUP_DEFAULT_MAX_RUNS", 50).clamp(1, CATCHUP_HARD_CAP as i64),
            rerun_failed: std::env::var("AUTO_RERUN_FAILED")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            rerun_max_attempts: env_i64("AUTO_RERUN_MAX_ATTEMPTS", 3).max(1),
            rerun_cooldown_secs: env_i64("AUTO_RERUN_COOLDOWN_SECS", 300).max(1),
            stall_secs: env_i64("RUN_STALL_SECS", 3600).max(1),
        })
    }
}

/// Drive the auto-backfill loop until the process exits. Sweeps only while
/// `is_leader` is set; a follower idles (the dedup ledger makes a stray double
/// sweep safe, but gating keeps the work on one node).
pub async fn run(pool: db::Pool, cfg: Config, is_leader: Arc<AtomicBool>, metrics: Arc<Metrics>) {
    info!(
        interval_secs = cfg.interval.as_secs(),
        catchup_window_secs = cfg.default_window_secs,
        rerun_failed = cfg.rerun_failed,
        "auto-backfill loop running"
    );
    loop {
        tokio::time::sleep(cfg.interval).await;
        if !is_leader.load(Ordering::SeqCst) {
            continue;
        }
        let now = Utc::now();

        // Case 1: catch up missed schedule fires; the sweep also yields the
        // current overdue-count + max-lag for the state gauges.
        let (overdue, max_lag) = match sweep_catchup(&pool, &cfg, now, &metrics).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "catch-up sweep failed");
                (0, 0)
            }
        };

        // Case 2: auto-rerun terminally-failed runs (opt-in).
        if cfg.rerun_failed {
            if let Err(e) = sweep_reruns(&pool, &cfg, now, &metrics).await {
                warn!(error = %e, "auto-rerun sweep failed");
            }
        }

        // Case 3: surface stalled (still-running past SLA) runs as a gauge.
        let stall_cutoff = (now - TimeDelta::seconds(cfg.stall_secs)).to_rfc3339();
        let incomplete = match db::count_incomplete_runs(&pool, &stall_cutoff).await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "stall count failed");
                0
            }
        };
        if incomplete > 0 {
            warn!(
                incomplete,
                stall_secs = cfg.stall_secs,
                "runs running past stall SLA — see scheduler_incomplete_runs gauge"
            );
        }

        // Republish the state gauges (single writer → cheap scrapes downstream).
        metrics.set_backfill_state(
            overdue as u64,
            max_lag.max(0) as u64,
            incomplete.max(0) as u64,
        );
    }
}

/// Catch up every catch-up schedule's missed fires. Returns
/// `(overdue_schedules, max_lag_secs)` for the state gauges: a schedule is
/// *overdue* when at least one fire-time fell between its lower bound and `now`,
/// and the lag is `now - oldest_missed_fire`.
async fn sweep_catchup(
    pool: &db::Pool,
    cfg: &Config,
    now: DateTime<Utc>,
    metrics: &Metrics,
) -> anyhow::Result<(i64, i64)> {
    let schedules = db::list_catchup_schedules(pool).await?;
    let now_s = now.to_rfc3339();
    let mut overdue = 0i64;
    let mut max_lag = 0i64;

    for s in schedules {
        let window = s.catchup_window_secs.unwrap_or(cfg.default_window_secs).max(0);
        let cap = s
            .catchup_max_runs
            .unwrap_or(cfg.default_max_runs)
            .clamp(1, CATCHUP_HARD_CAP as i64) as usize;

        // Lower bound: the later of `last_fired_at` and `now - window`, so a long
        // outage is still clamped to the window (no unbounded historical replay)
        // and a schedule that fired recently only fills the true gap.
        let window_start = now - TimeDelta::seconds(window);
        let lower = s
            .last_fired_at
            .as_deref()
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.with_timezone(&Utc))
            .filter(|t| *t > window_start)
            .unwrap_or(window_start);

        let sched = match Schedule::from_str(&s.cron_expr) {
            Ok(sch) => sch,
            Err(e) => {
                warn!(schedule = %s.id, cron = %s.cron_expr, error = %e, "catch-up: cron no longer parses");
                continue;
            }
        };
        // Enumerate in the schedule's timezone (default UTC) so a missed fire
        // lands on the correct wall-clock instant across DST. The row was
        // tz-validated at write time; a bad value degrades to UTC.
        let tz = parse_tz_or_utc(&s.timezone);

        // Enumerate missed fire-times in (lower, now]. CATCHUP_HARD_CAP bounds the
        // iterator so a wildly-misconfigured schedule cannot iterate millions of
        // past fires in one tick. The per-sweep cap is applied AFTER the dedup
        // check so successive sweeps advance past already-claimed slots instead of
        // repeatedly selecting the same capped page and stalling.
        let all_fires: Vec<DateTime<Utc>> = sched
            .after(&lower.with_timezone(&tz))
            .map(|d| d.with_timezone(&Utc))
            .take_while(|d| *d <= now)
            .take(CATCHUP_HARD_CAP)
            .collect();
        if all_fires.is_empty() {
            continue;
        }
        overdue += 1;
        let lag = (now - all_fires[0]).num_seconds();
        if lag > max_lag {
            max_lag = lag;
        }
        debug!(schedule = %s.id, candidates = all_fires.len(), lag_secs = lag, "catch-up: sweeping missed fires");

        let mut claimed_this_sweep = 0usize;
        for fire in &all_fires {
            if claimed_this_sweep >= cap {
                break;
            }
            let logical = fire.to_rfc3339();
            // Dedup gate: already-claimed slots are skipped without counting
            // against the sweep cap so the next sweep can keep marching forward.
            if !db::claim_backfill_slot(pool, &s.id, &logical, &now_s).await? {
                continue;
            }
            claimed_this_sweep += 1;
            // Re-validate the spec per fire (a bad edit since load must not panic
            // the loop), then create the run on its own transaction. The missed
            // fire's logical date is injected as `{{ scheduled_time }}` so a
            // backfilled run processes *its* interval, not "now".
            let mut params = std::collections::BTreeMap::new();
            params.insert("scheduled_time".to_string(), logical.clone());
            let dag = match DagGraph::from_yaml_with_params(&s.spec, &params) {
                Ok(d) => d,
                Err(e) => {
                    db::release_backfill_slot(pool, &s.id, &logical).await?;
                    warn!(schedule = %s.id, error = %e, "catch-up: spec no longer parses; slot released");
                    break;
                }
            };
            match db::create_run(pool, &dag, &s.spec).await {
                Ok(run_id) => {
                    db::record_backfill_run(pool, &s.id, &logical, &run_id).await?;
                    metrics.inc_runs_created();
                    metrics.inc_catchup_runs();
                    let payload = json!({
                        "run_id": run_id,
                        "schedule_id": s.id,
                        "logical_date": logical,
                        "reason": "catchup",
                    })
                    .to_string();
                    // Best-effort: a missing event must not fail the heal.
                    if let Err(e) = db::enqueue_outbox_event(pool, &run_id, "backfill.catchup", &payload).await {
                        warn!(%run_id, error = %e, "catch-up: outbox enqueue failed");
                    }
                    info!(schedule = %s.id, %run_id, logical_date = %logical, "catch-up: backfilled missed run");
                }
                Err(e) => {
                    // Release so the slot stays reclaimable on the next sweep.
                    db::release_backfill_slot(pool, &s.id, &logical).await?;
                    warn!(schedule = %s.id, logical_date = %logical, error = %e, "catch-up: create_run failed; slot released");
                }
            }
        }
    }

    Ok((overdue, max_lag))
}

/// Auto-rerun terminally-failed runs from their failure frontier, bounded by the
/// per-run attempt cap + cooldown (the `run_reruns` ledger).
async fn sweep_reruns(
    pool: &db::Pool,
    cfg: &Config,
    now: DateTime<Utc>,
    metrics: &Metrics,
) -> anyhow::Result<()> {
    let cooldown_cutoff = (now - TimeDelta::seconds(cfg.rerun_cooldown_secs)).to_rfc3339();
    let now_s = now.to_rfc3339();
    let candidates =
        db::list_failed_runs_for_rerun(pool, cfg.rerun_max_attempts, &cooldown_cutoff, RERUN_BATCH)
            .await?;

    for run_id in candidates {
        // Bump the ledger FIRST: rerun_from_failed flips the run to `running`, so
        // it would no longer match the failed-runs query — recording the attempt
        // up front guarantees the cap is enforced even if the event enqueue below
        // fails. A lost race (another node re-armed it) yields `None` and we skip.
        match db::rerun_from_failed(pool, &run_id).await? {
            Some(reset) => {
                db::bump_rerun_attempt(pool, &run_id, &now_s).await?;
                metrics.inc_auto_reruns();
                let payload = json!({
                    "run_id": run_id,
                    "reset_tasks": reset,
                    "reason": "auto_rerun_incomplete",
                })
                .to_string();
                if let Err(e) = db::enqueue_outbox_event(pool, &run_id, "run.auto_rerun", &payload).await {
                    warn!(%run_id, error = %e, "auto-rerun: outbox enqueue failed");
                }
                info!(%run_id, reset_tasks = reset, "auto-rerun: re-armed failed run from frontier");
            }
            None => debug!(%run_id, "auto-rerun: run no longer rerunnable (race or already re-armed)"),
        }
    }
    Ok(())
}
