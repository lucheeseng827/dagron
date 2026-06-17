//! Cron-triggered run starts (v5).
//!
//! A `WorkflowSource` (v4) is *external* submission — work pushed into the queue.
//! Cron is *time* submission: the scheduler itself originates a run when a
//! schedule fires. Each entry pairs a cron expression with a DAG file; when the
//! next fire time arrives the loop calls [`db::create_run`], exactly as ingestion
//! does, so a cron-started run is indistinguishable from a queued one downstream.
//!
//! **Singleton-gated.** With N schedulers sharing one datastore, only the
//! [leadership](crate::leadership) holder actually fires — otherwise every
//! process would start the same run on every schedule. Non-leaders still advance
//! their own `next` times so that gaining leadership does not unleash a backlog
//! of missed fires; they simply skip the `create_run` call.
//!
//! Cron expressions use the `cron` crate's 6-or-7 field form
//! (`sec min hour day-of-month month day-of-week [year]`), e.g. `0 0 2 * * *`
//! fires at 02:00:00 every day.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::Deserialize;
use tracing::{info, warn};

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;

/// How often the loop wakes to check whether any schedule is due.
const TICK: Duration = Duration::from_secs(1);

#[derive(Debug, Deserialize)]
struct RawConfig {
    schedules: Vec<RawEntry>,
}

#[derive(Debug, Deserialize)]
struct RawEntry {
    name: String,
    cron: String,
    /// Path to the DAG YAML fired on this schedule.
    dag: String,
}

/// A loaded, validated cron schedule: its parsed expression, the DAG YAML to
/// submit, and the next time it is due.
pub struct CronEntry {
    name: String,
    schedule: Schedule,
    dag_yaml: String,
    next: DateTime<Utc>,
}

/// Load and validate a cron config file. Each entry's cron expression is parsed,
/// its DAG file read and checked with [`DagGraph::from_yaml`], and its first fire
/// time computed — so a malformed schedule or DAG fails at startup, not at 2 a.m.
pub async fn load(path: &str) -> Result<Vec<CronEntry>> {
    let text = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("reading cron config '{path}'"))?;
    let raw: RawConfig =
        serde_yaml::from_str(&text).with_context(|| format!("parsing cron config '{path}'"))?;

    let now = Utc::now();
    let mut entries = Vec::with_capacity(raw.schedules.len());
    for e in raw.schedules {
        let schedule = Schedule::from_str(&e.cron)
            .with_context(|| format!("invalid cron expression '{}' for '{}'", e.cron, e.name))?;
        let dag_yaml = tokio::fs::read_to_string(&e.dag)
            .await
            .with_context(|| format!("reading DAG '{}' for schedule '{}'", e.dag, e.name))?;
        // Validate the DAG now so a bad spec can't silently no-op every fire.
        DagGraph::from_yaml(&dag_yaml)
            .with_context(|| format!("invalid DAG '{}' for schedule '{}'", e.dag, e.name))?;
        let next = schedule
            .after(&now)
            .next()
            .with_context(|| format!("cron '{}' for '{}' has no upcoming fire time", e.cron, e.name))?;
        info!(name = %e.name, cron = %e.cron, %next, "cron schedule loaded");
        entries.push(CronEntry { name: e.name, schedule, dag_yaml, next });
    }
    Ok(entries)
}

/// Drive the cron loop until the process exits. Only fires runs while
/// `is_leader` is set; always keeps each entry's `next` advancing so a
/// follower-turned-leader resumes from the present rather than replaying misses.
pub async fn run(
    pool: db::Pool,
    mut entries: Vec<CronEntry>,
    is_leader: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
) {
    info!(schedules = entries.len(), "cron loop running");
    loop {
        tokio::time::sleep(TICK).await;
        let now = Utc::now();
        let leader = is_leader.load(Ordering::SeqCst);

        for entry in &mut entries {
            while entry.next <= now {
                if leader {
                    fire(&pool, entry, &metrics).await;
                }
                // Advance regardless of leadership to avoid a backlog of misses.
                entry.next = entry
                    .schedule
                    .after(&now)
                    .next()
                    // A schedule with nothing left to fire is parked far out.
                    .unwrap_or_else(|| now + chrono::TimeDelta::days(36_500));
            }
        }
    }
}

/// Create one run for a due schedule. The DAG was validated at load, but
/// `create_run` can still fail transiently (DB blip) — log and move on; the next
/// fire is independent.
async fn fire(pool: &db::Pool, entry: &CronEntry, metrics: &Metrics) {
    match DagGraph::from_yaml(&entry.dag_yaml) {
        Ok(dag) => match db::create_run(pool, &dag, &entry.dag_yaml).await {
            Ok(run_id) => {
                metrics.inc_runs_created();
                info!(schedule = %entry.name, %run_id, name = %dag.spec.name, "cron fired run");
            }
            Err(e) => warn!(schedule = %entry.name, error = %e, "cron create_run failed"),
        },
        Err(e) => warn!(schedule = %entry.name, error = %e, "cron DAG no longer parses"),
    }
}
