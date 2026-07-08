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
use chrono_tz::Tz;
use cron::Schedule;
use serde::Deserialize;
use tracing::{info, warn};

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;
use crate::schedule_time::{gate_context, next_fire_in_tz, passes_when, validate_tz};

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
    /// Optional IANA timezone (e.g. `America/New_York`) the `cron` expression is
    /// interpreted in. Absent / `UTC` = UTC (the historical behavior).
    #[serde(default)]
    timezone: Option<String>,
    /// Optional per-fire conditional gate evaluated against the scheduled time's
    /// calendar fields (`hour`/`day`/`weekday`/`days_in_month`/…). When it is
    /// false the fire is skipped and only the next fire time advances — e.g.
    /// `when: "{{ weekday }} <= 5"` (weekdays only). Absent = always fire.
    #[serde(default)]
    when: Option<String>,
}

/// A loaded, validated cron schedule: its parsed expression, the timezone it is
/// evaluated in, an optional `when:` gate, the DAG YAML to submit, and the next
/// time it is due.
pub struct CronEntry {
    name: String,
    schedule: Schedule,
    tz: Tz,
    when: Option<String>,
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
        let tz_name = e.timezone.as_deref().unwrap_or("UTC");
        let tz = validate_tz(tz_name)
            .with_context(|| format!("invalid timezone '{tz_name}' for schedule '{}'", e.name))?;
        let dag_yaml = tokio::fs::read_to_string(&e.dag)
            .await
            .with_context(|| format!("reading DAG '{}' for schedule '{}'", e.dag, e.name))?;
        // Validate the DAG now so a bad spec can't silently no-op every fire.
        DagGraph::from_yaml(&dag_yaml)
            .with_context(|| format!("invalid DAG '{}' for schedule '{}'", e.dag, e.name))?;
        let next = next_fire_in_tz(&schedule, tz, now)
            .with_context(|| format!("cron '{}' for '{}' has no upcoming fire time", e.cron, e.name))?;
        info!(name = %e.name, cron = %e.cron, tz = %tz, when = e.when.as_deref().unwrap_or(""), %next, "cron schedule loaded");
        entries.push(CronEntry { name: e.name, schedule, tz, when: e.when, dag_yaml, next });
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
                    // The nominal (scheduled) time is the fire's logical date,
                    // regardless of how late the tick actually ran.
                    let nominal = entry.next;
                    // `when:` gate — skip this fire when the condition is false;
                    // a malformed gate fires-on-error (logged) so a typo can't
                    // silently stop the schedule.
                    let gated = entry.when.as_ref().is_some_and(|expr| {
                        let ctx = gate_context(nominal, entry.tz);
                        match passes_when(expr, &ctx) {
                            Ok(passes) => !passes,
                            Err(e) => {
                                warn!(schedule = %entry.name, error = %e, when = %expr, "when gate invalid — firing anyway");
                                false
                            }
                        }
                    });
                    if gated {
                        metrics.inc_schedule_gated();
                        info!(schedule = %entry.name, when = entry.when.as_deref().unwrap_or(""), scheduled_time = %nominal, "cron fire gated (when: false) — skipping");
                    } else {
                        fire(&pool, entry, nominal, &metrics).await;
                    }
                }
                // Advance regardless of leadership to avoid a backlog of misses.
                // Evaluated in the schedule's timezone so DST shifts the UTC
                // instant but not the wall-clock fire time.
                entry.next = next_fire_in_tz(&entry.schedule, entry.tz, now)
                    // A schedule with nothing left to fire is parked far out.
                    .unwrap_or_else(|| now + chrono::TimeDelta::days(36_500));
            }
        }
    }
}

/// Create one run for a due schedule. The DAG was validated at load, but
/// `create_run` can still fail transiently (DB blip) — log and move on; the next
/// fire is independent. The fire's nominal time is injected as the
/// `{{ scheduled_time }}` parameter (RFC-3339) so tasks can reference their
/// logical date — the data-interval idiom.
async fn fire(pool: &db::Pool, entry: &CronEntry, nominal: DateTime<Utc>, metrics: &Metrics) {
    let mut params = std::collections::BTreeMap::new();
    params.insert("scheduled_time".to_string(), nominal.to_rfc3339());
    match DagGraph::from_yaml_with_params(&entry.dag_yaml, &params) {
        Ok(dag) => match db::create_run(pool, &dag, &entry.dag_yaml).await {
            Ok(run_id) => {
                metrics.inc_runs_created();
                info!(schedule = %entry.name, %run_id, name = %dag.spec.name, scheduled_time = %nominal, "cron fired run");
            }
            Err(e) => warn!(schedule = %entry.name, error = %e, "cron create_run failed"),
        },
        Err(e) => warn!(schedule = %entry.name, error = %e, "cron DAG no longer parses"),
    }
}
