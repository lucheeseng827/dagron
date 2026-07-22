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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{info, warn};

use crate::dag::DagGraph;
use crate::db;
use crate::metrics::Metrics;
use crate::schedule_time::{gate_context, next_fire_after, parse_tz_or_utc, passes_when, should_stop};

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
            // ── stopStrategy: auto-stop before firing if the expression trips ──
            // Evaluate against this schedule's run outcome counts. On a true
            // result, disable the schedule (recording why) and do NOT fire or
            // advance — it won't be claimed again.
            if let Some(expr) = &s.stop_expr {
                match db::schedule_run_counts(&pool, &s.id).await {
                    Ok((succeeded, failed, total)) => match should_stop(expr, succeeded, failed, total) {
                        Ok(true) => {
                            let reason = format!("{expr} (succeeded={succeeded} failed={failed} total={total})");
                            if let Err(e) = db::stop_schedule(&pool, &s.id, &reason, &now_s).await {
                                warn!(schedule = %s.id, error = %e, "stop_schedule failed");
                            } else {
                                metrics.inc_schedules_stopped();
                                info!(schedule = %s.id, %reason, "schedule auto-stopped (stopStrategy)");
                            }
                            continue; // stopped — skip fire + advance
                        }
                        Ok(false) => {}
                        Err(e) => warn!(schedule = %s.id, error = %e, stop_expr = %expr, "stopStrategy expression invalid — ignoring"),
                    },
                    Err(e) => warn!(schedule = %s.id, error = %e, "stopStrategy count query failed — firing anyway"),
                }
            }

            // ── when: gate — skip this fire when the condition is false ────────
            // Evaluated against the scheduled time's calendar fields in the
            // schedule's timezone. A malformed gate fires-on-error (a typo must
            // not silently stop a pipeline) but is logged.
            let gated = match &s.when_expr {
                Some(expr) => {
                    let tz = parse_tz_or_utc(&s.timezone);
                    let scheduled = DateTime::parse_from_rfc3339(&s.next_fire_at)
                        .map(|d| d.with_timezone(&Utc))
                        .unwrap_or(now);
                    let ctx = gate_context(scheduled, tz);
                    match passes_when(expr, &ctx) {
                        Ok(passes) => !passes,
                        Err(e) => {
                            warn!(schedule = %s.id, error = %e, when = %expr, "when gate invalid — firing anyway");
                            false
                        }
                    }
                }
                None => false,
            };

            if gated {
                metrics.inc_schedule_gated();
                info!(schedule = %s.id, when = s.when_expr.as_deref().unwrap_or(""), scheduled_time = %s.next_fire_at, "schedule fire gated (when: false) — skipping");
            } else {
                // Fire: submit the workflow's spec as a run (validated at save
                // time, but re-check so a bad edit can't panic the loop). The
                // row's nominal due time is injected as the `{{ scheduled_time }}`
                // parameter so tasks can reference their logical date.
                let mut params = std::collections::BTreeMap::new();
                params.insert("scheduled_time".to_string(), s.next_fire_at.clone());
                // `{{ env.* }}` variables from the spec's declared environment;
                // an unknown environment skips the fire (and logs) rather than
                // running without its variables.
                let parsed = match crate::environments::template_params(&pool, &s.spec).await {
                    Ok(extra) => {
                        params.extend(extra);
                        DagGraph::from_yaml_with_params(&s.spec, &params)
                    }
                    Err(e) => Err(e),
                };
                match parsed {
                    Ok(dag) => match db::create_run(&pool, &dag, &s.spec).await {
                        Ok(run_id) => {
                            metrics.inc_runs_created();
                            // Stamp the run with its schedule so stopStrategy can
                            // count its outcome (best-effort — the run still ran).
                            if let Err(e) = db::stamp_run_schedule(&pool, &run_id, &s.id).await {
                                warn!(schedule = %s.id, %run_id, error = %e, "stamp_run_schedule failed");
                            }
                            info!(schedule = %s.id, %run_id, name = %dag.spec.name, scheduled_time = %s.next_fire_at, "schedule fired run");
                        }
                        Err(e) => warn!(schedule = %s.id, error = %e, "schedule create_run failed"),
                    },
                    Err(e) => warn!(schedule = %s.id, error = %e, "scheduled workflow no longer parses"),
                }
            }

            // Advance to the next fire time regardless of fire/gate outcome (the
            // next fire is independent), so a transient failure or a gated slot
            // doesn't wedge the row. Evaluated in the schedule's timezone
            // (default UTC) so DST shifts the UTC instant, not the wall clock. A
            // gated fire advances next_fire_at only (last_fired_at unchanged,
            // since nothing ran).
            let next = next_fire_after(&s.cron_expr, &s.timezone, now).unwrap_or_else(|e| {
                warn!(schedule = %s.id, error = %e, "advance: cron/timezone no longer valid");
                None
            });
            let next_s = next
                .map(|d| d.to_rfc3339())
                .unwrap_or_else(|| (now + chrono::TimeDelta::days(FAR_FUTURE_DAYS)).to_rfc3339());
            let advanced = if gated {
                db::advance_schedule_gated(&pool, &s.id, &next_s, &now_s).await
            } else {
                db::advance_schedule(&pool, &s.id, &next_s, &now_s).await
            };
            if let Err(e) = advanced {
                warn!(schedule = %s.id, error = %e, "advance_schedule failed");
            }
        }
    }
}
