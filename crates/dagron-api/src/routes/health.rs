//! Rich health endpoint backing the sidebar "Scheduler live" widget.
//!
//! Unlike `/healthz` (liveness of this API process only), `GET /api/health`
//! reports the state an operator actually cares about: is the database
//! reachable, is a scheduler holding a fresh leadership lease (i.e. is anything
//! going to fire cron schedules), and the attention counters the chrome
//! surfaces (pending approval gates, dead letters, active runs).

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::auth::AuthUser;
use crate::state::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    /// Always "ok" — the request reaching this handler proves the API is up.
    pub api: String,
    /// Build edition: "oss" or "enterprise" — the UI hides enterprise-only
    /// screens (audit log, viewer role) when the backend can't serve them.
    pub edition: &'static str,
    /// "ok" when the database answered; "error" otherwise (counters are 0).
    pub db: String,
    /// True when any leadership lease (cron/GC singleton roles) is unexpired —
    /// i.e. a scheduler engine is alive and will fire due schedules.
    pub scheduler_leader: bool,
    /// Holder id of the freshest unexpired lease, for the tooltip.
    pub leader_holder: Option<String>,
    pub active_runs: i64,
    /// Tasks parked in `awaiting_approval` — the sidebar approvals badge.
    pub awaiting_approvals: i64,
    pub dead_letters: i64,
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    holder: String,
    lease_expires_at: String,
}

/// `GET /api/health` — DB + scheduler-leadership health and attention counters.
/// Never 500s: a DB failure is itself a health *finding* (`db: "error"`), not an
/// internal error, so the widget can render the outage instead of a blank.
pub async fn health(_auth: AuthUser, State(state): State<AppState>) -> Json<HealthResponse> {
    let mut resp = HealthResponse {
        api: "ok".to_string(),
        edition: if cfg!(feature = "enterprise") { "enterprise" } else { "oss" },
        db: "ok".to_string(),
        scheduler_leader: false,
        leader_holder: None,
        active_runs: 0,
        awaiting_approvals: 0,
        dead_letters: 0,
    };

    let counts: Result<(i64, i64, i64), sqlx::Error> = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM workflow_runs WHERE status IN ('pending','running')),
            (SELECT COUNT(*) FROM task_runs WHERE status = 'awaiting_approval'),
            (SELECT COUNT(*) FROM dead_letters)",
    )
    .fetch_one(&state.read_pool)
    .await;
    match counts {
        Ok((active, approvals, dead)) => {
            resp.active_runs = active;
            resp.awaiting_approvals = approvals;
            resp.dead_letters = dead;
        }
        Err(e) => {
            tracing::warn!(error = ?e, "health: counter query failed");
            resp.db = "error".to_string();
            return Json(resp);
        }
    }

    // Leadership: any unexpired lease means an engine is alive. Timestamps are
    // RFC-3339 strings; compare as timestamptz. The table may not exist yet on a
    // fresh DB (engine migrations not run) — treat that as "no leader", not 500.
    let lease: Result<Option<LeaseRow>, sqlx::Error> = sqlx::query_as(
        "SELECT holder, lease_expires_at FROM leader_election
         WHERE lease_expires_at::timestamptz > now()
         ORDER BY lease_expires_at DESC LIMIT 1",
    )
    .fetch_optional(&state.read_pool)
    .await;
    if let Ok(Some(row)) = lease {
        resp.scheduler_leader = true;
        resp.leader_holder = Some(row.holder);
        let _ = row.lease_expires_at;
    }

    Json(resp)
}
