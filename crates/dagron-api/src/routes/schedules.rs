//! Workflow schedules — the UI "schedule drawer" backend.
//!
//! Manages rows in the `schedules` table (workflow + cron expression). The engine
//! (leadership-gated `schedule.rs`) is what actually fires due rows; dagron-api
//! only validates the cron expression and computes `next_fire_at`. `enabled` is
//! stored as INTEGER (0/1) for SQLite/Postgres portability and exposed as bool.

use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use cron::Schedule as CronSchedule;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
struct ScheduleRow {
    id: String,
    workflow_id: String,
    workflow_name: String,
    cron_expr: String,
    enabled: i64,
    // QW3 auto-catchup policy (read by the engine's auto-backfill loop).
    catchup: i64,
    catchup_window_secs: Option<i64>,
    catchup_max_runs: Option<i64>,
    next_fire_at: Option<String>,
    last_fired_at: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Serialize)]
pub struct Schedule {
    pub id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    pub cron_expr: String,
    pub enabled: bool,
    /// Opt this schedule into automatic catch-up of missed fires.
    pub catchup: bool,
    /// Catch-up look-back override (seconds); `None` → engine default.
    pub catchup_window_secs: Option<i64>,
    /// Per-sweep run-cap override; `None` → engine default.
    pub catchup_max_runs: Option<i64>,
    pub next_fire_at: Option<String>,
    pub last_fired_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<ScheduleRow> for Schedule {
    fn from(r: ScheduleRow) -> Self {
        Schedule {
            id: r.id,
            workflow_id: r.workflow_id,
            workflow_name: r.workflow_name,
            cron_expr: r.cron_expr,
            enabled: r.enabled != 0,
            catchup: r.catchup != 0,
            catchup_window_secs: r.catchup_window_secs,
            catchup_max_runs: r.catchup_max_runs,
            next_fire_at: r.next_fire_at,
            last_fired_at: r.last_fired_at,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

const SELECT: &str = "SELECT s.id AS id, s.workflow_id AS workflow_id, w.name AS workflow_name,
        s.cron_expr AS cron_expr, s.enabled AS enabled,
        s.catchup AS catchup, s.catchup_window_secs AS catchup_window_secs,
        s.catchup_max_runs AS catchup_max_runs, s.next_fire_at AS next_fire_at,
        s.last_fired_at AS last_fired_at, s.created_at AS created_at, s.updated_at AS updated_at
 FROM schedules s JOIN workflows w ON w.id = s.workflow_id";

#[derive(Deserialize)]
pub struct ListParams {
    pub workflow_id: Option<String>,
}

/// `GET /api/schedules?workflow_id=` — all schedules, or one workflow's.
pub async fn list_schedules(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<Schedule>>, StatusCode> {
    let rows = if let Some(wid) = params.workflow_id {
        sqlx::query_as::<_, ScheduleRow>(&format!("{SELECT} WHERE s.workflow_id = $1 ORDER BY s.created_at"))
            .bind(wid)
            .fetch_all(&state.read_pool)
            .await
    } else {
        sqlx::query_as::<_, ScheduleRow>(&format!("{SELECT} ORDER BY s.created_at"))
            .fetch_all(&state.read_pool)
            .await
    }
    .map_err(internal)?;
    Ok(Json(rows.into_iter().map(Schedule::from).collect()))
}

#[derive(Deserialize)]
pub struct CreateBody {
    pub workflow_id: String,
    pub cron_expr: String,
    pub enabled: Option<bool>,
    /// Opt into automatic catch-up of missed fires (QW3 auto-catchup). Default false.
    pub catchup: Option<bool>,
    /// Catch-up look-back override (seconds); omit to use the engine default.
    pub catchup_window_secs: Option<i64>,
    /// Per-sweep run-cap override; omit to use the engine default.
    pub catchup_max_runs: Option<i64>,
}

/// Reject negative catch-up limits before they reach the DB or the engine config.
fn validate_catchup_policy(
    catchup_window_secs: Option<i64>,
    catchup_max_runs: Option<i64>,
) -> Result<(), (StatusCode, String)> {
    if matches!(catchup_window_secs, Some(v) if v < 0) {
        return Err((StatusCode::BAD_REQUEST, "catchup_window_secs must be non-negative".to_string()));
    }
    if matches!(catchup_max_runs, Some(v) if v < 0) {
        return Err((StatusCode::BAD_REQUEST, "catchup_max_runs must be non-negative".to_string()));
    }
    Ok(())
}

/// `POST /api/schedules` — validate the cron expr, compute the first fire, insert.
pub async fn create_schedule(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> Result<(StatusCode, Json<Schedule>), (StatusCode, String)> {
    validate_catchup_policy(body.catchup_window_secs, body.catchup_max_runs)?;
    let next = next_fire(&body.cron_expr)?;

    // 404 if the workflow doesn't exist (clearer than an FK violation).
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM workflows WHERE id = $1")
        .bind(&body.workflow_id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal_msg)?;
    if exists.is_none() {
        return Err((StatusCode::NOT_FOUND, format!("workflow '{}' not found", body.workflow_id)));
    }

    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let enabled: i64 = if body.enabled.unwrap_or(true) { 1 } else { 0 };
    let catchup: i64 = if body.catchup.unwrap_or(false) { 1 } else { 0 };

    sqlx::query(
        "INSERT INTO schedules
           (id, workflow_id, cron_expr, enabled, catchup, catchup_window_secs, catchup_max_runs,
            next_fire_at, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)",
    )
    .bind(&id)
    .bind(&body.workflow_id)
    .bind(&body.cron_expr)
    .bind(enabled)
    .bind(catchup)
    .bind(body.catchup_window_secs)
    .bind(body.catchup_max_runs)
    .bind(&next)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    let row = fetch_one(&state, &id).await?;
    Ok((StatusCode::CREATED, Json(row)))
}

#[derive(Deserialize)]
pub struct UpdateBody {
    pub cron_expr: Option<String>,
    pub enabled: Option<bool>,
    /// Toggle automatic catch-up. Re-enabling catch-up after a pause is the
    /// canonical moment this matters — the engine then heals the paused window.
    pub catchup: Option<bool>,
    pub catchup_window_secs: Option<i64>,
    pub catchup_max_runs: Option<i64>,
}

/// `PUT /api/schedules/:id` — change cron / enabled / catch-up policy; recompute
/// next fire when the expression changes or the schedule is (re)enabled. Each
/// catch-up field is left unchanged when its key is omitted from the body.
pub async fn update_schedule(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpdateBody>,
) -> Result<Json<Schedule>, (StatusCode, String)> {
    let current = sqlx::query_as::<_, ScheduleRow>(&format!("{SELECT} WHERE s.id = $1"))
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal_msg)?
        .ok_or((StatusCode::NOT_FOUND, format!("schedule '{id}' not found")))?;

    let was_enabled = current.enabled != 0;
    let cron_changed = body.cron_expr.as_ref().map_or(false, |e| e != &current.cron_expr);
    let cron_expr = body.cron_expr.unwrap_or_else(|| current.cron_expr.clone());
    let enabled = body.enabled.unwrap_or(was_enabled);
    let catchup = body.catchup.unwrap_or(current.catchup != 0);
    // A present key overrides; an absent one keeps the stored value.
    let catchup_window_secs = body.catchup_window_secs.or(current.catchup_window_secs);
    let catchup_max_runs = body.catchup_max_runs.or(current.catchup_max_runs);
    validate_catchup_policy(catchup_window_secs, catchup_max_runs)?;
    // Recompute next_fire_at only when the cron expression changed, the schedule
    // transitions from disabled to enabled, or the stored value is missing — so a
    // catchup-only update does not move an already-scheduled fire slot.
    let next = if !enabled {
        None
    } else if cron_changed || (!was_enabled && enabled) || current.next_fire_at.is_none() {
        Some(next_fire(&cron_expr)?)
    } else {
        current.next_fire_at.clone()
    };
    let now = chrono::Utc::now().to_rfc3339();

    sqlx::query(
        "UPDATE schedules
         SET cron_expr=$2, enabled=$3, catchup=$4, catchup_window_secs=$5,
             catchup_max_runs=$6, next_fire_at=$7, updated_at=$8
         WHERE id=$1",
    )
    .bind(&id)
    .bind(&cron_expr)
    .bind(if enabled { 1i64 } else { 0 })
    .bind(if catchup { 1i64 } else { 0 })
    .bind(catchup_window_secs)
    .bind(catchup_max_runs)
    .bind(&next)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    Ok(Json(fetch_one(&state, &id).await?))
}

/// `DELETE /api/schedules/:id`.
pub async fn delete_schedule(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let n = sqlx::query("DELETE FROM schedules WHERE id = $1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 { Err(StatusCode::NOT_FOUND) } else { Ok(StatusCode::NO_CONTENT) }
}

// ── Backfill (QW3) ─────────────────────────────────────────────────────────────

/// Hard ceiling on runs materialized by a single backfill call — the bound that
/// keeps a wide range from stampeding the cluster. `max_runs` may lower it but
/// never raise it past this.
const BACKFILL_HARD_CAP: usize = 1000;

#[derive(Deserialize)]
pub struct BackfillBody {
    /// Inclusive lower bound (RFC3339); fire-times strictly after this are used.
    pub from: String,
    /// Inclusive upper bound (RFC3339).
    pub to: String,
    /// Cap on runs created this call (clamped to `[1, BACKFILL_HARD_CAP]`).
    pub max_runs: Option<usize>,
}

#[derive(Serialize)]
pub struct BackfillResponse {
    /// Runs newly created this call.
    pub scheduled: usize,
    /// Fire-times already materialized by a prior backfill (deduped, not re-run).
    pub skipped: usize,
    pub from: String,
    pub to: String,
    pub run_ids: Vec<String>,
}

/// `POST /api/schedules/:id/backfill` — materialize the schedule's missed runs
/// across `[from, to]` (QW3). Enumerates the cron fire-times in range, **caps the
/// count** (`max_runs`, hard ceiling [`BACKFILL_HARD_CAP`]) so a wide window can't
/// stampede, then submits one run per fire-time through the same create_run path
/// as a normal submit.
///
/// Re-issuing the same window is safe: each fire-time is a slot in the
/// `schedule_backfills` ledger, claimed with `INSERT ... ON CONFLICT DO NOTHING`,
/// so a slot already materialized by a prior call is skipped (reported in
/// `skipped`) rather than double-run.
///
/// NOTE (MVP scope): runs are created directly rather than enqueued through the
/// engine's `MAX_INFLIGHT_RUNS` admission valve, so actual concurrency is bounded
/// by the engine's `WORKER_COUNT` and this per-call cap, not the valve. A durable
/// queue-backed backfill (survives a restart mid-call) is the remaining beta
/// follow-up documented in the roadmap.
pub async fn backfill(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<BackfillBody>,
) -> Result<Json<BackfillResponse>, (StatusCode, String)> {
    use chrono::{DateTime, Utc};

    let from = DateTime::parse_from_rfc3339(&body.from)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid 'from' (RFC3339): {e}")))?
        .with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339(&body.to)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid 'to' (RFC3339): {e}")))?
        .with_timezone(&Utc);
    if from >= to {
        return Err((StatusCode::BAD_REQUEST, "'from' must be before 'to'".to_string()));
    }
    let cap = body.max_runs.unwrap_or(BACKFILL_HARD_CAP).clamp(1, BACKFILL_HARD_CAP);

    // Load the schedule's cron + its workflow spec (404 if the schedule is gone).
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT s.cron_expr, w.spec
         FROM schedules s JOIN workflows w ON w.id = s.workflow_id
         WHERE s.id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    let Some((cron_expr, spec_yaml)) = row else {
        return Err((StatusCode::NOT_FOUND, format!("schedule '{id}' not found")));
    };

    let sched = CronSchedule::from_str(&cron_expr)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid cron '{cron_expr}': {e}")))?;

    // Enumerate fire-times in (from, to]. Bound the (infinite) cron iterator at
    // cap+1 so we can detect — and reject — an over-wide range without looping.
    let fires: Vec<DateTime<Utc>> = sched
        .after(&from)
        .take_while(|d| *d <= to)
        .take(cap + 1)
        .collect();
    if fires.len() > cap {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "backfill range yields more than {cap} runs; narrow the range or raise \
                 max_runs (hard cap {BACKFILL_HARD_CAP})"
            ),
        ));
    }

    // Validate the spec once, then create one run per *newly-claimed* fire-time.
    let spec = crate::routes::control::parse_and_validate(&spec_yaml)?;
    let now = Utc::now().to_rfc3339();
    let mut run_ids = Vec::new();
    let mut skipped = 0usize;
    for fire in &fires {
        let logical_date = fire.to_rfc3339();
        // Claim the slot; ON CONFLICT means a prior backfill already ran it.
        let claimed = sqlx::query(
            "INSERT INTO schedule_backfills (schedule_id, logical_date, created_at)
             VALUES ($1, $2, $3) ON CONFLICT (schedule_id, logical_date) DO NOTHING",
        )
        .bind(&id)
        .bind(&logical_date)
        .bind(&now)
        .execute(&state.write_pool)
        .await
        .map_err(internal_msg)?
        .rows_affected();
        if claimed == 0 {
            skipped += 1;
            continue;
        }

        // create_run is its own transaction, so the claim and the run can't share
        // one. If it fails, release the claim (DELETE) so the slot stays reclaimable
        // instead of being permanently counted as `skipped` on a later retry.
        let run_id = match crate::routes::control::create_run(&state, &spec, &spec_yaml).await {
            Ok(run_id) => run_id,
            Err(e) => {
                let _ = sqlx::query(
                    "DELETE FROM schedule_backfills WHERE schedule_id = $1 AND logical_date = $2",
                )
                .bind(&id)
                .bind(&logical_date)
                .execute(&state.write_pool)
                .await;
                return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")));
            }
        };
        // Record which run filled the slot (best-effort; the slot is already claimed).
        sqlx::query(
            "UPDATE schedule_backfills SET run_id = $1 WHERE schedule_id = $2 AND logical_date = $3",
        )
        .bind(&run_id)
        .bind(&id)
        .bind(&logical_date)
        .execute(&state.write_pool)
        .await
        .map_err(internal_msg)?;
        run_ids.push(run_id);
    }

    tracing::info!(schedule_id = %id, scheduled = run_ids.len(), skipped, from = %body.from, to = %body.to, "backfill submitted");
    Ok(Json(BackfillResponse {
        scheduled: run_ids.len(),
        skipped,
        from: body.from,
        to: body.to,
        run_ids,
    }))
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Validate a cron expression and return its next fire time (RFC3339).
/// 400 on a bad expression or one with no upcoming fire.
fn next_fire(expr: &str) -> Result<String, (StatusCode, String)> {
    let sched = CronSchedule::from_str(expr)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid cron '{expr}': {e}")))?;
    let now = chrono::Utc::now();
    sched
        .after(&now)
        .next()
        .map(|d| d.to_rfc3339())
        .ok_or((StatusCode::BAD_REQUEST, format!("cron '{expr}' has no upcoming fire time")))
}

async fn fetch_one(state: &AppState, id: &str) -> Result<Schedule, (StatusCode, String)> {
    sqlx::query_as::<_, ScheduleRow>(&format!("{SELECT} WHERE s.id = $1"))
        .bind(id)
        .fetch_one(&state.read_pool)
        .await
        .map(Schedule::from)
        .map_err(internal_msg)
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}
