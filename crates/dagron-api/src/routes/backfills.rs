//! First-class backfill jobs (fast-win #18).
//!
//! The synchronous `POST /api/schedules/:id/backfill` materializes a whole window
//! in one capped call. These endpoints create a durable, listable, cancellable
//! *job* instead: `POST /api/backfills` snapshots the schedule's cron + timezone +
//! workflow spec and a `[from, to]` range into a `backfills` row, and the engine's
//! leadership-gated pacer (`dagron-engine::backfill_jobs`) fires the range
//! gradually — so a much larger backfill can drip into the cluster over many
//! ticks without stampeding it, and an operator can watch `fired`/`requested` or
//! cancel mid-flight. Slots are deduped through the same `schedule_backfills`
//! ledger as the manual/auto backfills.

use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

/// Absolute ceiling on runs a single paced job will fire. Higher than the
/// synchronous backfill's cap because the job is paced (it drips, not stampedes),
/// but still bounded so a mis-set range can't enqueue unbounded work.
const BACKFILL_JOB_HARD_CAP: usize = 100_000;

const DEFAULT_LIST_LIMIT: i64 = 100;
const MAX_LIST_LIMIT: i64 = 500;

#[derive(Deserialize)]
pub struct CreateBody {
    /// Schedule to backfill (its cron + timezone + workflow spec are snapshotted).
    pub schedule_id: String,
    /// Inclusive lower bound (RFC3339); fire-times strictly after this are fired.
    pub from: String,
    /// Inclusive upper bound (RFC3339).
    pub to: String,
    /// Cap on runs this job fires (clamped to `[1, BACKFILL_JOB_HARD_CAP]`).
    pub max_runs: Option<usize>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct BackfillView {
    pub id: String,
    pub schedule_id: String,
    pub cron_expr: String,
    pub timezone: String,
    pub range_from: String,
    pub range_to: String,
    pub cursor: String,
    pub status: String,
    pub max_runs: i64,
    pub requested: i64,
    pub fired: i64,
    pub created_at: String,
    pub updated_at: String,
}

const VIEW_COLS: &str = "id, schedule_id, cron_expr, timezone, range_from, range_to, \
                         cursor, status, max_runs, requested, fired, created_at, updated_at";

/// `POST /api/backfills` — create a paced backfill job over `[from, to]` for a
/// schedule. 404 if the schedule is unknown, 400 on a bad range / cron / spec or
/// a range with no fire-times. Returns `201` with the job (status `running`); the
/// engine paces it from here.
pub async fn create(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<CreateBody>,
) -> Result<(StatusCode, Json<BackfillView>), (StatusCode, String)> {
    let from = DateTime::parse_from_rfc3339(&body.from)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid 'from' (RFC3339): {e}")))?
        .with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339(&body.to)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid 'to' (RFC3339): {e}")))?
        .with_timezone(&Utc);
    if from >= to {
        return Err((StatusCode::BAD_REQUEST, "'from' must be before 'to'".to_string()));
    }
    let max_runs = body.max_runs.unwrap_or(BACKFILL_JOB_HARD_CAP).clamp(1, BACKFILL_JOB_HARD_CAP);

    // Snapshot the schedule's cron + timezone + workflow spec (404 if gone).
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT s.cron_expr, s.timezone, w.spec
         FROM schedules s JOIN workflows w ON w.id = s.workflow_id
         WHERE s.id = $1",
    )
    .bind(&body.schedule_id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    let Some((cron_expr, timezone, spec_yaml)) = row else {
        return Err((StatusCode::NOT_FOUND, format!("schedule '{}' not found", body.schedule_id)));
    };

    // Validate the cron + spec now so a bad job never reaches the pacer.
    let sched = CronSchedule::from_str(&cron_expr)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid cron '{cron_expr}': {e}")))?;
    let tz = parse_tz(&timezone)?;
    crate::routes::control::parse_and_validate(&spec_yaml)?;

    // Count fire-times strictly after `from` through `to` (bounded), so `requested`
    // reflects what the job will actually fire (min of range count and max_runs).
    let in_range = sched
        .after(&from.with_timezone(&tz))
        .map(|d| d.with_timezone(&Utc))
        .take_while(|d| *d <= to)
        .take(BACKFILL_JOB_HARD_CAP)
        .count();
    if in_range == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "backfill range yields no fire-times for this cron".to_string(),
        ));
    }
    let requested = in_range.min(max_runs) as i64;

    let id = Uuid::new_v4().to_string();
    let now = Utc::now().to_rfc3339();
    // cursor = from: the pacer enumerates strictly after it (sync-backfill parity).
    sqlx::query(
        "INSERT INTO backfills
           (id, schedule_id, cron_expr, timezone, spec, range_from, range_to, cursor,
            status, max_runs, requested, fired, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'running',$9,$10,0,$11,$11)",
    )
    .bind(&id)
    .bind(&body.schedule_id)
    .bind(&cron_expr)
    .bind(&timezone)
    .bind(&spec_yaml)
    .bind(from.to_rfc3339())
    .bind(to.to_rfc3339())
    .bind(from.to_rfc3339())
    .bind(max_runs as i64)
    .bind(requested)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    tracing::info!(backfill = %id, schedule = %body.schedule_id, requested, max_runs, "backfill job created");
    let view = fetch_one(&state, &id).await?.ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "backfill vanished after insert".to_string(),
    ))?;
    Ok((StatusCode::CREATED, Json(view)))
}

#[derive(Deserialize)]
pub struct ListParams {
    pub schedule_id: Option<String>,
    pub limit: Option<i64>,
}

/// `GET /api/backfills?schedule_id=&limit=` — most-recent-first list.
pub async fn list(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<BackfillView>>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT);
    let rows = sqlx::query_as::<_, BackfillView>(&format!(
        "SELECT {VIEW_COLS} FROM backfills
         WHERE ($1::text IS NULL OR schedule_id = $1)
         ORDER BY created_at DESC LIMIT $2"
    ))
    .bind(&params.schedule_id)
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    Ok(Json(rows))
}

/// `GET /api/backfills/:id` — one job for monitoring. 404 if unknown.
pub async fn get(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<BackfillView>, (StatusCode, String)> {
    let view = fetch_one(&state, &id)
        .await?
        .ok_or((StatusCode::NOT_FOUND, format!("backfill '{id}' not found")))?;
    Ok(Json(view))
}

#[derive(Serialize)]
pub struct CancelResponse {
    pub id: String,
    pub cancelled: bool,
}

/// `POST /api/backfills/:id/cancel` — stop pacing a running job. 404 if unknown,
/// 409 if it already finished (completed/cancelled).
pub async fn cancel(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CancelResponse>, (StatusCode, String)> {
    let now = Utc::now().to_rfc3339();
    let n = sqlx::query(
        "UPDATE backfills SET status = 'cancelled', updated_at = $1 WHERE id = $2 AND status = 'running'",
    )
    .bind(&now)
    .bind(&id)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?
    .rows_affected();
    if n > 0 {
        tracing::info!(backfill = %id, "backfill job cancelled");
        return Ok(Json(CancelResponse { id, cancelled: true }));
    }
    // Distinguish unknown (404) from already-terminal (409).
    match fetch_one(&state, &id).await? {
        Some(_) => Err((StatusCode::CONFLICT, format!("backfill '{id}' is not running"))),
        None => Err((StatusCode::NOT_FOUND, format!("backfill '{id}' not found"))),
    }
}

async fn fetch_one(state: &AppState, id: &str) -> Result<Option<BackfillView>, (StatusCode, String)> {
    sqlx::query_as::<_, BackfillView>(&format!("SELECT {VIEW_COLS} FROM backfills WHERE id = $1"))
        .bind(id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal_msg)
}

fn parse_tz(tz: &str) -> Result<chrono_tz::Tz, (StatusCode, String)> {
    chrono_tz::Tz::from_str(tz)
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("unknown timezone '{tz}'")))
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
}
