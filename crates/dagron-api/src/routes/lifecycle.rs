//! Workflow lifecycle: version history, and a state that is not deletion.
//!
//! Two holes this closes.
//!
//! **Edits were destructive.** `PUT /api/workflows/{id}` overwrote `spec` in
//! place. There was no version column and no history, so the previous definition
//! was gone — you could not see what a workflow looked like before an edit, let
//! alone put it back. Past *runs* were safe (`task_runs` snapshots its TaskSpec
//! at dispatch), but the definition's own history was not.
//!
//! **The only stop button was DELETE**, which is worse than it looks:
//! `schedules.workflow_id` is `ON DELETE CASCADE`, so deleting a workflow
//! silently takes its cron schedules with it, no warning and no undo. "Stop
//! running this for a while" had to be spelled "destroy it".

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

/// What a workflow is currently allowed to do.
///
/// `paused` and `retired` both refuse to run; they differ in intent and in
/// whether the workflow stays in the default listing. Keeping them apart is the
/// point — "off for now" and "we are done with this" are different answers, and
/// a single `disabled` flag would lose which one was meant.
pub const STATES: [&str; 3] = ["active", "paused", "retired"];

/// The states in which a workflow will not start a run.
pub fn is_runnable(state: &str) -> bool {
    state == "active"
}

/// Map a sqlx error to an opaque 500 — logged server-side, never returned, so a
/// client cannot read database internals (column/constraint/driver text) out of
/// the response body. Mirrors `routes::tokens` / `routes::environments`.
fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "workflow lifecycle db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}

#[derive(Debug, Deserialize)]
pub struct StateRequest {
    /// `active` — normal.
    /// `paused` — schedules do not fire, manual runs refused. Reversible, and
    ///   it does not touch the schedules themselves.
    /// `retired` — soft delete: hidden from the default listing, refuses to
    ///   run, but the definition and its history survive.
    pub state: String,
}

#[derive(Debug, Serialize)]
pub struct StateResponse {
    pub id: String,
    pub state: String,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct WorkflowVersion {
    pub id: String,
    pub version: i64,
    pub name: String,
    pub spec: String,
    pub created_at: String,
    pub created_by: Option<String>,
}

/// Record the current definition as a new version.
///
/// Called on create and on every update, so `workflow_versions` is the history
/// and `workflows.spec` is only the current head — kept there so the scheduler's
/// hot JOIN stays a single indexed row rather than a lookup into history.
///
/// Takes a connection rather than a pool so a caller that needs the write to be
/// atomic with another statement (creation, notably — a workflow must never
/// exist without its version-1 history row backing it) can pass a transaction;
/// one that's fine with best-effort (an edit's history row, where losing it
/// must not fail an edit the user already made) can pass a plain connection.
///
/// Returns the version number written.
pub async fn record_version(
    conn: &mut sqlx::PgConnection,
    workflow_id: &str,
    name: &str,
    spec: &str,
    author: Option<&str>,
) -> Result<i64, sqlx::Error> {
    // Next per-workflow number. Not a global sequence: "version 3" should mean
    // the third edit of *this* workflow, not the third edit in the installation.
    let next: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_versions WHERE workflow_id = $1",
    )
    .bind(workflow_id)
    .fetch_one(&mut *conn)
    .await?;

    sqlx::query(
        "INSERT INTO workflow_versions (id, workflow_id, version, name, spec, created_at, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(workflow_id)
    .bind(next)
    .bind(name)
    .bind(spec)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(author)
    .execute(&mut *conn)
    .await?;

    sqlx::query("UPDATE workflows SET version = $1 WHERE id = $2")
        .bind(next)
        .bind(workflow_id)
        .execute(&mut *conn)
        .await?;

    Ok(next)
}

/// `GET /api/workflows/{id}/versions` — history, newest first.
pub async fn list_versions(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<WorkflowVersion>>, (StatusCode, String)> {
    let rows = sqlx::query_as::<_, WorkflowVersion>(
        "SELECT id, version, name, spec, created_at, created_by
         FROM workflow_versions WHERE workflow_id = $1 ORDER BY version DESC",
    )
    .bind(&id)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    Ok(Json(rows))
}

/// `POST /api/workflows/{id}/state` — pause, retire, or reactivate.
///
/// Note what this does *not* do: it leaves the workflow's schedules alone. A
/// paused workflow keeps its cron rows and resumes on exactly the schedule it
/// had, which is the whole difference from deleting it.
pub async fn set_state(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<StateRequest>,
) -> Result<Json<StateResponse>, (StatusCode, String)> {
    let wanted = body.state.trim().to_ascii_lowercase();
    if !STATES.contains(&wanted.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "unknown workflow state {:?} (expected one of {})",
                body.state,
                STATES.join(", ")
            ),
        ));
    }

    let res = sqlx::query("UPDATE workflows SET state = $1, updated_at = $2 WHERE id = $3")
        .bind(&wanted)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal_msg)?;

    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")));
    }

    tracing::info!(workflow_id = %id, state = %wanted, by = %auth.0.email, "workflow state changed");
    Ok(Json(StateResponse { id, state: wanted }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_active_runs() {
        assert!(is_runnable("active"));
        // Both non-active states refuse to run. They differ in visibility and
        // intent, not in whether work starts.
        assert!(!is_runnable("paused"));
        assert!(!is_runnable("retired"));
    }

    #[test]
    fn paused_and_retired_stay_distinct() {
        // Guards the design: collapsing these to one "disabled" flag loses
        // which was meant — "off for now" versus "we are done with this".
        assert_eq!(STATES.len(), 3);
        for s in ["active", "paused", "retired"] {
            assert!(STATES.contains(&s), "{s} must remain a state");
        }
    }

    #[test]
    fn an_unknown_state_is_not_silently_accepted() {
        // The handler compares against this list; anything else must be
        // refused rather than written through as a state nothing understands.
        assert!(!STATES.contains(&"disabled"));
        assert!(!STATES.contains(&"deleted"));
    }
}
