//! Failure triage — recording that a human dealt with a failed run.
//!
//! `status` says what the engine did. It has no way to say what a person then
//! decided, so a failed run stayed failed forever and any "needs attention"
//! list built on it could only drain by the failure ageing out. That is not
//! triage, it is forgetting: the queue empties whether or not anyone looked.
//!
//! This is deliberately *not* the dead-letter queue. `dead_letters` holds
//! submissions that never became runs — payload, error, source. A failed run has
//! tasks, logs and its own recovery actions, and one table pretending to be both
//! would serve neither.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

/// What a person concluded about a failure.
///
/// Three states, because collapsing them loses the distinction that makes the
/// record worth keeping: "someone is on it" is not "it is dealt with", and
/// neither is "we have decided not to care". A single `triaged` flag would turn
/// this into a mark-as-read button.
pub const STATES: [&str; 3] = ["acknowledged", "resolved", "ignored"];

fn describe_states() -> String {
    format!("expected one of {}", STATES.join(", "))
}

#[derive(Debug, Deserialize)]
pub struct TriageRequest {
    /// `acknowledged` — seen, being worked on; still open.
    /// `resolved` — dealt with (rerun, fixed upstream, data corrected).
    /// `ignored` — a real failure we have decided to accept.
    pub state: String,
    /// Why. Optional, but this is the field that is worth reading in three
    /// months, so the UI should ask for it on `ignored`.
    pub note: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TriageResponse {
    pub run_id: String,
    pub triage_state: Option<String>,
    pub triage_note: Option<String>,
    pub triaged_at: Option<String>,
    pub triaged_by: Option<String>,
}

/// `POST /api/runs/{id}/triage` — record (or change) the decision on a run.
///
/// Re-triaging is allowed and overwrites: a failure first acknowledged and later
/// resolved is the normal path, not an error.
pub async fn set_triage(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<TriageRequest>,
) -> Result<Json<TriageResponse>, (StatusCode, String)> {
    let wanted = body.state.trim().to_ascii_lowercase();
    if !STATES.contains(&wanted.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("unknown triage state {:?} ({})", body.state, describe_states()),
        ));
    }
    let note = body.note.as_deref().map(str::trim).filter(|n| !n.is_empty());
    let now = chrono::Utc::now().to_rfc3339();

    let res = sqlx::query(
        "UPDATE workflow_runs
            SET triage_state = $1, triage_note = $2, triaged_at = $3, triaged_by = $4
          WHERE id = $5",
    )
    .bind(&wanted)
    .bind(note)
    .bind(&now)
    .bind(&auth.0.email)
    .bind(&id)
    .execute(&state.write_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }

    tracing::info!(run_id = %id, state = %wanted, by = %auth.0.email, "run triaged");
    Ok(Json(TriageResponse {
        run_id: id,
        triage_state: Some(wanted),
        triage_note: note.map(str::to_string),
        triaged_at: Some(now),
        triaged_by: Some(auth.0.email),
    }))
}

/// `DELETE /api/runs/{id}/triage` — undo, putting the run back in the queue.
///
/// Triage is a human judgement and humans mis-click. Without this the only way
/// back into the attention list would be to fail the run again.
pub async fn clear_triage(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<TriageResponse>, (StatusCode, String)> {
    let res = sqlx::query(
        "UPDATE workflow_runs
            SET triage_state = NULL, triage_note = NULL, triaged_at = NULL, triaged_by = NULL
          WHERE id = $1",
    )
    .bind(&id)
    .execute(&state.write_pool)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    }
    tracing::info!(run_id = %id, by = %auth.0.email, "run triage cleared");
    Ok(Json(TriageResponse {
        run_id: id,
        triage_state: None,
        triage_note: None,
        triaged_at: None,
        triaged_by: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_three_states_stay_distinct() {
        // Guards the design, not the spelling: if these ever collapse to one
        // flag, triage becomes a mark-as-read button and the record stops being
        // worth keeping.
        assert_eq!(STATES.len(), 3);
        assert!(STATES.contains(&"acknowledged"), "someone is on it");
        assert!(STATES.contains(&"resolved"), "dealt with");
        assert!(STATES.contains(&"ignored"), "accepted on purpose");
    }

    #[test]
    fn the_error_message_lists_what_is_allowed() {
        // A rejection that does not say what would have worked just costs the
        // caller another round trip.
        let msg = describe_states();
        for s in STATES {
            assert!(msg.contains(s), "{msg:?} should mention {s}");
        }
    }
}
