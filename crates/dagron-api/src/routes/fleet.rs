//! Fleet plane read (`GET /api/fleet`).
//!
//! Registered in every build. The open build answers with a **signpost** — the
//! error names what was attempted, that this build does not carry it, and what
//! build does instead — because a user hits this route at the exact moment they
//! have a second unit to manage. The enterprise implementation lives in
//! `fleet_ee.rs`, `include!`d (not `mod`-declared) so a checkout without the
//! file still formats and builds; see `audit.rs` for the same pattern.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

use crate::auth::AuthUser;
use crate::state::AppState;

/// What the open build says instead of managing a fleet.
pub(crate) const OSS_SIGNPOST: &str = "fleet management (unit registry, enrolment, cohorts, staged \
     rollout, fan-out) is not in this build — \
     https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. This build manages one unit: run \
     the engine on the machine and drive it through this API, `SOURCE=dir`, or GitOps sync \
     (docs/OPERATIONS.md, docs/CONFIG.md).";

#[cfg(feature = "enterprise")]
include!("fleet_ee.rs");

/// `GET /api/fleet` — the units this deployment manages.
pub async fn list_fleet(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    #[cfg(not(feature = "enterprise"))]
    {
        let _ = state;
        Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": OSS_SIGNPOST })),
        ))
    }
    #[cfg(feature = "enterprise")]
    {
        fleet_ee_list(&state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_build_signposts_the_gap_and_the_fallback() {
        assert!(OSS_SIGNPOST.contains("is not in this build"));
        assert!(OSS_SIGNPOST.contains("#what-this-build-does-not-do"));
        assert!(OSS_SIGNPOST.contains("SOURCE=dir"));
    }
}
