//! Fleet link (`GET /api/link`, `POST /api/link/enrol`).
//!
//! The instance side of belonging to a fleet: what this deployment is linked
//! to, whether it is licensed, and the one action that enrols it as a unit of
//! a control plane's org. Registered in every build. The open build answers
//! with a **signpost** — the error names what was attempted, that this build
//! does not carry it, and what does — because the operator hits this route at
//! the exact moment they have a second instance to manage. The enterprise
//! implementation lives in `link_ee.rs`, `include!`d (not `mod`-declared) so a
//! checkout without the file still formats and builds; see `fleet.rs` and
//! `audit.rs` for the same pattern.
//!
//! **Generate, then apply.** Enrolment here mints the unit credential on the
//! operator's behalf and hands back the configuration to apply
//! (`DAGRON_FLEET_URL` / `DAGRON_FLEET_TOKEN`); the link takes effect on the
//! engine's next start, and `GET /api/link` reads the resulting state back.
//! This API never writes runtime configuration and never hot-reloads the
//! uplink: a unit is a machine nobody logs into, and a link that "applied
//! itself" would be a link nobody can find the config for six months later.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

use crate::auth::AuthUser;
use crate::state::AppState;

/// What the open build says instead of linking.
pub(crate) const OSS_SIGNPOST: &str = "fleet link (enrolling this instance as a unit of a control \
     plane's org, and the offline licence) is not in this build — \
     https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. This build runs one \
     instance on its own: drive it through this API, `SOURCE=dir`, or GitOps sync \
     (docs/OPERATIONS.md, docs/CONFIG.md).";

#[cfg(feature = "enterprise")]
include!("link_ee.rs");

/// `GET /api/link` — link and licence state of this instance.
pub async fn get_link(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    #[cfg(not(feature = "enterprise"))]
    {
        let _ = (claims, state);
        Err(signpost())
    }
    #[cfg(feature = "enterprise")]
    {
        link_ee_state(&claims, &state).await
    }
}

/// `POST /api/link/enrol` — enrol this instance as a unit; returns the credential once.
pub async fn enrol(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    #[cfg(not(feature = "enterprise"))]
    {
        let _ = (claims, state, body);
        Err(signpost())
    }
    #[cfg(feature = "enterprise")]
    {
        link_ee_enrol(&claims, &state, &body).await
    }
}

#[cfg(not(feature = "enterprise"))]
fn signpost() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({ "error": OSS_SIGNPOST, "edition": "oss" })),
    )
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
