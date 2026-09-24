//! Audit trail for control-plane mutations, plus viewer read-only enforcement.
//!
//! **The audit log is feature-gated** (`--features enterprise`): it is a compliance surface, and
//! without that feature there is no `audit_log` table and no `/api/audit` route.
//!
//! **Viewer read-only is not.** It was, which let a `viewer` write in an open build: the console
//! only offers the role under enterprise, but `POST /api/users` accepts `groups: ["viewer"]` in
//! both, and a downgrade from enterprise keeps the viewers it had. A role meaning "read-only"
//! that silently grants writes is a bug, not a boundary, so the rule lives in `crate::auth` and
//! this middleware applies it in every build.
//!
//! The enterprise implementation lives in `audit_ee.rs`, which is not part of
//! the open tree. It is `include!`d rather than declared as a module so that a
//! checkout without the file still runs `cargo fmt` — rustfmt resolves every
//! `mod` declaration regardless of `cfg`, and would hard-error on a missing
//! one, while it leaves an inactive `include!` alone. `cargo build` and
//! `cargo test` are happy either way. Items land directly in this module, so
//! `main.rs` spells `routes::audit::ensure_schema`, `::audit_mutations` and
//! `::list_audit` the same way in both builds.

#[cfg(not(feature = "enterprise"))]
use axum::{body::Body, extract::State, http::Request, middleware::Next, response::Response};

#[cfg(not(feature = "enterprise"))]
use crate::state::AppState;

#[cfg(feature = "enterprise")]
include!("audit_ee.rs");

/// Middleware: enforces the read-only `viewer` role. This build records nothing — the audit trail
/// is the enterprise half — but applies the same rule off the same definition.
///
/// An unauthenticated mutation still 401s at the handler's `AuthUser` extractor, not here: this
/// layer only refuses a caller it can identify. It authenticates inside the `is_control_mutation`
/// guard so reads pay nothing, and through `authenticate` rather than the JWT decoder, so a
/// viewer's personal access token cannot do what their session cannot.
#[cfg(not(feature = "enterprise"))]
pub async fn audit_mutations(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if crate::auth::is_control_mutation(req.method(), req.uri().path()) {
        if let Some(claims) = crate::auth::authenticate(req.headers(), &state).await {
            if crate::auth::is_viewer(&claims) {
                return crate::auth::read_only_refusal();
            }
        }
    }
    next.run(req).await
}
