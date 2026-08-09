//! Audit trail for control-plane mutations, plus viewer read-only enforcement.
//!
//! **Enterprise-gated** (`--features enterprise`): the audit log (a compliance
//! surface) and the read-only `viewer` role ship with dagron Enterprise. Without
//! that feature this module is a passthrough middleware — no `audit_log` table,
//! no `/api/audit` route, no role enforcement — and every mutation behaves
//! exactly as it did before either existed.
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

/// Middleware: a pure passthrough in this build — nothing is recorded and no
/// role is enforced.
///
/// Auth itself stays with each handler's `AuthUser` extractor, so an
/// unauthenticated mutation still 401s there; this layer adds nothing to it.
#[cfg(not(feature = "enterprise"))]
pub async fn audit_mutations(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let _ = state;
    next.run(req).await
}
