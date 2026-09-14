//! Mount adapter for the `dagron-state` component.
//!
//! `dagron-state` compiles a backfill planner's *state plan* — the minimal set of
//! SQL models to rebuild — into a dagron run graph. It is deliberately a separable
//! component: it links neither the planner (which reaches it over that planner's
//! frozen JSON contract) nor `dagron-core`, and it knows nothing about
//! [`AppState`], sqlx or dagron's identity provider. Its single seam is
//! [`PlanSubmitter`].
//!
//! This file is that seam's dagron implementation, and it is the *whole* coupling:
//! together with the two lines in `main.rs` that nest it, deleting this file
//! detaches the component. Standing it up as its own service means implementing
//! [`PlanSubmitter`] over `POST /api/runs` instead of in-process, and moving the
//! directory. See `crates/dagron-state/README.md`.
//!
//! Note this is the **`state`** noun, not `backfill`. dagron's `backfills` are
//! schedule-interval catch-up over `[from, to]`, driven by cron fire-times; a state
//! plan has no cron and is a topologically ordered set of models, so it targets the
//! run-submit path rather than the `backfills` table.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use dagron_state::{PlanSubmitter, SubmitError};

use crate::state::AppState;

/// Submits compiled plans through dagron-api's own run-submit path — the same
/// parse → expand → validate pipeline `POST /api/runs` uses, so a plan cannot enter
/// by a laxer door than a hand-written workflow.
pub struct ApiSubmitter {
    state: AppState,
}

impl ApiSubmitter {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

impl PlanSubmitter for ApiSubmitter {
    fn submit(&self, yaml: String) -> impl Future<Output = Result<String, SubmitError>> + Send {
        let state = self.state.clone();
        async move {
            // No caller parameters: a compiled plan is already fully rendered — the
            // planner resolved every model name and partition list before dagron
            // saw it, so there is nothing left to substitute.
            let params = BTreeMap::new();
            crate::routes::control::submit_yaml_with_params(&state, &yaml, &yaml, &params)
                .await
                .map_err(|(status, message)| match status {
                    // The spec was rejected by the validator: the caller's plan or
                    // command template is at fault, and the status must stay 4xx
                    // rather than becoming a 500 the caller cannot act on.
                    StatusCode::BAD_REQUEST => SubmitError::Rejected(message),
                    StatusCode::SERVICE_UNAVAILABLE => SubmitError::Unavailable(message),
                    _ => SubmitError::Internal(message),
                })
        }
    }
}

/// Reject unauthenticated callers before the component sees the request.
///
/// The component ships no auth of its own by design — it cannot know what a
/// credential means in its host. `POST /plans/submit` creates runs, so the host has
/// to supply that answer, and this is dagron's. It reuses [`crate::auth::authenticate`],
/// the single entry point the `AuthUser` extractor and the audit middleware both
/// go through, so these routes accept exactly the credentials every other mutating
/// route accepts — session cookie or PAT — and no others.
async fn require_auth(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    match crate::auth::authenticate(req.headers(), &state).await {
        Some(_claims) => next.run(req).await,
        None => (StatusCode::UNAUTHORIZED, "authentication required").into_response(),
    }
}

/// The mounted component: its router, behind dagron's auth.
///
/// Nested by `main.rs` at [`dagron_state::MOUNT_PREFIX`]. The outer router's
/// TraceLayer, CORS and audit middleware wrap it like any other route, because
/// they are layered on the app above this mount.
pub fn router(state: AppState) -> Router {
    dagron_state::router(Arc::new(ApiSubmitter::new(state.clone())))
        .layer(axum::middleware::from_fn_with_state(state, require_auth))
}
