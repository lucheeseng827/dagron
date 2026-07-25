//! Shared application state for the dagron management API.
//!
//! `AppState` is cheap to clone (PgPool and broadcast::Sender are Arc-backed),
//! so it is handed to every handler via axum's `State` extractor.

use std::sync::Arc;

use dagron_identity::IdentityProvider;
use serde::Serialize;
use sqlx::postgres::PgPool;
use tokio::sync::{broadcast, Mutex};

/// A task-state-change event, carrying the affected run_id from the
/// `task_events` NOTIFY payload (see engine `db::postgres::notify`).
/// Fanned out from one shared `PgListener` to all SSE clients (populated in 01-04).
#[derive(Debug, Clone, Serialize)]
pub struct TaskEvent {
    pub run_id: String,
}

/// Shared, cheaply-cloneable state for all routes.
#[derive(Clone)]
pub struct AppState {
    /// Pool for read queries (list/inspect/graph/logs). Points at the primary
    /// for now; a read-replica connection string can be swapped in later
    /// without touching handlers.
    pub read_pool: PgPool,
    /// Pool for control mutations (cancel/retry/submit). Primary.
    pub write_pool: PgPool,
    /// Broadcast channel fed by the shared `task_events` listener (01-04);
    /// each SSE client subscribes a receiver and filters by run_id.
    pub tx: broadcast::Sender<TaskEvent>,
    /// HMAC secret dagron-api uses to both sign (login) and validate its own
    /// HS256 session JWT. Self-contained — no external IdP.
    pub jwt_secret: String,
    /// Whether the session cookie is marked `Secure` (HTTPS-only). Defaults to
    /// true; set `DAGRON_COOKIE_SECURE=false` for plain-HTTP local dev.
    pub cookie_secure: bool,
    /// Authentication backend behind the identity seam. Default =
    /// `LocalIdentityProvider` (argon2 against the `users` table); an alternate
    /// provider can plug an SSO backend in here behind the same trait.
    pub identity: Arc<dyn IdentityProvider>,
    /// Programmatic artifact store (`put`/`get` by key). `Some` when
    /// `DAGRON_ARTIFACT_DIR` is set — transparently envelope-encrypted at rest when
    /// a KEK provider is configured (`dagron_artifact::store_from_env`). `None`
    /// disables the artifact endpoints (503).
    pub artifact_store: Option<Arc<dyn dagron_artifact::ArtifactStore>>,
    /// Single-flight guard for the store-wide key-rotation admin endpoint: only one
    /// rotation may run at a time (a second concurrent request gets `409`), so
    /// overlapping sweeps can't widen the write-vs-rotate window or double the work.
    pub rotation_lock: Arc<Mutex<()>>,
    /// Per-client budget for `POST /api/login` — the one unauthenticated route
    /// that costs an Argon2 verify per call. See [`crate::ratelimit`].
    pub login_limiter: Arc<crate::ratelimit::RateLimiter>,
}
