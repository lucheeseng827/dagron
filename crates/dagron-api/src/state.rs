//! Shared application state for the dagron management API.
//!
//! `AppState` is cheap to clone (the pool and broadcast::Sender are Arc-backed),
//! so it is handed to every handler via axum's `State` extractor.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dagron_identity::IdentityProvider;
use serde::Serialize;
use crate::db::Pool;
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
    pub read_pool: Pool,
    /// Pool for control mutations (cancel/retry/submit). Primary.
    pub write_pool: Pool,
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
    /// a KEK provider is configured (`dagron_artifact::store_from_env`). With
    /// `DAGRON_ARTIFACT_TIER=1` it is the tiered store (local tier + budgeted
    /// uplink to `DAGRON_ARTIFACT_URL`), which the periodic loop in `main.rs` and
    /// `POST /api/artifacts/sync` drain through `ArtifactStore::sync`. `None`
    /// disables the artifact endpoints (503).
    pub artifact_store: Option<Arc<dyn dagron_artifact::ArtifactStore>>,
    /// Where task logs are kept once out of the task rows
    /// (`DAGRON_LOG_STORE`, the engine's own setting): the log routes read a
    /// task's closed segments and open part from it. `None`: from the rows.
    pub log_store: Option<Arc<dagron_artifact::log_segments::SegmentStore>>,
    /// How the artifact routes are set: whether a transfer goes straight to
    /// the store, and how long an artifact is kept. See
    /// [crate::routes::artifact_index::Settings].
    #[cfg(feature = "postgres")]
    pub artifacts: crate::routes::artifact_index::Settings,
    /// Single-flight guard shared by the two store-wide sweeps — key rotation
    /// (`POST /api/artifacts/rotate`) and the tiered drain
    /// (`POST /api/artifacts/sync`): a second concurrent request of either kind
    /// gets `409`. One lock for both on purpose: a drain overlapping a re-key
    /// would upload objects still wrapped under the retiring KEK, and overlapping
    /// sweeps of one kind only widen the write-vs-sweep window and double the work.
    pub rotation_lock: Arc<Mutex<()>>,
    /// Per-client budget for `POST /api/login` — the one unauthenticated route
    /// that costs an Argon2 verify per call. See [`crate::ratelimit`].
    pub login_limiter: Arc<crate::ratelimit::RateLimiter>,
    /// True while the shared `task_events` listener is subscribed. `/readyz`
    /// folds it in: a replica whose LISTEN session is down serves stale SSE
    /// and long-polls, so it should not be routed new traffic (LOW_LATENCY R-1).
    pub listener_ready: Arc<AtomicBool>,
    /// Who each service token was, for a few seconds.
    pub token_cache: Arc<crate::auth::TokenCache>,
    /// The OIDC issuer, where `DAGRON_OIDC_ISSUER` is set: its keys
    /// and how long a token lasts. `None`: its routes answer 404.
    pub oidc: Option<Arc<crate::oidc::Issuer>>,
    /// The live log relay: one reader of each run somebody
    /// follows, for every stream of it (`routes::logstream`).
    pub relay: Arc<crate::relay::Relay>,
    /// `GET /api/metrics`'s counts in two tiers, each cached on
    /// its own: what has not finished, and the history. Per replica.
    #[cfg(feature = "postgres")]
    pub metrics_cache: Arc<dagron_core::gauges::GaugeCache>,
    /// The limits over this replica's sessions, as it last read them,
    /// for `GET /api/metrics`.
    #[cfg(feature = "postgres")]
    pub db_connections: Arc<crate::db::ConnectionGauge>,
}
