//! dagron management API — read-mostly axum service over the dagron Postgres.
//!
//! Stateless and horizontally scalable: it reads the same database the
//! schedulers write, plus issues control mutations (cancel/retry/submit) and
//! bridges Postgres `LISTEN/NOTIFY` to browser SSE for live updates.
//!
//! Auth is self-contained: dagron-api owns login (`POST /api/login`) and both
//! signs and validates its own HS256 session JWT (`DAGRON_JWT_SECRET`) — no
//! external IdP.
//! The token is delivered to browsers as an HttpOnly `dagron_session` cookie;
//! non-browser clients may instead send it as `Authorization: Bearer <jwt>`.

mod auth;
// Settings governance (LOW_LATENCY §5): the knob registry, effective-config
// startup log, and the fleet-drift fingerprint (also surfaced by /api/health).
mod config;
mod expand;
mod identity;
// `{{ }}` templating mirror for the submit path (core semantics, no core dep).
mod tmpl;
// Cloud archive URL → object_store dispatch (s3/gs/az) for /api/archive fetches.
#[cfg(feature = "archive-cloud")]
mod objstore;
mod pwhash;
mod ratelimit;
mod routes;
mod state;
mod stream;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    routing::{get, post},
    Json, Router,
};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use auth::AuthUser;
use state::{AppState, TaskEvent};

#[tokio::main]
async fn main() -> Result<()> {
    // Tunable, structured logging (RUST_LOG / LOG_LEVEL / LOG_FORMAT / …); see
    // the shared `dagron_logging` crate for the full env knob list.
    dagron_logging::init("api");
    // One effective-config record per boot: every explicitly-set knob
    // (secrets redacted) plus the fleet-drift fingerprint (LOW_LATENCY §5).
    config::log_effective();

    let database_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL must be set (postgres connection string)")?;
    // dagron-api signs and validates its own session JWT — no external IdP.
    // Require a present, non-empty secret of at least 32 bytes (256-bit) so HS256
    // session tokens can't be signed with a trivially brute-forceable key.
    let jwt_secret = std::env::var("DAGRON_JWT_SECRET")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| s.len() >= 32)
        .context("DAGRON_JWT_SECRET must be set and at least 32 characters")?;
    // Session cookie is marked `Secure` (HTTPS-only) by default; set
    // DAGRON_COOKIE_SECURE=false for plain-HTTP local dev (e.g. podman compose).
    let cookie_secure = std::env::var("DAGRON_COOKIE_SECURE")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);

    // One pool serves both read and write roles for now; a read replica can be
    // wired into read_pool later without changing handlers.
    //
    // Pool discipline (LOW_LATENCY R-2). Every knob defaults to the previous
    // hard-coded behaviour; the low-latency profile is what changes them:
    //  - DAGRON_DB_MAX_CONNECTIONS (default 8)
    //  - DAGRON_DB_MIN_CONNECTIONS (default 0) — a floor of warm connections;
    //    also acquired once at startup below, so a replica is never marked
    //    Ready with a cold pool (market open must not pay TLS + auth
    //    handshakes on the first N requests).
    //  - DAGRON_DB_ACQUIRE_TIMEOUT_MS (default 10000) — the profile sets
    //    ~250 so a saturated pool fails fast (503 + Retry-After via /readyz
    //    flipping) instead of queueing requests for ten seconds.
    //  - DAGRON_DB_TEST_BEFORE_ACQUIRE (default true) — sqlx's per-checkout
    //    liveness round trip; the profile turns it off and lets the query
    //    itself surface a dead connection.
    let db_max: u32 = env_parse("DAGRON_DB_MAX_CONNECTIONS", 8).max(1);
    let db_min: u32 = env_parse("DAGRON_DB_MIN_CONNECTIONS", 0).min(db_max);
    let db_acquire_ms: u64 = env_parse("DAGRON_DB_ACQUIRE_TIMEOUT_MS", 10_000).max(50);
    let db_test_before_acquire = std::env::var("DAGRON_DB_TEST_BEFORE_ACQUIRE")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let pool = PgPoolOptions::new()
        .max_connections(db_max)
        .min_connections(db_min)
        .acquire_timeout(Duration::from_millis(db_acquire_ms))
        .test_before_acquire(db_test_before_acquire)
        .connect(&database_url)
        .await
        .context("connecting to Postgres")?;
    info!(
        max = db_max,
        min = db_min,
        acquire_timeout_ms = db_acquire_ms,
        test_before_acquire = db_test_before_acquire,
        "db pool configured"
    );
    // Warm the floor eagerly: hold min_connections checkouts at once so the
    // pool actually opens them, then release. Best-effort — a partially warmed
    // pool is not a startup failure, and /readyz still gates on a live acquire.
    if db_min > 0 {
        let held: Vec<_> = futures::future::join_all((0..db_min).map(|_| pool.acquire())).await;
        let warmed = held.iter().filter(|c| c.is_ok()).count();
        info!(warmed, target = db_min, "db pool warmed");
    }

    // Broadcast channel for live task events; the listener that feeds it is added in 01-04.
    let (tx, _rx) = broadcast::channel::<TaskEvent>(1024);

    // Local identity: verify credentials against the local users table. An
    // alternate provider can swap an SSO backend in behind the same seam.
    let identity: Arc<dyn dagron_identity::IdentityProvider> =
        Arc::new(identity::LocalIdentityProvider::new(pool.clone()));

    // Programmatic artifact store (put/get by key), transparently envelope-
    // encrypted when a KEK provider is configured. A *misconfigured* provider
    // fails startup here rather than silently storing plaintext.
    let artifact_store = dagron_artifact::store_from_env()
        .context("configuring the artifact store (DAGRON_ARTIFACT_DIR / KEK provider)")?;
    if artifact_store.is_some() {
        let encrypted = matches!(dagron_crypto::provider_from_env(), Ok(Some(_)));
        info!(encrypted, "artifact API enabled (PUT/GET /api/runs/*/artifacts/*)");
    }

    let listener_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let state = AppState {
        read_pool: pool.clone(),
        write_pool: pool.clone(),
        tx: tx.clone(),
        jwt_secret,
        cookie_secure,
        identity,
        artifact_store,
        rotation_lock: Arc::new(tokio::sync::Mutex::new(())),
        login_limiter: Arc::new(ratelimit::RateLimiter::from_env()),
        listener_ready: Arc::clone(&listener_ready),
    };

    // Bring the tables dagron-api owns into existence, tolerating a concurrent
    // migrator creating the same ones (see `ensure_schemas`).
    ensure_schemas(&pool).await?;
    // Seed a first admin from env (idempotent) so the first login needs no manual
    // DB step. No-op when DAGRON_ADMIN_EMAIL / DAGRON_ADMIN_PASSWORD are unset.
    if let Err(e) = routes::login::bootstrap_admin(&pool).await {
        tracing::warn!(error = ?e, "admin bootstrap failed (continuing)");
    }

    // One shared listener fans task_events NOTIFY out to all SSE clients.
    stream::spawn_listener(pool, tx, listener_ready);

    let app = Router::new()
        .route("/healthz", get(healthz))
        // Readiness: unlike /healthz (static liveness), 200 here means the
        // pool answers within a bounded budget AND the SSE listener is
        // subscribed. Point the orchestrator's readinessProbe at this
        // (LOW_LATENCY R-1); keep /healthz for liveness.
        .route("/readyz", get(readyz))
        // Self-contained auth: login + logout (public) + user management (admin-only).
        .route("/api/logout", post(routes::login::logout))
        .route(
            "/api/users",
            get(routes::login::list_users).post(routes::login::create_user),
        )
        .route("/api/me", get(me))
        // Rich health (DB + scheduler leadership + attention counters) for the
        // sidebar widget; /healthz below stays the bare liveness probe.
        .route("/api/health", get(routes::health::health))
        // Global search behind the ⌘K palette (capped, parameterized).
        .route("/api/search", get(routes::search::search))
        // Instance-wide notification defaults (engine merges them per run).
        .route(
            "/api/settings/notifications",
            get(routes::settings::get_notifications).put(routes::settings::put_notifications),
        )
        .route(
            "/api/settings/notifications/test",
            post(routes::settings::test_notifications),
        )
        // Environments: named variable sets + write-only encrypted secrets.
        .route(
            "/api/environments",
            get(routes::environments::list).post(routes::environments::create),
        )
        .route(
            "/api/environments/{id}",
            axum::routing::put(routes::environments::update)
                .delete(routes::environments::delete),
        )
        .route(
            "/api/environments/{id}/secrets/{name}",
            axum::routing::put(routes::environments::put_secret)
                .delete(routes::environments::delete_secret),
        )
        .route("/api/runs", get(routes::runs::list_runs))
        .route("/api/runs/{id}", get(routes::runs::get_run))
        .route("/api/runs/{id}/spec", get(routes::runs::get_run_spec))
        .route("/api/runs/{id}/wait", get(routes::runs::wait_run))
        .route("/api/runs/{id}/graph", get(routes::graph::get_graph))
        // Log views: the whole run's output as one filtered stream, and one
        // task's output (live-tailable) under the same filter grammar.
        .route("/api/runs/{id}/logs", get(routes::logs::get_run_logs))
        .route("/api/runs/{id}/tasks/{tid}/logs", get(routes::logs::get_task_logs))
        .route("/api/runs/{id}/stream", get(routes::stream::stream_run))
        // Account-wide activity stream: feeds the list pages' live-updates mode.
        .route("/api/events/stream", get(routes::stream::stream_events))
        .route("/api/runs", post(routes::control::submit_run))
        .route("/api/runs/{id}/cancel", post(routes::control::cancel_run))
        .route("/api/runs/{id}/rerun", post(routes::control::rerun_run))
        // Failure triage: what a *human* decided about a failed run, which
        // `status` (what the engine did) cannot express.
        .route(
            "/api/runs/{id}/triage",
            post(routes::triage::set_triage).delete(routes::triage::clear_triage),
        )
        .route("/api/runs/{id}/resubmit", post(routes::control::resubmit_run))
        .route("/api/runs/{id}/tasks/{tid}/retry", post(routes::control::retry_task))
        .route("/api/runs/{id}/tasks/{tid}/clear", post(routes::control::clear_task))
        .route("/api/runs/{id}/tasks/{tid}/approve", post(routes::control::approve_task))
        .route("/api/runs/{id}/tasks/{tid}/reject", post(routes::control::reject_task))
        // Observability + dead-letter queue (authed UI edge over engine ops surface).
        .route("/api/archive/runs", get(routes::archive::list_archived_runs))
        .route("/api/archive/runs/{id}", get(routes::archive::get_archived_run))
        .route("/api/metrics", get(routes::ops::metrics))
        .route("/api/metrics/timeseries", get(routes::ops::metrics_timeseries))
        // Human-in-the-loop worklist: every gate parked in awaiting_approval.
        .route("/api/approvals", get(routes::ops::list_approvals))
        // Data-aware scheduling: the dataset registry + its lineage ledger. Read
        // -only here — updates come from `produces:` tasks (or the Enterprise
        // external-events route on the engine).
        .route("/api/datasets", get(routes::datasets::list_datasets))
        .route("/api/datasets/events", get(routes::datasets::list_dataset_events))
        .route("/api/dead-letters", get(routes::ops::list_dead_letters))
        .route("/api/dead-letters/{id}/redrive", post(routes::ops::redrive_dead_letter))
        .route("/api/dead-letters/{id}", axum::routing::delete(routes::ops::delete_dead_letter))
        // Dead-letter policy. Only the retry count: STREAM_DLQ_PATH is a path
        // on the engine's host and belongs in deployment config, not a form.
        .route(
            "/api/settings/dead-letters",
            get(routes::settings::get_dead_letters).put(routes::settings::put_dead_letters),
        )
        // Personal access tokens: long-lived credentials for CI and the SDKs,
        // so automation never has to store a password. Managing them requires a
        // password session (see auth::SessionAuth).
        .route(
            "/api/tokens",
            get(routes::tokens::list_tokens).post(routes::tokens::create_token),
        )
        .route("/api/tokens/{id}", axum::routing::delete(routes::tokens::revoke_token))
        // First-class workflows (named, reusable DAG definitions).
        .route(
            "/api/workflows",
            get(routes::workflows::list_workflows).post(routes::workflows::create_workflow),
        )
        .route(
            "/api/workflows/{id}",
            get(routes::workflows::get_workflow)
                .put(routes::workflows::update_workflow)
                .delete(routes::workflows::delete_workflow),
        )
        .route("/api/workflows/{id}/run", post(routes::workflows::run_workflow))
        // Lifecycle: version history, and a state that is not deletion. Pausing
        // leaves the workflow's schedules intact — that is the whole difference
        // from DELETE, which cascades them away.
        .route("/api/workflows/{id}/versions", get(routes::lifecycle::list_versions))
        .route("/api/workflows/{id}/state", post(routes::lifecycle::set_state))
        .route("/api/workflows/{id}/runs", get(routes::workflows::workflow_runs))
        .route("/api/workflows/{id}/sync-to-git", post(routes::gitsync::sync_to_git))
        // Public run-status badge (embeds in READMEs; status label only, no auth).
        .route("/api/badges/{name}", get(routes::badge::workflow_badge))
        // GitOps repository registry (connect / list / sync / disconnect).
        .route(
            "/api/git-repos",
            get(routes::gitrepos::list_repos).post(routes::gitrepos::connect_repo),
        )
        .route("/api/git-repos/{id}", axum::routing::delete(routes::gitrepos::delete_repo))
        .route("/api/git-repos/{id}/sync", post(routes::gitrepos::sync_repo))
        // Per-repository credential (HTTPS token or SSH key), write-only: set or
        // rotate it here, clear it with DELETE. It is never readable back.
        .route(
            "/api/git-repos/{id}/auth",
            axum::routing::put(routes::gitrepos::put_auth)
                .delete(routes::gitrepos::delete_auth),
        )
        // Workflow schedules (UI schedule drawer; engine fires them).
        .route(
            "/api/schedules",
            get(routes::schedules::list_schedules).post(routes::schedules::create_schedule),
        )
        .route(
            "/api/schedules/{id}",
            axum::routing::put(routes::schedules::update_schedule)
                .delete(routes::schedules::delete_schedule),
        )
        .route("/api/schedules/{id}/backfill", post(routes::schedules::backfill))
        // First-class paced backfill jobs (#18): create/list/monitor/cancel.
        .route(
            "/api/backfills",
            post(routes::backfills::create).get(routes::backfills::list),
        )
        .route("/api/backfills/{id}", get(routes::backfills::get))
        .route("/api/backfills/{id}/cancel", post(routes::backfills::cancel));

    // Admin: store-wide artifact key rotation (KEK_OLD → current KEK). Rotation
    // only means anything where a KEK exists, and KEKs are Enterprise
    // (`dagron_crypto::build_provider`) — so the route is absent from an open
    // build rather than present and guaranteed to fail.
    #[cfg(feature = "enterprise")]
    let app = app.route("/api/artifacts/rotate", post(routes::artifacts::rotate_artifacts));

    // Enterprise routes (the open-core split): the audit trail read.
    // OSS builds answer 404 here, matching the feature being absent.
    #[cfg(feature = "enterprise")]
    let app = app.route("/api/audit", get(routes::audit::list_audit));

    // Core routes keep the tight 1 MiB body cap (submit YAML etc.) to resist abuse.
    let app = app.layer(tower_http::limit::RequestBodyLimitLayer::new(1024 * 1024));

    // Artifact PUT carries data blobs (checkpoints/outputs) that legitimately
    // exceed the core cap, so its routes get a larger, separately-configured limit
    // (`DAGRON_ARTIFACT_MAX_BYTES`, default 128 MiB). NOTE: bodies are buffered in
    // memory — streaming very large artifacts is a follow-up.
    let artifact_max = std::env::var("DAGRON_ARTIFACT_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024);
    let artifacts = Router::new()
        .route(
            "/api/runs/{run_id}/artifacts/{task}/{name}",
            axum::routing::put(routes::artifacts::put_artifact)
                .get(routes::artifacts::get_artifact),
        )
        .route(
            "/api/runs/{run_id}/artifacts/{task}/{name}/exists",
            get(routes::artifacts::artifact_exists),
        )
        // Disable the extractor's default 2 MiB cap so the tower limit below is the
        // single, explicit body ceiling for artifacts.
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(tower_http::limit::RequestBodyLimitLayer::new(artifact_max));

    // Login is the one unauthenticated route that costs an Argon2 verify per
    // call, so it carries a per-client budget the other routes don't need. Its
    // own Router keeps the middleware off everything else.
    let login = Router::new()
        .route("/api/login", post(routes::login::login))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            ratelimit::login_rate_limit,
        ));

    let app = app
        .merge(login)
        .merge(artifacts)
        // Viewer read-only enforcement + audit trail for successful mutations
        // (a passthrough on OSS builds — see routes/audit.rs). Applies to both.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            routes::audit::audit_mutations,
        ))
        .layer(TraceLayer::new_for_http())
        // Dev CORS: permissive. Tighten to the frontend origin in production.
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.context("binding listener")?;
    info!(%addr, "dagron-api listening");
    // Rolling restarts drain: on SIGTERM/ctrl-c axum stops accepting and lets
    // in-flight requests finish instead of severing them mid-response
    // (LOW_LATENCY R-1). The drain is BOUNDED (DAGRON_SHUTDOWN_DRAIN_MS,
    // default 15 s): open SSE streams and long-polls never finish on their
    // own, so an unbounded graceful shutdown would hang every rollout until
    // the kubelet's SIGKILL — better to close the stragglers ourselves,
    // on our schedule, after the real requests have drained. Keep the knob
    // under the pod's terminationGracePeriodSeconds (default 30 s) so the
    // orderly path always wins over the SIGKILL.
    let drain_ms: u64 = env_parse("DAGRON_SHUTDOWN_DRAIN_MS", 15_000);
    let (drained_tx, drained_rx) = tokio::sync::oneshot::channel::<()>();
    // with_connect_info so the login limiter can key on the socket peer — the
    // one client identifier a caller cannot forge (see `ratelimit::client_key`).
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = drained_tx.send(());
    });
    tokio::select! {
        res = server => res.context("serving")?,
        _ = async {
            // Arms only once the signal has fired; dropping the server future
            // closes the connections the drain window could not flush.
            let _ = drained_rx.await;
            tokio::time::sleep(Duration::from_millis(drain_ms)).await;
        } => {
            tracing::warn!(
                drain_ms,
                "shutdown drain deadline reached — closing remaining connections \
                 (long-lived SSE/long-poll clients reconnect to the next replica)"
            );
        }
    }
    Ok(())
}

/// Create the tables dagron-api owns, retrying past a concurrent migrator.
///
/// `CREATE TABLE IF NOT EXISTS` is not atomic against another session running
/// the *same* statement: both check the catalog, both find nothing, and both
/// create. The loser does not get the no-op the `IF NOT EXISTS` implies — it
/// fails on a catalog constraint, because creating a table also inserts its row
/// type into `pg_type`:
///
/// ```text
/// Error: ensuring users schema
/// Caused by: duplicate key value violates unique constraint "pg_type_typname_nsp_index"
/// ```
///
/// That is reachable in the shipped quickstart: `users` is created both here and
/// by the engine's sqlx migrator (dagron-core/migrations_pg/008_users.sql), and
/// compose starts both processes the moment Postgres reports healthy. The
/// migrator holds sqlx's own advisory lock, which serializes migrator against
/// migrator but knows nothing about these statements — so on a clean volume the
/// two collide and dagron-api exits before it ever listens.
///
/// Every step below is idempotent, so the whole sequence is simply replayed: by
/// the retry the other side has committed, `IF NOT EXISTS` sees the object and
/// the no-op path is taken. Retries are bounded — a race resolves on the first
/// one, and anything still failing after that is a real error worth dying on.
async fn ensure_schemas(pool: &sqlx::postgres::PgPool) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 4;
    let mut attempt = 1;
    loop {
        match ensure_schemas_once(pool).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_ATTEMPTS && is_concurrent_ddl_race(&e) => {
                tracing::warn!(
                    attempt,
                    error = format!("{e:#}"),
                    "schema bootstrap raced a concurrent migrator; retrying"
                );
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One pass of the schema bootstrap. Every statement is idempotent so the caller
/// can replay it wholesale.
async fn ensure_schemas_once(pool: &sqlx::postgres::PgPool) -> Result<()> {
    // dagron-api owns the users table; ensure it exists before serving login.
    routes::login::ensure_schema(pool)
        .await
        .context("ensuring users schema")?;
    // dagron-api also owns the GitOps repo registry.
    routes::gitrepos::ensure_schema(pool)
        .await
        .context("ensuring git_repos schema")?;
    // … and, on enterprise builds, the audit trail for control-plane mutations.
    #[cfg(feature = "enterprise")]
    routes::audit::ensure_schema(pool)
        .await
        .context("ensuring audit_log schema")?;
    // … and the UI-configurable instance settings (notification defaults).
    routes::settings::ensure_schema(pool)
        .await
        .context("ensuring ui_settings schema")?;
    // … and environments (variable sets + encrypted secrets).
    routes::environments::ensure_schema(pool)
        .await
        .context("ensuring environments schema")?;
    // Additive `description` column on the engine-owned `workflows` table (the UI
    // owns this field). Idempotent + tolerant of the table not existing yet (the
    // engine creates it on first migrate); mirrors migrations_pg/010.
    sqlx::query("ALTER TABLE IF EXISTS workflows ADD COLUMN IF NOT EXISTS description TEXT")
        .execute(pool)
        .await
        .context("ensuring workflows.description column")?;
    Ok(())
}

/// Does this error mean "another session created the object first"?
///
/// Scoped deliberately to the schema bootstrap. `23505` is an ordinary
/// application error elsewhere (a duplicate email on signup); what makes it a
/// race *here* is that the only inserts in that path are Postgres' own catalog
/// writes. The rest are the DDL-specific codes Postgres raises when an object
/// appears between the existence check and the create.
fn is_concurrent_ddl_race(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .filter_map(|e| e.as_database_error())
        .any(|db| {
            matches!(
                db.code().as_deref(),
                // unique_violation on a pg_catalog index (e.g. pg_type_typname_nsp_index)
                Some("23505")
                // duplicate_table / duplicate_object / duplicate_column
                    | Some("42P07")
                    | Some("42710")
                    | Some("42701")
            )
        })
}

/// Resolves on SIGTERM (Kubernetes stop) or ctrl-c (local dev).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!("shutdown signal received — draining");
}

/// Parse a numeric env var with a default (invalid values fall back).
fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
}

/// Liveness probe — no auth, no DB.
async fn healthz() -> &'static str {
    "ok"
}

/// Readiness probe — no auth, one bounded DB round trip. 503 with a reason
/// when the database is unreachable or the pool is exhausted, so an
/// orchestrator stops routing to that replica (LOW_LATENCY R-1). The probe
/// budget is its own knob (memoized: the environment is boot-immutable) so a
/// long DAGRON_DB_ACQUIRE_TIMEOUT_MS never makes the probe itself hang past
/// the kubelet's patience.
///
/// The SSE listener flag is ADVISORY by default — reported in the body and in
/// `/api/health`, but not a 503. The listener watches one shared direct
/// Postgres endpoint (`DATABASE_LISTEN_URL`), so its failure is correlated
/// across every replica at once: gating readiness on it cannot route around
/// the problem (no replica has a listener either), it can only empty the
/// Service and take down every endpoint that still worked — login, listings,
/// submits. Live events are a degradable feature (`/wait` falls back to its
/// 5 s recheck; SSE clients reconnect), so degraded beats down.
/// `DAGRON_READY_REQUIRE_LISTENER=1` opts into strict gating for deployments
/// with per-replica listen endpoints, where eviction genuinely reroutes.
async fn readyz(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    static BUDGET: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    static REQUIRE_LISTENER: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let budget = *BUDGET
        .get_or_init(|| Duration::from_millis(env_parse("DAGRON_READY_TIMEOUT_MS", 500).max(50)));
    let probe = sqlx::query("SELECT 1").execute(&state.read_pool);
    match tokio::time::timeout(budget, probe).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = ?e, "readiness probe: db query failed");
            return (StatusCode::SERVICE_UNAVAILABLE, "database unreachable").into_response();
        }
        Err(_) => {
            tracing::warn!(budget_ms = budget.as_millis() as u64, "readiness probe: acquire timed out");
            return (StatusCode::SERVICE_UNAVAILABLE, "pool acquire timed out").into_response();
        }
    }
    if !state.listener_ready.load(std::sync::atomic::Ordering::Acquire) {
        let strict = *REQUIRE_LISTENER.get_or_init(|| {
            std::env::var("DAGRON_READY_REQUIRE_LISTENER")
                .map(|v| { let v = v.trim(); v == "1" || v.eq_ignore_ascii_case("true") })
                .unwrap_or(false)
        });
        if strict {
            return (StatusCode::SERVICE_UNAVAILABLE, "event listener not subscribed")
                .into_response();
        }
        return (StatusCode::OK, "ready (event listener degraded)").into_response();
    }
    (StatusCode::OK, "ready").into_response()
}

/// Auth probe — returns the validated session claims, proving the shared-token
/// contract works end-to-end. Gated by the `AuthUser` extractor.
async fn me(AuthUser(claims): AuthUser) -> Json<auth::SessionClaims> {
    Json(claims)
}
