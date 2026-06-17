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
mod expand;
mod routes;
mod state;
mod stream;

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
    // Tunable, SaaS-ready logging (RUST_LOG / LOG_LEVEL / LOG_FORMAT / …); see
    // the shared `dagron_logging` crate for the full env knob list.
    dagron_logging::init("api");

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
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .context("connecting to Postgres")?;

    // Broadcast channel for live task events; the listener that feeds it is added in 01-04.
    let (tx, _rx) = broadcast::channel::<TaskEvent>(1024);

    let state = AppState {
        read_pool: pool.clone(),
        write_pool: pool.clone(),
        tx: tx.clone(),
        jwt_secret,
        cookie_secure,
    };

    // dagron-api owns the users table; ensure it exists before serving login.
    routes::login::ensure_schema(&pool)
        .await
        .context("ensuring users schema")?;
    // dagron-api also owns the GitOps repo registry.
    routes::gitrepos::ensure_schema(&pool)
        .await
        .context("ensuring git_repos schema")?;
    // Additive `description` column on the engine-owned `workflows` table (the UI
    // owns this field). Idempotent + tolerant of the table not existing yet (the
    // engine creates it on first migrate); mirrors migrations_pg/010.
    sqlx::query("ALTER TABLE IF EXISTS workflows ADD COLUMN IF NOT EXISTS description TEXT")
        .execute(&pool)
        .await
        .context("ensuring workflows.description column")?;
    // Seed a first admin from env (idempotent) so the first login needs no manual
    // DB step. No-op when DAGRON_ADMIN_EMAIL / DAGRON_ADMIN_PASSWORD are unset.
    if let Err(e) = routes::login::bootstrap_admin(&pool).await {
        tracing::warn!(error = ?e, "admin bootstrap failed (continuing)");
    }

    // One shared listener fans task_events NOTIFY out to all SSE clients.
    stream::spawn_listener(pool, tx);

    let app = Router::new()
        .route("/healthz", get(healthz))
        // Self-contained auth: login + logout (public) + user management (admin-only).
        .route("/api/login", post(routes::login::login))
        .route("/api/logout", post(routes::login::logout))
        .route("/api/users", post(routes::login::create_user))
        .route("/api/me", get(me))
        .route("/api/runs", get(routes::runs::list_runs))
        .route("/api/runs/{id}", get(routes::runs::get_run))
        .route("/api/runs/{id}/graph", get(routes::graph::get_graph))
        .route("/api/runs/{id}/tasks/{tid}/logs", get(routes::graph::get_task_logs))
        .route("/api/runs/{id}/stream", get(routes::stream::stream_run))
        .route("/api/runs", post(routes::control::submit_run))
        .route("/api/runs/{id}/cancel", post(routes::control::cancel_run))
        .route("/api/runs/{id}/rerun", post(routes::control::rerun_run))
        .route("/api/runs/{id}/resubmit", post(routes::control::resubmit_run))
        .route("/api/runs/{id}/tasks/{tid}/retry", post(routes::control::retry_task))
        // Observability + dead-letter queue (authed UI edge over engine ops surface).
        .route("/api/metrics", get(routes::ops::metrics))
        .route("/api/dead-letters", get(routes::ops::list_dead_letters))
        .route("/api/dead-letters/{id}/redrive", post(routes::ops::redrive_dead_letter))
        .route("/api/dead-letters/{id}", axum::routing::delete(routes::ops::delete_dead_letter))
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
        .route("/api/workflows/{id}/sync-to-git", post(routes::gitsync::sync_to_git))
        // GitOps repository registry (connect / list / sync / disconnect).
        .route(
            "/api/git-repos",
            get(routes::gitrepos::list_repos).post(routes::gitrepos::connect_repo),
        )
        .route("/api/git-repos/{id}", axum::routing::delete(routes::gitrepos::delete_repo))
        .route("/api/git-repos/{id}/sync", post(routes::gitrepos::sync_repo))
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
        // Cap request bodies (submit YAML) to resist abuse.
        .layer(tower_http::limit::RequestBodyLimitLayer::new(1024 * 1024))
        .layer(TraceLayer::new_for_http())
        // Dev CORS: permissive. Tighten to the frontend origin in production.
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.context("binding listener")?;
    info!(%addr, "dagron-api listening");
    axum::serve(listener, app).await.context("serving")?;
    Ok(())
}

/// Liveness probe — no auth, no DB.
async fn healthz() -> &'static str {
    "ok"
}

/// Auth probe — returns the validated session claims, proving the shared-token
/// contract works end-to-end. Gated by the `AuthUser` extractor.
async fn me(AuthUser(claims): AuthUser) -> Json<auth::SessionClaims> {
    Json(claims)
}
