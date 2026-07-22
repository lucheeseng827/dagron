//! Audit trail for control-plane mutations, plus viewer read-only enforcement.
//!
//! **Enterprise-gated** (`--features enterprise`): per
//! `docs/COMMERCIALIZATION.md` §3, the audit log (compliance surface) and the
//! `viewer` RBAC role are paid-tier — OSS builds compile this module to a
//! passthrough middleware, no `audit_log` table, and no `/api/audit` route.
//! The source stays in the open tree, feature-off, like the other
//! `enterprise`-gated code paths (rerun params, enterprise migrations).
//!
//! Enterprise behavior: one middleware covers every route — any authenticated
//! non-GET request that succeeds (2xx) is recorded in `audit_log` with who /
//! what / when, so "who cancelled that run" has an answer without grepping
//! service logs. The same pass enforces the `viewer` group as read-only: a
//! viewer's mutating request is rejected with 403 before it reaches a handler.
//! Both are additive — users without the `viewer` group behave exactly as
//! before.

use axum::body::Body;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
#[cfg(feature = "enterprise")]
use axum::{
    extract::Query,
    http::{Method, StatusCode},
    response::IntoResponse,
    Json,
};
#[cfg(feature = "enterprise")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "enterprise")]
use uuid::Uuid;

use axum::extract::State;

#[cfg(feature = "enterprise")]
use crate::auth::AuthUser;
use crate::state::AppState;

/// Ensure the `audit_log` table exists (enterprise builds only — OSS builds
/// create no compliance table). dagron-api is its sole owner.
#[cfg(feature = "enterprise")]
pub async fn ensure_schema(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS audit_log (
            id          TEXT PRIMARY KEY NOT NULL,
            at          TEXT NOT NULL,
            user_email  TEXT NOT NULL,
            method      TEXT NOT NULL,
            path        TEXT NOT NULL,
            status      BIGINT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_audit_log_at ON audit_log(at DESC)")
        .execute(pool)
        .await?;
    Ok(())
}

/// Paths whose mutations are not audited (auth plumbing, not control actions).
#[cfg(feature = "enterprise")]
fn skip_audit(path: &str) -> bool {
    matches!(path, "/api/login" | "/api/logout")
}

/// Middleware: viewer read-only gate + success-audit for mutating requests.
/// OSS builds: a pure passthrough (no RBAC enforcement, nothing recorded).
///
/// Auth itself stays with each handler's `AuthUser` extractor — an
/// unauthenticated mutation still 401s there; this pass only *observes* the
/// claims when a valid session is present.
#[cfg(not(feature = "enterprise"))]
pub async fn audit_mutations(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let _ = state;
    next.run(req).await
}

/// Enterprise implementation — see the module docs.
#[cfg(feature = "enterprise")]
pub async fn audit_mutations(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let mutating = !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    let path = req.uri().path().to_string();
    let method = req.method().to_string();

    // Decode the session (if any) before the handler consumes the request.
    let claims = crate::auth::claims_from_request(req.headers(), &state.jwt_secret);

    if mutating && !skip_audit(&path) {
        if let Some(c) = &claims {
            // Viewer = read-only: block the mutation before it reaches a handler.
            if c.groups.iter().any(|g| g == "viewer") {
                return (
                    StatusCode::FORBIDDEN,
                    "viewer role is read-only".to_string(),
                )
                    .into_response();
            }
        }
    }

    let res = next.run(req).await;

    if mutating && !skip_audit(&path) && res.status().is_success() {
        if let Some(c) = claims {
            let id = Uuid::new_v4().to_string();
            let now = chrono::Utc::now().to_rfc3339();
            let status = res.status().as_u16() as i64;
            // Best-effort: an audit insert failure must never fail the mutation
            // it records (the mutation already committed).
            if let Err(e) = sqlx::query(
                "INSERT INTO audit_log (id, at, user_email, method, path, status)
                 VALUES ($1,$2,$3,$4,$5,$6)",
            )
            .bind(&id)
            .bind(&now)
            .bind(&c.email)
            .bind(&method)
            .bind(&path)
            .bind(status)
            .execute(&state.write_pool)
            .await
            {
                tracing::warn!(error = ?e, %path, "audit insert failed");
            }
        }
    }

    res
}

// ── Read endpoint (enterprise builds only) ───────────────────────────────────

#[cfg(feature = "enterprise")]
#[derive(Serialize, sqlx::FromRow)]
pub struct AuditEntry {
    pub id: String,
    pub at: String,
    pub user_email: String,
    pub method: String,
    pub path: String,
    pub status: i64,
}

#[cfg(feature = "enterprise")]
#[derive(Deserialize)]
pub struct ListParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// `GET /api/audit?limit=&offset=` — newest-first audit entries. Admin only.
#[cfg(feature = "enterprise")]
pub async fn list_audit(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<AuditEntry>>, (StatusCode, String)> {
    if !claims.groups.iter().any(|g| g == "admin") {
        return Err((StatusCode::FORBIDDEN, "admin group required".to_string()));
    }
    let limit = params.limit.unwrap_or(100).clamp(1, 500);
    let offset = params.offset.unwrap_or(0).max(0);
    let rows = sqlx::query_as::<_, AuditEntry>(
        "SELECT id, at, user_email, method, path, status
         FROM audit_log ORDER BY at DESC LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await
    .map_err(|e| {
        tracing::error!(error = ?e, "audit query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
    })?;
    Ok(Json(rows))
}
