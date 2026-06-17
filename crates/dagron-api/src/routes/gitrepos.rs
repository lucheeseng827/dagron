//! GitOps repository registry — the set of Git repos dagron tracks, surfaced on
//! the UI's GitOps page (connect / list / sync / disconnect).
//!
//! dagron-api owns the `git_repos` table (it ensures the schema itself, like the
//! users table) — the engine never reads it. **Scope note:** this is the registry
//! + UI state. Actual repo polling / reconcile of `.dagron/*.yaml` is a follow-up;
//! today `state` is set by connect (`OutOfSync`) and the Sync action (`Synced`).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

type ApiError = (StatusCode, String);

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct GitRepo {
    pub id: String,
    pub name: String,
    pub url: String,
    pub branch: String,
    pub rev: Option<String>,
    pub state: String,
    pub auto_sync: i64,
    pub workflow_count: i64,
    pub drift: i64,
    pub last_message: Option<String>,
    pub last_synced_at: Option<String>,
    pub created_at: String,
}

/// Ensure the `git_repos` table exists (dagron-api is its sole owner).
pub async fn ensure_schema(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS git_repos (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            branch TEXT NOT NULL DEFAULT 'main',
            rev TEXT,
            state TEXT NOT NULL DEFAULT 'OutOfSync',
            auto_sync BIGINT NOT NULL DEFAULT 0,
            workflow_count BIGINT NOT NULL DEFAULT 0,
            drift BIGINT NOT NULL DEFAULT 0,
            last_message TEXT,
            last_synced_at TEXT,
            created_at TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// `GET /api/git-repos` — all tracked repos, newest first.
pub async fn list_repos(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<GitRepo>>, ApiError> {
    let rows = sqlx::query_as::<_, GitRepo>("SELECT * FROM git_repos ORDER BY created_at DESC")
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?;
    Ok(Json(rows))
}

#[derive(Deserialize)]
pub struct ConnectBody {
    pub url: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub auto_sync: bool,
}

/// `POST /api/git-repos` — connect (register) a repository.
pub async fn connect_repo(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<ConnectBody>,
) -> Result<(StatusCode, Json<GitRepo>), ApiError> {
    let url = body.url.trim().to_string();
    if url.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "url is required".into()));
    }
    let name = repo_name(&url);
    let branch = body
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .unwrap_or("main")
        .to_string();
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let row = sqlx::query_as::<_, GitRepo>(
        "INSERT INTO git_repos (id, name, url, branch, state, auto_sync, created_at)
         VALUES ($1,$2,$3,$4,'OutOfSync',$5,$6)
         ON CONFLICT (url) DO NOTHING
         RETURNING *",
    )
    .bind(&id)
    .bind(&name)
    .bind(&url)
    .bind(&branch)
    .bind(if body.auto_sync { 1_i64 } else { 0 })
    .bind(&now)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal)?;

    match row {
        Some(r) => Ok((StatusCode::CREATED, Json(r))),
        None => Err((StatusCode::CONFLICT, "repository already connected".into())),
    }
}

/// `POST /api/git-repos/:id/sync` — mark the repo synced now.
///
/// Scope: state transition + timestamp (drift cleared). Real `git fetch` +
/// reconcile of `.dagron/*.yaml` is a follow-up; this is the UI action's contract.
pub async fn sync_repo(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GitRepo>, ApiError> {
    let now = chrono::Utc::now().to_rfc3339();
    let row = sqlx::query_as::<_, GitRepo>(
        "UPDATE git_repos SET state='Synced', drift=0, last_synced_at=$1 WHERE id=$2 RETURNING *",
    )
    .bind(&now)
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal)?;
    row.map(Json).ok_or((StatusCode::NOT_FOUND, "repository not found".into()))
}

/// `DELETE /api/git-repos/:id` — disconnect (stop tracking).
pub async fn delete_repo(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let res = sqlx::query("DELETE FROM git_repos WHERE id=$1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, "repository not found".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Best-effort "owner/repo" from a Git URL (https or scp-style); falls back to the
/// trimmed input.
fn repo_name(url: &str) -> String {
    let s = url.trim_end_matches('/').trim_end_matches(".git");
    let s = s.split("://").last().unwrap_or(s); // drop scheme
    let s = s.splitn(2, '@').last().unwrap_or(s); // drop user@
    // Keep the last two path segments (owner/repo).
    let parts: Vec<&str> = s.split(['/', ':']).filter(|p| !p.is_empty()).collect();
    let n = parts.len();
    if n >= 2 {
        format!("{}/{}", parts[n - 2], parts[n - 1])
    } else if n == 1 {
        parts[0].to_string()
    } else {
        url.to_string()
    }
}

fn internal(e: sqlx::Error) -> ApiError {
    tracing::error!(error = ?e, "git_repos query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".into())
}
