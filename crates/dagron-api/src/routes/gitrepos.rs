//! GitOps repository registry — the set of Git repos dagron tracks, surfaced on
//! the UI's GitOps page (connect / list / request sync / disconnect).
//!
//! dagron-api owns the `git_repos` and `gitops_workers` tables (it ensures the
//! schema itself, like the users table) — the engine never reads them.
//!
//! **The clone/reconcile half lives in `dagron-gitops`, not here.** This gateway
//! ships on distroless (no shell, no git binary), so running `git clone` in
//! process failed every time with `running git: No such file or directory` — the
//! feature was unreachable on the image people deploy. Putting git into the
//! internet-facing container to fix that would tax every deployment for a feature
//! most never enable, so syncing moved to its own optional image. What stays here
//! is registry CRUD, URL/scheme hardening at connect time, and the *request* the
//! worker acts on.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

type ApiError = (StatusCode, String);

/// Default in-repo directory scanned for workflow YAML when none is given.
const DEFAULT_PATH: &str = "dagron";

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct GitRepo {
    pub id: String,
    pub name: String,
    pub url: String,
    pub branch: String,
    /// In-repo directory scanned for `*.yaml` / `*.yml` workflow specs.
    pub path: String,
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
            path TEXT NOT NULL DEFAULT 'dagron',
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
    // Upgrade path: add `path` to a git_repos table created before pull sync.
    sqlx::query(
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS path TEXT NOT NULL DEFAULT 'dagron'",
    )
    .execute(pool)
    .await?;
    // Sync is executed by the `dagron-gitops` worker, not here: this gateway runs
    // on distroless (no shell, no git binary), which is why every in-process sync
    // failed with "running git: No such file or directory". The Sync button now
    // stamps this column and the worker claims it.
    sqlx::query("ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS sync_requested_at TEXT")
        .execute(pool)
        .await?;
    // Worker liveness. Without it the console would show "Auto-sync ON" whenever
    // no worker is deployed — the same silent failure this split exists to fix,
    // just relocated.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS gitops_workers (
            id           TEXT PRIMARY KEY NOT NULL,
            last_seen_at TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Seconds a worker's heartbeat may age before the console calls it gone. Two
/// missed 5s ticks plus slack — long enough to ride out a datastore blip.
const WORKER_STALE_SECS: i64 = 30;

/// Whether any GitOps worker has heartbeated recently.
pub(crate) async fn worker_online(pool: &sqlx::postgres::PgPool) -> bool {
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(WORKER_STALE_SECS)).to_rfc3339();
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gitops_workers WHERE last_seen_at >= $1")
        .bind(&cutoff)
        .fetch_one(pool)
        .await
        .map(|n| n > 0)
        .unwrap_or(false)
}

/// `GET /api/git-repos` — tracked repos (newest first) plus whether a GitOps
/// worker is alive to act on them.
///
/// `worker_online` rides along on purpose: syncing happens in the separate
/// `dagron-gitops` process, so without it the console would keep promising
/// "Auto-sync ON" in a deployment where nothing polls.
#[derive(Serialize)]
pub struct RepoList {
    pub repos: Vec<GitRepo>,
    pub worker_online: bool,
}

pub async fn list_repos(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<RepoList>, ApiError> {
    let repos = sqlx::query_as::<_, GitRepo>("SELECT * FROM git_repos ORDER BY created_at DESC")
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?;
    let worker_online = worker_online(&state.read_pool).await;
    Ok(Json(RepoList { repos, worker_online }))
}

#[derive(Deserialize)]
pub struct ConnectBody {
    pub url: String,
    #[serde(default)]
    pub branch: Option<String>,
    /// In-repo directory to scan for workflow YAML (default `dagron`).
    #[serde(default)]
    pub path: Option<String>,
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
    // Reject a URL scheme we won't clone, before it ever reaches `git`.
    validate_git_url(&url)?;
    let name = repo_name(&url);
    let branch = body
        .branch
        .as_deref()
        .map(str::trim)
        .filter(|b| !b.is_empty())
        .unwrap_or("main")
        .to_string();
    let path = body
        .path
        .as_deref()
        .map(str::trim)
        .map(|p| p.trim_matches('/'))
        .filter(|p| !p.is_empty())
        .unwrap_or(DEFAULT_PATH)
        .to_string();
    // A leading '-' would be read by `git` as a flag; a '..' escapes the clone.
    if branch.starts_with('-') {
        return Err((
            StatusCode::BAD_REQUEST,
            "branch must not start with '-'".into(),
        ));
    }
    if path.starts_with('-') || path.split('/').any(|seg| seg == "..") {
        return Err((
            StatusCode::BAD_REQUEST,
            "path must be a relative in-repo directory".into(),
        ));
    }
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let row = sqlx::query_as::<_, GitRepo>(
        "INSERT INTO git_repos (id, name, url, branch, path, state, auto_sync, created_at)
         VALUES ($1,$2,$3,$4,$5,'OutOfSync',$6,$7)
         ON CONFLICT (url) DO NOTHING
         RETURNING *",
    )
    .bind(&id)
    .bind(&name)
    .bind(&url)
    .bind(&branch)
    .bind(&path)
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

/// `POST /api/git-repos/:id/sync` — fetch the repo and reconcile its workflows.
///
/// Shallow-clones the registered branch, validates every `*.yaml`/`*.yml` under
/// the repo's `path`, and upserts each valid workflow into the `workflows` table
/// keyed by name. The row is updated with the fetched `rev`, the synced count,
/// and a `state`/`last_message` reflecting success or the per-file errors. A
/// clone/parse failure is reported on the row (state `Error`) rather than as an
/// HTTP error, so the UI always gets the repo's current state; only a datastore
/// `POST /api/git-repos/:id/sync` — **request** a sync.
///
/// This gateway no longer runs `git`. It ships on distroless (no shell, no git
/// binary), so doing the clone here failed on every attempt with `running git:
/// No such file or directory` — the feature was unreachable on the image people
/// actually deploy. Sync now belongs to the `dagron-gitops` worker; this stamps
/// the request and NOTIFYs so a listening worker picks it up immediately rather
/// than on its next poll.
///
/// Returns `503` when no worker has heartbeated recently: a request nothing will
/// ever act on should say so, not look accepted.
pub async fn sync_repo(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GitRepo>, ApiError> {
    if !worker_online(&state.read_pool).await {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "no GitOps worker is running — deploy the dagron-gitops container to sync repositories"
                .to_string(),
        ));
    }
    let now = chrono::Utc::now().to_rfc3339();
    let row = sqlx::query_as::<_, GitRepo>(
        "UPDATE git_repos
         SET sync_requested_at = $1, last_message = 'sync requested'
         WHERE id = $2 RETURNING *",
    )
    .bind(&now)
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal)?;
    let row = row.ok_or((StatusCode::NOT_FOUND, "repository not found".to_string()))?;

    // Best-effort wake: the worker also polls, so a missed NOTIFY only costs
    // latency, never the sync.
    let _ = sqlx::query("SELECT pg_notify('gitops_sync', $1)")
        .bind(&id)
        .execute(&state.write_pool)
        .await;
    Ok(Json(row))
}

/// Whether insecure/local clone transports (`http`, `git`, `file`) are permitted.
/// Off by default so a server-side `git clone` can't be pointed at a plaintext
/// fetch, an internal host (SSRF), or a local path; opt in with
/// `DAGRON_GIT_ALLOW_INSECURE=1` (e.g. for `file://` in tests / air-gapped dev).
fn allow_insecure_git() -> bool {
    std::env::var("DAGRON_GIT_ALLOW_INSECURE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Only clone over schemes we accept. `https`/`ssh` are always allowed; the
/// plaintext/local transports (`http`, `git`, `file`) require
/// `DAGRON_GIT_ALLOW_INSECURE` (SSRF / internal-probing / local-read hardening).
/// Anything that would let `git` treat the URL as a flag or a local-command
/// transport (`ext::`), or that embeds credentials (`scheme://user:pass@host`),
/// is rejected so no secret is persisted in `git_repos.url`.
fn validate_git_url(url: &str) -> Result<(), ApiError> {
    let is_safe = ["https://", "ssh://"].iter().any(|s| url.starts_with(s));
    let is_insecure = ["http://", "git://", "file://"].iter().any(|s| url.starts_with(s));
    if url.starts_with('-') || (!is_safe && !is_insecure) {
        return Err((
            StatusCode::BAD_REQUEST,
            "url must start with https:// or ssh:// (set DAGRON_GIT_ALLOW_INSECURE=1 to also allow http:// git:// file://)".into(),
        ));
    }
    if is_insecure && !allow_insecure_git() {
        return Err((
            StatusCode::BAD_REQUEST,
            "insecure clone scheme (http/git/file) is disabled; use https:// or ssh:// (or set DAGRON_GIT_ALLOW_INSECURE=1)".into(),
        ));
    }
    // Reject userinfo in the authority (credentials before the host).
    if let Some((_, rest)) = url.split_once("://") {
        let authority = rest.split('/').next().unwrap_or_default();
        if authority.contains('@') {
            return Err((
                StatusCode::BAD_REQUEST,
                "url must not contain embedded credentials".into(),
            ));
        }
    }
    Ok(())
}


/// Inject a token into an https URL for private-repo clones — **only for trusted
/// forge hosts** (the worker re-checks before it dials), so a user-registered
/// `https://attacker.example/repo.git` never receives the global credential.
/// Token comes from `DAGRON_GIT_TOKEN` (fallback `GITHUB_TOKEN`); returns the
/// (possibly rewritten) URL and the token used, so it can be redacted from errors.

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
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_scheme_validation() {
        assert!(validate_git_url("https://github.com/o/r.git").is_ok());
        assert!(validate_git_url("ssh://git@host/o/r.git").is_err()); // userinfo rejected
        assert!(validate_git_url("ssh://host/o/r.git").is_ok());
        // Insecure/local schemes are off by default, on with the opt-in flag.
        std::env::remove_var("DAGRON_GIT_ALLOW_INSECURE");
        assert!(validate_git_url("file:///tmp/r").is_err());
        std::env::set_var("DAGRON_GIT_ALLOW_INSECURE", "1");
        assert!(validate_git_url("file:///tmp/r").is_ok());
        std::env::remove_var("DAGRON_GIT_ALLOW_INSECURE");
        assert!(validate_git_url("git@github.com:o/r.git").is_err()); // scp-style rejected
        assert!(validate_git_url("--upload-pack=evil").is_err());
        assert!(validate_git_url("ext::sh -c evil").is_err());
        // Embedded credentials are rejected so a secret can't be persisted.
        assert!(validate_git_url("https://tok@github.com/o/r.git").is_err());
        assert!(validate_git_url("https://u:p@github.com/o/r.git").is_err());

    }

    #[test]
    fn repo_name_parses_forms() {
        assert_eq!(repo_name("https://github.com/acme/etl.git"), "acme/etl");
        assert_eq!(repo_name("git@github.com:acme/etl.git"), "acme/etl");
        assert_eq!(repo_name("file:///srv/git/etl"), "git/etl");
    }

    // Real clone + walk + validate against a local file:// repo — offline, no DB.
}
