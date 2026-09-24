//! "Sync to Git" — commit a workflow's raw DAG spec to the configured GitOps
//! repo on a new branch and open a pull request, via the GitHub REST API.
//!
//! Server-side auth: dagron-api holds a single GitHub token (env), so PRs are
//! authored by that identity. The committed file is the **raw DAG spec** at
//! `<GIT_PATH_PREFIX><name>.yaml` — it is version history, not a Workflow CR, so
//! merging it does not feed the operator (by design).
//!
//! Config (all required except where noted):
//!   GITHUB_TOKEN        PAT / app token with `repo` (contents + pull_requests)
//!   GIT_REPO            "owner/name"
//!   GIT_BASE            base branch (default "main")
//!   GIT_PATH_PREFIX     path prefix for committed specs (default: the `path` of the connected
//!                       git repo matching GIT_REPO, so a merged PR lands where the pull loop
//!                       watches; "dags/" when no such repo is connected)
//!   GIT_API_BASE        API root (default "https://api.github.com"; set for GHE)

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use base64::Engine;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

type ApiError = (StatusCode, String);

struct GitConfig {
    token: String,
    repo: String,
    base: String,
    path_prefix: String,
    api_base: String,
}

impl GitConfig {
    fn from_env() -> Result<Self, ApiError> {
        let token = env("GITHUB_TOKEN")?;
        let repo = env("GIT_REPO")?;
        Ok(Self {
            token,
            repo,
            base: std::env::var("GIT_BASE").unwrap_or_else(|_| "main".to_string()),
            path_prefix: std::env::var("GIT_PATH_PREFIX").unwrap_or_else(|_| "dags/".to_string()),
            api_base: std::env::var("GIT_API_BASE")
                .unwrap_or_else(|_| "https://api.github.com".to_string()),
        })
    }
}

/// Whether a connected repo URL (`https://github.com/o/n.git`, `git@github.com:o/n.git`, ...) is the
/// `owner/name` in `GIT_REPO`.
fn url_is_repo(url: &str, repo: &str) -> bool {
    let url = url.trim().trim_end_matches('/').trim_end_matches(".git").to_lowercase();
    let repo = repo.trim().to_lowercase();
    url.ends_with(&format!("/{repo}")) || url.ends_with(&format!(":{repo}"))
}

/// `<path>/` of the connected git repo that is `GIT_REPO`: the directory the pull loop already
/// watches. `Ok(None)` means the lookup succeeded and no connected repo is that one.
///
/// The datastore error is returned, never folded into `None`: the caller reads `None` as "keep the
/// `dags/` default", so a transient failure here would open a pull request against a directory the
/// pull loop is not watching and report it as a success — the exact failure this default exists to
/// prevent. Better a 500 the operator retries.
async fn connected_repo_prefix(pool: &sqlx::PgPool, repo: &str) -> Result<Option<String>, ApiError> {
    // Ordered: the https and ssh spellings of one forge repo are distinct `url`s, so the UNIQUE
    // constraint allows both to be connected, and which of the two answered would otherwise be
    // whichever row the scan happened to return first. `created_at` is RFC3339, so it sorts as text.
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT url, path FROM git_repos ORDER BY created_at, id")
        .fetch_all(pool)
        .await
        .map_err(|e| internal(format!("reading the connected repositories: {e}")))?;
    Ok(rows
        .into_iter()
        .find(|(url, _)| url_is_repo(url, repo))
        .map(|(_, path)| format!("{}/", path.trim_matches('/'))))
}

fn env(key: &str) -> Result<String, ApiError> {
    std::env::var(key).map_err(|_| {
        (
            StatusCode::NOT_IMPLEMENTED,
            format!("git sync is not configured: set {key}"),
        )
    })
}

#[derive(Serialize)]
pub struct SyncResponse {
    pub pr_url: String,
    pub branch: String,
    pub path: String,
}

/// `POST /api/workflows/:id/sync-to-git`
pub async fn sync_to_git(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SyncResponse>, ApiError> {
    let mut cfg = GitConfig::from_env()?;
    if std::env::var("GIT_PATH_PREFIX").is_err() {
        if let Some(prefix) = connected_repo_prefix(&state.read_pool, &cfg.repo).await? {
            cfg.path_prefix = prefix;
        }
    }

    // Load the workflow's name + spec.
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT name, spec FROM workflows WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(|e| internal(format!("db: {e}")))?;
    let Some((name, spec)) = row else {
        return Err((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")));
    };

    let slug = slugify(&name);
    let path = format!("{}{}.yaml", cfg.path_prefix, slug);
    let branch = format!("dagron/{}-{}", slug, &Uuid::new_v4().simple().to_string()[..8]);

    let http = reqwest::Client::builder()
        .user_agent("dagron-api")
        // Bound every GitHub call so a stalled upstream can't pin a request handler.
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| internal(format!("http client: {e}")))?;

    // 1) base branch head sha.
    let base_sha = gh_get(&http, &cfg, &format!("/git/ref/heads/{}", cfg.base))
        .await?
        .pointer("/object/sha")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| internal("could not read base branch sha".into()))?;

    // 2) create the feature branch off base.
    gh_post(
        &http,
        &cfg,
        "/git/refs",
        json!({ "ref": format!("refs/heads/{branch}"), "sha": base_sha }),
    )
    .await?;

    // 3) existing file sha on the new branch (the PUT's target ref, so an update
    //    carries the right blob sha even if base advanced after we cut the branch).
    let existing_sha = gh_get_opt(&http, &cfg, &format!("/contents/{path}?ref={branch}"))
        .await?
        .as_ref()
        .and_then(|v| v.pointer("/sha"))
        .and_then(Value::as_str)
        .map(str::to_string);

    // 4) commit the spec onto the branch.
    let content_b64 = base64::engine::general_purpose::STANDARD.encode(spec.as_bytes());
    let mut put = json!({
        "message": format!("dagron: sync workflow '{name}'"),
        "content": content_b64,
        "branch": branch,
    });
    if let Some(sha) = existing_sha {
        put["sha"] = Value::String(sha);
    }
    gh_put(&http, &cfg, &format!("/contents/{path}"), put).await?;

    // 5) open the PR.
    // Attribute by display name only; keep the requester's email out of repo metadata.
    let body = format!(
        "Synced from the dagron UI by {}.\n\nUpdates the raw DAG spec for `{name}`.",
        claims.name
    );
    let pr = gh_post(
        &http,
        &cfg,
        "/pulls",
        json!({
            "title": format!("dagron: update workflow '{name}'"),
            "head": branch,
            "base": cfg.base,
            "body": body,
        }),
    )
    .await?;
    let pr_url = pr
        .get("html_url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    tracing::info!(%name, %branch, %pr_url, "workflow synced to git");
    Ok(Json(SyncResponse { pr_url, branch, path }))
}

// ── GitHub REST helpers ─────────────────────────────────────────────────────

async fn gh_get(http: &reqwest::Client, cfg: &GitConfig, path: &str) -> Result<Value, ApiError> {
    gh_get_opt(http, cfg, path)
        .await?
        .ok_or_else(|| (StatusCode::BAD_GATEWAY, format!("github: {path} not found")))
}

/// GET that maps 404 to `None` (used to probe whether a file already exists).
async fn gh_get_opt(
    http: &reqwest::Client,
    cfg: &GitConfig,
    path: &str,
) -> Result<Option<Value>, ApiError> {
    let res = http
        .get(format!("{}/repos/{}{}", cfg.api_base, cfg.repo, path))
        .bearer_auth(&cfg.token)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("github request failed: {e}")))?;
    if res.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let v = parse(res).await?;
    Ok(Some(v))
}

async fn gh_post(
    http: &reqwest::Client,
    cfg: &GitConfig,
    path: &str,
    body: Value,
) -> Result<Value, ApiError> {
    let res = http
        .post(format!("{}/repos/{}{}", cfg.api_base, cfg.repo, path))
        .bearer_auth(&cfg.token)
        .header("Accept", "application/vnd.github+json")
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("github request failed: {e}")))?;
    parse(res).await
}

async fn gh_put(
    http: &reqwest::Client,
    cfg: &GitConfig,
    path: &str,
    body: Value,
) -> Result<Value, ApiError> {
    let res = http
        .put(format!("{}/repos/{}{}", cfg.api_base, cfg.repo, path))
        .bearer_auth(&cfg.token)
        .header("Accept", "application/vnd.github+json")
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("github request failed: {e}")))?;
    parse(res).await
}

/// Turn a GitHub response into JSON, surfacing the API's error message on non-2xx.
async fn parse(res: reqwest::Response) -> Result<Value, ApiError> {
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if !status.is_success() {
        let msg = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
            .unwrap_or(text);
        return Err((StatusCode::BAD_GATEWAY, format!("github {status}: {msg}")));
    }
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// Make a filesystem/branch-safe slug from a (possibly namespaced) workflow name.
fn slugify(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    let s = s.trim_matches('-').to_lowercase();
    if s.is_empty() { "workflow".to_string() } else { s }
}

fn internal(msg: String) -> ApiError {
    tracing::error!(%msg, "git sync failed");
    (StatusCode::INTERNAL_SERVER_ERROR, msg)
}

#[cfg(test)]
mod tests {
    use super::url_is_repo;

    /// The connected repo is found from the forms a URL is stored in, and only when `owner/name` matches whole.
    #[test]
    fn a_connected_url_is_matched_to_git_repo() {
        assert!(url_is_repo("https://github.com/Acme/Flows.git", "acme/flows"));
        assert!(url_is_repo("git@github.com:acme/flows.git", "acme/flows"));
        assert!(url_is_repo("ssh://git@host/acme/flows/", "acme/flows"));
        assert!(!url_is_repo("https://github.com/acme/flows-extra.git", "acme/flows"));
        assert!(!url_is_repo("https://github.com/other-acme/flows.git", "acme/flows"));
    }
}
