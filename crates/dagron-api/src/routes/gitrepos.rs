//! GitOps repository registry + pull reconcile — the set of Git repos dagron
//! tracks, surfaced on the UI's GitOps page (connect / list / sync / disconnect).
//!
//! dagron-api owns the `git_repos` table (it ensures the schema itself, like the
//! users table) — the engine never reads it. The **Sync** action performs a real
//! shallow `git clone` of the registered branch, validates every workflow YAML
//! under the repo's configured path through the same parser the submit path uses
//! ([`control::parse_and_validate`]), and upserts each into the `workflows`
//! table keyed by name — the Git → datastore *pull* half of GitOps, decoupled
//! from CRDs. The fetched commit (`rev`), synced count, and per-file errors are
//! recorded on the row. (A background `auto_sync` poller is the remaining piece;
//! `auto_sync` is stored so it can drive one.)

use std::path::{Path as FsPath, PathBuf};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::routes::control;
use crate::state::AppState;

type ApiError = (StatusCode, String);

/// Default in-repo directory scanned for workflow YAML when none is given.
const DEFAULT_PATH: &str = "dagron";

/// Wall-clock cap on a `git` subprocess (clone / rev-parse) so a slow or stuck
/// remote can't pin the API request indefinitely.
const GIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

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
/// failure is a 5xx.
pub async fn sync_repo(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GitRepo>, ApiError> {
    let repo = sqlx::query_as::<_, GitRepo>("SELECT * FROM git_repos WHERE id=$1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, "repository not found".to_string()))?;

    let now = chrono::Utc::now().to_rfc3339();
    let (rev, new_state, count, message) = match reconcile(&state, &repo).await {
        Ok(report) => {
            let count = report.synced.len() as i64;
            if report.errors.is_empty() {
                let msg = if count == 0 {
                    format!(
                        "no workflow files under '{}' at {}",
                        repo.path,
                        short(&report.rev)
                    )
                } else {
                    format!("synced {count} workflow(s) at {}", short(&report.rev))
                };
                (Some(report.rev), "Synced", count, msg)
            } else {
                let msg = format!(
                    "synced {count}, {} error(s): {}",
                    report.errors.len(),
                    report.errors.join("; ")
                );
                (Some(report.rev), "Error", count, msg)
            }
        }
        Err(e) => (repo.rev.clone(), "Error", repo.workflow_count, e),
    };

    let row = sqlx::query_as::<_, GitRepo>(
        "UPDATE git_repos
         SET state=$1, rev=$2, workflow_count=$3, drift=0, last_message=$4, last_synced_at=$5
         WHERE id=$6 RETURNING *",
    )
    .bind(new_state)
    .bind(&rev)
    .bind(count)
    .bind(&message)
    .bind(&now)
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal)?;
    row.map(Json)
        .ok_or((StatusCode::NOT_FOUND, "repository not found".into()))
}

/// Outcome of a repo reconcile: the fetched commit, the workflow names upserted,
/// and any per-file validation errors (which don't abort the whole sync).
struct Reconcile {
    rev: String,
    synced: Vec<String>,
    errors: Vec<String>,
}

/// Shallow-clone `repo` into a scratch dir, validate + upsert every workflow YAML
/// under its `path`. The `Err(String)` case (clone failed, path missing, …) is a
/// human-readable message stored on the row; per-file parse errors are collected
/// into `Reconcile::errors` so one bad file doesn't block the good ones.
async fn reconcile(state: &AppState, repo: &GitRepo) -> Result<Reconcile, String> {
    let (rev, valid, mut errors) = fetch_and_validate(&repo.url, &repo.branch, &repo.path).await?;
    let mut synced = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (name, yaml) in valid {
        // `upsert_workflow` keys by name, so two files with the same DAG name would
        // silently clobber each other while `synced` counts both — reject the dup.
        if !seen.insert(name.clone()) {
            errors.push(format!("duplicate workflow name '{name}'"));
            continue;
        }
        match upsert_workflow(state, &name, &yaml).await {
            Ok(()) => synced.push(name),
            Err(e) => errors.push(format!("{name}: {e}")),
        }
    }
    Ok(Reconcile {
        rev,
        synced,
        errors,
    })
}

/// Clone the branch and validate every workflow YAML under `path`, returning
/// `(rev, valid [(name, yaml)], per-file errors)`. No datastore access — the DB
/// upsert is layered on in [`reconcile`], so this half is testable offline
/// against a `file://` repo.
async fn fetch_and_validate(
    url: &str,
    branch: &str,
    path: &str,
) -> Result<(String, Vec<(String, String)>, Vec<String>), String> {
    let scratch = std::env::temp_dir().join(format!("dagron-gitsync-{}", Uuid::new_v4()));
    let _guard = TempDir(scratch.clone());
    clone(url, branch, &scratch).await?;
    let rev = rev_parse(&scratch).await?;

    let dir = scratch.join(path);
    // Reject a symlinked top directory (e.g. `dagron -> /etc`) before descending —
    // symlink_metadata does not follow the link.
    match std::fs::symlink_metadata(&dir) {
        Ok(md) if md.file_type().is_symlink() => {
            return Err(format!("path '{path}' is a symlink — refusing to sync"));
        }
        _ => {}
    }
    if !dir.is_dir() {
        return Err(format!("path '{path}' not found in {branch}"));
    }
    let mut files = Vec::new();
    collect_yaml(&dir, &mut files).map_err(|e| format!("reading '{path}': {e}"))?;
    files.sort();

    let mut valid = Vec::new();
    let mut errors = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(&scratch)
            .unwrap_or(file)
            .display()
            .to_string();
        let yaml = match std::fs::read_to_string(file) {
            Ok(y) => y,
            Err(e) => {
                errors.push(format!("{rel}: {e}"));
                continue;
            }
        };
        match control::parse_and_validate(&yaml) {
            Ok(spec) => valid.push((spec.name, yaml)),
            Err((_, msg)) => errors.push(format!("{rel}: {msg}")),
        }
    }
    Ok((rev, valid, errors))
}

/// Upsert a workflow definition by name (the reconcile is idempotent — the same
/// commit synced twice is a no-op beyond `updated_at`).
async fn upsert_workflow(state: &AppState, name: &str, yaml: &str) -> Result<(), String> {
    let now = chrono::Utc::now().to_rfc3339();
    let id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO workflows (id, name, spec, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$4)
         ON CONFLICT (name) DO UPDATE SET spec = EXCLUDED.spec, updated_at = EXCLUDED.updated_at",
    )
    .bind(&id)
    .bind(name)
    .bind(yaml)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map(|_| ())
    .map_err(|e| format!("upsert failed: {e}"))
}

/// `git clone --depth 1 --single-branch --branch <branch> -- <url> <dir>`.
/// Secrets are injected into the URL only if a token env is set; the token is
/// redacted from any error surfaced to the caller.
async fn clone(url: &str, branch: &str, dir: &FsPath) -> Result<(), String> {
    let (auth_url, token) = with_token(url);
    let fut = tokio::process::Command::new("git")
        .args([
            "clone",
            "--depth",
            "1",
            "--single-branch",
            "--branch",
            branch,
            "--",
            &auth_url,
        ])
        .arg(dir)
        .output();
    let out = tokio::time::timeout(GIT_TIMEOUT, fut)
        .await
        .map_err(|_| "git clone timed out".to_string())?
        .map_err(|e| format!("running git: {e} (is git installed?)"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stderr = redact(&stderr, token.as_deref());
        return Err(format!("git clone failed: {}", stderr.trim()));
    }
    Ok(())
}

/// Short HEAD SHA of the cloned worktree.
async fn rev_parse(dir: &FsPath) -> Result<String, String> {
    let fut = tokio::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "HEAD"])
        .output();
    let out = tokio::time::timeout(GIT_TIMEOUT, fut)
        .await
        .map_err(|_| "git rev-parse timed out".to_string())?
        .map_err(|e| format!("running git rev-parse: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "git rev-parse failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
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

/// Whether `url`'s host is a trusted Git forge the global token may be sent to.
/// Defaults to `github.com` / `gitlab.com` (and their subdomains); extend with
/// `DAGRON_GIT_TRUSTED_HOSTS` (comma-separated) for GHE / self-managed GitLab.
fn is_trusted_git_host(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("https://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    // Userinfo is already rejected by validate_git_url, but be defensive.
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    let host = host_port.split(':').next().unwrap_or_default().to_ascii_lowercase();
    let mut trusted: Vec<String> = vec!["github.com".to_string(), "gitlab.com".to_string()];
    if let Ok(extra) = std::env::var("DAGRON_GIT_TRUSTED_HOSTS") {
        trusted.extend(
            extra
                .split(',')
                .map(|h| h.trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty()),
        );
    }
    trusted.iter().any(|t| host == *t || host.ends_with(&format!(".{t}")))
}

/// Inject a token into an https URL for private-repo clones — **only for trusted
/// forge hosts** (see [`is_trusted_git_host`]), so a user-registered
/// `https://attacker.example/repo.git` never receives the global credential.
/// Token comes from `DAGRON_GIT_TOKEN` (fallback `GITHUB_TOKEN`); returns the
/// (possibly rewritten) URL and the token used, so it can be redacted from errors.
fn with_token(url: &str) -> (String, Option<String>) {
    let token = std::env::var("DAGRON_GIT_TOKEN")
        .or_else(|_| std::env::var("GITHUB_TOKEN"))
        .ok()
        .filter(|t| !t.is_empty());
    match &token {
        Some(t) if url.starts_with("https://") && is_trusted_git_host(url) => (
            format!("https://x-access-token:{t}@{}", &url["https://".len()..]),
            token.clone(),
        ),
        _ => (url.to_string(), None),
    }
}

fn redact(s: &str, token: Option<&str>) -> String {
    match token {
        Some(t) if !t.is_empty() => s.replace(t, "***"),
        _ => s.to_string(),
    }
}

fn short(rev: &str) -> String {
    rev.chars().take(8).collect()
}

/// Recursively collect `*.yaml` / `*.yml` files under `dir` (hidden dirs skipped).
/// **Symlinks are skipped** (via `file_type()`, which does not follow them) so a
/// malicious repo can't `dagron -> /etc` or `secret.yaml -> /host/file` and make
/// sync read outside the checkout.
fn collect_yaml(dir: &FsPath, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        // read_dir's DirEntry::file_type does NOT traverse symlinks — reject them.
        if entry.file_type()?.is_symlink() {
            continue;
        }
        let path = entry.path();
        let hidden = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with('.'))
            .unwrap_or(false);
        if hidden {
            continue;
        }
        if path.is_dir() {
            collect_yaml(&path, out)?;
        } else if matches!(
            path.extension().and_then(|s| s.to_str()),
            Some("yaml") | Some("yml")
        ) {
            out.push(path);
        }
    }
    Ok(())
}

/// Removes a scratch clone directory on drop (best-effort).
struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_scheme_and_token_handling() {
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

        // Token is injected only for https on a TRUSTED host, and is redactable.
        std::env::set_var("DAGRON_GIT_TOKEN", "s3cr3t");
        let (u, tok) = with_token("https://github.com/o/r.git");
        assert_eq!(u, "https://x-access-token:s3cr3t@github.com/o/r.git");
        assert_eq!(tok.as_deref(), Some("s3cr3t"));
        // …but NOT to an arbitrary/untrusted host (no credential leak).
        let (u2, tok2) = with_token("https://attacker.example/o/r.git");
        assert_eq!(u2, "https://attacker.example/o/r.git");
        assert_eq!(tok2, None);
        assert_eq!(
            redact("fatal: auth for s3cr3t failed", tok.as_deref()),
            "fatal: auth for *** failed"
        );
        // Non-https URLs are never rewritten (no token leak into ssh/file).
        assert_eq!(with_token("file:///tmp/r").1, None);
        std::env::remove_var("DAGRON_GIT_TOKEN");
    }

    #[test]
    fn repo_name_parses_forms() {
        assert_eq!(repo_name("https://github.com/acme/etl.git"), "acme/etl");
        assert_eq!(repo_name("git@github.com:acme/etl.git"), "acme/etl");
        assert_eq!(repo_name("file:///srv/git/etl"), "git/etl");
    }

    // Real clone + walk + validate against a local file:// repo — offline, no DB.
    #[tokio::test]
    async fn fetch_and_validate_against_local_repo() {
        use std::process::Command;
        let root = std::env::temp_dir().join(format!("dagron-gitsrc-{}", Uuid::new_v4()));
        std::fs::create_dir_all(root.join("dagron")).unwrap();
        std::fs::write(
            root.join("dagron/good.yaml"),
            "name: nightly\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .unwrap();
        std::fs::write(
            root.join("dagron/bad.yaml"),
            "name: broken\ntasks:\n  - { name: a, command: [\"true\"], depends_on: [ghost] }\n",
        )
        .unwrap();
        std::fs::write(root.join("README.md"), "not a workflow").unwrap();

        let git = |args: &[&str]| {
            let ok = Command::new("git")
                .current_dir(&root)
                .args(args)
                .output()
                .expect("run git")
                .status
                .success();
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "-b", "main"]);
        git(&["-c", "user.email=t@t", "-c", "user.name=t", "add", "."]);
        git(&[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-m",
            "seed",
        ]);

        let url = format!("file://{}", root.display());
        let (rev, valid, errors) = fetch_and_validate(&url, "main", "dagron").await.unwrap();

        assert_eq!(rev.len(), 40, "full SHA expected, got {rev:?}");
        // The valid workflow is keyed by its DAG name; the cyclic one is an error.
        assert_eq!(
            valid.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            vec!["nightly"]
        );
        assert_eq!(errors.len(), 1, "one invalid file: {errors:?}");
        assert!(errors[0].contains("bad.yaml"));

        // A missing path is a hard error (not silently empty).
        assert!(fetch_and_validate(&url, "main", "nope").await.is_err());

        std::fs::remove_dir_all(&root).ok();
    }
}
