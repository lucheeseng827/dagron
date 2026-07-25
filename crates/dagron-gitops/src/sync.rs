//! Clone a connected repo, validate its workflow specs, upsert them.
//!
//! Moved verbatim-in-behaviour out of `dagron-api::routes::gitrepos`, which used
//! to do this inside the HTTP gateway. The gateway runs on distroless (no shell,
//! no git binary), so every sync failed with `running git: No such file or
//! directory` — the feature was unreachable on the shipped image. Rather than put
//! git into the internet-facing container for a feature most deployments never
//! enable, the work lives here, in an image that is deployed only when GitOps is
//! wanted.
//!
//! The hardening the gateway version carried comes along unchanged: symlinks are
//! never followed (a repo cannot `dagron -> /etc`), the clone directory is a
//! scratch dir removed on drop, tokens are injected only for trusted https hosts
//! and redacted from every surfaced error, and both git invocations are bounded
//! by a timeout.

use std::collections::HashSet;
use std::path::{Path as FsPath, PathBuf};
use std::time::Duration;

use uuid::Uuid;

/// Ceiling on a single `git` invocation — an unreachable host must not wedge the
/// poll loop.
const GIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Outcome of a repo reconcile: the fetched commit, the workflow names upserted,
/// and any per-file validation errors (which don't abort the whole sync).
pub struct Reconcile {
    pub rev: String,
    pub synced: Vec<String>,
    /// Files that *are* dagron specs but failed to parse or upsert — actionable.
    pub errors: Vec<String>,
    /// Files that are not dagron specs at all (no `tasks:`) and were ignored.
    /// A repo's workflow directory routinely sits beside compose files, CI
    /// config, Argo manifests and Grafana provisioning; counting those as errors
    /// parked the whole repo in a red state and buried the failures worth acting
    /// on among them.
    pub skipped: usize,
}

/// One connected repo, as far as syncing is concerned.
pub struct Repo {
    pub url: String,
    pub branch: String,
    pub path: String,
}

/// Clone, validate, and upsert every workflow under `repo.path`.
///
/// `Err` is a human-readable message stored on the row (clone failed, path
/// missing, …); per-file parse errors are collected into `Reconcile::errors` so
/// one bad file doesn't block the good ones.
pub async fn reconcile(pool: &sqlx::PgPool, repo: &Repo) -> Result<Reconcile, String> {
    let (rev, valid, mut errors, skipped) =
        fetch_and_validate(&repo.url, &repo.branch, &repo.path).await?;
    let mut synced = Vec::new();
    let mut seen = HashSet::new();
    for (name, yaml) in valid {
        // `upsert_workflow` keys by name, so two files with the same DAG name
        // would silently clobber each other while `synced` counts both.
        if !seen.insert(name.clone()) {
            errors.push(format!("duplicate workflow name '{name}'"));
            continue;
        }
        match upsert_workflow(pool, &name, &yaml).await {
            Ok(()) => synced.push(name),
            Err(e) => errors.push(format!("{name}: {e}")),
        }
    }
    Ok(Reconcile { rev, synced, errors, skipped })
}

/// Clone the branch and validate every workflow YAML under `path`, returning
/// `(rev, valid [(name, yaml)], per-file errors)`. No datastore access, so this
/// half is testable offline against a `file://` repo.
async fn fetch_and_validate(
    url: &str,
    branch: &str,
    path: &str,
) -> Result<(String, Vec<(String, String)>, Vec<String>, usize), String> {
    let scratch = std::env::temp_dir().join(format!("dagron-gitops-{}", Uuid::new_v4()));
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
    let mut skipped = 0usize;
    for file in &files {
        let rel = file.strip_prefix(&scratch).unwrap_or(file).display().to_string();
        let yaml = match std::fs::read_to_string(file) {
            Ok(y) => y,
            Err(e) => {
                errors.push(format!("{rel}: {e}"));
                continue;
            }
        };
        // Not a dagron spec → not this repo's problem. `tasks:` is the one
        // field every workflow has and no unrelated YAML does, so it is the
        // cheapest honest discriminator.
        if !looks_like_spec(&yaml) {
            skipped += 1;
            continue;
        }
        match validate(&yaml) {
            Ok(name) => valid.push((name, yaml)),
            Err(msg) => errors.push(format!("{rel}: {msg}")),
        }
    }
    Ok((rev, valid, errors, skipped))
}

/// Whether a YAML file is a dagron workflow spec at all: a mapping carrying a
/// `tasks:` key. Anything else in the scanned directory is ignored rather than
/// reported — see [`Reconcile::skipped`].
fn looks_like_spec(yaml: &str) -> bool {
    serde_yaml::from_str::<serde_yaml::Value>(yaml)
        .ok()
        .and_then(|v| v.get("tasks").cloned())
        .is_some()
}

/// Validate one spec with the **engine's** parser and return its DAG name, so a
/// file that syncs is a file the engine can actually run.
///
/// `workflow_ref` tasks are the one thing the engine's parser cannot judge: the
/// reference is resolved by dagron-api against saved workflows at submit, so to
/// the engine such a task looks like a leaf with no command. They are given a
/// placeholder command for validation only — the stored YAML is untouched.
fn validate(yaml: &str) -> Result<String, String> {
    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|e| format!("invalid YAML: {e}"))?;
    if let Some(tasks) = doc.get_mut("tasks").and_then(serde_yaml::Value::as_sequence_mut) {
        for task in tasks {
            let is_ref = task.get("workflow_ref").is_some();
            let has_cmd = task.get("command").is_some();
            if is_ref && !has_cmd {
                if let Some(map) = task.as_mapping_mut() {
                    map.insert(
                        serde_yaml::Value::from("command"),
                        serde_yaml::Value::Sequence(vec![serde_yaml::Value::from("true")]),
                    );
                }
            }
        }
    }
    let probe = serde_yaml::to_string(&doc).map_err(|e| format!("re-serializing spec: {e}"))?;
    let dag = dagron_core::dag::DagGraph::from_yaml(&probe).map_err(|e| format!("{e}"))?;
    Ok(dag.spec.name.clone())
}

/// Upsert a workflow definition by name. The reconcile is idempotent — the same
/// commit synced twice is a no-op beyond `updated_at`.
async fn upsert_workflow(pool: &sqlx::PgPool, name: &str, yaml: &str) -> Result<(), String> {
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
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|e| format!("upsert failed: {e}"))
}

/// `git clone --depth 1 --single-branch --branch <branch> -- <url> <dir>`.
async fn clone(url: &str, branch: &str, dir: &FsPath) -> Result<(), String> {
    let (auth_url, token) = with_token(url);
    let fut = tokio::process::Command::new("git")
        .args(["clone", "--depth", "1", "--single-branch", "--branch", branch, "--", &auth_url])
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

/// Full HEAD SHA of the cloned worktree.
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

/// Inject a token into the clone URL, but only for https on a trusted host, so a
/// repo URL pointing at an attacker's server can never receive the credential.
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

/// Hosts the token may be sent to. Mirrors dagron-api's connect-time check; kept
/// here too because this process is the one that actually dials.
fn is_trusted_git_host(url: &str) -> bool {
    let rest = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => url,
    };
    let host = rest.split('/').next().unwrap_or("");
    let host = host.rsplit('@').next().unwrap_or(host);
    let host = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
    matches!(host.as_str(), "github.com" | "gitlab.com" | "bitbucket.org")
        || host.ends_with(".github.com")
        || host.ends_with(".gitlab.com")
}

fn redact(s: &str, token: Option<&str>) -> String {
    match token {
        Some(t) if !t.is_empty() => s.replace(t, "***"),
        _ => s.to_string(),
    }
}

pub fn short(rev: &str) -> String {
    rev.chars().take(8).collect()
}

/// Recursively collect `*.yaml` / `*.yml` under `dir` (hidden dirs skipped).
/// **Symlinks are skipped** (via `file_type()`, which does not follow them) so a
/// malicious repo can't `secret.yaml -> /host/file` and make sync read outside
/// the checkout.
fn collect_yaml(dir: &FsPath, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_symlink() {
            continue;
        }
        let path = entry.path();
        let hidden =
            path.file_name().and_then(|n| n.to_str()).map(|n| n.starts_with('.')).unwrap_or(false);
        if hidden {
            continue;
        }
        if path.is_dir() {
            collect_yaml(&path, out)?;
        } else if matches!(path.extension().and_then(|s| s.to_str()), Some("yaml") | Some("yml")) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The 27-errors-on-a-green-sync case: unrelated YAML beside the workflows
    /// must not be reported as failures, but a broken *workflow* still must be.
    #[test]
    fn ignores_yaml_that_is_not_a_spec() {
        assert!(!looks_like_spec("services:
  db:
    image: postgres
"));
        assert!(!looks_like_spec("apiVersion: argoproj.io/v1alpha1
kind: ApplicationSet
"));
        assert!(!looks_like_spec("not: yaml: at: all: ["));
        assert!(looks_like_spec("name: wf
tasks:
  - name: a
    command: [\"true\"]
"));
        // A file that *is* a spec but is malformed stays an error, not a skip.
        assert!(looks_like_spec("tasks:
  - name: a
"));
    }

    #[test]
    fn validates_with_the_engine_parser() {
        let name = validate("name: ok\ntasks:\n  - name: a\n    command: [\"true\"]\n").unwrap();
        assert_eq!(name, "ok");
        // A spec the engine would reject never reaches the workflows table.
        assert!(validate("name: bad\ntasks:\n  - name: a\n").is_err());
    }

    /// A `workflow_ref` task is command-less on purpose — dagron-api resolves it
    /// at submit. Validation must not reject the file for that alone.
    #[test]
    fn tolerates_workflow_ref_tasks() {
        let name =
            validate("name: chain\ntasks:\n  - name: call\n    workflow_ref: other\n").unwrap();
        assert_eq!(name, "chain");
    }

    #[test]
    fn token_only_goes_to_trusted_https_hosts() {
        std::env::set_var("DAGRON_GIT_TOKEN", "secret-token");
        let (url, tok) = with_token("https://evil.example.com/x/y.git");
        assert!(!url.contains("secret-token"), "token leaked to an untrusted host");
        assert!(tok.is_none());
        let (url, tok) = with_token("https://github.com/o/r.git");
        assert!(url.contains("secret-token"));
        assert_eq!(tok.as_deref(), Some("secret-token"));
        assert_eq!(redact("fatal: secret-token bad", tok.as_deref()), "fatal: *** bad");
        std::env::remove_var("DAGRON_GIT_TOKEN");
    }
}
