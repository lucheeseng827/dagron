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

/// HTTPS username used with the worker-wide token when none is configured, and
/// the fallback when a stored credential has none. Public so `main.rs` uses this
/// one rather than repeating the literal.
pub const DEFAULT_TOKEN_USER: &str = "x-access-token";

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
    /// This repo's own credential, already decrypted by the caller. `None` falls
    /// back to the worker-wide `DAGRON_GIT_TOKEN` (trusted forge hosts only), or
    /// to an unauthenticated clone.
    pub auth: Option<GitAuth>,
}

/// A decrypted Git credential. Two kinds because the forges hand out two kinds:
/// HTTPS tokens (GitHub App installation tokens, GitLab project access tokens)
/// and SSH keys (deploy keys, and every host that only speaks `git@`).
#[derive(Clone)]
pub enum GitAuth {
    Token {
        username: String,
        token: String,
    },
    Ssh {
        key: String,
        /// SSH account to connect as. dagron-api stores one for every SSH
        /// credential (operator input, else the URL's, else `git`) and the
        /// console shows it back, so dropping it here would mean the clone
        /// connecting as somebody other than the user the console reports.
        user: Option<String>,
        known_hosts: Option<String>,
    },
}

impl std::fmt::Debug for GitAuth {
    /// Hand-written so a stray `{:?}` in a log line can never print a token or
    /// a private key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitAuth::Token { username, .. } => write!(f, "Token {{ username: {username:?} }}"),
            GitAuth::Ssh { user, known_hosts, .. } => {
                write!(f, "Ssh {{ user: {user:?}, known_hosts: {} }}", known_hosts.is_some())
            }
        }
    }
}

/// Clone, validate, and upsert every workflow under `repo.path`.
///
/// `Err` is a human-readable message stored on the row (clone failed, path
/// missing, …); per-file parse errors are collected into `Reconcile::errors` so
/// one bad file doesn't block the good ones.
pub async fn reconcile(pool: &sqlx::PgPool, repo: &Repo) -> Result<Reconcile, String> {
    let (rev, valid, mut errors, skipped) =
        fetch_and_validate(&repo.url, &repo.branch, &repo.path, repo.auth.as_ref()).await?;
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
    auth: Option<&GitAuth>,
) -> Result<(String, Vec<(String, String)>, Vec<String>, usize), String> {
    // dagron-api vets a URL when the repo is connected, but this worker clones
    // whatever `git_repos.url` holds — a row written before that check existed,
    // or straight into the datastore, has never been through it. `git` reads
    // `ext::<command>` as a transport that *runs the command*, so an unchecked
    // URL here is remote code execution in the one container that holds the
    // credentials. Re-check at the point of use.
    check_clonable(url)?;
    // One scratch dir per sync, holding the checkout *and* the credential files
    // git is pointed at. They are siblings rather than nested because `git clone`
    // insists on an empty destination — and because a single guard then removes
    // the key and the credential file on every exit path, including the error
    // ones.
    let work = std::env::temp_dir().join(format!("dagron-gitops-{}", Uuid::new_v4()));
    let _guard = TempDir(work.clone());
    std::fs::create_dir_all(&work).map_err(|e| format!("creating scratch dir: {e}"))?;
    let creds = GitCreds::prepare(&work, url, auth)?;
    let scratch = work.join("repo");
    clone(url, branch, &scratch, &creds).await?;
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

/// `git clone --depth 1 --single-branch --branch <branch> -- <url> <dir>`, with
/// the credential supplied out-of-band (see [`GitCreds`]).
async fn clone(url: &str, branch: &str, dir: &FsPath, creds: &GitCreds) -> Result<(), String> {
    let mut cmd = tokio::process::Command::new("git");
    for cfg in &creds.config {
        cmd.arg("-c").arg(cfg);
    }
    for (k, v) in &creds.env {
        cmd.env(k, v);
    }
    let fut = cmd
        .args(["clone", "--depth", "1", "--single-branch", "--branch", branch, "--", url])
        .arg(dir)
        .output();
    let out = tokio::time::timeout(GIT_TIMEOUT, fut)
        .await
        .map_err(|_| "git clone timed out".to_string())?
        .map_err(|e| format!("running git: {e} (is git installed?)"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let stderr = creds.redact(&stderr);
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

/// How a credential is handed to `git`, without ever putting it on the command
/// line.
///
/// The secret used to be spliced into the clone URL
/// (`https://x-access-token:TOKEN@host/…`), which meant it appeared in this
/// process's argv — and `/proc/<pid>/cmdline` is world-readable, so anything
/// sharing the container could read the deploy token out of it. Both kinds now
/// travel by a path git is told to read:
///
/// * **token** → a `credential.helper=store` file, mode 0600, in the scratch dir;
/// * **ssh key** → a key file, mode 0600, named by `GIT_SSH_COMMAND -i`.
///
/// Only file paths reach argv; the scratch dir is removed when the sync ends.
struct GitCreds {
    /// `-c key=value` settings prepended to the git invocation.
    config: Vec<String>,
    env: Vec<(String, String)>,
    /// Values to scrub from anything git prints back at us.
    secrets: Vec<String>,
}

impl GitCreds {
    /// Materialize the credential for one clone into `work`.
    fn prepare(work: &FsPath, url: &str, auth: Option<&GitAuth>) -> Result<Self, String> {
        // `git` must never stop to ask a human: this is a background worker with
        // no terminal, and without these a missing credential hangs to the
        // timeout instead of failing with the reason.
        let mut creds = GitCreds {
            // `ext::<command>` is a git transport that executes the command.
            // `check_clonable` already refuses to hand git such a URL; this
            // turns it off in git itself as well, so a future path that reaches
            // `clone` without that check is not an execution primitive.
            config: vec!["protocol.ext.allow=never".into()],
            env: vec![("GIT_TERMINAL_PROMPT".into(), "0".into())],
            secrets: Vec::new(),
        };
        match effective_auth(url, auth) {
            None => Ok(creds),
            Some(GitAuth::Token { username, token }) => {
                let file = work.join("git-credentials");
                let origin = credential_origin(url)
                    .ok_or_else(|| "cannot use a token with this URL — it is not http(s)".to_string())?;
                // git-credential-store's format is one URL per line. Both fields
                // are percent-encoded: a token containing '@' or '/' (GitLab's
                // do) would otherwise re-parse as a different host entirely.
                let entry = format!("{}://{}:{}@{}\n", origin.0, pct(&username), pct(&token), origin.1);
                write_private(&file, &entry)?;
                // The empty helper first *clears* any helper inherited from
                // system/global config, so the only credential in play is ours.
                creds.config.push("credential.helper=".into());
                // Quoted for the same reason as GIT_SSH_COMMAND below: git
                // splits a helper value with arguments, and `TMPDIR` is the
                // operator's to set.
                creds.config
                    .push(format!("credential.helper=store --file={}", shell_quote(&file)?));
                // Match on host alone: the stored entry has no path, and with
                // path matching on git would look for a per-path entry and find
                // nothing.
                creds.config.push("credential.useHttpPath=false".into());
                creds.secrets.push(token);
                Ok(creds)
            }
            Some(GitAuth::Ssh { key, user, known_hosts }) => {
                // Symmetric with the token arm, which fails via
                // `credential_origin` when the URL is not http(s). git only
                // consults `GIT_SSH_COMMAND` for the SSH transport, so a key
                // against an https URL is silently ignored and the clone fails
                // as unauthenticated — "repository not found", with the console
                // still reporting a credential as installed. dagron-api refuses
                // to pair them, so this catches a row written around it.
                if !is_ssh_url(url) {
                    return Err(format!(
                        "this repository has an SSH key but '{url}' is not an SSH URL — git would ignore the key; re-connect it as ssh:// (or git@host:owner/repo), or attach a token instead"
                    ));
                }
                let key_file = work.join("id");
                write_private(&key_file, &key)?;
                // A key is not echoed by ssh today, so this scrubs nothing at
                // present. It costs one line and makes the guarantee hold
                // whatever a future ssh or git decides to print.
                creds.secrets.push(key);
                let kh_file = work.join("known_hosts");
                write_private(&kh_file, known_hosts.as_deref().unwrap_or(""))?;
                // Host-key policy. With known_hosts supplied the forge's key is
                // verified for real. Without it there is nothing to verify
                // against — `accept-new` against a scratch file that is deleted
                // after the sync is trust-on-first-use with no memory, i.e. no
                // authentication of the server at all. That is the historical
                // default for this feature and stays the default so existing
                // deployments keep working, but `DAGRON_GIT_SSH_STRICT=1` turns
                // it into a refusal instead of a silent downgrade.
                let strict = match &known_hosts {
                    Some(_) => "yes",
                    None if ssh_strict_required() => {
                        return Err(
                            "DAGRON_GIT_SSH_STRICT is set but this repository has no known_hosts entries — add the forge's host key to its credential"
                                .to_string(),
                        )
                    }
                    None => "accept-new",
                };
                // git splits GIT_SSH_COMMAND on whitespace (and runs it through
                // a shell once it sees metacharacters), so the paths are
                // single-quoted. The scratch root comes from `temp_dir()`, i.e.
                // from `TMPDIR`, which an operator sets — assuming it has no
                // space in it was wrong, and the failure would have been a
                // baffling ssh error rather than an honest one.
                let user_opt =
                    user.as_deref().map(|u| format!(" -o User={u}")).unwrap_or_default();
                creds.env.push((
                    "GIT_SSH_COMMAND".into(),
                    format!(
                        "ssh -i {}{user_opt} -o IdentitiesOnly=yes -o UserKnownHostsFile={} -o StrictHostKeyChecking={strict} -o BatchMode=yes",
                        shell_quote(&key_file)?,
                        shell_quote(&kh_file)?,
                    ),
                ));
                Ok(creds)
            }
        }
    }

    /// Scrub every secret this clone used out of a message before it is stored
    /// on the repo row or logged.
    fn redact(&self, s: &str) -> String {
        let mut out = s.to_string();
        for secret in &self.secrets {
            if !secret.is_empty() {
                out = out.replace(secret, "***");
            }
        }
        out
    }
}

/// The credential this clone will use.
///
/// A repo's own credential is used wherever it points: an operator who attached
/// a token to *this* repository chose the host it goes to. The worker-wide
/// `DAGRON_GIT_TOKEN` gets no such consent, so it stays restricted to trusted
/// forge hosts over https — otherwise registering
/// `https://attacker.example/repo.git` would be enough to have the worker post
/// the shared token to it.
fn effective_auth(url: &str, auth: Option<&GitAuth>) -> Option<GitAuth> {
    if let Some(a) = auth {
        return Some(a.clone());
    }
    let token = std::env::var("DAGRON_GIT_TOKEN")
        .or_else(|_| std::env::var("GITHUB_TOKEN"))
        .ok()
        .filter(|t| !t.is_empty())?;
    if url.starts_with("https://") && is_trusted_git_host(url) {
        Some(GitAuth::Token { username: DEFAULT_TOKEN_USER.to_string(), token })
    } else {
        None
    }
}

/// Whether an SSH sync must refuse to run without pinned host keys.
fn ssh_strict_required() -> bool {
    std::env::var("DAGRON_GIT_SSH_STRICT")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// `(scheme, host[:port])` for a credential-store entry, or `None` when the URL
/// is not one a token applies to.
///
/// **https only.** `http://` is deliberately excluded: dagron-api refuses to
/// store a token against a plaintext URL, and this is the second half of that —
/// the worker will not put a credential on an unencrypted connection even if a
/// row somehow carries one.
fn credential_origin(url: &str) -> Option<(String, String)> {
    let (scheme, rest) = url.split_once("://")?;
    if scheme != "https" {
        return None;
    }
    let authority = rest.split('/').next().unwrap_or_default();
    // Any userinfo is dropped: the credential we are about to write is the one
    // that should apply, and dagron-api rejects userinfo on https URLs anyway.
    let host = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
    if host.is_empty() {
        return None;
    }
    Some((scheme.to_string(), host.to_string()))
}

/// Transports this worker will hand to `git clone`. Deliberately a short
/// allowlist rather than a mirror of dagron-api's classifier: this check exists
/// to stop `git` being handed a *command* (`ext::`, and anything else it may
/// grow), not to re-litigate which forges are acceptable — dagron-api owns that
/// policy and rejects far more than this does.
const CLONABLE_SCHEMES: [&str; 5] = ["https://", "ssh://", "http://", "git://", "file://"];

/// Refuse a stored URL `git` must not be pointed at.
fn check_clonable(url: &str) -> Result<(), String> {
    if url.starts_with('-') || url.contains(char::is_whitespace) {
        return Err(format!("refusing to clone '{url}': not a usable repository URL"));
    }
    if CLONABLE_SCHEMES.iter().any(|s| url.starts_with(s)) {
        return Ok(());
    }
    // scp-style `[user@]host:path`. A second colon is what separates it from
    // `ext::sh -c …`, so it is the thing to reject.
    if !url.contains("://") {
        if let Some((authority, path)) = url.split_once(':') {
            let host = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
            let host_ok = !host.is_empty()
                && host.chars().all(|c| c.is_ascii_alphanumeric() || ".-".contains(c));
            let path_ok = !path.is_empty() && !path.contains(':') && !path.starts_with('/');
            if host_ok && path_ok {
                return Ok(());
            }
        }
    }
    Err(format!(
        "refusing to clone '{url}': only https/ssh (and, where enabled, http/git/file) URLs are supported"
    ))
}

/// Whether git will use the SSH transport for this URL — `ssh://…` or the
/// scp-style `[user@]host:path`. Only meaningful after [`check_clonable`], which
/// is what rules out the other colon-bearing forms.
fn is_ssh_url(url: &str) -> bool {
    url.starts_with("ssh://") || (!url.contains("://") && url.contains(':'))
}

/// Single-quote a scratch path for the two places git re-parses a string it was
/// given: `GIT_SSH_COMMAND` and a `credential.helper` value with arguments.
///
/// A path containing a single quote is refused rather than escaped. These paths
/// are always `$TMPDIR/dagron-gitops-<uuid>/…`, so the only way to get one is a
/// `TMPDIR` with a quote in it, and failing with a clear message beats emitting
/// a command whose quoting depends on which shell git picked.
fn shell_quote(path: &FsPath) -> Result<String, String> {
    let s = path.display().to_string();
    if s.contains('\'') {
        return Err(format!(
            "scratch path {s} contains a single quote — set TMPDIR to a path without one"
        ));
    }
    Ok(format!("'{s}'"))
}

/// Percent-encode everything outside the URL unreserved set, so a token with
/// `@`, `:` or `/` in it survives the credential-store line intact.
fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Write a credential file only this process can read. The 0600 is not
/// decoration for the SSH key: `ssh` refuses to use a key file others can read
/// and fails the clone with "UNPROTECTED PRIVATE KEY FILE".
fn write_private(path: &FsPath, contents: &str) -> Result<(), String> {
    use std::io::Write as _;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    let mut f = opts
        .open(path)
        .map_err(|e| format!("writing credential file: {e}"))?;
    f.write_all(contents.as_bytes()).map_err(|e| format!("writing credential file: {e}"))?;
    Ok(())
}

/// Hosts the *worker-wide* token may be sent to. Per-repo credentials are not
/// filtered by this — see [`effective_auth`] for why the two differ.
///
/// `bitbucket.org` matches exactly while GitHub and GitLab also match their
/// subdomains — intentional, since those two hand out per-tenant hostnames and
/// Bitbucket does not.
///
/// `DAGRON_GIT_TRUSTED_HOSTS` (comma-separated) adds hosts and their subdomains,
/// which is how a self-managed GHE / GitLab / Gitea reaches the shared token.
/// `docs/CONFIG.md` has documented that variable since the feature shipped, but
/// nothing read it: on any host but the three below, the token was silently
/// dropped and the clone failed as "repository not found".
fn is_trusted_git_host(url: &str) -> bool {
    let rest = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => url,
    };
    let host = rest.split('/').next().unwrap_or("");
    let host = host.rsplit('@').next().unwrap_or(host);
    let host = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
    let builtin = matches!(host.as_str(), "github.com" | "gitlab.com" | "bitbucket.org")
        || host.ends_with(".github.com")
        || host.ends_with(".gitlab.com");
    builtin || configured_trusted_hosts().iter().any(|t| host == *t || host.ends_with(&format!(".{t}")))
}

/// Extra trusted hosts from `DAGRON_GIT_TRUSTED_HOSTS`, lowercased. An entry
/// with a scheme, port or path is ignored rather than half-matched — "trusted
/// host" has to mean a host.
fn configured_trusted_hosts() -> Vec<String> {
    std::env::var("DAGRON_GIT_TRUSTED_HOSTS")
        .unwrap_or_default()
        .split(',')
        .map(|h| h.trim().to_ascii_lowercase())
        .filter(|h| !h.is_empty() && h.chars().all(|c| c.is_ascii_alphanumeric() || ".-".contains(c)))
        .collect()
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

    /// Sets an environment variable for the length of a test and restores it on
    /// drop — including when an assertion panics part-way through.
    ///
    /// Environment is process-wide and Rust runs these tests in parallel
    /// threads, so a `remove_var` that a failed assertion skips would leak into
    /// whatever runs next: a `DAGRON_GIT_SSH_STRICT` left set turns an unrelated
    /// test red and sends the next reader after the wrong bug.
    struct EnvGuard(&'static str, Option<String>);

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            std::env::set_var(key, value);
            EnvGuard(key, previous)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.1 {
                Some(v) => std::env::set_var(self.0, v),
                None => std::env::remove_var(self.0),
            }
        }
    }

    #[test]
    fn worker_wide_token_only_goes_to_trusted_https_hosts() {
        let _token = EnvGuard::set("DAGRON_GIT_TOKEN", "secret-token");
        assert!(
            effective_auth("https://evil.example.com/x/y.git", None).is_none(),
            "shared token offered to an untrusted host"
        );
        assert!(effective_auth("ssh://git@github.com/o/r.git", None).is_none());
        match effective_auth("https://github.com/o/r.git", None) {
            Some(GitAuth::Token { username, token }) => {
                assert_eq!(username, DEFAULT_TOKEN_USER);
                assert_eq!(token, "secret-token");
            }
            other => panic!("expected the shared token, got {other:?}"),
        }
        // An internal forge reaches the shared token only once it is named in
        // DAGRON_GIT_TRUSTED_HOSTS — the variable docs/CONFIG.md documents.
        let hosts = EnvGuard::set("DAGRON_GIT_TRUSTED_HOSTS", "git.acme.internal, evil.example");
        assert!(effective_auth("https://git.acme.internal/x/y.git", None).is_some());
        assert!(effective_auth("https://ci.git.acme.internal/x/y.git", None).is_some());
        // A host that merely *ends with* a trusted name is a different host.
        assert!(effective_auth("https://notgit.acme.internal/x/y.git", None).is_none());
        drop(hosts);
        assert!(effective_auth("https://git.acme.internal/x/y.git", None).is_none());
        // Not asserting "no token → no credential" here: `GITHUB_TOKEN` is the
        // documented fallback and is set in plenty of environments this test
        // runs in, including CI.
    }

    /// A repo's own credential is used wherever that repo points — the operator
    /// bound it to this URL — while the shared token is not (test above).
    #[test]
    fn per_repo_credential_is_used_for_any_host() {
        let auth = GitAuth::Token { username: "u".into(), token: "t".into() };
        assert!(matches!(
            effective_auth("https://git.internal.example/o/r.git", Some(&auth)),
            Some(GitAuth::Token { .. })
        ));
    }

    /// The credential must not reach argv — that is the whole reason for the
    /// file-based handoff — and must survive tokens containing URL syntax.
    #[test]
    fn token_is_written_to_a_private_file_not_the_command_line() {
        let work = std::env::temp_dir().join(format!("dagron-gitops-test-{}", Uuid::new_v4()));
        let _guard = TempDir(work.clone());
        std::fs::create_dir_all(&work).unwrap();
        let auth = GitAuth::Token { username: "x-access-token".into(), token: "p@ss/w0rd".into() };
        let creds =
            GitCreds::prepare(&work, "https://git.example.com:8443/o/r.git", Some(&auth)).unwrap();
        assert!(
            creds.config.iter().all(|c| !c.contains("p@ss/w0rd")),
            "token leaked into a -c argument: {:?}",
            creds.config
        );
        let stored = std::fs::read_to_string(work.join("git-credentials")).unwrap();
        assert_eq!(stored, "https://x-access-token:p%40ss%2Fw0rd@git.example.com:8443\n");
        assert_eq!(creds.redact("fatal: p@ss/w0rd bad"), "fatal: *** bad");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(work.join("git-credentials")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    /// The key goes to a 0600 file (ssh refuses to read anything looser) and the
    /// host-key policy follows whether known_hosts were supplied.
    #[test]
    fn ssh_key_is_written_0600_and_pins_host_keys_when_given() {
        let work = std::env::temp_dir().join(format!("dagron-gitops-test-{}", Uuid::new_v4()));
        let _guard = TempDir(work.clone());
        std::fs::create_dir_all(&work).unwrap();

        let auth = GitAuth::Ssh { key: "KEY\n".into(), user: None, known_hosts: None };
        let creds = GitCreds::prepare(&work, "git@github.com:o/r.git", Some(&auth)).unwrap();
        let ssh_cmd = creds.env.iter().find(|(k, _)| k == "GIT_SSH_COMMAND").unwrap().1.clone();
        assert!(ssh_cmd.contains("StrictHostKeyChecking=accept-new"), "{ssh_cmd}");
        assert!(ssh_cmd.contains("IdentitiesOnly=yes"), "{ssh_cmd}");
        assert!(ssh_cmd.contains("BatchMode=yes"), "{ssh_cmd}");
        assert!(!ssh_cmd.contains("KEY"), "private key leaked into the ssh command");
        // No stored user → ssh uses the URL's, so no -o User at all.
        assert!(!ssh_cmd.contains("-o User="), "{ssh_cmd}");
        // Paths are quoted: TMPDIR is the operator's to set, and git splits this
        // string on whitespace.
        assert!(ssh_cmd.contains(&format!("-i '{}'", work.join("id").display())), "{ssh_cmd}");
        assert_eq!(std::fs::read_to_string(work.join("id")).unwrap(), "KEY\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(work.join("id")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "ssh refuses a key file others can read");
        }

        let auth = GitAuth::Ssh {
            key: "KEY\n".into(),
            user: Some("deploy-bot".into()),
            known_hosts: Some("github.com ssh-ed25519 AAA".into()),
        };
        let creds = GitCreds::prepare(&work, "git@github.com:o/r.git", Some(&auth)).unwrap();
        let ssh_cmd = creds.env.iter().find(|(k, _)| k == "GIT_SSH_COMMAND").unwrap().1.clone();
        assert!(ssh_cmd.contains("StrictHostKeyChecking=yes"), "{ssh_cmd}");
        // The stored account actually reaches ssh — the console reporting a
        // deploy user while the clone connected as someone else was the bug.
        assert!(ssh_cmd.contains("-o User=deploy-bot"), "{ssh_cmd}");
        assert_eq!(
            std::fs::read_to_string(work.join("known_hosts")).unwrap(),
            "github.com ssh-ed25519 AAA"
        );

        // Opting into strict checking must fail the sync rather than quietly
        // downgrade to trust-on-first-use. Kept in this test because
        // `DAGRON_GIT_SSH_STRICT` is process-wide: EnvGuard restores it even on
        // a panic, but a separate test would still race the cases above.
        let _strict = EnvGuard::set("DAGRON_GIT_SSH_STRICT", "1");
        let auth = GitAuth::Ssh { key: "KEY\n".into(), user: None, known_hosts: None };
        // `unwrap_err` would require Debug on GitCreds, and GitCreds holds the
        // secrets — it deliberately has none.
        let err = match GitCreds::prepare(&work, "git@github.com:o/r.git", Some(&auth)) {
            Err(e) => e,
            Ok(_) => panic!("strict mode accepted an SSH sync with no known_hosts"),
        };
        assert!(err.contains("known_hosts"), "{err}");
    }

    /// A key against an https URL is the mirror of a token against ssh:// —
    /// dagron-api refuses to pair either, and both must fail here with the
    /// reason rather than let git ignore the credential and report the
    /// repository as missing.
    #[test]
    fn refuses_a_credential_the_transport_cannot_use() {
        let work = std::env::temp_dir().join(format!("dagron-gitops-test-{}", Uuid::new_v4()));
        let _guard = TempDir(work.clone());
        std::fs::create_dir_all(&work).unwrap();

        let ssh = GitAuth::Ssh { key: "KEY\n".into(), user: None, known_hosts: None };
        let err = match GitCreds::prepare(&work, "https://github.com/o/r.git", Some(&ssh)) {
            Err(e) => e,
            Ok(_) => panic!("an SSH key was accepted for an https URL"),
        };
        assert!(err.contains("not an SSH URL"), "{err}");

        let token = GitAuth::Token { username: "u".into(), token: "t".into() };
        let err = match GitCreds::prepare(&work, "ssh://git@github.com/o/r.git", Some(&token)) {
            Err(e) => e,
            Ok(_) => panic!("a token was accepted for an ssh URL"),
        };
        assert!(err.contains("not http(s)"), "{err}");

        assert!(is_ssh_url("ssh://git@host/o/r.git"));
        assert!(is_ssh_url("git@github.com:o/r.git"));
        assert!(!is_ssh_url("https://github.com/o/r.git"));
    }

    #[test]
    fn credential_origin_keeps_the_port_and_drops_userinfo() {
        assert_eq!(
            credential_origin("https://u@host:8443/o/r.git"),
            Some(("https".into(), "host:8443".into()))
        );
        assert_eq!(credential_origin("ssh://git@host/o/r.git"), None);
        // A token is never put on a plaintext connection, whatever
        // DAGRON_GIT_ALLOW_INSECURE says — that flag is consent to fetch over
        // http, not to send a credential in the clear.
        assert_eq!(credential_origin("http://host/o/r.git"), None);
    }

    /// The worker clones whatever the row holds, so the URL is re-checked here.
    /// `ext::` is the one that matters: git reads it as "run this command".
    #[test]
    fn refuses_a_stored_url_git_must_not_be_handed() {
        assert!(check_clonable("https://github.com/o/r.git").is_ok());
        assert!(check_clonable("ssh://git@host/o/r.git").is_ok());
        assert!(check_clonable("git@github.com:o/r.git").is_ok());
        assert!(check_clonable("file:///srv/git/r").is_ok());
        assert!(check_clonable("ext::sh -c evil").is_err());
        assert!(check_clonable("ext::whoami").is_err());
        assert!(check_clonable("--upload-pack=evil").is_err());
        assert!(check_clonable("hg::https://host/repo").is_err());
    }
}
