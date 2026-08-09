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
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

type ApiError = (StatusCode, String);

/// Default in-repo directory scanned for workflow YAML when none is given.
const DEFAULT_PATH: &str = "dagron";

/// Username sent with an HTTPS token when the operator does not name one. Both
/// GitHub and GitLab accept any non-empty username beside a PAT; this is the one
/// GitHub documents for its app/installation tokens.
const DEFAULT_TOKEN_USER: &str = "x-access-token";

/// Username for SSH when neither the URL nor the operator supplies one — the
/// account every forge exposes Git over SSH as.
const DEFAULT_SSH_USER: &str = "git";

/// Columns returned to the console, spelled out rather than `SELECT *`.
///
/// `auth_secret` holds the encrypted deploy token / private key, and a `SELECT *`
/// into a `Serialize` row would have shipped it to the browser the moment the
/// column was added. The credential is write-only by construction: the only
/// things that leave here are its *kind*, its username, and a non-secret hint.
const COLUMNS: &str = "id, name, url, branch, path, rev, state, auto_sync, workflow_count, drift,
     last_message, last_synced_at, created_at,
     auth_kind, auth_username, auth_hint, auth_known_hosts, auth_updated_at";

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
    /// `none` | `token` | `ssh` — how the worker authenticates to this repo.
    pub auth_kind: String,
    /// HTTPS username beside the token, or the SSH user. Not a secret.
    pub auth_username: Option<String>,
    /// Non-secret identifier for the stored credential: `••••cdef` for a token,
    /// `ssh-ed25519 SHA256:…` for a key. Lets an operator confirm *which*
    /// credential is installed without the credential being readable back.
    pub auth_hint: Option<String>,
    /// SSH `known_hosts` entries used to verify the forge's host key. Public
    /// data (it is the server's *public* key), so it round-trips to the UI.
    pub auth_known_hosts: Option<String>,
    pub auth_updated_at: Option<String>,
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
    // Per-repository credentials. Before these, the only way to reach a private
    // repo was the process-wide `DAGRON_GIT_TOKEN` on the worker: one token for
    // every repo, editable only by whoever can redeploy the container, and no
    // SSH at all — so a deploy-key-only forge, or two repos on two accounts,
    // simply could not be connected. The credential is stored encrypted
    // (dagron-crypto, the same `v1:`/`v2:` envelope as environment secrets) and
    // is never read back out of the API; `auth_hint` exists so the console can
    // still say which credential is installed.
    for ddl in [
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS auth_kind TEXT NOT NULL DEFAULT 'none'",
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS auth_username TEXT",
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS auth_secret TEXT",
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS auth_hint TEXT",
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS auth_known_hosts TEXT",
        "ALTER TABLE git_repos ADD COLUMN IF NOT EXISTS auth_updated_at TEXT",
    ] {
        sqlx::query(ddl).execute(pool).await?;
    }
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
    /// Whether a credential can be stored at all — i.e. whether encryption is
    /// configured. Rides along for the same reason `worker_online` does: without
    /// it the console would offer a credential form that can only ever 503.
    pub credentials_configured: bool,
}

pub async fn list_repos(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<RepoList>, ApiError> {
    let repos = sqlx::query_as::<_, GitRepo>(&format!(
        "SELECT {COLUMNS} FROM git_repos ORDER BY created_at DESC"
    ))
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let worker_online = worker_online(&state.read_pool).await;
    Ok(Json(RepoList {
        repos,
        worker_online,
        credentials_configured: credentials_configured(),
    }))
}

/// Credentials can be stored when either the legacy single key or an envelope
/// KEK provider is configured — the same two tiers environment secrets use.
fn credentials_configured() -> bool {
    dagron_crypto::key_configured() || matches!(dagron_crypto::provider_from_env(), Ok(Some(_)))
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
    /// Optional credential to store with the repo. Omitted (or `kind: "none"`)
    /// keeps the previous behaviour: public repos, or the worker's process-wide
    /// `DAGRON_GIT_TOKEN` for trusted forge hosts.
    #[serde(default)]
    pub auth: Option<AuthBody>,
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
    // Reject a URL scheme we won't clone, before it ever reaches `git`. The
    // transport it resolves to also decides which credential kinds are legal.
    let transport = classify_git_url(&url)?;
    let auth = prepare_auth(body.auth.as_ref(), &url, transport)?;
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

    let row = sqlx::query_as::<_, GitRepo>(&format!(
        "INSERT INTO git_repos (id, name, url, branch, path, state, auto_sync, created_at,
                                auth_kind, auth_username, auth_secret, auth_hint,
                                auth_known_hosts, auth_updated_at)
         VALUES ($1,$2,$3,$4,$5,'OutOfSync',$6,$7,$8,$9,$10,$11,$12,$13)
         ON CONFLICT (url) DO NOTHING
         RETURNING {COLUMNS}"
    ))
    .bind(&id)
    .bind(&name)
    .bind(&url)
    .bind(&branch)
    .bind(&path)
    .bind(if body.auto_sync { 1_i64 } else { 0 })
    .bind(&now)
    .bind(auth.kind)
    .bind(&auth.username)
    .bind(&auth.secret)
    .bind(&auth.hint)
    .bind(&auth.known_hosts)
    .bind(auth.secret.as_ref().map(|_| now.clone()))
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
    let row = sqlx::query_as::<_, GitRepo>(&format!(
        "UPDATE git_repos
         SET sync_requested_at = $1, last_message = 'sync requested'
         WHERE id = $2 RETURNING {COLUMNS}"
    ))
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

// ── Credentials ──────────────────────────────────────────────────────────────
//
// Two kinds, because the two things forges actually hand out are a token and a
// key, and a deployment usually cannot choose: GitHub App installations and
// GitLab project access tokens are HTTPS-only, while a locked-down enterprise
// forge or a Gerrit/`git@` host is frequently SSH-only. Supporting one of them
// is supporting half the deployments.
//
// Both are stored the same way — encrypted, write-only, per repository — so the
// console, the API and the worker each have one shape to understand rather than
// two parallel credential paths.

/// A credential as the console sends it. `kind` decides which of the value
/// fields is read; the rest are ignored rather than rejected, so a form that
/// keeps its SSH box populated while switching to a token still validates.
#[derive(Default, Deserialize)]
pub struct AuthBody {
    /// `none` | `token` | `ssh`. Absent means `none`.
    #[serde(default)]
    pub kind: Option<String>,
    /// HTTPS username beside the token, or the SSH user. Optional — both have a
    /// sane default and the SSH one is usually already in the URL.
    #[serde(default)]
    pub username: Option<String>,
    /// The HTTPS token / password. Write-only.
    #[serde(default)]
    pub token: Option<String>,
    /// PEM-encoded SSH private key (no passphrase). Write-only.
    #[serde(default)]
    pub ssh_private_key: Option<String>,
    /// `known_hosts` entries for the forge. Optional but strongly encouraged:
    /// without them the host key cannot be verified — see [`prepare_auth`].
    #[serde(default)]
    pub known_hosts: Option<String>,
}

impl std::fmt::Debug for AuthBody {
    /// Hand-written, like `sync::GitAuth` in the worker: a derived `Debug` prints
    /// the token and the private key in full, and it only takes one
    /// `tracing::debug!(?body)` or one `.expect()` on a rejected extraction for
    /// a deploy key to land in the logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthBody")
            .field("kind", &self.kind)
            .field("username", &self.username)
            .field("token", &self.token.is_some())
            .field("ssh_private_key", &self.ssh_private_key.is_some())
            .field("known_hosts", &self.known_hosts.is_some())
            .finish()
    }
}

/// A credential resolved into the columns it is stored in. `secret` is already
/// ciphertext; nothing downstream of here handles the cleartext.
struct StoredAuth {
    kind: &'static str,
    username: Option<String>,
    secret: Option<String>,
    hint: Option<String>,
    known_hosts: Option<String>,
}

impl StoredAuth {
    /// The "no credential" form — also what a DELETE writes.
    fn none() -> Self {
        StoredAuth { kind: "none", username: None, secret: None, hint: None, known_hosts: None }
    }
}

/// Validate a submitted credential against the repo's URL and encrypt it.
///
/// The transport check is the substance here: a token in an `ssh://` URL is
/// never sent (git would ignore it and the sync would fail as "permission
/// denied", with the operator looking at a credential the console says is
/// installed), and a key against `https://` likewise. Refusing the combination
/// at write time is the difference between a typo and a silent misconfiguration.
fn prepare_auth(
    body: Option<&AuthBody>,
    url: &str,
    transport: Transport,
) -> Result<StoredAuth, ApiError> {
    let Some(body) = body else { return Ok(StoredAuth::none()) };
    let kind = body.kind.as_deref().unwrap_or("none").trim().to_ascii_lowercase();
    match kind.as_str() {
        "" | "none" => Ok(StoredAuth::none()),
        "token" => {
            let token = trimmed(body.token.as_deref())
                .ok_or((StatusCode::BAD_REQUEST, "token is required for kind 'token'".into()))?;
            if transport != Transport::Https {
                // Including `git://` / `file://`, where there is no credential
                // exchange at all for a token to take part in.
                return Err((
                    StatusCode::BAD_REQUEST,
                    "a token applies only to an https:// repository — use an SSH key for an ssh:// one".into(),
                ));
            }
            // `DAGRON_GIT_ALLOW_INSECURE` folds `http://` into the HTTPS
            // transport, and that flag is consent to fetch over plaintext — not
            // consent to put a deploy token on the wire in the clear. Checked
            // against the scheme itself so the opt-in cannot widen it.
            if !url.starts_with("https://") {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "refusing to store a token for a plaintext http:// repository — it would be sent unencrypted; use https://".into(),
                ));
            }
            let username = trimmed(body.username.as_deref())
                .unwrap_or_else(|| DEFAULT_TOKEN_USER.to_string());
            if username.contains(':') {
                // The username is written into a git credential entry as
                // `user:token`, so a colon in it would move the split.
                return Err((StatusCode::BAD_REQUEST, "username must not contain ':'".into()));
            }
            let hint = token_hint(&token);
            Ok(StoredAuth {
                kind: "token",
                username: Some(username),
                secret: Some(encrypt_secret(&token)?),
                hint: Some(hint),
                known_hosts: None,
            })
        }
        "ssh" => {
            let key = trimmed(body.ssh_private_key.as_deref()).ok_or((
                StatusCode::BAD_REQUEST,
                "ssh_private_key is required for kind 'ssh'".into(),
            ))?;
            if transport != Transport::Ssh {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "this repository uses an HTTPS URL — use a token, or re-connect it with an ssh:// (or git@host:owner/repo) URL".into(),
                ));
            }
            let hint = inspect_ssh_key(&key)?;
            // The key is written to a file for `ssh -i`, which requires the
            // trailing newline OpenSSH's own tooling emits; a pasted key
            // routinely loses it and ssh then reports "invalid format" — a
            // message that sends people looking at the wrong thing entirely.
            let key = format!("{}\n", key.trim_end());
            let username = trimmed(body.username.as_deref())
                .or_else(|| ssh_user_from_url(url))
                .unwrap_or_else(|| DEFAULT_SSH_USER.to_string());
            // The worker passes this to `ssh -o User=…` inside `GIT_SSH_COMMAND`,
            // which git splits on whitespace and hands to a shell. Constrain it
            // to what an account name actually is, here, rather than trusting
            // the quoting downstream.
            if !username.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "ssh username may contain only letters, digits, '.', '_' and '-'".into(),
                ));
            }
            Ok(StoredAuth {
                kind: "ssh",
                username: Some(username),
                secret: Some(encrypt_secret(&key)?),
                hint: Some(hint),
                known_hosts: trimmed(body.known_hosts.as_deref()),
            })
        }
        other => Err((
            StatusCode::BAD_REQUEST,
            format!("unknown credential kind '{other}' (expected none, token or ssh)"),
        )),
    }
}

fn trimmed(s: Option<&str>) -> Option<String> {
    s.map(str::trim).filter(|v| !v.is_empty()).map(str::to_string)
}

/// Encrypt a credential for storage, mirroring environment secrets exactly:
/// envelope (`v2:`) when a KEK provider is configured, legacy single key (`v1:`)
/// otherwise. 503 when neither is — storing a deploy key in plaintext because
/// the operator forgot a variable is not an acceptable fallback.
fn encrypt_secret(value: &str) -> Result<String, ApiError> {
    let provider = dagron_crypto::provider_from_env()
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, format!("KEK provider misconfigured: {e}")))?;
    match &provider {
        Some(p) => dagron_crypto::encrypt_envelope(p.as_ref(), value).map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("envelope encrypt failed: {e}"))
        }),
        None => {
            let key = dagron_crypto::load_key().map_err(|e| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!(
                        "cannot store a Git credential: secret encryption is not configured ({e}). \
                         Set {} on dagron-api and the dagron-gitops worker.",
                        dagron_crypto::KEY_ENV
                    ),
                )
            })?;
            dagron_crypto::encrypt(&key, value)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))
        }
    }
}

/// Last four characters of a token, the convention every forge uses for showing
/// which credential is installed without showing the credential.
fn token_hint(token: &str) -> String {
    let chars: Vec<char> = token.chars().collect();
    let tail: String = chars[chars.len().saturating_sub(4)..].iter().collect();
    format!("••••{tail}")
}

/// Validate an SSH private key and describe it for the console.
///
/// Two failures are worth catching before the key is ever stored, because both
/// surface later as an unrelated-looking clone error:
///
/// * **not a private key** — a public key pasted into the private key box is the
///   single most common mistake, and `ssh` reports it only as a refused auth;
/// * **passphrase-protected** — `ssh` cannot prompt in a worker, so the sync
///   hangs to the batch-mode timeout and then fails on "permission denied".
///
/// For OpenSSH-format keys the returned hint is the real `SHA256:…` fingerprint
/// (the one `ssh-keygen -lf` prints), computed from the public key embedded in
/// the private key file, so it can be compared against the deploy key listed on
/// the forge. Legacy PEM keys carry no such blob, so those fall back to a
/// digest of the key material, labelled as one.
fn inspect_ssh_key(key: &str) -> Result<String, ApiError> {
    let bad = |msg: &str| (StatusCode::BAD_REQUEST, msg.to_string());
    let key = key.trim();
    if key.starts_with("ssh-") || key.starts_with("ecdsa-") {
        return Err(bad(
            "that looks like a public key — paste the private key (the file without the .pub suffix)",
        ));
    }
    if !key.starts_with("-----BEGIN ") || !key.contains("PRIVATE KEY-----") {
        return Err(bad("expected a PEM private key beginning with '-----BEGIN ... PRIVATE KEY-----'"));
    }
    // Legacy PEM encryption announces itself in headers.
    if key.contains("Proc-Type: 4,ENCRYPTED") || key.contains("DEK-Info:") {
        return Err(bad(
            "passphrase-protected keys are not supported — the worker cannot be prompted; supply a key with no passphrase",
        ));
    }
    if key.contains("OPENSSH PRIVATE KEY") {
        let body: String = key
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .flat_map(|l| l.trim().chars())
            .collect();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(body.as_bytes())
            .map_err(|_| bad("the OPENSSH key body is not valid base64 — was the paste truncated?"))?;
        let parsed = parse_openssh_key(&raw)
            .ok_or_else(|| bad("could not read the OPENSSH private key — was the paste truncated?"))?;
        if parsed.cipher != "none" {
            return Err(bad(
                "passphrase-protected keys are not supported — the worker cannot be prompted; supply a key with no passphrase (ssh-keygen -p -N '' -f key)",
            ));
        }
        let digest = Sha256::digest(&parsed.public_blob);
        let fp = base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest);
        return Ok(format!("{} SHA256:{fp}", parsed.key_type));
    }
    // PEM RSA/EC key: no embedded public blob to fingerprint, so identify it by
    // a digest of the key itself. Named "key digest" and not "SHA256:" so it is
    // never mistaken for an OpenSSH fingerprint it would not match.
    let kind = if key.contains("RSA PRIVATE KEY") {
        "rsa"
    } else if key.contains("EC PRIVATE KEY") {
        "ecdsa"
    } else {
        "pem"
    };
    let digest = Sha256::digest(key.as_bytes());
    Ok(format!("{kind} key digest {:x}{:x}{:x}{:x}", digest[0], digest[1], digest[2], digest[3]))
}

/// The fields of an `openssh-key-v1` private key file this code cares about.
struct OpenSshKey {
    cipher: String,
    key_type: String,
    /// The embedded public key blob — what OpenSSH fingerprints.
    public_blob: Vec<u8>,
}

/// Parse the OpenSSH private key container:
/// `"openssh-key-v1\0" ‖ string cipher ‖ string kdf ‖ string kdfopts ‖ u32 n ‖ string pubkey…`,
/// where the public key blob itself opens with its type string. Only the header
/// is read — never the encrypted/private half.
fn parse_openssh_key(raw: &[u8]) -> Option<OpenSshKey> {
    const MAGIC: &[u8] = b"openssh-key-v1\0";
    let rest = raw.strip_prefix(MAGIC)?;
    let mut r = SshReader { buf: rest, pos: 0 };
    let cipher = String::from_utf8(r.string()?.to_vec()).ok()?;
    let _kdf = r.string()?;
    let _kdfopts = r.string()?;
    let n = r.u32()?;
    if n == 0 {
        return None;
    }
    let public_blob = r.string()?.to_vec();
    let key_type = {
        let mut pr = SshReader { buf: &public_blob, pos: 0 };
        String::from_utf8(pr.string()?.to_vec()).ok()?
    };
    Some(OpenSshKey { cipher, key_type, public_blob })
}

/// Reader for SSH wire encoding: big-endian `u32` length prefixes.
struct SshReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> SshReader<'a> {
    fn u32(&mut self) -> Option<u32> {
        let end = self.pos.checked_add(4)?;
        let bytes: [u8; 4] = self.buf.get(self.pos..end)?.try_into().ok()?;
        self.pos = end;
        Some(u32::from_be_bytes(bytes))
    }

    fn string(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        let end = self.pos.checked_add(len)?;
        let out = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }
}

/// The SSH user already written into the URL (`ssh://git@host/…` or
/// `git@host:…`), so an operator who typed it once is not asked for it twice.
fn ssh_user_from_url(url: &str) -> Option<String> {
    let rest = url.strip_prefix("ssh://").unwrap_or(url);
    let authority = rest.split(['/', ':']).next().unwrap_or(rest);
    authority.split_once('@').map(|(user, _)| user.to_string()).filter(|u| !u.is_empty())
}

/// `PUT /api/git-repos/:id/auth` — set or rotate this repo's credential.
///
/// Separate from connect on purpose: rotating a deploy token is a routine
/// operation, and making it require disconnect-and-reconnect would throw away
/// the repo's sync history and its `auto_sync` setting to change one secret.
pub async fn put_auth(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<AuthBody>,
) -> Result<Json<GitRepo>, ApiError> {
    let url: String = sqlx::query_scalar("SELECT url FROM git_repos WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal)?
        .ok_or((StatusCode::NOT_FOUND, "repository not found".to_string()))?;
    let transport = classify_git_url(&url)?;
    let auth = prepare_auth(Some(&body), &url, transport)?;
    let updated = write_auth(&state, &id, &auth).await?;
    tracing::info!(repo = %id, kind = auth.kind, "git credential updated");
    Ok(Json(updated))
}

/// `DELETE /api/git-repos/:id/auth` — remove the stored credential. The repo
/// keeps syncing if it is public or the worker's global token reaches it.
pub async fn delete_auth(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<GitRepo>, ApiError> {
    let updated = write_auth(&state, &id, &StoredAuth::none()).await?;
    tracing::info!(repo = %id, "git credential removed");
    Ok(Json(updated))
}

async fn write_auth(state: &AppState, id: &str, auth: &StoredAuth) -> Result<GitRepo, ApiError> {
    let now = chrono::Utc::now().to_rfc3339();
    let row = sqlx::query_as::<_, GitRepo>(&format!(
        "UPDATE git_repos
         SET auth_kind = $1, auth_username = $2, auth_secret = $3, auth_hint = $4,
             auth_known_hosts = $5, auth_updated_at = $6
         WHERE id = $7 RETURNING {COLUMNS}"
    ))
    .bind(auth.kind)
    .bind(&auth.username)
    .bind(&auth.secret)
    .bind(&auth.hint)
    .bind(&auth.known_hosts)
    .bind(auth.secret.as_ref().map(|_| now))
    .bind(id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal)?;
    row.ok_or((StatusCode::NOT_FOUND, "repository not found".to_string()))
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

/// Which transport a registered URL resolves to. Decides both what `git` will be
/// asked to do and which credential kind may be attached to the repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// `https://` — and, behind the insecure opt-in, `http://`.
    Https,
    /// `ssh://[user@]host/path` or the scp-style `[user@]host:path`.
    Ssh,
    /// `git://` / `file://` — no credential applies to either.
    Local,
}

/// Only clone over transports we accept, and say which one it is.
///
/// `https`/`ssh` are always allowed; the plaintext/local transports (`http`,
/// `git`, `file`) require `DAGRON_GIT_ALLOW_INSECURE` (SSRF / internal-probing /
/// local-read hardening). Anything that would let `git` treat the URL as a flag
/// or a local-command transport (`ext::`) is rejected.
///
/// **Userinfo rules differ by transport, and the difference is the point.** An
/// HTTPS URL still may not carry any, because there the userinfo *is* the
/// credential and persisting it would write a token into `git_repos.url` in
/// cleartext. On SSH the user is not a secret — `git@github.com` is the account
/// name every forge documents, and the scp-style `git@host:owner/repo` is the
/// form they hand you — so a bare user is accepted and a `user:password@` pair
/// is not.
fn classify_git_url(url: &str) -> Result<Transport, ApiError> {
    let bad = |msg: &str| (StatusCode::BAD_REQUEST, msg.to_string());
    if url.starts_with('-') || url.contains(char::is_whitespace) {
        return Err(bad("url must not start with '-' or contain whitespace"));
    }
    if let Some((scheme, rest)) = url.split_once("://") {
        let authority = rest.split('/').next().unwrap_or_default();
        let userinfo = authority.rsplit_once('@').map(|(u, _)| u);
        let transport = match scheme {
            "https" => Transport::Https,
            "ssh" => Transport::Ssh,
            "http" if allow_insecure_git() => Transport::Https,
            "git" | "file" if allow_insecure_git() => Transport::Local,
            "http" | "git" | "file" => {
                return Err(bad(
                    "insecure clone scheme (http/git/file) is disabled; use https:// or ssh:// (or set DAGRON_GIT_ALLOW_INSECURE=1)",
                ))
            }
            _ => {
                return Err(bad(
                    "url must start with https:// or ssh:// (set DAGRON_GIT_ALLOW_INSECURE=1 to also allow http:// git:// file://)",
                ))
            }
        };
        match (transport, userinfo) {
            (_, None) => {}
            (Transport::Ssh, Some(u)) if !u.contains(':') && !u.is_empty() => {}
            (Transport::Ssh, Some(_)) => {
                return Err(bad(
                    "ssh url may name a user (ssh://git@host/…) but must not embed a password — attach an SSH key instead",
                ))
            }
            (_, Some(_)) => return Err(bad("url must not contain embedded credentials")),
        }
        return Ok(transport);
    }
    // scp-style `[user@]host:path`. Accepted because it is what forges print on
    // their clone buttons; constrained tightly because `git` reads other
    // colon-bearing strings (`ext::sh -c …`) as transports of their own.
    let (authority, path) = url.split_once(':').ok_or_else(|| {
        bad("url must start with https:// or ssh://, or be an scp-style git@host:owner/repo")
    })?;
    let host = authority.rsplit_once('@').map(|(_, h)| h).unwrap_or(authority);
    let user_ok = match authority.rsplit_once('@') {
        Some((u, _)) => !u.is_empty() && u.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)),
        None => true,
    };
    let host_ok = !host.is_empty()
        && host.chars().all(|c| c.is_ascii_alphanumeric() || ".-".contains(c))
        && host.contains(|c: char| c.is_ascii_alphanumeric());
    // A further colon is what tells `ext::sh -c …` apart from `host:owner/repo`:
    // real scp-style paths never contain one, and `ext::` is a transport that
    // runs a command of the attacker's choosing.
    let path_ok = !path.is_empty()
        && !path.contains(':')
        && !path.starts_with('/')
        && !path.starts_with('-')
        && !path.split('/').any(|s| s == "..");
    if user_ok && host_ok && path_ok {
        Ok(Transport::Ssh)
    } else {
        Err(bad("url must start with https:// or ssh://, or be an scp-style git@host:owner/repo"))
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
    fn url_scheme_validation() {
        assert_eq!(classify_git_url("https://github.com/o/r.git").unwrap(), Transport::Https);
        assert_eq!(classify_git_url("ssh://host/o/r.git").unwrap(), Transport::Ssh);
        // An SSH *user* is not a credential — it is the account name every forge
        // documents, and refusing it left SSH deploy keys unusable.
        assert_eq!(classify_git_url("ssh://git@host/o/r.git").unwrap(), Transport::Ssh);
        assert_eq!(classify_git_url("git@github.com:o/r.git").unwrap(), Transport::Ssh);
        assert_eq!(classify_git_url("github.com:o/r.git").unwrap(), Transport::Ssh);
        // An SSH *password* still is one.
        assert!(classify_git_url("ssh://u:p@host/o/r.git").is_err());
        // Insecure/local schemes are off by default, on with the opt-in flag.
        std::env::remove_var("DAGRON_GIT_ALLOW_INSECURE");
        assert!(classify_git_url("file:///tmp/r").is_err());
        std::env::set_var("DAGRON_GIT_ALLOW_INSECURE", "1");
        assert_eq!(classify_git_url("file:///tmp/r").unwrap(), Transport::Local);
        std::env::remove_var("DAGRON_GIT_ALLOW_INSECURE");
        assert!(classify_git_url("--upload-pack=evil").is_err());
        // `ext::` is a git transport that runs a command; scp-style parsing must
        // not be the hole it comes back through.
        assert!(classify_git_url("ext::sh -c evil").is_err());
        assert!(classify_git_url("ext::whoami").is_err());
        // Embedded credentials are rejected on https so a secret can't be persisted.
        assert!(classify_git_url("https://tok@github.com/o/r.git").is_err());
        assert!(classify_git_url("https://u:p@github.com/o/r.git").is_err());
    }

    /// A credential must match the transport that will actually be dialled.
    /// Storing the mismatch would leave the console showing an installed
    /// credential while every sync failed on "permission denied".
    #[test]
    fn credential_kind_must_match_transport() {
        let token = AuthBody { kind: Some("token".into()), token: Some("ghp_x".into()), ..Default::default() };
        let ssh = AuthBody {
            kind: Some("ssh".into()),
            ssh_private_key: Some("-----BEGIN OPENSSH PRIVATE KEY-----".into()),
            ..Default::default()
        };
        assert!(prepare_auth(Some(&token), "ssh://git@host/o/r.git", Transport::Ssh).is_err());
        assert!(prepare_auth(Some(&ssh), "https://github.com/o/r.git", Transport::Https).is_err());
        // No credential at all stays legal for every transport.
        assert_eq!(
            prepare_auth(None, "https://github.com/o/r.git", Transport::Https).unwrap().kind,
            "none"
        );
        // DAGRON_GIT_ALLOW_INSECURE folds http:// into the HTTPS transport. That
        // is consent to fetch over plaintext, not to put a deploy token on the
        // wire in the clear.
        assert!(prepare_auth(Some(&token), "http://host/o/r.git", Transport::Https).is_err());
    }

    /// The mismatch cases above pass on the transport check alone, so they would
    /// still pass if the credential itself stopped being validated. This is the
    /// positive half: a real key on a real SSH URL is accepted, stored
    /// encrypted, and hinted — and a username that is not an account name is
    /// refused before it can reach `GIT_SSH_COMMAND`.
    #[test]
    fn accepts_a_valid_ssh_key_on_an_ssh_url() {
        // Fabricated, not a real key: an openssh-key-v1 container whose public
        // blob is `string "ssh-ed25519"`, with cipher "none".
        fn s(bytes: &[u8]) -> Vec<u8> {
            let mut out = (bytes.len() as u32).to_be_bytes().to_vec();
            out.extend_from_slice(bytes);
            out
        }
        let mut raw = b"openssh-key-v1\0".to_vec();
        raw.extend(s(b"none"));
        raw.extend(s(b"none"));
        raw.extend(s(b""));
        raw.extend(1u32.to_be_bytes());
        raw.extend(s(&s(b"ssh-ed25519")));
        let key = format!(
            "-----BEGIN OPENSSH PRIVATE KEY-----\n{}\n-----END OPENSSH PRIVATE KEY-----",
            base64::engine::general_purpose::STANDARD.encode(&raw)
        );

        // prepare_auth encrypts, so storage has to be configured for this path.
        std::env::set_var(dagron_crypto::KEY_ENV, "test-key-for-gitrepos-credentials");
        let body = AuthBody {
            kind: Some("ssh".into()),
            ssh_private_key: Some(key.clone()),
            known_hosts: Some("github.com ssh-ed25519 AAA".into()),
            ..Default::default()
        };
        let stored = prepare_auth(Some(&body), "git@github.com:o/r.git", Transport::Ssh).unwrap();
        assert_eq!(stored.kind, "ssh");
        // The user came from the URL rather than being asked for twice.
        assert_eq!(stored.username.as_deref(), Some("git"));
        assert!(stored.hint.unwrap().starts_with("ssh-ed25519 SHA256:"));
        let ciphertext = stored.secret.unwrap();
        assert!(!ciphertext.contains("OPENSSH"), "the key was stored in the clear");
        assert_eq!(
            dagron_crypto::decrypt(&dagron_crypto::load_key().unwrap(), &ciphertext).unwrap().trim(),
            key
        );

        // A username that is not an account name is refused: it ends up in
        // `ssh -o User=…`, which git re-parses through a shell.
        let body = AuthBody {
            kind: Some("ssh".into()),
            ssh_private_key: Some(key),
            username: Some("evil -o ProxyCommand=sh".into()),
            ..Default::default()
        };
        assert!(prepare_auth(Some(&body), "git@github.com:o/r.git", Transport::Ssh).is_err());
        std::env::remove_var(dagron_crypto::KEY_ENV);
    }

    #[test]
    fn token_hint_shows_only_the_tail() {
        assert_eq!(token_hint("ghp_abcdefgh1234"), "••••1234");
        assert_eq!(token_hint("ab"), "••••ab");
    }

    /// The two paste mistakes that otherwise surface as an unrelated clone
    /// failure much later.
    #[test]
    fn ssh_key_input_is_checked_before_storage() {
        assert!(inspect_ssh_key("ssh-ed25519 AAAAC3Nz user@host").is_err());
        assert!(inspect_ssh_key("hunter2").is_err());
        assert!(inspect_ssh_key(
            "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,X\n\nZm9v\n-----END RSA PRIVATE KEY-----"
        )
        .is_err());
        let pem = inspect_ssh_key(
            "-----BEGIN RSA PRIVATE KEY-----\nZm9vYmFy\n-----END RSA PRIVATE KEY-----",
        )
        .unwrap();
        assert!(pem.starts_with("rsa key digest "), "{pem}");
    }

    /// The hint for an OpenSSH key is the same `SHA256:…` the forge shows next
    /// to the deploy key, so the two can actually be compared.
    #[test]
    fn openssh_key_hint_is_the_real_fingerprint() {
        // A minimal openssh-key-v1 container: cipher "none", one key, whose
        // public blob is `string "ssh-ed25519"`.
        fn s(bytes: &[u8]) -> Vec<u8> {
            let mut out = (bytes.len() as u32).to_be_bytes().to_vec();
            out.extend_from_slice(bytes);
            out
        }
        let pub_blob = s(b"ssh-ed25519");
        let mut raw = b"openssh-key-v1\0".to_vec();
        raw.extend(s(b"none")); // cipher
        raw.extend(s(b"none")); // kdf
        raw.extend(s(b"")); // kdfopts
        raw.extend(1u32.to_be_bytes()); // number of keys
        raw.extend(s(&pub_blob));
        let parsed = parse_openssh_key(&raw).unwrap();
        assert_eq!(parsed.cipher, "none");
        assert_eq!(parsed.key_type, "ssh-ed25519");
        assert_eq!(parsed.public_blob, pub_blob);

        let armored = format!(
            "-----BEGIN OPENSSH PRIVATE KEY-----\n{}\n-----END OPENSSH PRIVATE KEY-----",
            base64::engine::general_purpose::STANDARD.encode(&raw)
        );
        let hint = inspect_ssh_key(&armored).unwrap();
        let expect = base64::engine::general_purpose::STANDARD_NO_PAD.encode(Sha256::digest(&pub_blob));
        assert_eq!(hint, format!("ssh-ed25519 SHA256:{expect}"));

        // The same container with a cipher set is a passphrase-protected key.
        let mut enc = b"openssh-key-v1\0".to_vec();
        enc.extend(s(b"aes256-ctr"));
        enc.extend(s(b"bcrypt"));
        enc.extend(s(b""));
        enc.extend(1u32.to_be_bytes());
        enc.extend(s(&pub_blob));
        let armored = format!(
            "-----BEGIN OPENSSH PRIVATE KEY-----\n{}\n-----END OPENSSH PRIVATE KEY-----",
            base64::engine::general_purpose::STANDARD.encode(&enc)
        );
        assert!(inspect_ssh_key(&armored).is_err());
    }

    #[test]
    fn ssh_user_comes_from_the_url_when_not_given() {
        assert_eq!(ssh_user_from_url("ssh://git@host/o/r.git").as_deref(), Some("git"));
        assert_eq!(ssh_user_from_url("git@github.com:o/r.git").as_deref(), Some("git"));
        assert_eq!(ssh_user_from_url("ssh://host/o/r.git"), None);
    }

    #[test]
    fn repo_name_parses_forms() {
        assert_eq!(repo_name("https://github.com/acme/etl.git"), "acme/etl");
        assert_eq!(repo_name("git@github.com:acme/etl.git"), "acme/etl");
        assert_eq!(repo_name("file:///srv/git/etl"), "git/etl");
    }

    // Real clone + walk + validate against a local file:// repo — offline, no DB.
}
