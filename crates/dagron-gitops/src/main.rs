//! dagron GitOps worker — the process that actually talks to git.
//!
//! Split out of `dagron-api` deliberately. The gateway ships on distroless (no
//! shell, no git binary), so GitOps sync failed there on every attempt with
//! `running git: No such file or directory`; and GitOps is opt-in for a minority
//! of deployments, so neither a git binary in the internet-facing container nor a
//! pure-Rust git port belongs in everyone's build. This image is deployed only
//! when GitOps is wanted.
//!
//! Responsibilities:
//!
//! * **Heartbeat.** Writes `gitops_workers` every tick so the console can say
//!   "no GitOps worker running" instead of showing Auto-sync ON while nothing
//!   polls. The bug this split fixes was invisible for exactly that reason —
//!   moving the work without making its absence visible would reproduce it.
//! * **Auto-sync.** Repos with `auto_sync = 1` reconcile every
//!   `GITOPS_POLL_SECS` (default 60). Nothing polled them before: `auto_sync`
//!   was stored and advertised in the UI, but no loop existed anywhere.
//! * **Requested syncs.** The console's Sync button stamps `sync_requested_at`
//!   (dagron-api no longer runs git itself); this loop claims and clears it.
//! * **Signed bundles.** A repo path carrying `manifest.json` + `manifest.sig`
//!   is verified against `DAGRON_BUNDLE_PUBKEYS` and applied whole, in one
//!   transaction, with a version row per workflow naming the bundle
//!   (`sync::Fetched::Bundle` → `bundle::reconcile_bundle`;
//!   docs/BUNDLES.md). `DAGRON_BUNDLE_REQUIRE=1` refuses anything unsigned.
//!
//! Schema is owned by dagron-api, which creates `git_repos` and the tables here
//! on startup — one owner, no duplicate DDL to drift. This worker waits for them
//! rather than creating them.

mod bundle;
mod sync;

use std::time::Duration;

use anyhow::Context;
use tracing::{error, info, warn};

/// How often the loop wakes: heartbeat, then any due work.
const TICK: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dagron_logging::init("gitops");

    let db_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL is required (the datastore dagron-api uses)")?;
    let poll_secs: i64 = std::env::var("GITOPS_POLL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(60);
    let worker_id = format!("gitops-{}", uuid::Uuid::new_v4());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await
        .context("connecting to DATABASE_URL")?;

    info!(worker_id, poll_secs, "gitops worker starting");
    wait_for_schema(&pool).await;

    let mut shutdown = std::pin::pin!(tokio::signal::ctrl_c());
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal — releasing heartbeat");
                let _ = sqlx::query("DELETE FROM gitops_workers WHERE id = $1")
                    .bind(&worker_id)
                    .execute(&pool)
                    .await;
                return Ok(());
            }
            _ = tokio::time::sleep(TICK) => {}
        }

        if let Err(e) = heartbeat(&pool, &worker_id).await {
            // A datastore blip must not kill the worker; the stale heartbeat is
            // itself the signal the console shows.
            warn!(error = %e, "heartbeat failed");
            continue;
        }
        if let Err(e) = sweep(&pool, poll_secs).await {
            warn!(error = %e, "sweep failed");
        }
    }
}

/// Block until dagron-api has created the tables. Starting order is not
/// guaranteed in compose or k8s, and creating them here would mean two owners of
/// one schema — the drift that has bitten this codebase repeatedly.
///
/// `workflow_versions` is on the list because a signed bundle writes a
/// version row per workflow inside the same transaction as the upsert: were
/// the table still missing, the first bundle sync after a fresh deploy would
/// fail — and, being all-or-nothing, apply none of its workflows — for a
/// reason that has nothing to do with the bundle.
async fn wait_for_schema(pool: &sqlx::PgPool) {
    loop {
        let ready = sqlx::query_scalar::<_, i64>(
            // Scoped to the search path and counted DISTINCT: the same table name
            // in a second visible schema would otherwise push the count past four
            // and wedge this loop forever on a schema that is in fact ready.
            "SELECT count(DISTINCT table_name) FROM information_schema.tables
             WHERE table_schema = ANY (current_schemas(false))
               AND table_name IN ('git_repos', 'gitops_workers', 'workflows', 'workflow_versions')",
        )
        .fetch_one(pool)
        .await;
        match ready {
            Ok(n) if n >= 4 => return,
            Ok(_) => info!("waiting for dagron-api to create the GitOps schema"),
            Err(e) => warn!(error = %e, "schema probe failed"),
        }
        tokio::time::sleep(TICK).await;
    }
}

async fn heartbeat(pool: &sqlx::PgPool, worker_id: &str) -> anyhow::Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO gitops_workers (id, last_seen_at) VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at",
    )
    .bind(worker_id)
    .bind(&now)
    .execute(pool)
    .await?;
    // Forget peers that stopped heartbeating, so "is a worker running" stays a
    // question about the present.
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339();
    sqlx::query("DELETE FROM gitops_workers WHERE last_seen_at < $1")
        .bind(&cutoff)
        .execute(pool)
        .await?;
    Ok(())
}

/// One row of work: a repo that asked for a sync, or one whose auto-sync is due.
#[derive(sqlx::FromRow)]
struct Due {
    id: String,
    url: String,
    branch: String,
    path: String,
    requested: bool,
    /// Value read in the sweep — the CAS target, so a peer that syncs this repo
    /// between the SELECT and the claim wins and we skip.
    last_synced_at: Option<String>,
    /// `none` | `token` | `ssh` — see `dagron-api::routes::gitrepos`.
    auth_kind: String,
    auth_username: Option<String>,
    /// Ciphertext of the token / private key, decrypted here and nowhere else.
    auth_secret: Option<String>,
    auth_known_hosts: Option<String>,
}

/// Decrypt a repo's stored credential into the form the clone needs.
///
/// `Ok(None)` is "this repo has no credential" — the worker then falls back to
/// the process-wide token, as it always did. An `Err` is a *configured*
/// credential that could not be read (missing or mismatched
/// `DAGRON_ENV_SECRET_KEY`, a KEK provider that will not build), and it fails
/// the sync on purpose: cloning unauthenticated instead would report the repo as
/// "not found" and send the operator looking at the wrong thing.
fn decrypt_auth(repo: &Due) -> Result<Option<sync::GitAuth>, String> {
    let kind = repo.auth_kind.as_str();
    if kind == "none" || kind.is_empty() {
        return Ok(None);
    }
    let Some(ciphertext) = repo.auth_secret.as_deref() else {
        return Err(format!("credential kind '{kind}' is set but no secret is stored"));
    };
    let plain = decrypt(ciphertext)?;
    match kind {
        "token" => Ok(Some(sync::GitAuth::Token {
            username: repo
                .auth_username
                .clone()
                .filter(|u| !u.trim().is_empty())
                .unwrap_or_else(|| sync::DEFAULT_TOKEN_USER.to_string()),
            token: plain,
        })),
        "ssh" => Ok(Some(sync::GitAuth::Ssh {
            key: plain,
            // dagron-api stores an SSH user for every key credential and the
            // console displays it. Dropping it here meant ssh fell back to the
            // URL's user or the process user, so an operator could enter a
            // deploy account, see it reported as installed, and have the clone
            // authenticate as someone else entirely — the failure this feature
            // exists to prevent, reintroduced one layer down.
            user: ssh_user(repo.auth_username.as_deref()),
            known_hosts: repo
                .auth_known_hosts
                .clone()
                .map(|k| k.trim().to_string())
                .filter(|k| !k.is_empty()),
        })),
        other => Err(format!("unknown credential kind '{other}'")),
    }
}

/// The SSH account from a stored credential, dropped unless it looks like one.
///
/// dagron-api constrains this on write, but the value arrives from a datastore
/// row and lands inside `GIT_SSH_COMMAND`, which git re-parses through a shell.
/// A row that predates that check — or one written directly — must not be able
/// to smuggle an argument in; an unusable name is discarded so ssh falls back to
/// the URL's user, rather than failing the sync over it.
fn ssh_user(stored: Option<&str>) -> Option<String> {
    let user = stored?.trim();
    if user.is_empty() || !user.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)) {
        return None;
    }
    Some(user.to_string())
}

/// Dispatch on the stored ciphertext version, exactly as the engine does for
/// environment secrets: `v2:` needs the KEK provider, `v1:` the single key.
fn decrypt(ciphertext: &str) -> Result<String, String> {
    match dagron_crypto::version_of(ciphertext) {
        Some("v2") => {
            let provider = dagron_crypto::provider_from_env()
                .map_err(|e| format!("KEK provider ({}) failed to initialize: {e}", dagron_crypto::PROVIDER_ENV))?
                .ok_or_else(|| {
                    format!(
                        "the credential is envelope-encrypted (v2) but no KEK provider is configured (set {} on this worker)",
                        dagron_crypto::PROVIDER_ENV
                    )
                })?;
            dagron_crypto::decrypt_envelope(provider.as_ref(), ciphertext)
                .map_err(|e| format!("decrypting the Git credential: {e}"))
        }
        Some("v1") => {
            let key = dagron_crypto::load_key().map_err(|e| {
                format!(
                    "the repository has a stored credential but {} is not set on this worker ({e})",
                    dagron_crypto::KEY_ENV
                )
            })?;
            dagron_crypto::decrypt(&key, ciphertext).map_err(|_| {
                format!(
                    "decrypting the Git credential failed — does this worker's {} match dagron-api's?",
                    dagron_crypto::KEY_ENV
                )
            })
        }
        _ => Err("the stored Git credential has an unrecognized ciphertext version".to_string()),
    }
}

/// Claim and reconcile everything due. Claiming is a conditional UPDATE, so two
/// workers racing the same repo cannot both run it.
async fn sweep(pool: &sqlx::PgPool, poll_secs: i64) -> anyhow::Result<()> {
    let stale = (chrono::Utc::now() - chrono::Duration::seconds(poll_secs)).to_rfc3339();
    let due: Vec<Due> = sqlx::query_as(
        "SELECT id, url, branch, path, last_synced_at,
                auth_kind, auth_username, auth_secret, auth_known_hosts,
                (sync_requested_at IS NOT NULL) AS requested
         FROM git_repos
         WHERE sync_requested_at IS NOT NULL
            OR (auto_sync = 1 AND (last_synced_at IS NULL OR last_synced_at < $1))
         ORDER BY sync_requested_at NULLS LAST, last_synced_at NULLS FIRST",
    )
    .bind(&stale)
    .fetch_all(pool)
    .await?;

    for repo in due {
        // Claim: clearing the request (or stamping the sync time) under a
        // condition means the loser of a race skips instead of double-cloning.
        let claimed = sqlx::query(
            "UPDATE git_repos SET sync_requested_at = NULL, last_synced_at = $1
             WHERE id = $2 AND (sync_requested_at IS NOT NULL OR last_synced_at IS NOT DISTINCT FROM $3)",
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(&repo.id)
        .bind(&repo.last_synced_at)
        .execute(pool)
        .await?
        .rows_affected();
        if claimed == 0 && !repo.requested {
            // Another worker took it, or it was synced since the query.
            continue;
        }
        run_one(pool, &repo).await;
    }
    Ok(())
}

/// Reconcile one repo and write its outcome back to the row — the same state,
/// rev, count and message the console already renders.
async fn run_one(pool: &sqlx::PgPool, repo: &Due) {
    // A credential that cannot be decrypted is an operator-actionable failure,
    // so it takes the same path as a failed clone: `Error` on the row with the
    // reason, rather than a silent unauthenticated retry.
    let auth = match decrypt_auth(repo) {
        Ok(auth) => auth,
        Err(e) => {
            error!(repo = %repo.url, error = %e, "credential unusable");
            write_result(pool, repo, None, "Error", 0, &e).await;
            return;
        }
    };
    let target = sync::Repo {
        url: repo.url.clone(),
        branch: repo.branch.clone(),
        path: repo.path.clone(),
        auth,
    };
    // State semantics: `Error` is reserved for failures an operator can act on —
    // the clone failed, the path is missing, auth was rejected. A sync that
    // produced workflows is `Synced` even if some files alongside them were
    // malformed; those ride along as a warning count. Marking the repo red
    // because unrelated YAML sat in the same directory made a working sync look
    // broken and hid the failures that mattered.
    let (rev, state, count, message) = match sync::reconcile(pool, &target).await {
        Ok(report) => {
            let count = report.synced.len() as i64;
            let at = sync::short(&report.rev);
            // A signed bundle names itself: the operator reading the row
            // should see *which* signed statement is live, not just a count.
            // Its errors are empty by construction (a bundle with any is
            // never applied), so only the plain message gets the suffixes.
            let mut msg = match &report.bundle {
                Some(bundle) => format!("applied signed bundle {bundle}: {count} workflow(s) at {at}"),
                None if count == 0 => format!("no workflow files under '{}' at {at}", repo.path),
                None => format!("synced {count} workflow(s) at {at}"),
            };
            if report.skipped > 0 {
                msg.push_str(&format!(" · {} non-workflow file(s) ignored", report.skipped));
            }
            if !report.errors.is_empty() {
                msg.push_str(&format!(
                    " · {} warning(s): {}",
                    report.errors.len(),
                    report.errors.join("; ")
                ));
            }
            // Nothing synced and nothing even looked like a spec: the path is
            // almost certainly wrong, which *is* actionable.
            let state = if count == 0 && report.errors.is_empty() && report.skipped == 0 {
                "OutOfSync"
            } else {
                "Synced"
            };
            (Some(report.rev), state, count, msg)
        }
        Err(e) => {
            error!(repo = %repo.url, error = %e, "reconcile failed");
            (None, "Error", 0, e)
        }
    };
    info!(repo = %repo.url, state, count, "reconciled");
    write_result(pool, repo, rev.as_deref(), state, count, &message).await;
}

/// Write one reconcile outcome back to the repo row.
async fn write_result(
    pool: &sqlx::PgPool,
    repo: &Due,
    rev: Option<&str>,
    state: &str,
    count: i64,
    message: &str,
) {
    let res = sqlx::query(
        "UPDATE git_repos
         SET state = $1,
             rev = COALESCE($2, rev),
             workflow_count = $3,
             drift = 0,
             last_message = $4,
             last_synced_at = $5
         WHERE id = $6",
    )
    .bind(state)
    .bind(rev)
    .bind(count)
    .bind(message)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(&repo.id)
    .execute(pool)
    .await;
    if let Err(e) = res {
        warn!(error = %e, "writing sync result failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(kind: &str, secret: Option<String>, known_hosts: Option<String>) -> Due {
        row_as(kind, secret, known_hosts, None)
    }

    fn row_as(
        kind: &str,
        secret: Option<String>,
        known_hosts: Option<String>,
        username: Option<&str>,
    ) -> Due {
        Due {
            id: "r1".into(),
            url: "https://github.com/o/r.git".into(),
            branch: "main".into(),
            path: "dagron".into(),
            requested: true,
            last_synced_at: None,
            auth_kind: kind.into(),
            auth_username: username.map(str::to_string),
            auth_secret: secret,
            auth_known_hosts: known_hosts,
        }
    }

    /// Restores `DAGRON_ENV_SECRET_KEY` even when an assertion panics, so a
    /// failure here cannot change what a parallel test sees.
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

    /// The seam that would break silently: dagron-api encrypts, this worker
    /// decrypts, and nothing between them would notice a format drift until a
    /// real repository failed to sync.
    #[test]
    fn decrypts_what_dagron_api_stored() {
        let _key_env = EnvGuard::set(dagron_crypto::KEY_ENV, "test-key-for-gitops-credentials");
        let key = dagron_crypto::load_key().unwrap();

        let stored = dagron_crypto::encrypt(&key, "ghp_secret").unwrap();
        match decrypt_auth(&row("token", Some(stored), None)).unwrap() {
            Some(sync::GitAuth::Token { username, token }) => {
                assert_eq!(token, "ghp_secret");
                // No username stored → the documented default, not an empty one
                // (git would send an empty user and the forge would refuse it).
                assert_eq!(username, "x-access-token");
            }
            other => panic!("expected a token credential, got {other:?}"),
        }

        let stored = dagron_crypto::encrypt(&key, "-----BEGIN OPENSSH PRIVATE KEY-----\n").unwrap();
        match decrypt_auth(&row("ssh", Some(stored), Some("  ".into()))).unwrap() {
            Some(sync::GitAuth::Ssh { key, user, known_hosts }) => {
                assert!(key.starts_with("-----BEGIN OPENSSH"));
                // Blank known_hosts is "none", not an empty pin file that would
                // fail every host-key check.
                assert!(known_hosts.is_none());
                assert!(user.is_none());
            }
            other => panic!("expected an ssh credential, got {other:?}"),
        }

        // The stored SSH account reaches the clone. It used to be dropped here,
        // so the console reported a deploy user that ssh never used.
        let stored = dagron_crypto::encrypt(&key, "-----BEGIN OPENSSH PRIVATE KEY-----\n").unwrap();
        match decrypt_auth(&row_as("ssh", Some(stored.clone()), None, Some("deploy-bot"))).unwrap() {
            Some(sync::GitAuth::Ssh { user, .. }) => assert_eq!(user.as_deref(), Some("deploy-bot")),
            other => panic!("expected an ssh credential, got {other:?}"),
        }
        // A row carrying something that is not an account name — one written
        // before the API validated it, or written directly — is dropped rather
        // than interpolated into GIT_SSH_COMMAND.
        match decrypt_auth(&row_as("ssh", Some(stored), None, Some("evil -o ProxyCommand=sh"))).unwrap() {
            Some(sync::GitAuth::Ssh { user, .. }) => assert!(user.is_none(), "{user:?}"),
            other => panic!("expected an ssh credential, got {other:?}"),
        }

        // No credential is not an error — the worker falls back as it always did.
        assert!(decrypt_auth(&row("none", None, None)).unwrap().is_none());
        // A credential that cannot be read is: cloning unauthenticated instead
        // would report the repo as "not found".
        assert!(decrypt_auth(&row("token", Some("garbage".into()), None)).is_err());
        assert!(decrypt_auth(&row("token", None, None)).is_err());
    }
}
