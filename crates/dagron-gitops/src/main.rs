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
//!
//! Schema is owned by dagron-api, which creates `git_repos` and the tables here
//! on startup — one owner, no duplicate DDL to drift. This worker waits for them
//! rather than creating them.

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
async fn wait_for_schema(pool: &sqlx::PgPool) {
    loop {
        let ready = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM information_schema.tables
             WHERE table_name IN ('git_repos', 'gitops_workers', 'workflows')",
        )
        .fetch_one(pool)
        .await;
        match ready {
            Ok(3) => return,
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
}

/// Claim and reconcile everything due. Claiming is a conditional UPDATE, so two
/// workers racing the same repo cannot both run it.
async fn sweep(pool: &sqlx::PgPool, poll_secs: i64) -> anyhow::Result<()> {
    let stale = (chrono::Utc::now() - chrono::Duration::seconds(poll_secs)).to_rfc3339();
    let due: Vec<Due> = sqlx::query_as(
        "SELECT id, url, branch, path, last_synced_at,
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
    let target = sync::Repo {
        url: repo.url.clone(),
        branch: repo.branch.clone(),
        path: repo.path.clone(),
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
            let mut msg = if count == 0 {
                format!("no workflow files under '{}' at {at}", repo.path)
            } else {
                format!("synced {count} workflow(s) at {at}")
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
    .bind(&rev)
    .bind(count)
    .bind(&message)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(&repo.id)
    .execute(pool)
    .await;
    if let Err(e) = res {
        warn!(error = %e, "writing sync result failed");
    }
}
