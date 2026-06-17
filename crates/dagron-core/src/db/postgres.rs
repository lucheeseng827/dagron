//! Postgres backend (v2, horizontal scale). Enabled with `--features postgres`.
//!
//! Two things change versus the SQLite path, both about running N schedulers
//! against one datastore with zero coordination:
//!
//! 1. **Claiming is contention-free at the query level.** Instead of the
//!    read-then-CAS loop, `claim_ready` is a single `UPDATE ... WHERE id IN
//!    (SELECT ... FOR UPDATE SKIP LOCKED)`. Each worker's claim transaction skips
//!    rows another worker has already locked, so N workers carve up the ready set
//!    into disjoint batches without ever blocking on each other.
//!
//! 2. **The poll interval becomes a safety net, not the heartbeat.** Every
//!    mutation that can unblock work (`create_run`, `mark_task_succeeded`,
//!    `mark_task_failed`, `recover_expired_leases`) fires `pg_notify` on the
//!    `task_events` channel. Each scheduler holds a [`Waker`] backed by a
//!    `PgListener`, so it wakes the instant *any* worker (including itself)
//!    changes readiness, rather than waiting out the full tick.
//!
//! The state machine, lease semantics, dependency-counter decrement, and
//! recursive downstream cancellation are identical to the SQLite path — only the
//! claim mechanism and wake source differ.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Result};
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    dag::DagGraph,
    models::{RunStatus, TaskRun},
};

/// NOTIFY channel the reconcile loop listens on for early wakeups.
const EVENT_CHANNEL: &str = "task_events";

/// Backend-agnostic pool alias; `db::Pool` resolves to this when the `postgres`
/// feature is active.
pub type Pool = PgPool;

pub async fn init_pool(conn: &str) -> Result<Pool> {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(conn)
        .await?;

    sqlx::migrate!("./migrations_pg").run(&pool).await?;
    Ok(pool)
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction, then NOTIFYs so any idle scheduler wakes to advance
/// the root tasks. Returns the new run_id.
pub async fn create_run(pool: &Pool, dag: &DagGraph, yaml_spec: &str) -> Result<String> {
    let def_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let mut tx = pool.begin().await?;

    sqlx::query(
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(&def_id)
    .bind(&dag.spec.name)
    .bind(yaml_spec)
    .bind(&now)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO workflow_runs (id, definition_id, status, created_at) VALUES ($1, $2, 'running', $3)",
    )
    .bind(&run_id)
    .bind(&def_id)
    .bind(&now)
    .execute(&mut *tx)
    .await?;

    // Create task_run rows; store the full TaskSpec as JSON in `input` so the
    // row is self-contained — dispatch only needs task.id + task.input.
    let mut task_ids: HashMap<String, String> = HashMap::new();
    for task_spec in &dag.spec.tasks {
        let task_id = Uuid::new_v4().to_string();
        let dep_count = dag.dep_count(&task_spec.name) as i64;
        let input_json = serde_json::to_string(task_spec)?;

        // Fail fast on duplicate names — the DB UNIQUE index is the final guard.
        if task_ids.insert(task_spec.name.clone(), task_id.clone()).is_some() {
            bail!("duplicate task name '{}' in run '{}'", task_spec.name, run_id);
        }

        sqlx::query(
            "INSERT INTO task_runs
             (id, run_id, name, status, remaining_deps, input, scheduled_at)
             VALUES ($1, $2, $3, 'pending', $4, $5, $6)",
        )
        .bind(&task_id)
        .bind(&run_id)
        .bind(&task_spec.name)
        .bind(dep_count)
        .bind(&input_json)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    }

    // Wire up dependency edges.
    for task_spec in &dag.spec.tasks {
        let dependent_id = &task_ids[&task_spec.name];
        for dep_name in &task_spec.depends_on {
            // DagGraph::from_yaml already rejects unknown deps, but don't panic
            // if create_run is ever handed an unvalidated graph — reject the run.
            let Some(dependency_id) = task_ids.get(dep_name) else {
                bail!(
                    "task '{}' depends on unknown task '{}' in run '{}'",
                    task_spec.name,
                    dep_name,
                    run_id
                );
            };
            sqlx::query(
                "INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES ($1, $2)",
            )
            .bind(dependent_id)
            .bind(dependency_id)
            .execute(&mut *tx)
            .await?;
        }
    }

    notify(&mut *tx, &run_id).await?;
    tx.commit().await?;
    Ok(run_id)
}

/// Reclaim tasks whose worker lease expired — the core crash-recovery primitive.
/// NOTIFYs when anything was recovered so peers re-evaluate the now-ready rows.
pub async fn recover_expired_leases(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    // RETURNING run_id so we can fire one NOTIFY per distinct affected run —
    // a single expired-lease sweep may span multiple runs, and the SSE bridge
    // routes per run_id.
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL
         WHERE status = 'running'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < $1
         RETURNING run_id",
    )
    .bind(&now)
    .fetch_all(pool)
    .await?;

    let mut seen = std::collections::HashSet::new();
    for row in &rows {
        let run_id: String = row.try_get("run_id")?;
        if seen.insert(run_id.clone()) {
            notify(pool, &run_id).await?;
        }
    }
    Ok(rows.len() as u64)
}

/// Flip any pending task whose dependency counter has reached zero to 'ready'.
pub async fn advance_ready_tasks(pool: &Pool) -> Result<u64> {
    let r = sqlx::query(
        "UPDATE task_runs SET status = 'ready'
         WHERE status = 'pending' AND remaining_deps = 0",
    )
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// Claim up to `limit` ready tasks for `worker_id` using `FOR UPDATE SKIP LOCKED`.
///
/// A single statement locks-and-claims a disjoint batch: any row another worker
/// has already locked in its own claim transaction is skipped rather than waited
/// on, so N schedulers partition the ready set with zero coordination and no
/// CAS retries. `RETURNING attempt - 1` / `version - 1` reconstructs the
/// pre-claim snapshot the reconcile loop expects (the attempt that will run is
/// `attempt + 1`), keeping the return contract identical to the SQLite path.
pub async fn claim_ready(pool: &Pool, worker_id: &str, limit: i64) -> Result<Vec<TaskRun>> {
    let now = chrono::Utc::now().to_rfc3339();
    let lease_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(30)).to_rfc3339();

    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'running',
             claimed_by = $1,
             lease_expires_at = $2,
             attempt = attempt + 1,
             version = version + 1
         WHERE id IN (
             SELECT id FROM task_runs
             WHERE status = 'ready'
               AND (scheduled_at IS NULL OR scheduled_at <= $3)
             ORDER BY scheduled_at
             LIMIT $4
             FOR UPDATE SKIP LOCKED
         )
         RETURNING id, run_id, name, status,
                   attempt - 1 AS attempt, remaining_deps,
                   input, output, claimed_by, lease_expires_at,
                   version - 1 AS version, scheduled_at, finished_at",
    )
    .bind(worker_id)
    .bind(&lease_exp)
    .bind(&now)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    // Map manually so the status TEXT column never has to round-trip through a
    // native Postgres enum type — keeps the schema portable plain TEXT.
    let mut claimed = Vec::with_capacity(rows.len());
    for row in &rows {
        let status: String = row.try_get("status")?;
        claimed.push(TaskRun {
            id: row.try_get("id")?,
            run_id: row.try_get("run_id")?,
            name: row.try_get("name")?,
            status: status.parse()?,
            attempt: row.try_get("attempt")?,
            remaining_deps: row.try_get("remaining_deps")?,
            input: row.try_get("input")?,
            output: row.try_get("output")?,
            claimed_by: row.try_get("claimed_by")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
            version: row.try_get("version")?,
            scheduled_at: row.try_get("scheduled_at")?,
            finished_at: row.try_get("finished_at")?,
        });
    }
    Ok(claimed)
}

/// Mark a task succeeded and decrement remaining_deps for all direct dependents.
///
/// Guarded by `claimed_by = worker_id AND version = fence` (the post-claim
/// version pinning this exact attempt) so a stale executor whose lease was
/// reclaimed — even by this same process, which reuses one worker_id — cannot
/// double-apply dep decrements. NOTIFYs (in-transaction, so it fires only on
/// commit) to wake peers whose dependents may now be ready.
pub async fn mark_task_succeeded(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    output: Option<String>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'succeeded', finished_at = $1, output = $2, claimed_by = NULL
         WHERE id = $3 AND claimed_by = $4 AND version = $5
         RETURNING run_id",
    )
    .bind(&now)
    .bind(&output)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated) = updated else {
        tx.commit().await?;
        tracing::warn!(task_id, "stale completion ignored — task already reclaimed");
        return Ok(false);
    };
    let run_id: String = updated.try_get("run_id")?;

    // Decrement remaining_deps; advance_ready_tasks will flip zeros to 'ready'.
    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, &run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// Mark a task failed and cancel the entire downstream subgraph.
///
/// Same stale-worker guard as mark_task_succeeded. NOTIFYs so peers can observe
/// the cancellations and re-check run completion promptly.
pub async fn mark_task_failed(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    error: Option<String>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'failed', finished_at = $1, output = $2, claimed_by = NULL
         WHERE id = $3 AND claimed_by = $4 AND version = $5
         RETURNING run_id",
    )
    .bind(&now)
    .bind(&error)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated) = updated else {
        tx.commit().await?;
        tracing::warn!(task_id, "stale failure ignored — task already reclaimed");
        return Ok(false);
    };
    let run_id: String = updated.try_get("run_id")?;

    // Transitively cancel every downstream task so is_run_complete can terminate.
    sqlx::query(
        "WITH RECURSIVE downstream(id) AS (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
             UNION
             SELECT td.dependent_id
             FROM task_dependencies td
             JOIN downstream d ON td.dependency_id = d.id
         )
         UPDATE task_runs SET status = 'cancelled'
         WHERE id IN (SELECT id FROM downstream)
           AND status IN ('pending', 'ready')",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, &run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// Reset a failed task to `ready` for a later retry attempt.
///
/// `scheduled_at = retry_at` keeps `claim_ready` from picking it up until the
/// backoff window elapses; `attempt` is left for `claim_ready` to increment.
/// No NOTIFY is issued — the row is deliberately not yet claimable, so the
/// fixed-interval safety-net poll is what eventually picks it up at `retry_at`.
pub async fn retry_task(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    error: Option<String>,
    retry_at: String,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready',
             scheduled_at = $1,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = $2
         WHERE id = $3 AND claimed_by = $4 AND version = $5",
    )
    .bind(&retry_at)
    .bind(&error)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();

    if rows == 0 {
        tracing::warn!(task_id, "stale retry ignored — task already reclaimed");
        return Ok(false);
    }
    Ok(true)
}

/// Resurrect a failed/cancelled task from the management API (UI retry).
///
/// Unlike [`retry_task`], this carries NO worker fence — a human retrying a dead
/// task has no claim to match. Resets the task to `ready` (version bumped so any
/// late stale worker is fenced off) and re-arms the run if it had already been
/// finalized to `failed`. Phase 1 retries only this task; cascade-retry of
/// downstream tasks the failure cancelled is deferred. Returns false if the task
/// was not in a retryable (`failed`/`cancelled`) state.
#[allow(dead_code)] // consumed by dagron-api (management API), not the engine binary
pub async fn retry_task_from_ui(pool: &Pool, task_id: &str) -> Result<bool> {
    let mut tx = pool.begin().await?;

    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready',
             claimed_by = NULL,
             lease_expires_at = NULL,
             scheduled_at = NULL,
             output = NULL,
             version = version + 1
         WHERE id = $1 AND status IN ('failed', 'cancelled')
         RETURNING run_id",
    )
    .bind(task_id)
    .fetch_optional(&mut *tx)
    .await?;

    let Some(updated) = updated else {
        tx.commit().await?;
        return Ok(false);
    };
    let run_id: String = updated.try_get("run_id")?;

    // Re-arm a run that was already finalized failed so the reconcile loop re-engages.
    sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL
         WHERE id = $1 AND status = 'failed'",
    )
    .bind(&run_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, &run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// Cascade rerun-from-failed: resurrect every failed/cancelled task in a run and
/// re-arm the run so the reconcile loop resumes from the failure frontier, while
/// every already-`succeeded` task is left intact. The mirror image of
/// [`mark_task_failed`]'s downstream-cancel — see the SQLite copy for the full
/// rationale (the `remaining_deps` recompute is order-independent).
///
/// Returns `None` when the run does not exist, is not in a rerunnable
/// (`failed`/`cancelled`) state, or loses a concurrent-rerun race; otherwise
/// `Some(n)` with the number of tasks reset. `version` is bumped to fence stale
/// workers, and a `pg_notify` wakes idle peers the instant the run is re-armed.
#[cfg(feature = "ops")]
pub async fn rerun_from_failed(pool: &Pool, run_id: &str) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    // Re-arm the run as the atomic gate. This guarded UPDATE serializes concurrent
    // reruns: the row lock blocks a second tx until the first commits, after which
    // the status is already 'running' and it matches zero rows. A miss (run absent,
    // not rerunnable, or a lost race) → `None`, so no caller reports a false success.
    let armed = sqlx::query(
        "UPDATE workflow_runs
         SET status = 'running', finished_at = NULL, output = NULL
         WHERE id = $1 AND status IN ('failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if armed == 0 {
        tx.commit().await?;
        return Ok(None);
    }

    let reset = sqlx::query(
        "UPDATE task_runs
         SET status = 'pending',
             attempt = 0,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = NULL,
             finished_at = NULL,
             scheduled_at = $1,
             version = version + 1,
             remaining_deps = (
                 SELECT COUNT(*) FROM task_dependencies d
                 JOIN task_runs dep ON dep.id = d.dependency_id
                 WHERE d.dependent_id = task_runs.id
                   AND dep.status NOT IN ('succeeded', 'skipped')
             )
         WHERE run_id = $2 AND status IN ('failed', 'cancelled')",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(Some(reset))
}

/// Enabled schedules whose `next_fire_at` is due (v7 UI). Joined to the workflow
/// for its spec. Only the leadership holder calls this (see `schedule.rs`), and
/// `next_fire_at` lives in shared state, so there is no cross-node double-fire.
#[cfg(feature = "ops")]
pub async fn claim_due_schedules(pool: &Pool, now: &str) -> Result<Vec<crate::models::DueSchedule>> {
    use crate::models::DueSchedule;
    let rows = sqlx::query_as::<_, DueSchedule>(
        "SELECT s.id AS id, s.cron_expr AS cron_expr, w.spec AS spec
         FROM schedules s
         JOIN workflows w ON w.id = s.workflow_id
         WHERE s.enabled = 1
           AND s.next_fire_at IS NOT NULL
           AND s.next_fire_at <= $1",
    )
    .bind(now)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Advance a schedule after firing: set the next fire time + last-fired stamp.
#[cfg(feature = "ops")]
pub async fn advance_schedule(pool: &Pool, id: &str, next_fire_at: &str, fired_at: &str) -> Result<()> {
    sqlx::query(
        "UPDATE schedules SET next_fire_at = $2, last_fired_at = $3, updated_at = $3 WHERE id = $1",
    )
    .bind(id)
    .bind(next_fire_at)
    .bind(fired_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Number of `workflow_runs` still in the `running` state.
///
/// Used by the queue-ingestion path as an admission gate: the `IngestActor`
/// refuses to create new runs while this is at or above `MAX_INFLIGHT_RUNS`, so
/// a burst of submissions is buffered in the queue rather than exploding the
/// `task_runs` table. This is the backpressure that lets the scheduler absorb a
/// large influx without unbounded memory/DB growth.
pub async fn count_active_runs(pool: &Pool) -> Result<i64> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs WHERE status = 'running'")
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// Finalize every `running` workflow_run whose task_runs are all terminal.
///
/// The multi-run generalization of [`is_run_complete`]: rather than polling one
/// `run_id`, this sweeps all in-flight runs in one pass and flips each completed
/// one to `succeeded`/`failed`. The terminal `UPDATE` is guarded with
/// `status = 'running'`, so when several schedulers race only one finalizes a
/// given run. Returns the runs newly transitioned to terminal (for logging).
pub async fn reap_completed_runs(pool: &Pool) -> Result<Vec<(String, RunStatus)>> {
    let rows = sqlx::query(
        "SELECT wr.id AS run_id, tr.status AS status, COUNT(*) AS cnt
         FROM workflow_runs wr
         JOIN task_runs tr ON tr.run_id = wr.id
         WHERE wr.status = 'running'
         GROUP BY wr.id, tr.status",
    )
    .fetch_all(pool)
    .await?;

    struct Agg {
        total: i64,
        terminal: i64,
        failed: i64,
    }
    let mut runs: HashMap<String, Agg> = HashMap::new();
    for row in &rows {
        let run_id: String = row.try_get("run_id")?;
        let status: String = row.try_get("status")?;
        let cnt: i64 = row.try_get("cnt")?;
        let agg = runs.entry(run_id).or_insert(Agg { total: 0, terminal: 0, failed: 0 });
        agg.total += cnt;
        match status.as_str() {
            "succeeded" | "skipped" | "cancelled" => agg.terminal += cnt,
            "failed" => {
                agg.failed += cnt;
                agg.terminal += cnt;
            }
            _ => {}
        }
    }

    let now = chrono::Utc::now().to_rfc3339();
    let mut finalized = Vec::new();
    for (run_id, agg) in runs {
        if agg.total == 0 || agg.terminal < agg.total {
            continue;
        }
        let (status, status_str) = if agg.failed > 0 {
            (RunStatus::Failed, "failed")
        } else {
            (RunStatus::Succeeded, "succeeded")
        };
        let affected = sqlx::query(
            "UPDATE workflow_runs SET status = $1, finished_at = $2 WHERE id = $3 AND status = 'running'",
        )
        .bind(status_str)
        .bind(&now)
        .bind(&run_id)
        .execute(pool)
        .await?
        .rows_affected();
        if affected > 0 {
            finalized.push((run_id, status));
        }
    }
    Ok(finalized)
}

/// Returns the terminal RunStatus once every task_run is in a terminal state,
/// or None while work is still in progress.
#[allow(dead_code)] // retained as documented single-run API; the daemon loop uses reap_completed_runs
pub async fn is_run_complete(pool: &Pool, run_id: &str) -> Result<Option<RunStatus>> {
    let rows = sqlx::query(
        "SELECT status, COUNT(*) as cnt FROM task_runs WHERE run_id = $1 GROUP BY status",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    let mut total: i64 = 0;
    let mut terminal: i64 = 0;
    let mut failed: i64 = 0;

    for row in &rows {
        let status: String = row.try_get("status")?;
        let cnt: i64 = row.try_get("cnt")?;
        total += cnt;
        match status.as_str() {
            "succeeded" | "skipped" | "cancelled" => terminal += cnt,
            "failed" => {
                failed += cnt;
                terminal += cnt;
            }
            _ => {}
        }
    }

    if total == 0 || terminal < total {
        return Ok(None);
    }

    let final_status = if failed > 0 { RunStatus::Failed } else { RunStatus::Succeeded };
    let now = chrono::Utc::now().to_rfc3339();
    let status_str = if failed > 0 { "failed" } else { "succeeded" };

    sqlx::query("UPDATE workflow_runs SET status = $1, finished_at = $2 WHERE id = $3")
        .bind(status_str)
        .bind(&now)
        .bind(run_id)
        .execute(pool)
        .await?;

    Ok(Some(final_status))
}

// ── v4 dead-letter store ────────────────────────────────────────────────────

/// Park a poison submission that could not become a run. Core ingest-path write
/// (not ops-gated): the routing that stops a nack-loop must work in a lean build
/// too.
pub async fn record_dead_letter(
    pool: &Pool,
    payload: &str,
    error: &str,
    source: &str,
    failures: i64,
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO dead_letters
            (id, payload, error, source, failures, first_seen_at, last_error_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&id)
    .bind(payload)
    .bind(error)
    .bind(source)
    .bind(failures)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(id)
}

/// List parked dead letters, newest-first. Backs `GET /dead-letters`.
#[cfg(feature = "ops")]
pub async fn list_dead_letters(
    pool: &Pool,
    limit: i64,
) -> Result<Vec<crate::models::DeadLetter>> {
    use crate::models::DeadLetter;
    let rows = sqlx::query_as::<_, DeadLetter>(
        "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
         FROM dead_letters ORDER BY first_seen_at DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Fetch one dead letter by id (for redrive). Backs `POST /dead-letters/{id}/redrive`.
#[cfg(feature = "ops")]
pub async fn get_dead_letter(pool: &Pool, id: &str) -> Result<Option<crate::models::DeadLetter>> {
    use crate::models::DeadLetter;
    let row = sqlx::query_as::<_, DeadLetter>(
        "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
         FROM dead_letters WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Delete a dead letter (after a successful redrive, or to discard). Returns
/// whether a row was removed. Backs `DELETE /dead-letters/{id}`.
#[cfg(feature = "ops")]
pub async fn delete_dead_letter(pool: &Pool, id: &str) -> Result<bool> {
    let n = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

// ── v5 management API reads ─────────────────────────────────────────────────

/// Map a `task_runs` row to [`TaskRun`], parsing the TEXT `status` manually so the
/// column never round-trips through a native Postgres enum type.
#[cfg(feature = "ops")]
fn row_to_task(row: &sqlx::postgres::PgRow) -> Result<TaskRun> {
    let status: String = row.try_get("status")?;
    Ok(TaskRun {
        id: row.try_get("id")?,
        run_id: row.try_get("run_id")?,
        name: row.try_get("name")?,
        status: status.parse()?,
        attempt: row.try_get("attempt")?,
        remaining_deps: row.try_get("remaining_deps")?,
        input: row.try_get("input")?,
        output: row.try_get("output")?,
        claimed_by: row.try_get("claimed_by")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        version: row.try_get("version")?,
        scheduled_at: row.try_get("scheduled_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

/// List runs newest-first, optionally filtered by status. Joins the definition
/// for the DAG `name`. Backs `GET /runs` on the management API.
#[cfg(feature = "ops")]
pub async fn list_runs(
    pool: &Pool,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<crate::models::RunSummary>> {
    use crate::models::RunSummary;
    let base = "SELECT wr.id AS id, wd.name AS name, wr.status AS status,
                       wr.created_at AS created_at, wr.finished_at AS finished_at
                FROM workflow_runs wr
                JOIN workflow_definitions wd ON wd.id = wr.definition_id";
    let rows = match status {
        Some(s) => {
            // The status is bound as a parameter below (never interpolated), so
            // this is not injectable; validate against the enum anyway to reject
            // garbage early and satisfy SQL-injection scanners on the format!.
            let _: RunStatus = s
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid run status filter '{s}'"))?;
            sqlx::query(&format!(
                "{base} WHERE wr.status = $1 ORDER BY wr.created_at DESC LIMIT $2"
            ))
            .bind(s)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query(&format!("{base} ORDER BY wr.created_at DESC LIMIT $1"))
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let status: String = row.try_get("status")?;
        out.push(RunSummary {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            status: status.parse()?,
            created_at: row.try_get("created_at")?,
            finished_at: row.try_get("finished_at")?,
        });
    }
    Ok(out)
}

/// Fetch one run by id (or `None`). Backs `GET /runs/:id`.
#[cfg(feature = "ops")]
pub async fn get_run(pool: &Pool, run_id: &str) -> Result<Option<crate::models::WorkflowRun>> {
    use crate::models::WorkflowRun;
    let row = sqlx::query(
        "SELECT id, definition_id, status, input, output, created_at, finished_at
         FROM workflow_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    match row {
        None => Ok(None),
        Some(row) => {
            let status: String = row.try_get("status")?;
            Ok(Some(WorkflowRun {
                id: row.try_get("id")?,
                definition_id: row.try_get("definition_id")?,
                status: status.parse()?,
                input: row.try_get("input")?,
                output: row.try_get("output")?,
                created_at: row.try_get("created_at")?,
                finished_at: row.try_get("finished_at")?,
            }))
        }
    }
}

/// All task rows of a run, ordered by name. Backs `GET /runs/:id`.
#[cfg(feature = "ops")]
pub async fn list_tasks(pool: &Pool, run_id: &str) -> Result<Vec<TaskRun>> {
    let rows = sqlx::query(
        "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at
         FROM task_runs WHERE run_id = $1 ORDER BY name",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    rows.iter().map(row_to_task).collect()
}

/// Cancel a still-running run: every non-terminal task → `cancelled`, the run row
/// → `cancelled`. Idempotent — a second call (or a run already terminal) returns
/// `false`. NOTIFYs so any scheduler re-checks completion promptly. A `running`
/// task's lease is cleared; if its executor finishes anyway the fence guard in
/// `mark_task_*` rejects the stale write, so cancellation cannot be clobbered.
#[cfg(feature = "ops")]
pub async fn cancel_run(pool: &Pool, run_id: &str) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let run_rows = sqlx::query(
        "UPDATE workflow_runs SET status = 'cancelled', finished_at = $1
         WHERE id = $2 AND status = 'running'",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if run_rows == 0 {
        tx.commit().await?;
        return Ok(false);
    }

    sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', finished_at = $1, claimed_by = NULL, lease_expires_at = NULL
         WHERE run_id = $2 AND status IN ('pending', 'ready', 'running')",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// Run- and task-count gauges grouped by status, read straight from the
/// datastore for the `/metrics` endpoint.
#[cfg(feature = "ops")]
pub async fn status_counts(pool: &Pool) -> Result<crate::models::MetricsSnapshot> {
    let runs: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM workflow_runs GROUP BY status")
            .fetch_all(pool)
            .await?;
    let tasks: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM task_runs GROUP BY status")
            .fetch_all(pool)
            .await?;
    let dead_letters: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters").fetch_one(pool).await?;
    Ok(crate::models::MetricsSnapshot {
        runs_by_status: runs,
        tasks_by_status: tasks,
        dead_letters,
    })
}

// ── v6 retention GC ─────────────────────────────────────────────────────────

/// Delete terminal runs finished before `cutoff` (an RFC-3339 timestamp), along
/// with their task rows, dependency edges, and any now-unreferenced definitions.
/// Returns the number of `workflow_runs` removed. Single transaction so a partial
/// purge is impossible. Gated behind the leadership singleton so only one
/// scheduler reclaims at a time.
#[cfg(feature = "ops")]
pub async fn gc_old_runs(pool: &Pool, cutoff: &str) -> Result<u64> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        "DELETE FROM task_dependencies
         WHERE dependent_id IN (
             SELECT tr.id FROM task_runs tr
             JOIN workflow_runs wr ON wr.id = tr.run_id
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < $1
         )",
    )
    .bind(cutoff)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "DELETE FROM task_runs
         WHERE run_id IN (
             SELECT id FROM workflow_runs
             WHERE status IN ('succeeded','failed','cancelled')
               AND finished_at IS NOT NULL AND finished_at < $1
         )",
    )
    .bind(cutoff)
    .execute(&mut *tx)
    .await?;

    let deleted = sqlx::query(
        "DELETE FROM workflow_runs
         WHERE status IN ('succeeded','failed','cancelled')
           AND finished_at IS NOT NULL AND finished_at < $1",
    )
    .bind(cutoff)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    sqlx::query(
        "DELETE FROM workflow_definitions
         WHERE id NOT IN (SELECT DISTINCT definition_id FROM workflow_runs)",
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(deleted)
}

// ── v5 leadership singleton ─────────────────────────────────────────────────

/// Try to acquire (or renew) the lease for `role`, valid for `lease_secs`.
///
/// One `leader_election` row per role; the caller wins iff the row is absent,
/// already held by it, or its current lease has expired — decided atomically in a
/// single `INSERT … ON CONFLICT DO UPDATE … WHERE`. Returns `true` while this
/// `holder` owns the role. (On Postgres `pg_advisory_lock` is a native
/// alternative, but the lease row keeps the API identical to the SQLite backend.)
#[cfg(feature = "ops")]
pub async fn try_acquire_leadership(
    pool: &Pool,
    role: &str,
    holder: &str,
    lease_secs: i64,
) -> Result<bool> {
    let now = chrono::Utc::now();
    let now_str = now.to_rfc3339();
    let new_exp = (now + chrono::TimeDelta::seconds(lease_secs)).to_rfc3339();

    let rows = sqlx::query(
        "INSERT INTO leader_election (role, holder, lease_expires_at)
         VALUES ($1, $2, $3)
         ON CONFLICT(role) DO UPDATE SET
             holder = excluded.holder,
             lease_expires_at = excluded.lease_expires_at
         WHERE leader_election.holder = excluded.holder
            OR leader_election.lease_expires_at < $4",
    )
    .bind(role)
    .bind(holder)
    .bind(&new_exp)
    .bind(&now_str)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(rows > 0)
}

/// Fire a `task_events` NOTIFY on the given executor (pool or transaction).
///
/// When run inside a transaction the notification is buffered by Postgres and
/// delivered only on commit, so a rolled-back mutation never wakes a peer for
/// work that did not actually happen.
///
/// The reconcile-loop [`Waker`] ignores the payload (it only needs the wakeup
/// edge), so carrying the run_id is backward compatible — but it lets the
/// management API's SSE bridge route each event to only the clients watching
/// that run, instead of waking every browser on every transition.
async fn notify<'e, E>(executor: E, run_id: &str) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(EVENT_CHANNEL)
        .bind(run_id)
        .execute(executor)
        .await?;
    Ok(())
}

/// Reconcile-loop waker backed by `LISTEN/NOTIFY`.
///
/// Holds a dedicated listener connection subscribed to `task_events`. `wait`
/// returns the moment a notification arrives *or* the poll interval elapses,
/// whichever comes first — so steady-state latency tracks real events while the
/// timer remains a safety net for missed wakeups and time-based retries.
pub struct Waker {
    listener: PgListener,
}

impl Waker {
    pub async fn connect(pool: &Pool) -> Result<Self> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen(EVENT_CHANNEL).await?;
        Ok(Self { listener })
    }

    pub async fn wait(&mut self, interval: Duration) -> Result<()> {
        tokio::select! {
            recv = self.listener.recv() => {
                // A transient listener/connection blip must not stop scheduling:
                // log and fall back to a timer tick. PgListener auto-reconnects,
                // and the next tick's queries run against the live pool regardless.
                if let Err(err) = recv {
                    tracing::warn!(error = ?err, "listener recv failed; timer fallback for this tick");
                    tokio::time::sleep(interval).await;
                }
            }
            _ = tokio::time::sleep(interval) => {}
        }
        Ok(())
    }
}
