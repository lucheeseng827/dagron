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
    #[cfg(feature = "enterprise")]
    sqlx::migrate!("./migrations_pg_ee").run(&pool).await?;
    Ok(pool)
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction, then NOTIFYs so any idle scheduler wakes to advance
/// the root tasks. Returns the new run_id.
pub async fn create_run(pool: &Pool, dag: &DagGraph, yaml_spec: &str) -> Result<String> {
    let def_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let created = chrono::Utc::now();
    let now = created.to_rfc3339();
    // Run-level wall-clock budget (spec `run_timeout_secs`): persist the absolute
    // deadline so the sweep is a pure indexed comparison, no spec re-parse.
    let deadline_at = dag
        .spec
        .run_timeout_secs
        .map(|secs| (created + chrono::TimeDelta::seconds(secs.min(i64::MAX as u64) as i64)).to_rfc3339());
    // Soft SLA deadline (spec `deadline`): emit-only, never cancels (#20).
    let alert_deadline_at = dag.spec.deadline.as_ref().and_then(|d| {
        crate::dag::parse_duration_secs(&d.within)
            .ok()
            .map(|secs| (created + chrono::TimeDelta::seconds(secs.min(i64::MAX as u64) as i64)).to_rfc3339())
    });

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
        "INSERT INTO workflow_runs
           (id, definition_id, status, created_at, deadline_at, alert_deadline_at, result_from)
         VALUES ($1, $2, 'running', $3, $4, $5, $6)",
    )
    .bind(&run_id)
    .bind(&def_id)
    .bind(&now)
    .bind(&deadline_at)
    .bind(&alert_deadline_at)
    .bind(&dag.spec.result_from)
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

        let trigger_rule =
            task_spec.trigger_rule.as_deref().unwrap_or(crate::models::DEFAULT_TRIGGER_RULE);
        let allow_failure = i64::from(task_spec.allow_failure);
        let is_approval = i64::from(task_spec.is_approval());
        let approval_timeout = task_spec.approval_timeout_secs.map(|s| s as i64);
        let approval_on_timeout = task_spec.approval_on_timeout.as_deref();
        sqlx::query(
            "INSERT INTO task_runs
             (id, run_id, name, status, remaining_deps, input, scheduled_at, trigger_rule,
              allow_failure, is_approval, approval_timeout_secs, approval_on_timeout)
             VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(&task_id)
        .bind(&run_id)
        .bind(&task_spec.name)
        .bind(dep_count)
        .bind(&input_json)
        .bind(&now)
        .bind(trigger_rule)
        .bind(allow_failure)
        .bind(is_approval)
        .bind(approval_timeout)
        .bind(approval_on_timeout)
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

/// Advance pending tasks whose dependencies are all terminal
/// (`remaining_deps == 0`): evaluate each task's `trigger_rule` against its
/// dependencies' outcomes and either flip it to `ready` (rule satisfied) or
/// `skipped` (not satisfied). A newly-skipped task is itself terminal, so its
/// dependents' `remaining_deps` are decremented; the cascade resolves over
/// subsequent reconcile ticks. Returns the number of tasks transitioned.
///
/// Each transition is guarded by `status = 'pending'`, so concurrent schedulers
/// are winner-take-all and a skip's dependent-decrement runs exactly once.
pub async fn advance_ready_tasks(pool: &Pool) -> Result<u64> {
    let candidates: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT id, trigger_rule, is_approval FROM task_runs
         WHERE status = 'pending' AND remaining_deps = 0
         ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut transitioned = 0u64;
    for (task_id, rule, is_approval) in candidates {
        let dep_statuses: Vec<String> = sqlx::query_scalar(
            "SELECT dep.status FROM task_dependencies d
             JOIN task_runs dep ON dep.id = d.dependency_id
             WHERE d.dependent_id = $1",
        )
        .bind(&task_id)
        .fetch_all(pool)
        .await?;

        if crate::models::trigger_rule_ready(&rule, &dep_statuses) {
            // An approval gate (#19) parks in `awaiting_approval` (never claimed by
            // a worker); `scheduled_at` marks when it began waiting for the sweep.
            let rows = if is_approval != 0 {
                let now = chrono::Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE task_runs SET status = 'awaiting_approval', scheduled_at = $1
                     WHERE id = $2 AND status = 'pending'",
                )
                .bind(&now)
                .bind(&task_id)
                .execute(pool)
                .await?
                .rows_affected()
            } else {
                sqlx::query(
                    "UPDATE task_runs SET status = 'ready' WHERE id = $1 AND status = 'pending'",
                )
                .bind(&task_id)
                .execute(pool)
                .await?
                .rows_affected()
            };
            transitioned += rows;
        } else {
            let now = chrono::Utc::now().to_rfc3339();
            let mut tx = pool.begin().await?;
            let rows = sqlx::query(
                "UPDATE task_runs SET status = 'skipped', finished_at = $1
                 WHERE id = $2 AND status = 'pending'",
            )
            .bind(&now)
            .bind(&task_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if rows > 0 {
                sqlx::query(
                    "UPDATE task_runs SET remaining_deps = remaining_deps - 1
                     WHERE id IN (
                         SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
                     ) AND status = 'pending'",
                )
                .bind(&task_id)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            transitioned += rows;
        }
    }
    Ok(transitioned)
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

/// Mark a task failed and decrement its direct dependents' dependency counters
/// (a failure is terminal, exactly like success). Whether each dependent then
/// runs or is skipped is decided by its `trigger_rule` in
/// [`advance_ready_tasks`] — so an `all_success` dependent is skipped when this
/// task failed, while an `all_done`/`one_failed` dependent still runs.
///
/// Same stale-worker guard as mark_task_succeeded. NOTIFYs so peers re-check
/// readiness / run completion promptly.
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

    // A failure is terminal — decrement dependents so their trigger_rule can be
    // evaluated (mirrors mark_task_succeeded); advance_ready_tasks then runs or
    // skips each dependent per its rule.
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

/// Append a live-output chunk to a still-running task so the API/UI can tail it
/// before the task exits (fast-win #17). Guarded by `version = fence AND status =
/// 'running'`: only the current attempt writes, and a terminal row is immutable
/// (a stale attempt's late chunk can't resurrect output). `reset` marks the first
/// chunk of an attempt — it replaces any prior-attempt output so a retried task's
/// tail starts clean; subsequent chunks append. The task's final output is
/// overwritten whole by `mark_task_*` at completion, so this is a live view only.
/// A `pg_notify` wakes SSE subscribers so a browser tail updates promptly.
pub async fn append_task_output(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    chunk: &str,
    reset: bool,
) -> Result<()> {
    let sql = if reset {
        "UPDATE task_runs SET output = $1
         WHERE id = $2 AND version = $3 AND status = 'running' RETURNING run_id"
    } else {
        "UPDATE task_runs SET output = COALESCE(output, '') || $1
         WHERE id = $2 AND version = $3 AND status = 'running' RETURNING run_id"
    };
    let mut tx = pool.begin().await?;
    let run_id: Option<String> = sqlx::query_scalar(sql)
        .bind(chunk)
        .bind(task_id)
        .bind(fence)
        .fetch_optional(&mut *tx)
        .await?;
    // Only notify if a row actually changed (the guard matched) — a stale/late
    // chunk that hit nothing shouldn't wake subscribers.
    if let Some(run_id) = run_id {
        notify(&mut *tx, &run_id).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Resolve a human approval gate (#19): `approve` → the task succeeds and its
/// dependents advance; reject → it fails and its `all_success` dependents skip.
/// Guarded on `status = 'awaiting_approval'` AND the run, so a double-approve or a
/// wrong-run task id is a no-op. Returns whether it resolved (false → 404/409). A
/// `pg_notify` wakes the reconcile loop so the freed dependents advance promptly.
#[cfg(feature = "ops")]
pub async fn resolve_approval(
    pool: &Pool,
    run_id: &str,
    task_id: &str,
    approve: bool,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) =
        if approve { ("succeeded", "approved") } else { ("failed", "rejected") };
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2, output = $3
         WHERE id = $4 AND run_id = $5 AND status = 'awaiting_approval'",
    )
    .bind(status)
    .bind(&now)
    .bind(output)
    .bind(task_id)
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if rows == 0 {
        tx.commit().await?;
        return Ok(false);
    }
    sqlx::query(
        "UPDATE task_runs SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// Auto-resolve approval gates whose `approval_timeout_secs` elapsed since they
/// began waiting (`scheduled_at`), applying `approval_on_timeout` (default
/// `reject`). Returns the `(task_id, approved)` decisions. Idempotent via
/// `resolve_approval`'s guard.
#[cfg(feature = "ops")]
pub async fn resolve_expired_approvals(pool: &Pool) -> Result<Vec<(String, bool)>> {
    let now = chrono::Utc::now();
    let candidates: Vec<(String, String, Option<String>, i64, Option<String>)> = sqlx::query_as(
        "SELECT id, run_id, scheduled_at, approval_timeout_secs, approval_on_timeout
         FROM task_runs
         WHERE status = 'awaiting_approval' AND approval_timeout_secs IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    let mut resolved = Vec::new();
    for (id, run_id, scheduled_at, timeout, on_timeout) in candidates {
        let Some(started) = scheduled_at
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        else {
            continue;
        };
        let deadline =
            started.with_timezone(&chrono::Utc) + chrono::TimeDelta::seconds(timeout);
        if now < deadline {
            continue;
        }
        let approve = on_timeout.as_deref() == Some("approve");
        if resolve_approval(pool, &run_id, &id, approve).await? {
            resolved.push((id, approve));
        }
    }
    Ok(resolved)
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

    // Reset the broken cone (failed, cancelled, and rule-skipped tasks) in two
    // statements so the remaining_deps recompute sees the post-reset statuses —
    // a skipped→pending row must count as outstanding, which a single
    // self-referential UPDATE cannot guarantee.
    let reset = sqlx::query(
        "UPDATE task_runs
         SET status = 'pending',
             attempt = 0,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = NULL,
             finished_at = NULL,
             scheduled_at = $1,
             version = version + 1
         WHERE run_id = $2 AND status IN ('failed', 'cancelled', 'skipped')",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = (
                 SELECT COUNT(*) FROM task_dependencies d
                 JOIN task_runs dep ON dep.id = d.dependency_id
                 WHERE d.dependent_id = task_runs.id
                   AND dep.status NOT IN ('succeeded', 'skipped')
             )
         WHERE run_id = $1 AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(Some(reset))
}

/// Whether a task with `task_id` exists within `run_id`. Used by the ops API to
/// tell an unknown-task `404` apart from a not-clearable `409` on the error path.
#[cfg(feature = "ops")]
pub async fn task_exists(pool: &Pool, run_id: &str, task_id: &str) -> Result<bool> {
    let found: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM task_runs WHERE id = $1 AND run_id = $2")
            .bind(task_id)
            .bind(run_id)
            .fetch_optional(pool)
            .await?;
    Ok(found.is_some())
}

/// Clear a completed task and re-run it together with its transitive downstream
/// cone ("clear + downstream"). The target task and every terminal task
/// that (transitively) depends on it are reset to `pending` (attempt cleared,
/// `version` bumped to fence any stale worker), `remaining_deps` is recomputed,
/// and the run is re-armed to `running` if it had finished. Returns the number
/// of tasks reset, or `None` if the target doesn't exist in the run or isn't in
/// a terminal state (a running/pending task can't be cleared — `409`). See the
/// SQLite copy for the full rationale; a `pg_notify` wakes idle peers on commit.
#[cfg(feature = "ops")]
pub async fn clear_task_with_downstream(
    pool: &Pool,
    run_id: &str,
    task_id: &str,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM task_runs WHERE id = $1 AND run_id = $2")
            .bind(task_id)
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(status) = status else {
        tx.commit().await?;
        return Ok(None);
    };
    if !matches!(status.as_str(), "succeeded" | "failed" | "skipped" | "cancelled") {
        tx.commit().await?;
        return Ok(None); // only a completed task can be cleared
    }

    // Reset the target + its transitive downstream cone (terminal tasks only).
    let reset = sqlx::query(
        "WITH RECURSIVE cone(id) AS (
             SELECT id FROM task_runs WHERE id = $1 AND run_id = $2
             UNION
             SELECT td.dependent_id FROM task_dependencies td
             JOIN cone c ON td.dependency_id = c.id
         )
         UPDATE task_runs
         SET status = 'pending', attempt = 0, claimed_by = NULL, lease_expires_at = NULL,
             output = NULL, finished_at = NULL, scheduled_at = $3, version = version + 1
         WHERE id IN (SELECT id FROM cone)
           AND status IN ('succeeded', 'failed', 'skipped', 'cancelled')",
    )
    .bind(task_id)
    .bind(run_id)
    .bind(&now)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // Recompute remaining_deps for the reset frontier from the post-reset
    // statuses. A dep counts as outstanding only if it is *non-terminal*: a
    // terminal `failed`/`cancelled` upstream outside the reset cone will never
    // decrement again, so counting it would strand the cleared task forever
    // (its trigger_rule should decide once all deps are terminal).
    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = (
                 SELECT COUNT(*) FROM task_dependencies d
                 JOIN task_runs dep ON dep.id = d.dependency_id
                 WHERE d.dependent_id = task_runs.id
                   AND dep.status NOT IN ('succeeded', 'skipped', 'failed', 'cancelled')
             )
         WHERE run_id = $1 AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    // Re-arm a run that had finished so the reconcile loop resumes.
    sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL, output = NULL
         WHERE id = $1 AND status IN ('succeeded', 'failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

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
        "SELECT s.id AS id, s.cron_expr AS cron_expr, w.spec AS spec,
                s.next_fire_at AS next_fire_at, s.timezone AS timezone,
                s.when_expr AS when_expr, s.stop_expr AS stop_expr
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

/// Advance only `next_fire_at` (not `last_fired_at`) — used when a `when:` gate
/// skips a fire: the slot is consumed so the schedule doesn't re-evaluate it,
/// but `last_fired_at` stays put because nothing actually fired.
#[cfg(feature = "ops")]
pub async fn advance_schedule_gated(pool: &Pool, id: &str, next_fire_at: &str, now: &str) -> Result<()> {
    sqlx::query("UPDATE schedules SET next_fire_at = $2, updated_at = $3 WHERE id = $1")
        .bind(id)
        .bind(next_fire_at)
        .bind(now)
        .execute(pool)
        .await?;
    Ok(())
}

/// Outcome counts for a schedule's runs — `(succeeded, failed, total)` — the
/// variables a `stopStrategy` expression is evaluated against. Only runs stamped
/// with this `schedule_id` (i.e. fired by the DB-schedule loop) are counted.
#[cfg(feature = "ops")]
pub async fn schedule_run_counts(pool: &Pool, schedule_id: &str) -> Result<(i64, i64, i64)> {
    let row: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0)::bigint AS succeeded,
            COALESCE(SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END), 0)::bigint AS failed,
            COUNT(*)::bigint AS total
         FROM workflow_runs WHERE schedule_id = $1",
    )
    .bind(schedule_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Stamp a run with the schedule that created it, so `stopStrategy` can count
/// its outcomes. Called by the DB-schedule loop right after `create_run`.
#[cfg(feature = "ops")]
pub async fn stamp_run_schedule(pool: &Pool, run_id: &str, schedule_id: &str) -> Result<()> {
    sqlx::query("UPDATE workflow_runs SET schedule_id = $1 WHERE id = $2")
        .bind(schedule_id)
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Auto-stop a schedule when its `stopStrategy` expression trips: disable it and
/// record why. Reuses the existing `enabled = 0` gate (so `claim_due_schedules`
/// skips it) and surfaces `stopped_at`/`stop_reason` to the UI.
#[cfg(feature = "ops")]
pub async fn stop_schedule(pool: &Pool, id: &str, reason: &str, now: &str) -> Result<()> {
    sqlx::query(
        "UPDATE schedules
         SET enabled = 0, stopped_at = $2, stop_reason = $3, updated_at = $2
         WHERE id = $1",
    )
    .bind(id)
    .bind(now)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

// ── QW3-catchup automatic backfill & self-healing ───────────────────────────────────
//
// Postgres mirror of the SQLite QW3-catchup family — identical contracts, `$n` binds. See
// the SQLite copies for the full rationale. Powers the engine's leadership-gated
// `backfill.rs`: catch a schedule up after a gap, and auto-rerun failed runs, both
// bounded and emitted to the transactional outbox.

/// Schedules opted into automatic catch-up, joined to their workflow spec.
#[cfg(feature = "enterprise")]
pub async fn list_catchup_schedules(pool: &Pool) -> Result<Vec<crate::models::CatchupSchedule>> {
    use crate::models::CatchupSchedule;
    let rows = sqlx::query_as::<_, CatchupSchedule>(
        "SELECT s.id AS id, s.cron_expr AS cron_expr, w.spec AS spec,
                s.timezone AS timezone, s.last_fired_at AS last_fired_at,
                s.catchup_window_secs AS catchup_window_secs,
                s.catchup_max_runs AS catchup_max_runs
         FROM schedules s JOIN workflows w ON w.id = s.workflow_id
         WHERE s.enabled = 1 AND s.catchup = 1",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Claim one backfill slot in the dedup ledger (true iff newly inserted).
#[cfg(feature = "ops")]
pub async fn claim_backfill_slot(
    pool: &Pool,
    schedule_id: &str,
    logical_date: &str,
    now: &str,
) -> Result<bool> {
    let n = sqlx::query(
        "INSERT INTO schedule_backfills (schedule_id, logical_date, created_at)
         VALUES ($1, $2, $3) ON CONFLICT (schedule_id, logical_date) DO NOTHING",
    )
    .bind(schedule_id)
    .bind(logical_date)
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// Record which run filled a claimed slot (best-effort).
#[cfg(feature = "ops")]
pub async fn record_backfill_run(
    pool: &Pool,
    schedule_id: &str,
    logical_date: &str,
    run_id: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE schedule_backfills SET run_id = $1 WHERE schedule_id = $2 AND logical_date = $3",
    )
    .bind(run_id)
    .bind(schedule_id)
    .bind(logical_date)
    .execute(pool)
    .await?;
    Ok(())
}

/// Release a claimed slot whose `create_run` failed so a later sweep can retry it.
#[cfg(feature = "ops")]
pub async fn release_backfill_slot(pool: &Pool, schedule_id: &str, logical_date: &str) -> Result<()> {
    sqlx::query("DELETE FROM schedule_backfills WHERE schedule_id = $1 AND logical_date = $2")
        .bind(schedule_id)
        .bind(logical_date)
        .execute(pool)
        .await?;
    Ok(())
}

// ── First-class backfill jobs (#18) ─────────────────────────────────────────

/// Insert a new paced backfill job.
#[cfg(feature = "ops")]
pub async fn create_backfill(pool: &Pool, job: &crate::models::BackfillJob) -> Result<()> {
    sqlx::query(
        "INSERT INTO backfills
           (id, schedule_id, cron_expr, timezone, spec, range_from, range_to, cursor,
            status, max_runs, requested, fired, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(&job.id)
    .bind(&job.schedule_id)
    .bind(&job.cron_expr)
    .bind(&job.timezone)
    .bind(&job.spec)
    .bind(&job.range_from)
    .bind(&job.range_to)
    .bind(&job.cursor)
    .bind(&job.status)
    .bind(job.max_runs)
    .bind(job.requested)
    .bind(job.fired)
    .bind(&job.created_at)
    .bind(&job.updated_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Active (`running`) backfill jobs — the pacing loop's work-list each tick.
#[cfg(feature = "ops")]
pub async fn list_active_backfills(pool: &Pool) -> Result<Vec<crate::models::BackfillJob>> {
    let rows = sqlx::query_as::<_, crate::models::BackfillJob>(
        "SELECT id, schedule_id, cron_expr, timezone, spec, range_from, range_to, cursor,
                status, max_runs, requested, fired, created_at, updated_at
         FROM backfills WHERE status = 'running' ORDER BY created_at",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Backfill jobs for the API list view, most-recent-first, optionally filtered by
/// schedule. Bounded by `limit`.
#[cfg(feature = "ops")]
pub async fn list_backfills(
    pool: &Pool,
    schedule_id: Option<&str>,
    limit: i64,
) -> Result<Vec<crate::models::BackfillJob>> {
    let rows = sqlx::query_as::<_, crate::models::BackfillJob>(
        "SELECT id, schedule_id, cron_expr, timezone, spec, range_from, range_to, cursor,
                status, max_runs, requested, fired, created_at, updated_at
         FROM backfills
         WHERE ($1::text IS NULL OR schedule_id = $1)
         ORDER BY created_at DESC LIMIT $2",
    )
    .bind(schedule_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One backfill job by id.
#[cfg(feature = "ops")]
pub async fn get_backfill(pool: &Pool, id: &str) -> Result<Option<crate::models::BackfillJob>> {
    let row = sqlx::query_as::<_, crate::models::BackfillJob>(
        "SELECT id, schedule_id, cron_expr, timezone, spec, range_from, range_to, cursor,
                status, max_runs, requested, fired, created_at, updated_at
         FROM backfills WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Advance a job's cursor and set its absolute `fired` count after a pacing tick.
#[cfg(feature = "ops")]
pub async fn advance_backfill(pool: &Pool, id: &str, cursor: &str, fired: i64, now: &str) -> Result<()> {
    sqlx::query("UPDATE backfills SET cursor = $1, fired = $2, updated_at = $3 WHERE id = $4")
        .bind(cursor)
        .bind(fired)
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Mark a job `completed` (range exhausted or `max_runs` reached).
#[cfg(feature = "ops")]
pub async fn complete_backfill(pool: &Pool, id: &str, now: &str) -> Result<()> {
    sqlx::query("UPDATE backfills SET status = 'completed', updated_at = $1 WHERE id = $2 AND status = 'running'")
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Cancel a running job so the pacing loop stops firing it. Returns `true` only
/// when a `running` job was actually cancelled (a completed/unknown/already-
/// cancelled job returns `false` → the API answers `409`/`404`).
#[cfg(feature = "ops")]
pub async fn cancel_backfill(pool: &Pool, id: &str, now: &str) -> Result<bool> {
    let n = sqlx::query("UPDATE backfills SET status = 'cancelled', updated_at = $1 WHERE id = $2 AND status = 'running'")
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

/// Terminally-`failed` runs eligible for an automatic rerun (under the attempt
/// cap, past the cooldown). Newest failures first; bounded by `limit`.
#[cfg(feature = "enterprise")]
pub async fn list_failed_runs_for_rerun(
    pool: &Pool,
    max_attempts: i64,
    cooldown_cutoff: &str,
    limit: i64,
) -> Result<Vec<String>> {
    let ids: Vec<(String,)> = sqlx::query_as(
        "SELECT wr.id
         FROM workflow_runs wr
         LEFT JOIN run_reruns rr ON rr.run_id = wr.id
         WHERE wr.status = 'failed'
           AND COALESCE(rr.attempts, 0) < $1
           AND (rr.last_rerun_at IS NULL OR rr.last_rerun_at < $2)
         ORDER BY wr.finished_at DESC
         LIMIT $3",
    )
    .bind(max_attempts)
    .bind(cooldown_cutoff)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(ids.into_iter().map(|(id,)| id).collect())
}

/// Record one auto-rerun attempt against a run (upsert).
#[cfg(feature = "enterprise")]
pub async fn bump_rerun_attempt(pool: &Pool, run_id: &str, now: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO run_reruns (run_id, attempts, last_rerun_at)
         VALUES ($1, 1, $2)
         ON CONFLICT(run_id) DO UPDATE SET
             attempts = run_reruns.attempts + 1,
             last_rerun_at = excluded.last_rerun_at",
    )
    .bind(run_id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Count runs still `running` whose `created_at` predates `stall_cutoff`.
#[cfg(feature = "enterprise")]
pub async fn count_incomplete_runs(pool: &Pool, stall_cutoff: &str) -> Result<i64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running' AND created_at < $1",
    )
    .bind(stall_cutoff)
    .fetch_one(pool)
    .await?;
    Ok(n)
}

/// Append a `pending` event to the transactional outbox out-of-band, and NOTIFY
/// so a listening drainer wakes promptly (parity with the run-finalization path).
/// Used by the auto-backfill loop and the backfill-job pacer (#18).
#[cfg(feature = "ops")]
pub async fn enqueue_outbox_event(
    pool: &Pool,
    run_id: &str,
    event_type: &str,
    payload: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO event_outbox
           (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
         VALUES ($1, $2, $3, $4, 'pending', 0, $5, $6)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(run_id)
    .bind(event_type)
    .bind(payload)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
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
    // Group by (status, allow_failure) so a `failed` task with allow_failure=1
    // counts as terminal but does not fail the run (fast-win #11).
    let rows = sqlx::query(
        "SELECT wr.id AS run_id, tr.status AS status, tr.allow_failure AS allow_failure, COUNT(*) AS cnt
         FROM workflow_runs wr
         JOIN task_runs tr ON tr.run_id = wr.id
         WHERE wr.status = 'running'
         GROUP BY wr.id, tr.status, tr.allow_failure",
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
        let allow_failure: i64 = row.try_get("allow_failure")?;
        let cnt: i64 = row.try_get("cnt")?;
        let agg = runs.entry(run_id).or_insert(Agg { total: 0, terminal: 0, failed: 0 });
        agg.total += cnt;
        match status.as_str() {
            "succeeded" | "skipped" | "cancelled" => agg.terminal += cnt,
            "failed" => {
                agg.terminal += cnt;
                if allow_failure == 0 {
                    agg.failed += cnt;
                }
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
        // Finalize the run and append its outbox event in ONE transaction, so the
        // event exists iff the finalization commits (transactional outbox).
        let mut tx = pool.begin().await?;
        let affected = sqlx::query(
            "UPDATE workflow_runs SET status = $1, finished_at = $2 WHERE id = $3 AND status = 'running'",
        )
        .bind(status_str)
        .bind(&now)
        .bind(&run_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected > 0 {
            // Run result (fast-win #15): on success, copy the `result_from` task's
            // output into the run so a waiting caller gets a single return value.
            if matches!(status, RunStatus::Succeeded) {
                sqlx::query(
                    "UPDATE workflow_runs
                     SET output = (
                             SELECT tr.output FROM task_runs tr
                             WHERE tr.run_id = workflow_runs.id
                               AND tr.name = workflow_runs.result_from
                         )
                     WHERE id = $1 AND result_from IS NOT NULL",
                )
                .bind(&run_id)
                .execute(&mut *tx)
                .await?;
            }
            let payload = serde_json::json!({ "run_id": run_id, "status": status_str }).to_string();
            sqlx::query(
                "INSERT INTO event_outbox
                   (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
                 VALUES ($1, $2, 'run.completed', $3, 'pending', 0, $4, $5)",
            )
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&run_id)
            .bind(&payload)
            .bind(&now)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            finalized.push((run_id, status));
        } else {
            // Another reaper won the finalize; nothing to emit.
            tx.rollback().await?;
        }
    }
    Ok(finalized)
}

/// The workflow definition's name for a run (for lineage / display), if found.
pub async fn workflow_name_for_run(pool: &Pool, run_id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT wd.name FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

/// The original (un-expanded) spec YAML a run was created from — used by forge
/// feedback to read the run's `notify.git` block + `parameters` at finalization.
pub async fn spec_for_run(pool: &Pool, run_id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT wd.spec FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

// ── Transactional outbox: drain API (for the delivery worker) ──────────────────

/// Claim up to `limit` due, pending outbox events for delivery with
/// `FOR UPDATE SKIP LOCKED` (coordination-free, like task claiming), deferring
/// each by `lease_secs` so a concurrent worker won't grab the same event
/// mid-delivery. At-least-once: a crashed worker's lease lapses and the event is
/// re-claimed.
pub async fn claim_outbox_batch(
    pool: &Pool,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<crate::models::OutboxEvent>> {
    let now = chrono::Utc::now();
    let now_s = now.to_rfc3339();
    let lease_until = (now + chrono::TimeDelta::seconds(lease_secs)).to_rfc3339();

    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, run_id, event_type, payload, attempts FROM event_outbox
         WHERE status = 'pending' AND next_attempt_at <= $1
         ORDER BY next_attempt_at LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind(&now_s)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: String = row.try_get("id")?;
        sqlx::query("UPDATE event_outbox SET next_attempt_at = $1 WHERE id = $2")
            .bind(&lease_until)
            .bind(&id)
            .execute(&mut *tx)
            .await?;
        out.push(crate::models::OutboxEvent {
            id,
            run_id: row.try_get("run_id")?,
            event_type: row.try_get("event_type")?,
            payload: row.try_get("payload")?,
            attempts: row.try_get("attempts")?,
        });
    }
    tx.commit().await?;
    Ok(out)
}

/// Mark an outbox event delivered.
pub async fn mark_outbox_delivered(pool: &Pool, id: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("UPDATE event_outbox SET status = 'delivered', delivered_at = $1 WHERE id = $2 AND status = 'pending'")
        .bind(&now)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Delivery failed but is retryable: bump attempts, record the error, and set the
/// next eligible time (`retry_at`, caller computes the backoff).
pub async fn mark_outbox_failed(pool: &Pool, id: &str, error: &str, retry_at: &str) -> Result<()> {
    sqlx::query(
        "UPDATE event_outbox SET attempts = attempts + 1, last_error = $1, next_attempt_at = $2 WHERE id = $3 AND status = 'pending'",
    )
    .bind(error)
    .bind(retry_at)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delivery exhausted its retries: park the event as `dead` (the broker-DLQ analog).
pub async fn mark_outbox_dead(pool: &Pool, id: &str, error: &str) -> Result<()> {
    sqlx::query("UPDATE event_outbox SET status = 'dead', attempts = attempts + 1, last_error = $1 WHERE id = $2 AND status = 'pending'")
        .bind(error)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
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
         WHERE run_id = $2 AND status IN ('pending', 'ready', 'running', 'awaiting_approval')",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// Enforce run-level deadlines (spec `run_timeout_secs`): every `running` run
/// whose `deadline_at` has passed is marked **failed** (deadline exceeded is an
/// error, unlike an operator cancel) and its non-terminal tasks are cancelled,
/// mirroring [`cancel_run`]'s task semantics — leases cleared, and any executor
/// that finishes anyway is rejected by the fence guard in `mark_task_*`.
/// Idempotent by construction (the run leaves `running` in the same statement);
/// safe to call from every scheduler's reconcile tick without leadership.
/// Returns the ids of runs failed by this sweep.
pub async fn cancel_overdue_runs(pool: &Pool) -> Result<Vec<String>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let overdue: Vec<String> = sqlx::query_scalar(
        "UPDATE workflow_runs
         SET status = 'failed', finished_at = $1,
             output = 'run deadline exceeded (run_timeout_secs)'
         WHERE status = 'running' AND deadline_at IS NOT NULL AND deadline_at < $1
         RETURNING id",
    )
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;

    for run_id in &overdue {
        sqlx::query(
            "UPDATE task_runs
             SET status = 'cancelled', finished_at = $1, claimed_by = NULL, lease_expires_at = NULL
             WHERE run_id = $2 AND status IN ('pending', 'ready', 'running', 'awaiting_approval')",
        )
        .bind(&now)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        notify(&mut *tx, run_id).await?;
    }

    tx.commit().await?;
    Ok(overdue)
}

/// Emit a soft SLA deadline alert (spec `deadline`) for each still-running run
/// past its `alert_deadline_at` that hasn't alerted yet (#20). Fire-once and
/// idempotent via a guarded `RETURNING` update (winner-take-all across
/// schedulers). Does NOT cancel — appends a `run.deadline_exceeded` event to the
/// transactional outbox in the same transaction and NOTIFYs the drainer.
/// Returns the alerted run ids.
pub async fn fire_deadline_alerts(pool: &Pool) -> Result<Vec<String>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let fired: Vec<String> = sqlx::query_scalar(
        "UPDATE workflow_runs SET alert_fired_at = $1
         WHERE status = 'running' AND alert_deadline_at IS NOT NULL
           AND alert_deadline_at < $1 AND alert_fired_at IS NULL
         RETURNING id",
    )
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;

    for run_id in &fired {
        let payload = serde_json::json!({ "run_id": run_id, "reason": "deadline_exceeded" }).to_string();
        sqlx::query(
            "INSERT INTO event_outbox
               (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
             VALUES ($1, $2, 'run.deadline_exceeded', $3, 'pending', 0, $4, $5)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(run_id)
        .bind(&payload)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        notify(&mut *tx, run_id).await?;
    }

    tx.commit().await?;
    Ok(fired)
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
