//! SQLite backend (v0/v1, default). Single-writer, optimistic-concurrency claim.
//!
//! Correctness comes from CAS on `version`: this path is already safe for the
//! v2 multi-worker model, it just contends harder than the Postgres
//! `FOR UPDATE SKIP LOCKED` path. SQLite has no `LISTEN/NOTIFY`, so the
//! reconcile loop falls back to a fixed-interval timer (see [`Waker`]).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

use crate::{
    dag::DagGraph,
    models::{RunStatus, TaskRun},
};

/// Backend-agnostic pool alias; `db::Pool` resolves to this when the `sqlite`
/// feature is active.
pub type Pool = SqlitePool;

pub async fn init_pool(db_path: &str) -> Result<Pool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        // WAL lets readers and the single writer proceed concurrently instead of
        // a reader's shared lock blocking the writer (the default rollback
        // journal). Combined with the busy timeout below, incidental concurrent
        // access — an ops/admin query, a monitoring probe, a second process
        // reading the file — never stalls or crashes the reconcile loop.
        .journal_mode(SqliteJournalMode::Wal)
        // Wait out brief lock contention instead of erroring with SQLITE_BUSY.
        .busy_timeout(Duration::from_secs(5))
        .pragma("foreign_keys", "ON");

    // One connection: SQLite is single-writer, and `claim_ready` reads-then-writes
    // in a deferred transaction. With multiple pool connections that read→write
    // upgrade can lose the write lock to a sibling connection and fail *instantly*
    // with SQLITE_BUSY (a busy timeout cannot rescue a lock upgrade). Serializing
    // all access through one connection removes that race entirely; WAL still lets
    // outside readers (ops queries, probes) run without blocking the writer. The
    // Postgres backend is the path for real write concurrency.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    #[cfg(feature = "enterprise")]
    sqlx::migrate!("./migrations_ee").run(&pool).await?;
    Ok(pool)
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction. Returns the new run_id.
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
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES (?, ?, ?, ?)",
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
         VALUES (?, ?, 'running', ?, ?, ?, ?)",
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
             VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)",
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
                "INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES (?, ?)",
            )
            .bind(dependent_id)
            .bind(dependency_id)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(run_id)
}

/// Reclaim tasks whose worker lease expired — the core crash-recovery primitive.
pub async fn recover_expired_leases(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    let r = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL
         WHERE status = 'running'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < ?",
    )
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// Advance pending tasks whose dependencies are all terminal
/// (`remaining_deps == 0`): evaluate each task's `trigger_rule` against its
/// dependencies' outcomes and either flip it to `ready` (rule satisfied) or
/// `skipped` (not satisfied — e.g. an `all_success` task with a failed
/// dependency). A newly-skipped task is itself terminal, so its dependents'
/// `remaining_deps` are decremented; the resulting cascade resolves over
/// subsequent reconcile ticks. Returns the number of tasks transitioned.
///
/// Each transition is guarded by `status = 'pending'`, so concurrent schedulers
/// are winner-take-all and a skip's dependent-decrement runs exactly once.
pub async fn advance_ready_tasks(pool: &Pool) -> Result<u64> {
    // (id, trigger_rule, is_approval) for every task whose deps are all terminal.
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
             WHERE d.dependent_id = ?",
        )
        .bind(&task_id)
        .fetch_all(pool)
        .await?;

        if crate::models::trigger_rule_ready(&rule, &dep_statuses) {
            // An approval gate (#19) parks in `awaiting_approval` (never claimed by
            // a worker) instead of going `ready`; `scheduled_at` marks when it
            // began waiting so the timeout sweep can measure the deadline.
            let rows = if is_approval != 0 {
                let now = chrono::Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE task_runs SET status = 'awaiting_approval', scheduled_at = ?
                     WHERE id = ? AND status = 'pending'",
                )
                .bind(&now)
                .bind(&task_id)
                .execute(pool)
                .await?
                .rows_affected()
            } else {
                sqlx::query(
                    "UPDATE task_runs SET status = 'ready' WHERE id = ? AND status = 'pending'",
                )
                .bind(&task_id)
                .execute(pool)
                .await?
                .rows_affected()
            };
            transitioned += rows;
        } else {
            // Skip the task (rule unsatisfiable) and, if we won the transition,
            // decrement its dependents so they can advance in turn.
            let now = chrono::Utc::now().to_rfc3339();
            let mut tx = pool.begin().await?;
            let rows = sqlx::query(
                "UPDATE task_runs SET status = 'skipped', finished_at = ?
                 WHERE id = ? AND status = 'pending'",
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
                         SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
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

/// Claim up to `limit` ready tasks for `worker_id`.
///
/// Uses CAS on `version` so this is safe to call from multiple workers in v2.
/// Returns the snapshot of claimed rows (attempt is the pre-claim value).
pub async fn claim_ready(pool: &Pool, worker_id: &str, limit: i64) -> Result<Vec<TaskRun>> {
    let mut tx = pool.begin().await?;

    let now = chrono::Utc::now().to_rfc3339();
    let candidates: Vec<TaskRun> = sqlx::query_as::<_, TaskRun>(
        "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at
         FROM task_runs
         WHERE status = 'ready'
           AND (scheduled_at IS NULL OR scheduled_at <= ?)
         ORDER BY scheduled_at
         LIMIT ?",
    )
    .bind(&now)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;

    if candidates.is_empty() {
        tx.commit().await?;
        return Ok(vec![]);
    }

    let lease_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(30)).to_rfc3339();
    let mut claimed = Vec::with_capacity(candidates.len());

    for task in candidates {
        let rows = sqlx::query(
            "UPDATE task_runs
             SET status = 'running',
                 claimed_by = ?,
                 lease_expires_at = ?,
                 attempt = attempt + 1,
                 version = version + 1
             WHERE id = ? AND status = 'ready' AND version = ?",
        )
        .bind(worker_id)
        .bind(&lease_exp)
        .bind(&task.id)
        .bind(task.version)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if rows > 0 {
            claimed.push(task);
        } else {
            tracing::warn!(task_id = %task.id, "CAS miss — skipping");
        }
    }

    tx.commit().await?;
    Ok(claimed)
}

/// Mark a task succeeded and decrement remaining_deps for all direct dependents.
///
/// Guards the UPDATE with `claimed_by = worker_id AND version = fence`, where
/// `fence` is the post-claim version returned to this attempt. `claimed_by`
/// alone is insufficient — a process reuses one worker_id, so if it reclaims its
/// own expired lease the older attempt would still match. The version fence
/// pins the mutation to this exact claim, so a stale executor that finishes
/// after its lease was reclaimed (by any process, including this one) cannot
/// overwrite the newer runner or double-apply dep decrements. Returns false
/// (and logs a warning) when the fence no longer matches.
pub async fn mark_task_succeeded(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    output: Option<String>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'succeeded', finished_at = ?, output = ?, claimed_by = NULL
         WHERE id = ? AND claimed_by = ? AND version = ?",
    )
    .bind(&now)
    .bind(&output)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if rows == 0 {
        tx.commit().await?;
        tracing::warn!(task_id, "stale completion ignored — task already reclaimed");
        return Ok(false);
    }

    // Decrement remaining_deps; advance_ready_tasks will flip zeros to 'ready'.
    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

/// Mark a task failed and decrement its direct dependents' dependency counters
/// (a failure is a terminal outcome, exactly like success). Whether each
/// dependent then runs or is skipped is decided by its `trigger_rule` in
/// [`advance_ready_tasks`] — so an `all_success` dependent is skipped when this
/// task failed, while an `all_done`/`one_failed` dependent still runs.
///
/// Same stale-worker guard as mark_task_succeeded: the UPDATE requires
/// `claimed_by = worker_id AND version = fence`, so only the exact claim that
/// still owns the row can fan out side effects.
pub async fn mark_task_failed(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    error: Option<String>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'failed', finished_at = ?, output = ?, claimed_by = NULL
         WHERE id = ? AND claimed_by = ? AND version = ?",
    )
    .bind(&now)
    .bind(&error)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if rows == 0 {
        tx.commit().await?;
        tracing::warn!(task_id, "stale failure ignored — task already reclaimed");
        return Ok(false);
    }

    // A failure is terminal — decrement dependents so their trigger_rule can be
    // evaluated (mirrors mark_task_succeeded). advance_ready_tasks then skips or
    // runs each dependent per its rule.
    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;

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
pub async fn append_task_output(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    chunk: &str,
    reset: bool,
) -> Result<()> {
    let sql = if reset {
        "UPDATE task_runs SET output = ?
         WHERE id = ? AND version = ? AND status = 'running'"
    } else {
        "UPDATE task_runs SET output = COALESCE(output, '') || ?
         WHERE id = ? AND version = ? AND status = 'running'"
    };
    sqlx::query(sql)
        .bind(chunk)
        .bind(task_id)
        .bind(fence)
        .execute(pool)
        .await?;
    Ok(())
}

/// Resolve a human approval gate (#19): `approve` → the task succeeds and its
/// dependents advance; reject → it fails and its `all_success` dependents skip.
/// Guarded on `status = 'awaiting_approval'` AND the run, so a double-approve, a
/// wrong-run task id, or an already-resolved gate is a no-op. Returns whether it
/// actually resolved (false → the handler answers 404/409). The reconcile loop's
/// next `advance_ready_tasks` picks up the now-decremented dependents.
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
        "UPDATE task_runs SET status = ?, finished_at = ?, output = ?
         WHERE id = ? AND run_id = ? AND status = 'awaiting_approval'",
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
    // Terminal transition → decrement dependents so their trigger_rule evaluates.
    sqlx::query(
        "UPDATE task_runs SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Auto-resolve approval gates whose `approval_timeout_secs` elapsed since they
/// began waiting (`scheduled_at`), applying `approval_on_timeout` (default
/// `reject` — a gate fails safe). Returns the `(task_id, approved)` decisions.
/// Idempotent: `resolve_approval`'s guard means an already-resolved gate is a
/// no-op, so a re-sweep does nothing.
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
            continue; // no start marker → can't measure the deadline
        };
        let deadline = started.with_timezone(&chrono::Utc)
            + chrono::TimeDelta::seconds(timeout);
        if now < deadline {
            continue;
        }
        let approve = on_timeout.as_deref() == Some("approve"); // default: reject
        if resolve_approval(pool, &run_id, &id, approve).await? {
            resolved.push((id, approve));
        }
    }
    Ok(resolved)
}

/// Reset a failed task to `ready` for a later retry attempt.
///
/// Sets `scheduled_at` to `retry_at` (a future RFC-3339 timestamp) so that
/// `claim_ready` will not pick it up until the backoff window has elapsed.
/// The `attempt` counter is NOT touched here — it is incremented by `claim_ready`
/// at the next claim, preserving the monotonic increment invariant.
/// Guards with `claimed_by = worker_id AND version = fence` to reject stale
/// retries from reclaimed workers.
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
             scheduled_at = ?,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = ?
         WHERE id = ? AND claimed_by = ? AND version = ?",
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
/// was not in a retryable (`failed`/`cancelled`) state. No NOTIFY (SQLite).
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
         WHERE id = ? AND status IN ('failed', 'cancelled')",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if updated == 0 {
        tx.commit().await?;
        return Ok(false);
    }

    // Re-arm a run that was already finalized failed so the reconcile loop re-engages.
    let run_id: String =
        sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = ?")
            .bind(task_id)
            .fetch_one(&mut *tx)
            .await?;
    sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL
         WHERE id = ? AND status = 'failed'",
    )
    .bind(&run_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

/// Cascade rerun-from-failed: resurrect every failed/cancelled task in a run and
/// re-arm the run so the reconcile loop resumes from the failure frontier, while
/// every already-`succeeded` task is left intact.
///
/// This is the mirror image of [`mark_task_failed`]'s downstream-cancel: because a
/// terminal run's broken cone is exactly its `failed` + `cancelled` tasks,
/// resetting that whole set and recomputing each row's `remaining_deps` from the
/// still-unsatisfied dependencies reproduces the original ready-frontier. The
/// recompute is order-independent — a reset task counts a dependency as
/// outstanding unless it is `succeeded`/`skipped`, and reset rows transition
/// failed/cancelled → pending (both "not succeeded"), so the count is identical
/// however the single UPDATE visits rows.
///
/// Returns `None` when the run does not exist, is not in a rerunnable
/// (`failed`/`cancelled`) state, or loses a concurrent-rerun race; otherwise
/// `Some(n)` with the number of tasks reset. `version` is bumped on every reset
/// row to fence any late stale worker. No NOTIFY (SQLite); the reconcile poll
/// picks the work up.
#[cfg(feature = "ops")]
pub async fn rerun_from_failed(pool: &Pool, run_id: &str) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    // Re-arm the run as the atomic gate: only the tx that flips the run
    // failed/cancelled → running proceeds to reset its tasks. A miss (run absent,
    // not rerunnable, or a lost race) → `None`, so no caller reports a false
    // success. SQLite serializes writers, so the loser sees 'running' here.
    let armed = sqlx::query(
        "UPDATE workflow_runs
         SET status = 'running', finished_at = NULL, output = NULL
         WHERE id = ? AND status IN ('failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if armed == 0 {
        tx.commit().await?;
        return Ok(None);
    }

    // Reset the broken cone: failed, cancelled, and rule-skipped tasks (a task
    // skipped because a dependency failed should get another chance on rerun).
    // Done in two statements so the remaining_deps recompute below sees the
    // *post-reset* statuses — a skipped→pending row must be counted as
    // outstanding, which a single self-referential UPDATE cannot guarantee.
    let reset = sqlx::query(
        "UPDATE task_runs
         SET status = 'pending',
             attempt = 0,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = NULL,
             finished_at = NULL,
             scheduled_at = ?,
             version = version + 1
         WHERE run_id = ? AND status IN ('failed', 'cancelled', 'skipped')",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // Recompute remaining_deps for the reset frontier from the now-committed
    // statuses: a dependency counts as outstanding unless it is
    // `succeeded`/`skipped`. Succeeded upstreams stay satisfied (so the frontier
    // becomes ready at once); reset dependencies are `pending` again, so tasks
    // behind them wait for the normal decrement in `mark_task_*`.
    sqlx::query(
        "UPDATE task_runs
         SET remaining_deps = (
                 SELECT COUNT(*) FROM task_dependencies d
                 JOIN task_runs dep ON dep.id = d.dependency_id
                 WHERE d.dependent_id = task_runs.id
                   AND dep.status NOT IN ('succeeded', 'skipped')
             )
         WHERE run_id = ? AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(Some(reset))
}

/// Whether a task with `task_id` exists within `run_id`. Used by the ops API to
/// tell an unknown-task `404` apart from a not-clearable `409` on the error path.
#[cfg(feature = "ops")]
pub async fn task_exists(pool: &Pool, run_id: &str, task_id: &str) -> Result<bool> {
    let found: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM task_runs WHERE id = ? AND run_id = ?")
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
/// a terminal state (a running/pending task can't be cleared — `409`).
#[cfg(feature = "ops")]
pub async fn clear_task_with_downstream(
    pool: &Pool,
    run_id: &str,
    task_id: &str,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM task_runs WHERE id = ? AND run_id = ?")
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
             SELECT id FROM task_runs WHERE id = ? AND run_id = ?
             UNION
             SELECT td.dependent_id FROM task_dependencies td
             JOIN cone c ON td.dependency_id = c.id
         )
         UPDATE task_runs
         SET status = 'pending', attempt = 0, claimed_by = NULL, lease_expires_at = NULL,
             output = NULL, finished_at = NULL, scheduled_at = ?, version = version + 1
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
         WHERE run_id = ? AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    // Re-arm a run that had finished so the reconcile loop resumes.
    sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL, output = NULL
         WHERE id = ? AND status IN ('succeeded', 'failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(Some(reset))
}

/// Enabled schedules whose `next_fire_at` is due (v7 UI). Joined to the workflow
/// for its spec. Only the leadership holder calls this (see `schedule.rs`).
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
           AND s.next_fire_at <= ?",
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
        "UPDATE schedules SET next_fire_at = ?, last_fired_at = ?, updated_at = ? WHERE id = ?",
    )
    .bind(next_fire_at)
    .bind(fired_at)
    .bind(fired_at)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Advance only `next_fire_at` (not `last_fired_at`) — used when a `when:` gate
/// skips a fire: the slot is consumed so the schedule doesn't re-evaluate it,
/// but `last_fired_at` stays put because nothing actually fired.
#[cfg(feature = "ops")]
pub async fn advance_schedule_gated(pool: &Pool, id: &str, next_fire_at: &str, now: &str) -> Result<()> {
    sqlx::query("UPDATE schedules SET next_fire_at = ?, updated_at = ? WHERE id = ?")
        .bind(next_fire_at)
        .bind(now)
        .bind(id)
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
            COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0) AS succeeded,
            COALESCE(SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END), 0) AS failed,
            COUNT(*) AS total
         FROM workflow_runs WHERE schedule_id = ?",
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
    sqlx::query("UPDATE workflow_runs SET schedule_id = ? WHERE id = ?")
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
         SET enabled = 0, stopped_at = ?, stop_reason = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(now)
    .bind(reason)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

// ── QW3-catchup automatic backfill & self-healing ───────────────────────────────────
//
// The QW3 backfill (dagron-api `POST /schedules/:id/backfill`) is operator-driven;
// these power the engine's leadership-gated auto-backfill loop (`backfill.rs`),
// which (1) catches a schedule up after a downtime/leadership gap and (2) reruns
// terminally-failed runs from their failure frontier — both bounded, both emitting
// to the existing transactional outbox so the action is observable downstream.

/// Schedules opted into automatic catch-up. Joined to the workflow for its spec;
/// carries `last_fired_at` (the catch-up lower bound) and the per-schedule
/// window/cap overrides. Only the leadership holder calls this.
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

/// Claim one backfill slot `(schedule_id, logical_date)` in the dedup ledger.
/// Returns `true` only when this call newly inserted the row — a slot a prior
/// (manual or automatic) backfill already materialized returns `false`, so a
/// re-sweep of the same window never double-runs it. The composite PK is the gate.
#[cfg(feature = "ops")]
pub async fn claim_backfill_slot(
    pool: &Pool,
    schedule_id: &str,
    logical_date: &str,
    now: &str,
) -> Result<bool> {
    let n = sqlx::query(
        "INSERT INTO schedule_backfills (schedule_id, logical_date, created_at)
         VALUES (?, ?, ?) ON CONFLICT (schedule_id, logical_date) DO NOTHING",
    )
    .bind(schedule_id)
    .bind(logical_date)
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// Record which run filled a claimed slot (best-effort; the slot is already held).
#[cfg(feature = "ops")]
pub async fn record_backfill_run(
    pool: &Pool,
    schedule_id: &str,
    logical_date: &str,
    run_id: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE schedule_backfills SET run_id = ? WHERE schedule_id = ? AND logical_date = ?",
    )
    .bind(run_id)
    .bind(schedule_id)
    .bind(logical_date)
    .execute(pool)
    .await?;
    Ok(())
}

/// Release a claimed slot whose `create_run` failed, so the next sweep can retry
/// it instead of counting it permanently materialized.
#[cfg(feature = "ops")]
pub async fn release_backfill_slot(pool: &Pool, schedule_id: &str, logical_date: &str) -> Result<()> {
    sqlx::query("DELETE FROM schedule_backfills WHERE schedule_id = ? AND logical_date = ?")
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
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
         WHERE (?1 IS NULL OR schedule_id = ?1)
         ORDER BY created_at DESC LIMIT ?2",
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
         FROM backfills WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Advance a job's cursor and set its absolute `fired` count after a pacing tick.
#[cfg(feature = "ops")]
pub async fn advance_backfill(pool: &Pool, id: &str, cursor: &str, fired: i64, now: &str) -> Result<()> {
    sqlx::query("UPDATE backfills SET cursor = ?, fired = ?, updated_at = ? WHERE id = ?")
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
    sqlx::query("UPDATE backfills SET status = 'completed', updated_at = ? WHERE id = ? AND status = 'running'")
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
    let n = sqlx::query("UPDATE backfills SET status = 'cancelled', updated_at = ? WHERE id = ? AND status = 'running'")
        .bind(now)
        .bind(id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

/// Terminally-`failed` runs eligible for an automatic rerun: under the per-run
/// attempt cap and past the cooldown since their last auto-rerun. The LEFT JOIN to
/// `run_reruns` treats a run never auto-rerun (no ledger row) as `attempts = 0`
/// with no cooldown. Newest failures first; bounded by `limit`.
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
           AND COALESCE(rr.attempts, 0) < ?
           AND (rr.last_rerun_at IS NULL OR rr.last_rerun_at < ?)
         ORDER BY wr.finished_at DESC
         LIMIT ?",
    )
    .bind(max_attempts)
    .bind(cooldown_cutoff)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(ids.into_iter().map(|(id,)| id).collect())
}

/// Record one auto-rerun attempt against a run (upsert: first attempt inserts,
/// subsequent attempts increment). Bounds the self-healing loop so a
/// deterministically-failing DAG cannot be re-armed forever.
#[cfg(feature = "enterprise")]
pub async fn bump_rerun_attempt(pool: &Pool, run_id: &str, now: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO run_reruns (run_id, attempts, last_rerun_at)
         VALUES (?, 1, ?)
         ON CONFLICT(run_id) DO UPDATE SET
             attempts = attempts + 1,
             last_rerun_at = excluded.last_rerun_at",
    )
    .bind(run_id)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Count runs still `running` whose `created_at` predates `stall_cutoff` — the
/// suspected-incomplete population surfaced as the `scheduler_incomplete_runs`
/// gauge (a stall-SLA alerting signal, not an auto-action).
#[cfg(feature = "enterprise")]
pub async fn count_incomplete_runs(pool: &Pool, stall_cutoff: &str) -> Result<i64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running' AND created_at < ?",
    )
    .bind(stall_cutoff)
    .fetch_one(pool)
    .await?;
    Ok(n)
}

/// Append a `pending` event to the transactional outbox out-of-band (i.e. not
/// inside a run-finalization transaction). The auto-backfill loop and the
/// backfill-job pacer (#18) use this to make each catch-up / backfill action
/// deliverable to the same drain worker that ships `run.completed` — so the
/// self-healing / backfill actions are observable downstream.
#[cfg(feature = "ops")]
pub async fn enqueue_outbox_event(
    pool: &Pool,
    run_id: &str,
    event_type: &str,
    payload: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO event_outbox
           (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
         VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(run_id)
    .bind(event_type)
    .bind(payload)
    .bind(&now)
    .bind(&now)
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
    #[derive(sqlx::FromRow)]
    struct Row {
        run_id: String,
        status: String,
        allow_failure: i64,
        cnt: i64,
    }

    // Group by (status, allow_failure) so a `failed` task with allow_failure=1
    // counts as terminal but does not fail the run (fast-win #11).
    let rows: Vec<Row> = sqlx::query_as::<_, Row>(
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
    for row in rows {
        let agg = runs.entry(row.run_id).or_insert(Agg { total: 0, terminal: 0, failed: 0 });
        agg.total += row.cnt;
        match row.status.as_str() {
            "succeeded" | "skipped" | "cancelled" => agg.terminal += row.cnt,
            "failed" => {
                agg.terminal += row.cnt;
                if row.allow_failure == 0 {
                    agg.failed += row.cnt;
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
            "UPDATE workflow_runs SET status = ?, finished_at = ? WHERE id = ? AND status = 'running'",
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
                     WHERE id = ? AND result_from IS NOT NULL",
                )
                .bind(&run_id)
                .execute(&mut *tx)
                .await?;
            }
            let payload = serde_json::json!({ "run_id": run_id, "status": status_str }).to_string();
            sqlx::query(
                "INSERT INTO event_outbox
                   (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
                 VALUES (?, ?, 'run.completed', ?, 'pending', 0, ?, ?)",
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
         WHERE wr.id = ?",
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
         WHERE wr.id = ?",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.0))
}

// ── Transactional outbox: drain API (for the delivery worker) ──────────────────

/// Claim up to `limit` due, pending outbox events for delivery, deferring each by
/// `lease_secs` (bump `next_attempt_at`) so a concurrent worker won't grab the
/// same event mid-delivery. At-least-once: a worker that dies after claiming but
/// before marking simply lets the lease lapse and the event is re-claimed.
pub async fn claim_outbox_batch(
    pool: &Pool,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<crate::models::OutboxEvent>> {
    let now = chrono::Utc::now();
    let now_s = now.to_rfc3339();
    let lease_until = (now + chrono::TimeDelta::seconds(lease_secs)).to_rfc3339();

    #[derive(sqlx::FromRow)]
    struct Row {
        id: String,
        run_id: String,
        event_type: String,
        payload: String,
        attempts: i64,
    }

    let mut tx = pool.begin().await?;
    let rows: Vec<Row> = sqlx::query_as::<_, Row>(
        "SELECT id, run_id, event_type, payload, attempts FROM event_outbox
         WHERE status = 'pending' AND next_attempt_at <= ?
         ORDER BY next_attempt_at LIMIT ?",
    )
    .bind(&now_s)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    for r in &rows {
        sqlx::query("UPDATE event_outbox SET next_attempt_at = ? WHERE id = ?")
            .bind(&lease_until)
            .bind(&r.id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(rows
        .into_iter()
        .map(|r| crate::models::OutboxEvent {
            id: r.id,
            run_id: r.run_id,
            event_type: r.event_type,
            payload: r.payload,
            attempts: r.attempts,
        })
        .collect())
}

/// Mark an outbox event delivered.
pub async fn mark_outbox_delivered(pool: &Pool, id: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query("UPDATE event_outbox SET status = 'delivered', delivered_at = ? WHERE id = ? AND status = 'pending'")
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
        "UPDATE event_outbox SET attempts = attempts + 1, last_error = ?, next_attempt_at = ? WHERE id = ? AND status = 'pending'",
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
    sqlx::query("UPDATE event_outbox SET status = 'dead', attempts = attempts + 1, last_error = ? WHERE id = ? AND status = 'pending'")
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
    #[derive(sqlx::FromRow)]
    struct Row {
        status: String,
        cnt: i64,
    }

    let rows: Vec<Row> = sqlx::query_as::<_, Row>(
        "SELECT status, COUNT(*) as cnt FROM task_runs WHERE run_id = ? GROUP BY status",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    let mut total: i64 = 0;
    let mut terminal: i64 = 0;
    let mut failed: i64 = 0;

    for row in &rows {
        total += row.cnt;
        match row.status.as_str() {
            "succeeded" | "skipped" | "cancelled" => terminal += row.cnt,
            "failed" => {
                failed += row.cnt;
                terminal += row.cnt;
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

    sqlx::query("UPDATE workflow_runs SET status = ?, finished_at = ? WHERE id = ?")
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
/// too. `failures` is how many times the ingest actor tried before giving up.
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
         VALUES (?, ?, ?, ?, ?, ?, ?)",
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
         FROM dead_letters ORDER BY first_seen_at DESC LIMIT ?",
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
         FROM dead_letters WHERE id = ?",
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
    let n = sqlx::query("DELETE FROM dead_letters WHERE id = ?")
        .bind(id)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

// ── v5 management API reads ─────────────────────────────────────────────────

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
            sqlx::query_as::<_, RunSummary>(&format!(
                "{base} WHERE wr.status = ? ORDER BY wr.created_at DESC LIMIT ?"
            ))
            .bind(s)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, RunSummary>(&format!(
                "{base} ORDER BY wr.created_at DESC LIMIT ?"
            ))
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    Ok(rows)
}

/// Fetch one run by id (or `None`). Backs `GET /runs/:id`.
#[cfg(feature = "ops")]
pub async fn get_run(pool: &Pool, run_id: &str) -> Result<Option<crate::models::WorkflowRun>> {
    use crate::models::WorkflowRun;
    let row = sqlx::query_as::<_, WorkflowRun>(
        "SELECT id, definition_id, status, input, output, created_at, finished_at
         FROM workflow_runs WHERE id = ?",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// All task rows of a run, ordered by name. Backs `GET /runs/:id`.
#[cfg(feature = "ops")]
pub async fn list_tasks(pool: &Pool, run_id: &str) -> Result<Vec<TaskRun>> {
    let rows = sqlx::query_as::<_, TaskRun>(
        "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at
         FROM task_runs WHERE run_id = ? ORDER BY name",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Cancel a still-running run: every non-terminal task → `cancelled`, the run row
/// → `cancelled`. Idempotent — a second call (or a run already terminal) returns
/// `false`. Backs `POST /runs/:id/cancel`. A `running` task's lease is also
/// cleared; if its executor finishes anyway the fence guard in `mark_task_*`
/// rejects the stale write, so cancellation cannot be clobbered.
#[cfg(feature = "ops")]
pub async fn cancel_run(pool: &Pool, run_id: &str) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let run_rows = sqlx::query(
        "UPDATE workflow_runs SET status = 'cancelled', finished_at = ?
         WHERE id = ? AND status = 'running'",
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
         SET status = 'cancelled', finished_at = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE run_id = ? AND status IN ('pending', 'ready', 'running', 'awaiting_approval')",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

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
         SET status = 'failed', finished_at = ?,
             output = 'run deadline exceeded (run_timeout_secs)'
         WHERE status = 'running' AND deadline_at IS NOT NULL AND deadline_at < ?
         RETURNING id",
    )
    .bind(&now)
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;

    for run_id in &overdue {
        sqlx::query(
            "UPDATE task_runs
             SET status = 'cancelled', finished_at = ?, claimed_by = NULL, lease_expires_at = NULL
             WHERE run_id = ? AND status IN ('pending', 'ready', 'running', 'awaiting_approval')",
        )
        .bind(&now)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(overdue)
}

/// Emit a soft SLA deadline alert (spec `deadline`) for each still-running run
/// past its `alert_deadline_at` that hasn't alerted yet (#20). Fire-once and
/// idempotent: the guarded UPDATE stamps `alert_fired_at` and `RETURNING` makes
/// it winner-take-all across schedulers, so each run alerts exactly once. Unlike
/// the run-timeout sweep this does NOT cancel — it appends a
/// `run.deadline_exceeded` event to the transactional outbox (drained by the
/// outbox delivery worker) in the same transaction. Returns the alerted run ids.
pub async fn fire_deadline_alerts(pool: &Pool) -> Result<Vec<String>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let fired: Vec<String> = sqlx::query_scalar(
        "UPDATE workflow_runs SET alert_fired_at = ?
         WHERE status = 'running' AND alert_deadline_at IS NOT NULL
           AND alert_deadline_at < ? AND alert_fired_at IS NULL
         RETURNING id",
    )
    .bind(&now)
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;

    for run_id in &fired {
        let payload = serde_json::json!({ "run_id": run_id, "reason": "deadline_exceeded" }).to_string();
        sqlx::query(
            "INSERT INTO event_outbox
               (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
             VALUES (?, ?, 'run.deadline_exceeded', ?, 'pending', 0, ?, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(run_id)
        .bind(&payload)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
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

    // Children first to respect the FK edges (dependencies → tasks → run).
    sqlx::query(
        "DELETE FROM task_dependencies
         WHERE dependent_id IN (
             SELECT tr.id FROM task_runs tr
             JOIN workflow_runs wr ON wr.id = tr.run_id
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < ?
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
               AND finished_at IS NOT NULL AND finished_at < ?
         )",
    )
    .bind(cutoff)
    .execute(&mut *tx)
    .await?;

    let deleted = sqlx::query(
        "DELETE FROM workflow_runs
         WHERE status IN ('succeeded','failed','cancelled')
           AND finished_at IS NOT NULL AND finished_at < ?",
    )
    .bind(cutoff)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    // Drop definitions no run references any more.
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
/// The same lease-is-the-truth pattern as task recovery: one `leader_election`
/// row per role. The caller wins iff the row is absent, already held by it, or
/// its current lease has expired — all decided atomically in a single
/// `INSERT … ON CONFLICT DO UPDATE … WHERE`. Returns `true` while this `holder`
/// owns the role. Renewing is the same call (the `holder = excluded.holder`
/// branch), so the ops loop just calls this on a timer.
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
         VALUES (?, ?, ?)
         ON CONFLICT(role) DO UPDATE SET
             holder = excluded.holder,
             lease_expires_at = excluded.lease_expires_at
         WHERE leader_election.holder = excluded.holder
            OR leader_election.lease_expires_at < ?",
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

/// Reconcile-loop waker.
///
/// SQLite has no `LISTEN/NOTIFY`, so the wake strategy is a plain fixed-interval
/// timer. The Postgres backend swaps this for an event-driven listener that wakes
/// the loop the instant any worker changes task readiness (see `db::postgres`).
pub struct Waker;

impl Waker {
    pub async fn connect(_pool: &Pool) -> Result<Self> {
        Ok(Self)
    }

    /// Sleep for the full poll interval — there is no early-wake source.
    pub async fn wait(&mut self, interval: Duration) -> Result<()> {
        tokio::time::sleep(interval).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagGraph;

    /// Per-test SQLite database in a unique temp file (a pool can't share one
    /// `:memory:` db across its connections, so a file is the simplest fixture).
    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_test_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    /// The whole point of the fencing token: a stale attempt whose lease was
    /// reclaimed (so the row's version moved on) must NOT be able to mark the
    /// row, while the current attempt (matching version) still can.
    #[tokio::test]
    async fn stale_fence_is_rejected() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "worker-A", 10).await.unwrap();
        assert_eq!(claimed.len(), 1, "the single root task should be claimed");
        let task = &claimed[0];
        let fence = task.version + 1; // post-claim version handed to this attempt

        // Simulate the same worker reclaiming its own expired lease: the row's
        // version advances past the stale attempt's fence.
        sqlx::query("UPDATE task_runs SET version = version + 1 WHERE id = ?")
            .bind(&task.id)
            .execute(&pool)
            .await
            .unwrap();

        // Stale attempt (old fence) is fenced off.
        let stale = mark_task_succeeded(&pool, &task.id, "worker-A", fence, Some("stale".into()))
            .await
            .unwrap();
        assert!(!stale, "stale fence must be rejected");

        // Current attempt (matching fence) wins.
        let current = mark_task_succeeded(&pool, &task.id, "worker-A", fence + 1, Some("ok".into()))
            .await
            .unwrap();
        assert!(current, "current fence must be accepted");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Run-level deadline: create_run persists `deadline_at` from the spec's
    /// `run_timeout_secs`, and the sweep fails an overdue run (tasks cancelled)
    /// while leaving on-time and deadline-free runs alone. Idempotent: a second
    /// sweep finds nothing.
    #[tokio::test]
    async fn run_deadline_sweep_fails_overdue_run() {
        let (pool, path) = temp_pool().await;

        let deadlined = "name: slow\nrun_timeout_secs: 3600\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let free = "name: free\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let overdue_run =
            create_run(&pool, &DagGraph::from_yaml(deadlined).unwrap(), deadlined).await.unwrap();
        let ontime_run =
            create_run(&pool, &DagGraph::from_yaml(deadlined).unwrap(), deadlined).await.unwrap();
        let free_run = create_run(&pool, &DagGraph::from_yaml(free).unwrap(), free).await.unwrap();

        // create_run stamped a deadline exactly when the spec asked for one.
        let dl: Option<String> =
            sqlx::query_scalar("SELECT deadline_at FROM workflow_runs WHERE id = ?")
                .bind(&overdue_run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(dl.is_some(), "run_timeout_secs must persist a deadline_at");
        let dl_free: Option<String> =
            sqlx::query_scalar("SELECT deadline_at FROM workflow_runs WHERE id = ?")
                .bind(&free_run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(dl_free.is_none(), "no run_timeout_secs → no deadline");

        // Backdate one run past its budget (avoids sleeping in the test).
        let past = (chrono::Utc::now() - chrono::TimeDelta::seconds(5)).to_rfc3339();
        sqlx::query("UPDATE workflow_runs SET deadline_at = ? WHERE id = ?")
            .bind(&past)
            .bind(&overdue_run)
            .execute(&pool)
            .await
            .unwrap();

        let swept = cancel_overdue_runs(&pool).await.unwrap();
        assert_eq!(swept, vec![overdue_run.clone()]);

        let (run_status, run_output): (String, Option<String>) =
            sqlx::query_as("SELECT status, output FROM workflow_runs WHERE id = ?")
                .bind(&overdue_run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(run_status, "failed");
        assert!(run_output.unwrap().contains("deadline exceeded"));
        let task_status: String =
            sqlx::query_scalar("SELECT status FROM task_runs WHERE run_id = ?")
                .bind(&overdue_run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(task_status, "cancelled");

        // On-time and deadline-free runs untouched; sweep is idempotent.
        for id in [&ontime_run, &free_run] {
            let status: String =
                sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = ?")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(status, "running", "run {id} must be untouched");
        }
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Cron `when`/`stopStrategy` support (fast-win #7): a run stamped with its
    /// schedule is counted by outcome via `schedule_run_counts`, and
    /// `stop_schedule` disables the row while recording why.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn schedule_stop_strategy_counts_and_stops() {
        let (pool, path) = temp_pool().await;
        let now = chrono::Utc::now().to_rfc3339();

        // A schedule needs a workflows row (FK). Insert both directly.
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ('wf1','w','name: w\ntasks: []',?,?)")
            .bind(&now).bind(&now).execute(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO schedules (id, workflow_id, cron_expr, enabled, next_fire_at, created_at, updated_at)
             VALUES ('sch1','wf1','0 0 * * * *',1,?,?,?)",
        )
        .bind(&now).bind(&now).bind(&now).execute(&pool).await.unwrap();

        // Three runs; stamp them with the schedule, then set terminal statuses:
        // 2 succeeded, 1 failed.
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let rid = create_run(&pool, &dag, yaml).await.unwrap();
            stamp_run_schedule(&pool, &rid, "sch1").await.unwrap();
            ids.push(rid);
        }
        for (i, rid) in ids.iter().enumerate() {
            let status = if i < 2 { "succeeded" } else { "failed" };
            sqlx::query("UPDATE workflow_runs SET status = ? WHERE id = ?")
                .bind(status).bind(rid).execute(&pool).await.unwrap();
        }

        // Counts reflect the stamped outcomes; an unrelated schedule sees none.
        assert_eq!(schedule_run_counts(&pool, "sch1").await.unwrap(), (2, 1, 3));
        assert_eq!(schedule_run_counts(&pool, "other").await.unwrap(), (0, 0, 0));

        // Auto-stop: disables the row and records the reason; a subsequent
        // claim_due_schedules must not return it (enabled = 0).
        stop_schedule(&pool, "sch1", "{{ failed }} >= 1 (failed=1)", &now).await.unwrap();
        let (enabled, stopped_at, stop_reason): (i64, Option<String>, Option<String>) =
            sqlx::query_as("SELECT enabled, stopped_at, stop_reason FROM schedules WHERE id = 'sch1'")
                .fetch_one(&pool).await.unwrap();
        assert_eq!(enabled, 0);
        assert!(stopped_at.is_some());
        assert!(stop_reason.unwrap().contains("failed"));
        let due = claim_due_schedules(&pool, &chrono::Utc::now().to_rfc3339()).await.unwrap();
        assert!(due.iter().all(|s| s.id != "sch1"), "stopped schedule must not be due");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Trigger rules (#10): when a dependency fails, `advance_ready_tasks`
    /// routes each dependent by its rule — an `all_success` dep is skipped (not
    /// cancelled), `all_done`/`one_failed` run, `none_failed` skips — and the
    /// skip cascades to further-downstream `all_success` tasks.
    #[tokio::test]
    async fn trigger_rules_route_around_a_failed_dep() {
        use crate::models::TaskStatus;
        let (pool, path) = temp_pool().await;
        let yaml = "name: tr\ntasks:\n  \
            - { name: a, command: [\"false\"] }\n  \
            - { name: b, command: [\"true\"], depends_on: [a] }\n  \
            - { name: c, command: [\"true\"], depends_on: [a], trigger_rule: all_done }\n  \
            - { name: d, command: [\"true\"], depends_on: [a], trigger_rule: one_failed }\n  \
            - { name: e, command: [\"true\"], depends_on: [a], trigger_rule: none_failed }\n  \
            - { name: f, command: [\"true\"], depends_on: [b] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        // a is the only root → ready; claim and fail it.
        advance_ready_tasks(&pool).await.unwrap();
        let a = claim_ready(&pool, "w", 10).await.unwrap().into_iter().find(|t| t.name == "a").unwrap();
        assert!(mark_task_failed(&pool, &a.id, "w", a.version + 1, Some("boom".into())).await.unwrap());

        // Let the rule evaluation + skip-cascade settle over a few ticks.
        for _ in 0..4 {
            advance_ready_tasks(&pool).await.unwrap();
        }

        let by: std::collections::HashMap<String, TaskRun> =
            list_tasks(&pool, &run_id).await.unwrap().into_iter().map(|t| (t.name.clone(), t)).collect();
        assert_eq!(by["a"].status, TaskStatus::Failed);
        assert_eq!(by["b"].status, TaskStatus::Skipped, "all_success dep failed → skipped, not cancelled");
        assert_eq!(by["c"].status, TaskStatus::Ready, "all_done runs regardless");
        assert_eq!(by["d"].status, TaskStatus::Ready, "one_failed runs because a failed");
        assert_eq!(by["e"].status, TaskStatus::Skipped, "none_failed skips because a failed");
        assert_eq!(by["f"].status, TaskStatus::Skipped, "skip of b cascades to its all_success dependent f");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Deadline alerts (#20): a still-running run past its `alert_deadline_at`
    /// gets exactly one `run.deadline_exceeded` outbox event; the run is NOT
    /// cancelled, and a second sweep is a no-op.
    #[tokio::test]
    async fn deadline_alert_fires_once_without_cancelling() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: slo\ndeadline: { in: 1h }\ntasks:\n  - { name: a, command: [\"true\"] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        // create_run stamped an alert deadline from the spec.
        let dl: Option<String> =
            sqlx::query_scalar("SELECT alert_deadline_at FROM workflow_runs WHERE id = ?")
                .bind(&run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(dl.is_some());
        // Backdate it into the past.
        sqlx::query("UPDATE workflow_runs SET alert_deadline_at = ? WHERE id = ?")
            .bind((chrono::Utc::now() - chrono::TimeDelta::seconds(5)).to_rfc3339())
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(fire_deadline_alerts(&pool).await.unwrap(), vec![run_id.clone()]);
        // The run keeps running (not cancelled/failed).
        let status: String = sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = ?")
            .bind(&run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "running");
        // Exactly one outbox event of the alert type.
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_outbox WHERE run_id = ? AND event_type = 'run.deadline_exceeded'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(n, 1);
        // Fire-once: a second sweep alerts nobody.
        assert!(fire_deadline_alerts(&pool).await.unwrap().is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `allow_failure` (#11): a task that fails with allow_failure=1 is terminal
    /// but does not fail the run, so reap finalizes the run as `succeeded`.
    #[tokio::test]
    async fn allow_failure_task_does_not_fail_the_run() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: opt\ntasks:\n  \
            - { name: main, command: [\"true\"] }\n  \
            - { name: best_effort, command: [\"false\"], allow_failure: true }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        // Drive both to terminal: main succeeds, best_effort fails.
        advance_ready_tasks(&pool).await.unwrap();
        for t in claim_ready(&pool, "w", 10).await.unwrap() {
            if t.name == "main" {
                mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None).await.unwrap();
            } else {
                mark_task_failed(&pool, &t.id, "w", t.version + 1, Some("boom".into())).await.unwrap();
            }
        }

        let finalized = reap_completed_runs(&pool).await.unwrap();
        assert_eq!(finalized.len(), 1);
        // The run succeeds despite best_effort failing (allow_failure ignored it).
        assert_eq!(finalized[0].1.to_string(), "succeeded");
        // …but the task itself still records `failed`.
        let st: String = sqlx::query_scalar("SELECT status FROM task_runs WHERE run_id = ? AND name = 'best_effort'")
            .bind(&run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(st, "failed");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Live-log append (#17): reset then append chunks build the running task's
    /// output; a stale fence and a terminal row are both refused.
    #[tokio::test]
    async fn append_task_output_streams_then_guards() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let task = &claimed[0]; // status=running; claim bumped version to +1
        let fence = task.version + 1; // post-claim version = the attempt's fence

        // First chunk resets, subsequent chunks append.
        append_task_output(&pool, &task.id, fence, "hello\n", true).await.unwrap();
        append_task_output(&pool, &task.id, fence, "world\n", false).await.unwrap();
        let out: Option<String> = sqlx::query_scalar("SELECT output FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("hello\nworld\n"));

        // A stale fence (wrong version) writes nothing.
        append_task_output(&pool, &task.id, fence + 5, "STALE\n", false).await.unwrap();
        let out: Option<String> = sqlx::query_scalar("SELECT output FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("hello\nworld\n"), "stale fence must not append");

        // A terminal task refuses appends (guarded on status='running').
        assert!(mark_task_succeeded(&pool, &task.id, "w", fence, Some("final".into())).await.unwrap());
        append_task_output(&pool, &task.id, fence, "LATE\n", false).await.unwrap();
        let out: Option<String> = sqlx::query_scalar("SELECT output FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("final"), "terminal row is immutable to appends");

        let _ = run_id;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Run result (#15): create_run stamps `result_from`, and on a successful
    /// reap the run's `output` is copied from the named task's output — so a
    /// waiting caller gets a single return value.
    #[tokio::test]
    async fn result_from_populates_run_output_on_success() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: fn\nresult_from: b\ntasks:\n  \
            - { name: a, command: [\"true\"] }\n  \
            - { name: b, command: [\"true\"], depends_on: [\"a\"] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        // create_run persisted the result_from marker.
        let rf: Option<String> =
            sqlx::query_scalar("SELECT result_from FROM workflow_runs WHERE id = ?")
                .bind(&run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rf.as_deref(), Some("b"));

        // Drive a → b to success; b returns the run's "result". Two rounds: `a`
        // becomes ready first, then `b` once `a` succeeds.
        for _round in 0..2 {
            advance_ready_tasks(&pool).await.unwrap();
            for t in claim_ready(&pool, "w", 10).await.unwrap() {
                let out = if t.name == "b" { Some("the-answer".to_string()) } else { None };
                mark_task_succeeded(&pool, &t.id, "w", t.version + 1, out).await.unwrap();
            }
        }

        let finalized = reap_completed_runs(&pool).await.unwrap();
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].1.to_string(), "succeeded");

        // The run's output is now the result_from task's output.
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.output.as_deref(), Some("the-answer"));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Transactional outbox: finalizing a run emits exactly one pending
    /// `run.completed` event (atomic with the finalize), and the drain API can
    /// claim → deliver it, after which it is not re-claimed.
    #[tokio::test]
    async fn reap_emits_outbox_event_then_drains() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: ob\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let task = &claimed[0];
        assert!(mark_task_succeeded(&pool, &task.id, "w", task.version + 1, None)
            .await
            .unwrap());

        let finalized = reap_completed_runs(&pool).await.unwrap();
        assert_eq!(finalized.len(), 1, "the run finalizes");

        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_outbox WHERE run_id = ? AND status = 'pending'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(pending, 1, "finalization emitted exactly one pending event");

        let batch = claim_outbox_batch(&pool, 10, 30).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_type, "run.completed");
        assert_eq!(batch[0].run_id, run_id);

        // Leased rows aren't re-claimed within the lease window.
        assert!(
            claim_outbox_batch(&pool, 10, 30).await.unwrap().is_empty(),
            "a leased event is not re-claimed"
        );

        mark_outbox_delivered(&pool, &batch[0].id).await.unwrap();
        let delivered: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox WHERE status = 'delivered'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(delivered, 1, "delivered event is marked");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A two-task DAG used by the v5/v6 read/cancel/GC tests.
    #[cfg(feature = "ops")]
    async fn seed_run(pool: &Pool) -> String {
        let yaml = "name: demo\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [\"a\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(pool, &dag, yaml).await.unwrap()
    }

    /// cancel_run flips a running run + all its non-terminal tasks to cancelled,
    /// and is a no-op (false) the second time.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn cancel_run_is_idempotent() {
        let (pool, path) = temp_pool().await;
        let run_id = seed_run(&pool).await;

        assert!(cancel_run(&pool, &run_id).await.unwrap(), "first cancel succeeds");
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status.to_string(), "cancelled");
        let tasks = list_tasks(&pool, &run_id).await.unwrap();
        assert!(
            tasks.iter().all(|t| t.status == crate::models::TaskStatus::Cancelled),
            "all tasks cancelled"
        );

        assert!(!cancel_run(&pool, &run_id).await.unwrap(), "second cancel is a no-op");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// rerun_from_failed resets exactly the broken cone (failed + cancelled) of a
    /// terminal run, leaves succeeded tasks intact, recomputes `remaining_deps`
    /// from the still-unsatisfied dependencies so the failure frontier becomes
    /// ready while tasks behind a reset dependency keep waiting, bumps `version`,
    /// and re-arms the run. Non-rerunnable runs return `None`.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn rerun_from_failed_resets_broken_cone() {
        let (pool, path) = temp_pool().await;
        // a → b → c chain (b depends on a, c depends on b).
        let yaml = "name: chain\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [\"a\"]\n  - name: c\n    command: [\"true\"]\n    depends_on: [\"b\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        let by_name = |tasks: &[TaskRun]| -> std::collections::HashMap<String, TaskRun> {
            tasks.iter().map(|t| (t.name.clone(), t.clone())).collect()
        };
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        let v_b = tasks["b"].version;
        let v_c = tasks["c"].version;

        // Drive to a terminal failure state: a succeeded, b failed, c (downstream)
        // cancelled, run failed — exactly the shape mark_task_failed leaves behind.
        for (name, status) in [("a", "succeeded"), ("b", "failed"), ("c", "cancelled")] {
            sqlx::query("UPDATE task_runs SET status = ? WHERE id = ?")
                .bind(status)
                .bind(&tasks[name].id)
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE workflow_runs SET status = 'failed', finished_at = '2026-01-01T00:00:00Z' WHERE id = ?")
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();

        let reset = rerun_from_failed(&pool, &run_id).await.unwrap();
        assert_eq!(reset, Some(2), "only b and c (failed + cancelled) are reset");

        let after = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        // a untouched.
        assert_eq!(after["a"].status, crate::models::TaskStatus::Succeeded);
        // b: pending, dep a already succeeded → frontier, remaining_deps 0, fenced.
        assert_eq!(after["b"].status, crate::models::TaskStatus::Pending);
        assert_eq!(after["b"].remaining_deps, 0, "b's only dep (a) succeeded");
        assert!(after["b"].version > v_b, "b version bumped to fence stale workers");
        // c: pending, dep b is being rerun (not succeeded) → still blocked by 1.
        assert_eq!(after["c"].status, crate::models::TaskStatus::Pending);
        assert_eq!(after["c"].remaining_deps, 1, "c waits on b to re-succeed");
        assert!(after["c"].version > v_c);
        // run re-armed.
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status.to_string(), "running");
        assert!(run.finished_at.is_none(), "finished_at cleared on re-arm");

        // A second rerun on the now-running run is a no-op signal (None).
        assert_eq!(rerun_from_failed(&pool, &run_id).await.unwrap(), None);
        // Unknown run → None.
        assert_eq!(rerun_from_failed(&pool, "does-not-exist").await.unwrap(), None);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// clear_task_with_downstream resets a completed task *and* its transitive
    /// downstream cone (leaving siblings/ancestors intact), recomputes
    /// `remaining_deps` so the target becomes ready while its dependents wait on
    /// it again, bumps `version`, and re-arms a finished run. A non-terminal or
    /// unknown task returns `None`.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn clear_task_resets_target_and_downstream() {
        let (pool, path) = temp_pool().await;
        // a → b → c chain plus an independent d (b depends on a, c on b; d alone).
        let yaml = "name: chain\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [\"a\"]\n  - name: c\n    command: [\"true\"]\n    depends_on: [\"b\"]\n  - name: d\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        let by_name = |tasks: &[TaskRun]| -> std::collections::HashMap<String, TaskRun> {
            tasks.iter().map(|t| (t.name.clone(), t.clone())).collect()
        };
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        let v_b = tasks["b"].version;
        let v_c = tasks["c"].version;

        // Drive the whole run to success.
        for name in ["a", "b", "c", "d"] {
            sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE id = ?")
                .bind(&tasks[name].id)
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded', finished_at = '2026-01-01T00:00:00Z' WHERE id = ?")
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();

        // Clear b: b + its downstream (c) reset; a and the independent d untouched.
        let reset = clear_task_with_downstream(&pool, &run_id, &tasks["b"].id).await.unwrap();
        assert_eq!(reset, Some(2), "b and its downstream c reset (a and d intact)");

        let after = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(after["a"].status, crate::models::TaskStatus::Succeeded, "ancestor intact");
        assert_eq!(after["d"].status, crate::models::TaskStatus::Succeeded, "sibling intact");
        // b: pending, dep a still succeeded → frontier, remaining_deps 0, fenced.
        assert_eq!(after["b"].status, crate::models::TaskStatus::Pending);
        assert_eq!(after["b"].remaining_deps, 0, "b's only dep (a) still succeeded");
        assert!(after["b"].version > v_b, "b version bumped to fence stale workers");
        // c: pending, dep b is being rerun (not succeeded) → blocked by 1.
        assert_eq!(after["c"].status, crate::models::TaskStatus::Pending);
        assert_eq!(after["c"].remaining_deps, 1, "c waits on b to re-succeed");
        assert!(after["c"].version > v_c);
        // run re-armed.
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status.to_string(), "running");
        assert!(run.finished_at.is_none(), "finished_at cleared on re-arm");

        // b is now pending (non-terminal) → not clearable → None.
        assert_eq!(clear_task_with_downstream(&pool, &run_id, &tasks["b"].id).await.unwrap(), None);
        // Unknown task → None; and task_exists agrees.
        assert_eq!(clear_task_with_downstream(&pool, &run_id, "nope").await.unwrap(), None);
        assert!(!task_exists(&pool, &run_id, "nope").await.unwrap());
        assert!(task_exists(&pool, &run_id, &tasks["b"].id).await.unwrap());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Approval gates (#19): a `type: approval` task parks in `awaiting_approval`
    /// (not claimed); approving it succeeds it and advances its dependent, while
    /// rejecting skips the dependent (default all_success). Timeout auto-resolves.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn approval_gate_parks_then_resolves() {
        let (pool, path) = temp_pool().await;
        // build → gate(approval) → deploy
        let yaml = "name: appr\ntasks:\n  \
            - { name: build, command: [\"true\"] }\n  \
            - { name: gate, type: approval, depends_on: [build] }\n  \
            - { name: deploy, command: [\"true\"], depends_on: [gate] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        let by_name = |ts: &[TaskRun]| -> std::collections::HashMap<String, TaskRun> {
            ts.iter().map(|t| (t.name.clone(), t.clone())).collect()
        };

        // Drive build to success, then advance: the gate must park (not go ready).
        advance_ready_tasks(&pool).await.unwrap();
        let build = claim_ready(&pool, "w", 10).await.unwrap();
        assert_eq!(build.len(), 1, "only build is claimable (the gate never is)");
        mark_task_succeeded(&pool, &build[0].id, "w", build[0].version + 1, None).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].status, crate::models::TaskStatus::AwaitingApproval);
        // The gate is never claimable while awaiting.
        assert!(claim_ready(&pool, "w", 10).await.unwrap().is_empty());

        // Approve → gate succeeds, deploy becomes ready.
        assert!(resolve_approval(&pool, &run_id, &tasks["gate"].id, true).await.unwrap());
        advance_ready_tasks(&pool).await.unwrap();
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].status, crate::models::TaskStatus::Succeeded);
        assert_eq!(tasks["deploy"].status, crate::models::TaskStatus::Ready);
        // Re-approving an already-resolved gate is a no-op (guarded).
        assert!(!resolve_approval(&pool, &run_id, &tasks["gate"].id, true).await.unwrap());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Rejecting a gate fails it and skips its all_success dependent; a timeout
    /// with the default (reject) auto-resolves an expired gate.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn approval_reject_and_timeout() {
        let (pool, path) = temp_pool().await;
        let by_name = |ts: &[TaskRun]| -> std::collections::HashMap<String, TaskRun> {
            ts.iter().map(|t| (t.name.clone(), t.clone())).collect()
        };

        // (1) Reject skips the dependent.
        let yaml = "name: rej\ntasks:\n  \
            - { name: gate, type: approval }\n  \
            - { name: deploy, command: [\"true\"], depends_on: [gate] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap(); // gate (no deps) → awaiting
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].status, crate::models::TaskStatus::AwaitingApproval);
        assert!(resolve_approval(&pool, &run_id, &tasks["gate"].id, false).await.unwrap());
        advance_ready_tasks(&pool).await.unwrap();
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].status, crate::models::TaskStatus::Failed);
        assert_eq!(tasks["deploy"].status, crate::models::TaskStatus::Skipped, "all_success dependent skipped");

        // (2) Timeout auto-resolves to the default (reject) once expired.
        let yaml2 = "name: to\ntasks:\n  - { name: gate, type: approval, approval_timeout_secs: 60 }\n";
        let dag2 = DagGraph::from_yaml(yaml2).unwrap();
        let run2 = create_run(&pool, &dag2, yaml2).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let g2 = by_name(&list_tasks(&pool, &run2).await.unwrap());
        let gate2 = g2["gate"].id.clone();
        // Not yet expired → sweep is a no-op.
        assert!(resolve_expired_approvals(&pool).await.unwrap().is_empty());
        // Backdate the awaiting-since marker past the timeout.
        let past = (chrono::Utc::now() - chrono::TimeDelta::seconds(120)).to_rfc3339();
        sqlx::query("UPDATE task_runs SET scheduled_at = ? WHERE id = ?")
            .bind(&past)
            .bind(&gate2)
            .execute(&pool)
            .await
            .unwrap();
        let resolved = resolve_expired_approvals(&pool).await.unwrap();
        assert_eq!(resolved, vec![(gate2.clone(), false)], "default reject");
        let st: String = sqlx::query_scalar("SELECT status FROM task_runs WHERE id = ?")
            .bind(&gate2)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(st, "failed");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// list_runs carries the DAG name; status_counts reflects the seeded rows.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn list_runs_and_counts() {
        let (pool, path) = temp_pool().await;
        let run_id = seed_run(&pool).await;

        let runs = list_runs(&pool, None, 50).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].id, run_id);
        assert_eq!(runs[0].name, "demo");

        // Status filter excludes non-matching runs.
        assert!(list_runs(&pool, Some("succeeded"), 50).await.unwrap().is_empty());

        let snap = status_counts(&pool).await.unwrap();
        assert_eq!(snap.runs_by_status, vec![("running".to_string(), 1)]);
        let pending: i64 = snap
            .tasks_by_status
            .iter()
            .find(|(s, _)| s == "pending")
            .map(|(_, n)| *n)
            .unwrap_or(0);
        assert_eq!(pending, 2, "two pending tasks seeded");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// GC removes only terminal runs older than the cutoff, cascading to tasks,
    /// edges, and orphaned definitions.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn gc_purges_old_terminal_runs() {
        let (pool, path) = temp_pool().await;
        let run_id = seed_run(&pool).await;

        // A still-running run is never collected, regardless of cutoff.
        let future_cutoff = (chrono::Utc::now() + chrono::TimeDelta::days(1)).to_rfc3339();
        assert_eq!(gc_old_runs(&pool, &future_cutoff).await.unwrap(), 0);

        // Finalize the run in the past, then collect with a now() cutoff.
        let past = (chrono::Utc::now() - chrono::TimeDelta::days(2)).to_rfc3339();
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded', finished_at = ? WHERE id = ?")
            .bind(&past)
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE run_id = ?")
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();

        let cutoff = chrono::Utc::now().to_rfc3339();
        assert_eq!(gc_old_runs(&pool, &cutoff).await.unwrap(), 1, "the old run is purged");
        assert!(get_run(&pool, &run_id).await.unwrap().is_none());
        assert!(list_tasks(&pool, &run_id).await.unwrap().is_empty());
        let defs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_definitions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(defs, 0, "orphaned definition removed too");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Leadership is exclusive while the lease holds, renewable by the holder, and
    /// transfers once the lease expires.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn leadership_is_exclusive() {
        let (pool, path) = temp_pool().await;

        assert!(try_acquire_leadership(&pool, "ops", "A", 30).await.unwrap(), "A takes a free role");
        assert!(!try_acquire_leadership(&pool, "ops", "B", 30).await.unwrap(), "B blocked while A holds");
        assert!(try_acquire_leadership(&pool, "ops", "A", 30).await.unwrap(), "A renews its own lease");

        // Force the lease to look expired; B can now take over.
        let past = (chrono::Utc::now() - chrono::TimeDelta::seconds(1)).to_rfc3339();
        sqlx::query("UPDATE leader_election SET lease_expires_at = ? WHERE role = 'ops'")
            .bind(&past)
            .execute(&pool)
            .await
            .unwrap();
        assert!(try_acquire_leadership(&pool, "ops", "B", 30).await.unwrap(), "B takes the expired role");
        assert!(!try_acquire_leadership(&pool, "ops", "A", 30).await.unwrap(), "A now blocked");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Dead-letter store round-trips: record → list/get → delete (idempotent).
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn dead_letters_record_list_delete() {
        let (pool, path) = temp_pool().await;

        let id = record_dead_letter(&pool, "name: x\n bad", "parse error", "redis", 2)
            .await
            .unwrap();
        let listed = list_dead_letters(&pool, 50).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].source, "redis");
        assert_eq!(listed[0].failures, 2);
        assert!(get_dead_letter(&pool, &id).await.unwrap().is_some());

        assert!(delete_dead_letter(&pool, &id).await.unwrap(), "first delete removes it");
        assert!(!delete_dead_letter(&pool, &id).await.unwrap(), "second delete is a no-op");
        assert!(list_dead_letters(&pool, 50).await.unwrap().is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── Crash-recovery invariant (v0/v1), deterministic library-level mirror ──

    /// Crash at the `running` transition: a task whose holder died (lease lapsed)
    /// is reclaimed from DB state and driven to terminal — **nothing stranded**.
    /// This is the in-process mirror of the `kill -9` integration test.
    #[tokio::test]
    async fn expired_lease_recovers_and_run_completes() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "worker-dead", 10).await.unwrap();
        assert_eq!(claimed.len(), 1, "root task claimed");

        // Holder dies mid-task: lease lapses, the row is never marked done.
        sqlx::query("UPDATE task_runs SET lease_expires_at = '1970-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&claimed[0].id)
            .execute(&pool)
            .await
            .unwrap();

        // A surviving scheduler's tick reclaims and completes it.
        assert_eq!(recover_expired_leases(&pool).await.unwrap(), 1, "expired lease reclaimed");
        advance_ready_tasks(&pool).await.unwrap();
        let reclaimed = claim_ready(&pool, "worker-live", 10).await.unwrap();
        assert_eq!(reclaimed.len(), 1, "task is re-claimable after lease expiry");
        assert_eq!(reclaimed[0].attempt, 1, "snapshot shows the prior attempt; this run is attempt 2");

        let fence = reclaimed[0].version + 1;
        assert!(
            mark_task_succeeded(&pool, &reclaimed[0].id, "worker-live", fence, Some("ok".into()))
                .await
                .unwrap(),
            "the live worker's fence is accepted"
        );

        let finalized = reap_completed_runs(&pool).await.unwrap();
        assert!(
            finalized.iter().any(|(rid, st)| *rid == run_id && matches!(st, RunStatus::Succeeded)),
            "run finalizes succeeded — not stranded"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// No double dispatch absent a crash: a claimed (running) task is not
    /// claimable again — the status guard alone blocks a second dispatch, and the
    /// version fence (see `stale_fence_is_rejected`) blocks a stale completion.
    #[tokio::test]
    async fn claimed_task_is_not_reclaimed() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let first = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(first.len(), 1, "first claim wins the task");
        let second = claim_ready(&pool, "w2", 10).await.unwrap();
        assert!(second.is_empty(), "a running task cannot be claimed a second time");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── QW3-catchup automatic backfill & self-healing ────────────────────────────────

    /// Insert a workflow + a catch-up-enabled schedule, returning the schedule id.
    /// `last_fired_at` seeds the catch-up lower bound (None → never fired).
    #[cfg(all(test, feature = "enterprise"))]
    async fn seed_catchup_schedule(
        pool: &Pool,
        cron: &str,
        spec_yaml: &str,
        last_fired_at: Option<&str>,
    ) -> String {
        let wf_id = Uuid::new_v4().to_string();
        let sched_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES (?,?,?,?,?)")
            .bind(&wf_id)
            .bind(format!("wf-{wf_id}"))
            .bind(spec_yaml)
            .bind(&now)
            .bind(&now)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO schedules
               (id, workflow_id, cron_expr, enabled, catchup, next_fire_at, last_fired_at, created_at, updated_at)
             VALUES (?,?,?,1,1,NULL,?,?,?)",
        )
        .bind(&sched_id)
        .bind(&wf_id)
        .bind(cron)
        .bind(last_fired_at)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sched_id
    }

    /// A catch-up schedule round-trips through `list_catchup_schedules`, and the
    /// `schedule_backfills` slot claim dedups: the first claim of a logical date
    /// wins, a second is a no-op (so a re-sweep can't double-run a missed fire),
    /// and releasing a slot makes it reclaimable again.
    #[tokio::test]
    #[cfg(all(test, feature = "enterprise"))]
    async fn catchup_listing_and_slot_dedup() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: nightly\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let sched_id = seed_catchup_schedule(&pool, "0 0 * * * *", yaml, None).await;

        let listed = list_catchup_schedules(&pool).await.unwrap();
        assert_eq!(listed.len(), 1, "the catch-up schedule is listed");
        assert_eq!(listed[0].id, sched_id);
        assert_eq!(listed[0].spec, yaml, "the workflow spec travels with the row");

        let now = chrono::Utc::now().to_rfc3339();
        let logical = "2026-01-01T00:00:00+00:00";
        assert!(
            claim_backfill_slot(&pool, &sched_id, logical, &now).await.unwrap(),
            "first claim of a fresh slot wins"
        );
        assert!(
            !claim_backfill_slot(&pool, &sched_id, logical, &now).await.unwrap(),
            "re-claiming the same slot is a no-op (dedup)"
        );

        // A non-catch-up schedule is excluded: disabling catchup hides it.
        sqlx::query("UPDATE schedules SET catchup = 0 WHERE id = ?")
            .bind(&sched_id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(list_catchup_schedules(&pool).await.unwrap().is_empty());

        // Release makes the slot reclaimable again (the create_run-failed path).
        release_backfill_slot(&pool, &sched_id, logical).await.unwrap();
        assert!(
            claim_backfill_slot(&pool, &sched_id, logical, &now).await.unwrap(),
            "a released slot can be re-claimed"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Auto-rerun eligibility: a fresh `failed` run is a candidate; once its
    /// attempt ledger reaches the cap it drops out; and a recent rerun is held off
    /// by the cooldown until the cutoff passes.
    #[tokio::test]
    #[cfg(all(test, feature = "enterprise"))]
    async fn failed_run_rerun_cap_and_cooldown() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        // Drive the run terminal-failed.
        sqlx::query("UPDATE workflow_runs SET status = 'failed', finished_at = ? WHERE id = ?")
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();

        // A fresh failed run (no ledger row → last_rerun_at IS NULL) is a
        // candidate regardless of cutoff. The cap and cooldown are AND'd, so the
        // two are exercised independently below.
        let far_past = "2000-01-01T00:00:00+00:00";
        let future = (chrono::Utc::now() + chrono::TimeDelta::days(1)).to_rfc3339();
        let cands = list_failed_runs_for_rerun(&pool, 3, far_past, 100).await.unwrap();
        assert_eq!(cands, vec![run_id.clone()], "fresh failed run is a candidate");

        // Cap (with a future cutoff so the cooldown clause always passes): under
        // the cap it stays a candidate; at the cap it drops out.
        let now = chrono::Utc::now().to_rfc3339();
        bump_rerun_attempt(&pool, &run_id, &now).await.unwrap();
        bump_rerun_attempt(&pool, &run_id, &now).await.unwrap();
        assert_eq!(
            list_failed_runs_for_rerun(&pool, 3, &future, 100).await.unwrap().len(),
            1,
            "attempts=2 < cap=3 → still a candidate"
        );
        bump_rerun_attempt(&pool, &run_id, &now).await.unwrap();
        assert!(
            list_failed_runs_for_rerun(&pool, 3, &future, 100).await.unwrap().is_empty(),
            "attempts=3 == cap → no longer auto-rerun"
        );

        // Cooldown (with a generous cap so only the cooldown clause can exclude):
        // a far-past cutoff is before last_rerun_at, holding the run off; a future
        // cutoff lets it through again.
        assert!(
            list_failed_runs_for_rerun(&pool, 99, far_past, 100).await.unwrap().is_empty(),
            "last_rerun_at is newer than the far-past cutoff — cooldown blocks it"
        );
        assert_eq!(
            list_failed_runs_for_rerun(&pool, 99, &future, 100).await.unwrap().len(),
            1,
            "once the cutoff passes last_rerun_at the cooldown clears"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The stall gauge counts only runs `running` past the cutoff, and
    /// `enqueue_outbox_event` lands a deliverable pending row out-of-band.
    #[tokio::test]
    #[cfg(all(test, feature = "enterprise"))]
    async fn incomplete_count_and_outbox_enqueue() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        // Backdate the (still running) run's creation so it is past any near cutoff.
        let old = (chrono::Utc::now() - chrono::TimeDelta::hours(5)).to_rfc3339();
        sqlx::query("UPDATE workflow_runs SET created_at = ? WHERE id = ?")
            .bind(&old)
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();

        let cutoff = (chrono::Utc::now() - chrono::TimeDelta::hours(1)).to_rfc3339();
        assert_eq!(
            count_incomplete_runs(&pool, &cutoff).await.unwrap(),
            1,
            "a 5h-old running run is past the 1h stall cutoff"
        );
        // A future-ish cutoff (creation after it) excludes the run.
        let near = (chrono::Utc::now() - chrono::TimeDelta::hours(9)).to_rfc3339();
        assert_eq!(
            count_incomplete_runs(&pool, &near).await.unwrap(),
            0,
            "with a 9h cutoff the 5h-old run is not yet stalled"
        );

        enqueue_outbox_event(&pool, &run_id, "backfill.catchup", "{\"k\":1}").await.unwrap();
        let batch = claim_outbox_batch(&pool, 10, 30).await.unwrap();
        assert_eq!(batch.len(), 1, "the enqueued event is claimable");
        assert_eq!(batch[0].event_type, "backfill.catchup");
        assert_eq!(batch[0].run_id, run_id);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
