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

/// Opens the SQLite pool (single connection — see below) and runs migrations:
/// the base set first, then — with the `enterprise` feature — the enterprise
/// set. Both migrators run ignore-missing so the two migration dirs can share
/// sqlx's single `_sqlx_migrations` table without colliding.
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

    // The base and enterprise (900+) sets share sqlx's single `_sqlx_migrations`
    // table. ignore_missing lets the base migrator tolerate the enterprise rows it
    // does not own; without it an enterprise DB (900 already applied) fails the base
    // migrator on every reboot with "previously applied but missing in the resolved
    // migrations" — before the enterprise migrator runs.
    let mut base = sqlx::migrate!("./migrations");
    base.set_ignore_missing(true);
    base.run(&pool).await?;
    #[cfg(feature = "enterprise")]
    {
        // enterprise migrations share sqlx's single `_sqlx_migrations` table with the
        // base set; their versions are offset above the base range (900+) and
        // ignore_missing lets this migrator tolerate the base migrations it does
        // not own. Mirrors the Postgres path in db/postgres.rs.
        let mut ee = sqlx::migrate!("./migrations_ee");
        ee.set_ignore_missing(true);
        ee.run(&pool).await?;
    }
    Ok(pool)
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction. Returns the new run_id.
pub async fn create_run(pool: &Pool, dag: &DagGraph, yaml_spec: &str) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, None).await
}

/// [`create_run`] that also commits a streaming source's cursor **in the same
/// transaction** — the exactly-once ingestion primitive: the run and the
/// source position `(source_name, position)` become durable atomically, so a
/// crash can never leave a created run whose input position would be replayed
/// (a duplicate run) or an advanced position whose run was lost.
pub async fn create_run_with_offset(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    source_name: &str,
    position: &str,
) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, Some((source_name, position))).await
}

async fn create_run_inner(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    offset: Option<(&str, &str)>,
) -> Result<String> {
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

    // Per-workflow concurrency cap (#21): refuse to start a run if this workflow
    // (by name) already has `max_active_runs` runs in flight. Checked inside the
    // write transaction, so on single-writer SQLite it is race-free; the count
    // filters the small set of `running` runs and PK-joins their definitions.
    if let Some(max) = dag.spec.max_active_runs {
        if max > 0 {
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_runs wr
                 JOIN workflow_definitions d ON d.id = wr.definition_id
                 WHERE wr.status = 'running' AND d.name = ?",
            )
            .bind(&dag.spec.name)
            .fetch_one(&mut *tx)
            .await?;
            if active >= max as i64 {
                return Err(anyhow::Error::new(crate::models::MaxActiveRunsReached {
                    name: dag.spec.name.clone(),
                    max,
                    active,
                }));
            }
        }
    }

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
           (id, definition_id, status, created_at, deadline_at, alert_deadline_at, result_from, environment)
         VALUES (?, ?, 'running', ?, ?, ?, ?, ?)",
    )
    .bind(&run_id)
    .bind(&def_id)
    .bind(&now)
    .bind(&deadline_at)
    .bind(&alert_deadline_at)
    .bind(&dag.spec.result_from)
    .bind(&dag.spec.environment)
    .execute(&mut *tx)
    .await?;

    // Create task_run rows; store the full TaskSpec as JSON in `input` so the
    // row is self-contained — dispatch only needs task.id + task.input.
    // A `gang:` task expands into `size` member rows (`<name>.<rank>`) that
    // share a gang_id — the unit the gang-aware claimer seizes all-or-nothing.
    // `task_ids` maps the AUTHORED name to every row it produced, so an edge
    // to a gang fans out to all members (a dependent waits for the whole gang).
    let mut task_ids: HashMap<String, Vec<String>> = HashMap::new();
    for task_spec in &dag.spec.tasks {
        if task_ids.contains_key(&task_spec.name) {
            bail!("duplicate task name '{}' in run '{}'", task_spec.name, run_id);
        }
        // A dependency on a gang is a dependency on every member.
        let dep_count: i64 = task_spec
            .depends_on
            .iter()
            .map(|d| {
                dag.task_spec(d)
                    .and_then(|s| s.gang.as_ref())
                    .map(|g| g.size as i64)
                    .unwrap_or(1)
            })
            .sum();

        let trigger_rule =
            task_spec.trigger_rule.as_deref().unwrap_or(crate::models::DEFAULT_TRIGGER_RULE);
        let allow_failure = i64::from(task_spec.allow_failure);
        let is_approval = i64::from(task_spec.is_approval());
        let approval_timeout = task_spec.approval_timeout_secs.map(|s| s as i64);
        let approval_on_timeout = task_spec.approval_on_timeout.as_deref();
        // Resolved task → DAG default → 'default', so the claim-path filter can
        // stay a plain column predicate with no spec re-parse.
        let runner_class = task_spec
            .runner_class
            .as_deref()
            .or(dag.spec.runner_class.as_deref())
            .unwrap_or(crate::dag::DEFAULT_RUNNER_CLASS);

        let gang_id = task_spec.gang.as_ref().map(|_| Uuid::new_v4().to_string());
        let gang_size = task_spec.gang.as_ref().map(|g| g.size);
        let member_count = gang_size.unwrap_or(1);
        let mut ids = Vec::with_capacity(member_count as usize);
        for rank in 0..member_count {
            let task_id = Uuid::new_v4().to_string();
            let (name, input_json) = match (&gang_id, gang_size) {
                (Some(gid), Some(size)) => {
                    let mut member = task_spec.clone();
                    member.name = format!("{}.{rank}", task_spec.name);
                    member.gang_member = Some(crate::dag::GangMember {
                        id: gid.clone(),
                        rank,
                        size,
                    });
                    (member.name.clone(), serde_json::to_string(&member)?)
                }
                _ => (task_spec.name.clone(), serde_json::to_string(task_spec)?),
            };
            sqlx::query(
                "INSERT INTO task_runs
                 (id, run_id, name, status, remaining_deps, input, scheduled_at, trigger_rule,
                  allow_failure, is_approval, approval_timeout_secs, approval_on_timeout,
                  runner_class, gang_id, gang_rank, gang_size, priority, pool)
                 VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&task_id)
            .bind(&run_id)
            .bind(&name)
            .bind(dep_count)
            .bind(&input_json)
            .bind(&now)
            .bind(trigger_rule)
            .bind(allow_failure)
            .bind(is_approval)
            .bind(approval_timeout)
            .bind(approval_on_timeout)
            .bind(runner_class)
            .bind(&gang_id)
            .bind(gang_id.as_ref().map(|_| rank as i64))
            .bind(gang_size.map(|s| s as i64))
            // Priority (#25) and pool (#21) are per-task claim inputs; every
            // member of a gang inherits the authored task's values.
            .bind(task_spec.priority)
            .bind(task_spec.pool.as_deref())
            .execute(&mut *tx)
            .await?;
            ids.push(task_id);
        }
        task_ids.insert(task_spec.name.clone(), ids);
    }

    // Wire up dependency edges (fanning across gang members on either side).
    for task_spec in &dag.spec.tasks {
        for dep_name in &task_spec.depends_on {
            // DagGraph::from_yaml already rejects unknown deps, but don't panic
            // if create_run is ever handed an unvalidated graph — reject the run.
            let Some(dependency_ids) = task_ids.get(dep_name) else {
                bail!(
                    "task '{}' depends on unknown task '{}' in run '{}'",
                    task_spec.name,
                    dep_name,
                    run_id
                );
            };
            for dependent_id in &task_ids[&task_spec.name] {
                for dependency_id in dependency_ids {
                    sqlx::query(
                        "INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES (?, ?)",
                    )
                    .bind(dependent_id)
                    .bind(dependency_id)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
    }

    // Exactly-once: the source cursor rides the run's transaction.
    if let Some((source_name, position)) = offset {
        upsert_source_offset(&mut *tx, source_name, position, &now).await?;
    }

    tx.commit().await?;
    Ok(run_id)
}

/// Upsert one source's committed position (see `source_offsets`). Runs on any
/// executor so it can join a caller's transaction.
async fn upsert_source_offset<'e, E>(ex: E, source_name: &str, position: &str, now: &str) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query(
        "INSERT INTO source_offsets (source_name, position, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT (source_name)
         DO UPDATE SET position = excluded.position, updated_at = excluded.updated_at",
    )
    .bind(source_name)
    .bind(position)
    .bind(now)
    .execute(ex)
    .await?;
    Ok(())
}

/// The committed position for `source_name`, or `None` if it never committed.
/// A resuming source starts *after* this position — the read half of the
/// exactly-once contract.
pub async fn source_offset(pool: &Pool, source_name: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar("SELECT position FROM source_offsets WHERE source_name = ?")
        .bind(source_name)
        .fetch_optional(pool)
        .await?)
}

// ── Per-partition range leases (multi-consumer sources) ──────────────────────
// N engines split one logical stream by leasing partitions: claim free/expired
// rows, heartbeat-renew while consuming, and a dead consumer's partitions are
// re-claimable at lease expiry — the task-claim shape applied to stream shards.
// Positions ride `source_offsets` keyed "<source>/<partition>".

/// Ensure rows exist for every discovered partition (idempotent).
pub async fn register_source_partitions(
    pool: &Pool,
    source_name: &str,
    partitions: &[String],
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    for p in partitions {
        sqlx::query(
            "INSERT INTO source_partitions (source_name, partition, updated_at)
             VALUES (?, ?, ?) ON CONFLICT (source_name, partition) DO NOTHING",
        )
        .bind(source_name)
        .bind(p)
        .bind(&now)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Claim up to `limit` free (never-claimed or lease-expired) partitions for
/// `worker_id`, leased for `lease_secs`. Returns the claimed partition names.
pub async fn claim_source_partitions(
    pool: &Pool,
    source_name: &str,
    worker_id: &str,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<String>> {
    let now = chrono::Utc::now();
    let now_s = now.to_rfc3339();
    let lease = (now + chrono::TimeDelta::seconds(lease_secs)).to_rfc3339();
    sqlx::query(
        "UPDATE source_partitions
         SET claimed_by = ?, lease_expires_at = ?, updated_at = ?
         WHERE (source_name, partition) IN (
             SELECT source_name, partition FROM source_partitions
             WHERE source_name = ?
               AND (claimed_by IS NULL OR lease_expires_at IS NULL OR lease_expires_at < ?)
               AND claimed_by IS NOT ?
             LIMIT ?
         )",
    )
    .bind(worker_id)
    .bind(&lease)
    .bind(&now_s)
    .bind(source_name)
    .bind(&now_s)
    .bind(worker_id)
    .bind(limit)
    .execute(pool)
    .await?;
    // Exact-match on the lease stamp we just wrote isolates this claim's rows
    // (the CAS-style read-back the SQLite backend uses elsewhere).
    Ok(sqlx::query_scalar(
        "SELECT partition FROM source_partitions
         WHERE source_name = ? AND claimed_by = ? AND lease_expires_at = ?
         ORDER BY partition",
    )
    .bind(source_name)
    .bind(worker_id)
    .bind(&lease)
    .fetch_all(pool)
    .await?)
}

/// Renew every partition `worker_id` holds on `source_name`; returns how many
/// renewed. Fewer than held = some leases were reclaimed — resync via
/// [`held_source_partitions`].
pub async fn renew_source_partitions(
    pool: &Pool,
    source_name: &str,
    worker_id: &str,
    lease_secs: i64,
) -> Result<u64> {
    let now = chrono::Utc::now();
    let lease = (now + chrono::TimeDelta::seconds(lease_secs)).to_rfc3339();
    Ok(sqlx::query(
        "UPDATE source_partitions
         SET lease_expires_at = ?, updated_at = ?
         WHERE source_name = ? AND claimed_by = ?",
    )
    .bind(&lease)
    .bind(now.to_rfc3339())
    .bind(source_name)
    .bind(worker_id)
    .execute(pool)
    .await?
    .rows_affected())
}

/// The partitions `worker_id` currently holds on `source_name`.
pub async fn held_source_partitions(
    pool: &Pool,
    source_name: &str,
    worker_id: &str,
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT partition FROM source_partitions
         WHERE source_name = ? AND claimed_by = ? ORDER BY partition",
    )
    .bind(source_name)
    .bind(worker_id)
    .fetch_all(pool)
    .await?)
}

/// Release every partition `worker_id` holds (clean shutdown — peers can claim
/// immediately instead of waiting out the lease).
pub async fn release_source_partitions(
    pool: &Pool,
    source_name: &str,
    worker_id: &str,
) -> Result<u64> {
    Ok(sqlx::query(
        "UPDATE source_partitions
         SET claimed_by = NULL, lease_expires_at = NULL, updated_at = ?
         WHERE source_name = ? AND claimed_by = ?",
    )
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(source_name)
    .bind(worker_id)
    .execute(pool)
    .await?
    .rows_affected())
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
    // (id, run_id, trigger_rule, is_approval, input) for every task whose deps
    // are all terminal.
    let candidates: Vec<(String, String, String, i64, Option<String>)> = sqlx::query_as(
        "SELECT id, run_id, trigger_rule, is_approval, input FROM task_runs
         WHERE status = 'pending' AND remaining_deps = 0
         ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut transitioned = 0u64;
    for (task_id, run_id, rule, is_approval, input) in candidates {
        let dep_statuses: Vec<String> = sqlx::query_scalar(
            "SELECT dep.status FROM task_dependencies d
             JOIN task_runs dep ON dep.id = d.dependency_id
             WHERE d.dependent_id = ?",
        )
        .bind(&task_id)
        .fetch_all(pool)
        .await?;

        // Runtime `when` gate — mirrors the Postgres path: a leaf-surviving
        // condition references upstream outputs, evaluable only now. False ⇒
        // skipped like an unsatisfied trigger rule; unevaluable ⇒ failed loudly.
        let mut when_pass = true;
        if let Some(cond) = input
            .as_deref()
            .and_then(|json| serde_json::from_str::<crate::dag::TaskSpec>(json).ok())
            .and_then(|spec| spec.when)
        {
            let mut ctx: std::collections::BTreeMap<String, String> = Default::default();
            for name in crate::expand::when_output_refs(&cond) {
                let output: Option<Option<String>> = sqlx::query_scalar(
                    "SELECT output FROM task_runs WHERE run_id = ? AND name = ?",
                )
                .bind(&run_id)
                .bind(&name)
                .fetch_optional(pool)
                .await?;
                let value = output.flatten().unwrap_or_default();
                ctx.insert(format!("tasks.{name}.output"), value.trim().to_string());
            }
            match crate::expand::eval_when(&crate::expand::substitute(&cond, &ctx)) {
                Ok(pass) => when_pass = pass,
                Err(e) => {
                    let now = chrono::Utc::now().to_rfc3339();
                    let msg = format!("runtime when '{cond}' failed to evaluate: {e}");
                    let mut tx = pool.begin().await?;
                    let rows = sqlx::query(
                        "UPDATE task_runs SET status = 'failed', output = ?, finished_at = ?
                         WHERE id = ? AND status = 'pending'",
                    )
                    .bind(&msg)
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
                    continue;
                }
            }
        }

        if when_pass && crate::models::trigger_rule_ready(&rule, &dep_statuses) {
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

/// Count of tasks genuinely occupying a worker slot in a pool.
///
/// A **parked** task deliberately keeps `status = 'running'` while holding *no*
/// worker: the sub-workflow trigger (`sub_run_id`) and the time / HTTP / dataset
/// wait sensors (`wake_at` / `wait_url` / `wait_dataset`) all park that way so
/// the claim scan skips them and lease recovery leaves them alone. Pool budgets
/// meter worker slots, so those shapes must be excluded — otherwise an
/// hour-long sensor squats its pool's budget for the whole hour, which is
/// precisely what parking exists to avoid. Shared by the ordinary and gang
/// claim paths so the two can never drift.
const POOL_RUNNING_COUNT: &str = "SELECT COUNT(*) FROM task_runs
     WHERE status = 'running' AND pool = ?
       AND wake_at IS NULL AND wait_url IS NULL
       AND wait_dataset IS NULL AND sub_run_id IS NULL";

/// Claim up to `limit` ready tasks for `worker_id`.
///
/// Uses CAS on `version` so this is safe to call from multiple workers in v2.
/// Returns the snapshot of claimed rows (attempt is the pre-claim value).
pub async fn claim_ready(pool: &Pool, worker_id: &str, limit: i64) -> Result<Vec<TaskRun>> {
    claim_ready_classes(pool, worker_id, limit, &[], &std::collections::BTreeMap::new()).await
}

/// [`claim_ready`] restricted to a set of runner classes — the routing seam for
/// segmented runner pools (`RUNNER_CLASSES`). An empty slice claims every class
/// (the unsegmented scheduler). SQLite has no array binds, so the class set is
/// matched as a delimited string (`",a,b,"` contains `",<class>,"`); class names
/// are `[a-z0-9_-]`-validated so the delimiter cannot appear in a value.
pub async fn claim_ready_classes(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
    classes: &[String],
    pool_caps: &std::collections::BTreeMap<String, i64>,
) -> Result<Vec<TaskRun>> {
    claim_ready_filtered(pool, worker_id, limit, classes, pool_caps, false).await
}

/// Shared claim implementation behind [`claim_ready_classes`] and
/// [`claim_ready_classes_nongang`]. `exclude_gangs` adds `gang_id IS NULL` so the
/// gang-aware scheduler never claims a gang member individually — everything else
/// (priority ordering #25, per-pool budgets #21, class routing, the CAS loop) is
/// identical, so both callers get the same scheduling semantics.
async fn claim_ready_filtered(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
    classes: &[String],
    pool_caps: &std::collections::BTreeMap<String, i64>,
    exclude_gangs: bool,
) -> Result<Vec<TaskRun>> {
    let class_set = if classes.is_empty() {
        String::new()
    } else {
        format!(",{},", classes.join(","))
    };
    let mut tx = pool.begin().await?;

    let now = chrono::Utc::now().to_rfc3339();

    // Per-pool concurrency budgets (#21): remaining slots for each configured
    // pool = capacity − currently running. Empty `pool_caps` ⇒ no gating, so the
    // claim is byte-for-byte the original unpooled query. SQLite is
    // single-writer, so computing budgets and claiming in one write transaction
    // is race-free. `exhausted_set` (a `,a,b,` delimited string — pool names are
    // validated `[a-z0-9_-]`, so a name can never contain the delimiter)
    // pre-excludes full pools from the candidate scan so a full pool at the head
    // of the queue can't starve claimable work behind it.
    let mut avail: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut exhausted_set = String::new();
    if !pool_caps.is_empty() {
        let mut exhausted: Vec<&str> = Vec::new();
        for (name, cap) in pool_caps {
            let running: i64 = sqlx::query_scalar(
                POOL_RUNNING_COUNT,
            )
            .bind(name)
            .fetch_one(&mut *tx)
            .await?;
            let remaining = (cap - running).max(0);
            if remaining == 0 {
                exhausted.push(name);
            }
            avail.insert(name.clone(), remaining);
        }
        if !exhausted.is_empty() {
            exhausted_set = format!(",{},", exhausted.join(","));
        }
    }

    let candidates: Vec<TaskRun> = sqlx::query_as::<_, TaskRun>(
        "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at, pool
         FROM task_runs
         WHERE status = 'ready'
           AND (scheduled_at IS NULL OR scheduled_at <= ?1)
           AND (?2 = '' OR instr(?2, ',' || runner_class || ',') > 0)
           AND (?4 = '' OR pool IS NULL OR instr(?4, ',' || pool || ',') = 0)
           AND (?5 = 0 OR gang_id IS NULL)
         ORDER BY priority DESC, scheduled_at
         LIMIT ?3",
    )
    .bind(&now)
    .bind(&class_set)
    .bind(limit)
    .bind(&exhausted_set)
    .bind(i64::from(exclude_gangs))
    .fetch_all(&mut *tx)
    .await?;

    if candidates.is_empty() {
        tx.commit().await?;
        return Ok(vec![]);
    }

    let lease_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(30)).to_rfc3339();
    let mut claimed = Vec::with_capacity(candidates.len());

    for task in candidates {
        // A pooled task claims only while its pool still has a free slot; a pool
        // draining to zero mid-batch skips its remaining tasks (they claim on a
        // later tick). Unpooled and uncapped-pool tasks are never gated.
        if let Some(p) = &task.pool {
            if let Some(remaining) = avail.get(p) {
                if *remaining <= 0 {
                    continue;
                }
            }
        }
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
            if let Some(p) = &task.pool {
                if let Some(remaining) = avail.get_mut(p) {
                    *remaining -= 1;
                }
            }
            claimed.push(task);
        } else {
            tracing::warn!(task_id = %task.id, "CAS miss — skipping");
        }
    }

    tx.commit().await?;
    Ok(claimed)
}

/// All-or-nothing gang claim: seize every member of ONE gang whose members are
/// all `ready` and whose size fits `capacity` — or nothing (single-writer
/// SQLite serializes racers; a lost race matches zero `ready` rows). Ordinary
/// claims must then use [`claim_ready_classes_nongang`] so members are never
/// claimed individually. The gang-aware scheduler (RUNNER_GANGS) drives this.
pub async fn claim_ready_gang(
    pool: &Pool,
    worker_id: &str,
    capacity: i64,
    classes: &[String],
    pool_caps: &std::collections::BTreeMap<String, i64>,
) -> Result<Vec<TaskRun>> {
    if capacity < 2 {
        return Ok(Vec::new()); // no gang fits in fewer than two slots
    }
    let class_set = if classes.is_empty() {
        String::new()
    } else {
        format!(",{},", classes.join(","))
    };
    let now = chrono::Utc::now().to_rfc3339();
    // Budget resolution, gang selection, and the claiming UPDATE run in ONE write
    // transaction. SQLite serializes writers, so holding the transaction across
    // the pool count and the claim is what makes the budget check atomic against
    // ordinary pooled claims — counting outside it would let a gang and a
    // concurrent pooled claim both see the same free slots and jointly exceed the
    // pool's cap.
    let mut tx = pool.begin().await?;
    // Candidate gangs: fully `ready`, fitting this scheduler's free capacity.
    // Highest member priority first (#25), so an urgent gang is seized before a
    // routine one — matching the ordinary claim's ordering.
    let candidates: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT gang_id, gang_size, pool FROM task_runs
         WHERE status = 'ready' AND gang_id IS NOT NULL
           AND (scheduled_at IS NULL OR scheduled_at <= ?1)
           AND (?2 = '' OR instr(?2, ',' || runner_class || ',') > 0)
         GROUP BY gang_id, gang_size, pool
         HAVING COUNT(*) = gang_size AND gang_size <= ?3
         ORDER BY MAX(priority) DESC, MIN(scheduled_at)",
    )
    .bind(&now)
    .bind(&class_set)
    .bind(capacity)
    .fetch_all(&mut *tx)
    .await?;

    // A gang is all-or-nothing, so a pooled gang needs `gang_size` free slots in
    // its pool at once (#21 × gang co-scheduling): claiming it partially would
    // over-commit the pool, and claiming none keeps the pool's cap honest. Skip
    // to the next candidate gang when the budget is short.
    let mut chosen: Option<String> = None;
    for (gang_id, size, gang_pool) in candidates {
        if let Some(p) = &gang_pool {
            if let Some(cap) = pool_caps.get(p) {
                let running: i64 = sqlx::query_scalar(
                    POOL_RUNNING_COUNT,
                )
                .bind(p)
                .fetch_one(&mut *tx)
                .await?;
                if (cap - running).max(0) < size {
                    continue; // pool can't seat the whole gang yet
                }
            }
        }
        chosen = Some(gang_id);
        break;
    }
    let Some(gang_id) = chosen else {
        tx.commit().await?;
        return Ok(Vec::new());
    };

    let lease_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(30)).to_rfc3339();
    let claimed = sqlx::query_as::<_, TaskRun>(
        "UPDATE task_runs
         SET status = 'running',
             claimed_by = ?,
             lease_expires_at = ?,
             attempt = attempt + 1,
             version = version + 1
         WHERE gang_id = ? AND status = 'ready'
         RETURNING id, run_id, name, status,
                   attempt - 1 AS attempt, remaining_deps,
                   input, output, claimed_by, lease_expires_at,
                   version - 1 AS version, scheduled_at, finished_at, pool",
    )
    .bind(worker_id)
    .bind(&lease_exp)
    .bind(&gang_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(claimed)
}

/// [`claim_ready_classes`] that skips gang members — pair with
/// [`claim_ready_gang`] so gangs are only ever claimed whole. Shares
/// [`claim_ready_filtered`] with the ordinary claim, so priority ordering (#25)
/// and per-pool budgets (#21) apply on the gang-aware path too.
pub async fn claim_ready_classes_nongang(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
    classes: &[String],
    pool_caps: &std::collections::BTreeMap<String, i64>,
) -> Result<Vec<TaskRun>> {
    claim_ready_filtered(pool, worker_id, limit, classes, pool_caps, true).await
}

/// Ready gangs whose size exceeds `max_size` — the feed for the scheduler's
/// "this gang can never fit my pool" alarm (an oversized gang would otherwise
/// sit `ready` silently forever on a fleet with no bigger scheduler).
pub async fn oversized_ready_gangs(pool: &Pool, max_size: i64) -> Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT gang_id, gang_size FROM task_runs
         WHERE status = 'ready' AND gang_id IS NOT NULL AND gang_size > ?
         GROUP BY gang_id, gang_size",
    )
    .bind(max_size)
    .fetch_all(pool)
    .await?)
}

/// Die-together: a gang member failed — cancel every non-terminal sibling.
/// The version bump fences running siblings: their next heartbeat renewal
/// fails the claim-triple check and aborts their local execution, and any
/// late result write is rejected. Returns siblings cancelled.
pub async fn cancel_gang_siblings(pool: &Pool, member_id: &str) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    Ok(sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', claimed_by = NULL, lease_expires_at = NULL,
             version = version + 1, finished_at = ?1,
             output = COALESCE(output, 'cancelled: gang member failed')
         WHERE gang_id IS NOT NULL
           AND gang_id = (SELECT gang_id FROM task_runs WHERE id = ?2)
           AND id != ?2
           AND status IN ('pending', 'ready', 'running')",
    )
    .bind(&now)
    .bind(member_id)
    .execute(pool)
    .await?
    .rows_affected())
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
    mark_task_succeeded_inner(pool, task_id, worker_id, fence, output, false).await
}

/// [`mark_task_succeeded`] for a task resolved from the memoization store (#22)
/// instead of executed: identical mutation, plus `cache_hit` stamped in the
/// *same* transaction so the flag can never disagree with the success it
/// describes. Separate entry point rather than a parameter, so the six ordinary
/// completion call sites stay untouched.
pub async fn mark_task_succeeded_cached(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    output: Option<String>,
) -> Result<bool> {
    mark_task_succeeded_inner(pool, task_id, worker_id, fence, output, true).await
}

async fn mark_task_succeeded_inner(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    output: Option<String>,
    cache_hit: bool,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'succeeded', finished_at = ?, output = ?, claimed_by = NULL,
             cache_hit = ?
         WHERE id = ? AND claimed_by = ? AND version = ?",
    )
    .bind(&now)
    .bind(&output)
    .bind(cache_hit)
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
///
/// Not `ops`-gated: the reconcile loop's timeout sweep
/// ([`resolve_expired_approvals`]) needs it in every build — a lean engine
/// with an approval gate must still fail-safe the gate when its timeout
/// elapses, management API or not.
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
/// no-op, so a re-sweep does nothing. Not `ops`-gated — the reconcile loop
/// calls this every tick in every build.
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

/// The registered workflow spec YAML for `name`, or `None` if no such workflow —
/// resolves a `type: workflow` task's target at trigger time (#23).
pub async fn workflow_spec_by_name(pool: &Pool, name: &str) -> Result<Option<String>> {
    let spec: Option<String> = sqlx::query_scalar("SELECT spec FROM workflows WHERE name = ?")
        .bind(name)
        .fetch_optional(pool)
        .await?;
    Ok(spec)
}

/// Park a claimed `type: workflow` task on its child run (#23): keep it `running`
/// but drop the worker claim/lease and record `sub_run_id`, so no worker touches
/// it and lease recovery leaves it alone (NULL lease) until the sub-workflow
/// sweep resolves it. Fence-guarded to this claim; returns whether it parked.
/// How deep `run_id` sits in the sub-workflow chain (#23): `0` for a run nobody
/// triggered, `1` for a run a task in a root run spawned, and so on.
///
/// Walks `task_runs.sub_run_id` **backwards** — the parked parent of a run is
/// the task holding that run id — which is the only parentage the schema
/// records. That avoids a `parent_run_id` column and the migration to add it;
/// migration 034's partial index makes each hop a lookup rather than a scan.
///
/// `limit` caps the walk. Callers only ever need to know "deeper than N", so
/// stopping at `N` keeps the cost O(N) instead of O(chain), and — since nothing
/// in the schema *forbids* a cycle, only the code paths that write the column —
/// it also guarantees termination on malformed data rather than spinning
/// forever inside the guard meant to prevent runaway recursion.
pub async fn sub_workflow_depth(pool: &Pool, run_id: &str, limit: i64) -> Result<i64> {
    let mut current = run_id.to_string();
    let mut depth: i64 = 0;
    while depth < limit {
        let parent: Option<(String,)> =
            sqlx::query_as("SELECT run_id FROM task_runs WHERE sub_run_id = ? LIMIT 1")
                .bind(&current)
                .fetch_optional(pool)
                .await?;
        match parent {
            Some((parent_run,)) => {
                depth += 1;
                current = parent_run;
            }
            None => break,
        }
    }
    Ok(depth)
}

pub async fn park_subworkflow(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    child_run_id: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET sub_run_id = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ?",
    )
    .bind(child_run_id)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Release a claimed `type: workflow` task back to `ready` (#23) — used when the
/// child workflow can't start right now (e.g. at its own `max_active_runs` cap),
/// so the trigger retries on a later tick. Fence-guarded.
pub async fn release_subworkflow_task(pool: &Pool, task_id: &str, fence: i64) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ?",
    )
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Resolve a parked sub-workflow task (#23) once its child run is terminal:
/// `succeeded` → the task succeeds and dependents advance; otherwise it fails
/// (its `all_success` dependents skip). Guarded on the parked shape
/// (`status = 'running' AND sub_run_id IS NOT NULL`), so a re-sweep is a no-op.
pub async fn resolve_subworkflow(pool: &Pool, task_id: &str, succeeded: bool) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) = if succeeded {
        ("succeeded", "sub-workflow succeeded")
    } else {
        ("failed", "sub-workflow failed")
    };
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = ?, finished_at = ?, output = ?
         WHERE id = ? AND status = 'running' AND sub_run_id IS NOT NULL",
    )
    .bind(status)
    .bind(&now)
    .bind(output)
    .bind(task_id)
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Sweep parked sub-workflow tasks (#23): for each `running` task holding a
/// `sub_run_id`, if its child run is terminal, resolve the parent (succeed/fail).
/// Returns the `(task_id, succeeded)` resolutions. Run each reconcile tick;
/// idempotent via `resolve_subworkflow`'s guard.
pub async fn reconcile_subworkflows(pool: &Pool) -> Result<Vec<(String, bool)>> {
    let parked: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, sub_run_id FROM task_runs
         WHERE status = 'running' AND sub_run_id IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    let mut resolved = Vec::new();
    for (task_id, child_run_id) in parked {
        let child_status: Option<String> =
            sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = ?")
                .bind(&child_run_id)
                .fetch_optional(pool)
                .await?;
        let Some(cs) = child_status else { continue }; // child gone → leave parked
        let succeeded = match cs.as_str() {
            "succeeded" => true,
            "failed" | "cancelled" => false,
            _ => continue, // still running/pending
        };
        if resolve_subworkflow(pool, &task_id, succeeded).await? {
            resolved.push((task_id, succeeded));
        }
    }
    Ok(resolved)
}

/// Park a claimed `type: wait` task on its resume deadline (#27): keep it
/// `running` but drop the worker claim/lease and record `wake_at`, so no worker
/// holds a slot and lease recovery leaves it alone until the wait sweep resolves
/// it at the deadline. Fence-guarded; returns whether it parked.
pub async fn park_wait(pool: &Pool, task_id: &str, fence: i64, wake_at: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET wake_at = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ?",
    )
    .bind(wake_at)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Sweep parked wait sensors (#27): resolve (succeed) every `running` task whose
/// `wake_at` deadline has passed, advancing its dependents. Returns the resolved
/// task ids. Run each reconcile tick; the guarded UPDATE is idempotent and
/// HA-safe (only one scheduler resolves a given task).
pub async fn reconcile_waits(pool: &Pool) -> Result<Vec<String>> {
    let now = chrono::Utc::now().to_rfc3339();
    let due: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM task_runs
         WHERE status = 'running' AND wake_at IS NOT NULL AND wake_at <= ?",
    )
    .bind(&now)
    .fetch_all(pool)
    .await?;

    let mut resolved = Vec::new();
    for (task_id,) in due {
        let mut tx = pool.begin().await?;
        let rows = sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', finished_at = ?, output = 'wait elapsed'
             WHERE id = ? AND status = 'running' AND wake_at IS NOT NULL AND wake_at <= ?",
        )
        .bind(&now)
        .bind(&task_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows == 0 {
            tx.commit().await?;
            continue;
        }
        sqlx::query(
            "UPDATE task_runs SET remaining_deps = remaining_deps - 1
             WHERE id IN (
                 SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
             ) AND status = 'pending'",
        )
        .bind(&task_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        resolved.push(task_id);
    }
    Ok(resolved)
}

/// Park a claimed `type: wait` HTTP sensor (#27 follow-on): keep it `running`
/// but drop the worker claim/lease and record the endpoint in `wait_url` with an
/// immediate `next_poll_at`, so no worker holds a slot and the engine's poll
/// sweep GETs it starting on the next tick. Carries no `wake_at` (that column
/// distinguishes a time sensor). Fence-guarded; returns whether it parked.
pub async fn park_wait_url(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    url: &str,
    next_poll_at: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET wait_url = ?, next_poll_at = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ?",
    )
    .bind(url)
    .bind(next_poll_at)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// List up to `limit` parked HTTP wait sensors (#27 follow-on) due for a poll
/// (oldest deadline first — the cap bounds how much probing one tick can do):
/// `running` tasks holding a `wait_url` whose `next_poll_at` has elapsed (or is
/// NULL). Returns `(task_id, wait_url)` pairs for the engine to GET.
pub async fn due_url_waits(pool: &Pool, limit: i64) -> Result<Vec<(String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, wait_url FROM task_runs
         WHERE status = 'running' AND wait_url IS NOT NULL
           AND (next_poll_at IS NULL OR next_poll_at <= ?1)
         ORDER BY next_poll_at
         LIMIT ?2",
    )
    .bind(&now)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Push a parked HTTP wait sensor's next poll out to `next_poll_at` (#27
/// follow-on) after a non-2xx / errored GET, so the sweep re-polls at the fixed
/// interval instead of hot-looping. Guarded on the parked shape; idempotent.
pub async fn repark_url_wait(pool: &Pool, task_id: &str, next_poll_at: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs SET next_poll_at = ?
         WHERE id = ? AND status = 'running' AND wait_url IS NOT NULL",
    )
    .bind(next_poll_at)
    .bind(task_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Resolve a parked HTTP wait sensor (#27 follow-on) once its endpoint returned a
/// 2xx: succeed the task and advance its dependents. Guarded on the parked shape
/// (`status = 'running' AND wait_url IS NOT NULL`), so a concurrent re-sweep is a
/// no-op and only one scheduler resolves it (HA-safe).
pub async fn resolve_url_wait(pool: &Pool, task_id: &str) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = 'succeeded', finished_at = ?, output = 'wait url ready'
         WHERE id = ? AND status = 'running' AND wait_url IS NOT NULL",
    )
    .bind(&now)
    .bind(task_id)
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

// ── datasets (produce → track → trigger; data-aware scheduling) ──────────────

/// Record dataset updates from a succeeded `produces:` task: upsert each URI in
/// the `datasets` registry and append one `dataset_events` lineage row. The
/// events' monotonically increasing ids are what dataset sensors and triggers
/// key their cursors off. One transaction, so a multi-URI task is all-or-nothing.
pub async fn record_dataset_updates(
    pool: &Pool,
    workflow: &str,
    task_id: &str,
    task_name: &str,
    uris: &[String],
) -> Result<()> {
    if uris.is_empty() {
        return Ok(());
    }
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    // The producing run comes off the task row itself, so callers (the worker
    // result path) don't need to carry it.
    let run_id: Option<String> =
        sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = ?")
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?;
    for uri in uris {
        sqlx::query(
            "INSERT INTO datasets (uri, updated_at, last_run_id, last_task, updates)
             VALUES (?, ?, ?, ?, 1)
             ON CONFLICT(uri) DO UPDATE SET
                 updated_at = excluded.updated_at,
                 last_run_id = excluded.last_run_id,
                 last_task = excluded.last_task,
                 updates = datasets.updates + 1",
        )
        .bind(uri)
        .bind(&now)
        .bind(&run_id)
        .bind(task_name)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO dataset_events (uri, workflow, run_id, task_id, task_name, source, at)
             VALUES (?, ?, ?, ?, ?, 'task', ?)",
        )
        .bind(uri)
        .bind(workflow)
        .bind(&run_id)
        .bind(task_id)
        .bind(task_name)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Record an **external** dataset event (dagron Enterprise: producers outside
/// dagron — CDC, object-store notifications — post updates via the API). Same
/// ledger, `source = 'external'`; sensors and triggers see it identically.
pub async fn record_external_dataset_event(pool: &Pool, uri: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO datasets (uri, updated_at, last_run_id, last_task, updates)
         VALUES (?, ?, NULL, NULL, 1)
         ON CONFLICT(uri) DO UPDATE SET
             updated_at = excluded.updated_at, updates = datasets.updates + 1",
    )
    .bind(uri)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO dataset_events (uri, source, at) VALUES (?, 'external', ?)",
    )
    .bind(uri)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// All registered workflows `(name, spec)` — the dataset-trigger sweep parses
/// each spec's `on_datasets:` to keep `dataset_triggers` subscriptions in sync.
pub async fn list_registered_workflows(pool: &Pool) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as("SELECT name, spec FROM workflows")
        .fetch_all(pool)
        .await?)
}

/// Sync one workflow's dataset subscriptions to exactly `uris` (mode `any`|`all`):
/// removed URIs are deleted, new ones inserted with their cursor initialized to
/// the dataset's **current** high-water mark — registering a trigger never fires
/// on history, only on updates that arrive after it. Existing rows keep their
/// cursor (re-sync is not a reset) but follow a mode change.
pub async fn sync_dataset_triggers(
    pool: &Pool,
    workflow_name: &str,
    uris: &[String],
    mode: &str,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    if uris.is_empty() {
        sqlx::query("DELETE FROM dataset_triggers WHERE workflow_name = ?")
            .bind(workflow_name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(());
    }
    // Delete subscriptions the spec no longer declares. The set is matched as a
    // delimited string (uris are validated whitespace-free, but may contain
    // commas — use a unit separator, which validate_dataset_uri's control-char
    // ban makes impossible in a URI).
    let keep = format!("\u{1f}{}\u{1f}", uris.join("\u{1f}"));
    sqlx::query(
        "DELETE FROM dataset_triggers
         WHERE workflow_name = ? AND instr(?, char(31) || uri || char(31)) = 0",
    )
    .bind(workflow_name)
    .bind(&keep)
    .execute(&mut *tx)
    .await?;
    for uri in uris {
        sqlx::query(
            "INSERT INTO dataset_triggers (workflow_name, uri, cursor, mode)
             VALUES (?, ?, (SELECT COALESCE(MAX(id), 0) FROM dataset_events WHERE uri = ?), ?)
             ON CONFLICT(workflow_name, uri) DO UPDATE SET mode = excluded.mode",
        )
        .bind(workflow_name)
        .bind(uri)
        .bind(uri)
        .bind(mode)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Drop subscriptions whose workflow is no longer registered (deleted/renamed),
/// so orphaned trigger rows can't accumulate. Run with the sweep; idempotent.
pub async fn prune_dataset_triggers(pool: &Pool) -> Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM dataset_triggers
         WHERE workflow_name NOT IN (SELECT name FROM workflows)",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

/// Claim dataset triggers that are due to fire. For each subscribed workflow:
/// `any` mode fires when **any** subscribed dataset has an event newer than its
/// cursor; `all` mode (Enterprise composition) fires only when **every**
/// subscribed dataset does. Claiming CAS-advances the fresh cursors to their
/// current high-water marks in one transaction — the scheduler that wins the
/// CAS owns the fire (HA-safe with no leadership; losers roll back and skip).
/// Multiple updates to one dataset coalesce into a single fire.
/// One `dataset_triggers` subscription in the claim snapshot:
/// `(uri, cursor, mode, current max event id)`.
type TriggerSub = (String, i64, String, i64);

pub async fn claim_due_dataset_triggers(pool: &Pool) -> Result<Vec<crate::models::DatasetFire>> {
    // Snapshot all subscriptions with each dataset's current max event id.
    let rows: Vec<(String, String, i64, String, i64)> = sqlx::query_as(
        "SELECT t.workflow_name, t.uri, t.cursor, t.mode,
                COALESCE((SELECT MAX(id) FROM dataset_events e WHERE e.uri = t.uri), 0)
         FROM dataset_triggers t
         ORDER BY t.workflow_name, t.uri",
    )
    .fetch_all(pool)
    .await?;

    // Group by workflow, preserving order.
    let mut by_wf: Vec<(String, Vec<TriggerSub>)> = Vec::new();
    for (wf, uri, cursor, mode, max_id) in rows {
        match by_wf.last_mut() {
            Some((name, subs)) if *name == wf => subs.push((uri, cursor, mode, max_id)),
            _ => by_wf.push((wf, vec![(uri, cursor, mode, max_id)])),
        }
    }

    let mut fires = Vec::new();
    for (wf, subs) in by_wf {
        let fresh: Vec<&TriggerSub> =
            subs.iter().filter(|(_, cursor, _, max_id)| max_id > cursor).collect();
        if fresh.is_empty() {
            continue;
        }
        // 'all' composition: every subscription must be fresh before firing.
        let mode_all = subs.iter().any(|(_, _, m, _)| m == "all");
        if mode_all && fresh.len() < subs.len() {
            continue;
        }
        // CAS-advance every fresh cursor; all must win or the claim rolls back
        // (another scheduler owns this fire).
        let mut tx = pool.begin().await?;
        let mut advanced = Vec::with_capacity(fresh.len());
        let mut won = true;
        for (uri, cursor, _, max_id) in &fresh {
            let n = sqlx::query(
                "UPDATE dataset_triggers SET cursor = ?
                 WHERE workflow_name = ? AND uri = ? AND cursor = ?",
            )
            .bind(max_id)
            .bind(&wf)
            .bind(uri)
            .bind(cursor)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if n == 0 {
                won = false;
                break;
            }
            advanced.push((uri.clone(), *cursor, *max_id));
        }
        if !won {
            tx.rollback().await?;
            continue;
        }
        tx.commit().await?;
        fires.push(crate::models::DatasetFire {
            workflow_name: wf,
            trigger_uri: fresh[0].0.clone(),
            advanced,
        });
    }
    Ok(fires)
}

/// Roll a claimed fire's cursors back (guarded on the values we advanced them
/// to), so a fire whose run couldn't be created — e.g. the workflow is at its
/// `max_active_runs` cap — is retried by a later sweep instead of lost.
pub async fn unclaim_dataset_trigger(
    pool: &Pool,
    workflow_name: &str,
    advanced: &[(String, i64, i64)],
) -> Result<()> {
    for (uri, prev, new) in advanced {
        sqlx::query(
            "UPDATE dataset_triggers SET cursor = ?
             WHERE workflow_name = ? AND uri = ? AND cursor = ?",
        )
        .bind(prev)
        .bind(workflow_name)
        .bind(uri)
        .bind(new)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Stamp a fired trigger with its run for observability (best-effort).
pub async fn stamp_dataset_trigger_fired(
    pool: &Pool,
    workflow_name: &str,
    run_id: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "UPDATE dataset_triggers SET last_fired_at = ?, last_fired_run_id = ?
         WHERE workflow_name = ?",
    )
    .bind(&now)
    .bind(run_id)
    .bind(workflow_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Park a claimed `type: wait` dataset sensor: keep it `running`, drop the
/// worker claim/lease, record the dataset and the ledger's **current**
/// high-water mark — only an update recorded after the park resolves it (fresh
/// data, not any data). Fence-guarded; returns whether it parked.
pub async fn park_wait_dataset(pool: &Pool, task_id: &str, fence: i64, uri: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET wait_dataset = ?,
             wait_dataset_cursor = (SELECT COALESCE(MAX(id), 0) FROM dataset_events WHERE uri = ?),
             claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ?",
    )
    .bind(uri)
    .bind(uri)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Sweep parked dataset sensors: resolve (succeed) every one whose dataset has
/// an event newer than its park-time cursor, advancing dependents. Returns
/// `(task_id, uri)` resolutions. Idempotent, HA-safe (guarded UPDATE).
pub async fn reconcile_dataset_waits(pool: &Pool) -> Result<Vec<(String, String)>> {
    let due: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, wait_dataset FROM task_runs t
         WHERE status = 'running' AND wait_dataset IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM dataset_events e
               WHERE e.uri = t.wait_dataset AND e.id > COALESCE(t.wait_dataset_cursor, 0)
           )",
    )
    .fetch_all(pool)
    .await?;

    let now = chrono::Utc::now().to_rfc3339();
    let mut resolved = Vec::new();
    for (task_id, uri) in due {
        let mut tx = pool.begin().await?;
        let rows = sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', finished_at = ?, output = 'dataset updated'
             WHERE id = ? AND status = 'running' AND wait_dataset IS NOT NULL",
        )
        .bind(&now)
        .bind(&task_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if rows == 0 {
            tx.commit().await?;
            continue;
        }
        sqlx::query(
            "UPDATE task_runs SET remaining_deps = remaining_deps - 1
             WHERE id IN (
                 SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
             ) AND status = 'pending'",
        )
        .bind(&task_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        resolved.push((task_id, uri));
    }
    Ok(resolved)
}

/// The dataset registry, most recently updated first (management API read).
#[cfg(feature = "ops")]
pub async fn list_datasets(
    pool: &Pool,
    limit: i64,
) -> Result<Vec<(String, String, Option<String>, Option<String>, i64)>> {
    Ok(sqlx::query_as(
        "SELECT uri, updated_at, last_run_id, last_task, updates
         FROM datasets ORDER BY updated_at DESC LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

/// Lineage read: dataset update events, newest first, optionally for one URI
/// (management API read). Row = (id, uri, workflow, run_id, task_name, source, at).
#[cfg(feature = "ops")]
#[allow(clippy::type_complexity)]
pub async fn list_dataset_events(
    pool: &Pool,
    uri: Option<&str>,
    limit: i64,
) -> Result<Vec<(i64, String, Option<String>, Option<String>, Option<String>, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT id, uri, workflow, run_id, task_name, source, at
         FROM dataset_events
         WHERE (? IS NULL OR uri = ?)
         ORDER BY id DESC LIMIT ?",
    )
    .bind(uri)
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await?)
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

// ── Long-task primitives: heartbeat lease renewal + checkpoint pointers ──────

/// Renew a running task's lease — the worker heartbeat that lets a task run
/// longer than the claim-time lease window without being reclaimed mid-run
/// (spot preemption tolerance for training jobs, long consumers, any task whose
/// `timeout_secs` exceeds the lease). Guarded by the full claim triple
/// (`claimed_by` + post-claim `version` + `status = 'running'`), so a worker
/// whose task was already reclaimed cannot resurrect the old lease. Returns
/// `false` when the claim is gone — the caller must abort its local execution:
/// the task now belongs to (or is queued for) another attempt.
pub async fn renew_task_lease(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    extend_secs: i64,
) -> Result<bool> {
    let new_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(extend_secs)).to_rfc3339();
    let rows = sqlx::query(
        "UPDATE task_runs
         SET lease_expires_at = ?
         WHERE id = ? AND claimed_by = ? AND version = ? AND status = 'running'",
    )
    .bind(&new_exp)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows == 1)
}

/// Record the latest committed checkpoint pointer for a **running** task
/// (checkpoint-aware resume). The task itself owns what the checkpoint
/// contains; dagron stores only the pointer (`uri`) plus an optional
/// step/epoch `marker`, and hands both back on the next attempt via
/// `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER`. Scoped to `run_id` so the
/// management route can't cross runs; restricted to `running` so a finished
/// or reclaimed attempt can't overwrite a newer attempt's progress with a
/// stale pointer. Returns `false` when no such running task exists.
pub async fn record_task_checkpoint(
    pool: &Pool,
    run_id: &str,
    task_id: &str,
    uri: &str,
    marker: Option<&str>,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET checkpoint_uri = ?, checkpoint_marker = ?
         WHERE id = ? AND run_id = ? AND status = 'running'",
    )
    .bind(uri)
    .bind(marker)
    .bind(task_id)
    .bind(run_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows == 1)
}

/// The carried-forward checkpoint pointer for a task — `(uri, marker)` —
/// consulted at dispatch of retry attempts to inject `DAGRON_RESUME_FROM`.
/// `None` when the task never reported a checkpoint.
pub async fn task_checkpoint(
    pool: &Pool,
    task_id: &str,
) -> Result<Option<(String, Option<String>)>> {
    let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT checkpoint_uri, checkpoint_marker FROM task_runs WHERE id = ?",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(uri, marker)| uri.map(|u| (u, marker))))
}

/// Placement fallback: move **ready** tasks of `from_class` to `to_class` when
/// they have either waited claimable for at least `min_ready_age_secs` (the
/// pool is starved/unserved) or already burned `min_attempt`+ attempts
/// (repeated preemption). The policy loop deciding *when* to call this lives
/// outside the engine; this is the atomic reclassification primitive. Only
/// unclaimed `ready` rows move, so no fence is disturbed. Returns tasks moved.
pub async fn reclass_ready_tasks(
    pool: &Pool,
    from_class: &str,
    to_class: &str,
    min_ready_age_secs: i64,
    min_attempt: i64,
) -> Result<u64> {
    let aged_before =
        (chrono::Utc::now() - chrono::TimeDelta::seconds(min_ready_age_secs)).to_rfc3339();
    let rows = sqlx::query(
        "UPDATE task_runs
         SET runner_class = ?
         WHERE status = 'ready'
           AND runner_class = ?
           AND gang_id IS NULL
           AND (
                 (scheduled_at IS NOT NULL AND scheduled_at <= ?)
              OR attempt >= ?
           )",
    )
    .bind(to_class)
    .bind(from_class)
    .bind(&aged_before)
    .bind(min_attempt)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows)
}

/// Deadline-urgency variant of [`reclass_ready_tasks`]: move **ready** tasks
/// of `from_class` whose *run* is within `within_secs` of its deadline
/// (hard `run_timeout_secs` deadline first, else the soft `deadline:` alert)
/// to `to_class` — the "stop waiting for spot, the SLA is close" policy.
/// Ready-only, so no fence is disturbed.
pub async fn reclass_ready_tasks_near_deadline(
    pool: &Pool,
    from_class: &str,
    to_class: &str,
    within_secs: i64,
) -> Result<u64> {
    let horizon =
        (chrono::Utc::now() + chrono::TimeDelta::seconds(within_secs)).to_rfc3339();
    Ok(sqlx::query(
        "UPDATE task_runs
         SET runner_class = ?
         WHERE status = 'ready'
           AND runner_class = ?
           AND gang_id IS NULL
           AND run_id IN (
               SELECT id FROM workflow_runs
               WHERE status = 'running'
                 AND COALESCE(deadline_at, alert_deadline_at) IS NOT NULL
                 AND COALESCE(deadline_at, alert_deadline_at) <= ?
           )",
    )
    .bind(to_class)
    .bind(from_class)
    .bind(&horizon)
    .execute(pool)
    .await?
    .rows_affected())
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
             output = NULL, finished_at = NULL, scheduled_at = ?, version = version + 1,
             checkpoint_uri = NULL, checkpoint_marker = NULL
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
         -- A paused or retired workflow does not fire, and this is the only
         -- place that can enforce it: a state the scheduler ignores is a label,
         -- not a control. Deliberately separate from s.enabled, which disables
         -- one schedule; this stops the workflow however many schedules it has.
         WHERE w.state = 'active'
           AND s.enabled = 1
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
         -- Same gate as claim_due_schedules: a paused workflow must not catch
         -- up either, or pausing would merely defer the backlog rather than
         -- stop it.
         WHERE w.state = 'active' AND s.enabled = 1 AND s.catchup = 1",
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

/// Workflow (definition) name for a task by its id — the context a memo write
/// needs when only the task id is at hand (#22).
pub async fn workflow_name_for_task(pool: &Pool, task_id: &str) -> Result<Option<String>> {
    let name: Option<String> = sqlx::query_scalar(
        "SELECT d.name FROM task_runs tr
         JOIN workflow_runs wr ON wr.id = tr.run_id
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE tr.id = ?",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(name)
}

/// Look up a memoized task result (#22): the cached output when a memo exists for
/// `(workflow, task, key)` and is younger than `max_age_secs` (if set). A stale
/// entry misses (returns `None`) so the task re-runs and refreshes it.
pub async fn memo_lookup(
    pool: &Pool,
    workflow: &str,
    task: &str,
    key: &str,
    max_age_secs: Option<u64>,
) -> Result<Option<String>> {
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT output, created_at FROM task_memo
         WHERE workflow_name = ? AND task_name = ? AND cache_key = ?",
    )
    .bind(workflow)
    .bind(task)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    let Some((output, created_at)) = row else {
        return Ok(None);
    };
    if let Some(max) = max_age_secs {
        // An unreadable stamp must fail *closed* — serving a memo whose age
        // cannot be established would defeat `max_age_secs` entirely.
        let Ok(created) = chrono::DateTime::parse_from_rfc3339(&created_at) else {
            return Ok(None);
        };
        let age = (chrono::Utc::now() - created.with_timezone(&chrono::Utc)).num_seconds();
        if age > max as i64 {
            return Ok(None);
        }
    }
    Ok(Some(output.unwrap_or_default()))
}

/// Store (upsert) a memoized task result (#22). Called after a cached task
/// succeeds; a later run with the same key reuses the output.
pub async fn memo_store(
    pool: &Pool,
    workflow: &str,
    task: &str,
    key: &str,
    output: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO task_memo (workflow_name, task_name, cache_key, output, created_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(workflow_name, task_name, cache_key)
         DO UPDATE SET output = excluded.output, created_at = excluded.created_at",
    )
    .bind(workflow)
    .bind(task)
    .bind(key)
    .bind(output)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
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

/// Read a dagron-api-owned `ui_settings` value (e.g. the instance-wide
/// notification defaults). The table may not exist on engine-only deployments;
/// callers should treat an `Err` as "no setting".
pub async fn ui_setting(pool: &Pool, key: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT value FROM ui_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
}

/// One task's stored TaskSpec JSON (the `input` column), for spec-driven
/// post-success decisions (the `repeat:` loop operator).
pub async fn task_input_json(pool: &Pool, task_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT input FROM task_runs WHERE id = ?")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|r| r.0))
}

// ── Environments (variable sets + encrypted secrets, spec `environment:`) ────

/// The environment name a run was created with (None = none declared).
pub async fn run_environment(pool: &Pool, run_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT environment FROM workflow_runs WHERE id = ?")
            .bind(run_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|r| r.0))
}

/// A named environment's plain variables. `None` = no such environment (the
/// caller decides whether that's an error); an unparseable `variables` blob is.
pub async fn environment_vars(
    pool: &Pool,
    name: &str,
) -> Result<Option<std::collections::BTreeMap<String, String>>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT variables FROM environments WHERE name = ?")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    match row {
        None => Ok(None),
        Some((json,)) => Ok(Some(
            serde_json::from_str(&json)
                .map_err(|e| anyhow::anyhow!("environment '{name}' variables unparseable: {e}"))?,
        )),
    }
}

/// The stored ciphertext of one environment secret (decrypted by the engine
/// via dagron-crypto at dispatch). `None` = not defined in that environment.
pub async fn environment_secret(
    pool: &Pool,
    env_name: &str,
    secret_name: &str,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT s.ciphertext FROM environment_secrets s
         JOIN environments e ON e.id = s.environment_id
         WHERE e.name = ? AND s.name = ?",
    )
    .bind(env_name)
    .bind(secret_name)
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
    record_dead_letter_inner(pool, payload, error, source, failures, None).await
}

/// [`record_dead_letter`] that also commits the source's cursor in the same
/// transaction — so a poison event advances the exactly-once position exactly
/// like a successful run does, and a restart never re-parks the same event.
pub async fn record_dead_letter_with_offset(
    pool: &Pool,
    payload: &str,
    error: &str,
    source: &str,
    failures: i64,
    position: &str,
) -> Result<String> {
    record_dead_letter_inner(pool, payload, error, source, failures, Some(position)).await
}

async fn record_dead_letter_inner(
    pool: &Pool,
    payload: &str,
    error: &str,
    source: &str,
    failures: i64,
    position: Option<&str>,
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
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
    .execute(&mut *tx)
    .await?;
    if let Some(position) = position {
        upsert_source_offset(&mut *tx, source, position, &now).await?;
    }
    tx.commit().await?;
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
                scheduled_at, finished_at, pool, priority, cache_hit,
                wake_at, wait_url, wait_dataset, sub_run_id
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
        ready_by_class: ready_backlog_by_class(pool).await?,
    })
}

/// Ready backlog grouped by runner class: count + oldest `scheduled_at`. The
/// per-class analog of the queue-depth gauge; in a segmented fleet a class no
/// live scheduler serves shows up here as an ever-growing `oldest` age.
#[cfg(feature = "ops")]
pub async fn ready_backlog_by_class(
    pool: &Pool,
) -> Result<Vec<crate::models::ReadyClassBacklog>> {
    let rows: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT runner_class, COUNT(*), MIN(scheduled_at)
         FROM task_runs WHERE status = 'ready'
         GROUP BY runner_class ORDER BY runner_class",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(runner_class, count, oldest_scheduled_at)| crate::models::ReadyClassBacklog {
            runner_class,
            count,
            oldest_scheduled_at,
        })
        .collect())
}

// ── v6 retention GC ─────────────────────────────────────────────────────────

/// Convert any engine row to JSON generically. The whole schema is TEXT +
/// integer columns by design (see migrations 001), so two decode attempts cover
/// every column — and the archiver survives future migrations without a
/// column-list edit.
#[cfg(feature = "ops")]
fn row_to_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    use sqlx::{Column, Row};
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let v = if let Ok(s) = row.try_get::<Option<String>, _>(i) {
            s.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null)
        } else if let Ok(n) = row.try_get::<Option<i64>, _>(i) {
            n.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null)
        } else if let Ok(f) = row.try_get::<Option<f64>, _>(i) {
            f.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };
        obj.insert(col.name().to_string(), v);
    }
    serde_json::Value::Object(obj)
}

/// Terminal runs finished before `cutoff`, each as a self-contained archive
/// document (`dagron.run-archive.v1`): the run row (+ its definition's
/// name/spec), every task row, and the run's outbox events. The archive-
/// before-purge GC writes these to the archive sink, then purges exactly the
/// ids it archived via [`purge_runs_by_id`] — so history leaves the hot store
/// only after it durably exists elsewhere (the hot/cold split).
#[cfg(feature = "ops")]
pub async fn archivable_runs(
    pool: &Pool,
    cutoff: &str,
    limit: i64,
) -> Result<Vec<(String, serde_json::Value)>> {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM workflow_runs
         WHERE status IN ('succeeded','failed','cancelled')
           AND finished_at IS NOT NULL AND finished_at < ?
         ORDER BY finished_at LIMIT ?",
    )
    .bind(cutoff)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let run = sqlx::query(
            "SELECT wr.*, wd.name AS definition_name, wd.spec AS definition_spec
             FROM workflow_runs wr
             JOIN workflow_definitions wd ON wd.id = wr.definition_id
             WHERE wr.id = ?",
        )
        .bind(&id)
        .fetch_one(pool)
        .await?;
        let tasks = sqlx::query("SELECT * FROM task_runs WHERE run_id = ? ORDER BY name")
            .bind(&id)
            .fetch_all(pool)
            .await?;
        let outbox =
            sqlx::query("SELECT * FROM event_outbox WHERE run_id = ? ORDER BY created_at")
                .bind(&id)
                .fetch_all(pool)
                .await?;
        let doc = serde_json::json!({
            "format": "dagron.run-archive.v1",
            "run": row_to_json(&run),
            "tasks": tasks.iter().map(row_to_json).collect::<Vec<_>>(),
            "outbox_events": outbox.iter().map(row_to_json).collect::<Vec<_>>(),
        });
        out.push((id, doc));
    }
    Ok(out)
}

/// Upsert one row into the `archived_runs` index — the listable map of what
/// left the hot store for the archive sink. Called by the GC sweep after the
/// sink write verifies and before the purge; re-archiving (a crash between
/// archive and purge) just refreshes `archived_at`.
#[cfg(feature = "ops")]
pub async fn index_archived_run(
    pool: &Pool,
    run_id: &str,
    name: &str,
    status: &str,
    created_at: Option<&str>,
    finished_at: Option<&str>,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO archived_runs (run_id, name, status, created_at, finished_at, archived_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (run_id) DO UPDATE SET
             name = excluded.name, status = excluded.status,
             created_at = excluded.created_at, finished_at = excluded.finished_at,
             archived_at = excluded.archived_at",
    )
    .bind(run_id)
    .bind(name)
    .bind(status)
    .bind(created_at)
    .bind(finished_at)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Stamp index rows as compacted into `parquet_path` — after this the runs'
/// per-run JSON documents are gone and history reads answer "analytics only".
/// Called by `dagron archive-compact` after the Parquet part file verifies.
#[cfg(feature = "ops")]
pub async fn mark_runs_compacted(
    pool: &Pool,
    run_ids: &[String],
    parquet_path: &str,
) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut stamped = 0u64;
    for id in run_ids {
        stamped += sqlx::query(
            "UPDATE archived_runs SET compacted_at = ?, parquet_path = ? WHERE run_id = ?",
        )
        .bind(&now)
        .bind(parquet_path)
        .bind(id)
        .execute(pool)
        .await?
        .rows_affected();
    }
    Ok(stamped)
}

/// Purge exactly these runs (dependency edges → tasks → run, then orphaned
/// definitions) in one transaction. Only still-terminal runs are touched, and a
/// run's children go only when the run itself goes. Outbox rows are left for
/// the delivery worker (they are archived, not owned, by the GC). Returns the
/// number of `workflow_runs` removed.
#[cfg(feature = "ops")]
pub async fn purge_runs_by_id(pool: &Pool, run_ids: &[String]) -> Result<u64> {
    if run_ids.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    let mut purged = 0u64;
    for id in run_ids {
        let terminal: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM workflow_runs
             WHERE id = ? AND status IN ('succeeded','failed','cancelled')",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if terminal.is_none() {
            continue;
        }
        sqlx::query(
            "DELETE FROM task_dependencies
             WHERE dependent_id IN (SELECT id FROM task_runs WHERE run_id = ?)",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM task_runs WHERE run_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        purged += sqlx::query("DELETE FROM workflow_runs WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    }
    sqlx::query(
        "DELETE FROM workflow_definitions
         WHERE id NOT IN (SELECT DISTINCT definition_id FROM workflow_runs)",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(purged)
}

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

    /// Heartbeat lease renewal (long tasks): only the live claim triple
    /// (worker + fence + running) can extend the lease; a stale fence or a
    /// finished task cannot — so a reclaimed worker can never resurrect its
    /// old lease out from under the new attempt.
    #[tokio::test]
    async fn renew_task_lease_extends_only_live_claims() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: hb\ntasks:\n  - name: long\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let task = &claim_ready(&pool, "worker-A", 10).await.unwrap()[0];
        let fence = task.version + 1;

        let before: Option<String> =
            sqlx::query_scalar("SELECT lease_expires_at FROM task_runs WHERE id = ?")
                .bind(&task.id)
                .fetch_one(&pool)
                .await
                .unwrap();

        // Live claim renews (and the lease moves past the claim-time horizon).
        assert!(renew_task_lease(&pool, &task.id, "worker-A", fence, 300).await.unwrap());
        let after: Option<String> =
            sqlx::query_scalar("SELECT lease_expires_at FROM task_runs WHERE id = ?")
                .bind(&task.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(after > before, "renewal must push the lease forward");

        // Wrong fence / wrong worker: fenced off.
        assert!(!renew_task_lease(&pool, &task.id, "worker-A", fence + 1, 300).await.unwrap());
        assert!(!renew_task_lease(&pool, &task.id, "worker-B", fence, 300).await.unwrap());

        // A finished task has no lease to renew.
        mark_task_succeeded(&pool, &task.id, "worker-A", fence, None).await.unwrap();
        assert!(!renew_task_lease(&pool, &task.id, "worker-A", fence, 300).await.unwrap());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Checkpoint-aware resume: a running task records a pointer, the pointer
    /// survives the retry path (that is the whole feature — the next attempt
    /// resumes instead of restarting), and only a running task in the right
    /// run can write it.
    #[tokio::test]
    async fn checkpoint_pointer_survives_retry() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: ck\ntasks:\n  - name: train\n    command: [\"true\"]\n    max_attempts: 3\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let task = &claim_ready(&pool, "worker-A", 10).await.unwrap()[0];
        let fence = task.version + 1;

        // No pointer until one is reported.
        assert!(task_checkpoint(&pool, &task.id).await.unwrap().is_none());

        // Wrong run scope refuses the write; the right one records it.
        assert!(!record_task_checkpoint(&pool, "other-run", &task.id, "x", None).await.unwrap());
        assert!(record_task_checkpoint(
            &pool,
            &run_id,
            &task.id,
            "artifacts/ck/epoch-7.pt",
            Some("epoch=7"),
        )
        .await
        .unwrap());
        assert_eq!(
            task_checkpoint(&pool, &task.id).await.unwrap(),
            Some(("artifacts/ck/epoch-7.pt".into(), Some("epoch=7".into())))
        );

        // The attempt dies (preemption); the retry keeps the pointer so the
        // next claim can be dispatched with DAGRON_RESUME_FROM.
        retry_task(&pool, &task.id, "worker-A", fence, Some("preempted".into()), chrono::Utc::now().to_rfc3339())
            .await
            .unwrap();
        assert_eq!(
            task_checkpoint(&pool, &task.id).await.unwrap(),
            Some(("artifacts/ck/epoch-7.pt".into(), Some("epoch=7".into()))),
            "checkpoint pointer must survive the retry path"
        );

        // Once parked (not running), further writes are refused.
        assert!(!record_task_checkpoint(&pool, &run_id, &task.id, "y", None).await.unwrap());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Gang co-scheduling: a `gang:` task expands into member rows sharing a
    /// gang_id, dependents wait for every member, the gang claims
    /// all-or-nothing (or not at all when capacity/readiness falls short),
    /// ordinary claims skip members, and one member's failure cancels the rest.
    #[tokio::test]
    async fn gang_expands_claims_whole_and_dies_together() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: g\ntasks:\n  - name: prep\n    command: [\"true\"]\n  - name: train\n    command: [\"true\"]\n    gang: { size: 3 }\n    depends_on: [prep]\n  - name: publish\n    command: [\"true\"]\n    depends_on: [train]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();

        // Expansion: 1 prep + 3 members + 1 publish; publish waits for all 3.
        let rows: Vec<(String, i64, Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT name, remaining_deps, gang_id, gang_rank FROM task_runs ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(
            rows.iter().filter(|r| r.2.is_some()).count(),
            3,
            "three gang member rows"
        );
        let publish = rows.iter().find(|r| r.0 == "publish").unwrap();
        assert_eq!(publish.1, 3, "dependent waits for every member");

        // prep runs; members become ready only after it succeeds.
        advance_ready_tasks(&pool).await.unwrap();
        let prep = &claim_ready(&pool, "w", 10).await.unwrap()[0];
        assert_eq!(prep.name, "prep");

        // While members are pending, a gang claim finds nothing.
        assert!(claim_ready_gang(&pool, "w", 10, &[], &Default::default()).await.unwrap().is_empty());

        mark_task_succeeded(&pool, &prep.id, "w", prep.version + 1, None).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Capacity below gang size → nothing; ordinary claim skips members.
        assert!(claim_ready_gang(&pool, "w", 2, &[], &Default::default()).await.unwrap().is_empty());
        assert!(claim_ready_classes_nongang(&pool, "w", 10, &[], &Default::default()).await.unwrap().is_empty());

        // Whole-gang claim at fitting capacity.
        let gang = claim_ready_gang(&pool, "w", 3, &[], &Default::default()).await.unwrap();
        assert_eq!(gang.len(), 3, "all members claimed together");

        // One member fails → siblings cancelled with a fencing version bump.
        let victim = &gang[0];
        mark_task_failed(&pool, &victim.id, "w", victim.version + 1, Some("rank died".into()))
            .await
            .unwrap();
        assert_eq!(cancel_gang_siblings(&pool, &victim.id).await.unwrap(), 2);
        let sibling = &gang[1];
        assert!(
            !renew_task_lease(&pool, &sibling.id, "w", sibling.version + 1, 30).await.unwrap(),
            "cancelled sibling's heartbeat is fenced off (aborts its execution)"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Gang co-scheduling composed with named pools (#21) and priority (#25):
    /// a gang is all-or-nothing, so a pooled gang must not be seized unless its
    /// pool can seat every member at once — otherwise the whole point of the cap
    /// (never more than N running) is defeated. The non-gang claim on the
    /// gang-aware path must also keep honoring priority ordering.
    #[tokio::test]
    async fn gang_claim_respects_pool_budget_and_priority() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: g\ntasks:\n  - name: train\n    command: [\"true\"]\n    pool: gpu\n    gang: { size: 3 }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Every member inherited the authored task's pool.
        let pooled: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE pool = 'gpu'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(pooled, 3, "gang members inherit the task's pool");

        // Pool of 2 cannot seat a gang of 3 — claim nothing rather than
        // over-commit the cap or claim a partial gang.
        let caps: std::collections::BTreeMap<String, i64> = [("gpu".to_string(), 2)].into();
        assert!(
            claim_ready_gang(&pool, "w", 10, &[], &caps).await.unwrap().is_empty(),
            "gang larger than its pool budget is not claimed"
        );
        // ...and the non-gang path never picks members off individually.
        assert!(
            claim_ready_classes_nongang(&pool, "w", 10, &[], &caps).await.unwrap().is_empty(),
            "gang members are never claimed individually"
        );

        // A pool that fits the whole gang claims it whole.
        let caps: std::collections::BTreeMap<String, i64> = [("gpu".to_string(), 3)].into();
        let gang = claim_ready_gang(&pool, "w", 10, &[], &caps).await.unwrap();
        assert_eq!(gang.len(), 3, "gang claimed whole once the pool can seat it");
        assert!(gang.iter().all(|t| t.pool.as_deref() == Some("gpu")), "pool survives the claim");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The gang-aware scheduler's ordinary claim (`claim_ready_classes_nongang`)
    /// shares the priority ordering (#25) and pool budgets (#21) of the plain
    /// claim — enabling gangs must not silently disable either.
    #[tokio::test]
    async fn nongang_claim_keeps_priority_and_pools() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: p\ntasks:\n  - { name: low, command: [\"true\"], priority: 1 }\n  - { name: high, command: [\"true\"], priority: 9 }\n  - { name: mid, command: [\"true\"], priority: 5 }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Highest priority first, exactly as the non-gang-aware claim orders.
        let first = claim_ready_classes_nongang(&pool, "w", 1, &[], &Default::default())
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].name, "high", "priority DESC honored on the gang-aware path");

        let second = claim_ready_classes_nongang(&pool, "w", 1, &[], &Default::default())
            .await
            .unwrap();
        assert_eq!(second[0].name, "mid");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Per-partition range leases: disjoint claims across consumers, renewal,
    /// rebalance at lease expiry, and immediate re-claim after a clean release.
    #[tokio::test]
    async fn partition_leases_split_renew_and_rebalance() {
        let (pool, path) = temp_pool().await;
        let parts: Vec<String> = ["p0", "p1", "p2"].iter().map(|s| s.to_string()).collect();
        register_source_partitions(&pool, "stream", &parts).await.unwrap();
        register_source_partitions(&pool, "stream", &parts).await.unwrap(); // idempotent

        let a = claim_source_partitions(&pool, "stream", "worker-A", 2, 30).await.unwrap();
        assert_eq!(a.len(), 2, "capped claim");
        let b = claim_source_partitions(&pool, "stream", "worker-B", 10, 30).await.unwrap();
        assert_eq!(b.len(), 1, "only the remainder is claimable");
        assert!(a.iter().all(|p| !b.contains(p)), "claims are disjoint");

        assert_eq!(renew_source_partitions(&pool, "stream", "worker-A", 30).await.unwrap(), 2);

        // B dies (lease lapses) → A rebalances its partition over.
        sqlx::query("UPDATE source_partitions SET lease_expires_at = ? WHERE claimed_by = 'worker-B'")
            .bind((chrono::Utc::now() - chrono::TimeDelta::seconds(5)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();
        let more = claim_source_partitions(&pool, "stream", "worker-A", 10, 30).await.unwrap();
        assert_eq!(more, b, "the expired partition rebalances");
        assert_eq!(held_source_partitions(&pool, "stream", "worker-A").await.unwrap().len(), 3);

        // Clean shutdown: released partitions are claimable immediately.
        assert_eq!(release_source_partitions(&pool, "stream", "worker-A").await.unwrap(), 3);
        let c = claim_source_partitions(&pool, "stream", "worker-C", 10, 30).await.unwrap();
        assert_eq!(c.len(), 3);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Deadline urgency: ready tasks move only when their RUN's deadline
    /// (hard run_timeout_secs or soft `deadline:`) is inside the horizon.
    #[tokio::test]
    async fn reclass_near_deadline_moves_only_pressed_runs() {
        let (pool, path) = temp_pool().await;
        let urgent = "name: urgent\nrun_timeout_secs: 60\nrunner_class: spot-gpu\ntasks:\n  - name: t\n    command: [\"true\"]\n";
        let calm = "name: calm\nrunner_class: spot-gpu\ntasks:\n  - name: t\n    command: [\"true\"]\n";
        for y in [urgent, calm] {
            let dag = DagGraph::from_yaml(y).unwrap();
            create_run(&pool, &dag, y).await.unwrap();
        }
        advance_ready_tasks(&pool).await.unwrap();

        let moved =
            reclass_ready_tasks_near_deadline(&pool, "spot-gpu", "ondemand-gpu", 120).await.unwrap();
        assert_eq!(moved, 1, "only the deadline-pressed run's task moves");
        let classes: Vec<(String,)> = sqlx::query_as(
            "SELECT runner_class FROM task_runs t JOIN workflow_runs r ON r.id = t.run_id
             JOIN workflow_definitions d ON d.id = r.definition_id WHERE d.name = 'urgent'",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(classes[0].0, "ondemand-gpu");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Placement fallback: only ready tasks of the source class that aged past
    /// the bar (or burned enough attempts) move; fresh tasks and other classes
    /// stay put.
    #[tokio::test]
    async fn reclass_ready_tasks_moves_only_aged_or_preempted() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: pl\nrunner_class: spot-gpu\ntasks:\n  - name: aged\n    command: [\"true\"]\n  - name: fresh\n    command: [\"true\"]\n  - name: other\n    runner_class: cpu\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Backdate one spot task's ready stamp past the age bar.
        let old = (chrono::Utc::now() - chrono::TimeDelta::seconds(600)).to_rfc3339();
        sqlx::query("UPDATE task_runs SET scheduled_at = ? WHERE name = 'aged'")
            .bind(&old)
            .execute(&pool)
            .await
            .unwrap();

        let moved =
            reclass_ready_tasks(&pool, "spot-gpu", "ondemand-gpu", 120, i64::MAX).await.unwrap();
        assert_eq!(moved, 1, "only the aged spot task moves");
        let classes: Vec<(String, String)> =
            sqlx::query_as("SELECT name, runner_class FROM task_runs ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            classes,
            vec![
                ("aged".into(), "ondemand-gpu".into()),
                ("fresh".into(), "spot-gpu".into()),
                ("other".into(), "cpu".into()),
            ]
        );

        // Attempt-based path: a much-retried fresh task moves regardless of age.
        sqlx::query("UPDATE task_runs SET attempt = 5 WHERE name = 'fresh'")
            .execute(&pool)
            .await
            .unwrap();
        let moved =
            reclass_ready_tasks(&pool, "spot-gpu", "ondemand-gpu", 999_999, 3).await.unwrap();
        assert_eq!(moved, 1, "the preemption-burned task moves on attempts");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Runner segmentation: tasks persist their resolved class (task override →
    /// DAG default), a class-restricted claim only sees its classes, and the
    /// unrestricted claim (empty set) still takes everything — the
    /// backward-compatible default.
    #[tokio::test]
    async fn runner_class_filter_routes_claims() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: seg\nrunner_class: etl\ntasks:\n  - name: extract\n    command: [\"true\"]\n  - name: check\n    runner_class: pulse\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Persisted resolution: task override wins, others take the DAG default.
        let stored: Vec<(String, String)> =
            sqlx::query_as("SELECT name, runner_class FROM task_runs ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            stored,
            vec![("check".into(), "pulse".into()), ("extract".into(), "etl".into())]
        );

        // A pool serving an unrelated class claims nothing.
        let none = claim_ready_classes(&pool, "ml-runner", 10, &["ml_training".into()], &Default::default())
            .await
            .unwrap();
        assert!(none.is_empty(), "class-restricted claim must not take other classes");

        // The pulse pool claims exactly its task.
        let pulse = claim_ready_classes(&pool, "pulse-runner", 10, &["pulse".into()], &Default::default())
            .await
            .unwrap();
        assert_eq!(pulse.len(), 1);
        assert_eq!(pulse[0].name, "check");

        // An unsegmented scheduler (empty class set) claims whatever remains.
        let rest = claim_ready(&pool, "any-runner", 10).await.unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].name, "extract");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Dispatch priority (fast-win #25): tasks persist their `priority`, and the
    /// ready-claim hands out higher-priority tasks first, breaking ties within
    /// the same `scheduled_at`. A task that names no priority stays at 0.
    #[tokio::test]
    async fn priority_orders_the_ready_claim() {
        let (pool, path) = temp_pool().await;

        // Three independent root tasks that all become ready at the same instant,
        // so `priority` alone decides claim order. `low` takes the default 0.
        let yaml = "name: prio\ntasks:\n  - name: low\n    command: [\"true\"]\n  - name: mid\n    priority: 5\n    command: [\"true\"]\n  - name: high\n    priority: 10\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Persisted resolution: task value wins; an unset task stays 0.
        let stored: Vec<(String, i64)> =
            sqlx::query_as("SELECT name, priority FROM task_runs ORDER BY name")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            stored,
            vec![("high".into(), 10), ("low".into(), 0), ("mid".into(), 5)]
        );

        // The claim scan returns rows in priority-desc order (then scheduled_at).
        let claimed = claim_ready(&pool, "worker-A", 10).await.unwrap();
        let order: Vec<&str> = claimed.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(order, vec!["high", "mid", "low"], "highest priority claimed first");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Sub-workflow trigger (#23): a `type: workflow` task resolves + parks on a
    /// child run (running, no lease, `sub_run_id` set — not claimable, not
    /// reclaimable); the reconcile sweep resolves it once the child is terminal,
    /// advancing the parent's dependents.
    #[tokio::test]
    async fn subworkflow_park_and_reconcile() {
        let (pool, path) = temp_pool().await;

        // Register a child workflow and resolve it by name.
        let child_yaml = "name: child\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ('wf-child','child',?,?,?)")
            .bind(child_yaml).bind("t").bind("t").execute(&pool).await.unwrap();
        assert_eq!(workflow_spec_by_name(&pool, "child").await.unwrap().as_deref(), Some(child_yaml));
        assert!(workflow_spec_by_name(&pool, "nope").await.unwrap().is_none());

        // Parent: a trigger task → a downstream task.
        let parent_yaml = "name: parent\ntasks:\n  - { name: trigger, type: workflow, workflow: child }\n  - name: down\n    command: [\"true\"]\n    depends_on: [trigger]\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        let parent_run = create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Simulate the engine: claim the trigger, create the child run, park.
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "trigger").expect("trigger claimed");
        let fence = trigger.version + 1;
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child_run = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger.id, fence, &child_run).await.unwrap());

        // Parked shape: running, NULL lease, sub_run_id set.
        let (status, lease, sub): (String, Option<String>, Option<String>) =
            sqlx::query_as("SELECT status, lease_expires_at, sub_run_id FROM task_runs WHERE id = ?")
                .bind(&trigger.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "running");
        assert!(lease.is_none(), "lease cleared so recovery won't reclaim it");
        assert_eq!(sub.as_deref(), Some(child_run.as_str()));

        // Lease recovery leaves it alone; a still-running child resolves nothing.
        recover_expired_leases(&pool).await.unwrap();
        assert!(reconcile_subworkflows(&pool).await.unwrap().is_empty(), "child still running → no resolution");

        // Child succeeds → the trigger resolves succeeded and `down` advances.
        sqlx::query("UPDATE workflow_runs SET status='succeeded' WHERE id = ?").bind(&child_run).execute(&pool).await.unwrap();
        let resolved = reconcile_subworkflows(&pool).await.unwrap();
        assert_eq!(resolved, vec![(trigger.id.clone(), true)]);
        advance_ready_tasks(&pool).await.unwrap();
        let down: (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
            .bind(&parent_run).fetch_one(&pool).await.unwrap();
        assert_eq!(down.0, "ready", "downstream advanced once the sub-workflow succeeded");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Sub-workflow recursion guard (#23): `sub_workflow_depth` reads the chain
    /// depth by walking `sub_run_id` backwards, so a workflow that triggers
    /// itself can be refused before it becomes an unbounded run factory. The
    /// `limit` both bounds the cost and guarantees termination — the second
    /// half of this test writes a *cycle* straight into the column, which the
    /// normal code paths cannot produce but which must not hang the guard.
    #[tokio::test]
    async fn subworkflow_depth_walks_the_chain_and_terminates() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: rec\ntasks:\n  - { name: t, type: workflow, workflow: rec }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();

        // A chain of three runs, each parked on the next: root → r1 → r2.
        let root = create_run(&pool, &dag, yaml).await.unwrap();
        let r1 = create_run(&pool, &dag, yaml).await.unwrap();
        let r2 = create_run(&pool, &dag, yaml).await.unwrap();
        for (parent, child) in [(&root, &r1), (&r1, &r2)] {
            sqlx::query("UPDATE task_runs SET status='running', sub_run_id = ? WHERE run_id = ?")
                .bind(child)
                .bind(parent)
                .execute(&pool)
                .await
                .unwrap();
        }

        // Depth counts hops up to the root, not runs in the table.
        assert_eq!(sub_workflow_depth(&pool, &root, 8).await.unwrap(), 0, "root has no parent");
        assert_eq!(sub_workflow_depth(&pool, &r1, 8).await.unwrap(), 1);
        assert_eq!(sub_workflow_depth(&pool, &r2, 8).await.unwrap(), 2);

        // The limit is a hard stop: the caller only needs "at least this deep".
        assert_eq!(sub_workflow_depth(&pool, &r2, 1).await.unwrap(), 1, "walk stops at the cap");

        // Close the chain into a cycle (root now parks on r2). Nothing in the
        // schema forbids this, and the guard must still return rather than spin.
        sqlx::query("UPDATE task_runs SET status='running', sub_run_id = ? WHERE run_id = ?")
            .bind(&root)
            .bind(&r2)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            sub_workflow_depth(&pool, &root, 4).await.unwrap(),
            4,
            "a cycle saturates at the limit instead of looping forever"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Deferrable wait sensor (#27): a `type: wait` task parks (running, no lease,
    /// `wake_at` set); the reconcile sweep leaves it alone until the deadline
    /// passes, then resolves it (succeeded) and advances its dependents.
    #[tokio::test]
    async fn wait_sensor_park_and_reconcile() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: pause, type: wait, wait: { for: 1h } }\n  - name: down\n    command: [\"true\"]\n    depends_on: [pause]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Claim the sensor and park it on a FUTURE deadline.
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let pause = claimed.iter().find(|t| t.name == "pause").expect("pause claimed");
        let fence = pause.version + 1;
        let future = (chrono::Utc::now() + chrono::TimeDelta::hours(1)).to_rfc3339();
        assert!(park_wait(&pool, &pause.id, fence, &future).await.unwrap());

        // Parked shape: running, NULL lease. Not reclaimed; not yet resolved.
        let (status, lease): (String, Option<String>) =
            sqlx::query_as("SELECT status, lease_expires_at FROM task_runs WHERE id = ?")
                .bind(&pause.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "running");
        assert!(lease.is_none());
        recover_expired_leases(&pool).await.unwrap();
        assert!(reconcile_waits(&pool).await.unwrap().is_empty(), "future deadline → not resolved");

        // Deadline in the past → the sweep resolves it and `down` advances.
        sqlx::query("UPDATE task_runs SET wake_at = '2000-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&pause.id).execute(&pool).await.unwrap();
        assert_eq!(reconcile_waits(&pool).await.unwrap(), vec![pause.id.clone()]);
        advance_ready_tasks(&pool).await.unwrap();
        let down: (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
            .bind(&run).fetch_one(&pool).await.unwrap();
        assert_eq!(down.0, "ready", "downstream advanced once the wait elapsed");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// HTTP wait sensor (#27 follow-on): a `wait.url` task parks (running, no
    /// lease, `wait_url` set, no `wake_at`); the time-sensor sweep ignores it; the
    /// poll sweep lists it while due, re-parks it (pushing `next_poll_at` out) on a
    /// miss, and resolves it (succeeded, dependents advance) on a hit.
    #[tokio::test]
    async fn url_wait_park_poll_and_resolve() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: probe, type: wait, wait: { url: \"https://example.com/ready\" } }\n  - name: down\n    command: [\"true\"]\n    depends_on: [probe]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Claim the sensor and park it on its endpoint, due for an immediate poll.
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let probe = claimed.iter().find(|t| t.name == "probe").expect("probe claimed");
        let fence = probe.version + 1;
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_wait_url(&pool, &probe.id, fence, "https://example.com/ready", &now)
            .await
            .unwrap());

        // Parked shape: running, NULL lease, no wake_at (so the time sweep skips it).
        let (status, lease, wake): (String, Option<String>, Option<String>) =
            sqlx::query_as("SELECT status, lease_expires_at, wake_at FROM task_runs WHERE id = ?")
                .bind(&probe.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "running");
        assert!(lease.is_none());
        assert!(wake.is_none());
        recover_expired_leases(&pool).await.unwrap();
        assert!(reconcile_waits(&pool).await.unwrap().is_empty(), "url sensor is not a time sensor");

        // It shows up as due for a poll.
        let due = due_url_waits(&pool, 32).await.unwrap();
        assert_eq!(due, vec![(probe.id.clone(), "https://example.com/ready".to_string())]);

        // A miss re-parks it far in the future → no longer due.
        let future = (chrono::Utc::now() + chrono::TimeDelta::hours(1)).to_rfc3339();
        assert!(repark_url_wait(&pool, &probe.id, &future).await.unwrap());
        assert!(due_url_waits(&pool, 32).await.unwrap().is_empty(), "re-parked → not due until next_poll_at");

        // A hit resolves it (succeeded) and `down` advances.
        assert!(resolve_url_wait(&pool, &probe.id).await.unwrap());
        assert!(!resolve_url_wait(&pool, &probe.id).await.unwrap(), "second resolve is a no-op");
        advance_ready_tasks(&pool).await.unwrap();
        let (dstatus,): (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
            .bind(&run).fetch_one(&pool).await.unwrap();
        assert_eq!(dstatus, "ready", "downstream advanced once the endpoint was ready");
        let (pstatus,): (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE id = ?")
            .bind(&probe.id).fetch_one(&pool).await.unwrap();
        assert_eq!(pstatus, "succeeded");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Datasets, part 1 — produce → track → sense: a `produces:` success upserts
    /// the registry and appends lineage; a `wait.dataset` sensor parks on the
    /// ledger's high-water mark (history never resolves it) and resolves only on
    /// an update recorded after the park, advancing its dependents.
    #[tokio::test]
    async fn dataset_produce_track_and_sensor() {
        let (pool, path) = temp_pool().await;
        let uri = "s3://lake/orders".to_string();

        // Producer run: claim `load`, succeed it, record its produces.
        let prod_yaml = "name: producer\ntasks:\n  - { name: load, command: [\"true\"], produces: [\"s3://lake/orders\"] }\n";
        let dag = DagGraph::from_yaml(prod_yaml).unwrap();
        let prod_run = create_run(&pool, &dag, prod_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let load = &claim_ready(&pool, "w", 10).await.unwrap()[0];
        assert!(mark_task_succeeded(&pool, &load.id, "w", load.version + 1, None).await.unwrap());
        record_dataset_updates(&pool, "producer", &load.id, "load", &[uri.clone()])
            .await
            .unwrap();

        // Registry upsert + lineage row carry the producing run/task.
        let (updated_at, last_run, last_task, updates): (String, Option<String>, Option<String>, i64) =
            sqlx::query_as("SELECT updated_at, last_run_id, last_task, updates FROM datasets WHERE uri = ?")
                .bind(&uri).fetch_one(&pool).await.unwrap();
        assert!(!updated_at.is_empty());
        assert_eq!(last_run.as_deref(), Some(prod_run.as_str()));
        assert_eq!(last_task.as_deref(), Some("load"));
        assert_eq!(updates, 1);
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dataset_events WHERE uri = ?")
            .bind(&uri).fetch_one(&pool).await.unwrap();
        assert_eq!(events, 1);

        // A second update increments, never duplicates, the registry row.
        record_dataset_updates(&pool, "producer", &load.id, "load", &[uri.clone()])
            .await
            .unwrap();
        let updates: i64 = sqlx::query_scalar("SELECT updates FROM datasets WHERE uri = ?")
            .bind(&uri).fetch_one(&pool).await.unwrap();
        assert_eq!(updates, 2);

        // Sensor run parks AFTER two events exist — history must not resolve it.
        let cons_yaml = "name: consumer\ntasks:\n  - { name: sense, type: wait, wait: { dataset: \"s3://lake/orders\" } }\n  - name: down\n    command: [\"true\"]\n    depends_on: [sense]\n";
        let dag = DagGraph::from_yaml(cons_yaml).unwrap();
        let cons_run = create_run(&pool, &dag, cons_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let sense = claimed.iter().find(|t| t.name == "sense").expect("sensor claimed");
        assert!(park_wait_dataset(&pool, &sense.id, sense.version + 1, &uri).await.unwrap());

        // Parked shape: running, NULL lease, cursor at the current high-water mark.
        let (status, lease, cursor): (String, Option<String>, Option<i64>) =
            sqlx::query_as("SELECT status, lease_expires_at, wait_dataset_cursor FROM task_runs WHERE id = ?")
                .bind(&sense.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "running");
        assert!(lease.is_none());
        assert_eq!(cursor, Some(2), "cursor = max event id at park time");
        recover_expired_leases(&pool).await.unwrap();
        assert!(reconcile_dataset_waits(&pool).await.unwrap().is_empty(), "park-time history never resolves the sensor");

        // A fresh update (external-event flavor) resolves it; `down` advances.
        record_external_dataset_event(&pool, &uri).await.unwrap();
        assert_eq!(
            reconcile_dataset_waits(&pool).await.unwrap(),
            vec![(sense.id.clone(), uri.clone())]
        );
        assert!(reconcile_dataset_waits(&pool).await.unwrap().is_empty(), "second sweep is a no-op");
        advance_ready_tasks(&pool).await.unwrap();
        let (dstatus,): (String,) =
            sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
                .bind(&cons_run).fetch_one(&pool).await.unwrap();
        assert_eq!(dstatus, "ready", "downstream advanced once the dataset updated");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Datasets, part 2 — trigger subscriptions and firing: sync initializes
    /// cursors at the high-water mark (registering never fires on history), a new
    /// event claims exactly one fire (CAS coalesces racers), rollback re-arms a
    /// refused fire, `all` composition waits for every subscribed dataset, and
    /// orphaned subscriptions prune.
    #[tokio::test]
    async fn dataset_triggers_sync_claim_and_compose() {
        let (pool, path) = temp_pool().await;
        let (a, b) = ("lake://a".to_string(), "lake://b".to_string());
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ('wf-c','consumer','name: consumer\ntasks: []',?,?)")
            .bind(&now).bind(&now).execute(&pool).await.unwrap();

        // History exists BEFORE the subscription: registering must not fire.
        record_external_dataset_event(&pool, &a).await.unwrap();
        sync_dataset_triggers(&pool, "consumer", &[a.clone()], "any").await.unwrap();
        assert!(claim_due_dataset_triggers(&pool).await.unwrap().is_empty(), "no fire on pre-subscription history");

        // A fresh event fires exactly once; the claim coalesces (second sweep empty).
        record_external_dataset_event(&pool, &a).await.unwrap();
        record_external_dataset_event(&pool, &a).await.unwrap(); // two updates → one fire
        let fires = claim_due_dataset_triggers(&pool).await.unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].workflow_name, "consumer");
        assert_eq!(fires[0].trigger_uri, a);
        assert!(claim_due_dataset_triggers(&pool).await.unwrap().is_empty(), "claim advanced the cursor — no double fire");

        // A refused fire rolls back and re-arms.
        unclaim_dataset_trigger(&pool, "consumer", &fires[0].advanced).await.unwrap();
        let refires = claim_due_dataset_triggers(&pool).await.unwrap();
        assert_eq!(refires.len(), 1, "rollback re-armed the fire");

        // `all` composition: one fresh dataset of two is not enough.
        sync_dataset_triggers(&pool, "consumer", &[a.clone(), b.clone()], "all").await.unwrap();
        record_external_dataset_event(&pool, &a).await.unwrap();
        assert!(claim_due_dataset_triggers(&pool).await.unwrap().is_empty(), "all-of waits for every dataset");
        record_external_dataset_event(&pool, &b).await.unwrap();
        let fires = claim_due_dataset_triggers(&pool).await.unwrap();
        assert_eq!(fires.len(), 1, "all fresh → fire");
        assert_eq!(fires[0].advanced.len(), 2, "both cursors advanced atomically");

        // Sync to a narrower set deletes the dropped edge; keeping cursors.
        sync_dataset_triggers(&pool, "consumer", &[a.clone()], "any").await.unwrap();
        let subs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dataset_triggers WHERE workflow_name='consumer'")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(subs, 1);

        // Deleting the workflow orphans the subscription; prune removes it.
        sqlx::query("DELETE FROM workflows WHERE name='consumer'").execute(&pool).await.unwrap();
        assert_eq!(prune_dataset_triggers(&pool).await.unwrap(), 1);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Memoization (#22): store→lookup roundtrip, key isolation, upsert refresh,
    /// and `max_age_secs` staleness (an aged entry misses so the task re-runs).
    #[tokio::test]
    async fn memo_store_and_lookup_roundtrip() {
        let (pool, path) = temp_pool().await;

        // Miss on an empty store.
        assert!(memo_lookup(&pool, "wf", "t", "k1", None).await.unwrap().is_none());

        // Store, then hit; a different key still misses.
        memo_store(&pool, "wf", "t", "k1", "cached-output").await.unwrap();
        assert_eq!(
            memo_lookup(&pool, "wf", "t", "k1", None).await.unwrap().as_deref(),
            Some("cached-output")
        );
        assert!(memo_lookup(&pool, "wf", "t", "k2", None).await.unwrap().is_none());

        // Upsert refreshes the output in place.
        memo_store(&pool, "wf", "t", "k1", "v2").await.unwrap();
        assert_eq!(
            memo_lookup(&pool, "wf", "t", "k1", None).await.unwrap().as_deref(),
            Some("v2")
        );

        // An aged entry misses when `max_age_secs` is small, but never with no cap.
        sqlx::query("UPDATE task_memo SET created_at = '2000-01-01T00:00:00+00:00' WHERE cache_key = 'k1'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            memo_lookup(&pool, "wf", "t", "k1", Some(60)).await.unwrap().is_none(),
            "stale entry misses under max_age_secs"
        );
        assert!(
            memo_lookup(&pool, "wf", "t", "k1", None).await.unwrap().is_some(),
            "no max_age → never stale"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A pooled gang must not be admitted past its pool's budget even when the
    /// pool has *some* free slots: the claim is all-or-nothing, so a gang of 3
    /// into a pool with 2 free slots would over-commit the cap. Guards the
    /// gang × pools composition against partial admission.
    #[tokio::test]
    async fn pooled_gang_never_overcommits_its_pool() {
        let (pool, path) = temp_pool().await;
        // One task already occupying a `gpu` slot, plus a gang of 3 in `gpu`.
        let yaml = "name: g\ntasks:\n  - { name: solo, command: [\"true\"], pool: gpu }\n  - { name: train, command: [\"true\"], pool: gpu, gang: { size: 3 } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Cap 4: `solo` takes one slot, leaving 3 — exactly enough for the gang.
        let caps: std::collections::BTreeMap<String, i64> =
            std::iter::once(("gpu".to_string(), 4)).collect();
        let solo = claim_ready_classes_nongang(&pool, "w", 1, &[], &caps).await.unwrap();
        assert_eq!(solo.len(), 1);
        assert_eq!(solo[0].name, "solo");

        // Now only 3 free — the gang fits exactly.
        let gang = claim_ready_gang(&pool, "w", 10, &[], &caps).await.unwrap();
        assert_eq!(gang.len(), 3, "gang admitted when the pool has exactly enough");

        // Same shape, but a tighter cap leaves too few slots for the whole gang.
        let (pool2, path2) = temp_pool().await;
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool2, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool2).await.unwrap();
        let caps: std::collections::BTreeMap<String, i64> =
            std::iter::once(("gpu".to_string(), 3)).collect();
        let solo = claim_ready_classes_nongang(&pool2, "w", 1, &[], &caps).await.unwrap();
        assert_eq!(solo.len(), 1);
        // 2 free slots < gang size 3 → claim nothing rather than partially admit.
        assert!(
            claim_ready_gang(&pool2, "w", 10, &[], &caps).await.unwrap().is_empty(),
            "gang must not be admitted into a pool that cannot seat all members"
        );
        let running: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE status='running' AND pool='gpu'")
                .fetch_one(&pool2).await.unwrap();
        assert_eq!(running, 1, "pool cap never exceeded");

        pool.close().await;
        pool2.close().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&path2);
    }

    /// A **parked** task must not consume its pool's budget. Parking keeps
    /// `status = 'running'` while holding no worker, so counting parked rows
    /// would let an hour-long sensor deadlock a `POOLS=db:1` pool for the whole
    /// hour — the exact opposite of what parking is for.
    #[tokio::test]
    async fn parked_tasks_do_not_consume_pool_slots() {
        let (pool, path) = temp_pool().await;
        let caps: std::collections::BTreeMap<String, i64> =
            std::iter::once(("db".to_string(), 1)).collect();

        // A pooled wait sensor and a pooled command task, both in a cap-1 pool.
        let yaml = "name: p\ntasks:\n  - { name: sensor, type: wait, wait: { for: 1h }, pool: db }\n  - { name: work, command: [\"true\"], pool: db }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Claim + park the sensor: it now sits `running` holding no worker.
        let claimed = claim_ready_classes(&pool, "w", 1, &[], &caps).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let sensor = claimed.iter().find(|t| t.name == "sensor").expect("sensor first (both due)");
        let future = (chrono::Utc::now() + chrono::TimeDelta::hours(1)).to_rfc3339();
        assert!(park_wait(&pool, &sensor.id, sensor.version + 1, &future).await.unwrap());

        // The parked sensor must NOT hold the pool's only slot.
        let next = claim_ready_classes(&pool, "w", 1, &[], &caps).await.unwrap();
        assert_eq!(next.len(), 1, "parked sensor must not squat the pool budget");
        assert_eq!(next[0].name, "work");

        // …and a genuinely running task does hold it: nothing else gets in.
        let none = claim_ready_classes(&pool, "w", 1, &[], &caps).await.unwrap();
        assert!(none.is_empty(), "the running task still consumes the cap-1 pool");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Named concurrency pool (#21): a pool with capacity 1 lets only one of its
    /// tasks be claimed at a time; an unpooled task is never gated; freeing the
    /// running slot lets the next pooled task claim.
    #[tokio::test]
    async fn pool_caps_limit_concurrent_claims() {
        let (pool, path) = temp_pool().await;
        let caps: std::collections::BTreeMap<String, i64> =
            std::iter::once(("db".to_string(), 1)).collect();

        // Two root tasks share pool `db` (cap 1); `c` is unpooled.
        let yaml = "name: pooled\ntasks:\n  - name: a\n    pool: db\n    command: [\"true\"]\n  - name: b\n    pool: db\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // First claim: pool `db` yields exactly one task; unpooled `c` is claimed too.
        let first = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        let db_claimed = first.iter().filter(|t| t.pool.as_deref() == Some("db")).count();
        assert_eq!(db_claimed, 1, "pool db cap 1 → exactly one db task claimed");
        assert!(first.iter().any(|t| t.name == "c"), "unpooled task is never gated");
        assert_eq!(first.len(), 2);

        let running: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE status='running' AND pool='db'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(running, 1, "pool db is at its cap");

        // While db is full, a further claim takes no new db task.
        let mid = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        assert!(
            mid.iter().all(|t| t.pool.as_deref() != Some("db")),
            "pool db full → no new db task claimed"
        );

        // Free the running db slot; the remaining db task now claims.
        sqlx::query("UPDATE task_runs SET status='succeeded' WHERE status='running' AND pool='db'")
            .execute(&pool)
            .await
            .unwrap();
        let after = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        assert_eq!(
            after.iter().filter(|t| t.pool.as_deref() == Some("db")).count(),
            1,
            "slot freed → the second db task claims"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Per-workflow concurrency cap (#21): with `max_active_runs: 1`, a second
    /// run is refused while the first is still running, and admitted again once
    /// the first reaches a terminal state (a slot frees).
    #[tokio::test]
    async fn max_active_runs_caps_concurrent_runs() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: capped\nmax_active_runs: 1\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();

        // First run admits.
        let run1 = create_run(&pool, &dag, yaml).await.unwrap();

        // Second run is refused while the first is still running — a typed error.
        let err = create_run(&pool, &dag, yaml).await.expect_err("cap reached");
        assert!(
            err.is::<crate::models::MaxActiveRunsReached>(),
            "expected MaxActiveRunsReached, got: {err}"
        );

        // Finish the first run; a slot frees and the next run admits.
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
            .bind(&run1)
            .execute(&pool)
            .await
            .unwrap();
        create_run(&pool, &dag, yaml)
            .await
            .expect("a slot freed once the first run finished");

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
    // Uses the ops-gated `list_tasks` read API, so it only compiles with it.
    #[cfg(feature = "ops")]
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
    // Uses the ops-gated `get_run` read API, so it only compiles with it.
    #[cfg(feature = "ops")]
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
