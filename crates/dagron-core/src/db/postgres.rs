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

/// Opens the Postgres pool and runs migrations: the base set first, then — with
/// the `enterprise` feature — the enterprise set. Both migrators run
/// ignore-missing so the two migration dirs can share sqlx's single
/// `_sqlx_migrations` table without colliding (see the inline comment below).
pub async fn init_pool(conn: &str) -> Result<Pool> {
    // DB_MAX_CONNECTIONS: per-process pool cap. On a shared state-store cell
    // small-class engines run at 2-3 so hundreds of tenant
    // engines fit one PgBouncer/cell budget; 8 stays the standalone default.
    let max_connections: u32 = std::env::var("DB_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
        .max(2); // claim tx + listener headroom; 1 would deadlock the loop
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(conn)
        .await?;

    // The base and enterprise (900+) sets share sqlx's single `_sqlx_migrations`
    // table. ignore_missing lets the base migrator tolerate the enterprise rows it
    // does not own; without it an enterprise DB (900 already applied) fails the base
    // migrator on every reboot with "migration 900 was previously applied but is
    // missing in the resolved migrations" — before the enterprise migrator runs.
    let mut base = sqlx::migrate!("./migrations_pg");
    base.set_ignore_missing(true);
    base.run(&pool).await?;
    #[cfg(feature = "enterprise")]
    {
        // enterprise migrations live in a separate dir but share sqlx's single
        // `_sqlx_migrations` table with the base set. Their versions are offset
        // above the base range (900+) so they never collide, and ignore_missing
        // lets this migrator tolerate the base migrations it doesn't own (else
        // sqlx errors "previously applied but missing in resolved migrations").
        let mut ee = sqlx::migrate!("./migrations_pg_ee");
        ee.set_ignore_missing(true);
        ee.run(&pool).await?;
    }
    Ok(pool)
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction, then NOTIFYs so any idle scheduler wakes to advance
/// the root tasks. Returns the new run_id.
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
    // (by name) already has `max_active_runs` runs in flight. The count filters
    // the small set of `running` runs and PK-joins their definitions.
    //
    // The cap is documented as a hard limit, so the check and the insert must be
    // atomic against other creators: a bare count-then-insert lets N concurrent
    // submitters all observe `active < max` and over-admit by up to (writers − 1).
    // A transaction-scoped advisory lock keyed on the workflow *name* serializes
    // admission for that workflow only — held to commit, so the row this
    // transaction inserts is visible to the next admitter. Workflows without a
    // cap never take the lock, so the uncapped path is unchanged. (SQLite needs
    // no equivalent: its single-writer transaction already serializes creators.)
    if let Some(max) = dag.spec.max_active_runs {
        if max > 0 {
            sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
                .bind(RUN_ADMISSION_LOCK)
                .bind(&dag.spec.name)
                .execute(&mut *tx)
                .await?;
            let active: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_runs wr
                 JOIN workflow_definitions d ON d.id = wr.definition_id
                 WHERE wr.status = 'running' AND d.name = $1",
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
           (id, definition_id, status, created_at, deadline_at, alert_deadline_at, result_from, environment)
         VALUES ($1, $2, 'running', $3, $4, $5, $6, $7)",
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
                 VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)",
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
                        "INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES ($1, $2)",
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

    notify(&mut *tx, &run_id).await?;
    tx.commit().await?;
    Ok(run_id)
}

/// Upsert one source's committed position (see `source_offsets`). Runs on any
/// executor so it can join a caller's transaction.
async fn upsert_source_offset<'e, E>(ex: E, source_name: &str, position: &str, now: &str) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO source_offsets (source_name, position, updated_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (source_name)
         DO UPDATE SET position = EXCLUDED.position, updated_at = EXCLUDED.updated_at",
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
    Ok(sqlx::query_scalar("SELECT position FROM source_offsets WHERE source_name = $1")
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
             VALUES ($1, $2, $3) ON CONFLICT (source_name, partition) DO NOTHING",
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
    let rows = sqlx::query(
        "UPDATE source_partitions
         SET claimed_by = $1, lease_expires_at = $2, updated_at = $3
         WHERE (source_name, partition) IN (
             SELECT source_name, partition FROM source_partitions
             WHERE source_name = $4
               AND (claimed_by IS NULL OR lease_expires_at IS NULL OR lease_expires_at < $3)
               AND (claimed_by IS DISTINCT FROM $1)
             LIMIT $5
             FOR UPDATE SKIP LOCKED
         )
         RETURNING partition",
    )
    .bind(worker_id)
    .bind(&lease)
    .bind(&now_s)
    .bind(source_name)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.iter().map(|r| Ok(r.try_get("partition")?)).collect()
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
         SET lease_expires_at = $1, updated_at = $2
         WHERE source_name = $3 AND claimed_by = $4",
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
         WHERE source_name = $1 AND claimed_by = $2 ORDER BY partition",
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
         SET claimed_by = NULL, lease_expires_at = NULL, updated_at = $1
         WHERE source_name = $2 AND claimed_by = $3",
    )
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(source_name)
    .bind(worker_id)
    .execute(pool)
    .await?
    .rows_affected())
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
             WHERE d.dependent_id = $1",
        )
        .bind(&task_id)
        .fetch_all(pool)
        .await?;

        // Runtime `when` gate: a leaf-surviving condition references upstream
        // outputs (`{{ tasks.<name>.output }}`), evaluable only now that the
        // deps are terminal. False ⇒ skipped, exactly like an unsatisfied
        // trigger rule; an unevaluable expression fails the task loudly.
        let mut when_pass = true;
        if let Some(cond) = input
            .as_deref()
            .and_then(|json| serde_json::from_str::<crate::dag::TaskSpec>(json).ok())
            .and_then(|spec| spec.when)
        {
            let mut ctx: std::collections::BTreeMap<String, String> = Default::default();
            for name in crate::expand::when_output_refs(&cond) {
                let output: Option<Option<String>> = sqlx::query_scalar(
                    "SELECT output FROM task_runs WHERE run_id = $1 AND name = $2",
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
                    // The gate itself is broken — fail the task so the author
                    // sees it, and let the failure path advance dependents.
                    let now = chrono::Utc::now().to_rfc3339();
                    let msg = format!("runtime when '{cond}' failed to evaluate: {e}");
                    let mut tx = pool.begin().await?;
                    let rows = sqlx::query(
                        "UPDATE task_runs SET status = 'failed', output = $1, finished_at = $2
                         WHERE id = $3 AND status = 'pending'",
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
                                 SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
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
/// Global advisory-lock key serializing pool-limited claims across schedulers
/// (#21) so concurrent controllers cannot over-commit a pool's slot budget. The
/// unpooled / uncapped-pool fast path never takes it — it keeps the lock-free
/// `FOR UPDATE SKIP LOCKED` claim. Pools exist to *limit* concurrency, so
/// serializing only their admission is an acceptable, well-scoped cost.
const POOL_CLAIM_LOCK: i64 = 0x6461_676f_6e70; // ascii "dagonp"

/// Advisory-lock *classid* for per-workflow run admission (`max_active_runs`,
/// #21). Paired with `hashtext(<workflow name>)` as the objid, so admission
/// serializes per workflow rather than globally — two different capped workflows
/// never block each other, and uncapped workflows never take the lock at all.
/// Distinct from [`POOL_CLAIM_LOCK`], which is a single-key (non-classid) lock.
const RUN_ADMISSION_LOCK: i32 = 0x6461_6772; // ascii "dagr"

/// Count of tasks genuinely occupying a worker slot in a pool — see the SQLite
/// twin. Parked tasks (`sub_run_id` / `wake_at` / `wait_url` / `wait_dataset`)
/// keep `status = 'running'` while holding no worker, so counting them would let
/// a sleeping sensor squat its pool's budget. Shared by both claim paths.
const POOL_RUNNING_COUNT: &str = "SELECT COUNT(*) FROM task_runs
     WHERE status = 'running' AND pool = $1
       AND wake_at IS NULL AND wait_url IS NULL
       AND wait_dataset IS NULL AND sub_run_id IS NULL";

pub async fn claim_ready(pool: &Pool, worker_id: &str, limit: i64) -> Result<Vec<TaskRun>> {
    claim_ready_classes(pool, worker_id, limit, &[], &std::collections::BTreeMap::new()).await
}

/// [`claim_ready`] restricted to a set of runner classes — the routing seam for
/// segmented runner pools (`RUNNER_CLASSES`). An empty slice claims every class
/// (the unsegmented scheduler), keeping the two call sites one query.
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
/// [`claim_ready_classes_nongang`]. `exclude_gangs` adds `gang_id IS NULL` to both
/// phases so the gang-aware scheduler never claims a gang member individually —
/// everything else (priority ordering #25, per-pool budgets #21, class routing,
/// the lock-free fast path) is identical, so both callers get the same semantics.
async fn claim_ready_filtered(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
    classes: &[String],
    pool_caps: &std::collections::BTreeMap<String, i64>,
    exclude_gangs: bool,
) -> Result<Vec<TaskRun>> {
    let now = chrono::Utc::now().to_rfc3339();
    let lease_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(30)).to_rfc3339();

    // ── No pools configured: the lock-free `FOR UPDATE SKIP LOCKED` claim ──────
    // N schedulers partition the ready set with zero coordination, globally
    // ordered by `priority DESC, scheduled_at`. This is the overwhelmingly common
    // deployment and is byte-for-byte the original claim.
    if pool_caps.is_empty() {
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
                   AND (cardinality($5::text[]) = 0 OR runner_class = ANY($5::text[]))
                   AND ($6 = false OR gang_id IS NULL)
                 ORDER BY priority DESC, scheduled_at
                 LIMIT $4
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, run_id, name, status,
                       attempt - 1 AS attempt, remaining_deps,
                       input, output, claimed_by, lease_expires_at,
                       version - 1 AS version, scheduled_at, finished_at, pool",
        )
        .bind(worker_id)
        .bind(&lease_exp)
        .bind(&now)
        .bind(limit)
        .bind(classes)
        .bind(exclude_gangs)
        .fetch_all(pool)
        .await?;
        // `row_to_task` maps the TEXT status without a native enum and reads `pool`.
        return rows.iter().map(row_to_task).collect::<Result<_>>();
    }

    // ── Pools configured: ONE globally priority-ordered admission pass ─────────
    // Pooled and unpooled candidates are ranked together by `priority DESC,
    // scheduled_at` and admitted in that order against their pool budgets.
    //
    // This deliberately does *not* claim unpooled tasks in a separate earlier
    // phase: doing so would let a low-priority unpooled task be claimed ahead of
    // a higher-priority pooled task that still had capacity, silently breaking
    // `priority` (and letting a sustained unpooled backlog starve pooled work).
    // Priority is a global ordering or it is nothing.
    //
    // The advisory lock makes the budget read and the claims atomic against
    // other schedulers (and against gang claims, which take the same lock), so
    // concurrent controllers cannot over-commit a pool. It is taken once per
    // batch claim — not per task — and only when pools are configured; pools
    // exist to *limit* concurrency, so serializing their admission is the
    // intended cost. The lock auto-releases at transaction end.
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(POOL_CLAIM_LOCK)
        .execute(&mut *tx)
        .await?;

    // Remaining slots per capped pool = capacity − currently running.
    let mut avail: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut exhausted: Vec<String> = Vec::new();
    for (name, cap) in pool_caps {
        let running: i64 = sqlx::query_scalar(
            POOL_RUNNING_COUNT,
        )
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        let r = (cap - running).max(0);
        if r == 0 {
            exhausted.push(name.clone());
        }
        avail.insert(name.clone(), r);
    }

    // Every claimable candidate — pooled or not — minus tasks in already-full
    // pools, so a full pool at the head of the queue can't starve claimable work
    // behind it. Rows carry their pre-claim attempt/version.
    let cand_rows = sqlx::query(
        "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at, pool
         FROM task_runs
         WHERE status = 'ready'
           AND (scheduled_at IS NULL OR scheduled_at <= $1)
           AND (cardinality($3::text[]) = 0 OR runner_class = ANY($3::text[]))
           AND (pool IS NULL OR NOT (pool = ANY($4::text[])))
           AND ($5 = false OR gang_id IS NULL)
         ORDER BY priority DESC, scheduled_at
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind(&now)
    .bind(limit)
    .bind(classes)
    .bind(&exhausted)
    .bind(exclude_gangs)
    .fetch_all(&mut *tx)
    .await?;

    let mut claimed: Vec<TaskRun> = Vec::with_capacity(cand_rows.len());
    for row in &cand_rows {
        let task = row_to_task(row)?;
        // A pool draining to zero mid-batch skips its remaining candidates;
        // unpooled and uncapped-pool tasks are never gated.
        if let Some(p) = &task.pool {
            if let Some(r) = avail.get(p) {
                if *r <= 0 {
                    continue;
                }
            }
        }
        let updated = sqlx::query(
            "UPDATE task_runs
             SET status = 'running', claimed_by = $1, lease_expires_at = $2,
                 attempt = attempt + 1, version = version + 1
             WHERE id = $3 AND status = 'ready' AND version = $4",
        )
        .bind(worker_id)
        .bind(&lease_exp)
        .bind(&task.id)
        .bind(task.version)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated > 0 {
            if let Some(p) = &task.pool {
                if let Some(r) = avail.get_mut(p) {
                    *r -= 1;
                }
            }
            claimed.push(task);
        }
    }
    tx.commit().await?;

    Ok(claimed)
}

/// All-or-nothing gang claim: seize every member of ONE gang whose members are
/// all `ready` and whose size fits `capacity` — or nothing. Two schedulers
/// racing the same gang serialize on the row locks; the loser's UPDATE matches
/// zero `ready` rows and returns empty (never a partial gang). Ordinary claims
/// must then use [`claim_ready_classes_nongang`] so members are never claimed
/// individually. The gang-aware scheduler (RUNNER_GANGS) drives this.
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
    let now = chrono::Utc::now().to_rfc3339();
    let lease_exp = (chrono::Utc::now() + chrono::TimeDelta::seconds(30)).to_rfc3339();

    // Budget resolution, gang selection, and the claiming UPDATE all run in ONE
    // transaction. When pools are configured it additionally holds
    // `POOL_CLAIM_LOCK` — the same lock `claim_ready_filtered`'s pool phase takes
    // — because a gang's budget check and its claim must be atomic *against
    // ordinary pooled claims too*: counting outside the lock lets a gang and a
    // concurrent pooled claim both observe the same free slots and jointly
    // exceed the pool's cap. Unpooled deployments skip the lock entirely and keep
    // the original lock-free behavior.
    let mut tx = pool.begin().await?;
    if !pool_caps.is_empty() {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(POOL_CLAIM_LOCK)
            .execute(&mut *tx)
            .await?;
    }

    // A gang is all-or-nothing, so a pooled gang needs `gang_size` free slots in
    // its pool at once (#21 × gang co-scheduling): seizing it while the pool is
    // short would over-commit the cap. Highest member priority first (#25).
    let candidates: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT gang_id, gang_size, pool FROM task_runs
         WHERE status = 'ready' AND gang_id IS NOT NULL
           AND (scheduled_at IS NULL OR scheduled_at <= $1)
           AND (cardinality($3::text[]) = 0 OR runner_class = ANY($3::text[]))
         GROUP BY gang_id, gang_size, pool
         HAVING COUNT(*) = gang_size AND gang_size <= $2
         ORDER BY MAX(priority) DESC, MIN(scheduled_at)",
    )
    .bind(&now)
    .bind(capacity)
    .bind(classes)
    .fetch_all(&mut *tx)
    .await?;

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

    // Two schedulers racing the same gang serialize on the row locks; the loser's
    // UPDATE matches zero `ready` rows and returns empty (never a partial gang).
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'running',
             claimed_by = $1,
             lease_expires_at = $2,
             attempt = attempt + 1,
             version = version + 1
         WHERE gang_id = $3 AND status = 'ready'
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
    // `row_to_task` maps the TEXT status without a native enum and reads `pool`.
    rows.iter().map(row_to_task).collect::<Result<_>>()
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
         WHERE status = 'ready' AND gang_id IS NOT NULL AND gang_size > $1
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
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', claimed_by = NULL, lease_expires_at = NULL,
             version = version + 1, finished_at = $1,
             output = COALESCE(output, 'cancelled: gang member failed')
         WHERE gang_id IS NOT NULL
           AND gang_id = (SELECT gang_id FROM task_runs WHERE id = $2)
           AND id != $2
           AND status IN ('pending', 'ready', 'running')
         RETURNING run_id",
    )
    .bind(&now)
    .bind(member_id)
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

    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'succeeded', finished_at = $1, output = $2, claimed_by = NULL,
             cache_hit = $6
         WHERE id = $3 AND claimed_by = $4 AND version = $5
         RETURNING run_id",
    )
    .bind(&now)
    .bind(&output)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .bind(cache_hit)
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
/// `resolve_approval`'s guard. Not `ops`-gated — the reconcile loop calls this
/// every tick in every build.
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

/// The registered workflow spec YAML for `name`, or `None` if no such workflow —
/// resolves a `type: workflow` task's target at trigger time (#23).
pub async fn workflow_spec_by_name(pool: &Pool, name: &str) -> Result<Option<String>> {
    let spec: Option<String> = sqlx::query_scalar("SELECT spec FROM workflows WHERE name = $1")
        .bind(name)
        .fetch_optional(pool)
        .await?;
    Ok(spec)
}

/// Park a claimed `type: workflow` task on its child run (#23): keep it `running`
/// but drop the worker claim/lease and record `sub_run_id`, so no worker touches
/// it and lease recovery leaves it alone until the sub-workflow sweep resolves
/// it. Fence-guarded to this claim; returns whether it parked.
/// How deep `run_id` sits in the sub-workflow chain (#23) — see the SQLite twin
/// for the reasoning. `0` for a run nobody triggered, `1` for a run spawned by a
/// task in a root run, and so on; the walk stops at `limit`, which both bounds
/// the cost and guarantees termination if the column ever describes a cycle.
pub async fn sub_workflow_depth(pool: &Pool, run_id: &str, limit: i64) -> Result<i64> {
    let mut current = run_id.to_string();
    let mut depth: i64 = 0;
    while depth < limit {
        let parent: Option<(String,)> =
            sqlx::query_as("SELECT run_id FROM task_runs WHERE sub_run_id = $1 LIMIT 1")
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
         SET sub_run_id = $1, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = $2 AND status = 'running' AND version = $3",
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
         WHERE id = $1 AND status = 'running' AND version = $2",
    )
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Resolve a parked sub-workflow task (#23) once its child run is terminal:
/// `succeeded` → the task succeeds and dependents advance; otherwise it fails.
/// Guarded on the parked shape (`status = 'running' AND sub_run_id IS NOT NULL`),
/// so a re-sweep is a no-op.
pub async fn resolve_subworkflow(pool: &Pool, task_id: &str, succeeded: bool) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) = if succeeded {
        ("succeeded", "sub-workflow succeeded")
    } else {
        ("failed", "sub-workflow failed")
    };
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2, output = $3
         WHERE id = $4 AND status = 'running' AND sub_run_id IS NOT NULL",
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
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
            sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
                .bind(&child_run_id)
                .fetch_optional(pool)
                .await?;
        let Some(cs) = child_status else { continue };
        let succeeded = match cs.as_str() {
            "succeeded" => true,
            "failed" | "cancelled" => false,
            _ => continue,
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
         SET wake_at = $1, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = $2 AND status = 'running' AND version = $3",
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
         WHERE status = 'running' AND wake_at IS NOT NULL AND wake_at <= $1",
    )
    .bind(&now)
    .fetch_all(pool)
    .await?;

    let mut resolved = Vec::new();
    for (task_id,) in due {
        let mut tx = pool.begin().await?;
        let rows = sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', finished_at = $1, output = 'wait elapsed'
             WHERE id = $2 AND status = 'running' AND wake_at IS NOT NULL AND wake_at <= $3",
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
                 SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
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

/// Park a claimed `type: wait` HTTP sensor (#27 follow-on): keep it `running` but
/// drop the worker claim/lease and record the endpoint in `wait_url` with an
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
         SET wait_url = $1, next_poll_at = $2, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = $3 AND status = 'running' AND version = $4",
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
/// (oldest deadline first — the cap bounds how much probing one tick can do): `running` tasks
/// holding a `wait_url` whose `next_poll_at` has elapsed (or is NULL). Returns
/// `(task_id, wait_url)` pairs for the engine to GET.
pub async fn due_url_waits(pool: &Pool, limit: i64) -> Result<Vec<(String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, wait_url FROM task_runs
         WHERE status = 'running' AND wait_url IS NOT NULL
           AND (next_poll_at IS NULL OR next_poll_at <= $1)
         ORDER BY next_poll_at
         LIMIT $2",
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
        "UPDATE task_runs SET next_poll_at = $1
         WHERE id = $2 AND status = 'running' AND wait_url IS NOT NULL",
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
        "UPDATE task_runs SET status = 'succeeded', finished_at = $1, output = 'wait url ready'
         WHERE id = $2 AND status = 'running' AND wait_url IS NOT NULL",
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
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
        sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = $1")
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await?;
    for uri in uris {
        sqlx::query(
            "INSERT INTO datasets (uri, updated_at, last_run_id, last_task, updates)
             VALUES ($1, $2, $3, $4, 1)
             ON CONFLICT (uri) DO UPDATE SET
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
             VALUES ($1, $2, $3, $4, $5, 'task', $6)",
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
         VALUES ($1, $2, NULL, NULL, 1)
         ON CONFLICT (uri) DO UPDATE SET
             updated_at = excluded.updated_at, updates = datasets.updates + 1",
    )
    .bind(uri)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO dataset_events (uri, source, at) VALUES ($1, 'external', $2)")
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
        sqlx::query("DELETE FROM dataset_triggers WHERE workflow_name = $1")
            .bind(workflow_name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(());
    }
    sqlx::query(
        "DELETE FROM dataset_triggers
         WHERE workflow_name = $1 AND uri <> ALL($2::text[])",
    )
    .bind(workflow_name)
    .bind(uris)
    .execute(&mut *tx)
    .await?;
    for uri in uris {
        sqlx::query(
            "INSERT INTO dataset_triggers (workflow_name, uri, cursor, mode)
             VALUES ($1, $2, (SELECT COALESCE(MAX(id), 0) FROM dataset_events WHERE uri = $2), $3)
             ON CONFLICT (workflow_name, uri) DO UPDATE SET mode = excluded.mode",
        )
        .bind(workflow_name)
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
    let rows: Vec<(String, String, i64, String, i64)> = sqlx::query_as(
        "SELECT t.workflow_name, t.uri, t.cursor, t.mode,
                COALESCE((SELECT MAX(id) FROM dataset_events e WHERE e.uri = t.uri), 0)
         FROM dataset_triggers t
         ORDER BY t.workflow_name, t.uri",
    )
    .fetch_all(pool)
    .await?;

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
        let mode_all = subs.iter().any(|(_, _, m, _)| m == "all");
        if mode_all && fresh.len() < subs.len() {
            continue;
        }
        let mut tx = pool.begin().await?;
        let mut advanced = Vec::with_capacity(fresh.len());
        let mut won = true;
        for (uri, cursor, _, max_id) in &fresh {
            let n = sqlx::query(
                "UPDATE dataset_triggers SET cursor = $1
                 WHERE workflow_name = $2 AND uri = $3 AND cursor = $4",
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
            "UPDATE dataset_triggers SET cursor = $1
             WHERE workflow_name = $2 AND uri = $3 AND cursor = $4",
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
        "UPDATE dataset_triggers SET last_fired_at = $1, last_fired_run_id = $2
         WHERE workflow_name = $3",
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
         SET wait_dataset = $1,
             wait_dataset_cursor = (SELECT COALESCE(MAX(id), 0) FROM dataset_events WHERE uri = $1),
             claimed_by = NULL, lease_expires_at = NULL
         WHERE id = $2 AND status = 'running' AND version = $3",
    )
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
            "UPDATE task_runs SET status = 'succeeded', finished_at = $1, output = 'dataset updated'
             WHERE id = $2 AND status = 'running' AND wait_dataset IS NOT NULL",
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
                 SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
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
         FROM datasets ORDER BY updated_at DESC LIMIT $1",
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
         WHERE ($1::text IS NULL OR uri = $1)
         ORDER BY id DESC LIMIT $2",
    )
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await?)
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
         SET lease_expires_at = $1
         WHERE id = $2 AND claimed_by = $3 AND version = $4 AND status = 'running'",
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
         SET checkpoint_uri = $1, checkpoint_marker = $2
         WHERE id = $3 AND run_id = $4 AND status = 'running'",
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
        "SELECT checkpoint_uri, checkpoint_marker FROM task_runs WHERE id = $1",
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
/// unclaimed `ready` rows move, so no fence is disturbed; affected runs are
/// notified so schedulers of the target class wake. Returns tasks moved.
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
         SET runner_class = $1
         WHERE status = 'ready'
           AND runner_class = $2
           AND gang_id IS NULL
           AND (
                 (scheduled_at IS NOT NULL AND scheduled_at <= $3)
              OR attempt >= $4
           )
         RETURNING run_id",
    )
    .bind(to_class)
    .bind(from_class)
    .bind(&aged_before)
    .bind(min_attempt)
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
    let rows = sqlx::query(
        "UPDATE task_runs
         SET runner_class = $1
         WHERE status = 'ready'
           AND runner_class = $2
           AND gang_id IS NULL
           AND run_id IN (
               SELECT id FROM workflow_runs
               WHERE status = 'running'
                 AND COALESCE(deadline_at, alert_deadline_at) IS NOT NULL
                 AND COALESCE(deadline_at, alert_deadline_at) <= $3
           )
         RETURNING run_id",
    )
    .bind(to_class)
    .bind(from_class)
    .bind(&horizon)
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
             output = NULL, finished_at = NULL, scheduled_at = $3, version = version + 1,
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

/// Workflow (definition) name for a task by its id — the context a memo write
/// needs when only the task id is at hand (#22).
pub async fn workflow_name_for_task(pool: &Pool, task_id: &str) -> Result<Option<String>> {
    let name: Option<String> = sqlx::query_scalar(
        "SELECT d.name FROM task_runs tr
         JOIN workflow_runs wr ON wr.id = tr.run_id
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE tr.id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(name)
}

/// Look up a memoized task result (#22): the cached output when a memo exists for
/// `(workflow, task, key)` and is younger than `max_age_secs` (if set). A stale
/// entry misses so the task re-runs and refreshes it.
pub async fn memo_lookup(
    pool: &Pool,
    workflow: &str,
    task: &str,
    key: &str,
    max_age_secs: Option<u64>,
) -> Result<Option<String>> {
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT output, created_at FROM task_memo
         WHERE workflow_name = $1 AND task_name = $2 AND cache_key = $3",
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
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT(workflow_name, task_name, cache_key)
         DO UPDATE SET output = EXCLUDED.output, created_at = EXCLUDED.created_at",
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
         WHERE wr.id = $1",
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
        sqlx::query_as("SELECT value FROM ui_settings WHERE key = $1")
            .bind(key)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|r| r.0))
}

/// One task's stored TaskSpec JSON (the `input` column), for spec-driven
/// post-success decisions (the `repeat:` loop operator).
pub async fn task_input_json(pool: &Pool, task_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT input FROM task_runs WHERE id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|r| r.0))
}

// ── Environments (variable sets + encrypted secrets, spec `environment:`) ────

/// The environment name a run was created with (None = none declared).
pub async fn run_environment(pool: &Pool, run_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT environment FROM workflow_runs WHERE id = $1")
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
        sqlx::query_as("SELECT variables FROM environments WHERE name = $1")
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
         WHERE e.name = $1 AND s.name = $2",
    )
    .bind(env_name)
    .bind(secret_name)
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
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
// Not `ops`-gated: the claim path uses it too, so gating it made
// `--features postgres` alone fail to compile. Nothing built that combination
// until the GitOps worker did (the engine always carries `ops`).
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
        // Tolerant: a projection that does not select `pool` still maps (→ None).
        pool: row.try_get("pool").ok().flatten(),
        // Dispatch priority + memo-resolution flag, same tolerance as `pool`.
        priority: row.try_get("priority").unwrap_or(0),
        cache_hit: row.try_get("cache_hit").unwrap_or(false),
        // Park reasons, same tolerance: only the run-detail read selects them.
        wake_at: row.try_get("wake_at").ok().flatten(),
        wait_url: row.try_get("wait_url").ok().flatten(),
        wait_dataset: row.try_get("wait_dataset").ok().flatten(),
        sub_run_id: row.try_get("sub_run_id").ok().flatten(),
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
                scheduled_at, finished_at, pool, priority, cache_hit,
                wake_at, wait_url, wait_dataset, sub_run_id
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
/// BIGINT columns by design (see migrations_pg 001), so two decode attempts
/// cover every column — and the archiver survives future migrations without a
/// column-list edit.
#[cfg(feature = "ops")]
fn row_to_json(row: &sqlx::postgres::PgRow) -> serde_json::Value {
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
           AND finished_at IS NOT NULL AND finished_at < $1
         ORDER BY finished_at LIMIT $2",
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
             WHERE wr.id = $1",
        )
        .bind(&id)
        .fetch_one(pool)
        .await?;
        let tasks = sqlx::query("SELECT * FROM task_runs WHERE run_id = $1 ORDER BY name")
            .bind(&id)
            .fetch_all(pool)
            .await?;
        let outbox =
            sqlx::query("SELECT * FROM event_outbox WHERE run_id = $1 ORDER BY created_at")
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
         VALUES ($1, $2, $3, $4, $5, $6)
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
    let stamped = sqlx::query(
        "UPDATE archived_runs SET compacted_at = $1, parquet_path = $2
         WHERE run_id = ANY($3)",
    )
    .bind(&now)
    .bind(parquet_path)
    .bind(run_ids)
    .execute(pool)
    .await?
    .rows_affected();
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
            "SELECT 1::bigint FROM workflow_runs
             WHERE id = $1 AND status IN ('succeeded','failed','cancelled')",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if terminal.is_none() {
            continue;
        }
        sqlx::query(
            "DELETE FROM task_dependencies
             WHERE dependent_id IN (SELECT id FROM task_runs WHERE run_id = $1)",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM task_runs WHERE run_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        purged += sqlx::query("DELETE FROM workflow_runs WHERE id = $1")
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
    /// Connect the LISTEN session. `DATABASE_LISTEN_URL`, when set, is used
    /// instead of the query pool's target: on a shared state-store cell the
    /// pool points at PgBouncer (transaction mode), which cannot serve a
    /// session-scoped `LISTEN` — the listener must hit the **direct** Postgres
    /// endpoint (the split-DSN setup). Unset, the listener shares
    /// the pool's connection config exactly as before.
    pub async fn connect(pool: &Pool) -> Result<Self> {
        let mut listener = match std::env::var("DATABASE_LISTEN_URL") {
            Ok(url) if !url.trim().is_empty() => PgListener::connect(url.trim()).await?,
            _ => PgListener::connect_with(pool).await?,
        };
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
