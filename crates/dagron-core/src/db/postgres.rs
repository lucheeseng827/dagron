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
//!    changes readiness, rather than waiting out the full tick. A wake takes
//!    every notification that is waiting, so a burst of them is one tick.
//!
//! The state machine, lease semantics, dependency-counter decrement, and
//! recursive downstream cancellation are identical to the SQLite path — only the
//! claim mechanism and wake source differ.

use std::collections::HashMap;
use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use anyhow::{bail, Result};
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    attempt_log::{AttemptEnd, Policy as AttemptPolicy},
    dag::DagGraph,
    models::{RunStatus, TaskRun},
    quota_wait::{self, QuotaRequeue, QuotaWaitStats},
};
/// Only `list_task_attempts` materializes this, and that is an `ops` read
/// endpoint — a lean engine build never constructs one.
#[cfg(feature = "ops")]
use crate::models::TaskAttempt;
use super::Woken;

/// NOTIFY channel the reconcile loop listens on for early wakeups.
const EVENT_CHANNEL: &str = "task_events";

/// One `pending`, deps-satisfied task the slow advance path has to decide
/// about: `(id, run_id, trigger_rule, is_approval, input, fanout_of)`.
///
/// An alias rather than the tuple inline: this grew a sixth element when the
/// runtime fan-out needed routing here, and six positional fields read at the
/// call site as a row of unlabelled values.
type AdvanceCandidate = (String, String, String, i64, Option<String>, Option<String>);

/// A parked runtime fan-out barrier, as `reconcile_fanouts` reads it:
/// `(id, run_id, input, allow_failure, version, runner_class, priority, pool)`.
///
/// The last three are claim inputs the instances inherit — taken from the
/// **row** rather than re-derived from the spec, because the row's
/// `runner_class` was already resolved against the DAG default at creation.
type ParkedFanout = (String, String, Option<String>, i64, i64, String, i64, Option<String>);

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
    migrate(&pool, &mut base).await?;
    #[cfg(feature = "enterprise")]
    {
        // enterprise migrations live in a separate dir but share sqlx's single
        // `_sqlx_migrations` table with the base set. Their versions are offset
        // above the base range (900+) so they never collide, and ignore_missing
        // lets this migrator tolerate the base migrations it doesn't own (else
        // sqlx errors "previously applied but missing in resolved migrations").
        let mut ee = sqlx::migrate!("./migrations_pg_ee");
        ee.set_ignore_missing(true);
        migrate(&pool, &mut ee).await?;
    }
    Ok(pool)
}

/// Run `migrator` under the migration lock, replaying it when a migration lost
/// a race to dagron-api.
///
/// Engines that start together migrate one at a time, under the session
/// advisory lock sqlx's migrator takes, by sqlx's key ([`migration_lock_key`]):
/// an engine of an earlier release, which takes it sqlx's way, waits for this
/// one, and this one for it. The key is the database's, whatever the schema,
/// so dagron-ci takes the same lock where it shares the engine's database, as
/// it does by default, and it waits for it as this does
/// (`crates/dagron-ci/src/db.rs`). The lock is taken here, and `migrator` has
/// sqlx's own locking turned off (which is why it is taken mutably), because
/// sqlx waits inside one `SELECT pg_advisory_lock`, and a session waiting in a
/// statement holds a snapshot. A `-- no-transaction` migration's `CREATE INDEX
/// CONCURRENTLY` waits for every older snapshot in the database to go, so the
/// holder's index build waited for the waiter while the waiter waited for the
/// holder. Postgres broke that deadlock by failing the side whose own check, a
/// `deadlock_timeout` (1 s) into its wait, found the cycle closed: the waiter,
/// which exited, when the build came to wait for it within that second; else
/// the build, as over a populated table, which left the index INVALID and the
/// engine exited, and the next engine's `IF NOT EXISTS` then took the index for
/// built and recorded the migration as applied. Here a waiter polls
/// `pg_try_advisory_lock`, each try a statement of its own, so between tries
/// it holds no snapshot. (An engine or a dagron-ci of an earlier release still
/// waits sqlx's way, so one started beside this one's index builds still meets
/// that deadlock: an old container restarted during an upgrade, one that a
/// rollback starts, or a dagron-ci pinned to an earlier image.)
///
/// The lock is a session's, so through a pooler in transaction mode (a cell's
/// PgBouncer) it keeps no two migrators apart, as sqlx's did not: a try that
/// reaches the holder's server session takes the lock again. Two engines that
/// come to the same `CREATE INDEX CONCURRENTLY` there deadlock as above, the
/// second waiting for the table's lock inside its statement, with a snapshot
/// the first's build waits out. Only a direct connection keeps them apart.
///
/// dagron-api's schema bootstrap creates some of these tables too (`users`,
/// `git_repos`, `environments`: migrations 008, 009 and 026), and compose and a
/// Helm install start both processes at once. `CREATE TABLE IF NOT EXISTS` is
/// not atomic against a concurrent creator. When dagron-api's statement commits
/// first, the migration fails on a catalog constraint instead of taking the
/// no-op path, and the engine exited before it served:
///
/// ```text
/// Error: while executing migration 8: error returned from database:
///   duplicate key value violates unique constraint "pg_type_typname_nsp_index"
/// ```
///
/// A migration that ran in a transaction rolled back whole and was not
/// recorded, so it is replayed, and this time `IF NOT EXISTS` sees the
/// committed table. One marked `-- no-transaction` (the concurrent index
/// builds) is never replayed: a failure can leave it half done.
///
/// The lock is the session's, so after a failure the attempt's connection is
/// closed rather than returned to the pool: closing ends the session, and
/// that frees the lock. Returned, the connection would keep the lock, and the
/// next attempt, on another connection after a lost race, would wait for it
/// forever.
async fn migrate(pool: &PgPool, migrator: &mut sqlx::migrate::Migrator) -> Result<()> {
    const MAX_ATTEMPTS: u32 = 4;
    migrator.set_locking(false);
    let mut attempt = 1;
    loop {
        let mut conn = pool.acquire().await?;
        let key = match lock_migrations(&mut conn).await {
            Ok(key) => key,
            Err(e) => {
                let _ = conn.close().await;
                return Err(e);
            }
        };
        match migrator.run_direct(&mut *conn).await {
            Ok(()) => {
                release_migration_lock(conn, key).await;
                return Ok(());
            }
            Err(e) if attempt < MAX_ATTEMPTS && lost_a_race(migrator, &e) => {
                let _ = conn.close().await;
                tracing::warn!(
                    attempt,
                    error = %e,
                    "a migration raced another session creating the same object; retrying"
                );
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
                attempt += 1;
            }
            Err(e) => {
                let _ = conn.close().await;
                return Err(e.into());
            }
        }
    }
}

/// How long a migrator waiting for another's lock sleeps between tries.
const MIGRATION_LOCK_POLL: Duration = Duration::from_millis(200);

/// How often a migrator still waiting for the lock says so again, louder.
const MIGRATION_LOCK_WARN_EVERY: Duration = Duration::from_secs(60);

/// Take the migration lock on `conn`, waiting for another session that holds
/// it, and return its key.
///
/// Each try is a `pg_try_advisory_lock` of its own, with a sleep between, so
/// a waiter holds no snapshot while it waits: see [`migrate`] for the deadlock
/// a waiter in `pg_advisory_lock` takes part in.
async fn lock_migrations(conn: &mut sqlx::PgConnection) -> Result<i64> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut *conn)
        .await?;
    let key = migration_lock_key(&database);
    let started = std::time::Instant::now();
    let mut warn_at = MIGRATION_LOCK_WARN_EVERY;
    let mut waiting = false;
    while !try_migration_lock(conn, key).await? {
        if !waiting {
            waiting = true;
            tracing::info!(
                database = %database,
                "waiting for another engine's or dagron-ci's migrations to finish"
            );
        } else if started.elapsed() >= warn_at {
            warn_at += MIGRATION_LOCK_WARN_EVERY;
            // A long index build can hold it this long; so can a session that
            // is not migrating at all, as a pooler in transaction mode can
            // leave one holding it. pg_locks shows a bigint key in two halves.
            tracing::warn!(
                database = %database,
                key,
                classid = key >> 32,
                objid = key & 0xffff_ffff,
                waited_secs = started.elapsed().as_secs(),
                "still waiting for the migration lock; its holder is the session \
                 pg_locks shows with locktype advisory, this classid and objid, and \
                 objsubid 1"
            );
        }
        tokio::time::sleep(MIGRATION_LOCK_POLL).await;
    }
    Ok(key)
}

/// Let the migration lock go. A connection whose unlock fails is closed rather
/// than returned to the pool, for closing the session frees the lock all the
/// same; one whose unlock finds the lock not its own is closed too, so that it
/// is not used again (the lock is on another server session, which closing
/// this connection does not end).
async fn release_migration_lock(mut conn: sqlx::pool::PoolConnection<sqlx::Postgres>, key: i64) {
    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await;
    match released {
        Ok(true) => {}
        Ok(false) => {
            // The try and the unlock reached two server sessions, as through
            // a pooler in transaction mode: the lock is left on the other.
            tracing::warn!(
                key,
                "the migration lock was not this session's to release; through a \
                 pooler in transaction mode the lock keeps no migrators apart"
            );
            let _ = conn.close().await;
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "could not release the migration lock; closing its connection"
            );
            let _ = conn.close().await;
        }
    }
}

async fn try_migration_lock(conn: &mut sqlx::PgConnection, key: i64) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(conn)
        .await?)
}

/// The advisory-lock key of a database's migrations, as sqlx-postgres 0.8
/// makes it for its own migrator's lock (`generate_lock_id`): the CRC-32
/// (ISO-HDLC) of the database's name times `0x3d32ad9e`. The product fits an
/// `i64`. It must stay sqlx's key, for engines of earlier releases take that.
fn migration_lock_key(database: &str) -> i64 {
    0x3d32_ad9e * i64::from(crc32_iso_hdlc(database.as_bytes()))
}

/// CRC-32 ISO-HDLC (the zlib and Ethernet CRC), bit by bit: it runs over one
/// database name per migrator, so a table would buy nothing.
fn crc32_iso_hdlc(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
    }
    !crc
}

/// Did a migration that ran in a transaction fail because another session
/// created the same object first, so a replay takes the no-op path?
///
/// `23505` counts only on a `pg_*` catalog index: a unique violation in a data
/// migration is that migration's own error. Then the codes Postgres raises when
/// an object appears between the existence check and the create, and a
/// deadlock, which also rolls the migration back whole.
fn lost_a_race(migrator: &sqlx::migrate::Migrator, err: &sqlx::migrate::MigrateError) -> bool {
    let sqlx::migrate::MigrateError::ExecuteMigration(e, version) = err else {
        return false;
    };
    let in_transaction = migrator.iter().any(|m| m.version == *version && !m.no_tx);
    in_transaction
        && e.as_database_error()
            .is_some_and(|db| match db.code().as_deref() {
                Some("23505") => db.constraint().is_some_and(|c| c.starts_with("pg_")),
                // duplicate_table / duplicate_object / duplicate_column / deadlock_detected
                Some("42P07" | "42710" | "42701" | "40P01") => true,
                _ => false,
            })
}

/// Readiness probe: one round trip proving a pooled connection can be acquired
/// and the datastore answers. Used by the ops API's `/readyz` (LOW_LATENCY R-1).
pub async fn ping(pool: &Pool) -> Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}

/// The indexes of this schema that an interrupted `CREATE INDEX CONCURRENTLY`
/// left INVALID, by name. Postgres keeps such an index up to date and never
/// reads it, so the statement it was for reads as if it had none. Nothing
/// here rebuilds one: when its migration runs again, `IF NOT EXISTS` takes
/// it for built, and the migration is then recorded as applied. `REINDEX
/// INDEX CONCURRENTLY` does.
pub async fn invalid_indexes(pool: &Pool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_index i
           JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE NOT i.indisvalid AND n.nspname = current_schema()
            AND NOT EXISTS (SELECT 1 FROM pg_stat_progress_create_index p
                             WHERE p.relid = i.indrelid)
          ORDER BY 1",
    )
    .fetch_all(pool)
    .await?)
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction, then NOTIFYs so any idle scheduler wakes to advance
/// the root tasks. Returns the new run_id.
pub async fn create_run(pool: &Pool, dag: &DagGraph, yaml_spec: &str) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, None, None, None, None, None).await
}

/// [`create_run`] that records what started the run (`run_triggers`), in the
/// same transaction: a run is never visible without what it was for, and a run
/// that was refused leaves no record behind.
pub async fn create_run_triggered(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    trigger: &crate::models::RunTrigger,
) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, None, None, None, Some(trigger), None).await
}

/// [`create_run_triggered`] for a run a gateway's caller submitted: who that
/// was is kept on the run as well (`triggered_by`), in the same insert.
pub async fn create_run_triggered_by(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    triggered_by: &str,
    trigger: &crate::models::RunTrigger,
) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, None, None, Some(triggered_by), Some(trigger), None).await
}

/// [`create_run_triggered_by`] for a submit made under an
/// `Idempotency-Key`: the caller's claim on the key is resolved to the run
/// **in the same transaction**. The run and the key's answer commit together
/// or not at all, so a key never names a run that is not there, and a run
/// made under a key is never left without it.
///
/// It used to be two steps: the run was created, and the claim was pointed at
/// it in a statement of its own afterwards, whose failure was logged and let
/// go. A submitter that died in between, or whose second statement failed,
/// left a run with an unresolved claim. Once that claim went stale the
/// caller's retry took the key over and made a second run: the one thing the
/// key is there to prevent. So did a submit slow enough for its claim to go
/// stale while it was still writing.
///
/// `Ok(None)` when the claim is no longer this submit's
/// ([`IdempotencyClaim::claimed_at`](super::IdempotencyClaim)): a later
/// submit took the key over, or the claim was released. Then nothing is
/// created, and the caller answers from what holds the key now.
///
/// The claim's row is locked as the transaction's first statement, so two
/// submits of one key queue there and a stale claim cannot be taken over
/// while the run that will resolve it is being written.
pub async fn create_run_claimed(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    triggered_by: Option<&str>,
    trigger: Option<&crate::models::RunTrigger>,
    claim: &super::IdempotencyClaim<'_>,
) -> Result<Option<String>> {
    match create_run_inner(pool, dag, yaml_spec, None, None, triggered_by, trigger, Some(claim)).await {
        Ok(run_id) => Ok(Some(run_id)),
        Err(e) if e.is::<super::IdempotencyClaimLost>() => Ok(None),
        Err(e) => Err(e),
    }
}

/// [`create_run`] that also commits a streaming source's cursor **in the same
/// transaction** — the exactly-once ingestion primitive: the run and the
/// source position `(source_name, position)` become durable atomically, so a
/// crash can never leave a created run whose input position would be replayed
/// (a duplicate run) or an advanced position whose run was lost.
///
/// `trigger` is what started the run, recorded with it.
pub async fn create_run_with_offset(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    source_name: &str,
    position: &str,
    trigger: Option<&crate::models::RunTrigger>,
) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, Some((source_name, position)), None, None, trigger, None).await
}

/// [`create_run`] that deletes the dead letter `dead_letter_id` **in the same
/// transaction**: the redrive primitive. The run and the delete commit together
/// or not at all, so a refusal after the claim (the run cap, any error) rolls
/// the delete back as well and leaves the dead letter exactly as it was, with
/// the same id and history, ready to redrive again. `Ok(None)` when the row is
/// already gone (redriven or discarded first), and then nothing is created.
///
/// The delete is the claim. Concurrent redrives of one id queue on its row
/// lock, and each loser finds the row gone once the winner commits, so a dead
/// letter becomes at most one run.
///
/// `trigger` is what started the run, recorded with it.
#[cfg(feature = "ops")]
pub async fn create_run_from_dead_letter(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    dead_letter_id: &str,
    trigger: Option<&crate::models::RunTrigger>,
) -> Result<Option<String>> {
    match create_run_inner(pool, dag, yaml_spec, None, Some(dead_letter_id), None, trigger, None).await {
        Ok(run_id) => Ok(Some(run_id)),
        Err(e) if e.is::<super::DeadLetterGone>() => Ok(None),
        Err(e) => Err(e),
    }
}

/// The run writer behind [`create_run`], [`create_run_with_offset`],
/// `create_run_from_dead_letter` and [`create_run_claimed`]. Besides the run,
/// `offset` commits a source cursor, `dead_letter` deletes that dead letter
/// first, as the claim, `trigger` records what started the run, and `claim`
/// resolves the caller's `Idempotency-Key` to the run, all in the one
/// transaction.
#[allow(clippy::too_many_arguments)]
async fn create_run_inner(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    offset: Option<(&str, &str)>,
    dead_letter: Option<&str>,
    triggered_by: Option<&str>,
    trigger: Option<&crate::models::RunTrigger>,
    claim: Option<&super::IdempotencyClaim<'_>>,
) -> Result<String> {
    // A run that is not trusted is confined (`crate::confine`). What such a
    // run may not use is refused here, where every run is made, before
    // anything of it is written; and its rows are written from a copy of the
    // graph without what it is not given.
    let held;
    let dag = match crate::confine::held(dag, crate::envpolicy::class_of(trigger))? {
        Some(graph) => {
            held = graph;
            &held
        }
        None => dag,
    };
    // And a task of such a run that names a protected environment waits on
    // a gate of the policy's approvers (`crate::protected`), put in here,
    // after the run was held to what it may write.
    let gated;
    let dag = match crate::protected::gated(pool, dag, trigger).await? {
        Some(graph) => {
            gated = graph;
            &gated
        }
        None => dag,
    };
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

    // The caller's `Idempotency-Key` claim (`create_run_claimed`), locked as
    // the transaction's first statement and resolved below, once the run's row
    // is there for it to name. Locked first for the reason the dead letter is
    // claimed first: nothing then waits on a claim while it holds a workflow's
    // admission lock. A claim that is not this submit's any more (taken over
    // once it went stale, or released) ends the transaction here, before
    // anything of the run is written.
    if let Some(claim) = claim {
        let ours: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM run_idempotency
              WHERE principal = $1 AND idempotency_key = $2
                AND run_id IS NULL AND created_at = $3
                FOR UPDATE",
        )
        .bind(claim.principal)
        .bind(claim.key)
        .bind(claim.claimed_at)
        .fetch_optional(&mut *tx)
        .await?;
        if ours.is_none() {
            return Err(anyhow::Error::new(super::IdempotencyClaimLost));
        }
    }

    // Redrive claim (`create_run_from_dead_letter`), the transaction's first
    // statement. A concurrent redrive of the same id blocks on this row lock
    // until the holder commits (the row is gone) or rolls back (the waiter
    // claims it). Claiming before the admission lock below means no redrive
    // waits on a dead letter while holding a workflow's admission lock, so two
    // redrives can never deadlock.
    if let Some(id) = dead_letter {
        let claimed = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if claimed == 0 {
            return Err(anyhow::Error::new(super::DeadLetterGone));
        }
    }

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
    //
    // A run that arrives at a full cap is refused, unless its workflow says a
    // run waits (`when_full`): then it is made, with `queued_at`, and each of
    // its tasks is written with one dependency more than it has. Nothing
    // advances such a task, and [`admit_waiting_runs`] takes the one away
    // when the run's turn comes. A run that waits is in the way of the next
    // one as a run that runs is, so runs are let in in the order they came.
    let mut waits = false;
    if let Some(max) = dag.spec.max_active_runs {
        if max > 0 {
            // With a `concurrency_key` the cap is per (workflow, key): the lock
            // narrows to that pair too, so two stacks admit in parallel.
            // \u{1f} cannot appear in a workflow name (`[A-Za-z0-9_.-]`), so
            // no name/key pair collides with another name.
            let lock_scope = match &dag.spec.concurrency_key {
                Some(key) => format!("{}\u{1f}{key}", dag.spec.name),
                None => dag.spec.name.clone(),
            };
            sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
                .bind(RUN_ADMISSION_LOCK)
                .bind(&lock_scope)
                .execute(&mut *tx)
                .await?;
            let when_full = dag.spec.when_full.unwrap_or_default();
            if when_full == crate::dag::WhenFull::Supersede {
                // A run that is not trusted takes the place of its own
                // source's runs alone (`crate::confine::SPEC_ALLOWED`).
                let own = trigger.filter(|t| crate::confine::confined(crate::envpolicy::class_of(Some(t))));
                supersede_runs(
                    &mut tx,
                    &dag.spec.name,
                    dag.spec.concurrency_key.as_deref(),
                    &run_id,
                    own,
                    &now,
                )
                .await?;
            }
            let (active, waiting) =
                runs_in_flight(&mut tx, &dag.spec.name, dag.spec.concurrency_key.as_deref(), None).await?;
            if active >= i64::from(max) {
                if !when_full.waits() || waiting >= crate::dag::max_waiting_runs() {
                    return Err(anyhow::Error::new(crate::models::MaxActiveRunsReached {
                        name: dag.spec.name.clone(),
                        max,
                        active,
                        waiting: if when_full.waits() { waiting } else { 0 },
                        key: dag.spec.concurrency_key.clone(),
                    }));
                }
                waits = true;
            }
        }
    }
    // And the installation's own cap on the runs running at once, where it
    // has one: over it, the run waits as it would for its workflow's. It
    // waits behind the runs that wait already, so a place a run's end frees
    // is the sweep's to give the oldest, not the newest's to take.
    if !waits && installation_room(&mut tx, Behind::Waiting).await?.is_some_and(|room| room <= 0) {
        waits = true;
    }
    // The dependency a waiting run's tasks are held by, which none of them has.
    let held_by = i64::from(waits);

    sqlx::query(
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(&def_id)
    .bind(&dag.spec.name)
    .bind(yaml_spec)
    .bind(&now)
    .execute(&mut *tx)
    .await?;

    // Clock confidence (migration 052): whatever this process currently knows
    // about its own wall clock rides the row. `unknown` is a stored value, not
    // NULL, so "never assessed" stays distinguishable from "written before
    // the column existed". See `crate::clock`.
    let clock = crate::clock::current();
    // The parameters the run was created with (migration 067), for templates
    // resolved after submit. Stored even when empty: `{}` says "none", NULL
    // says "written before the column existed".
    let parameters = serde_json::to_string(&dag.parameters)?;
    // A `notify.git` run's `pending` status is owed from the moment it exists
    // (migration 070); an engine with a forge token posts it.
    let start_pending_at = dag
        .spec
        .notify
        .as_ref()
        .and_then(|n| n.git.as_ref())
        .map(|_| &now);
    sqlx::query(
        "INSERT INTO workflow_runs
           (id, definition_id, status, created_at, deadline_at, alert_deadline_at, result_from, environment,
            clock_confidence, clock_offset_ms, clock_source, concurrency_key, triggered_by, parameters,
            start_pending_at, queued_at)
         VALUES ($1, $2, 'running', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
    )
    .bind(&run_id)
    .bind(&def_id)
    .bind(&now)
    .bind(&deadline_at)
    .bind(&alert_deadline_at)
    .bind(&dag.spec.result_from)
    .bind(&dag.spec.environment)
    .bind(clock.confidence.as_str())
    .bind(clock.offset_ms)
    .bind(&clock.source)
    .bind(&dag.spec.concurrency_key)
    .bind(triggered_by)
    .bind(&parameters)
    .bind(start_pending_at)
    .bind(waits.then_some(&now))
    .execute(&mut *tx)
    .await?;

    // What started the run (migrations 072, 077), with it or not at all.
    if let Some(t) = trigger {
        sqlx::query(
            "INSERT INTO run_triggers
               (run_id, kind, principal, actor, trust, source, forge_host, repo, ref, sha, base_sha, pr,
                event_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        )
        .bind(&run_id)
        .bind(&t.kind)
        .bind(&t.principal)
        .bind(&t.actor)
        .bind(&t.trust)
        .bind(&t.source)
        .bind(&t.forge_host)
        .bind(&t.repo)
        .bind(&t.git_ref)
        .bind(&t.sha)
        .bind(&t.base_sha)
        .bind(t.pr)
        .bind(&t.event_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    }

    // Its token budget, with it or not at all.
    #[cfg(feature = "enterprise")]
    if let Some(budget) = dag.spec.budget.as_ref().and_then(|b| b.tokens) {
        crate::tokens::open_budget(&mut *tx, &run_id, budget).await?;
    }

    // Create task_run rows; store the full TaskSpec as JSON in `input` so the
    // row is self-contained — dispatch only needs task.id + task.input.
    // A `gang:` task expands into `size` member rows (`<name>.<rank>`) that
    // share a gang_id — the unit the gang-aware claimer seizes all-or-nothing.
    // `task_ids` maps the AUTHORED name to every row it produced, so an edge
    // to a gang fans out to all members (a dependent waits for the whole gang).
    // (LOW_LATENCY B-3) Rows are inserted in multi-row chunks — one statement
    // per ~1,000 tasks / ~5,000 edges instead of one statement per row. On a
    // networked datastore the old shape made submit latency O(tasks + edges)
    // round trips inside one transaction (a 2,000-task / 19,500-edge DAG paid
    // ~21,500 sequential executions); the chunked shape is a handful. Chunks
    // flush as they fill rather than buffering the whole DAG: the serialized
    // TaskSpec is the heavy field, so peak memory is one chunk of rows, not
    // the full run (`task_ids` — name → row ids for edge wiring — is the only
    // whole-DAG structure, and it holds ids alone). Semantics are unchanged:
    // same transaction, same duplicate-name check, same gang fan-out, same
    // column values.
    struct TaskRow {
        id: String,
        name: String,
        dep_count: i64,
        input_json: String,
        trigger_rule: String,
        allow_failure: i64,
        is_approval: i64,
        approval_timeout: Option<i64>,
        approval_on_timeout: Option<String>,
        runner_class: String,
        gang_id: Option<String>,
        gang_rank: Option<i64>,
        gang_size: Option<i64>,
        priority: i64,
        pool_name: Option<String>,
        /// The producer task's authored name for a runtime fan-out
        /// (`with_output_of:`), else None. Denormalised onto the row so the
        /// advance path can route a barrier to its park with a column
        /// predicate instead of re-parsing every candidate's TaskSpec.
        ///
        /// The AUTHORED name, deliberately: the producer may have been fanned
        /// out into `<name>.0`, `<name>.1`, … and this is the only place the
        /// name the author wrote survives. The sweep turns it back into rows
        /// through the barrier's own `depends_on`
        /// (`expand::fanout_producer_rows`).
        fanout_of: Option<String>,
    }
    // Chunk sizes stay well inside both backends' bind limits: 19 columns ×
    // 1,000 rows = 19,000 binds (Postgres caps at 65,535; bundled SQLite at
    // 32,766), edges are 2 × 5,000 = 10,000.
    const TASK_INSERT_CHUNK: usize = 1_000;
    const EDGE_INSERT_CHUNK: usize = 5_000;

    // One multi-row INSERT of at most TASK_INSERT_CHUNK buffered rows — the
    // flush target for the build loop below.
    async fn insert_task_chunk(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        run_id: &str,
        now: &str,
        rows: &[TaskRow],
    ) -> Result<()> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO task_runs \
             (id, run_id, name, status, remaining_deps, input, scheduled_at, trigger_rule, \
              allow_failure, is_approval, approval_timeout_secs, approval_on_timeout, \
              runner_class, gang_id, gang_rank, gang_size, priority, pool, fanout_of) ",
        );
        qb.push_values(rows, |mut b, r| {
            b.push_bind(&r.id)
                .push_bind(run_id)
                .push_bind(&r.name)
                .push_bind("pending")
                .push_bind(r.dep_count)
                .push_bind(&r.input_json)
                .push_bind(now)
                .push_bind(&r.trigger_rule)
                .push_bind(r.allow_failure)
                .push_bind(r.is_approval)
                .push_bind(r.approval_timeout)
                .push_bind(r.approval_on_timeout.as_deref())
                .push_bind(&r.runner_class)
                .push_bind(r.gang_id.as_deref())
                .push_bind(r.gang_rank)
                .push_bind(r.gang_size)
                .push_bind(r.priority)
                .push_bind(r.pool_name.as_deref())
                .push_bind(r.fanout_of.as_deref());
        });
        qb.build().execute(&mut **tx).await?;
        Ok(())
    }

    // Same flush shape for dependency edges.
    async fn insert_edge_chunk(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        edges: &[(String, String)],
    ) -> Result<()> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO task_dependencies (dependent_id, dependency_id) ",
        );
        qb.push_values(edges, |mut b, (dependent_id, dependency_id)| {
            b.push_bind(dependent_id).push_bind(dependency_id);
        });
        qb.build().execute(&mut **tx).await?;
        Ok(())
    }

    let mut task_ids: HashMap<String, Vec<String>> = HashMap::new();
    let mut task_rows: Vec<TaskRow> = Vec::new();
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
        let approval_on_timeout = task_spec.approval_on_timeout.clone();
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
            task_rows.push(TaskRow {
                id: task_id.clone(),
                name,
                dep_count: dep_count + held_by,
                input_json,
                trigger_rule: trigger_rule.to_string(),
                allow_failure,
                is_approval,
                approval_timeout,
                approval_on_timeout: approval_on_timeout.clone(),
                runner_class: runner_class.to_string(),
                gang_id: gang_id.clone(),
                gang_rank: gang_id.as_ref().map(|_| rank as i64),
                gang_size: gang_size.map(|s| s as i64),
                // Priority (#25) and pool (#21) are per-task claim inputs;
                // every member of a gang inherits the authored task's values.
                priority: task_spec.priority,
                pool_name: task_spec.pool.clone(),
                // Gang members can't carry this — `with_output_of` and `gang`
                // are refused together (dag.rs) — but cloning it per member
                // rather than special-casing keeps one expression.
                fanout_of: task_spec.with_output_of.clone(),
            });
            ids.push(task_id);
            // Flush a full chunk immediately — the buffer never holds more
            // than one chunk (a gang wider than the chunk flushes mid-task).
            if task_rows.len() >= TASK_INSERT_CHUNK {
                insert_task_chunk(&mut tx, &run_id, &now, &task_rows).await?;
                task_rows.clear();
            }
        }
        task_ids.insert(task_spec.name.clone(), ids);
    }
    if !task_rows.is_empty() {
        insert_task_chunk(&mut tx, &run_id, &now, &task_rows).await?;
    }

    // Wire up dependency edges (fanning across gang members on either side),
    // flushed in chunks as they accumulate like the task rows above.
    let mut edges: Vec<(String, String)> = Vec::new();
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
                    edges.push((dependent_id.clone(), dependency_id.clone()));
                    if edges.len() >= EDGE_INSERT_CHUNK {
                        insert_edge_chunk(&mut tx, &edges).await?;
                        edges.clear();
                    }
                }
            }
        }
    }
    if !edges.is_empty() {
        insert_edge_chunk(&mut tx, &edges).await?;
    }

    // Exactly-once: the source cursor rides the run's transaction.
    if let Some((source_name, position)) = offset {
        upsert_source_offset(&mut *tx, source_name, position, &now).await?;
    }

    // The key's answer rides it too: the claim locked above now names the run.
    // The row cannot have moved under that lock; the count is checked all the
    // same, since a claim that resolved nothing must not leave a run behind.
    if let Some(claim) = claim {
        let resolved = sqlx::query(
            "UPDATE run_idempotency SET run_id = $1
              WHERE principal = $2 AND idempotency_key = $3
                AND run_id IS NULL AND created_at = $4",
        )
        .bind(&run_id)
        .bind(claim.principal)
        .bind(claim.key)
        .bind(claim.claimed_at)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if resolved != 1 {
            return Err(anyhow::Error::new(super::IdempotencyClaimLost));
        }
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
///
/// A task that was asked to stop is not one of them: it must not run again.
/// Its lapsed lease is [`claim_teardowns`]'s to take.
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
           AND stop_requested_at IS NULL
           AND session_id IS NULL
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
/// (`remaining_deps == 0`) to `ready` or `skipped`, per their `trigger_rule`.
/// Returns the number of tasks transitioned.
///
/// (LOW_LATENCY B-1) Two paths share the work. The **fast path** is one
/// set-based UPDATE pair covering the default shape — `trigger_rule =
/// 'all_success'`, not an approval gate, no runtime `when` — which is nearly
/// every task: ready when every dependency succeeded, skipped otherwise, with
/// skipped rows' dependents decremented in one grouped statement that commits
/// atomically with the skip itself. The old per-candidate implementation
/// issued a dependency-status SELECT plus an UPDATE per task, serially — a
/// 200-wide fan-out becoming ready cost ~400 sequential round trips on the
/// dispatch critical path. Everything not *provably* default-shaped (custom
/// rules, approvals, anything whose stored input merely *contains* a `"when"`
/// key — the LIKE filter is deliberately conservative) takes the **slow
/// path**: the original per-task evaluation, byte-for-byte, so exotic
/// semantics cannot drift.
///
/// Rounds alternate the two paths so a skip cascade feeds newly-unblocked
/// dependents of either shape back in; the cap bounds one call, and anything
/// deeper resolves on the next tick — the old one-level-per-call behaviour,
/// except cascades now usually finish in a single call.
///
/// Every transition stays guarded by `status = 'pending'`, so concurrent
/// schedulers remain winner-take-all and a skip's dependent-decrement runs
/// exactly once.
pub async fn advance_ready_tasks(pool: &Pool) -> Result<u64> {
    let mut transitioned = 0u64;
    for _round in 0..64 {
        // Cheap round gate: with no candidate at all — the common idle tick —
        // this probe is the whole call, matching the old implementation's
        // single candidates query instead of paying the fast path's two
        // UPDATE scans plus the slow path's fetch. `pending` rows only exist
        // for active runs, so every statement here scans live work, never
        // history.
        // `1::bigint`, not `1`. A bare integer literal is INT4 in Postgres and
        // sqlx will not decode INT4 into i64, so the probe panics the reconcile
        // loop the first tick it actually finds a row — an empty queue returns
        // None and never decodes, which is why an idle engine looks healthy.
        // Same cast as the retention purge below.
        let any: Option<i64> = sqlx::query_scalar(
            "SELECT 1::bigint FROM task_runs WHERE status = 'pending' AND remaining_deps = 0 LIMIT 1",
        )
        .fetch_optional(pool)
        .await?;
        if any.is_none() {
            break;
        }
        let n = advance_default_shaped(pool).await? + advance_exotic_once(pool).await?;
        transitioned += n;
        if n == 0 {
            break;
        }
    }
    Ok(transitioned)
}

/// One set-based pass over the default-shaped frontier (see
/// [`advance_ready_tasks`]). `remaining_deps = 0` guarantees every dependency
/// is terminal, so dependency statuses are frozen and the ready/skip split is
/// exact: `all_success` holds iff no dependency is in a non-`succeeded`
/// terminal state. Roots (no edges) satisfy the NOT EXISTS trivially and go
/// ready, matching `trigger_rule_ready`'s empty-deps contract. `allow_failure`
/// plays no part here — it only softens run finalization, never trigger
/// evaluation (`models::trigger_rule_ready`).
async fn advance_default_shaped(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    // Ready half: stamp `scheduled_at` with the readiness instant (the
    // dispatch-latency baseline; see the slow path's twin comment).
    let readied = sqlx::query(
        // `fanout_of IS NULL` keeps a runtime fan-out barrier out of the fast
        // path — mirrors the SQLite backend; see there for why a barrier must
        // never reach `ready` and why excluding it here leaves the claim path
        // untouched.
        "UPDATE task_runs SET status = 'ready', scheduled_at = $1
         WHERE status = 'pending' AND remaining_deps = 0
           AND trigger_rule = 'all_success' AND is_approval = 0 AND fanout_of IS NULL
           AND (input IS NULL OR input NOT LIKE '%\"when\"%')
           AND NOT EXISTS (
               SELECT 1 FROM task_dependencies d
               JOIN task_runs dep ON dep.id = d.dependency_id
               WHERE d.dependent_id = task_runs.id AND dep.status <> 'succeeded'
           )",
    )
    .bind(&now)
    .execute(pool)
    .await?
    .rows_affected();

    // Skip half: at least one dependency ended non-succeeded. The skip and
    // its dependent-decrement are ONE transaction — a skip is a terminal
    // transition, and the old per-task path made the pair atomic. Split
    // across auto-commit statements, a crash between them would leave the
    // skipped rows terminal but their dependents' `remaining_deps` inflated
    // forever: nothing ever revisits a terminal task, so the run stalls
    // permanently. The tx restores exactly the old guarantee, set-based.
    let mut tx = pool.begin().await?;
    let skipped: Vec<String> = sqlx::query_scalar(
        "UPDATE task_runs SET status = 'skipped', finished_at = $1
         WHERE status = 'pending' AND remaining_deps = 0
           AND trigger_rule = 'all_success' AND is_approval = 0
           AND (input IS NULL OR input NOT LIKE '%\"when\"%')
           AND EXISTS (
               SELECT 1 FROM task_dependencies d
               JOIN task_runs dep ON dep.id = d.dependency_id
               WHERE d.dependent_id = task_runs.id AND dep.status <> 'succeeded'
           )
         RETURNING id",
    )
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;
    if !skipped.is_empty() {
        // Grouped by edge count, because a dependent with several deps
        // skipped in this batch must be decremented once per edge, not once
        // per statement.
        sqlx::query(
            "UPDATE task_runs SET remaining_deps = task_runs.remaining_deps - sub.cnt
             FROM (
                 SELECT dependent_id, COUNT(*) AS cnt FROM task_dependencies
                 WHERE dependency_id = ANY($1)
                 GROUP BY dependent_id
             ) AS sub
             WHERE task_runs.id = sub.dependent_id AND task_runs.status = 'pending'",
        )
        .bind(&skipped)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(readied + skipped.len() as u64)
}

/// One per-task pass over candidates the fast path could not prove
/// default-shaped: custom trigger rules, approval gates, and runtime `when`
/// conditions (plus LIKE-filter false positives, which are merely slower here,
/// never wrong). Evaluates each task's `trigger_rule` against its
/// dependencies' outcomes and flips it to `ready` (satisfied) or `skipped`
/// (not satisfied); a newly-skipped task decrements its dependents. This is
/// the original `advance_ready_tasks` body, unchanged.
///
/// Each transition is guarded by `status = 'pending'`, so concurrent schedulers
/// are winner-take-all and a skip's dependent-decrement runs exactly once.
async fn advance_exotic_once(pool: &Pool) -> Result<u64> {
    let candidates: Vec<AdvanceCandidate> = sqlx::query_as(
        "SELECT id, run_id, trigger_rule, is_approval, input, fanout_of FROM task_runs
         WHERE status = 'pending' AND remaining_deps = 0
         ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut transitioned = 0u64;
    for (task_id, run_id, rule, is_approval, input, fanout_of) in candidates {
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
            let rows = if fanout_of.is_some() {
                // A runtime fan-out barrier parks instead of going `ready` —
                // mirrors the SQLite backend. `reconcile_fanouts` resolves it.
                let now = chrono::Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE task_runs
                     SET status = 'running', scheduled_at = $1,
                         claimed_by = NULL, lease_expires_at = NULL
                     WHERE id = $2 AND status = 'pending'",
                )
                .bind(&now)
                .bind(&task_id)
                .execute(pool)
                .await?
                .rows_affected()
            } else if is_approval != 0 {
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
                // Stamp `scheduled_at` with the moment the task became
                // claimable. At creation every task in a run shares the run's
                // single creation timestamp, so this refines — not reorders —
                // the `priority DESC, scheduled_at` claim order into
                // readiness-FIFO, and it is what makes ready-age honest
                // everywhere it is read: the dispatch-latency histogram
                // (LOW_LATENCY A-5), the stale-ready alarm, and spillover
                // reclassing all measure "time since claimable", not "time
                // since the run was created". Retries and approval parks
                // already stamp their own `scheduled_at` on transition.
                let now = chrono::Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE task_runs SET status = 'ready', scheduled_at = $1
                     WHERE id = $2 AND status = 'pending'",
                )
                .bind(&now)
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
/// Pooled tasks currently occupying a slot. See db/sqlite.rs's copy for the
/// reasoning, including why `defer:` is deliberately NOT excluded: the four
/// park shapes wait on something free, while a deferred row waits on a job
/// burning someone's cluster, and counting it is the only thing that lets
/// `pool:` mean "at most N concurrent remote jobs" rather than "N concurrent
/// submissions", which take seconds and bound nothing.
const POOL_RUNNING_COUNT: &str = "SELECT COUNT(*) FROM task_runs
     WHERE status = 'running' AND pool = $1
       AND wake_at IS NULL AND wait_url IS NULL
       AND wait_dataset IS NULL AND sub_run_id IS NULL
       AND fanout_of IS NULL";

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
    // ONE wall-clock read for the whole claim: the `scheduled_at <= now` gate
    // and the lease expiry derive from the same instant, so a clock step
    // between two reads can never hand a task a lease already expired (or a
    // window too long) relative to the gate that admitted it. A duration must
    // never come from two wall-clock reads (clock discipline).
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_exp = (now_dt + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339();

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
                 log = NULL, log_fence = NULL, log_gen = NULL,
                 version = version + 1,
                 session_id = NULL
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
                scheduled_at, finished_at, pool, external_epoch
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
                 attempt = attempt + 1, version = version + 1, log = NULL, log_fence = NULL, log_gen = NULL,
                 session_id = NULL
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
    // One wall-clock read for gate and lease alike — see `claim_ready_filtered`.
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_exp = (now_dt + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339();

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
             log = NULL, log_fence = NULL, log_gen = NULL,
             version = version + 1,
             session_id = NULL
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
///
/// A sibling no worker holds is `cancelled` here, and its version bumped so
/// that nothing late can be written under an earlier claim. One a worker
/// holds is **asked to stop** (`stop_requested_at`) and stays `running`, with
/// its claim, until that worker says its work is gone
/// ([`settle_stopped_task`]): its engine sees the request at its next sweep.
/// Returns the siblings cancelled or asked.
pub async fn cancel_gang_siblings(pool: &Pool, member_id: &str) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    // With a whole lease to answer in, as in `stop_run_tasks`.
    let asked = sqlx::query(
        "UPDATE task_runs
         SET lease_expires_at = CASE WHEN stop_requested_at IS NULL THEN $3
                                     ELSE lease_expires_at END,
             stop_requested_at = COALESCE(stop_requested_at, $1),
             output = COALESCE(output, 'cancelled: gang member failed')
         WHERE gang_id IS NOT NULL
           AND gang_id = (SELECT gang_id FROM task_runs WHERE id = $2)
           AND id != $2
           AND status = 'running' AND claimed_by IS NOT NULL
         RETURNING run_id",
    )
    .bind(&now)
    .bind(member_id)
    .bind(stop_lease())
    .fetch_all(&mut *tx)
    .await?;
    let cancelled = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', claimed_by = NULL, lease_expires_at = NULL,
             version = version + 1, finished_at = $1,
             output = COALESCE(output, 'cancelled: gang member failed')
         WHERE gang_id IS NOT NULL
           AND gang_id = (SELECT gang_id FROM task_runs WHERE id = $2)
           AND id != $2
           AND (status IN ('pending', 'ready') OR (status = 'running' AND claimed_by IS NULL))
         RETURNING run_id",
    )
    .bind(&now)
    .bind(member_id)
    .fetch_all(&mut *tx)
    .await?;
    let mut seen = std::collections::HashSet::new();
    for row in asked.iter().chain(&cancelled) {
        let run_id: String = row.try_get("run_id")?;
        if seen.insert(run_id.clone()) {
            notify(&mut *tx, &run_id).await?;
        }
    }
    tx.commit().await?;
    Ok((asked.len() + cancelled.len()) as u64)
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
         WHERE id = $3 AND claimed_by = $4 AND version = $5 AND stop_requested_at IS NULL
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
        // Asked to stop, and over by itself first: cancelled, not succeeded.
        if settle_stopping(&mut tx, task_id, worker_id, fence, &now).await? {
            tx.commit().await?;
            tracing::info!(task_id, "the task ended after it was asked to stop: cancelled");
            return Ok(false);
        }
        tx.commit().await?;
        tracing::warn!(task_id, "stale completion ignored — task already reclaimed");
        return Ok(false);
    };
    let run_id: String = updated.try_get("run_id")?;

    // Decrement remaining_deps; dependents this statement does not promote
    // below are flipped by advance_ready_tasks.
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

    // (LOW_LATENCY B-2) Advance-on-mark: promote this task's default-shaped
    // dependents that just hit `remaining_deps = 0` straight to `ready`, in
    // the same transaction as the decrement above. The happy-path hop then
    // needs no separate advance step at all — mark → NOTIFY → claim. Same
    // predicate as the fast path in `advance_default_shaped` (all_success,
    // not an approval, no `when` key), and the NOT EXISTS re-checks *every*
    // dependency of the dependent, not just this one, so a dependent with a
    // separately-failed dependency is left for the skip half of the next
    // advance sweep. Success-mark only: a failure's dependents skip-cascade
    // through `advance_ready_tasks` on the next tick, off the hot path.
        // `fanout_of IS NULL` is not cosmetic here. This is the happy-path hop
        // that promotes a dependent the instant its producer succeeds — which
        // is *precisely* when a runtime fan-out barrier's dependencies become
        // satisfied. Without it the barrier would go `ready`, a worker would
        // claim it, and it would execute the command that is only ever the
        // template for its instances. It has to be refused in every
        // pending -> ready transition, not just the sweep's.
    sqlx::query(
        "UPDATE task_runs SET status = 'ready', scheduled_at = $1
         WHERE status = 'pending' AND remaining_deps = 0
           AND trigger_rule = 'all_success' AND is_approval = 0 AND fanout_of IS NULL
           AND (input IS NULL OR input NOT LIKE '%\"when\"%')
           AND id IN (
               SELECT dependent_id FROM task_dependencies WHERE dependency_id = $2
           )
           AND NOT EXISTS (
               SELECT 1 FROM task_dependencies d
               JOIN task_runs dep ON dep.id = d.dependency_id
               WHERE d.dependent_id = task_runs.id AND dep.status <> 'succeeded'
           )",
    )
    .bind(&now)
    .bind(task_id)
    .execute(&mut *tx)
    .await?;

    notify(&mut *tx, &run_id).await?;
    tx.commit().await?;
    Ok(true)
}

/// What the task `name` of the run `run_id` came to: its status, and the
/// end of what it printed (its last `tail` characters, which is all a reader
/// of its last line needs of an output that may be megabytes). `None` when
/// the run has no such task.
pub async fn task_outcome(
    pool: &Pool,
    run_id: &str,
    name: &str,
    tail: usize,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, right(output, $1) FROM task_runs WHERE run_id = $2 AND name = $3",
    )
    .bind(tail as i32)
    .bind(run_id)
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(status, output)| (status, output.unwrap_or_default())))
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
         WHERE id = $3 AND claimed_by = $4 AND version = $5 AND stop_requested_at IS NULL
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
        // Asked to stop, and over by itself first: cancelled, not failed.
        if settle_stopping(&mut tx, task_id, worker_id, fence, &now).await? {
            tx.commit().await?;
            tracing::info!(task_id, "the task ended after it was asked to stop: cancelled");
            return Ok(false);
        }
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

/// Add a failure reason to a running task's streamed log, so a reason the
/// engine writes to `output` is not hidden by the log the views prefer. Only
/// when a log exists: a task with none already shows `output`, and starting a
/// log with just the reason would hide the rest of it.
pub async fn note_task_log(pool: &Pool, task_id: &str, fence: i64, note: &str) -> Result<()> {
    sqlx::query(
        "UPDATE task_runs SET log = log || $1
         WHERE id = $2 AND version = $3 AND status = 'running' AND log IS NOT NULL",
    )
    .bind(format!("{}
", note.trim_end()))
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?;
    Ok(())
}

/// Append a live-output chunk to a still-running task so the API/UI can tail it
/// before the task exits (fast-win #17). Guarded by `version = fence AND status =
/// 'running'`: only the current attempt writes, and a terminal row is immutable
/// (a stale attempt's late chunk can't resurrect output). `reset` marks the first
/// chunk of an attempt — it replaces any prior-attempt output so a retried task's
/// tail starts clean; subsequent chunks append. The task's final output is
/// overwritten whole by `mark_task_*` at completion, so this is a live view only.
///
/// Returns the run of the row it changed, and `None` where the guard matched
/// nothing (a stale or late chunk). **It does not notify:** the caller tells
/// of the runs it appended to, with [`notify_runs`], once it has made its
/// appends. [`append_task_outputs`] is that caller, and what the engine
/// uses.
///
/// It used to notify in its own transaction, and that was what held a cell
/// with many tasks writing. Postgres keeps its notification queue in commit
/// order by one lock for the whole database, which a transaction that has
/// notified takes as it commits and holds until its commit is flushed. So
/// every append of every engine was committed one at a time, a flush each,
/// and whatever else notifies (a task's end, a run's making) waited in the
/// same queue: with enough tasks writing, for seconds. An append that does
/// not notify takes no such lock, and those made together are flushed
/// together.
pub async fn append_task_output(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    chunk: &str,
    reset: bool,
) -> Result<Option<String>> {
    let sql = if reset {
        "UPDATE task_runs SET log = $1, log_fence = $3, log_gen = COALESCE(log_gen, 0) + 1
         WHERE id = $2 AND version = $3 AND status = 'running' RETURNING run_id"
    } else {
        "UPDATE task_runs SET log = COALESCE(log, '') || $1, log_fence = $3
         WHERE id = $2 AND version = $3 AND status = 'running' RETURNING run_id"
    };
    let run_id = sqlx::query_scalar(sql)
        .bind(chunk)
        .bind(task_id)
        .bind(fence)
        .fetch_optional(pool)
        .await?;
    Ok(run_id)
}

/// What beginning an attempt's log again left behind ([`reset_task_log`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogReset {
    /// The run of the task, to tell of the change.
    pub run_id: String,
    /// Which writing of the attempt's log this is (`log_gen`): what its
    /// segments and open part are named by.
    pub gen: i64,
    /// A writing before this one was begun in the log store, and is recorded
    /// for the store's GC to remove (`log_sweeps`): the caller wakes it.
    pub swept: bool,
}

/// The database's clock as a record of `log_sweeps` keeps it: the text the
/// outbox's trigger writes (090), so that no engine's clock matters.
const SWEEP_NOW: &str =
    "to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')";

/// [`SWEEP_NOW`], `secs` on: a parameter's or a column's number of seconds.
fn sweep_clock_in(secs: &str) -> String {
    format!(
        "to_char((clock_timestamp() + make_interval(secs => CAST({secs} AS DOUBLE PRECISION))) \
         AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')"
    )
}

/// Begin a running attempt's log again from its start: its first chunk, or
/// its whole text once more (a seal). One transaction: the row's `log`
/// becomes `text` (the open part, where the store keeps it in the row, or
/// `''` for a `file://` store's notes), its `log_fence` the attempt's and
/// its `log_gen` one more, and the closed segments' rows of this attempt and
/// of every one before it go. So a reader sees the old log or the new one,
/// never the old segments before the new text. `None` where the attempt is
/// not running (it is over, or another took the row): nothing is changed.
///
/// The row counts the writings begun in the log store (`log_writings`), and
/// where one was begun before this, the task is recorded for the store's GC
/// (`log_sweeps`, due at once), in the same transaction: what the store keeps
/// of the writing before is no reader's now, and goes from there, not here.
pub async fn reset_task_log(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    text: &str,
) -> Result<Option<LogReset>> {
    let mut tx = pool.begin().await?;
    let begun: Option<(String, i64, i64)> = sqlx::query_as(
        "UPDATE task_runs SET log = $1, log_fence = $3, log_gen = COALESCE(log_gen, 0) + 1,
             log_writings = COALESCE(log_writings, 0) + 1
         WHERE id = $2 AND version = $3 AND status = 'running'
         RETURNING run_id, log_gen, log_writings",
    )
    .bind(text)
    .bind(task_id)
    .bind(fence)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((run_id, gen, writings)) = begun else {
        return Ok(None);
    };
    // The row says `version = fence`, and a log's fence is never past its
    // row's version: every attempt's segments are this one's or before it.
    sqlx::query("DELETE FROM log_segments WHERE task_id = $1 AND fence <= $2")
        .bind(task_id)
        .bind(fence)
        .execute(&mut *tx)
        .await?;
    let swept = writings > 1;
    if swept {
        sqlx::query(&format!(
            "INSERT INTO log_sweeps (task_id, run_id, reason, marked_at, due_at)
             VALUES ($1, $2, 'superseded', {SWEEP_NOW}, {SWEEP_NOW})
             ON CONFLICT (task_id) DO UPDATE SET reason = 'superseded', run_id = EXCLUDED.run_id,
                 marked_at = EXCLUDED.marked_at, due_at = EXCLUDED.due_at,
                 stamp = log_sweeps.stamp + 1, tries = 0, last_error = NULL"
        ))
        .bind(task_id)
        .bind(&run_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(Some(LogReset { run_id, gen, swept }))
}

/// Mark a task's row as one whose log the store holds a writing of, before
/// the engine writes there what it learnt of an attempt from the database
/// rather than began (`log_writings`, where a reset has not set it): a task
/// is recorded for the store's GC when its row goes only so. Nothing where it
/// is set, or where the row is gone.
pub async fn mark_log_kept(pool: &Pool, task_id: &str) -> Result<()> {
    sqlx::query("UPDATE task_runs SET log_writings = 1 WHERE id = $1 AND log_writings IS NULL")
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Take up to `limit` records of `log_sweeps` that are due, the earliest
/// first, for `lease_secs`: each is due again then, unless it is finished
/// before, and its stamp moves on, so that whoever held it before cannot
/// finish it. Records another sweeper holds are passed over.
pub async fn claim_log_sweeps(
    pool: &Pool,
    limit: i64,
    lease_secs: i64,
    first: super::ClaimFirst,
) -> Result<Vec<super::LogSweep>> {
    let which = match first {
        super::ClaimFirst::Superseded => "reason = 'superseded' AND ",
        super::ClaimFirst::Any => "",
    };
    // Taken once, as a materialized CTE: a subquery joined to the table it
    // locks can be run again for each row of a nested loop, and then one
    // claim takes more than `limit` and another none. The time is taken once
    // too, so that the index on `due_at` bounds what is read.
    let rows: Vec<(String, Option<String>, String, i64, i64)> = sqlx::query_as(&format!(
        "WITH due AS MATERIALIZED (
             SELECT task_id FROM log_sweeps WHERE {which}due_at <= (SELECT {SWEEP_NOW})
             ORDER BY due_at LIMIT $1 FOR UPDATE SKIP LOCKED
         )
         UPDATE log_sweeps s SET due_at = {}, stamp = s.stamp + 1
         FROM due WHERE s.task_id = due.task_id
         RETURNING s.task_id, s.run_id, s.reason, s.stamp, s.tries",
        sweep_clock_in("$2")
    ))
    .bind(limit)
    .bind(lease_secs)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(task_id, run_id, reason, stamp, tries)| super::LogSweep {
            task_id,
            run_id,
            reason,
            stamp,
            tries,
        })
        .collect())
}

/// The writing each of `tasks` that has a row is on ([`LOG_FENCE`],
/// [`LOG_GEN`]): what its readers name, and what the store's GC keeps.
pub async fn log_writings_of(pool: &Pool, tasks: &[String]) -> Result<Vec<(String, i64, i64)>> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_as(&format!(
        "SELECT id, {LOG_FENCE}, {LOG_GEN} FROM task_runs WHERE id = ANY($1)"
    ))
    .bind(tasks)
    .fetch_all(pool)
    .await?)
}

/// Finish claimed records of `log_sweeps`, each by `(task, stamp)` as its
/// claim left it: `retire`'s go (a removal found nothing left), and
/// `relook`'s are due again in `relook_secs` (a removal took something, and
/// a writer that has not stopped may make more). A record whose stamp moved
/// on since (the task was recorded again, or another sweeper claimed it) is
/// left as it is.
///
/// One record a statement: a statement that held one record and waited for
/// another could close a cycle with a purge, whose trigger records the
/// tasks it deletes in an order of its own, in one transaction.
pub async fn finish_log_sweeps(
    pool: &Pool,
    retire: &[(String, i64)],
    relook: &[(String, i64)],
    relook_secs: i64,
) -> Result<()> {
    for (task_id, stamp) in retire {
        sqlx::query("DELETE FROM log_sweeps WHERE task_id = $1 AND stamp = $2")
            .bind(task_id)
            .bind(stamp)
            .execute(pool)
            .await?;
    }
    let relooked = format!(
        "UPDATE log_sweeps SET due_at = {}, stamp = stamp + 1, tries = 0, last_error = NULL
         WHERE task_id = $1 AND stamp = $2",
        sweep_clock_in("$3")
    );
    for (task_id, stamp) in relook {
        sqlx::query(&relooked)
            .bind(task_id)
            .bind(stamp)
            .bind(relook_secs)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// A claimed record whose removal failed: due again in `after_secs`, with
/// one more try and the error, where its stamp is still `stamp`. Whether it
/// was.
pub async fn defer_log_sweep(
    pool: &Pool,
    task_id: &str,
    stamp: i64,
    after_secs: i64,
    error: &str,
) -> Result<bool> {
    let done = sqlx::query(&format!(
        "UPDATE log_sweeps SET due_at = {}, stamp = stamp + 1, tries = tries + 1, last_error = $4
         WHERE task_id = $1 AND stamp = $2",
        sweep_clock_in("$3")
    ))
    .bind(task_id)
    .bind(stamp)
    .bind(after_secs)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// What waits in `log_sweeps` ([`super::LogSweepsWaiting`]).
pub async fn log_sweeps_waiting(pool: &Pool) -> Result<super::LogSweepsWaiting> {
    let (waiting, failing, oldest_marked_at): (i64, i64, Option<String>) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE tries > 0), MIN(marked_at) FROM log_sweeps",
    )
    .fetch_one(pool)
    .await?;
    Ok(super::LogSweepsWaiting {
        waiting,
        failing,
        oldest_marked_at,
    })
}

/// A closed segment of a task attempt's log: its object's name in the log
/// store, in the attempt's order.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct LogSegmentRow {
    pub task_id: String,
    pub fence: i64,
    pub seq: i64,
    pub object_key: String,
}

/// The closed log segments of `tasks`, each of the attempt its log is of
/// alone ([`LOG_FENCE`]), in order: what a reader puts before the open part.
pub async fn log_segments_of(pool: &Pool, tasks: &[String]) -> Result<Vec<LogSegmentRow>> {
    let mut out = Vec::new();
    for chunk in tasks.chunks(500) {
        let marks = (1..=chunk.len())
            .map(|n| format!("${n}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT s.task_id, s.fence, s.seq, s.object_key FROM log_segments s \
             JOIN task_runs t ON t.id = s.task_id \
             AND s.fence = (CASE WHEN t.log IS NULL THEN t.version ELSE COALESCE(t.log_fence, t.version) END) \
             WHERE s.task_id IN ({marks}) ORDER BY s.task_id, s.seq"
        );
        let mut q = sqlx::query_as::<_, LogSegmentRow>(&sql);
        for task in chunk {
            q = q.bind(task);
        }
        out.extend(q.fetch_all(pool).await?);
    }
    Ok(out)
}

/// Where a task attempt's log stands in the store: the next segment's number,
/// how many bytes its task row's `log` holds (the open part, where the store
/// keeps it there), and which writing of the log the row's is (`log_gen`).
/// What the engine reads of an attempt it was not writing when it started.
/// `None` where the task has no row: nothing of it is to be written.
pub async fn log_open_state(
    pool: &Pool,
    task_id: &str,
    fence: i64,
) -> Result<Option<(i64, i64, i64)>> {
    Ok(sqlx::query_as(
        "SELECT COALESCE((SELECT MAX(seq) + 1 FROM log_segments WHERE task_id = $1 AND fence = $2), 0), \
                CAST(COALESCE(octet_length(log), 0) AS BIGINT), \
                CAST(COALESCE(log_gen, 0) AS BIGINT) \
         FROM task_runs WHERE id = $1",
    )
    .bind(task_id)
    .bind(fence)
    .fetch_optional(pool)
    .await?)
}

/// A task attempt's row `log`: the open part of a store that keeps it there.
pub async fn read_row_log(pool: &Pool, task_id: &str, fence: i64) -> Result<Option<String>> {
    let log: Option<Option<String>> =
        sqlx::query_scalar("SELECT log FROM task_runs WHERE id = $1 AND version = $2")
            .bind(task_id)
            .bind(fence)
            .fetch_optional(pool)
            .await?;
    Ok(log.flatten())
}

/// Record a task attempt's closed segments `pieces` (each its number, its
/// object's name, its bytes and its lines), all in one transaction, or none.
/// Where their text was the row's open part (`from_row`: how many characters
/// of the row's `log` they took), the characters leave the row in the same
/// transaction, so that a reader sees each line once: as segment, or as open
/// part. What was appended after the text was read stays. A segment recorded
/// already is not recorded again.
pub async fn close_log_segments(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    pieces: &[(i64, String, i64, i64)],
    from_row: Option<i64>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    for (seq, object_key, bytes, lines) in pieces {
        sqlx::query(
            "INSERT INTO log_segments (task_id, fence, seq, object_key, bytes, lines, closed_at) \
             VALUES ($1, $2, $3, $4, $5, $6, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')) ON CONFLICT DO NOTHING",
        )
        .bind(task_id)
        .bind(fence)
        .bind(seq)
        .bind(object_key)
        .bind(bytes)
        .bind(lines)
        .execute(&mut *tx)
        .await?;
    }
    if let Some(chars) = from_row {
        sqlx::query(
            "UPDATE task_runs SET log = SUBSTR(log, CAST($3 AS INTEGER) + 1) WHERE id = $1 AND version = $2",
        )
        .bind(task_id)
        .bind(fence)
        .bind(chars)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// The number of closed log segments of the attempt a task row's log is of
/// ([`LOG_FENCE`]), as a column of a query of `task_runs`: read with the
/// row, in one statement, so that a reader's segments and the row's open
/// part are of one moment.
const SEGMENTS_COLUMN: &str = "(SELECT COUNT(*) FROM log_segments s \
     WHERE s.task_id = task_runs.id AND s.fence = (CASE WHEN task_runs.log IS NULL THEN task_runs.version \
     ELSE COALESCE(task_runs.log_fence, task_runs.version) END))";

/// A task row as a reader of logs wants it: its id, the attempt its log is of
/// ([`LOG_FENCE`]) and which writing of it ([`LOG_GEN`]), its `log`, its
/// `output`, and how many closed segments that writing has.
pub type TaskLogRow = (String, i64, i64, Option<String>, Option<String>, i64);

/// Each task row of a run as a reader of logs wants it ([`TaskLogRow`]).
pub async fn task_log_rows(pool: &Pool, run_id: &str) -> Result<Vec<TaskLogRow>> {
    Ok(sqlx::query_as(&format!(
        "SELECT id, {LOG_FENCE}, {LOG_GEN}, log, output, {SEGMENTS_COLUMN} FROM task_runs WHERE run_id = $1"
    ))
    .bind(run_id)
    .fetch_all(pool)
    .await?)
}

/// One task row of a run as a reader of logs wants it ([`TaskLogRow`]).
pub async fn task_log_row(pool: &Pool, run_id: &str, task_id: &str) -> Result<Option<TaskLogRow>> {
    Ok(sqlx::query_as(&format!(
        "SELECT id, {LOG_FENCE}, {LOG_GEN}, log, output, {SEGMENTS_COLUMN} FROM task_runs \
         WHERE run_id = $1 AND id = $2"
    ))
    .bind(run_id)
    .bind(task_id)
    .fetch_optional(pool)
    .await?)
}

/// The attempt a task row's log is of, as a column of a query of
/// `task_runs`: the fence it was begun under, which a reset keeps in
/// `log_fence`, or, with no log yet, the attempt the row is on. Not
/// `version` alone: a teardown's claim or a gang sibling's cancel moves
/// `version` on and leaves the log the one it was.
pub const LOG_FENCE: &str =
    "(CASE WHEN log IS NULL THEN version ELSE COALESCE(log_fence, version) END)";

/// Which writing of its attempt's log a task row's `log` is, as a column:
/// one more at every reset (a first chunk, a seal). A reader that follows a
/// log by its bytes knows by it that the bytes before its position are no
/// longer the ones it read.
pub const LOG_GEN: &str = "COALESCE(log_gen, 0)";

/// A task of a run the live relay follows, as it reads them each pass:
/// where its log stands, without its text.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct LogTail {
    pub run_id: String,
    pub task_id: String,
    pub name: String,
    pub status: String,
    /// The task's attempts so far, counted through its life, as the log views
    /// count them.
    pub attempt: i64,
    /// The attempt its log is of ([`LOG_FENCE`]), and which writing of it
    /// ([`LOG_GEN`]).
    pub fence: i64,
    pub gen: i64,
    /// How many closed segments the attempt has, and their bytes.
    pub segments: i64,
    pub closed: i64,
    /// The bytes of the row's `log`; `None` where it has none.
    pub row_bytes: Option<i64>,
    /// The bytes of the row's `output`: what a view shows of a task that kept
    /// no log.
    pub output_bytes: i64,
}

const LOG_TAILS_SELECT: &str = "SELECT t.run_id, t.id AS task_id, t.name, t.status, t.attempt, t.fence, t.gen,
        (SELECT COUNT(*) FROM log_segments s WHERE s.task_id = t.id AND s.fence = t.fence) AS segments,
        CAST((SELECT COALESCE(SUM(s.bytes), 0) FROM log_segments s
              WHERE s.task_id = t.id AND s.fence = t.fence) AS BIGINT) AS closed,
        CAST(octet_length(t.log) AS BIGINT) AS row_bytes,
        CAST(COALESCE(octet_length(t.output), 0) AS BIGINT) AS output_bytes
     FROM (SELECT run_id, id, name, status, attempt_base + attempt AS attempt, scheduled_at, log, output,
                  (CASE WHEN log IS NULL THEN version ELSE COALESCE(log_fence, version) END) AS fence,
                  COALESCE(log_gen, 0) AS gen
           FROM task_runs";

const LOG_TAILS_ORDER: &str = "ORDER BY t.run_id, t.scheduled_at IS NULL, t.scheduled_at, t.name";

/// Every task of `runs` as the live relay reads it ([`LogTail`]), in each
/// run's order of execution, in one statement: its segments and its row of
/// one moment.
pub async fn log_tails(pool: &Pool, runs: &[String]) -> Result<Vec<LogTail>> {
    Ok(sqlx::query_as::<_, LogTail>(&format!(
        "{LOG_TAILS_SELECT} WHERE run_id = ANY($1)) t {LOG_TAILS_ORDER}"
    ))
    .bind(runs)
    .fetch_all(pool)
    .await?)
}

/// One task as the live relay reads it ([`LogTail`]): what a stream reads
/// before it reads that task's log itself.
pub async fn log_tail_of(pool: &Pool, task_id: &str) -> Result<Option<LogTail>> {
    Ok(
        sqlx::query_as::<_, LogTail>(&format!("{LOG_TAILS_SELECT} WHERE id = $1) t"))
            .bind(task_id)
            .fetch_optional(pool)
            .await?,
    )
}

/// A read of a task row's `log` from a byte position of its attempt's whole
/// log ([`log_row_bytes`]): the attempt and writing it is of, the position,
/// how many bytes of the log are before the row's beside the closed segments
/// (a `file://` store's open part, 0 elsewhere), and at most how many to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRead {
    pub task_id: String,
    pub fence: i64,
    pub gen: i64,
    pub pos: i64,
    pub base: i64,
    pub max: i64,
}

/// What [`log_row_bytes`] read of a row: the closed segments' bytes as the
/// read found them, the row's `log` in bytes, and the bytes read, which begin
/// at `pos`, or at the row's start where `pos` is before it (a segment closed
/// since the position was read: the bytes before the row are the segments').
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct RowBytes {
    pub task_id: String,
    pub closed: i64,
    pub row_bytes: i64,
    pub bytes: Vec<u8>,
}

/// Read task rows' `log` from byte positions of their attempts' logs (the
/// live relay). A row is read only while its log is still the
/// attempt and writing `reads` name; one that is not is left out, its text
/// gone or begun again. The closed segments are summed in the statement that
/// reads the row, so that a segment closed since the caller looked is seen.
pub async fn log_row_bytes(pool: &Pool, reads: &[RowRead]) -> Result<Vec<RowBytes>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let col = |f: fn(&RowRead) -> i64| reads.iter().map(f).collect::<Vec<i64>>();
    let ids: Vec<&str> = reads.iter().map(|r| r.task_id.as_str()).collect();
    Ok(sqlx::query_as::<_, RowBytes>(
        "SELECT w.task_id, c.closed, CAST(octet_length(t.log) AS BIGINT) AS row_bytes,
                substring(convert_to(t.log, 'UTF8')
                          FROM CAST(GREATEST(w.pos - c.closed - w.base, 0) + 1 AS INTEGER)
                          FOR CAST(w.max AS INTEGER)) AS bytes
         FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[])
              AS w(task_id, fence, gen, pos, base, max)
         JOIN task_runs t ON t.id = w.task_id
         CROSS JOIN LATERAL (SELECT CAST(COALESCE(SUM(s.bytes), 0) AS BIGINT) AS closed
                             FROM log_segments s WHERE s.task_id = w.task_id AND s.fence = w.fence) c
         WHERE t.log IS NOT NULL AND COALESCE(t.log_fence, t.version) = w.fence
           AND COALESCE(t.log_gen, 0) = w.gen",
    )
    .bind(ids)
    .bind(col(|r| r.fence))
    .bind(col(|r| r.gen))
    .bind(col(|r| r.pos))
    .bind(col(|r| r.base))
    .bind(col(|r| r.max))
    .fetch_all(pool)
    .await?)
}

/// At most `max` bytes of a task row's `output` from byte `from`: what stands
/// in for the log of a task that kept none. `None` where it has none.
pub async fn log_output_bytes(
    pool: &Pool,
    task_id: &str,
    from: i64,
    max: i64,
) -> Result<Option<Vec<u8>>> {
    let bytes: Option<Option<Vec<u8>>> = sqlx::query_scalar("SELECT substring(convert_to(output, 'UTF8') FROM CAST($2 + 1 AS INTEGER) FOR CAST($3 AS INTEGER))
         FROM task_runs WHERE id = $1 AND output IS NOT NULL")
        .bind(task_id)
        .bind(from)
        .bind(max)
        .fetch_optional(pool)
        .await?;
    Ok(bytes.flatten())
}

/// The closed segments of a task attempt's log, in order: each its number and
/// its bytes, which put together say where each begins.
pub async fn log_segment_spans(pool: &Pool, task_id: &str, fence: i64) -> Result<Vec<(i64, i64)>> {
    Ok(sqlx::query_as(
        "SELECT seq, bytes FROM log_segments WHERE task_id = $1 AND fence = $2 ORDER BY seq",
    )
    .bind(task_id)
    .bind(fence)
    .fetch_all(pool)
    .await?)
}

/// Tell the subscribers of `runs` that something of theirs changed: one
/// notification a run, in one statement, so one commit and one turn at the
/// notification queue's lock however many runs there are. What a batch of
/// log appends calls once it is made, so that a browser's tail of any of
/// them updates promptly. Nothing is sent for no runs.
///
/// Not in the transaction of what changed, and after it: a subscriber that
/// reads on being told finds the change there. Told and then lost with the
/// engine is a tail that updates at the next append instead.
pub async fn notify_runs(pool: &Pool, runs: &[String]) -> Result<()> {
    if runs.is_empty() {
        return Ok(());
    }
    sqlx::query("SELECT pg_notify($1, run) FROM unnest($2::text[]) AS run")
        .bind(EVENT_CHANNEL)
        .bind(runs)
        .execute(pool)
        .await?;
    Ok(())
}

/// One append of a batch: a task's attempt, and what it wrote since the last.
pub struct LogAppend {
    pub task_id: String,
    /// The attempt's fence: an append of an attempt that is over reaches
    /// nothing.
    pub fence: i64,
    pub text: String,
    /// The row's log is replaced, not added to (see [`append_task_output`]).
    pub reset: bool,
}

/// The most log appends in flight at once, whatever the pool would take.
const LOG_APPEND_LANES: usize = 4;

/// How many log appends may be in flight at once on `pool`: up to four, and
/// never more than half of its connections, so that what else the process
/// does with the pool (its workers renew their leases through it) is not left
/// waiting on a log. A pool of the smallest size has them one at a time.
///
/// Each append is a row of its own and a transaction of its own, and none
/// notifies (see [`append_task_output`]), so several at once hold no lock
/// together, and the database commits those that end together with one
/// flush.
pub fn log_append_lanes(pool: &Pool) -> usize {
    (pool.options().get_max_connections() as usize / 2).clamp(1, LOG_APPEND_LANES)
}

/// Make a batch of log appends, as many at once as the pool takes
/// ([`log_append_lanes`]), and then tell of the runs they were made to, each
/// once, in one statement ([`notify_runs`]). What the reconcile loop calls
/// with the chunks that have waited their second.
///
/// Best-effort, as a log's tail is: an append that fails is logged and its
/// text is gone, and so is a telling that fails, which a tail makes up for at
/// the next append. Returns how many appends were made, those that reached
/// no row among them.
///
/// No two of `appends` may be to one task: they would be made in no order.
pub async fn append_task_outputs(pool: &Pool, appends: Vec<LogAppend>) -> usize {
    append_task_outputs_in(pool, appends, log_append_lanes(pool)).await
}

/// [`append_task_outputs`], with `lanes` appends in flight at once, four at
/// most. Which lane an append falls in decides nothing.
pub async fn append_task_outputs_in(pool: &Pool, appends: Vec<LogAppend>, lanes: usize) -> usize {
    let lanes = lanes.clamp(1, LOG_APPEND_LANES);
    let mut lane: [Vec<LogAppend>; LOG_APPEND_LANES] = Default::default();
    for (n, append) in appends.into_iter().enumerate() {
        lane[n % lanes].push(append);
    }
    let [a, b, c, d] = lane;
    let made = tokio::join!(
        append_each(pool, a),
        append_each(pool, b),
        append_each(pool, c),
        append_each(pool, d),
    );
    let mut written = 0;
    let mut runs = Vec::new();
    for (lane_written, lane_runs) in [made.0, made.1, made.2, made.3] {
        written += lane_written;
        runs.extend(lane_runs);
    }
    // The runs that were appended to are told of once, each once.
    runs.sort_unstable();
    runs.dedup();
    if let Err(e) = notify_runs(pool, &runs).await {
        tracing::warn!(error = %e, runs = runs.len(), "telling of live-log appends failed");
    }
    written
}

/// One lane's appends, one after another: how many were made, and the runs
/// of the rows they changed.
async fn append_each(pool: &Pool, lane: Vec<LogAppend>) -> (usize, Vec<String>) {
    let mut written = 0;
    let mut runs = Vec::new();
    for append in lane {
        match append_task_output(
            pool,
            &append.task_id,
            append.fence,
            &append.text,
            append.reset,
        )
        .await
        {
            Ok(run) => {
                written += 1;
                runs.extend(run);
            }
            Err(e) => {
                tracing::warn!(task_id = %append.task_id, error = %e, "live-log append failed")
            }
        }
    }
    (written, runs)
}

// ── Runtime fan-out (`with_output_of:`) ──────────────────────────────────────

/// Statuses meaning an instance will not change again.
const FANOUT_TERMINAL: [&str; 4] = ["succeeded", "failed", "skipped", "cancelled"];

/// The fan-out sweep's read of the parked barriers ([`reconcile_fanouts`]),
/// for its index (migrations_pg/101): those with no instance of their
/// generation still to end, a batch at a time. A barrier whose instances
/// still run is the common case, and the sweep has nothing to do for it.
///
/// Exact: the expansion stamps each instance with the version its CAS wrote
/// (`RETURNING version`), and an instance of an older generation is one the
/// sweep drops (`fanout_epoch < version`), so a barrier left out here is one
/// the sweep would read and pass over. The statuses are [`FANOUT_TERMINAL`].
pub(crate) const PARKED_FANOUTS: &str =
    "SELECT b.id, b.run_id, b.input, b.allow_failure, b.version,
                    b.runner_class, b.priority, b.pool
             FROM task_runs b
             WHERE b.status = 'running' AND b.fanout_of IS NOT NULL AND b.claimed_by IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM task_runs i
                   WHERE i.fanout_parent = b.id AND i.fanout_epoch >= b.version
                     AND i.status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')
               )
             LIMIT $1";

/// Resolve parked runtime fan-out barriers — the sweep that makes
/// `with_output_of:` work.
///
/// A barrier is a task whose instance count could not be known when the run was
/// created, because it is the *result* of an upstream task. It is created as
/// one row, parks when its dependencies are satisfied (`status = 'running'`,
/// claim and lease NULL — the same shape as every other park), and is resolved
/// here in two phases:
///
/// 1. **Expand.** No instances yet: read the producer's output, parse a JSON
///    array, insert one `ready` row per element with `{{ item }}` substituted,
///    and wire the barrier to depend on each. The barrier stays parked.
/// 2. **Join.** Instances exist: once all of them are terminal, the barrier
///    resolves with them and its dependents advance.
///
/// The barrier deliberately survives expansion as the **join point**. The
/// alternative is re-parenting every dependent onto the new instances, which
/// means rewriting dependency edges underneath a scheduler that is concurrently
/// reading them; keeping the barrier means dependents were already wired to the
/// thing they should wait for, `trigger_rule` and `allow_failure` keep working
/// unchanged, and the only edges this writes are new ones.
///
/// Mirrors the SQLite backend statement for statement; see there for the full
/// reasoning, including why the barrier survives expansion as the join point
/// rather than the dependents being re-parented onto the instances.
///
/// Idempotent and HA-safe. Phase 1's insert is one transaction gated on a CAS
/// of the barrier's `version`, so two schedulers sweeping at once cannot both
/// expand it; phase 2's resolve is guarded on the parked shape, so a
/// double-resolve is a no-op. A crash mid-expansion commits nothing and the
/// barrier is simply expanded again next tick.
///
/// Reads up to [`super::SWEEP_BATCH`] barriers a pass, and says when it read
/// that many.
pub async fn reconcile_fanouts(
    pool: &Pool,
) -> Result<super::Pass<(String, crate::models::FanoutOutcome)>> {
    let parked: Vec<ParkedFanout> = sqlx::query_as(PARKED_FANOUTS)
        .bind(super::SWEEP_BATCH)
        .fetch_all(pool)
        .await?;
    let full = parked.len() as i64 >= super::SWEEP_BATCH;

    let mut out = Vec::new();
    for (task_id, run_id, input, allow_failure, version, runner_class, priority, pool_name) in
        parked
    {
        // Instances from a PREVIOUS generation of this barrier, dropped before
        // anything else looks at them.
        //
        // A barrier's instances are its dependencies (`dependent_id = barrier`),
        // so no reset path's downstream cone reaches them: clearing a barrier —
        // or its producer — leaves them terminal and attached. Without this the
        // join branch below would resolve the re-run against the previous
        // attempt's results and never re-read the producer, so clearing a
        // producer and re-running would process the old partitions.
        //
        // Strictly `<`, never `<>`: a concurrent scheduler that won the CAS
        // first has already stamped instances with a HIGHER epoch than this
        // stale read, and `<>` would delete the rows it just created. `version`
        // only increases, so older is the only kind of stale there is.
        // Edges first — `task_dependencies` references `task_runs` with no
        // cascade, so the reverse order is a foreign-key violation.
        sqlx::query(
            "DELETE FROM task_dependencies
             WHERE dependent_id = $1
               AND dependency_id IN (
                   SELECT id FROM task_runs
                   WHERE fanout_parent = $2 AND fanout_epoch < $3
               )",
        )
        .bind(&task_id)
        .bind(&task_id)
        .bind(version)
        .execute(pool)
        .await?;
        sqlx::query("DELETE FROM task_runs WHERE fanout_parent = $1 AND fanout_epoch < $2")
            .bind(&task_id)
            .bind(version)
            .execute(pool)
            .await?;

        let instances_present: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = $1")
                .bind(&task_id)
                .fetch_one(pool)
                .await?;

        // ── phase 2: join ───────────────────────────────────────────────────
        if instances_present > 0 {
            let statuses: Vec<String> =
                sqlx::query_scalar("SELECT status FROM task_runs WHERE fanout_parent = $1")
                    .bind(&task_id)
                    .fetch_all(pool)
                    .await?;
            if statuses.iter().any(|s| !FANOUT_TERMINAL.contains(&s.as_str())) {
                continue; // still running — the common tick
            }
            let failed = statuses.iter().filter(|s| *s == "failed" || *s == "cancelled").count();
            let succeeded = failed == 0 || allow_failure != 0;
            let summary = fanout_summary(&statuses);
            if finish_fanout(pool, &task_id, succeeded, &summary).await? {
                out.push((
                    task_id,
                    crate::models::FanoutOutcome::Joined {
                        succeeded,
                        instances: statuses.len(),
                    },
                ));
            }
            continue;
        }

        // ── phase 1: expand ─────────────────────────────────────────────────
        let Some(spec) = input
            .as_deref()
            .and_then(|j| serde_json::from_str::<crate::dag::TaskSpec>(j).ok())
        else {
            let why = "the task's stored spec could not be read".to_string();
            if finish_fanout(pool, &task_id, false, &why).await? {
                out.push((task_id, crate::models::FanoutOutcome::Failed { reason: why }));
            }
            continue;
        };
        let producer = spec.with_output_of.clone().unwrap_or_default();

        // Which rows hold the producer's output. Taken from this task's own
        // expanded `depends_on`, which is what makes a producer that is ITSELF
        // fanned out work: expansion replaced `regions` with `regions.0`,
        // `regions.1`, … and rewired this task onto exactly those rows, so the
        // dependency list already names them and nothing has to be matched
        // against the rest of the run. See `expand::fanout_producer_rows`.
        let want = crate::expand::fanout_producer_rows(&producer, &spec.depends_on);
        if want.is_empty() {
            let why = format!(
                "with_output_of names '{producer}', which is not among this task's \
                 dependencies — nothing to read"
            );
            if finish_fanout(pool, &task_id, false, &why).await? {
                out.push((task_id, crate::models::FanoutOutcome::Failed { reason: why }));
            }
            continue;
        }

        // Read through the dependency EDGES rather than by name: one query
        // whatever the producer's width, no bind limit to chunk around, and a
        // row this task does not depend on cannot be reached even in
        // principle. Ordering comes from `want` below, not from the query.
        let dep_rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT t.name, t.output
             FROM task_runs t
             JOIN task_dependencies d ON d.dependency_id = t.id
             WHERE d.dependent_id = $1",
        )
        .bind(&task_id)
        .fetch_all(pool)
        .await?;
        let by_name: std::collections::HashMap<&str, &str> = dep_rows
            .iter()
            .map(|(n, o)| (n.as_str(), o.as_deref().unwrap_or("")))
            .collect();
        // `depends_on` order, which is instance order — so a chained fan-out
        // concatenates `regions.0`'s items before `regions.1`'s rather than in
        // whatever order a string sort would give.
        let producers: Vec<(&str, &str)> =
            want.iter().map(|n| (*n, *by_name.get(n).unwrap_or(&""))).collect();

        let instances =
            match crate::expand::fanout_instances(&spec, &producers) {
                Ok(v) => v,
                Err(e) => {
                    let why = e.to_string();
                    if finish_fanout(pool, &task_id, false, &why).await? {
                        out.push((task_id, crate::models::FanoutOutcome::Failed { reason: why }));
                    }
                    continue;
                }
            };

        // The admission decision this feature owes the rest of the system. A
        // run's task count is otherwise fixed before the run exists, which is
        // what lets `budget:` refuse a fan-out blow-up at submit; a runtime
        // fan-out breaks that invariant, so the ceiling is re-checked here —
        // and exceeding it fails the task with the same typed message
        // expansion uses, rather than inserting the rows and finding out.
        let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = $1")
            .bind(&run_id)
            .fetch_one(pool)
            .await?;
        let ceiling = crate::expand::max_tasks_per_run() as i64;
        if existing + instances.len() as i64 > ceiling {
            let why = format!(
                "runtime fan-out over '{producer}' would add {} tasks to a run that already has \
                 {existing}, exceeding {ceiling} (DAGRON_MAX_TASKS_PER_RUN)",
                instances.len()
            );
            if finish_fanout(pool, &task_id, false, &why).await? {
                out.push((task_id, crate::models::FanoutOutcome::Failed { reason: why }));
            }
            continue;
        }

        // An empty list is a result, not a mistake (see `fanout_instances`):
        // the barrier succeeds with nothing under it and its dependents
        // advance, which is what "there were no partitions" should mean.
        if instances.is_empty() {
            if finish_fanout(pool, &task_id, true, "[]").await? {
                out.push((
                    task_id,
                    crate::models::FanoutOutcome::Joined { succeeded: true, instances: 0 },
                ));
            }
            continue;
        }

        let now = chrono::Utc::now().to_rfc3339();
        let mut tx = pool.begin().await?;
        // Winner-take-all: the same optimistic CAS the claim path uses. Two
        // schedulers sweeping the same tick both read zero instances, and
        // without this both would insert a full set.
        // `RETURNING version` rather than `version + 1` computed here: the
        // number the instances are stamped with has to be the one this
        // statement actually wrote, or a stamp and a barrier could disagree
        // about which generation the rows belong to.
        let epoch: Option<(i64,)> = sqlx::query_as(
            "UPDATE task_runs SET version = version + 1
             WHERE id = $1 AND version = $2 AND status = 'running' AND claimed_by IS NULL
             RETURNING version",
        )
        .bind(&task_id)
        .bind(version)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((epoch,)) = epoch else {
            tx.rollback().await?;
            continue; // another scheduler is expanding this barrier
        };

        for inst in &instances {
            let instance_id = Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO task_runs
                     (id, run_id, name, status, remaining_deps, input, scheduled_at,
                      trigger_rule, allow_failure, is_approval, runner_class, priority,
                      pool, fanout_parent, fanout_epoch)
                 VALUES ($1, $2, $3, 'ready', 0, $4, $5, 'all_success', $6, 0, $7, $8, $9, $10, $11)",
            )
            .bind(&instance_id)
            .bind(&run_id)
            .bind(&inst.name)
            .bind(serde_json::to_string(&inst.spec)?)
            // `ready` with `scheduled_at` now: the dependencies that gate this
            // fan-out have already succeeded (that is what un-parked the
            // barrier), so an instance has nothing left to wait for.
            .bind(&now)
            .bind(allow_failure)
            // Claim inputs come from the barrier's row, not its spec: the row's
            // `runner_class` was already resolved against the DAG default at
            // creation, and re-deriving it here is how the two come to differ.
            .bind(&runner_class)
            .bind(priority)
            .bind(pool_name.as_deref())
            .bind(&task_id)
            // The generation this instance belongs to. A later reset bumps the
            // barrier's version past it, which is how the sweep above knows to
            // throw this row away rather than join against it.
            .bind(epoch)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES ($1, $2)",
            )
            .bind(&task_id)
            .bind(&instance_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        out.push((
            task_id,
            crate::models::FanoutOutcome::Expanded { instances: instances.len() },
        ));
    }
    Ok(super::Pass { done: out, full })
}

/// A one-line account of how the instances went, stored as the barrier's
/// output.
///
/// Deliberately a summary rather than the instances' concatenated output. The
/// barrier is one `task_runs.output` column, that column is uncapped, and
/// collecting N unbounded outputs into it is the same unbounded multiplication
/// `attempt_log` exists to avoid — the per-instance output is already readable
/// on each instance's own row.
fn fanout_summary(statuses: &[String]) -> String {
    let count = |want: &str| statuses.iter().filter(|s| s.as_str() == want).count();
    serde_json::json!({
        "instances": statuses.len(),
        "succeeded": count("succeeded"),
        "failed": count("failed") + count("cancelled"),
        "skipped": count("skipped"),
    })
    .to_string()
}

/// Resolve a parked barrier and advance its dependents, in one transaction.
///
/// Guarded on the parked shape (`running` + `fanout_of` + no claim), so a
/// second scheduler resolving the same barrier is a no-op and the dependent
/// decrement runs exactly once — the same winner-take-all contract
/// `resolve_approval` carries. Returns whether this call won.
async fn finish_fanout(pool: &Pool, task_id: &str, succeeded: bool, output: &str) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2, output = $3
         WHERE id = $4 AND status = 'running' AND fanout_of IS NOT NULL AND claimed_by IS NULL",
    )
    .bind(if succeeded { "succeeded" } else { "failed" })
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
    // Decremented on failure as well as success, exactly as `mark_task_failed`
    // does and for the same reason: `remaining_deps` counts dependencies that
    // are not yet *terminal*, not ones that succeeded. Gating this on success
    // leaves every dependent of a failed barrier pending forever — the advance
    // sweep only looks at `remaining_deps = 0`, so it never gets to apply the
    // trigger rule that would skip them, and the run hangs rather than failing.
    // It is also what lets an `all_done` dependent run after a failed fan-out.
    sqlx::query(
        "UPDATE task_runs SET remaining_deps = remaining_deps - 1
         WHERE id IN (SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1)
           AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
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
    decided_by: Option<&str>,
    comment: Option<&str>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) =
        if approve { ("succeeded", "approved") } else { ("failed", "rejected") };
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2, output = $3,
                decided_by = $4, decision_comment = $5, log = $6
         WHERE id = $7 AND run_id = $8 AND status = 'awaiting_approval'",
    )
    .bind(status)
    .bind(&now)
    .bind(output)
    .bind(decided_by)
    .bind(comment)
    .bind(crate::dag::approval_log(approve, decided_by, comment, &now))
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
/// `reject`). Returns the `(task_id, approved, deadline)` decisions, the
/// deadline being when the gate timed out (RFC 3339). Idempotent via
/// `resolve_approval`'s guard. Not `ops`-gated — the reconcile loop calls this
/// every tick in every build.
pub async fn resolve_expired_approvals(pool: &Pool) -> Result<Vec<(String, bool, String)>> {
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
        let note = format!(
            "approval_timeout_secs ({timeout}s) elapsed; defaulted to {}",
            if approve { "approve" } else { "reject" }
        );
        if resolve_approval(pool, &run_id, &id, approve, Some("timeout"), Some(&note)).await? {
            resolved.push((id, approve, deadline.to_rfc3339()));
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
         WHERE id = $2 AND status = 'running' AND version = $3 AND stop_requested_at IS NULL",
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
         WHERE id = $1 AND status = 'running' AND version = $2 AND stop_requested_at IS NULL",
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
///
/// **The child run's own output becomes this task's output** when it has one,
/// falling back to the constant below when it does not. Without that a
/// sub-workflow is a procedure and not a function: `result_from` gives a run a
/// single return value, and the parent used to throw it away and write
/// "sub-workflow succeeded" instead, so `{{ tasks.<child>.output }}` in the
/// parent was a constant and nothing downstream could branch on what the child
/// decided.
///
/// It matters on the failure side too. A child that died on its
/// `run_timeout_secs` deadline records *why* on the run row; propagating it puts
/// that reason in the parent's failure summary instead of the word "failed".
///
/// It is also what any loop over a sub-workflow needs: a condition on the
/// trigger's output can only mean something once that output is the child's
/// verdict rather than a constant.
///
/// Guarded on the **specific** `child_run_id` the sweep observed, for the same
/// reason [`requeue_parked_subworkflow`] is. Before `repeat:` existed a trigger
/// parked exactly once, so `sub_run_id IS NOT NULL` was enough: a stale
/// duplicate sweep found the row already resolved and did nothing. A repeating
/// trigger re-parks, so under Postgres HA a stale sweep could resolve a trigger
/// that had already moved on — ending the loop early, releasing downstream work,
/// and orphaning the turn still running. The CAS makes that a no-op.
pub async fn resolve_subworkflow(
    pool: &Pool,
    task_id: &str,
    child_run_id: &str,
    succeeded: bool,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) = if succeeded {
        ("succeeded", "sub-workflow succeeded")
    } else {
        ("failed", "sub-workflow failed")
    };
    let mut tx = pool.begin().await?;
    // The child's output, or the constant when it has none. A correlated
    // subquery rather than a second round trip: the read and the write must see
    // the same row, and the guard below already makes this statement the one
    // place the parked shape is resolved.
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2,
                output = COALESCE(
                    (SELECT wr.output FROM workflow_runs wr WHERE wr.id = task_runs.sub_run_id),
                    $3)
         WHERE id = $4 AND status = 'running' AND sub_run_id = $5",
    )
    .bind(status)
    .bind(&now)
    .bind(output)
    .bind(task_id)
    .bind(child_run_id)
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

/// Re-arm a **parked** sub-workflow trigger for another iteration (`repeat:`).
///
/// The loop operator's existing re-queue, [`retry_task`], is fence-guarded on
/// `claimed_by = worker_id AND version = fence` — it re-arms a task whose worker
/// is handing it back. A parked trigger has `claimed_by = NULL` by construction:
/// `park_subworkflow` clears the lease precisely so lease recovery leaves it
/// alone while its child run works. There is no worker to authenticate, so this
/// guards on the **parked shape** instead, exactly as `resolve_subworkflow`
/// does, which makes it idempotent under a concurrent sweep on another
/// scheduler.
///
/// `sub_run_id` is cleared in the same statement, and that is the whole
/// correctness argument: it is what takes the row out of the parked set, so the
/// sweep cannot see this iteration twice and start two child runs for one turn.
///
/// `version` is bumped so any fence still held from the original claim — a
/// worker that dispatched the trigger and has not noticed — cannot act on the
/// re-armed row.
///
/// Deliberately does **not** touch `remaining_deps` on dependents. This is not a
/// completion: the trigger is going round again, and its dependents must stay
/// blocked until the loop is genuinely over. That is why the caller decides
/// *before* resolving rather than resolving and re-arming — resolving already
/// decrements them, and a loop that decremented once per turn would release
/// downstream work on the first turn.
pub async fn requeue_parked_subworkflow(
    pool: &Pool,
    task_id: &str,
    child_run_id: &str,
    output: Option<&str>,
    retry_at: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready',
             scheduled_at = $1,
             sub_run_id = NULL,
             claimed_by = NULL,
             lease_expires_at = NULL,
             finished_at = NULL,
             output = $2,
             version = version + 1
         WHERE id = $3 AND status = 'running' AND sub_run_id = $4",
    )
    .bind(retry_at)
    .bind(output)
    .bind(task_id)
    .bind(child_run_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Fail a **parked** sub-workflow trigger with `reason`, advancing its
/// dependents the way a normal failure does.
///
/// Used when `repeat:` runs out of iterations: the child run succeeded, so
/// [`resolve_subworkflow`] would record a success, but the *loop* failed and the
/// task must say so. Same dependent decrement as a resolution, so it is
/// interchangeable with one and equally idempotent — and, like
/// [`requeue_parked_subworkflow`], guarded on the **specific** `child_run_id` the
/// sweep observed (`sub_run_id = $4`). A stale sweep on another scheduler
/// therefore cannot fail a trigger that has already moved on to a newer
/// iteration; Postgres is the only backend where concurrent sweeps race.
pub async fn fail_parked_subworkflow(
    pool: &Pool,
    task_id: &str,
    child_run_id: &str,
    reason: &str,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = 'failed', finished_at = $1, output = $2,
                sub_run_id = NULL
         WHERE id = $3 AND status = 'running' AND sub_run_id = $4",
    )
    .bind(&now)
    .bind(reason)
    .bind(task_id)
    .bind(child_run_id)
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

/// The sub-workflow sweep's read of the parked triggers whose child run has
/// ended ([`reconcile_subworkflows`]), for its index (migrations_pg/105), a
/// batch at a time. A trigger whose child is gone is left parked, as a
/// trigger whose child still runs is: the join reads neither.
pub(crate) const PARKED_SUBWORKFLOWS: &str =
    "SELECT t.id, t.run_id, t.sub_run_id, c.status, c.output
         FROM task_runs t JOIN workflow_runs c ON c.id = t.sub_run_id
         WHERE t.status = 'running' AND t.sub_run_id IS NOT NULL
           AND c.status IN ('succeeded', 'failed', 'cancelled')
         LIMIT $1";

/// Sweep parked sub-workflow tasks (#23): for each `running` task holding a
/// `sub_run_id`, if its child run is terminal, decide what happens to the
/// trigger.
///
/// Without `repeat:` that decision is the old one: the trigger succeeds or fails
/// with its child. With `repeat:` the trigger is a **loop** and each child run is
/// one iteration — the condition is evaluated against the child's result and,
/// unless it holds, the trigger is re-armed to start the next child run.
///
/// **The decision is made before resolving, never after.** `resolve_subworkflow`
/// decrements dependents' `remaining_deps` as part of completing the task, so
/// resolving first and re-arming afterwards would release downstream work on the
/// first iteration and again on every one after it. Deciding first means a
/// continuing loop never touches a dependent at all.
///
/// Returns the `(task_id, succeeded)` resolutions — iterations that continued are
/// not resolutions and are not reported. Run each reconcile tick; idempotent via
/// the parked-shape guards, so every scheduler in an HA set may sweep.
///
/// Reads up to [`super::SWEEP_BATCH`] triggers a pass, each with its child's
/// status and output, and says when it read that many.
pub async fn reconcile_subworkflows(pool: &Pool) -> Result<super::Pass<(String, bool)>> {
    // `run_id` rides along so a re-arm can wake idle peers (see the `Again`
    // arm): moving the trigger to `ready` is a readiness change, and every other
    // one in this module fires `pg_notify` rather than making a scheduler wait
    // out the poll interval — which, per conversation turn, this loop would
    // otherwise pay N times.
    let parked: Vec<(String, String, String, String, Option<String>)> =
        sqlx::query_as(PARKED_SUBWORKFLOWS)
            .bind(super::SWEEP_BATCH)
            .fetch_all(pool)
            .await?;
    let full = parked.len() as i64 >= super::SWEEP_BATCH;

    let mut resolved = Vec::new();
    for (task_id, run_id, child_run_id, child_status, child_output) in parked {
        let succeeded = match child_status.as_str() {
            "succeeded" => true,
            "failed" | "cancelled" => false,
            _ => continue, // still running/pending
        };

        // A *successful* iteration is the only thing a loop condition gets to
        // judge. A failed child fails the trigger outright: `repeat` decides when
        // a loop is over, not whether a failure counts — retries are the task's
        // own `max_attempts`.
        if succeeded {
            match repeat_spec_of(pool, &task_id).await {
                Ok(Some(rep)) => {
                    // `attempt` is incremented on claim, so the stored value is the
                    // number of iterations that have run, this one included.
                    let iteration: i64 =
                        sqlx::query_scalar("SELECT attempt FROM task_runs WHERE id = $1")
                            .bind(&task_id)
                            .fetch_optional(pool)
                            .await?
                            .unwrap_or(1);
                    let output = child_output.clone().unwrap_or_default();
                    match rep.decide(&output, iteration) {
                        crate::dag::RepeatDecision::Done => {}
                        crate::dag::RepeatDecision::Again { delay_secs } => {
                            let retry_at = crate::dag::delayed_retry_at(delay_secs);
                            // The last turn's result is carried forward as the
                            // trigger's output, so a run inspected mid-loop shows
                            // where the conversation has got to, not a stale value.
                            if requeue_parked_subworkflow(
                                pool,
                                &task_id,
                                &child_run_id,
                                child_output.as_deref(),
                                &retry_at,
                            )
                            .await?
                            {
                                // Wake idle peers so the re-armed trigger is
                                // claimed at once rather than a poll interval
                                // later — the next turn should start now, not
                                // after a wait the conversation pays every turn.
                                notify(pool, &run_id).await?;
                            }
                            continue;
                        }
                        crate::dag::RepeatDecision::Fail { reason } => {
                            // Report a resolution only if this sweep actually
                            // landed the failure. The CAS guard means a stale HA
                            // sweep can match nothing; pushing regardless would
                            // log a phantom "resolved" for a task it never
                            // touched — the `Done` path already checks its bool.
                            if fail_parked_subworkflow(pool, &task_id, &child_run_id, &reason)
                                .await?
                            {
                                // The failure frees dependents; wake peers to
                                // advance them, as every other resolver does.
                                notify(pool, &run_id).await?;
                                resolved.push((task_id, false));
                            }
                            continue;
                        }
                    }
                }
                // Not a loop — fall through to the ordinary resolution below.
                Ok(None) => {}
                // A *read* error (not a parse failure — see `repeat_spec_of`) must
                // not be mistaken for "no loop": resolving here would end a
                // conversation and release its downstream on a transient blip.
                // Leave the trigger parked and let the next sweep retry.
                Err(e) => {
                    tracing::warn!(%task_id, error = %e, "could not read a parked trigger's spec; leaving it parked for the next sweep");
                    continue;
                }
            }
        }

        if resolve_subworkflow(pool, &task_id, &child_run_id, succeeded).await? {
            resolved.push((task_id, succeeded));
        }
    }
    Ok(super::Pass {
        done: resolved,
        full,
    })
}

/// The `repeat:` spec of a task, from its stored `TaskSpec` JSON.
///
/// `Ok(None)` for the overwhelming majority of tasks, and for a row whose spec
/// will not parse — a malformed `input` must not wedge the sweep for every other
/// parked trigger, and a trigger with no readable loop resolves the way it always
/// did. A **read** error is different and propagates as `Err`: swallowing it as
/// `None` would let a transient DB blip silently end a conversation and release
/// its downstream, which is the one outcome this feature must never produce.
async fn repeat_spec_of(pool: &Pool, task_id: &str) -> Result<Option<crate::dag::RepeatSpec>> {
    let Some(json) = task_input_json(pool, task_id).await? else {
        return Ok(None);
    };
    match serde_json::from_str::<crate::dag::TaskSpec>(&json) {
        Ok(spec) => Ok(spec.repeat),
        Err(e) => {
            tracing::warn!(%task_id, error = %e, "parked trigger has an unparseable spec; resolving without repeat");
            Ok(None)
        }
    }
}

/// Park a claimed `type: wait` task on its resume deadline (#27): keep it
/// `running` but drop the worker claim/lease and record `wake_at`, so no worker
/// holds a slot and lease recovery leaves it alone until the wait sweep resolves
/// it at the deadline. Fence-guarded; returns whether it parked.
pub async fn park_wait(pool: &Pool, task_id: &str, fence: i64, wake_at: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET wake_at = $1, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = $2 AND status = 'running' AND version = $3 AND stop_requested_at IS NULL",
    )
    .bind(wake_at)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// The time sensors' read of the due waits ([`reconcile_waits`]), for its
/// index (migrations_pg/102): the longest due first, a batch at a time.
pub(crate) const DUE_WAITS: &str = "SELECT id, wake_at FROM task_runs
         WHERE status = 'running' AND wake_at IS NOT NULL AND wake_at <= $1
         ORDER BY wake_at
         LIMIT $2";

/// Sweep parked wait sensors (#27): resolve (succeed) every `running` task whose
/// `wake_at` deadline has passed, advancing its dependents. Returns the resolved
/// tasks, each `(task_id, wake_at)`. Run each reconcile tick; the guarded UPDATE
/// is idempotent and HA-safe (only one scheduler resolves a given task).
///
/// Reads up to [`super::SWEEP_BATCH`] a pass, the longest due first, and says
/// when it read that many.
pub async fn reconcile_waits(pool: &Pool) -> Result<super::Pass<(String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let due: Vec<(String, String)> = sqlx::query_as(DUE_WAITS)
        .bind(&now)
        .bind(super::SWEEP_BATCH)
        .fetch_all(pool)
        .await?;
    let full = due.len() as i64 >= super::SWEEP_BATCH;

    let mut resolved = Vec::new();
    for (task_id, wake_at) in due {
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
        resolved.push((task_id, wake_at));
    }
    Ok(super::Pass {
        done: resolved,
        full,
    })
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
         WHERE id = $3 AND status = 'running' AND version = $4 AND stop_requested_at IS NULL",
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

/// The HTTP sensors' read of the ones due a poll ([`claim_due_url_waits`]),
/// for its index (migrations_pg/103). No `next_poll_at IS NULL` branch:
/// [`park_wait_url`] always sets it, and migrations_pg/106 set it on any
/// sensor parked without one, so the branch only made the read take every
/// parked sensor.
pub(crate) const DUE_URL_WAITS: &str = "SELECT id, wait_url, next_poll_at FROM task_runs
         WHERE status = 'running' AND wait_url IS NOT NULL AND next_poll_at <= $1
         ORDER BY next_poll_at
         LIMIT $2";

/// **Claim** up to `limit` parked HTTP wait sensors (#27 follow-on) due for a
/// poll, the longest due first (the cap bounds how much probing one tick can
/// do), and return the ones this engine won: `(task_id, wait_url, due at)`.
///
/// Claimed as the external polls are ([`claim_due_external_polls`]): every
/// engine sweeps them, and a sensor read and not claimed was polled once by
/// each, which a vendor's endpoint sees as N times the rate. The claim moves
/// `next_poll_at` on to `claim_secs` from now, guarded on the value read, and
/// is the crash bound too: a sensor whose engine dies mid-GET is due again
/// then. The GET's answer moves it again ([`repark_url_wait`]) or resolves
/// the sensor ([`resolve_url_wait`]). The caller makes `claim_secs` longer
/// than its GET's timeout, so a sensor is never claimed while a GET of it is
/// in flight.
pub async fn claim_due_url_waits(
    pool: &Pool,
    limit: i64,
    claim_secs: u64,
) -> Result<Vec<(String, String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let until = crate::dag::delayed_retry_at(claim_secs);
    let rows: Vec<(String, String, String)> = sqlx::query_as(DUE_URL_WAITS)
        .bind(&now)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    let mut won = Vec::with_capacity(rows.len());
    for (id, url, due) in rows {
        let claimed = sqlx::query(
            "UPDATE task_runs SET next_poll_at = $1
             WHERE id = $2 AND status = 'running' AND wait_url IS NOT NULL
               AND next_poll_at = $3",
        )
        .bind(&until)
        .bind(&id)
        .bind(&due)
        .execute(pool)
        .await?
        .rows_affected();
        if claimed > 0 {
            won.push((id, url, due));
        }
    }
    Ok(won)
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

// ── deferred external jobs (`defer:`; the fifth park shape) ──────────────────
//
// Mirrors db/sqlite.rs's quartet — see there for the reasoning on each
// guard. The only differences here are placeholder syntax and that a resolve
// rides the same NOTIFY-free path the other parked resolutions do (the
// reconcile poll picks the advanced dependents up).

/// Park a task on a remote job it just submitted: drop the claim and the lease,
/// keep `status = 'running'`, and record the handle. Fence-guarded.
///
/// The NULL lease is the crash-recovery argument: `recover_expired_leases`
/// filters `lease_expires_at IS NOT NULL`, so a parked row is provably outside
/// the set it can reclaim.
#[allow(clippy::too_many_arguments)]
pub async fn park_external(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    kind: &str,
    handle: &str,
    endpoint: Option<&str>,
    next_poll_at: &str,
    deadline_at: Option<&str>,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET external_kind = $1, external_handle = $2, external_endpoint = $3,
             next_poll_at = $4, external_deadline_at = $5,
             claimed_by = NULL, lease_expires_at = NULL
         WHERE id = $6 AND status = 'running' AND version = $7 AND stop_requested_at IS NULL",
    )
    .bind(kind)
    .bind(handle)
    .bind(endpoint)
    .bind(next_poll_at)
    .bind(deadline_at)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// **Claim** up to `limit` parked external jobs due for a poll and return only
/// the rows this scheduler won. See db/sqlite.rs's copy for why claiming rather
/// than listing (N schedulers would otherwise make N vendor calls per due row)
/// and why the guard is null-safe.
///
/// `IS NOT DISTINCT FROM` is Postgres's null-safe equality, the counterpart of
/// SQLite's `IS`: a row parked with no interval carries `next_poll_at IS NULL`,
/// and an `=` guard would leave exactly those rows unclaimable forever.
pub async fn claim_due_external_polls(
    pool: &Pool,
    limit: i64,
    claim_secs: u64,
) -> Result<Vec<crate::models::ExternalPark>> {
    let now = chrono::Utc::now().to_rfc3339();
    let until = crate::dag::delayed_retry_at(claim_secs);
    let rows = sqlx::query_as::<_, (
        String, String, String, String, String, Option<String>, i64, Option<String>, Option<String>, Option<String>,
    )>(
        "SELECT id, name, run_id, external_kind, external_handle, external_endpoint,
                external_epoch, external_deadline_at, input, next_poll_at
         FROM task_runs
         WHERE status = 'running' AND external_handle IS NOT NULL
           AND (next_poll_at IS NULL OR next_poll_at <= $1)
         ORDER BY next_poll_at
         LIMIT $2",
    )
    .bind(&now)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut won = Vec::with_capacity(rows.len());
    for (id, name, run_id, kind, handle, endpoint, epoch, deadline, input, observed) in rows {
        let claimed = sqlx::query(
            "UPDATE task_runs SET next_poll_at = $1
             WHERE id = $2 AND status = 'running' AND external_handle = $3
               AND next_poll_at IS NOT DISTINCT FROM $4",
        )
        .bind(&until)
        .bind(&id)
        .bind(&handle)
        .bind(&observed)
        .execute(pool)
        .await?
        .rows_affected();
        if claimed > 0 {
            won.push(crate::models::ExternalPark {
                id,
                name,
                run_id,
                external_kind: kind,
                external_handle: handle,
                external_endpoint: endpoint,
                external_epoch: epoch,
                external_deadline_at: deadline,
                input,
            });
        }
    }
    Ok(won)
}

/// Push a parked external job's next poll out after a "still running" verdict.
/// Guarded on the **observed** handle so a stale HA sweep cannot delay a row
/// that has since moved on to a newer job.
pub async fn repark_external(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    next_poll_at: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs SET next_poll_at = $1
         WHERE id = $2 AND status = 'running' AND external_handle = $3",
    )
    .bind(next_poll_at)
    .bind(task_id)
    .bind(handle)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Resolve a parked external job whose remote work succeeded. Clearing the
/// handle in the same statement is what takes the row out of the parked set, so
/// only one scheduler ever resolves it.
pub async fn resolve_external(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    output: Option<&str>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'succeeded', finished_at = $1, output = COALESCE($2, output),
             external_handle = NULL, next_poll_at = NULL, external_deadline_at = NULL
         WHERE id = $3 AND status = 'running' AND external_handle = $4",
    )
    .bind(&now)
    .bind(output)
    .bind(task_id)
    .bind(handle)
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

/// Fail a parked external job and **bump `external_epoch`**, so a retry submits
/// a new remote job instead of adopting the one that just failed (the remote
/// name is `dagron-<id>-<epoch>` and `id` is stable across every reset).
pub async fn fail_external(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    reason: &str,
) -> Result<bool> {
    fail_external_impl(pool, task_id, handle, reason, false).await
}

/// [`fail_external`] that **keeps the handle**, so the row owes a teardown and
/// the cancel sweep stops the remote job. For the `max_wait_secs` ceiling, where
/// the job is by definition still running — unlike a remote failure, which
/// leaves nothing to stop.
pub async fn fail_external_keep_handle(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    reason: &str,
) -> Result<bool> {
    fail_external_impl(pool, task_id, handle, reason, true).await
}

async fn fail_external_impl(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    reason: &str,
    keep_handle: bool,
) -> Result<bool> {
    let handle_sql = if keep_handle { "external_handle" } else { "NULL" };
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(&format!(
        "UPDATE task_runs
         SET status = 'failed', finished_at = $1, output = $2,
             external_handle = {handle_sql}, next_poll_at = NULL, external_deadline_at = NULL,
             external_epoch = external_epoch + 1
         WHERE id = $3 AND status = 'running' AND external_handle = $4"
    ))
    .bind(&now)
    .bind(reason)
    .bind(task_id)
    .bind(handle)
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

// ── cancel teardown (reaching the remote job a cancelled run left running) ───
//
// Mirrors db/sqlite.rs — see there for why the predicate is "holds a handle and
// is not parked", why it is task-level rather than run-level, and why the sweep
// is a lease claim every replica runs rather than a leader-gated one.

/// Which of `ids` name a task that is **still live** — not yet terminal.
///
/// The fleet sweep's only question. It asks about the ids it actually observed
/// on workloads rather than listing every live task, so the query is bounded by
/// what the cluster showed rather than by how busy the datastore is.
///
/// An id that is **absent from the answer is absent for two different reasons**,
/// and both mean the same thing for a workload carrying it: the task reached a
/// terminal status, or the row is gone entirely because retention collected the
/// run. A workload for either is leftover. That conflation is safe only because
/// the caller has already scoped its listing to one installation — without
/// that, "no such row here" would also describe a *live* task belonging to a
/// different dagron sharing the namespace, and deleting on absence would delete
/// someone else's running work.
pub async fn live_task_ids(
    pool: &Pool,
    ids: &[String],
) -> Result<std::collections::HashSet<String>> {
    if ids.is_empty() {
        return Ok(std::collections::HashSet::new());
    }
    // One bound array rather than N placeholders — Postgres has the type, so
    // the statement text is constant and the plan is cacheable however many
    // workloads the sweep saw.
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM task_runs \
          WHERE id = ANY($1) \
            AND status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')",
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Claim up to `limit` rows that owe a remote teardown.
pub async fn claim_due_external_cancels(
    pool: &Pool,
    limit: i64,
    lease_secs: u64,
) -> Result<Vec<crate::models::ExternalCancel>> {
    let now = chrono::Utc::now().to_rfc3339();
    let lease_until = crate::dag::delayed_retry_at(lease_secs);
    // Read the candidates, then CAS each one on the `next_poll_at` this sweep
    // OBSERVED, keeping only the rows whose stamp actually landed — the same
    // shape as `claim_due_external_polls` above, and for the same reason.
    //
    // A transaction is not enough here and the first version of this function
    // relied on one. Under READ COMMITTED two schedulers read the same due
    // rows; the second `UPDATE` waits for the first to commit and then
    // overwrites it, and *both* returned every row they read. Both replicas
    // then send a cancel for the same remote job and both spend an
    // `external_cancel_attempts` on it, so the give-up budget burns at 2× the
    // rate with N replicas. The SQLite twin survives the same code only
    // because SQLite serialises writers through one connection; Postgres does
    // not, which is exactly the case this claim exists for.
    let candidates = sqlx::query_as::<_, (
        String, String, String, String, Option<String>, i64, i64, Option<String>, Option<String>,
        Option<String>,
    )>(
        "SELECT id, run_id, external_kind, external_handle, external_endpoint,
                external_epoch, external_cancel_attempts, finished_at, next_poll_at, input
         FROM task_runs
         WHERE external_handle IS NOT NULL
           AND status <> 'running'
           AND (next_poll_at IS NULL OR next_poll_at <= $1)
         ORDER BY next_poll_at
         LIMIT $2",
    )
    .bind(&now)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut won = Vec::with_capacity(candidates.len());
    for (id, run_id, kind, handle, endpoint, epoch, attempts, finished_at, observed, input) in
        candidates
    {
        let claimed = sqlx::query(
            "UPDATE task_runs SET next_poll_at = $1
             WHERE id = $2 AND external_handle = $3
               AND next_poll_at IS NOT DISTINCT FROM $4",
        )
        .bind(&lease_until)
        .bind(&id)
        .bind(&handle)
        .bind(&observed)
        .execute(pool)
        .await?
        .rows_affected();
        if claimed > 0 {
            won.push(crate::models::ExternalCancel {
                id,
                run_id,
                external_kind: kind,
                external_handle: handle,
                external_endpoint: endpoint,
                external_epoch: epoch,
                external_cancel_attempts: attempts,
                input,
                finished_at,
            });
        }
    }
    Ok(won)
}

/// Settle a row's teardown debt — torn down, or given up on. Guarded on the
/// observed handle so a stale sweep cannot clear a newer submission's debt.
pub async fn clear_external_handle(pool: &Pool, task_id: &str, handle: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET external_handle = NULL, next_poll_at = NULL, external_deadline_at = NULL
         WHERE id = $1 AND external_handle = $2",
    )
    .bind(task_id)
    .bind(handle)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Record a failed teardown attempt and schedule the retry, returning the new
/// attempt count. Only a genuine failure increments.
pub async fn record_external_cancel_failure(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    next_at: &str,
) -> Result<i64> {
    let n: Option<i64> = sqlx::query_scalar(
        "UPDATE task_runs
         SET external_cancel_attempts = external_cancel_attempts + 1, next_poll_at = $1
         WHERE id = $2 AND external_handle = $3
         RETURNING external_cancel_attempts",
    )
    .bind(next_at)
    .bind(task_id)
    .bind(handle)
    .fetch_optional(pool)
    .await?;
    Ok(n.unwrap_or_default())
}

/// Hand the row back without consuming an attempt: this engine has no poller
/// for the kind, so it tried nothing. A `RUNNER_CLASSES` pool runs the same
/// binary with different seams, so one replica can own a kind another cannot.
pub async fn release_external_cancel_claim(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    next_at: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs SET next_poll_at = $1 WHERE id = $2 AND external_handle = $3",
    )
    .bind(next_at)
    .bind(task_id)
    .bind(handle)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
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

/// Record an **external** dataset event (not in this build: producers outside
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

/// Drop every subscription of the registered workflows in `names`, which
/// declare no `on_datasets:` (any more): one statement for all of them, by
/// the table's key, whichever engine synced their rows, and whether or not
/// it has restarted since. Returns the rows dropped.
pub async fn clear_dataset_triggers(pool: &Pool, names: &[String]) -> Result<u64> {
    if names.is_empty() {
        return Ok(0);
    }
    Ok(
        sqlx::query("DELETE FROM dataset_triggers WHERE workflow_name = ANY($1::text[])")
            .bind(names)
            .execute(pool)
            .await?
            .rows_affected(),
    )
}

/// Claim dataset triggers that are due to fire. For each subscribed workflow:
/// `any` mode fires when **any** subscribed dataset has an event newer than its
/// cursor; `all` mode fires only when **every** subscribed dataset does — the
/// fan-in a `wait:` sensor cannot express, since a sensor stamps its cursor at
/// park time and so misses an upstream that landed before the run started.
/// Claiming CAS-advances the fresh cursors to their
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
         WHERE id = $2 AND status = 'running' AND version = $3 AND stop_requested_at IS NULL",
    )
    .bind(uri)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// The dataset sensors' read of the ones whose dataset has moved
/// ([`reconcile_dataset_waits`]), for its index (migrations_pg/104), a batch
/// at a time.
pub(crate) const PARKED_DATASET_WAITS: &str = "SELECT id, wait_dataset FROM task_runs t
         WHERE status = 'running' AND wait_dataset IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM dataset_events e
               WHERE e.uri = t.wait_dataset AND e.id > COALESCE(t.wait_dataset_cursor, 0)
           )
         LIMIT $1";

/// Sweep parked dataset sensors: resolve (succeed) every one whose dataset has
/// an event newer than its park-time cursor, advancing dependents. Returns
/// `(task_id, uri)` resolutions. Idempotent, HA-safe (guarded UPDATE).
///
/// Reads up to [`super::SWEEP_BATCH`] a pass, and says when it read that many.
pub async fn reconcile_dataset_waits(pool: &Pool) -> Result<super::Pass<(String, String)>> {
    let due: Vec<(String, String)> = sqlx::query_as(PARKED_DATASET_WAITS)
        .bind(super::SWEEP_BATCH)
        .fetch_all(pool)
        .await?;
    let full = due.len() as i64 >= super::SWEEP_BATCH;

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
    Ok(super::Pass {
        done: resolved,
        full,
    })
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

/// The datasets one run read and wrote, for its OpenLineage event:
/// `(waited_on, produced)`, each distinct and sorted. See the SQLite twin.
pub async fn run_dataset_io(pool: &Pool, run_id: &str) -> Result<(Vec<String>, Vec<String>)> {
    let waited_on: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT wait_dataset FROM task_runs
         WHERE run_id = $1 AND wait_dataset IS NOT NULL ORDER BY wait_dataset",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    let produced: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT uri FROM dataset_events WHERE run_id = $1 ORDER BY uri",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    Ok((waited_on, produced))
}

/// Reset a failed task to `ready` for a later retry attempt.
///
/// `scheduled_at = retry_at` keeps `claim_ready` from picking it up until the
/// backoff window elapses; `attempt` is left for `claim_ready` to increment,
/// and only [`requeue_quota_wait`] ever gives one back.
/// No NOTIFY is issued — the row is deliberately not yet claimable, so the
/// fixed-interval safety-net poll is what eventually picks it up at `retry_at`.
pub async fn retry_task(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    error: Option<String>,
    retry_at: String,
    end: AttemptEnd,
) -> Result<bool> {
    let policy = AttemptPolicy::from_env();
    let mut tx = pool.begin().await?;

    // `RETURNING` rather than a second read: this statement is the one place
    // that knows the CAS held, and the number it returns is the attempt whose
    // output the same statement is about to overwrite. Reading it separately
    // would race the next claim, which increments it. The number is the
    // task's count for its life (`attempt_base + attempt`), which a rerun or
    // a clear does not set back: it is what `task_attempts` is keyed by.
    let claimed: Option<(i64,)> = sqlx::query_as(
        // Mirrors the SQLite backend: the fault columns describe why this row
        // is *currently* failed, and a row about to run again is not failed.
        // See there for why leaving them costs the roll-up its meaning.
        "UPDATE task_runs
         SET status = 'ready',
             scheduled_at = $1,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = $2,
             fault_class = NULL,
             fault_detail = NULL,
             fault_confidence = NULL
         WHERE id = $3 AND claimed_by = $4 AND version = $5 AND stop_requested_at IS NULL
         RETURNING attempt_base + attempt",
    )
    .bind(&retry_at)
    .bind(&error)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((attempt,)) = claimed else {
        // Asked to stop: it does not run again. Cancelled, where it would
        // have been queued for its next attempt.
        let now = chrono::Utc::now().to_rfc3339();
        if settle_stopping(&mut tx, task_id, worker_id, fence, &now).await? {
            tx.commit().await?;
            tracing::info!(task_id, "the task ended after it was asked to stop: cancelled, not retried");
            return Ok(false);
        }
        tx.commit().await?;
        tracing::warn!(task_id, "stale retry ignored — task already reclaimed");
        return Ok(false);
    };

    // In the same transaction as the overwrite, deliberately: this is the exact
    // instant the attempt's output stops existing anywhere else.
    record_attempt(&mut tx, task_id, attempt, end, error.as_deref(), policy).await?;

    tx.commit().await?;
    Ok(true)
}

/// Keep a bounded tail of the attempt `retry_task` is overwriting, and evict
/// whatever that pushes out of the window. Mirrors the SQLite backend — see
/// there for why this shares the transition's transaction, and
/// [`crate::attempt_log`] for why it is a tail rather than the whole thing.
///
/// A no-op when retention is off (`DAGRON_ATTEMPT_LOG_BYTES=0`).
async fn record_attempt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task_id: &str,
    attempt: i64,
    end: AttemptEnd,
    output: Option<&str>,
    policy: AttemptPolicy,
) -> Result<()> {
    if !policy.enabled() {
        return Ok(());
    }
    let tail = output.map(|o| policy.tail(o));
    sqlx::query(
        "INSERT INTO task_attempts
             (task_id, attempt, reason, output, truncated, finished_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (task_id, attempt) DO UPDATE SET
             reason = excluded.reason,
             output = excluded.output,
             truncated = excluded.truncated,
             finished_at = excluded.finished_at",
    )
    .bind(task_id)
    .bind(attempt)
    .bind(end.as_str())
    .bind(tail.map(|(t, _)| t))
    .bind(tail.is_some_and(|(_, truncated)| truncated))
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut **tx)
    .await?;

    // A range delete on the primary key, not a "keep the newest N" subquery:
    // `attempt` only increases for a task, so the evictable rows are exactly a
    // prefix — one index seek per iteration instead of a scan.
    if let Some(cutoff) = policy.evict_below(attempt) {
        sqlx::query("DELETE FROM task_attempts WHERE task_id = $1 AND attempt <= $2")
            .bind(task_id)
            .bind(cutoff)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Every superseded attempt still retained for a task, oldest first.
///
/// The attempt currently on the row is **not** here — it is in
/// `task_runs.output`, whole.
#[cfg(feature = "ops")]
pub async fn list_task_attempts(pool: &Pool, task_id: &str) -> Result<Vec<TaskAttempt>> {
    // Newest first with a hard cap, then reversed — the caller wants oldest
    // first, but the rows worth keeping when there are too many are the recent
    // ones. Capped because `DAGRON_ATTEMPT_LOG_KEEP=0` disables eviction, and
    // an unbounded retention policy must not imply an unbounded response.
    let mut rows = sqlx::query_as::<_, TaskAttempt>(
        "SELECT task_id, attempt, reason, status, scheduled_at, output, truncated, finished_at
         FROM task_attempts WHERE task_id = $1 ORDER BY attempt DESC LIMIT $2",
    )
    .bind(task_id)
    .bind(i64::try_from(crate::attempt_log::MAX_READ).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;
    rows.reverse();
    Ok(rows)
}

/// How many attempts `task_id` made in the rounds a rerun or a clear ended
/// (`attempt_base`). The attempt on its row plus this is the task's count for
/// its life, which is what its kept attempts are numbered by. 0 for a task
/// that is not there.
#[cfg(feature = "ops")]
pub async fn task_attempt_base(pool: &Pool, task_id: &str) -> Result<i64> {
    let base: Option<i64> =
        sqlx::query_scalar("SELECT attempt_base FROM task_runs WHERE id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(base.unwrap_or(0))
}

// ── Quota back-pressure ──────────────────────────────────────────────────────

/// Hand a claimed task back to the queue because its pod was refused by a full
/// Kubernetes namespace `ResourceQuota` (migration 051/066): the row returns to
/// `ready` with a future `scheduled_at` and the attempt the claim consumed is
/// given back. Mirrors the SQLite backend — see there for why the give-back is
/// sound, how the wait marker (`quota_wait_version = version`) expires without
/// being cleared, and what continues an episode.
///
/// Fenced like [`retry_task`], plus `status = 'running'`, so it lands at most
/// once per claim; a lost fence is [`QuotaRequeue::Stale`]. At the bound
/// ([`QuotaRequeue::Expired`]) **nothing is written** and the caller still
/// holds the claim.
///
/// The read takes the row lock (`FOR UPDATE`), so the decision and the write
/// are about the same row state: a sweep on another scheduler (lease recovery,
/// a cancel) either lands before the read, and this call is `Stale`, or waits
/// for the commit. No NOTIFY, for the reason `retry_task` gives — the row is
/// deliberately not claimable yet.
pub async fn requeue_quota_wait(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    detail: &str,
    max_wait_secs: i64,
) -> Result<QuotaRequeue> {
    // One wall-clock read: the bound check, the probe time and the durations
    // reported back all derive from it.
    let now = chrono::Utc::now();
    let mut tx = pool.begin().await?;

    let held: Option<(Option<String>, i64, Option<i64>)> = sqlx::query_as(
        "SELECT quota_wait_since, quota_waits, quota_wait_version FROM task_runs
         WHERE id = $1 AND claimed_by = $2 AND version = $3 AND status = 'running'
           AND stop_requested_at IS NULL
         FOR UPDATE",
    )
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .fetch_optional(&mut *tx)
    .await?;

    let Some((since, waits, wait_version)) = held else {
        // Asked to stop, and nothing was started for this dispatch: cancelled,
        // where it would have been handed back to its queue.
        if settle_stopping(&mut tx, task_id, worker_id, fence, &now.to_rfc3339()).await? {
            tx.commit().await?;
            tracing::info!(task_id, "the task was asked to stop before it started: cancelled, not requeued");
            return Ok(QuotaRequeue::Stale);
        }
        tx.commit().await?;
        tracing::warn!(task_id, "stale quota requeue ignored — task already reclaimed");
        return Ok(QuotaRequeue::Stale);
    };

    // A `since` that does not parse is treated as no episode rather than as an
    // error: the cost is a bound that restarts, not a task that cannot be
    // handed back.
    let prior = (wait_version == Some(fence - 1))
        .then_some(since)
        .flatten()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
        .map(|s| (s.with_timezone(&chrono::Utc), waits));

    let jitter = quota_wait::jitter_permille(task_id, fence);
    let (since, waits, retry_at) = match quota_wait::step(now, prior, max_wait_secs, jitter) {
        quota_wait::Step::Wait { since, waits, retry_at } => (since, waits, retry_at),
        quota_wait::Step::GiveUp { waits, waited_secs } => {
            tx.commit().await?;
            return Ok(QuotaRequeue::Expired { waits, waited_secs });
        }
    };

    let rows = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready',
             scheduled_at = $1,
             claimed_by = NULL,
             lease_expires_at = NULL,
             attempt = GREATEST(attempt - 1, 0),
             quota_wait_since = $2,
             quota_waits = $3,
             quota_wait_version = version,
             quota_wait_detail = $4
         WHERE id = $5 AND claimed_by = $6 AND version = $7 AND status = 'running'
           AND stop_requested_at IS NULL",
    )
    .bind(retry_at.to_rfc3339())
    .bind(since.to_rfc3339())
    .bind(waits)
    .bind(quota_wait::clip_detail(detail))
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;

    if rows == 0 {
        tracing::warn!(task_id, "stale quota requeue ignored — task already reclaimed");
        return Ok(QuotaRequeue::Stale);
    }
    Ok(QuotaRequeue::Requeued {
        waits,
        waited_secs: (now - since).num_seconds().max(0),
        retry_in_secs: (retry_at - now).num_seconds().max(0),
    })
}

/// Tasks waiting on quota right now, and when the longest-waiting one was first
/// refused. See the SQLite twin: the predicate is the waiting rule itself,
/// because the columns outlive the wait. Not `ops`-gated.
pub async fn quota_wait_stats(pool: &Pool) -> Result<QuotaWaitStats> {
    let (waiting, oldest_since): (i64, Option<String>) = sqlx::query_as(
        "SELECT COUNT(*), MIN(quota_wait_since) FROM task_runs
         WHERE status = 'ready' AND quota_wait_version = version",
    )
    .fetch_one(pool)
    .await?;
    Ok(QuotaWaitStats { waiting, oldest_since })
}

// ── Fault attribution ────────────────────────────────────────────────────────

/// Record the fault attribution for the attempt that just failed (migration
/// 040/050): what broke, the line that says so, and how much to trust it.
///
/// **Fence-guarded like every other post-execution mutation.** Called
/// immediately before `retry_task` / `mark_task_failed`, which carry the same
/// guard — so a worker whose lease was already reclaimed writes neither the
/// verdict nor the transition, and a stale attempt can never relabel a newer
/// one's failure. The two are deliberately *not* one statement: `retry_task`
/// is also the repeat-loop's re-queue path, which has no fault to record, and
/// folding a diagnostic column into the state-machine transition would put a
/// classification bug on the critical path of every requeue.
///
/// Best-effort at the call site: a failed write here loses a breadcrumb, never
/// a state transition. Returns `false` when the fence no longer holds.
pub async fn record_task_fault(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    class: crate::fault::FaultClass,
    detail: Option<&str>,
    confidence: crate::fault::Confidence,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET fault_class = $1, fault_detail = $2, fault_confidence = $3
         WHERE id = $4 AND claimed_by = $5 AND version = $6",
    )
    .bind(class.as_str())
    .bind(detail)
    .bind(confidence.as_str())
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Read back the fault attribution for one task. The autopsy tool's `--from-db`
/// path and the ops surface both need it, and neither wants to re-select the
/// whole row (which carries the task's full spec JSON in `input`).
#[cfg_attr(not(feature = "ops"), allow(dead_code))]
pub async fn get_task_fault(
    pool: &Pool,
    task_id: &str,
) -> Result<Option<(String, Option<String>, Option<String>)>> {
    let row: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT fault_class, fault_detail, fault_confidence FROM task_runs WHERE id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(c, d, conf)| c.map(|c| (c, d, conf))))
}

/// Fleet roll-up: how many failures of each class finished in `[since, until)`,
/// and the total attempts they consumed. The query behind any
/// "what did this cost us" number — served by the partial index from migration
/// 040/051, which is why the `fault_class IS NOT NULL` predicate is spelled out
/// rather than left implicit in the GROUP BY.
#[cfg_attr(not(feature = "ops"), allow(dead_code))]
pub async fn fault_class_counts(
    pool: &Pool,
    since: &str,
    until: &str,
) -> Result<Vec<(String, i64, i64)>> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        // `status = 'failed'` as well as a non-NULL class — see the SQLite
        // backend for why, and keep the two predicates identical or the
        // backends report different numbers for the same fleet.
        "SELECT fault_class, COUNT(*)::bigint AS n, COALESCE(SUM(attempt), 0)::bigint AS attempts
         FROM task_runs
         WHERE fault_class IS NOT NULL AND status = 'failed'
           AND finished_at >= $1 AND finished_at < $2
         GROUP BY fault_class
         ORDER BY n DESC",
    )
    .bind(since)
    .bind(until)
    .fetch_all(pool)
    .await?;
    Ok(rows)
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

// ── Running again: one implementation ───────────────────────────────────────
//
// A rerun, a retry and a clear each put tasks of a run back to run again, and
// each is asked for through two doors: the engine's own API and dagron-api's.
// dagron-api carried its own SQL for all three, "kept in sync" by a comment,
// and the copies had drifted: its rerun left rule-skipped tasks skipped, and
// none of its three cleared a task's fault verdict, its log or its remote
// job's handle. Both doors call the functions below now, and a rerun of a run
// whole ([`rerun_all`]) is one more of them. Each, in one transaction:
// - **keeps what it replaces** ([`keep_replaced`]): a task that ran has that
//   attempt's status, times and output tail put in `task_attempts` before its
//   row is reset;
// - **asks the concurrency key** ([`lock_run_cap`], [`refuse_if_full`]) when
//   it sets a run that had ended running again, as `create_run` asks it for a
//   new run;
// - **writes one outbox event** ([`insert_run_again`]), so that what listens
//   for a run's end also hears that it is running again.

/// The tasks one of them resets: the `WHERE` its statements share.
#[cfg(feature = "ops")]
#[derive(Clone, Copy)]
enum Reset<'a> {
    /// A run's broken cone: its failed, cancelled and rule-skipped tasks.
    BrokenCone { run_id: &'a str },
    /// One failed or cancelled task.
    Task { task_id: &'a str },
    /// A completed task, and every completed task that depends on it.
    Cone { run_id: &'a str, task_id: &'a str },
    /// A run whole: every task of it that ended, what succeeded included,
    /// but a runtime fan-out's instances, which [`rerun_all_in`] drops.
    Whole { run_id: &'a str },
}

#[cfg(feature = "ops")]
impl<'a> Reset<'a> {
    /// The condition over `task_runs t`, its placeholders numbered from
    /// `first`, and what they are bound to, in order.
    fn condition(self, first: usize) -> (String, Vec<&'a str>) {
        match self {
            Reset::BrokenCone { run_id } => (
                format!("t.run_id = ${first} AND t.status IN ('failed', 'cancelled', 'skipped')"),
                vec![run_id],
            ),
            Reset::Whole { run_id } => (
                format!(
                    "t.run_id = ${first} AND t.fanout_parent IS NULL
                     AND t.status IN ('succeeded', 'failed', 'skipped', 'cancelled')"
                ),
                vec![run_id],
            ),
            Reset::Task { task_id } => (
                format!("t.id = ${first} AND t.status IN ('failed', 'cancelled')"),
                vec![task_id],
            ),
            Reset::Cone { run_id, task_id } => (
                format!(
                    "t.id IN (
                         WITH RECURSIVE cone(id) AS (
                             SELECT id FROM task_runs WHERE id = ${first} AND run_id = ${}
                             UNION
                             SELECT td.dependent_id FROM task_dependencies td
                             JOIN cone c ON td.dependency_id = c.id
                         )
                         SELECT id FROM cone)
                     AND t.status IN ('succeeded', 'failed', 'skipped', 'cancelled')",
                    first + 1
                ),
                vec![task_id, run_id],
            ),
        }
    }
}

/// A decided approval gate, over `task_runs t`. A gate is never claimed, so
/// its `attempt` stays 0, and yet its decision is what it did: a reset keeps
/// it as an attempt ([`keep_replaced`]), numbered `attempt_base + 1`, and
/// moves `attempt_base` on by one for it, as it moves it on by a task's
/// attempts. Else putting a gate back would leave no record of who approved
/// what it guarded.
#[cfg(feature = "ops")]
const DECIDED_GATE: &str = "(t.is_approval = 1 AND t.status IN ('succeeded', 'failed'))";

/// Keep the attempts `reset` is about to replace, in its transaction and
/// before it: for each task that ran, how the attempt ended, when it was
/// queued and when it ended, and the tail of what it printed, under `reason`
/// (what is replacing it). A task that never ran, cancelled or skipped before
/// a worker took it, has no attempt to keep. What it printed is what a reader
/// of its log saw: the log an executor streamed, or its output where none did.
/// A decided approval gate is kept as one attempt ([`DECIDED_GATE`]).
///
/// The row is written under `attempt_base + attempt`, the task's count for
/// its life ([`TaskAttempt::attempt`]), and the rows that pushes out of the
/// window go, as [`record_attempt`] does for a worker's retry. The tail is cut
/// here in characters, where that one cuts in bytes: it is taken in the
/// statement that reads the rows, so that a rerun of ten thousand failed tasks
/// moves no output through this process.
///
/// A no-op when retention is off (`DAGRON_ATTEMPT_LOG_BYTES=0`).
#[cfg(feature = "ops")]
async fn keep_replaced(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    reset: Reset<'_>,
    reason: &str,
    now: &str,
) -> Result<()> {
    let policy = AttemptPolicy::from_env();
    if !policy.enabled() {
        return Ok(());
    }
    let (condition, bound) = reset.condition(4);
    let sql = format!(
        "INSERT INTO task_attempts
             (task_id, attempt, reason, status, output, truncated, scheduled_at, finished_at)
         SELECT t.id, t.attempt_base + GREATEST(t.attempt, 1), $1, t.status,
                right(COALESCE(NULLIF(t.log, ''), t.output), $2),
                COALESCE(char_length(COALESCE(NULLIF(t.log, ''), t.output)) > $2, false),
                t.scheduled_at, COALESCE(t.finished_at, $3)
         FROM task_runs t
         WHERE (t.attempt > 0 OR {DECIDED_GATE}) AND {condition}
         ON CONFLICT (task_id, attempt) DO UPDATE SET
             reason = excluded.reason,
             status = excluded.status,
             output = excluded.output,
             truncated = excluded.truncated,
             scheduled_at = excluded.scheduled_at,
             finished_at = excluded.finished_at"
    );
    let mut kept = sqlx::query(&sql)
        .bind(reason)
        .bind(i32::try_from(policy.bytes).unwrap_or(i32::MAX))
        .bind(now);
    for value in &bound {
        kept = kept.bind(*value);
    }
    kept.execute(&mut **tx).await?;

    if policy.keep > 0 {
        let (condition, bound) = reset.condition(2);
        let sql = format!(
            "DELETE FROM task_attempts a USING task_runs t
             WHERE a.task_id = t.id AND (t.attempt > 0 OR {DECIDED_GATE}) AND {condition}
               AND a.attempt <= t.attempt_base + GREATEST(t.attempt, 1) - $1"
        );
        let mut evicted = sqlx::query(&sql).bind(i64::try_from(policy.keep).unwrap_or(i64::MAX));
        for value in &bound {
            evicted = evicted.bind(*value);
        }
        evicted.execute(&mut **tx).await?;
    }
    Ok(())
}

/// The cap a run's workflow puts on its runs in flight, and what it counts
/// them under.
#[cfg(feature = "ops")]
struct RunCap {
    name: String,
    key: Option<String>,
    max: u32,
}

/// `run_id`'s cap, with its admission lock taken, or `None` for a workflow
/// with no cap (or no such run). The lock is the one `create_run` takes, for
/// the same reason: the count in [`refuse_if_full`] and the write that sets
/// the run running must be one step against other admitters.
///
/// Taken first, before any row of the run is written, by each of the three:
/// two of them on one run then queue here and not on each other's rows.
#[cfg(feature = "ops")]
async fn lock_run_cap(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> Result<Option<RunCap>> {
    let run: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT d.name, d.spec, wr.concurrency_key
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.id = $1",
    )
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((name, spec, key)) = run else {
        return Ok(None);
    };
    let Some(max) = crate::dag::DagGraph::stored_max_active_runs(&spec) else {
        return Ok(None);
    };
    let lock_scope = match &key {
        Some(key) => format!("{name}\u{1f}{key}"),
        None => name.clone(),
    };
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(RUN_ADMISSION_LOCK)
        .bind(&lock_scope)
        .execute(&mut **tx)
        .await?;
    Ok(Some(RunCap { name, key, max }))
}

/// Count `run_id`'s deadlines again from now ([`super::deadlines_again`]), in
/// the transaction that sets it running again after it ended, and let its SLA
/// alert fire again: the run is held to its `run_timeout_secs` and its
/// `deadline` from when it runs again, as a run let in after a wait is
/// ([`admit_waiting_runs`]). Each of the four that set an ended run running
/// again calls it once it has.
#[cfg(feature = "ops")]
async fn rearm_deadlines(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> Result<()> {
    let spec: Option<String> = sqlx::query_scalar(
        "SELECT d.spec FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.id = $1",
    )
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(spec) = spec else {
        return Ok(());
    };
    let (deadline_at, alert_deadline_at) = super::deadlines_again(&spec, chrono::Utc::now());
    sqlx::query(
        "UPDATE workflow_runs
         SET deadline_at = COALESCE($1, deadline_at),
             alert_deadline_at = COALESCE($2, alert_deadline_at),
             alert_fired_at = NULL
         WHERE id = $3",
    )
    .bind(deadline_at)
    .bind(alert_deadline_at)
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Refuse to set `run_id` running again while its key is full without it:
/// [`MaxActiveRunsReached`](crate::models::MaxActiveRunsReached), as a new run
/// of the key would be refused. Called once the run reads `running` in this
/// transaction, which the error then rolls back.
///
/// Refused whatever the workflow says of a full cap (`when_full`): a run that
/// is set running again does not wait, and does not take another's place. A
/// run that waits for the key is in its way as one that runs is, so it does
/// not go ahead of one either.
#[cfg(feature = "ops")]
async fn refuse_if_full(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    cap: &RunCap,
) -> Result<()> {
    let (active, _) = runs_in_flight(tx, &cap.name, cap.key.as_deref(), Some(run_id)).await?;
    if active >= i64::from(cap.max) {
        return Err(anyhow::Error::new(crate::models::MaxActiveRunsReached {
            name: cap.name.clone(),
            max: cap.max,
            active,
            waiting: 0,
            key: cap.key.clone(),
        }));
    }
    Ok(())
}

/// Refuse to rerun `run_id` whole while tasks of it still owe the teardown
/// of a remote job: [`TeardownOwed`](crate::models::TeardownOwed), with
/// nothing written. The rerun resets each task's handle with the rest of its
/// result, and a handle it dropped would never be swept, so the job would run
/// on uncounted. The rule is `gc_old_runs`' own, for the same reason.
///
/// Called once the run reads `running` in this transaction: the cancel that
/// ended the run has committed, and its debt is seen.
#[cfg(feature = "ops")]
async fn refuse_if_teardown_owed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> Result<()> {
    let owing: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_runs
         WHERE run_id = $1 AND external_handle IS NOT NULL AND status <> 'running'",
    )
    .bind(run_id)
    .fetch_one(&mut **tx)
    .await?;
    if owing > 0 {
        return Err(anyhow::Error::new(crate::models::TeardownOwed {
            run_id: run_id.to_string(),
            tasks: owing,
        }));
    }
    Ok(())
}

/// How many runs of workflow `name` are in flight under `key`, and how many
/// of those wait for their turn: `(in flight, waiting)`. `except` leaves one
/// run out, for a run that asks about the others.
///
/// A run being stopped is in flight until its work is gone, and a run that
/// waits is in flight from the moment it is made. The caller holds the key's
/// admission lock.
async fn runs_in_flight(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
    key: Option<&str>,
    except: Option<&str>,
) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT COUNT(*),
                COUNT(*) FILTER (WHERE wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL)
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.status = 'running' AND d.name = $1
           AND wr.concurrency_key IS NOT DISTINCT FROM $2
           AND wr.id IS DISTINCT FROM $3",
    )
    .bind(name)
    .bind(key)
    .bind(except)
    .fetch_one(&mut **tx)
    .await?)
}

/// Cancel the runs a new run of workflow `name` and `key` takes the place of
/// (`when_full: supersede`): every run of the key that has not ended and is
/// not being stopped already, the ones that wait as the ones that run.
/// Returns how many were asked.
///
/// Each is cancelled as [`cancel_run`] cancels one, in the new run's own
/// transaction: a task nothing runs for is `cancelled` here, and one a worker
/// holds is asked to stop. A run with nothing stopping ends at once, with its
/// `run.completed` event, which the rest of its end-of-run work is owed to.
/// One with work still running stays `running`
/// until that is gone, and holds its key until then: the new run waits for
/// it, and is let in by [`admit_waiting_runs`]. Its `output` says which run
/// took its place (`by`).
///
/// `own` is the new run's record where that run is not trusted. Such a run
/// takes the place of runs of its own source alone: the same forge and
/// repository, and the same pull request and ref. Its key is its author's to
/// write, and one that named another pull request's key would otherwise
/// cancel that one's runs. What it may not cancel stays in its way, and it
/// waits behind it.
///
/// The caller holds the key's admission lock, taken before any run's row as
/// the three that set a run running again take it.
async fn supersede_runs(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
    key: Option<&str>,
    by: &str,
    own: Option<&crate::models::RunTrigger>,
    now: &str,
) -> Result<u64> {
    let before: Vec<String> = sqlx::query_scalar(
        "SELECT wr.id FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.status = 'running' AND wr.stopping_as IS NULL AND d.name = $1
           AND wr.concurrency_key IS NOT DISTINCT FROM $2
           AND (NOT $3 OR EXISTS (
               SELECT 1 FROM run_triggers t
               WHERE t.run_id = wr.id
                 AND t.forge_host IS NOT DISTINCT FROM $4
                 AND t.repo IS NOT DISTINCT FROM $5
                 AND t.pr IS NOT DISTINCT FROM $6
                 AND t.ref IS NOT DISTINCT FROM $7
           ))
         ORDER BY wr.created_at, wr.id",
    )
    .bind(name)
    .bind(key)
    .bind(own.is_some())
    .bind(own.and_then(|t| t.forge_host.as_deref()))
    .bind(own.and_then(|t| t.repo.as_deref()))
    .bind(own.and_then(|t| t.pr))
    .bind(own.and_then(|t| t.git_ref.as_deref()))
    .fetch_all(&mut **tx)
    .await?;

    let mut asked = 0;
    for run_id in &before {
        // The run row first, as a cancel takes it.
        let ours = sqlx::query(
            "UPDATE workflow_runs
             SET stopping_as = 'cancelled', stop_requested_at = $1, start_pending_at = NULL,
                 output = $3
             WHERE id = $2 AND status = 'running' AND stopping_as IS NULL",
        )
        .bind(now)
        .bind(run_id)
        .bind(format!("superseded by run {by}"))
        .execute(&mut **tx)
        .await?
        .rows_affected();
        if ours == 0 {
            continue;
        }
        stop_run_tasks(tx, run_id, now).await?;
        finish_stopped_run(tx, run_id, now).await?;
        notify(&mut **tx, run_id).await?;
        asked += 1;
    }
    Ok(asked)
}

/// Write the event that says tasks of `run_id` were put back to run again
/// ([`RunAgain::event`]), in the transaction that puts them back.
#[cfg(feature = "ops")]
async fn insert_run_again(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    by: crate::models::RunAgain,
    tasks: u64,
    task_id: Option<&str>,
    now: &str,
) -> Result<()> {
    let (event_type, payload) = by.event(run_id, tasks, task_id);
    sqlx::query(
        "INSERT INTO event_outbox
           (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
         VALUES ($1, $2, $3, $4, 'pending', 0, $5, $6)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(run_id)
    .bind(event_type)
    .bind(&payload)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Resurrect a failed/cancelled task from the management API (a retry).
///
/// Unlike [`retry_task`], this carries NO worker fence — a human retrying a dead
/// task has no claim to match. Resets the task to `ready`, queued now (version
/// bumped so any late stale worker is fenced off), and sets its run running
/// again if it had ended, failed or cancelled: a run whose task is about to
/// run is a running run. Only this task is retried; what its failure
/// cancelled downstream is a rerun's or a clear's to bring back. The attempt
/// it replaces is kept, the concurrency key is asked if the run had ended,
/// and an event is written (see "Running again", above).
///
/// `false` if the task is not in a retryable (`failed`/`cancelled`) state.
/// [`MaxActiveRunsReached`](crate::models::MaxActiveRunsReached) if its run had
/// ended and its key is full: then nothing is written.
#[cfg(feature = "ops")]
pub async fn retry_task_from_ui(pool: &Pool, task_id: &str) -> Result<bool> {
    retry_task_from_ui_by(pool, task_id, None).await
}

/// [`retry_task_from_ui`], saying who asked for it (`reset_by`): the person
/// dagron-api was asked by, from whom a gate put back to ask again takes no
/// approval where it is `not_triggerer`.
#[cfg(feature = "ops")]
pub async fn retry_task_from_ui_by(pool: &Pool, task_id: &str, who: Option<&str>) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let run_id: Option<String> = sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = $1")
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(run_id) = run_id else {
        tx.commit().await?;
        return Ok(false);
    };
    let cap = lock_run_cap(&mut tx, &run_id).await?;
    keep_replaced(&mut tx, Reset::Task { task_id }, crate::models::RunAgain::Retry.reason(), &now).await?;

    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready',
             claimed_by = NULL,
             lease_expires_at = NULL,
             scheduled_at = $3,
             finished_at = NULL,
             output = NULL,
             log = NULL, log_fence = NULL, log_gen = NULL,
             -- As a worker's own retry clears them: the verdict says why the
             -- row is failed, and a row about to run again is not.
             fault_class = NULL,
             fault_detail = NULL,
             fault_confidence = NULL,
             -- A rejected gate's decision is kept as an attempt of its
             -- (`DECIDED_GATE`), and the gate asks again from nothing; what
             -- parked a row last time goes as a rerun's reset has it go.
             attempt_base = attempt_base
                 + CASE WHEN attempt = 0 AND is_approval = 1 AND status IN ('succeeded', 'failed')
                        THEN 1 ELSE 0 END,
             sub_run_id = NULL, wake_at = NULL, wait_url = NULL,
             wait_dataset = NULL, wait_dataset_cursor = NULL,
             decided_by = NULL, decision_comment = NULL, cache_hit = FALSE,
             reset_by = $2,
             version = version + 1
         WHERE id = $1 AND status IN ('failed', 'cancelled')",
    )
    .bind(task_id)
    .bind(who)
    .bind(&now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if updated == 0 {
        // Not retryable, or no longer: what was kept above goes with it.
        tx.rollback().await?;
        return Ok(false);
    }

    let rearmed = sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END
         WHERE id = $1 AND status IN ('failed', 'cancelled')",
    )
    .bind(&run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if rearmed == 1 {
        if let Some(cap) = &cap {
            refuse_if_full(&mut tx, &run_id, cap).await?;
        }
        rearm_deadlines(&mut tx, &run_id).await?;
    }

    insert_run_again(&mut tx, &run_id, crate::models::RunAgain::Retry, 1, Some(task_id), &now).await?;
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
/// [`MaxActiveRunsReached`](crate::models::MaxActiveRunsReached) if the run's
/// concurrency key is full: then nothing is written. The attempts it replaces
/// are kept and an event is written (see "Running again", above).
#[cfg(feature = "ops")]
pub async fn rerun_from_failed(pool: &Pool, run_id: &str) -> Result<Option<u64>> {
    rerun_from_failed_as(pool, run_id, crate::models::RunAgain::Rerun).await
}

/// [`rerun_from_failed`], saying what asked for it: a person's rerun, or the
/// engine's own ([`RunAgain::AutoRerun`](crate::models::RunAgain::AutoRerun)),
/// whose event keeps its own type.
#[cfg(feature = "ops")]
pub async fn rerun_from_failed_as(
    pool: &Pool,
    run_id: &str,
    by: crate::models::RunAgain,
) -> Result<Option<u64>> {
    let mut tx = pool.begin().await?;
    let reset = rerun_from_failed_in(&mut tx, run_id, by, None).await?;
    tx.commit().await?;
    Ok(reset)
}

/// [`rerun_from_failed_as`] inside the caller's transaction, for a caller
/// with a write of its own to land with the rerun or not at all (dagron-api's
/// `params` override of the reset tasks' input). The caller commits. It must
/// hold no row lock of this run when it calls: the admission lock is taken
/// first here, as by the other two. `who` is the person dagron-api was asked
/// by, recorded on each task put back (`reset_by`).
#[cfg(feature = "ops")]
pub async fn rerun_from_failed_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    by: crate::models::RunAgain,
    who: Option<&str>,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let cap = lock_run_cap(tx, run_id).await?;

    // Re-arm the run as the atomic gate. This guarded UPDATE serializes concurrent
    // reruns: the row lock blocks a second tx until the first commits, after which
    // the status is already 'running' and it matches zero rows. A miss (run absent,
    // not rerunnable, or a lost race) → `None`, so no caller reports a false success.
    // A run that was cancelled as it waited for its key was never let in, and
    // its tasks are put back below with the dependencies they have: it does
    // not wait now, and `admit_waiting_runs` must not take it for one that
    // does. The same holds for the two other statements that set a run
    // running again.
    let armed = sqlx::query(
        "UPDATE workflow_runs
         SET status = 'running', finished_at = NULL, output = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END
         WHERE id = $1 AND status IN ('failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if armed == 0 {
        return Ok(None);
    }
    if let Some(cap) = &cap {
        refuse_if_full(tx, run_id, cap).await?;
    }
    rearm_deadlines(tx, run_id).await?;
    keep_replaced(tx, Reset::BrokenCone { run_id }, by.reason(), &now).await?;

    // Reset the broken cone (failed, cancelled, and rule-skipped tasks) in two
    // statements so the remaining_deps recompute sees the post-reset statuses —
    // a skipped→pending row must count as outstanding, which a single
    // self-referential UPDATE cannot guarantee.
    let reset = sqlx::query(
        "UPDATE task_runs
         SET status = 'pending',
             -- The round's attempts join the task's count for its life, which
             -- is what its kept attempts are numbered by, and the round starts
             -- again from none: the task has its `max_attempts` once more. A
             -- rejected gate's decision counts as one (`DECIDED_GATE`).
             attempt_base = attempt_base
                 + CASE WHEN attempt = 0 AND is_approval = 1 AND status IN ('succeeded', 'failed')
                        THEN 1 ELSE attempt END,
             attempt = 0,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = NULL,
             log = NULL, log_fence = NULL, log_gen = NULL,
             finished_at = NULL,
             -- The fault verdict describes why this row is *currently* failed.
             -- A row being reset to run again is not failed, so the previous
             -- attempt's attribution goes with the rest of its result. Missed
             -- here originally: `retry_task` cleared them and these two reset
             -- paths did not, so a rerun or a Clear task left a stale class on
             -- a row that could then fail for an entirely different reason —
             -- and the roll-up would report the old one.
             fault_class = NULL,
             fault_detail = NULL,
             fault_confidence = NULL,
             -- A re-armed deferred row must submit a NEW remote job, not adopt
             -- the one that just failed: the remote name is
             -- `dagron-<id>-<epoch>` and `id` is stable across every reset.
             external_kind = NULL,
             external_handle = NULL,
             external_endpoint = NULL,
             external_deadline_at = NULL,
             next_poll_at = NULL,
             external_epoch = external_epoch + 1,
             -- What parked the row last time, and who decided it, as a whole
             -- rerun's reset has them go (see the clear's).
             sub_run_id = NULL, wake_at = NULL, wait_url = NULL,
             wait_dataset = NULL, wait_dataset_cursor = NULL,
             decided_by = NULL, decision_comment = NULL, cache_hit = FALSE,
             reset_by = $3,
             scheduled_at = $1,
             version = version + 1
         WHERE run_id = $2 AND status IN ('failed', 'cancelled', 'skipped')",
    )
    .bind(&now)
    .bind(run_id)
    .bind(who)
    .execute(&mut **tx)
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
    .execute(&mut **tx)
    .await?;

    insert_run_again(tx, run_id, by, reset, None, &now).await?;
    notify(&mut **tx, run_id).await?;
    Ok(Some(reset))
}

/// Rerun a run whole (`from: all`): every task of a run that has ended,
/// succeeded, failed or cancelled, is put back, what succeeded included, and
/// the run runs again from its first tasks, as GitHub's "Re-run all jobs"
/// does. The same run, so the same id and trigger record.
///
/// As [`rerun_from_failed`] in all else: `None` when the run does not exist,
/// has not ended, or loses a race with another rerun; `Some(n)` with the
/// number of tasks reset; [`MaxActiveRunsReached`](crate::models::MaxActiveRunsReached)
/// if its concurrency key is full, and then nothing is written;
/// [`TeardownOwed`](crate::models::TeardownOwed), and nothing written, while a
/// task of the run still owes the teardown of a remote job (a deferred task
/// cancelled while its job ran). The attempts it replaces are kept, under
/// `rerun`, and its `run.rerun` event says `"from": "all"` (see "Running
/// again", above). Each task is reset as a clear resets one, its checkpoint
/// dropped with the rest of its result: the run starts over, and resumes
/// nothing. An approval gate asks again.
#[cfg(feature = "ops")]
pub async fn rerun_all(pool: &Pool, run_id: &str) -> Result<Option<u64>> {
    let mut tx = pool.begin().await?;
    let reset = rerun_all_in(&mut tx, run_id, None).await?;
    tx.commit().await?;
    Ok(reset)
}

/// [`rerun_all`] inside the caller's transaction, as
/// [`rerun_from_failed_in`] is for [`rerun_from_failed`]: for dagron-api's
/// `params` override, written with it. The caller commits, and holds no row
/// lock of this run when it calls. `who` is the person dagron-api was asked
/// by (`reset_by`).
#[cfg(feature = "ops")]
pub async fn rerun_all_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    who: Option<&str>,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let by = crate::models::RunAgain::RerunAll;
    let cap = lock_run_cap(tx, run_id).await?;

    // The gate, as a rerun from the failed tasks has it, and open to a run
    // that succeeded too. It counts the rerun of every job (`whole_reruns`),
    // by which a reader that counts runs tells it from a rerun of the failed
    // ones.
    let armed = sqlx::query(
        "UPDATE workflow_runs
         SET status = 'running', finished_at = NULL, output = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END,
             whole_reruns = whole_reruns + 1
         WHERE id = $1 AND status IN ('succeeded', 'failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if armed == 0 {
        return Ok(None);
    }
    if let Some(cap) = &cap {
        refuse_if_full(tx, run_id, cap).await?;
    }
    refuse_if_teardown_owed(tx, run_id).await?;
    rearm_deadlines(tx, run_id).await?;
    keep_replaced(tx, Reset::Whole { run_id }, by.reason(), &now).await?;

    // A runtime fan-out's instances are made from its producer's output, which
    // runs again first: they go, edges first, as the barrier's next expansion
    // would drop them (`reconcile_fanouts`). Kept, a barrier would be judged
    // by the last run's instances before it is expanded again, and wait for a
    // failed one forever.
    sqlx::query(
        "DELETE FROM task_dependencies
         WHERE dependency_id IN (
                   SELECT id FROM task_runs WHERE run_id = $1 AND fanout_parent IS NOT NULL)
            OR dependent_id IN (
                   SELECT id FROM task_runs WHERE run_id = $1 AND fanout_parent IS NOT NULL)",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM task_runs WHERE run_id = $1 AND fanout_parent IS NOT NULL")
        .bind(run_id)
        .execute(&mut **tx)
        .await?;

    // Every other task, as a clear resets one (see there for each column).
    let reset = sqlx::query(
        "UPDATE task_runs
         SET status = 'pending',
             attempt_base = attempt_base
                 + CASE WHEN attempt = 0 AND is_approval = 1 AND status IN ('succeeded', 'failed')
                        THEN 1 ELSE attempt END,
             attempt = 0,
             claimed_by = NULL, lease_expires_at = NULL,
             output = NULL, log = NULL, log_fence = NULL, log_gen = NULL, finished_at = NULL, scheduled_at = $1, version = version + 1,
             checkpoint_uri = NULL, checkpoint_marker = NULL,
             external_kind = NULL, external_handle = NULL, external_endpoint = NULL,
             external_deadline_at = NULL, next_poll_at = NULL,
             external_epoch = external_epoch + 1,
             fault_class = NULL, fault_detail = NULL, fault_confidence = NULL,
             sub_run_id = NULL, wake_at = NULL, wait_url = NULL,
             wait_dataset = NULL, wait_dataset_cursor = NULL,
             decided_by = NULL, decision_comment = NULL, cache_hit = FALSE,
             reset_by = $3
         WHERE run_id = $2 AND fanout_parent IS NULL
           AND status IN ('succeeded', 'failed', 'skipped', 'cancelled')",
    )
    .bind(&now)
    .bind(run_id)
    .bind(who)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    // Each task waits for each of its dependencies again, all of them pending.
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
    .execute(&mut **tx)
    .await?;

    insert_run_again(tx, run_id, by, reset, None, &now).await?;
    notify(&mut **tx, run_id).await?;
    Ok(Some(reset))
}

/// Whether a task with `task_id` exists within `run_id`. Used by the ops API to
/// tell an unknown-task `404` apart from a not-clearable `409` on the error path.
#[cfg(feature = "ops")]
pub async fn task_exists(pool: &Pool, run_id: &str, task_id: &str) -> Result<bool> {
    // `1::bigint` for the same reason as `advance_ready_tasks` above: a bare
    // literal is INT4 and will not decode into i64 once the row exists.
    let found: Option<i64> =
        sqlx::query_scalar("SELECT 1::bigint FROM task_runs WHERE id = $1 AND run_id = $2")
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
/// [`MaxActiveRunsReached`](crate::models::MaxActiveRunsReached) if the run had
/// ended and its concurrency key is full: then nothing is written. The attempts
/// it replaces are kept and an event is written (see "Running again", above).
#[cfg(feature = "ops")]
pub async fn clear_task_with_downstream(
    pool: &Pool,
    run_id: &str,
    task_id: &str,
) -> Result<Option<u64>> {
    clear_task_with_downstream_by(pool, run_id, task_id, None).await
}

/// [`clear_task_with_downstream`], saying who asked for it (`reset_by`), as
/// [`retry_task_from_ui_by`] does.
#[cfg(feature = "ops")]
pub async fn clear_task_with_downstream_by(
    pool: &Pool,
    run_id: &str,
    task_id: &str,
    who: Option<&str>,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let cap = lock_run_cap(&mut tx, run_id).await?;

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
    keep_replaced(
        &mut tx,
        Reset::Cone { run_id, task_id },
        crate::models::RunAgain::Clear.reason(),
        &now,
    )
    .await?;

    // Reset the target + its transitive downstream cone (terminal tasks only).
    let reset = sqlx::query(
        "WITH RECURSIVE cone(id) AS (
             SELECT id FROM task_runs WHERE id = $1 AND run_id = $2
             UNION
             SELECT td.dependent_id FROM task_dependencies td
             JOIN cone c ON td.dependency_id = c.id
         )
         UPDATE task_runs
         SET status = 'pending',
             -- A decided gate's decision counts as an attempt (`DECIDED_GATE`).
             attempt_base = attempt_base
                 + CASE WHEN attempt = 0 AND is_approval = 1 AND status IN ('succeeded', 'failed')
                        THEN 1 ELSE attempt END,
             attempt = 0,
             claimed_by = NULL, lease_expires_at = NULL,
             output = NULL, log = NULL, log_fence = NULL, log_gen = NULL, finished_at = NULL, scheduled_at = $3, version = version + 1,
             checkpoint_uri = NULL, checkpoint_marker = NULL,
             -- …and for the same reason a cleared deferred row drops its remote
             -- handle and takes a fresh epoch: the next submit must start a new
             -- job rather than adopt the one this row already ran.
             external_kind = NULL, external_handle = NULL, external_endpoint = NULL,
             external_deadline_at = NULL, next_poll_at = NULL,
             external_epoch = external_epoch + 1,
             -- Same reason as `retry_task` and `rerun_from_failed`: a cleared
             -- row is not a failed row, so it must not carry a failed row's
             -- verdict into whatever happens next.
             fault_class = NULL, fault_detail = NULL, fault_confidence = NULL,
             -- What parked the row last time, and who decided it: a cleared
             -- row is parked again, and a gate asks again, from nothing. Left,
             -- a sweep could resolve the row from its last child run or its
             -- last deadline between its claim and its park.
             sub_run_id = NULL, wake_at = NULL, wait_url = NULL,
             wait_dataset = NULL, wait_dataset_cursor = NULL,
             decided_by = NULL, decision_comment = NULL, cache_hit = FALSE,
             reset_by = $4
         WHERE id IN (SELECT id FROM cone)
           AND status IN ('succeeded', 'failed', 'skipped', 'cancelled')",
    )
    .bind(task_id)
    .bind(run_id)
    .bind(&now)
    .bind(who)
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
    let rearmed = sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL, output = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END
         WHERE id = $1 AND status IN ('succeeded', 'failed', 'cancelled')",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if rearmed == 1 {
        if let Some(cap) = &cap {
            refuse_if_full(&mut tx, run_id, cap).await?;
        }
        rearm_deadlines(&mut tx, run_id).await?;
    }

    insert_run_again(&mut tx, run_id, crate::models::RunAgain::Clear, reset, Some(task_id), &now).await?;
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
         -- A paused or retired workflow does not fire, and this is the only
         -- place that can enforce it: a state the scheduler ignores is a label,
         -- not a control. Deliberately separate from s.enabled, which disables
         -- one schedule; this stops the workflow however many schedules it has.
         WHERE w.state = 'active'
           AND s.enabled = 1
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
         -- Same gate as claim_due_schedules: a paused workflow must not catch
         -- up either, or pausing would merely defer the backlog rather than
         -- stop it.
         WHERE w.state = 'active' AND s.enabled = 1 AND s.catchup = 1",
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

/// Tasks currently in flight across every active run.
///
/// The companion to [`count_active_runs`], and the reason it exists: admission
/// counts RUNS, so one run containing 100k tasks and one containing four are
/// both "1". A fleet sitting comfortably under `MAX_INFLIGHT_RUNS` can be far
/// past what the scheduler and the datastore can carry, because the unit the
/// cap measures is not the unit that costs anything.
///
/// `('pending', 'ready', 'running')` mirrors the in-flight set used elsewhere in
/// this module. `awaiting_approval` is deliberately excluded: a task parked on a
/// human is not scheduler pressure, and counting it would let one forgotten
/// approval close the valve on everything else.
pub async fn count_active_tasks(pool: &Pool) -> Result<i64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_runs WHERE status IN ('pending', 'ready', 'running')",
    )
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
///
/// It joins every running run to all its tasks, so its cost grows with what
/// is running. A reconcile loop that knows which runs changed asks for those
/// alone ([`reap_woken`]), and covers the rest a slice at a time
/// ([`reap_slice`]).
pub async fn reap_completed_runs(pool: &Pool) -> Result<Vec<(String, RunStatus)>> {
    reap(pool, None).await
}

/// Finalize the runs a reconcile loop was told of, where their tasks are all
/// terminal: every running run for [`Woken::All`], and for [`Woken::Runs`]
/// those alone, by their keys, whatever else is running.
pub async fn reap_woken(pool: &Pool, woken: &Woken) -> Result<Vec<(String, RunStatus)>> {
    match woken {
        Woken::All => reap(pool, None).await,
        Woken::Runs(runs) if runs.is_empty() => Ok(Vec::new()),
        Woken::Runs(runs) => reap(pool, Some(runs)).await,
    }
}

/// Finalize what is complete among the next `limit` running runs after
/// `after`, in the order of their ids, and say where the next slice starts:
/// `None` when this one reached the end, so the next begins again.
///
/// The safety net under [`reap_woken`]: a run whose last task ended with a
/// notification nobody took is finalized when its slice comes round. A slice
/// is bounded, so a pass costs the same whatever is running, and passes cover
/// every running run in turn.
pub async fn reap_slice(
    pool: &Pool,
    after: Option<&str>,
    limit: i64,
) -> Result<(Vec<(String, RunStatus)>, Option<String>)> {
    let runs: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM workflow_runs
          WHERE status = 'running' AND stopping_as IS NULL AND id > $1
          ORDER BY id
          LIMIT $2",
    )
    .bind(after.unwrap_or(""))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let next = if (runs.len() as i64) < limit {
        None
    } else {
        runs.last().cloned()
    };
    if runs.is_empty() {
        return Ok((Vec::new(), None));
    }
    Ok((reap(pool, Some(&runs)).await?, next))
}

/// The reap itself: of every running run, or of `only` these.
async fn reap(pool: &Pool, only: Option<&[String]>) -> Result<Vec<(String, RunStatus)>> {
    // Group by (status, allow_failure) so a `failed` task with allow_failure=1
    // counts as terminal but does not fail the run (fast-win #11).
    let rows = match only {
        None => {
            sqlx::query(
                "SELECT wr.id AS run_id, tr.status AS status, tr.allow_failure AS allow_failure, COUNT(*) AS cnt
                 FROM workflow_runs wr
                 JOIN task_runs tr ON tr.run_id = wr.id
                 WHERE wr.status = 'running' AND wr.stopping_as IS NULL
                 GROUP BY wr.id, tr.status, tr.allow_failure",
            )
            .fetch_all(pool)
            .await?
        }
        // The same question of the named runs alone: each is found by its
        // key, and its tasks by their run's index.
        Some(runs) => {
            sqlx::query(
                "SELECT wr.id AS run_id, tr.status AS status, tr.allow_failure AS allow_failure, COUNT(*) AS cnt
                 FROM workflow_runs wr
                 JOIN task_runs tr ON tr.run_id = wr.id
                 WHERE wr.id = ANY($1) AND wr.status = 'running' AND wr.stopping_as IS NULL
                 GROUP BY wr.id, tr.status, tr.allow_failure",
            )
            .bind(runs)
            .fetch_all(pool)
            .await?
        }
    };

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
    // A run with no task at all (each one a `when:` of its parameters left
    // out) has nothing to wait for: it is complete, and succeeded. A run's
    // tasks are made in the transaction that makes it, so none is a run
    // whose tasks are yet to come. One that waits for its turn at a full cap
    // (`when_full: queue`) ends once it is let in, in the order it came.
    let empty: Vec<String> = match only {
        None => {
            sqlx::query_scalar(
                "SELECT wr.id FROM workflow_runs wr
                 WHERE wr.status = 'running' AND wr.stopping_as IS NULL
                   AND (wr.queued_at IS NULL OR wr.admitted_at IS NOT NULL)
                   AND NOT EXISTS (SELECT 1 FROM task_runs tr WHERE tr.run_id = wr.id)",
            )
            .fetch_all(pool)
            .await?
        }
        Some(only) => {
            sqlx::query_scalar(
                "SELECT wr.id FROM workflow_runs wr
                 WHERE wr.id = ANY($1) AND wr.status = 'running' AND wr.stopping_as IS NULL
                   AND (wr.queued_at IS NULL OR wr.admitted_at IS NOT NULL)
                   AND NOT EXISTS (SELECT 1 FROM task_runs tr WHERE tr.run_id = wr.id)",
            )
            .bind(only)
            .fetch_all(pool)
            .await?
        }
    };
    for run_id in empty {
        runs.entry(run_id).or_insert(Agg { total: 0, terminal: 0, failed: 0 });
    }

    let now = chrono::Utc::now().to_rfc3339();
    let mut finalized = Vec::new();
    for (run_id, agg) in runs {
        if agg.terminal < agg.total {
            continue;
        }
        let (status, status_str) = if agg.failed > 0 {
            (RunStatus::Failed, "failed")
        } else {
            (RunStatus::Succeeded, "succeeded")
        };
        // Finalize the run and append its outbox event in ONE transaction, so the
        // event exists iff the finalization commits (transactional outbox).
        //
        // `completion_pending_at` is cleared because the end-of-run work is
        // this end's event's. A stamp can only be here if a run a release
        // before 10.7 ended outside the reaper was rerun before any tick
        // claimed it; that end is superseded by this one. `start_pending_at` is cleared because
        // a start never announced is moot once the run has ended.
        let mut tx = pool.begin().await?;
        let affected = sqlx::query(
            "UPDATE workflow_runs
             SET status = $1, finished_at = $2, completion_pending_at = NULL, start_pending_at = NULL
             WHERE id = $3 AND status = 'running'",
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
            insert_run_completed(&mut tx, &run_id, status_str, &now).await?;
            tx.commit().await?;
            finalized.push((run_id, status));
        } else {
            // Another reaper won the finalize; nothing to emit.
            tx.rollback().await?;
        }
    }
    Ok(finalized)
}

/// Append `run_id`'s `run.completed` event to the outbox inside `tx`, the
/// transaction that ends the run, so the event exists iff the end commits.
/// Every path that ends a run writes it: the reaper, [`cancel_run`] and
/// [`cancel_overdue_runs`]. The payload is `{"run_id", "status"}` on all three.
async fn insert_run_completed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    status: &str,
    now: &str,
) -> Result<()> {
    let payload = serde_json::json!({ "run_id": run_id, "status": status }).to_string();
    sqlx::query(
        "INSERT INTO event_outbox
           (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
         VALUES ($1, $2, 'run.completed', $3, 'pending', 0, $4, $5)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(run_id)
    .bind(&payload)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Claim up to `limit` runs that a release before 10.7 ended outside the
/// reaper and that still owe their end-of-run work: cancelled through either
/// API, or failed by the `run_timeout_secs` sweep, which stamped
/// `completion_pending_at`. Nothing stamps a run now: its `run.completed`
/// event is owed to the engine's end-of-run sinks. The claim clears the stamp, and `SKIP LOCKED` lets replicas claim side by side, so
/// each run is returned to exactly one caller, which then runs the work the
/// reaper's own process runs for a run that finished.
///
/// A stamped run that is `running` again (rerun before any tick claimed it) is
/// left alone: its end was superseded, and its next end is owed instead.
pub async fn claim_pending_completions(
    pool: &Pool,
    limit: i64,
) -> Result<Vec<(String, RunStatus)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "UPDATE workflow_runs SET completion_pending_at = NULL
         WHERE id IN (
             SELECT id FROM workflow_runs
             WHERE completion_pending_at IS NOT NULL
               AND status IN ('succeeded', 'failed', 'cancelled')
             ORDER BY completion_pending_at
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         RETURNING id, status",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(id, status)| Ok((id, status.parse()?)))
        .collect()
}

/// Claim up to `limit` running runs whose start is still to be announced:
/// created with a `notify.git` block (`start_pending_at`), and no `pending`
/// commit status posted for them yet. The claim clears the stamp, and `SKIP
/// LOCKED` lets replicas claim side by side, so each run is announced by
/// exactly one caller. A run that has already ended is never returned: its end
/// cleared the stamp.
pub async fn claim_pending_starts(pool: &Pool, limit: i64) -> Result<Vec<String>> {
    let ids: Vec<String> = sqlx::query_scalar(
        "UPDATE workflow_runs SET start_pending_at = NULL
         WHERE id IN (
             SELECT id FROM workflow_runs
             WHERE start_pending_at IS NOT NULL AND status = 'running'
             ORDER BY start_pending_at
             LIMIT $1
             FOR UPDATE SKIP LOCKED
         )
         RETURNING id",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(ids)
}

/// A run's status, or `None` when there is no such run.
pub async fn run_status(pool: &Pool, run_id: &str) -> Result<Option<RunStatus>> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(pool)
            .await?;
    status.map(|s| s.parse()).transpose()
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

/// A task's name and attempt, with its workflow's name where its run has
/// one: what a build output's provenance says made it. The attempt is
/// numbered by the task's count for its life (`attempt_base + attempt`), as
/// its kept attempts are: a task that runs again after a rerun or a clear
/// does not give a second build the number of the first.
pub async fn task_in_workflow(
    pool: &Pool,
    task_id: &str,
) -> Result<Option<(String, i64, Option<String>)>> {
    let row: Option<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT tr.name, tr.attempt_base + tr.attempt, d.name FROM task_runs tr
         LEFT JOIN workflow_runs wr ON wr.id = tr.run_id
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE tr.id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
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

/// How many tasks `run_id` has: every row of it, as it is now. A run grows
/// where a task fans out over what another printed.
pub async fn run_task_count(pool: &Pool, run_id: &str) -> Result<i64> {
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await?;
    Ok(tasks)
}

/// What started `run_id`, as it was recorded when the run was made
/// (`run_triggers`). `None` for a run with no record: one made before every
/// run had one, or a run that does not exist.
pub async fn run_trigger(pool: &Pool, run_id: &str) -> Result<Option<crate::models::RunTrigger>> {
    #[allow(clippy::type_complexity)]
    let row: Option<(
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT kind, principal, actor, trust, source, forge_host, repo, ref, sha, base_sha, pr, event_id
         FROM run_triggers WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(kind, principal, actor, trust, source, forge_host, repo, git_ref, sha, base_sha, pr, event_id)| {
            crate::models::RunTrigger {
                kind,
                principal,
                actor,
                trust,
                source,
                forge_host,
                repo,
                git_ref,
                sha,
                base_sha,
                pr,
                event_id,
            }
        },
    ))
}

/// The parameters `run_id` was created with: the spec's defaults with the
/// caller's overrides merged over them (`DagGraph::parameters`). `None` when
/// the run does not exist, or predates the column, so a caller falls back to
/// the spec's own defaults, which is all such a run ever had.
pub async fn run_parameters(
    pool: &Pool,
    run_id: &str,
) -> Result<Option<std::collections::BTreeMap<String, String>>> {
    let raw: Option<Option<String>> =
        sqlx::query_scalar("SELECT parameters FROM workflow_runs WHERE id = $1")
            .bind(run_id)
            .fetch_optional(pool)
            .await?;
    match raw.flatten() {
        Some(json) => Ok(Some(serde_json::from_str(&json)?)),
        None => Ok(None),
    }
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

/// The policy of the environment `name` ([`crate::envpolicy`]): which runs
/// may use it. `None` where it has none, and where there is no such
/// environment: both have the default policy, which admits `trusted` runs
/// alone. A stored policy that cannot be read is an error, never "none": a
/// row someone wrote to close an environment must not read as an open one.
pub async fn environment_policy(
    pool: &Pool,
    name: &str,
) -> Result<Option<crate::envpolicy::EnvironmentPolicy>> {
    type Row = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT p.allowed_trust, p.allowed_refs, p.allowed_pipelines,
                p.allowed_identity, p.approvers
           FROM environment_policies p
           JOIN environments e ON e.id = p.environment_id
          WHERE e.name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?;
    let Some((trust, refs, pipelines, identity, approvers)) = row else {
        return Ok(None);
    };
    let list = |field: &str, json: &str| -> Result<Vec<String>> {
        serde_json::from_str(json).map_err(|e| {
            anyhow::anyhow!("environment '{name}': its policy's {field} is not a list: {e}")
        })
    };
    Ok(Some(crate::envpolicy::EnvironmentPolicy {
        allowed_trust: list("allowed_trust", &trust)?,
        allowed_refs: refs
            .as_deref()
            .map(|j| list("allowed_refs", j))
            .transpose()?,
        allowed_pipelines: pipelines
            .as_deref()
            .map(|j| list("allowed_pipelines", j))
            .transpose()?,
        allowed_identity: identity
            .as_deref()
            .map(|j| list("allowed_identity", j))
            .transpose()?
            .unwrap_or_default(),
        approvers: approvers
            .as_deref()
            .map(|j| list("approvers", j))
            .transpose()?
            .unwrap_or_default(),
    }))
}

// ── Transactional outbox: drain API, a sink at a time ─────────────────────────
//
// Each consumer of the outbox is a sink ([`super::OutboxSink`])
// with its own delivery state of each event, in `outbox_deliveries`: a sink
// is first handed an event by [`outbox_enroll`], then claims, leases and
// marks its own row of it exactly as the single-consumer outbox did its row
// of `event_outbox`. What one sink does with an event is no other's concern.

/// Name `sink` a consumer of the outbox, if it is not one yet: from now on it
/// is handed every event written, of its types, and where it
/// [`adopts_legacy`](super::OutboxSink::adopts_legacy) also those the
/// single-consumer outbox held pending when this release's migration ran. A sink is also registered by
/// its first enrolment; registering one first is how a process says that the
/// events written from here on are its, before it drains any of them.
pub async fn register_outbox_sink(pool: &Pool, sink: &super::OutboxSink<'_>) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    if sink.adopts_legacy {
        // What the single-consumer outbox held pending when this release's
        // migration ran (`outbox_epoch`), handed before the sink is named,
        // and only while it is not: a replica that registers it again finds
        // it named and adopts nothing.
        sqlx::query(
            "INSERT INTO outbox_deliveries (outbox_id, sink, status, attempts, next_attempt_at)
             SELECT e.id, $1, 'pending', 0, e.created_at
             FROM event_outbox e, outbox_epoch w
             WHERE e.status = 'pending' AND e.created_at < w.per_sink_since
               AND ($2::jsonb = '[]'::jsonb OR e.event_type IN (SELECT jsonb_array_elements_text($2::jsonb)))
               AND NOT EXISTS (SELECT 1 FROM outbox_sinks s WHERE s.sink = $1)
             ON CONFLICT (outbox_id, sink) DO NOTHING",
        )
        .bind(sink.name)
        .bind(super::outbox_types_json(sink))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO outbox_sinks (sink, since, cursor, registered_at) VALUES ($1, $2, $2, $2)
         ON CONFLICT (sink) DO NOTHING",
    )
    .bind(sink.name)
    .bind(&now)
    .execute(pool)
    .await?;
    if sink.wants_tasks() {
        renew_task_events(pool, sink, None).await?;
    }
    Ok(())
}

/// Have task events written for `sink` a day more (`wants_tasks_until`),
/// where it would otherwise run out within the day; `until` what it is now,
/// where read.
async fn renew_task_events(
    pool: &Pool,
    sink: &super::OutboxSink<'_>,
    until: Option<&str>,
) -> Result<()> {
    if let Some(next) = super::outbox_task_events_renew(until, chrono::Utc::now()) {
        sqlx::query(
            "UPDATE outbox_sinks SET wants_tasks_until = $1
             WHERE sink = $2 AND (wants_tasks_until IS NULL OR wants_tasks_until < $1)",
        )
        .bind(&next)
        .bind(sink.name)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Hand `sink` the events written since it last looked, up to
/// [`super::OUTBOX_ENROLL_ROWS`]: a `pending` row of `outbox_deliveries` for
/// each, due at once. Returns how many. Replicas enrol side by side: an event
/// is handed to a sink once, whichever enrols it.
///
/// It looks from its cursor less [`super::OUTBOX_LATE_COMMIT_SECS`], for an
/// event written with an earlier time than one already handed but committed
/// after it, so that a sink never reads the outbox back to its beginning.
pub async fn outbox_enroll(pool: &Pool, sink: &super::OutboxSink<'_>) -> Result<u64> {
    let now = chrono::Utc::now();
    let (since, cursor) = outbox_sink_position(pool, sink).await?;
    let from = super::outbox_enrol_from(&since, &cursor);
    // The cursor goes by what was found, not by what this call inserted: a
    // replica enrolling the same sink may have inserted some or all of them
    // first, and a batch it filled is still a full one.
    let (found, latest, enrolled): (i64, Option<String>, i64) = sqlx::query_as(
        "WITH found AS (
             SELECT e.id, e.created_at FROM event_outbox e
             WHERE e.status = 'pending' AND e.created_at >= $2
               AND (($3::jsonb = '[]'::jsonb AND e.event_type NOT LIKE 'task.%')
                    OR e.event_type IN (SELECT jsonb_array_elements_text($3::jsonb)))
               AND NOT EXISTS (SELECT 1 FROM outbox_deliveries d WHERE d.outbox_id = e.id AND d.sink = $1)
             ORDER BY e.created_at, e.id
             LIMIT $4
         ), added AS (
             INSERT INTO outbox_deliveries (outbox_id, sink, status, attempts, next_attempt_at)
             SELECT id, $1, 'pending', 0, created_at FROM found
             ON CONFLICT (outbox_id, sink) DO NOTHING
             RETURNING 1
         )
         SELECT (SELECT COUNT(*) FROM found), (SELECT MAX(created_at) FROM found),
                (SELECT COUNT(*) FROM added)",
    )
    .bind(sink.name)
    .bind(&from)
    .bind(super::outbox_types_json(sink))
    .bind(super::OUTBOX_ENROLL_ROWS)
    .fetch_one(pool)
    .await?;
    let n = enrolled;
    let next = super::outbox_next_cursor(latest, found, now);
    if let Some(next) = next.filter(|next| super::outbox_cursor_moves(&cursor, next, found)) {
        sqlx::query("UPDATE outbox_sinks SET cursor = $1 WHERE sink = $2 AND cursor < $1")
            .bind(&next)
            .bind(sink.name)
            .execute(pool)
            .await?;
    }
    Ok(n as u64)
}

/// Where `sink` enrols from, `(since, cursor)`, registering it first where
/// it is new.
async fn outbox_sink_position(
    pool: &Pool,
    sink: &super::OutboxSink<'_>,
) -> Result<(String, String)> {
    let read = || {
        sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT since, cursor, wants_tasks_until FROM outbox_sinks WHERE sink = $1",
        )
        .bind(sink.name)
        .fetch_optional(pool)
    };
    if let Some((since, cursor, until)) = read().await? {
        // A sink that asks for task events is written them while it claims.
        if sink.wants_tasks() {
            renew_task_events(pool, sink, until.as_deref()).await?;
        }
        return Ok((since, cursor));
    }
    register_outbox_sink(pool, sink).await?;
    let (since, cursor, _) = read().await?.expect("a sink just registered is there");
    Ok((since, cursor))
}

/// Enrol `sink` ([`outbox_enroll`]), then claim up to `limit` of its due,
/// pending events with `FOR UPDATE SKIP LOCKED` (coordination-free, like task
/// claiming), deferring each by `lease_secs` so a concurrent worker of the
/// same sink won't take the same event mid-delivery. At-least-once: a crashed
/// worker's lease lapses and the event is claimed again.
pub async fn claim_outbox_batch(
    pool: &Pool,
    sink: &super::OutboxSink<'_>,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<crate::models::OutboxEvent>> {
    Ok(claim_deliveries(pool, sink, limit, lease_secs)
        .await?
        .into_iter()
        .map(|row| row.event)
        .collect())
}

async fn claim_deliveries(
    pool: &Pool,
    sink: &super::OutboxSink<'_>,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<OutboxUplinkRow>> {
    outbox_enroll(pool, sink).await?;
    let now = chrono::Utc::now();
    let now_s = now.to_rfc3339();
    let lease_until =
        (now + chrono::TimeDelta::seconds(lease_secs.clamp(0, MAX_SPOOL_SECS))).to_rfc3339();

    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT e.id, e.run_id, e.event_type, e.payload, d.attempts, e.created_at
         FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
         WHERE d.sink = $1 AND d.status = 'pending' AND d.next_attempt_at <= $2
         ORDER BY d.next_attempt_at LIMIT $3
         FOR UPDATE OF d SKIP LOCKED",
    )
    .bind(sink.name)
    .bind(&now_s)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(OutboxUplinkRow {
            event: crate::models::OutboxEvent {
                id: row.try_get("id")?,
                run_id: row.try_get("run_id")?,
                event_type: row.try_get("event_type")?,
                payload: row.try_get("payload")?,
                attempts: row.try_get("attempts")?,
                created_at: row.try_get("created_at")?,
            },
            created_at: row.try_get("created_at")?,
        });
    }
    let ids: Vec<&str> = out.iter().map(|r| r.event.id.as_str()).collect();
    sqlx::query(
        "UPDATE outbox_deliveries SET next_attempt_at = $1 WHERE sink = $2 AND outbox_id = ANY($3)",
    )
    .bind(&lease_until)
    .bind(sink.name)
    .bind(&ids)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(out)
}

/// Delete up to `limit` task events (`task.*`) written before `before`
/// (RFC 3339), with every sink's delivery of them, oldest first. Returns how
/// many. A task's events are notices for a look to follow, written for each
/// attempt of each task; past [`super::OUTBOX_TASK_EVENTS_KEPT_SECS`] none is
/// worth keeping, and nothing else ever deletes from the outbox.
pub async fn prune_task_events(pool: &Pool, before: &str, limit: i64) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM event_outbox
         WHERE created_at < $1 AND event_type LIKE 'task.%'
         ORDER BY created_at LIMIT $2",
    )
    .bind(before)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    if ids.is_empty() {
        return Ok(0);
    }
    sqlx::query("DELETE FROM outbox_deliveries WHERE outbox_id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    let n = sqlx::query("DELETE FROM event_outbox WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    Ok(n)
}

/// Mark the event `id` delivered to `sink`.
pub async fn mark_outbox_delivered(pool: &Pool, sink: &str, id: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "UPDATE outbox_deliveries SET status = 'delivered', delivered_at = $1
         WHERE sink = $2 AND outbox_id = $3 AND status = 'pending'",
    )
    .bind(&now)
    .bind(sink)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delivery to `sink` failed but is retryable: bump attempts, record the
/// error, and set the next eligible time (`retry_at`, caller computes the
/// backoff).
pub async fn mark_outbox_failed(
    pool: &Pool,
    sink: &str,
    id: &str,
    error: &str,
    retry_at: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE outbox_deliveries SET attempts = attempts + 1, last_error = $1, next_attempt_at = $2
         WHERE sink = $3 AND outbox_id = $4 AND status = 'pending'",
    )
    .bind(error)
    .bind(retry_at)
    .bind(sink)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Delivery to `sink` exhausted its retries: park its row of the event as
/// `dead` (the broker-DLQ analog). Other sinks' rows of it are untouched.
pub async fn mark_outbox_dead(pool: &Pool, sink: &str, id: &str, error: &str) -> Result<()> {
    sqlx::query(
        "UPDATE outbox_deliveries SET status = 'dead', attempts = attempts + 1, last_error = $1
         WHERE sink = $2 AND outbox_id = $3 AND status = 'pending'",
    )
    .bind(error)
    .bind(sink)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// What each sink has of the outbox: `(sink, pending, dead, last delivered)`,
/// by sink name.
pub async fn outbox_sink_counts(pool: &Pool) -> Result<Vec<(String, i64, i64, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT s.sink,
                COUNT(d.outbox_id) FILTER (WHERE d.status = 'pending'),
                COUNT(d.outbox_id) FILTER (WHERE d.status = 'dead'),
                MAX(d.delivered_at) FILTER (WHERE d.status = 'delivered')
         FROM outbox_sinks s LEFT JOIN outbox_deliveries d ON d.sink = s.sink
         GROUP BY s.sink ORDER BY s.sink",
    )
    .fetch_all(pool)
    .await?)
}

// ── Transactional outbox: spool bounds for the fleet uplink ───────────────────
//
// A machine on a metered, duty-cycled or absent link fills its outbox far
// faster than it drains it, and the disk it fills is usually a small flash
// card. The functions below are what keeps that spool bounded: measure it
// ([`outbox_pending_bytes`], [`outbox_oldest_pending_age_secs`]), drop what no
// longer fits ([`shed_outbox`]), and claim a batch carrying the timestamps the
// fleet protocol needs ([`claim_outbox_batch_for_uplink`]). The engine itself
// never calls any of them — the fleet-link worker that ships with dagron
// a feature-on build does, and this is the datastore half of that contract, kept here
// so both backends stay identical and the SQL lives beside the outbox it
// touches.
//
// Each is of one sink's spool: what was handed to it ([`outbox_enroll`]) and
// it has not yet delivered. Signature-for-signature identical to the SQLite
// backend's region; the tests live there, because a unit runs on SQLite and
// that is where the behaviour is exercised.

/// The largest number of seconds this region will hand to
/// [`chrono::TimeDelta::seconds`]. That constructor panics outside its
/// millisecond range, and every duration here arrives from an operator's
/// environment, so each one is clamped *before* the arithmetic rather than
/// trusted to be sane. A hundred years is far past any spool budget worth
/// configuring and far short of the panic.
const MAX_SPOOL_SECS: i64 = 100 * 366 * 24 * 3600;

/// The status a sink's row shed by [`shed_outbox`] carries.
/// `outbox_deliveries.status` has no `CHECK` constraint; nothing re-claims
/// such a row, because every claim filters `pending`.
pub const OUTBOX_STATUS_SHED: &str = "shed";

/// One claimed outbox row, plus the wall-clock time the engine wrote it.
///
/// [`claim_outbox_batch`] deliberately does not carry `created_at`: a webhook
/// receives an event moments after it happened, so the receiver's own clock is
/// close enough. A unit reconnecting after a week offline is the opposite case
/// — what matters is when the machine produced the event, not when the link
/// came back — so the uplink claim returns the column too and the fleet plane
/// stores it verbatim.
#[derive(Debug, Clone)]
pub struct OutboxUplinkRow {
    pub event: crate::models::OutboxEvent,
    /// RFC 3339, exactly as the engine that emitted the event wrote it.
    pub created_at: String,
}

/// [`claim_outbox_batch`] with `created_at` attached (see [`OutboxUplinkRow`]).
///
/// Identical lease discipline — `FOR UPDATE SKIP LOCKED` plus a per-row lease —
/// so a worker that dies mid-upload simply lets the lease lapse and the rows are
/// re-claimed. That makes delivery at-least-once, which is safe because the
/// plane deduplicates on the row id: the same property the webhook drain relies
/// on.
pub async fn claim_outbox_batch_for_uplink(
    pool: &Pool,
    sink: &super::OutboxSink<'_>,
    limit: i64,
    lease_secs: i64,
) -> Result<Vec<OutboxUplinkRow>> {
    claim_deliveries(pool, sink, limit, lease_secs).await
}

/// Bytes of event payload `sink` still has to deliver (`pending`).
///
/// The payload is the row's whole variable cost — ids, types and timestamps are
/// bounded and small — so it is what a byte budget is measured against.
/// `octet_length`, not `length`: the latter counts characters, which
/// under-counts anything non-ASCII, and the budget is protecting a disk.
pub async fn outbox_pending_bytes(pool: &Pool, sink: &str) -> Result<i64> {
    // SUM over no rows is NULL, not 0.
    let total: Option<i64> = sqlx::query_scalar(
        "SELECT SUM(octet_length(e.payload))::bigint
         FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
         WHERE d.sink = $1 AND d.status = 'pending'",
    )
    .bind(sink)
    .fetch_one(pool)
    .await?;
    Ok(total.unwrap_or(0))
}

/// How long the oldest event `sink` has still to deliver has been waiting, in
/// seconds (`0` when its spool is empty).
///
/// `created_at` is written as `chrono::Utc::now().to_rfc3339()` everywhere the
/// engine emits an event, and that spelling is fixed-width and always UTC, so
/// the lexicographic `MIN` the datastore computes is also the chronological
/// one. A row whose timestamp does not parse reads as age `0` rather than as an
/// error: an unparseable clock is not a reason to refuse to measure the spool.
pub async fn outbox_oldest_pending_age_secs(pool: &Pool, sink: &str) -> Result<i64> {
    let oldest: Option<String> = sqlx::query_scalar(
        "SELECT MIN(e.created_at)
         FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
         WHERE d.sink = $1 AND d.status = 'pending'",
    )
    .bind(sink)
    .fetch_one(pool)
    .await?;
    let Some(oldest) = oldest else {
        return Ok(0);
    };
    let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&oldest) else {
        tracing::debug!(created_at = %oldest, "outbox row has an unparseable created_at");
        return Ok(0);
    };
    Ok((chrono::Utc::now() - ts.with_timezone(&chrono::Utc)).num_seconds().max(0))
}

/// Bring `sink`'s pending spool back inside its budget, parking what no
/// longer fits as [`OUTBOX_STATUS_SHED`]. Returns how many rows were shed.
///
/// Two budgets, applied in that order; `0` disables either one:
///
/// * **age** — `max_age_secs` sheds every pending row written longer ago than
///   that. Telemetry that has been undeliverable for a week is worth less than
///   the flash it is sitting on, and shedding it is what keeps the *recent*
///   events deliverable.
/// * **bytes** — `max_bytes` sheds until [`outbox_pending_bytes`] is back at or
///   under the budget. `policy` picks the victims: `"largest"` takes the biggest
///   payloads first (fewest rows lost per byte reclaimed — right when one
///   oversized capture is the problem), and anything else takes the oldest
///   first, which is the default because the events an operator is watching a
///   machine for are the newest ones.
///
/// Shed rows are never re-claimed (every claim filters `status = 'pending'`),
/// and carry the budget that dropped them in `last_error`, so
/// `SELECT COUNT(*), last_error FROM outbox_deliveries WHERE status = 'shed'` is
/// a readable answer to "what did this disk cost me". They are data loss by
/// design: the alternative is a spool that grows until the datastore itself
/// stops accepting writes, which loses the *run state* too. Only this sink's
/// rows are shed: the event is still every other sink's to deliver.
pub async fn shed_outbox(
    pool: &Pool,
    sink: &str,
    max_bytes: i64,
    max_age_secs: i64,
    policy: &str,
) -> Result<u64> {
    // Victims are taken a page at a time: a spool of a million rows must not
    // materialise in memory to drop the first few of them.
    const PAGE: i64 = 512;

    let due_now = chrono::Utc::now().to_rfc3339();
    // Only rows that are *due* are eligible. A claim leases a row by pushing
    // `next_attempt_at` into the future and leaving `status = 'pending'`, so a
    // row a delivery worker is mid-POST on is indistinguishable from an idle
    // one by status alone. Shedding it would be worse than losing it: the
    // finalizers are `status = 'pending'`-guarded, so `mark_outbox_delivered`
    // would silently match nothing and a successfully delivered event would sit
    // in the spool forever, counted as loss by the audit query. A row in backoff
    // is skipped for the same reason and becomes eligible once it is due again.

    let mut shed: u64 = 0;

    if max_age_secs > 0 {
        let cutoff = (chrono::Utc::now()
            - chrono::TimeDelta::seconds(max_age_secs.clamp(0, MAX_SPOOL_SECS)))
        .to_rfc3339();
        let reason = format!("shed: older than the {max_age_secs}s outbox age budget");
        shed += sqlx::query(
            "UPDATE outbox_deliveries d SET status = 'shed', last_error = $1
             FROM event_outbox e
             WHERE e.id = d.outbox_id AND d.sink = $2 AND d.status = 'pending'
               AND e.created_at < $3 AND d.next_attempt_at <= $4",
        )
        .bind(&reason)
        .bind(sink)
        .bind(&cutoff)
        .bind(&due_now)
        .execute(pool)
        .await?
        .rows_affected();
    }

    if max_bytes > 0 {
        let largest_first = policy.eq_ignore_ascii_case("largest");
        // Two fixed statements rather than an interpolated ORDER BY: a sort key
        // cannot be a bind parameter, so the only safe spelling is one where no
        // caller-supplied string ever reaches the SQL text.
        let sql = if largest_first {
            "SELECT e.id, octet_length(e.payload)::bigint AS bytes
             FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
             WHERE d.sink = $1 AND d.status = 'pending' AND d.next_attempt_at <= $2
             ORDER BY bytes DESC, e.created_at ASC LIMIT $3"
        } else {
            "SELECT e.id, octet_length(e.payload)::bigint AS bytes
             FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
             WHERE d.sink = $1 AND d.status = 'pending' AND d.next_attempt_at <= $2
             ORDER BY e.created_at ASC LIMIT $3"
        };
        let reason = format!(
            "shed: over the {max_bytes}-byte outbox budget ({} first)",
            if largest_first { "largest" } else { "oldest" }
        );
        let mut pending = outbox_pending_bytes(pool, sink).await?;
        loop {
            if pending <= max_bytes {
                break;
            }
            let victims: Vec<(String, i64)> = sqlx::query_as(sql)
                .bind(sink)
                .bind(&due_now)
                .bind(PAGE)
                .fetch_all(pool)
                .await?;
            if victims.is_empty() {
                break;
            }
            let mut progressed = false;
            for (id, bytes) in victims {
                if pending <= max_bytes {
                    break;
                }
                let n = sqlx::query(
                    "UPDATE outbox_deliveries SET status = 'shed', last_error = $1
                     WHERE sink = $2 AND outbox_id = $3 AND status = 'pending'",
                )
                .bind(&reason)
                .bind(sink)
                .bind(&id)
                .execute(pool)
                .await?
                .rows_affected();
                if n == 1 {
                    shed += n;
                    pending -= bytes;
                    progressed = true;
                }
            }
            // A whole page that shed nothing means another writer is racing us;
            // stop rather than spin.
            if !progressed {
                break;
            }
        }
    }

    if shed > 0 {
        tracing::warn!(
            shed,
            sink,
            max_bytes,
            max_age_secs,
            policy,
            "outbox rows shed to stay inside the spool budget — these events are lost"
        );
    }
    Ok(shed)
}

/// The newest run created from exactly these spec bytes at or after `since`
/// (RFC 3339), if there is one.
///
/// The fleet link acknowledges a work item with the `run_id` it became, so the
/// plane can follow a fanned-out run to its outcome. Nothing hands the source
/// that id, though — the ingest actor creates the run and the source's `ack`
/// runs afterwards — so this is the bridge, and it is deliberately a
/// **heuristic**: the spec text is the key, and `since` (the instant the work
/// item was handed out) is what stops an identical spec submitted an hour ago
/// from being reported instead. Two byte-identical specs created in the same
/// instant are indistinguishable here; the ack is best-effort by contract, and
/// the run's own lifecycle events carry the authoritative record.
pub async fn latest_run_for_spec(pool: &Pool, spec: &str, since: &str) -> Result<Option<String>> {
    let id: Option<String> = sqlx::query_scalar(
        "SELECT r.id FROM workflow_runs r
           JOIN workflow_definitions d ON d.id = r.definition_id
          WHERE d.spec = $1 AND r.created_at >= $2
          ORDER BY r.created_at DESC, r.id DESC
          LIMIT 1",
    )
    .bind(spec)
    .bind(since)
    .fetch_optional(pool)
    .await?;
    Ok(id)
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
        // Same tolerance again: only the claim projection selects it, and a
        // reader that does not gets 0 — which is the epoch a row that has never
        // deferred actually has, so the default is the truth rather than a
        // placeholder.
        external_epoch: row.try_get("external_epoch").unwrap_or(0),
        // Park reasons, same tolerance: only the run-detail read selects them.
        wake_at: row.try_get("wake_at").ok().flatten(),
        wait_url: row.try_get("wait_url").ok().flatten(),
        wait_dataset: row.try_get("wait_dataset").ok().flatten(),
        sub_run_id: row.try_get("sub_run_id").ok().flatten(),
        // Fault attribution (040/050), same tolerance: the claim path's lean
        // projection does not select these.
        fault_class: row.try_get("fault_class").ok().flatten(),
        fault_detail: row.try_get("fault_detail").ok().flatten(),
        fault_confidence: row.try_get("fault_confidence").ok().flatten(),
        decided_by: row.try_get("decided_by").ok().flatten(),
        decision_comment: row.try_get("decision_comment").ok().flatten(),
        // Quota wait (051/066), same tolerance: only the run-detail read
        // selects these, already guarded by the waiting rule.
        quota_wait_since: row.try_get("quota_wait_since").ok().flatten(),
        quota_wait_detail: row.try_get("quota_wait_detail").ok().flatten(),
        quota_waits: row.try_get("quota_waits").unwrap_or(0),
        // The stop request (073), same tolerance: the run-detail read and the
        // teardown claim select it.
        stop_requested_at: row.try_get("stop_requested_at").ok().flatten(),
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
                       wr.created_at AS created_at, wr.finished_at AS finished_at,
                       wr.stopping_as AS stopping_as,
                       wr.queued_at AS queued_at, wr.admitted_at AS admitted_at
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
            stopping_as: row.try_get("stopping_as")?,
            queued_at: row.try_get("queued_at")?,
            admitted_at: row.try_get("admitted_at")?,
        });
    }
    Ok(out)
}

/// Fetch one run by id (or `None`). Backs `GET /runs/:id`.
#[cfg(feature = "ops")]
pub async fn get_run(pool: &Pool, run_id: &str) -> Result<Option<crate::models::WorkflowRun>> {
    use crate::models::WorkflowRun;
    let row = sqlx::query(
        "SELECT id, definition_id, status, input, output, created_at, finished_at,
                clock_confidence, clock_offset_ms, clock_source,
                stopping_as, stop_requested_at, queued_at, admitted_at
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
                // Clock confidence (052), tolerant like `row_to_task`'s optional
                // columns — the same `#[sqlx(default)]` contract the SQLite
                // projection gets for free.
                clock_confidence: row.try_get("clock_confidence").ok().flatten(),
                clock_offset_ms: row.try_get("clock_offset_ms").ok().flatten(),
                clock_source: row.try_get("clock_source").ok().flatten(),
                stopping_as: row.try_get("stopping_as").ok().flatten(),
                stop_requested_at: row.try_get("stop_requested_at").ok().flatten(),
                queued_at: row.try_get("queued_at").ok().flatten(),
                admitted_at: row.try_get("admitted_at").ok().flatten(),
            }))
        }
    }
}

/// Stamp every **running** run `drifted` (clock discipline): the detector
/// caught a wall-clock step, or found the clock behind the datastore at boot,
/// so the timestamps these runs will finish under are not evidence. Runs
/// already terminal keep the confidence they were created with — their
/// record is closed, and rewriting it would be the detector inventing
/// history. Returns the number of runs touched. Not `ops`-gated: the lean
/// daemon runs the detector too. Identical contract to the SQLite twin.
pub async fn mark_runs_clock_drifted(pool: &Pool, offset_ms: i64, source: &str) -> Result<u64> {
    let r = sqlx::query(
        "UPDATE workflow_runs
         SET clock_confidence = 'drifted', clock_offset_ms = $1, clock_source = $2
         WHERE status = 'running'",
    )
    .bind(offset_ms)
    .bind(source)
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// `created_at` of the newest run in the datastore (RFC 3339), or `None` on
/// an empty store — the boot plausibility probe: a clock that reads *earlier*
/// than the newest record it is about to append to is wrong, whatever else it
/// says. `MAX` over the text is chronological here because every row is
/// written by `to_rfc3339()` in UTC, whose fixed field widths make lexical
/// order time order.
pub async fn latest_run_created_at(pool: &Pool) -> Result<Option<String>> {
    let latest: Option<String> = sqlx::query_scalar("SELECT MAX(created_at) FROM workflow_runs")
        .fetch_one(pool)
        .await?;
    Ok(latest)
}

/// One task's log for the log views, scoped to its run: `task_runs.log`, else
/// its `output`. `None` when no such task is in the run.
#[cfg(feature = "ops")]
pub async fn task_log(pool: &Pool, run_id: &str, task_id: &str) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> =
        sqlx::query_as("SELECT COALESCE(NULLIF(log, ''), output) FROM task_runs WHERE run_id = $1 AND id = $2")
            .bind(run_id)
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(log,)| log.unwrap_or_default()))
}

/// Each task's log for the log views: stdout and stderr as streamed
/// (`task_runs.log`), else its `output` for rows with no streamed log.
#[cfg(feature = "ops")]
pub async fn task_logs(pool: &Pool, run_id: &str) -> Result<std::collections::HashMap<String, String>> {
    let rows: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT id, COALESCE(NULLIF(log, ''), output) FROM task_runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(id, log)| (id, log.unwrap_or_default())).collect())
}

/// All task rows of a run, ordered by name. Backs `GET /runs/:id`, and the
/// engine's fetch ahead of a run's images, in every build.
pub async fn list_tasks(pool: &Pool, run_id: &str) -> Result<Vec<TaskRun>> {
    let rows = sqlx::query(
        // The quota columns are never cleared, so they are projected through
        // the waiting rule: set while the row waits, NULL / 0 on any other.
        "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at, pool, priority, cache_hit,
                wake_at, wait_url, wait_dataset, sub_run_id,
                decided_by, decision_comment, stop_requested_at,
                CASE WHEN status = 'ready' AND quota_wait_version = version
                     THEN quota_wait_since END AS quota_wait_since,
                CASE WHEN status = 'ready' AND quota_wait_version = version
                     THEN quota_wait_detail END AS quota_wait_detail,
                CASE WHEN status = 'ready' AND quota_wait_version = version
                     THEN quota_waits ELSE 0 END AS quota_waits
         FROM task_runs WHERE run_id = $1 ORDER BY name",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    rows.iter().map(row_to_task).collect()
}

/// Cancel a still-running run. Returns what it did to the run's tasks, or
/// `None` when there was nothing to cancel (no such run, or one already over).
///
/// **A cancel asks.** A task nothing is running for (`pending`, `ready`,
/// `awaiting_approval`, or parked) is `cancelled` here. A task a worker holds
/// is asked to stop (`stop_requested_at`) and stays `running`, with its claim:
/// its engine sees the request at its next sweep and tells the executor, and
/// the row becomes `cancelled` when that worker says the work is gone
/// ([`settle_stopped_task`]). Any other result it reports is not applied.
///
/// **The run ends when its work has.** With no task still stopping, that is
/// now: the run is `cancelled`, its `run.completed` event is written in this
/// transaction, and the rest of the end-of-run work is owed to that event
/// (the engine's end-of-run sinks). Otherwise
/// the run stays `running` with `stopping_as = 'cancelled'`, and all of that
/// happens in the transaction that settles its last stopping task. Until then
/// it holds its concurrency key, and no record says it is over.
///
/// A `pending` status never posted (`start_pending_at`) is dropped: the end is
/// what the commit should show. A second cancel of a run still stopping
/// changes nothing and answers with what is still stopping. dagron-api cancels
/// through this function too, so a cancel from either API is the same one.
#[cfg(feature = "ops")]
pub async fn cancel_run(pool: &Pool, run_id: &str) -> Result<Option<crate::models::Cancelled>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    // The run row first, here and in every statement that ends a stopping
    // task: two of those must not each see the other's task as still stopping.
    let asked = sqlx::query(
        "UPDATE workflow_runs
         SET stopping_as = COALESCE(stopping_as, 'cancelled'),
             stop_requested_at = COALESCE(stop_requested_at, $1),
             start_pending_at = NULL
         WHERE id = $2 AND status = 'running'",
    )
    .bind(&now)
    .bind(run_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if asked == 0 {
        tx.commit().await?;
        return Ok(None);
    }

    let did = stop_run_tasks(&mut tx, run_id, &now).await?;
    finish_stopped_run(&mut tx, run_id, &now).await?;
    notify(&mut *tx, run_id).await?;
    tx.commit().await?;
    Ok(Some(did))
}

/// Enforce run-level deadlines (spec `run_timeout_secs`): every `running` run
/// whose `deadline_at` has passed ends **failed** (deadline exceeded is an
/// error, unlike an operator cancel), and its tasks are stopped as
/// [`cancel_run`] stops them: one nothing runs for is `cancelled` here, one a
/// worker holds is asked to stop. The run is `failed` at once when none is
/// stopping, and otherwise when the last of them has
/// (`stopping_as = 'failed'`).
///
/// Idempotent by construction (a run it has asked is not asked again), and
/// safe to call from every scheduler's reconcile tick without leadership. A
/// run already being cancelled is left to end as cancelled. Returns the runs
/// whose deadline this sweep acted on, each `(run_id, deadline_at)`.
///
/// A run that waits for its workflow's cap is not one of them: its budget is
/// for what it runs, and is counted from when it is let in
/// ([`admit_waiting_runs`]).
pub async fn cancel_overdue_runs(pool: &Pool) -> Result<Vec<(String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let overdue: Vec<(String, String)> = sqlx::query_as(
        "UPDATE workflow_runs
         SET stopping_as = 'failed', stop_requested_at = $1, start_pending_at = NULL,
             output = 'run deadline exceeded (run_timeout_secs)'
         WHERE status = 'running' AND stopping_as IS NULL
           AND deadline_at IS NOT NULL AND deadline_at < $1
           AND (queued_at IS NULL OR admitted_at IS NOT NULL)
         RETURNING id, deadline_at",
    )
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;

    for (run_id, _) in &overdue {
        stop_run_tasks(&mut tx, run_id, &now).await?;
        finish_stopped_run(&mut tx, run_id, &now).await?;
        notify(&mut *tx, run_id).await?;
    }

    tx.commit().await?;
    Ok(overdue)
}

/// Let in the runs that wait for their workflow's cap (`when_full`), where a
/// run before them has ended: oldest first, as many as the cap has room for.
/// Returns the ids of the runs let in.
///
/// A run is let in by taking away the dependency its tasks were held by
/// (`create_run`), in the transaction that writes `admitted_at`: its roots
/// are then `pending` with nothing left to wait for, and the next advance
/// makes them ready. Their `scheduled_at` is now, so what they then wait for a
/// worker is counted from here. So is the run's `run_timeout_secs`: its
/// deadline is moved on by as long as it waited.
///
/// The cap is each waiting run's own, read from the definition it was made
/// with, and a run that cannot be let in stops the ones behind it: runs go in
/// the order they came. A run being stopped holds its place until its work is
/// gone, so a run that took another's place waits for that.
///
/// Safe from every scheduler's tick without leadership: each key is taken
/// under its admission lock, as a new run of it is, and a run is let in by a
/// statement that finds it waiting. Reads a partial index of the runs that
/// wait, which is empty when none does.
pub async fn admit_waiting_runs(pool: &Pool) -> Result<Vec<String>> {
    // The keys whose runs have waited longest first: under an installation's
    // cap, the runs that came first across the installation are let in first.
    let keys: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT d.name, wr.concurrency_key
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
           AND wr.status = 'running' AND wr.stopping_as IS NULL
         GROUP BY d.name, wr.concurrency_key
         ORDER BY MIN(wr.queued_at)",
    )
    .fetch_all(pool)
    .await?;

    let mut admitted = Vec::new();
    for (name, key) in &keys {
        let mut tx = pool.begin().await?;
        let lock_scope = match key {
            Some(key) => format!("{name}\u{1f}{key}"),
            None => name.clone(),
        };
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
            .bind(RUN_ADMISSION_LOCK)
            .bind(&lock_scope)
            .execute(&mut *tx)
            .await?;
        let (in_flight, waiting) = runs_in_flight(&mut tx, name, key.as_deref(), None).await?;
        let mut running = in_flight - waiting;
        // What the installation's cap leaves, taken after the key's lock as a
        // submit takes them, so the two never wait on each other.
        let mut room = installation_room(&mut tx, Behind::Running).await?;
        let next: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT wr.id, d.spec, wr.queued_at, wr.deadline_at
             FROM workflow_runs wr
             JOIN workflow_definitions d ON d.id = wr.definition_id
             WHERE wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
               AND wr.status = 'running' AND wr.stopping_as IS NULL
               AND d.name = $1 AND wr.concurrency_key IS NOT DISTINCT FROM $2
             ORDER BY wr.queued_at, wr.id
             LIMIT $3",
        )
        .bind(name)
        .bind(key)
        .bind(ADMIT_AT_ONCE)
        .fetch_all(&mut *tx)
        .await?;
        let now_dt = chrono::Utc::now();
        let now = now_dt.to_rfc3339();
        for (run_id, spec, queued_at, deadline_at) in &next {
            // A definition with no cap now reads as one that holds nothing
            // back, which only a row written by hand could be.
            let cap = crate::dag::DagGraph::stored_max_active_runs(spec).map_or(i64::MAX, i64::from);
            if running >= cap || room.is_some_and(|room| room <= 0) {
                break;
            }
            let let_in = sqlx::query(
                "UPDATE workflow_runs SET admitted_at = $1, deadline_at = $2
                 WHERE id = $3 AND status = 'running' AND stopping_as IS NULL
                   AND queued_at IS NOT NULL AND admitted_at IS NULL",
            )
            .bind(&now)
            .bind(super::deadline_after_wait(deadline_at.as_deref(), queued_at, now_dt))
            .bind(run_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if let_in == 0 {
                continue;
            }
            sqlx::query(
                "UPDATE task_runs SET remaining_deps = remaining_deps - 1, scheduled_at = $1
                 WHERE run_id = $2 AND status = 'pending' AND remaining_deps > 0",
            )
            .bind(&now)
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
            notify(&mut *tx, run_id).await?;
            running += 1;
            room = room.map(|room| room - 1);
            admitted.push(run_id.clone());
        }
        tx.commit().await?;
    }
    Ok(admitted)
}

/// What [`installation_room`] counts as taking the installation's room.
#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "enterprise"), allow(dead_code))]
enum Behind {
    /// The runs running and not waiting: the sweep's, letting the waiting in.
    Running,
    /// Those, and the runs that wait (for any cap): a submit's, which comes
    /// after every one of them. One that waits for its workflow's cap alone
    /// makes a new run wait a sweep at most.
    Waiting,
}

/// The room the installation's cap on runs running at once leaves (the
/// `run_admission_cap` row), taken under a lock every admission against it
/// waits on: its number less the runs `behind` counts. `None` where the
/// installation has no cap, as it has none but in a build that sets one.
#[cfg(feature = "enterprise")]
async fn installation_room(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    behind: Behind,
) -> Result<Option<i64>> {
    let cap: Option<i64> =
        sqlx::query_scalar("SELECT max_active_runs FROM run_admission_cap WHERE id = 1")
            .fetch_optional(&mut **tx)
            .await?;
    let Some(cap) = cap else {
        return Ok(None);
    };
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(RUN_ADMISSION_LOCK)
        .bind(INSTALLATION_ADMISSION_SCOPE)
        .execute(&mut **tx)
        .await?;
    let taken: i64 = sqlx::query_scalar(match behind {
        Behind::Running => {
            "SELECT COUNT(*) FROM workflow_runs
              WHERE status = 'running' AND (queued_at IS NULL OR admitted_at IS NOT NULL)"
        }
        // A run that waits and is asked to stop is no longer in line.
        Behind::Waiting => {
            "SELECT COUNT(*) FROM workflow_runs
              WHERE status = 'running'
                AND (queued_at IS NULL OR admitted_at IS NOT NULL OR stopping_as IS NULL)"
        }
    })
    .fetch_one(&mut **tx)
    .await?;
    Ok(Some(cap - taken))
}

#[cfg(not(feature = "enterprise"))]
async fn installation_room(
    _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    _behind: Behind,
) -> Result<Option<i64>> {
    Ok(None)
}

/// The admission lock's scope for the installation's cap: `\u{1f}` begins no
/// workflow's name, so it is no workflow's scope.
#[cfg(feature = "enterprise")]
const INSTALLATION_ADMISSION_SCOPE: &str = "\u{1f}installation";

/// How many waiting runs of one key a pass of [`admit_waiting_runs`] reads.
/// More than any cap lets in at once; what is left is the next pass's.
const ADMIT_AT_ONCE: i64 = 64;

/// End the runs that were asked to stop and have been left with no task
/// stopping: a sweep, for what the statements that settle a task cannot reach.
///
/// A cancel and a settle each end the run in their own transaction, so an
/// engine of this version never leaves one. An engine from before the stop
/// request does: it knows nothing of the request, finishes the task under its
/// claim as it always did (succeeded, failed, or queued for a retry), and ends
/// no run. With engines of both versions on one datastore, such a run would
/// read `running` for good.
///
/// For each, the stop is applied again (a task that came back to the queue is
/// cancelled; one a worker has since claimed is asked), a request left on a
/// task that is over is cleared, and the run ends if nothing is stopping now.
/// Returns how many runs it ended. Reads a partial index of the runs being
/// stopped, which is empty when none is.
pub async fn end_stopped_runs(pool: &Pool) -> Result<u64> {
    let stranded: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM workflow_runs
         WHERE stopping_as IS NOT NULL AND status = 'running'
           AND NOT EXISTS (
               SELECT 1 FROM task_runs t
               WHERE t.run_id = workflow_runs.id
                 AND t.status = 'running' AND t.stop_requested_at IS NOT NULL
           )",
    )
    .fetch_all(pool)
    .await?;

    let mut ended = 0;
    for run_id in &stranded {
        let now = chrono::Utc::now().to_rfc3339();
        let mut tx = pool.begin().await?;
        // The run's row first, as everywhere a stopping run is ended.
        sqlx::query("SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE")
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
        stop_run_tasks(&mut tx, run_id, &now).await?;
        sqlx::query(
            "UPDATE task_runs SET stop_requested_at = NULL
             WHERE run_id = $1 AND status <> 'running' AND stop_requested_at IS NOT NULL",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        if finish_stopped_run(&mut tx, run_id, &now).await?.is_some() {
            ended += 1;
        }
        notify(&mut *tx, run_id).await?;
        tx.commit().await?;
    }
    Ok(ended)
}

/// Stop a run's tasks, for [`cancel_run`] and [`cancel_overdue_runs`]: cancel
/// those nothing is running for, and ask the ones a worker holds.
///
/// A row that is `running` with no claim is parked (a sensor, a sub-workflow,
/// a deferred job): no worker runs anything for it. One with a claim was
/// dispatched, and only its worker can say when what it started is gone.
///
/// A task that is asked for the first time is given a whole lease from now to
/// answer in, whatever was left of its own. Its worker is watching it, and may
/// renew nothing (`TASK_LEASE_HEARTBEAT=false`): without this a task that had
/// outlived its lease would be claimed for a teardown in the tick that asked
/// it, ahead of the worker stopping it. A task asked before keeps the lease it
/// has, which [`release_stopping_task`] may have cut short on purpose.
async fn stop_run_tasks(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    now: &str,
) -> Result<crate::models::Cancelled> {
    let lease = stop_lease();
    let cancelled = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', finished_at = $1, claimed_by = NULL, lease_expires_at = NULL
         WHERE run_id = $2
           AND (status IN ('pending', 'ready', 'awaiting_approval')
                OR (status = 'running' AND claimed_by IS NULL))",
    )
    .bind(now)
    .bind(run_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let stopping = sqlx::query(
        "UPDATE task_runs
         SET lease_expires_at = CASE WHEN stop_requested_at IS NULL THEN $3
                                     ELSE lease_expires_at END,
             stop_requested_at = COALESCE(stop_requested_at, $1)
         WHERE run_id = $2 AND status = 'running' AND claimed_by IS NOT NULL",
    )
    .bind(now)
    .bind(run_id)
    .bind(&lease)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(crate::models::Cancelled { cancelled, stopping })
}

/// When the lease given to a task that is asked to stop runs out: a whole
/// lease from now (see [`stop_run_tasks`]).
fn stop_lease() -> String {
    (chrono::Utc::now() + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339()
}

/// End a run that was asked to stop, once none of its tasks is still stopping:
/// the status it was to take (`stopping_as`), and its `run.completed` event,
/// which the rest of the end-of-run work is owed to. Nothing while
/// a task still is, and nothing for a run that was not asked.
///
/// The caller holds the run's row (see [`settle_stopping`]).
async fn finish_stopped_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    now: &str,
) -> Result<Option<String>> {
    let ended: Option<String> = sqlx::query_scalar(
        "UPDATE workflow_runs
         SET status = stopping_as, finished_at = $1, completion_pending_at = NULL,
             start_pending_at = NULL, stopping_as = NULL, stop_requested_at = NULL
         WHERE id = $2 AND status = 'running' AND stopping_as IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM task_runs
               WHERE run_id = $2 AND status = 'running' AND stop_requested_at IS NOT NULL
           )
         RETURNING status",
    )
    .bind(now)
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(status) = &ended {
        insert_run_completed(tx, run_id, status, now).await?;
    }
    Ok(ended)
}

/// `cancelled`, for a task that was asked to stop and is held under this
/// claim: its work is gone, or was never started. Ends its run too when it was
/// the last one stopping ([`finish_stopped_run`]). `false`, and nothing
/// written, for any other row.
///
/// Locks the run's row before the task's, as a cancel does. Two tasks of one
/// run settling at once would otherwise each see the other still stopping and
/// leave the run running for good; and a cancel and a settle taking the two
/// rows in opposite orders would deadlock.
async fn settle_stopping(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    now: &str,
) -> Result<bool> {
    let run_id: Option<String> = sqlx::query_scalar(
        "SELECT run_id FROM task_runs
         WHERE id = $1 AND claimed_by = $2 AND version = $3
           AND status = 'running' AND stop_requested_at IS NOT NULL",
    )
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(run_id) = run_id else {
        return Ok(false);
    };
    sqlx::query("SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE")
        .bind(&run_id)
        .execute(&mut **tx)
        .await?;
    let settled = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', finished_at = $1, claimed_by = NULL, lease_expires_at = NULL,
             stop_requested_at = NULL
         WHERE id = $2 AND claimed_by = $3 AND version = $4
           AND status = 'running' AND stop_requested_at IS NOT NULL",
    )
    .bind(now)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if settled == 0 {
        return Ok(false);
    }
    finish_stopped_run(tx, &run_id, now).await?;
    notify(&mut **tx, &run_id).await?;
    Ok(true)
}

/// A task that was asked to stop, and whose worker says its work is gone:
/// `cancelled`, and its run ended if it was the last one stopping. `false`
/// when the row is not such a task under this claim (it was never asked, or
/// the claim is another's now): nothing is written.
pub async fn settle_stopped_task(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let settled = settle_stopping(&mut tx, task_id, worker_id, fence, &now).await?;
    tx.commit().await?;
    Ok(settled)
}

/// A task that was asked to stop, and whose worker could not say its work is
/// gone: left `running` and asked, with a lease that lapses in `retry_secs`,
/// so that [`claim_teardowns`] takes it and its work is looked for again.
/// `false` when the row is not such a task under this claim.
pub async fn release_stopping_task(
    pool: &Pool,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    retry_secs: i64,
) -> Result<bool> {
    let retry_at = (chrono::Utc::now() + chrono::TimeDelta::seconds(retry_secs)).to_rfc3339();
    let rows = sqlx::query(
        "UPDATE task_runs SET lease_expires_at = $1
         WHERE id = $2 AND claimed_by = $3 AND version = $4
           AND status = 'running' AND stop_requested_at IS NOT NULL",
    )
    .bind(&retry_at)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows == 1)
}

/// Claim tasks that were asked to stop and whose lease has lapsed: the worker
/// that held them died, or could not see its teardown through. Nobody is
/// watching their work, so it is looked for again, by a dispatch that only
/// tears down.
///
/// The claim is a new one (`version + 1`), which is what makes everything the
/// task left a predecessor of this dispatch, and voids a late word from the
/// worker that held it. No attempt is spent: nothing runs. Returns the rows as
/// a claim does, with the version from before it. `classes` is the claim's
/// own filter: a teardown goes to an engine that serves the task's backend.
///
/// `held` are the tasks the asking engine still has an execution out for.
/// They are not claimed: their worker is watching the work, lease or none,
/// and its word is the one to wait for. Another engine cannot know that, and
/// has the lease alone to go by.
pub async fn claim_teardowns(
    pool: &Pool,
    worker_id: &str,
    limit: i64,
    classes: &[String],
    held: &[String],
) -> Result<Vec<TaskRun>> {
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_exp = (now_dt + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339();
    let rows = sqlx::query(
        "UPDATE task_runs
         SET claimed_by = $1, lease_expires_at = $2, version = version + 1
         WHERE id IN (
             SELECT id FROM task_runs
             WHERE status = 'running' AND stop_requested_at IS NOT NULL
               AND lease_expires_at IS NOT NULL AND lease_expires_at < $3
               AND session_id IS NULL
               AND (cardinality($5::text[]) = 0 OR runner_class = ANY($5::text[]))
               AND NOT (id = ANY($6::text[]))
             ORDER BY lease_expires_at
             LIMIT $4
             FOR UPDATE SKIP LOCKED
         )
         RETURNING id, run_id, name, status, attempt, remaining_deps,
                   input, output, claimed_by, lease_expires_at,
                   version - 1 AS version, scheduled_at, finished_at, pool,
                   stop_requested_at",
    )
    .bind(worker_id)
    .bind(&lease_exp)
    .bind(&now)
    .bind(limit)
    .bind(classes)
    .bind(held)
    .fetch_all(pool)
    .await?;
    rows.iter().map(row_to_task).collect()
}

/// Of the tasks an engine has dispatched (`held`: each id with its fence),
/// those whose work should stop now: asked to stop, or no longer this
/// engine's under that claim. The engine asks at every sweep, so a cancel
/// reaches the work within one, whether or not its worker renews a lease.
pub async fn tasks_to_stop(
    pool: &Pool,
    worker_id: &str,
    held: &[(String, i64)],
) -> Result<Vec<String>> {
    if held.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<&str> = held.iter().map(|(id, _)| id.as_str()).collect();
    let rows: Vec<(String, i64, Option<String>, String, Option<String>)> = sqlx::query_as(
        "SELECT id, version, claimed_by, status, stop_requested_at
         FROM task_runs WHERE id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    Ok(super::not_held(worker_id, held, rows))
}

/// Emit a soft SLA deadline alert (spec `deadline`) for each still-running run
/// past its `alert_deadline_at` that hasn't alerted yet (#20). Fire-once and
/// idempotent via a guarded `RETURNING` update (winner-take-all across
/// schedulers). Does NOT cancel — appends a `run.deadline_exceeded` event to the
/// transactional outbox in the same transaction and NOTIFYs the drainer.
/// Returns the alerted runs, each `(run_id, alert_deadline_at)`.
pub async fn fire_deadline_alerts(pool: &Pool) -> Result<Vec<(String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let fired: Vec<(String, String)> = sqlx::query_as(
        "UPDATE workflow_runs SET alert_fired_at = $1
         WHERE status = 'running' AND alert_deadline_at IS NOT NULL
           AND alert_deadline_at < $1 AND alert_fired_at IS NULL
         RETURNING id, alert_deadline_at",
    )
    .bind(&now)
    .fetch_all(&mut *tx)
    .await?;

    for (run_id, _) in &fired {
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
/// datastore: [`status_counts_live`] and [`status_counts_history`] in one.
#[cfg(feature = "ops")]
pub async fn status_counts(pool: &Pool) -> Result<crate::models::MetricsSnapshot> {
    let live = status_counts_live(pool).await?;
    Ok(live.with_history(&status_counts_history(pool).await?, 0.0))
}

/// The `/metrics` gauges of the runs and tasks that have finished, by status,
/// and of the runs of the last 24 hours that have: what grows
/// with every run, read apart from the rest and cached longer. `NOT IN` the
/// statuses [`status_counts_live`] reads, so a status added later is still
/// counted.
#[cfg(feature = "ops")]
pub async fn status_counts_history(pool: &Pool) -> Result<crate::models::MetricsHistory> {
    let finished = status_counts_finished(pool).await?;
    Ok(crate::models::MetricsHistory {
        recent_runs: recent_runs(pool, false).await?,
        ..finished
    })
}

/// The runs of the last [`crate::models::RECENT_RUNS_WINDOW_SECS`] by
/// workflow, environment and status: those that have not finished where
/// `unfinished`, and the rest where not. Bounded by a time window, not by the
/// size of the run history, but a join of two tables that grow with it: the
/// finished are read with the history. Those that have not are read for a
/// scrape, by the index of the runs by status and when they were made
/// (migrations_pg/048), which reads only theirs.
#[cfg(feature = "ops")]
async fn recent_runs(pool: &Pool, unfinished: bool) -> Result<Vec<crate::models::RecentRuns>> {
    let since = (chrono::Utc::now()
        - chrono::TimeDelta::seconds(crate::models::RECENT_RUNS_WINDOW_SECS))
    .to_rfc3339();
    let recent: Vec<(String, Option<String>, String, i64)> = sqlx::query_as(&format!(
        "SELECT wd.name, wr.environment, wr.status, COUNT(*)
         FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.status {} ({}) AND wr.created_at >= $1
         GROUP BY wd.name, wr.environment, wr.status",
        if unfinished { "IN" } else { "NOT IN" },
        super::sql_list(&crate::models::LIVE_RUN_STATUSES)
    ))
    .bind(&since)
    .fetch_all(pool)
    .await?;
    Ok(recent
        .into_iter()
        .map(
            |(workflow, environment, status, count)| crate::models::RecentRuns {
                workflow,
                environment,
                status,
                count,
            },
        )
        .collect())
}

/// Of [`status_counts_history`], the runs and tasks that have finished, by
/// status, alone: all dagron-api's `GET /api/metrics` serves of the history.
#[cfg(feature = "ops")]
pub async fn status_counts_finished(pool: &Pool) -> Result<crate::models::MetricsHistory> {
    let runs: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT status, COUNT(*) FROM workflow_runs WHERE status NOT IN ({}) GROUP BY status",
        super::sql_list(&crate::models::LIVE_RUN_STATUSES)
    ))
    .fetch_all(pool)
    .await?;
    let tasks: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT status, COUNT(*) FROM task_runs WHERE status NOT IN ({}) GROUP BY status",
        super::sql_list(&crate::models::LIVE_TASK_STATUSES)
    ))
    .fetch_all(pool)
    .await?;
    Ok(crate::models::MetricsHistory {
        runs_by_status: runs,
        tasks_by_status: tasks,
        recent_runs: Vec::new(),
    })
}

/// Of [`status_counts_live`], the runs and tasks that have not finished, by
/// status, and the dead letters, alone: all dagron-api's `GET /api/metrics`
/// serves of what has not finished. Three reads, where the whole is nine.
#[cfg(feature = "ops")]
pub async fn status_counts_unfinished(pool: &Pool) -> Result<crate::models::MetricsSnapshot> {
    let runs: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT status, COUNT(*) FROM workflow_runs WHERE status IN ({}) GROUP BY status",
        super::sql_list(&crate::models::LIVE_RUN_STATUSES)
    ))
    .fetch_all(pool)
    .await?;
    let tasks: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT status, COUNT(*) FROM task_runs WHERE status IN ({}) GROUP BY status",
        super::sql_list(&crate::models::LIVE_TASK_STATUSES)
    ))
    .fetch_all(pool)
    .await?;
    let dead_letters: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters")
        .fetch_one(pool)
        .await?;
    Ok(crate::models::MetricsSnapshot {
        runs_by_status: runs,
        tasks_by_status: tasks,
        dead_letters,
        ..Default::default()
    })
}

/// The `/metrics` gauges of what has not finished: runs and
/// tasks in the statuses of work still to do, by status, each read by its
/// status index; the dead letters; the recent runs still to end; and the
/// rest of the snapshot, each bounded by what is waiting, not by what has
/// run. Read for a scrape.
#[cfg(feature = "ops")]
pub async fn status_counts_live(pool: &Pool) -> Result<crate::models::MetricsSnapshot> {
    let unfinished = status_counts_unfinished(pool).await?;
    // Per-workflow views. Each is bounded by something other than the size of
    // the run history: the non-terminal task set, the parked dead letters.
    let active_tasks: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT wd.name, tr.status, COUNT(*)
         FROM task_runs tr
         JOIN workflow_runs wr ON wr.id = tr.run_id
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE tr.status IN ('pending', 'ready', 'running', 'awaiting_approval')
         GROUP BY wd.name, tr.status",
    )
    .fetch_all(pool)
    .await?;
    let by_source: Vec<(String, i64, Option<String>)> = sqlx::query_as(
        "SELECT source, COUNT(*), MIN(first_seen_at) FROM dead_letters GROUP BY source",
    )
    .fetch_all(pool)
    .await?;
    let quota = quota_wait_stats(pool).await?;
    let sweeps = log_sweeps_waiting(pool).await?;
    // The partial index of the runs that wait, which is empty when none does.
    let (runs_waiting, runs_waiting_oldest_since): (i64, Option<String>) = sqlx::query_as(
        "SELECT COUNT(*), MIN(queued_at) FROM workflow_runs
         WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running'",
    )
    .fetch_one(pool)
    .await?;
    Ok(crate::models::MetricsSnapshot {
        ready_by_class: ready_backlog_by_class(pool).await?,
        recent_runs: recent_runs(pool, true).await?,
        active_tasks,
        dead_letters_by_source: by_source
            .into_iter()
            .map(|(source, count, oldest_first_seen_at)| crate::models::DeadLetterSource {
                source,
                count,
                oldest_first_seen_at,
            })
            .collect(),
        quota_waiting: quota.waiting,
        quota_wait_oldest_since: quota.oldest_since,
        runs_waiting,
        runs_waiting_oldest_since,
        log_sweeps_waiting: sweeps.waiting,
        log_sweeps_failing: sweeps.failing,
        log_sweeps_oldest_marked_at: sweeps.oldest_marked_at,
        history_age_secs: 0.0,
        ..unfinished
    })
}

/// What the run-duration metrics need to know about a run that just finished:
/// its workflow name and when it was created (RFC-3339).
#[cfg(feature = "ops")]
pub async fn run_metric_facts(pool: &Pool, run_id: &str) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT wd.name, wr.created_at FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.id = $1",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
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
        // The id came from the query above, so a missing run here would mean it
        // was purged between the two — treat that as nothing to archive rather
        // than failing the whole batch.
        if let Some(doc) = archive_doc_for_run(pool, &id).await? {
            out.push((id, doc));
        }
    }
    Ok(out)
}

/// Build one run's `dagron.run-archive.v1` document — run + definition + tasks
/// + outbox events, the self-contained record the sink stores.
///
/// `None` when the run does not exist. Deliberately **not** filtered by status
/// or age: the batch sweep selects its own eligible ids, and the API's per-run
/// archive route enforces its own guard so it can say *why* a run was refused
/// rather than reporting an empty result.
#[cfg(feature = "ops")]
pub async fn archive_doc_for_run(pool: &Pool, id: &str) -> Result<Option<serde_json::Value>> {
    let Some(run) = sqlx::query(
        "SELECT wr.*, wd.name AS definition_name, wd.spec AS definition_spec
         FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let tasks = sqlx::query("SELECT * FROM task_runs WHERE run_id = $1 ORDER BY name")
        .bind(id)
        .fetch_all(pool)
        .await?;
    let outbox = sqlx::query("SELECT * FROM event_outbox WHERE run_id = $1 ORDER BY created_at")
        .bind(id)
        .fetch_all(pool)
        .await?;
    // The retained per-attempt history (migration 044/056). Without it the
    // archive would drop exactly what per-attempt retention added — the purge
    // cascades these rows away, so the archive document is their last chance to
    // exist. Bounded by DAGRON_ATTEMPT_LOG_KEEP x _BYTES per task, which is
    // what makes including them affordable; a run with no loops and no retries
    // adds an empty array.
    let attempts = sqlx::query(
        "SELECT * FROM task_attempts
         WHERE task_id IN (SELECT id FROM task_runs WHERE run_id = $1)
         ORDER BY task_id, attempt",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    Ok(Some(serde_json::json!({
        "format": "dagron.run-archive.v1",
        "run": row_to_json(&run),
        "tasks": tasks.iter().map(row_to_json).collect::<Vec<_>>(),
        "outbox_events": outbox.iter().map(row_to_json).collect::<Vec<_>>(),
        "task_attempts": attempts.iter().map(row_to_json).collect::<Vec<_>>(),
    })))
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
    // A run whose tasks still owe a remote teardown is NOT collectable: deleting
    // it drops the only record of a job still running on someone's cluster, and
    // turns the visible leak the orphan counter advertises into a silent one.
    // The debt settles itself once the teardown sweep gives up, so this defers
    // collection by that window and no longer.
    const OWES_TEARDOWN: &str = "EXISTS (
             SELECT 1 FROM task_runs owing
             WHERE owing.run_id = wr.id AND owing.external_handle IS NOT NULL
         )";
    let mut tx = pool.begin().await?;

    sqlx::query(
        &format!(
        "DELETE FROM task_dependencies
         WHERE dependent_id IN (
             SELECT tr.id FROM task_runs tr
             JOIN workflow_runs wr ON wr.id = tr.run_id
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < $1
               AND NOT {OWES_TEARDOWN}
         )"
    ))
    .bind(cutoff)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        &format!(
        "DELETE FROM task_runs
         WHERE run_id IN (
             SELECT wr.id FROM workflow_runs wr
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < $1
               AND NOT {OWES_TEARDOWN}
         )"
    ))
    .bind(cutoff)
    .execute(&mut *tx)
    .await?;

    let deleted = sqlx::query(
        &format!(
        "DELETE FROM workflow_runs
         WHERE id IN (
             SELECT wr.id FROM workflow_runs wr
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < $1
               AND NOT {OWES_TEARDOWN}
         )"
    ))
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

// ── Sweep slots ────────────────────────────────────────────────────────────────

/// The database's clock as a `sweep_slots` row keeps it, [`SWEEP_NOW`]'s
/// text, `secs` on where given: read once for the whole statement. A claim
/// that waited on a row's lock stamps every row it takes with one time; a
/// clock read for each row split one claim across two times, and a third
/// engine took a row of it.
fn slot_clock(secs: Option<&str>) -> String {
    let at = match secs {
        Some(secs) => format!("statement_timestamp() + make_interval(secs => {secs})"),
        None => "statement_timestamp()".to_string(),
    };
    slot_text(&at)
}

/// The time `at` gives as a `sweep_slots` row keeps it.
fn slot_text(at: &str) -> String {
    format!("to_char(({at}) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')")
}

/// Take, for `holder`, the slots of `sweeps` (each `(name, seconds)`) that
/// are due, and return those this engine took: each is its own for its
/// `seconds` from now, and no other engine's until then.
///
/// One statement for every sweep, on the database's clock, so that no
/// engine's clock decides. A slot whose row is missing is made: one deleted
/// by hand comes back at the next claim, where an update alone would leave
/// its sweep unrun on every engine. The rows are taken in the order of their
/// names, so two engines lock them in one order and never wait on each
/// other in a ring.
///
/// A slot is not a lock: an engine whose tick stalls after it took one may
/// still be sweeping when the next window goes to another. So only a sweep
/// two engines may run at once is put on a slot ([`crate::sweep::Scope`]).
///
/// A slot due later than this claim would set it is taken too. Once the
/// database's clock has stepped back, every slot is due that much later,
/// and no engine would take one (the leases, the deadlines, the sensors)
/// until the clock caught up. Else it is another engine's of a longer
/// interval for that sweep, and this one runs it early, as a shared sweep
/// may. That is timed by the clock when the row is had, not when the
/// statement began: a claim that waited on another's take of the row would
/// find that take's window later than its own, and take the slot again.
pub async fn claim_sweep_slots(
    pool: &Pool,
    holder: &str,
    sweeps: &[(&str, f64)],
) -> Result<Vec<super::SweepSlot>> {
    if sweeps.is_empty() {
        return Ok(Vec::new());
    }
    let names: Vec<String> = sweeps.iter().map(|(name, _)| name.to_string()).collect();
    let secs: Vec<f64> = sweeps.iter().map(|(_, secs)| *secs).collect();
    let rows: Vec<(String, i64, Option<String>)> = sqlx::query_as(&format!(
        "INSERT INTO sweep_slots AS s (name, due_at, holder, turn, taken_at)
         SELECT c.name, {due}, $1, 1, {now}
           FROM unnest($2::text[], $3::float8[]) AS c(name, secs)
          ORDER BY c.name
         ON CONFLICT (name) DO UPDATE
            SET due_at = excluded.due_at, holder = excluded.holder,
                turn = s.turn + 1, taken_at = excluded.taken_at
          WHERE s.due_at <= excluded.taken_at OR s.due_at > {ahead}
         RETURNING s.name, s.turn, s.cursor",
        due = slot_clock(Some("c.secs")),
        now = slot_clock(None),
        ahead = slot_text(
            "clock_timestamp()
               + (excluded.due_at::timestamptz - excluded.taken_at::timestamptz)"
        ),
    ))
    .bind(holder)
    .bind(&names)
    .bind(&secs)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(name, turn, cursor)| super::SweepSlot { name, turn, cursor })
        .collect())
}

/// Write where a sweep got to (the reap's last run), for whichever engine
/// takes its slot next. Only at the `turn` the slot was taken at: an engine
/// that stalled after it took the slot writes nothing once another has
/// taken it since. Returns whether it was written.
pub async fn set_sweep_cursor(
    pool: &Pool,
    name: &str,
    turn: i64,
    cursor: Option<&str>,
) -> Result<bool> {
    let rows = sqlx::query("UPDATE sweep_slots SET cursor = $3 WHERE name = $1 AND turn = $2")
        .bind(name)
        .bind(turn)
        .bind(cursor)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(rows > 0)
}

/// Hand a sweep's slot back before its window is out: its pass read a full
/// batch, and the next is due now, for the next engine to ask. That is the
/// engine that handed it back, whose sweep is due again at once, unless
/// another asks before its next tick: the engines told the slot was taken
/// in this window wait a whole interval before they ask again. Only at the
/// `turn` it was taken at. Returns whether it was handed back.
pub async fn hand_back_sweep_slot(pool: &Pool, name: &str, turn: i64) -> Result<bool> {
    let rows =
        sqlx::query("UPDATE sweep_slots SET due_at = taken_at WHERE name = $1 AND turn = $2")
            .bind(name)
            .bind(turn)
            .execute(pool)
            .await?
            .rows_affected();
    Ok(rows > 0)
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
/// The payload is the run. The reconcile-loop [`Waker`] keeps the runs it
/// was told of, so that a tick looks for ended runs among those and not among
/// every run there is ([`Woken`]); and the management API's SSE bridge routes
/// each event to only the clients watching that run, instead of waking every
/// browser on every transition.
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
///
/// A notification says only that something changed, and a tick looks at
/// everything. So the notifications that are waiting when `wait` is called,
/// and those that arrive together, are one reason to tick: `wait` takes them
/// all and returns once. Taken one a wait, a channel that fills faster than
/// the loop ticks leaves a backlog the loop goes on ticking through after the
/// work that sent it is over.
///
/// What a notification does say is which run changed, and that is kept: see
/// [`Waker::take_woken`].
pub struct Waker {
    listener: PgListener,
    /// The runs named by the notifications taken since the last
    /// [`Waker::take_woken`].
    runs: std::collections::HashSet<String>,
    /// That `runs` cannot be all that changed: nothing has been heard yet,
    /// or the connection was lost, or more runs were named than are kept.
    all: bool,
    /// Notifications [`Waker::take_woken`] took that no [`Waker::wait`] has
    /// answered for yet. A notification is a reason to tick again, whoever
    /// took it off the connection: the next wait returns at once for these.
    owed: usize,
}

/// The most runs a waker keeps the names of between two ticks. Past it the
/// tick is told to look at everything, which is what it did before a wake
/// named its run.
const MAX_NAMED_RUNS: usize = 4096;

/// The most notifications one [`Waker::wait`] takes. It bounds the time one
/// wait spends taking them from a channel that fills as fast as it is
/// emptied. What is left is the next wait's.
const NOTIFICATIONS_PER_WAIT: usize = 65_536;

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
        // What was sent before this listened was not heard: the first tick
        // looks at everything.
        Ok(Self {
            listener,
            runs: std::collections::HashSet::new(),
            all: true,
            owed: 0,
        })
    }

    /// What has changed since this was last asked: the runs named by every
    /// notification taken, those that are waiting now among them.
    ///
    /// It is asked for at the point of a tick that looks for ended runs, and
    /// not where the tick began. A notification the tick's own work sent (a
    /// task it marked) has then most often arrived, so the run that task
    /// ended is looked at in the tick that ended it.
    ///
    /// What it takes off the connection here is still owed a tick. A run
    /// made while a tick was going is named by a notification this takes,
    /// and that run's first task is the next tick's to start: were the
    /// notification simply spent here, the wait that follows would find
    /// nothing waiting and sleep out the poll interval over it.
    pub async fn take_woken(&mut self) -> Woken {
        self.owed += self.take_waiting().await;
        if std::mem::take(&mut self.all) {
            self.runs.clear();
            return Woken::All;
        }
        Woken::Runs(self.runs.drain().collect())
    }

    /// Keep the run a notification names: its payload, up to a `:` should
    /// one ever say more than the run (a run's id holds none).
    fn heard(&mut self, payload: &str) {
        if self.all {
            return;
        }
        let run = payload.split(':').next().unwrap_or(payload);
        self.runs.insert(run.to_string());
        if self.runs.len() > MAX_NAMED_RUNS {
            self.runs.clear();
            self.all = true;
        }
    }

    /// Wait for a reason to tick, and return how many notifications were
    /// taken: 0 when it was the interval that passed.
    pub async fn wait(&mut self, interval: Duration) -> Result<usize> {
        // What arrived while the caller was busy: still on the connection,
        // or taken off it already when the caller asked which runs changed.
        let waiting = self.take_waiting().await + std::mem::take(&mut self.owed);
        if waiting > 0 {
            return Ok(waiting);
        }
        // `try_recv` and not `recv`: it says when the connection was lost,
        // where `recv` connects again and says nothing. What was sent while
        // there was no connection is gone, and a tick must not take the runs
        // it has heard of for all that changed.
        tokio::select! {
            recv = self.listener.try_recv() => match recv {
                // The first of a burst wakes the wait, and the rest of what
                // has arrived with it is taken before the caller ticks.
                Ok(Some(first)) => {
                    self.heard(first.payload());
                    Ok(1 + self.take_waiting().await)
                }
                // The connection was lost, and is made again by the next
                // asking. Tick now, and look at everything.
                Ok(None) => {
                    self.all = true;
                    Ok(0)
                }
                // A transient listener/connection blip must not stop scheduling:
                // log and fall back to a timer tick. PgListener auto-reconnects,
                // and the next tick's queries run against the live pool regardless.
                Err(err) => {
                    self.all = true;
                    tracing::warn!(error = ?err, "listener recv failed; timer fallback for this tick");
                    tokio::time::sleep(interval).await;
                    Ok(0)
                }
            },
            _ = tokio::time::sleep(interval) => Ok(0),
        }
    }

    /// Take every notification that can be had without waiting for it, and
    /// count them.
    ///
    /// Each is asked for and the asking dropped the moment it would wait. sqlx
    /// takes a message off the connection whole or leaves it there, so a
    /// dropped asking loses nothing. And nothing here yields, so a caller that
    /// stops waiting cannot drop a count that was already taken.
    ///
    /// An error ends the taking and is not reported here: the wait that finds
    /// nothing waiting meets it again, says so, and falls back to the timer.
    /// Either way what was heard is no longer known to be everything.
    async fn take_waiting(&mut self) -> usize {
        let mut taken = 0;
        while taken < NOTIFICATIONS_PER_WAIT {
            let next = {
                let mut recv = std::pin::pin!(self.listener.try_recv());
                std::future::poll_fn(|cx| Poll::Ready(recv.as_mut().poll(cx))).await
            };
            match next {
                Poll::Ready(Ok(Some(one))) => {
                    self.heard(one.payload());
                    taken += 1;
                }
                Poll::Ready(Ok(None)) | Poll::Ready(Err(_)) => {
                    self.all = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        taken
    }
}

// ── Path caches ──────────────────────────────────────────────────────────────
// The index of what `workspace.cache` keeps in the engine's cache store. The
// bytes are in the store; these rows say where, how big, and when last read.

/// The ready entries of one cache in one scope, newest first: what a task of
/// that repository and ref may restore.
pub async fn path_cache_entries(
    pool: &Pool,
    scope: &str,
    prefix: &str,
    version: &str,
    limit: i64,
) -> Result<Vec<crate::models::PathCacheEntry>> {
    let rows: Vec<(String, String, String, Option<i64>)> = sqlx::query_as(
        "SELECT id, cache_key, object_key, size_bytes FROM path_caches
         WHERE scope = $1 AND prefix = $2 AND version = $3 AND state = 'ready'
         ORDER BY created_at DESC, id DESC LIMIT $4",
    )
    .bind(scope)
    .bind(prefix)
    .bind(version)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, cache_key, object_key, size_bytes)| crate::models::PathCacheEntry {
                id,
                cache_key,
                object_key,
                size_bytes,
            },
        )
        .collect())
}

/// An entry was offered to a task to restore: it was used now, for the
/// quota's least-recently-used order.
pub async fn path_cache_used(pool: &Pool, id: &str) -> Result<()> {
    sqlx::query("UPDATE path_caches SET last_used_at = $1 WHERE id = $2 AND state = 'ready'")
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// A task was given a URL to save an entry to: its row, pending until the
/// task has succeeded.
pub async fn path_cache_offered(pool: &Pool, entry: &crate::models::NewPathCache) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO path_caches
           (id, repo, scope, prefix, version, object_key, state, task_id, created_at, last_used_at)
         VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $8)",
    )
    .bind(&entry.id)
    .bind(&entry.repo)
    .bind(&entry.scope)
    .bind(&entry.prefix)
    .bind(&entry.version)
    .bind(&entry.object_key)
    .bind(&entry.task_id)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

/// The pending entries the sweep has something to do with: their task has
/// ended (or its row is gone), or they were offered before `stale_before` and
/// the URL they were offered with is spent. Oldest first.
pub async fn path_caches_settled(
    pool: &Pool,
    stale_before: &str,
    limit: i64,
) -> Result<Vec<crate::models::PendingPathCache>> {
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT p.id, p.object_key, p.created_at, t.status
         FROM path_caches p LEFT JOIN task_runs t ON t.id = p.task_id
         WHERE p.state = 'pending'
           AND (t.id IS NULL OR t.status IN ('succeeded', 'failed', 'skipped', 'cancelled')
                OR p.created_at < $1)
         ORDER BY p.created_at LIMIT $2",
    )
    .bind(stale_before)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, object_key, created_at, task_status)| crate::models::PendingPathCache {
                id,
                object_key,
                created_at,
                task_status,
            },
        )
        .collect())
}

/// Make a pending entry ready under the key its task saved it with, in the
/// place of any ready entry of the same key in its scope. Returns the object
/// keys of the entries it took the place of, for the store to forget, or
/// `None` when the entry was no longer pending (another sweep took it).
pub async fn path_cache_ready(
    pool: &Pool,
    id: &str,
    cache_key: &str,
    size_bytes: i64,
) -> Result<Option<Vec<String>>> {
    let mut tx = pool.begin().await?;
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT scope, prefix, version FROM path_caches WHERE id = $1 AND state = 'pending' FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((scope, prefix, version)) = row else {
        return Ok(None);
    };
    let replaced: Vec<(String,)> = sqlx::query_as(
        "DELETE FROM path_caches
         WHERE scope = $1 AND prefix = $2 AND version = $3 AND cache_key = $4 AND state = 'ready'
         RETURNING object_key",
    )
    .bind(&scope)
    .bind(&prefix)
    .bind(&version)
    .bind(cache_key)
    .fetch_all(&mut *tx)
    .await?;
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "UPDATE path_caches SET state = 'ready', cache_key = $1, size_bytes = $2, task_id = NULL,
           created_at = $3, last_used_at = $3
         WHERE id = $4",
    )
    .bind(cache_key)
    .bind(size_bytes)
    .bind(&now)
    .bind(id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(replaced.into_iter().map(|(k,)| k).collect()))
}

/// Forget an entry. Returns its object key, for the store to forget too, or
/// `None` when another sweep already did.
pub async fn path_cache_drop(pool: &Pool, id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("DELETE FROM path_caches WHERE id = $1 RETURNING object_key")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(k,)| k))
}

/// The ready entries to evict, least recently used first: those of a
/// repository past what is newer of it within `quota_bytes`, and those not
/// read since `unused_since`.
pub async fn path_cache_victims(
    pool: &Pool,
    quota_bytes: i64,
    unused_since: &str,
    limit: i64,
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM (
           SELECT id, last_used_at,
                  SUM(COALESCE(size_bytes, 0)) OVER (
                    PARTITION BY repo ORDER BY last_used_at DESC, id DESC ROWS UNBOUNDED PRECEDING
                  ) AS kept
           FROM path_caches WHERE state = 'ready'
         ) r
         WHERE r.kept > $1 OR r.last_used_at < $2
         ORDER BY r.last_used_at LIMIT $3",
    )
    .bind(quota_bytes)
    .bind(unused_since)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

// ── Workflow artifacts ───────────────────────────────────────────────────────

type ArtifactRow = (
    String,
    i64,
    String,
    String,
    String,
    String,
    Option<i64>,
    Option<String>,
    String,
    String,
);

fn workflow_artifact(row: ArtifactRow) -> crate::models::WorkflowArtifact {
    let (
        id,
        artifact_id,
        run_id,
        task_id,
        name,
        object_key,
        size_bytes,
        sha256,
        created_at,
        expires_at,
    ) = row;
    crate::models::WorkflowArtifact {
        id,
        artifact_id,
        run_id,
        task_id,
        name,
        object_key,
        size_bytes,
        sha256,
        created_at,
        expires_at,
    }
}

/// A job asked to upload an artifact: its row, pending until the upload is
/// said to be complete, and expiring as `artifact.expires_at` says.
pub async fn workflow_artifact_begin(
    pool: &Pool,
    artifact: &crate::models::WorkflowArtifact,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO workflow_artifacts
           (id, artifact_id, run_id, task_id, name, object_key, state, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8)",
    )
    .bind(&artifact.id)
    .bind(artifact.artifact_id)
    .bind(&artifact.run_id)
    .bind(&artifact.task_id)
    .bind(&artifact.name)
    .bind(&artifact.object_key)
    .bind(&artifact.created_at)
    .bind(&artifact.expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// An upload is complete: the artifact is its run's, under its name, unless
/// another of that name became so first. `false` then, or where the row is
/// no longer pending. Two at once: the unique index refuses the second,
/// which is `false` too.
pub async fn workflow_artifact_ready(
    pool: &Pool,
    id: &str,
    size_bytes: i64,
    sha256: &str,
    expires_at: &str,
    provenance: Option<&str>,
) -> Result<bool> {
    let done = sqlx::query(
        "UPDATE workflow_artifacts
            SET state = 'ready', size_bytes = $1, sha256 = $2, expires_at = $3, created_at = $4,
                provenance = $6
          WHERE id = $5 AND state = 'pending'
            AND NOT EXISTS (
                SELECT 1 FROM workflow_artifacts AS w
                 WHERE w.run_id = workflow_artifacts.run_id
                   AND w.name = workflow_artifacts.name
                   AND w.state = 'ready')",
    )
    .bind(size_bytes)
    .bind(sha256)
    .bind(expires_at)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(id)
    .bind(provenance)
    .execute(pool)
    .await;
    match done {
        Ok(done) => Ok(done.rows_affected() == 1),
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// A run's artifacts whose uploads are complete, oldest first.
pub async fn workflow_artifacts_of_run(
    pool: &Pool,
    run_id: &str,
) -> Result<Vec<crate::models::WorkflowArtifact>> {
    let rows: Vec<ArtifactRow> = sqlx::query_as(
        "SELECT id, artifact_id, run_id, task_id, name, object_key, size_bytes, sha256,
                created_at, expires_at
           FROM workflow_artifacts WHERE run_id = $1 AND state = 'ready'
          ORDER BY created_at, artifact_id",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(workflow_artifact).collect())
}

/// When `task_id`'s attempt that runs now was queued (`scheduled_at`): what
/// it uploaded is later than this, and what an earlier attempt of it uploaded,
/// before a rerun, a clear or a retry queued this one, is not. `None` for a
/// task that is not there.
pub async fn task_queued_at(pool: &Pool, task_id: &str) -> Result<Option<String>> {
    let at: Option<Option<String>> =
        sqlx::query_scalar("SELECT scheduled_at FROM task_runs WHERE id = $1")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(at.flatten())
}

/// Drop an artifact's row: its object key, to delete its bytes by; `None`
/// when another had dropped it.
pub async fn workflow_artifact_drop(pool: &Pool, id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("DELETE FROM workflow_artifacts WHERE id = $1 RETURNING object_key")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(k,)| k))
}

/// The artifacts that expired before `now`, pending ones too, soonest first:
/// their ids and object keys. The rows stay until they are dropped, once
/// their bytes are gone.
pub async fn workflow_artifacts_expired(
    pool: &Pool,
    now: &str,
    limit: i64,
) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT id, object_key FROM workflow_artifacts WHERE expires_at < $1
          ORDER BY expires_at LIMIT $2",
    )
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

// ── Task identity ────────────────────────────────────────────────────────────

/// A task that asks for an identity is given a request credential: its row,
/// in place of any an earlier attempt of the task had. Rows past their time
/// go with it.
pub async fn oidc_request_offered(pool: &Pool, request: &crate::models::OidcRequest) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM oidc_requests WHERE task_id = $1 OR expires_at < $2")
        .bind(&request.task_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO oidc_requests
           (token_sha256, run_id, task_id, task_name, attempt, audiences, environment, expires_at, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(&request.token_sha256)
    .bind(&request.run_id)
    .bind(&request.task_id)
    .bind(&request.task_name)
    .bind(request.attempt)
    .bind(serde_json::to_string(&request.audiences)?)
    .bind(&request.environment)
    .bind(&request.expires_at)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// The request credential of this SHA-256, with its task's status now
/// (`None` when the task's row is gone).
pub async fn oidc_request(
    pool: &Pool,
    token_sha256: &str,
) -> Result<Option<(crate::models::OidcRequest, Option<String>)>> {
    #[allow(clippy::type_complexity)]
    let row: Option<(
        String,
        String,
        String,
        String,
        i64,
        String,
        Option<String>,
        String,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT r.token_sha256, r.run_id, r.task_id, r.task_name, r.attempt, r.audiences,
                    r.environment, r.expires_at, t.status
             FROM oidc_requests r LEFT JOIN task_runs t ON t.id = r.task_id
             WHERE r.token_sha256 = $1",
    )
    .bind(token_sha256)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(
            token_sha256,
            run_id,
            task_id,
            task_name,
            attempt,
            audiences,
            environment,
            expires_at,
            status,
        )| {
            (
                crate::models::OidcRequest {
                    token_sha256,
                    run_id,
                    task_id,
                    task_name,
                    attempt,
                    audiences: serde_json::from_str(&audiences).unwrap_or_default(),
                    environment,
                    expires_at,
                },
                status,
            )
        },
    ))
}

// ── Remote runner sessions ────────────────────────────────────────────────────
//
// A remote runner's session holds the tasks it has taken. The session's lease
// is renewed by its heartbeat, one statement for every task it holds; a task
// row is not rewritten. A session-held row keeps the lease it was claimed with
// (non-NULL, so the predicates that read a NULL lease as parked keep their
// meaning) and points at its session, and the engine's expired-lease sweeps
// pass it by: it is [`recover_expired_sessions`]'s to take back.
//
// What a runner says of a task is kept in `runner_results`, and an engine
// takes it ([`take_runner_results`]) and applies it as it does its own
// workers' results. The claim and the runner's words are dagron-api's, which
// is Postgres's alone: the functions it calls are here, and not in SQLite.

/// How long an engine that took a runner's result has to apply it before
/// another may take it: one that died with it in hand.
const RUNNER_RESULT_RETAKE_SECS: i64 = 60;

/// How long a session may hold a task its heartbeats do not name before it
/// is taken for a claim the runner never heard of ([`renew_session`]): a
/// heartbeat already on its way when the claim was answered does not name it.
pub const ORPHAN_CLAIM_SECS: i64 = 60;

/// Claim, for a runner's session, up to `limit` ready tasks of the classes it
/// serves: the engine's own claim ([`claim_ready_classes`]), under the
/// session's holder, and then each claimed row pointed at the session. A
/// session that serves no class claims nothing: an empty list would claim
/// every class.
///
/// The row's lease is a session lease from the claim, and is never renewed
/// on the row: it says when the claim was made, which is what
/// [`renew_session`] reads to find a claim the runner never heard of.
///
/// A row the session no longer exists to hold (it expired, and was swept,
/// between the two statements) is given back as it was before the claim, its
/// attempt with it: nothing ran.
pub async fn claim_for_session(
    pool: &Pool,
    session_id: &str,
    holder: &str,
    limit: i64,
    classes: &[String],
    pool_caps: &std::collections::BTreeMap<String, i64>,
) -> Result<Vec<TaskRun>> {
    if classes.is_empty() || limit <= 0 {
        return Ok(Vec::new());
    }
    let claimed = claim_ready_classes(pool, holder, limit, classes, pool_caps).await?;
    if claimed.is_empty() {
        return Ok(claimed);
    }
    let ids: Vec<&str> = claimed.iter().map(|t| t.id.as_str()).collect();
    let lease = (chrono::Utc::now()
        + chrono::TimeDelta::seconds(crate::runner::session_lease_secs()))
    .to_rfc3339();
    let stamped: Vec<String> = sqlx::query_scalar(
        "UPDATE task_runs t
         SET session_id = s.id, lease_expires_at = $4
         FROM runner_sessions s
         WHERE s.id = $1 AND s.holder = $2
           AND t.id = ANY($3) AND t.claimed_by = $2 AND t.status = 'running'
         RETURNING t.id",
    )
    .bind(session_id)
    .bind(holder)
    .bind(&ids)
    .bind(&lease)
    .fetch_all(pool)
    .await?;
    if stamped.len() == claimed.len() {
        return Ok(claimed);
    }
    let lost: Vec<&str> = ids.iter().copied().filter(|id| !stamped.iter().any(|s| s == id)).collect();
    sqlx::query(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL,
             attempt = GREATEST(attempt - 1, 0)
         WHERE id = ANY($1) AND claimed_by = $2 AND status = 'running' AND session_id IS NULL",
    )
    .bind(&lost)
    .bind(holder)
    .execute(pool)
    .await?;
    tracing::warn!(session_id, given_back = lost.len(), "the session was gone as its tasks were claimed: given back");
    Ok(claimed.into_iter().filter(|t| stamped.contains(&t.id)).collect())
}

/// Whether ready work of any of `classes` is due: asked before a session's
/// claim, which is a write, so that a session long-polling an empty queue
/// costs reads alone.
pub async fn ready_for_classes(pool: &Pool, classes: &[String]) -> Result<bool> {
    if classes.is_empty() {
        return Ok(false);
    }
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM task_runs
             WHERE status = 'ready' AND runner_class = ANY($1)
               AND (scheduled_at IS NULL OR scheduled_at <= $2)
         )",
    )
    .bind(classes)
    .bind(chrono::Utc::now().to_rfc3339())
    .fetch_one(pool)
    .await?)
}

/// What a session's heartbeat comes to ([`renew_session`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRenewed {
    pub lease_expires_at: String,
    /// Of the tasks the heartbeat named, those whose work should stop.
    pub cancel: Vec<String>,
    /// The session is to drain: by its runner's word, or an admin's.
    pub drain: bool,
}

/// A session's heartbeat: its lease renewed, and with it every task it holds,
/// in one statement, which also reads what the heartbeat named of its tasks
/// (a task row is not written). `None` where the session is not the
/// runner's, or its lease has already lapsed: its tasks are, or are about to
/// be, taken back, and the runner is to stop them and open another.
///
/// `held` is what the runner is running, each with its fence. Those to stop
/// are as an engine's are ([`super::not_held`]): asked to stop, or no longer
/// held under this session's claim and that fence.
///
/// A task the session holds and the heartbeat does not name, claimed longer
/// ago than [`ORPHAN_CLAIM_SECS`], is a claim the runner never heard of: the
/// answer to its claim was lost, or the task could not be made ready for it.
/// It is given back as it was before the claim, its attempt with it, since
/// nothing ran; or `cancelled`, where it was asked to stop. That second
/// statement is made only when there is such a task.
pub async fn renew_session(
    pool: &Pool,
    session_id: &str,
    runner_id: &str,
    held: &[(String, i64)],
) -> Result<Option<SessionRenewed>> {
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease = crate::runner::session_lease_secs();
    let until = (now_dt + chrono::TimeDelta::seconds(lease)).to_rfc3339();
    // A row's lease is its claim's time and a lease on: claimed before the
    // cutoff, its lease is before this.
    let orphaned_before =
        (now_dt + chrono::TimeDelta::seconds(lease - ORPHAN_CLAIM_SECS)).to_rfc3339();
    let ids: Vec<&str> = held.iter().map(|(id, _)| id.as_str()).collect();
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        String,
        Option<String>,
        bool,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "WITH s AS (
             UPDATE runner_sessions SET lease_expires_at = $3
             WHERE id = $1 AND runner_id = $2 AND lease_expires_at >= $4
             RETURNING holder, draining_at
         )
         SELECT s.holder, s.draining_at, true,
                t.id, t.version, t.claimed_by, t.status, t.stop_requested_at
         FROM s LEFT JOIN task_runs t ON t.id = ANY($5)
         UNION ALL
         SELECT s.holder, s.draining_at, false,
                t.id, t.version, t.claimed_by, t.status, t.stop_requested_at
         FROM s JOIN task_runs t
           ON t.session_id = $1 AND t.status = 'running' AND t.claimed_by = s.holder
         WHERE NOT (t.id = ANY($5)) AND t.lease_expires_at < $6
           AND NOT EXISTS (SELECT 1 FROM runner_results r WHERE r.task_id = t.id)",
    )
    .bind(session_id)
    .bind(runner_id)
    .bind(&until)
    .bind(&now)
    .bind(&ids)
    .bind(&orphaned_before)
    .fetch_all(pool)
    .await?;
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let holder = first.0.clone();
    let drain = first.1.is_some();
    let mut named = Vec::new();
    let mut orphans: Vec<(String, i64, bool)> = Vec::new();
    for (_, _, was_named, id, version, by, status, asked) in rows {
        let (Some(id), Some(version), Some(status)) = (id, version, status) else {
            continue;
        };
        if was_named {
            named.push((id, version, by, status, asked));
        } else {
            orphans.push((id, version, asked.is_some()));
        }
    }
    if !orphans.is_empty() {
        let given_back: Vec<&str> =
            orphans.iter().filter(|(_, _, asked)| !asked).map(|(id, _, _)| id.as_str()).collect();
        if !given_back.is_empty() {
            let runs: Vec<String> = sqlx::query_scalar(
                "UPDATE task_runs
                 SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL,
                     session_id = NULL, attempt = GREATEST(attempt - 1, 0)
                 WHERE id = ANY($1) AND claimed_by = $2 AND session_id = $3
                   AND status = 'running' AND stop_requested_at IS NULL
                 RETURNING run_id",
            )
            .bind(&given_back)
            .bind(&holder)
            .bind(session_id)
            .fetch_all(pool)
            .await?;
            tracing::warn!(
                session_id, given_back = runs.len(),
                "a session held tasks its runner never named: given back"
            );
            notify_runs(pool, &runs).await?;
        }
        for (id, fence, _) in orphans.iter().filter(|(_, _, asked)| *asked) {
            settle_stopped_task(pool, id, &holder, *fence).await?;
        }
    }
    Ok(Some(SessionRenewed {
        lease_expires_at: until,
        cancel: super::not_held(&holder, held, named),
        drain,
    }))
}

/// What became of a runner's word on how a task came out
/// ([`record_runner_result`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerResultRecorded {
    /// Kept for an engine to apply; the task's run, which was told.
    Recorded(String),
    /// The same word, under the same fence, was kept already.
    Duplicate,
    /// The claim is not the session's any more, under that fence: the word
    /// is too late, and changes nothing.
    Stale,
}

/// Keep what a runner said of a task, for an engine to apply
/// ([`take_runner_results`]). Kept only while the task is still held under
/// the session's claim (`holder`) and `fence`; a word on an older claim of
/// the same task, never applied, is replaced. The run is told, so that an
/// engine wakes for it.
pub async fn record_runner_result(
    pool: &Pool,
    task_id: &str,
    holder: &str,
    fence: i64,
    outcome: crate::runner::Outcome,
    exit_code: Option<i32>,
    output: &str,
) -> Result<RunnerResultRecorded> {
    let now = chrono::Utc::now().to_rfc3339();
    let row: Option<(String, i64)> = sqlx::query_as(
        "WITH t AS (
             SELECT id, run_id, version, claimed_by FROM task_runs
             WHERE id = $1 AND claimed_by = $2 AND version = $3
               AND status = 'running' AND session_id IS NOT NULL
         ), kept AS (
             INSERT INTO runner_results
                 (task_id, fence, claimed_by, status, exit_code, output, created_at)
             SELECT id, version, claimed_by, $4, $5, $6, $7 FROM t
             ON CONFLICT (task_id) DO UPDATE
                 SET fence = EXCLUDED.fence, claimed_by = EXCLUDED.claimed_by,
                     status = EXCLUDED.status, exit_code = EXCLUDED.exit_code,
                     output = EXCLUDED.output, created_at = EXCLUDED.created_at,
                     taken_by = NULL, taken_at = NULL
                 WHERE runner_results.fence < EXCLUDED.fence
             RETURNING task_id
         )
         SELECT t.run_id, (SELECT COUNT(*) FROM kept) FROM t",
    )
    .bind(task_id)
    .bind(holder)
    .bind(fence)
    .bind(outcome.as_str())
    .bind(exit_code)
    .bind(output)
    .bind(&now)
    .fetch_optional(pool)
    .await?;
    match row {
        Some((run_id, kept)) if kept > 0 => {
            notify(pool, &run_id).await?;
            Ok(RunnerResultRecorded::Recorded(run_id))
        }
        // Held, and a word under this fence is kept already.
        Some(_) => Ok(RunnerResultRecorded::Duplicate),
        None => Ok(RunnerResultRecorded::Stale),
    }
}

/// What became of a segment of a session's task's log
/// ([`append_session_log`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionLogKept {
    /// Written; the task's run, which the caller tells of.
    Appended(String),
    /// This segment, under this fence, was written already: sent again by a
    /// runner that did not hear the answer. Nothing is written.
    Duplicate,
    /// The task is not held under the session's claim and that fence, or the
    /// segment would take its log past the most that is kept of one.
    NotKept,
}

/// Add segment `seq` of what a session's task wrote to its live log. A
/// `reset` segment, and segment 0, replace the log: the attempt's first, or a
/// seal (the whole log over again, redacted as one piece). Only while the
/// task is held under the session's claim and `fence`, and only up to `most`
/// bytes of log: the runner keeps to its cap and the end it keeps past it
/// itself, and this is the floor under them
/// ([`crate::runner::session_log_most`]).
///
/// A runner numbers an attempt's segments 0, 1, 2, …, a seal's among them,
/// and the numbers only rise; one it did not hear the answer to it sends
/// again, the same text under the same number. The last segment written, and
/// its fence, are kept with the task, so a segment numbered at or below it
/// under the same fence is one written already
/// ([`SessionLogKept::Duplicate`]): the same one sent again, or one held up
/// on its way until a later one, a seal, say, was written. One numbered past
/// the next is written: its text is new, and a runner moves on only once it
/// has been answered or has given a segment up for a seal.
#[allow(clippy::too_many_arguments)]
pub async fn append_session_log(
    pool: &Pool,
    task_id: &str,
    holder: &str,
    fence: i64,
    seq: u64,
    reset: bool,
    text: &str,
    most: usize,
) -> Result<SessionLogKept> {
    let seq = i64::try_from(seq).unwrap_or(i64::MAX);
    let reset = reset || seq == 0;
    let mut tx = pool.begin().await?;
    // Locked, so that a segment sent again while the first is still being
    // written waits for it and then reads it as written.
    let held: Option<(String, bool, i64)> = sqlx::query_as(
        "SELECT run_id,
                COALESCE(log_fence = $3 AND $4 <= log_seq, FALSE),
                COALESCE(octet_length(log), 0)::BIGINT
         FROM task_runs
         WHERE id = $1 AND claimed_by = $2 AND version = $3 AND status = 'running'
         FOR UPDATE",
    )
    .bind(task_id)
    .bind(holder)
    .bind(fence)
    .bind(seq)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((run_id, written, len)) = held else {
        return Ok(SessionLogKept::NotKept);
    };
    if written {
        return Ok(SessionLogKept::Duplicate);
    }
    let len = if reset { 0 } else { len };
    if len.saturating_add(text.len() as i64) > i64::try_from(most).unwrap_or(i64::MAX) {
        return Ok(SessionLogKept::NotKept);
    }
    sqlx::query(
        "UPDATE task_runs
         SET log = CASE WHEN $5 THEN $4 ELSE COALESCE(log, '') || $4 END,
             log_seq = $3, log_fence = $2,
             log_gen = CASE WHEN $5 THEN COALESCE(log_gen, 0) + 1 ELSE log_gen END
         WHERE id = $1",
    )
    .bind(task_id)
    .bind(fence)
    .bind(seq)
    .bind(text)
    .bind(reset)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(SessionLogKept::Appended(run_id))
}

/// How many tasks a session holds now, its results not yet applied among
/// them: what its claims are counted against.
pub async fn session_running(pool: &Pool, session_id: &str, holder: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_runs
         WHERE session_id = $1 AND status = 'running' AND claimed_by = $2",
    )
    .bind(session_id)
    .bind(holder)
    .fetch_one(pool)
    .await?)
}

/// The work there is for each of `labels`: ready tasks of the class,
/// those sessions hold, and the live sessions that serve it and what they run
/// at once together. One row a label, in the order asked.
pub async fn runner_queue(
    pool: &Pool,
    labels: &[String],
) -> Result<Vec<crate::runner::LabelQueue>> {
    let now = chrono::Utc::now().to_rfc3339();
    let rows: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT l.label,
                (SELECT COUNT(*) FROM task_runs t
                 WHERE t.status = 'ready' AND t.runner_class = l.label
                   AND (t.scheduled_at IS NULL OR t.scheduled_at <= $2)),
                (SELECT COUNT(*) FROM task_runs t
                 WHERE t.status = 'running' AND t.session_id IS NOT NULL
                   AND t.claimed_by IS NOT NULL AND t.runner_class = l.label),
                (SELECT COUNT(*) FROM runner_sessions s
                 WHERE l.label = ANY(s.labels) AND s.lease_expires_at >= $2
                   AND s.draining_at IS NULL),
                (SELECT COALESCE(SUM(s.capacity), 0)::BIGINT FROM runner_sessions s
                 WHERE l.label = ANY(s.labels) AND s.lease_expires_at >= $2
                   AND s.draining_at IS NULL)
         FROM unnest($1::text[]) WITH ORDINALITY AS l(label, n)
         ORDER BY l.n",
    )
    .bind(labels)
    .bind(&now)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(label, ready, running, sessions, capacity)| crate::runner::LabelQueue {
            label,
            ready,
            running,
            sessions,
            capacity,
        })
        .collect())
}

/// Take back the tasks of sessions whose lease has lapsed: their runner died,
/// or was cut off. A task still held under its session's claim goes back to
/// `ready`, as one whose lease lapsed does ([`recover_expired_leases`]). One
/// that was asked to stop is `cancelled`, its run ended where it was the last
/// such task: nobody can watch its work now. A task whose result the runner
/// said, and no engine has applied yet, is left for that result. Then the
/// sessions that hold nothing more are dropped.
///
/// Returns how many tasks were taken back or cancelled.
pub async fn recover_expired_sessions(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    // Most sweeps find no lapsed session (and a SQLite engine never has one):
    // a read, and nothing written.
    let any: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM runner_sessions WHERE lease_expires_at < $1)",
    )
    .bind(&now)
    .fetch_one(pool)
    .await?;
    if !any {
        return Ok(0);
    }
    const HELD_BY_EXPIRED: &str = "status = 'running' AND session_id IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM runner_sessions s
               WHERE s.id = task_runs.session_id AND s.holder = task_runs.claimed_by
                 AND s.lease_expires_at < $1
           )
           AND NOT EXISTS (SELECT 1 FROM runner_results r WHERE r.task_id = task_runs.id)";
    let requeued = sqlx::query(&format!(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL, session_id = NULL
         WHERE {HELD_BY_EXPIRED} AND stop_requested_at IS NULL
         RETURNING run_id"
    ))
    .bind(&now)
    .fetch_all(pool)
    .await?;
    let mut seen = std::collections::HashSet::new();
    for row in &requeued {
        let run_id: String = row.try_get("run_id")?;
        if seen.insert(run_id.clone()) {
            notify(pool, &run_id).await?;
        }
    }
    let stopping: Vec<(String, String, i64)> = sqlx::query_as(&format!(
        "SELECT id, claimed_by, version FROM task_runs
         WHERE {HELD_BY_EXPIRED} AND stop_requested_at IS NOT NULL"
    ))
    .bind(&now)
    .fetch_all(pool)
    .await?;
    let mut cancelled = 0u64;
    for (task_id, holder, fence) in &stopping {
        if settle_stopped_task(pool, task_id, holder, *fence).await? {
            cancelled += 1;
        }
    }
    sqlx::query(
        "DELETE FROM runner_sessions s
         WHERE s.lease_expires_at < $1
           AND NOT EXISTS (
               SELECT 1 FROM task_runs t
               WHERE t.session_id = s.id AND t.status = 'running' AND t.claimed_by = s.holder
           )",
    )
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(requeued.len() as u64 + cancelled)
}

/// Take up to `limit` of what remote runners said of their tasks, for this
/// engine (`taker`) to apply. Each is the engine's until it drops it
/// ([`drop_runner_results`]), or for a minute: one that an engine took and did
/// not apply, because it died, is taken again. Applying one twice changes
/// nothing the second time: every mark it makes is fenced.
pub async fn take_runner_results(
    pool: &Pool,
    taker: &str,
    limit: i64,
) -> Result<Vec<crate::models::RunnerResult>> {
    let now_dt = chrono::Utc::now();
    let stale = (now_dt - chrono::TimeDelta::seconds(RUNNER_RESULT_RETAKE_SECS)).to_rfc3339();
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        String,
        i64,
        String,
        String,
        Option<i32>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
    )> = sqlx::query_as(
        "WITH taken AS (
             UPDATE runner_results SET taken_by = $1, taken_at = $2
             WHERE task_id IN (
                 SELECT task_id FROM runner_results
                 WHERE taken_at IS NULL OR taken_at < $3
                 ORDER BY created_at
                 LIMIT $4
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING task_id, fence, claimed_by, status, exit_code, output
         )
         SELECT taken.task_id, taken.fence, taken.claimed_by, taken.status,
                taken.exit_code, taken.output, t.run_id, t.attempt, t.input
         FROM taken LEFT JOIN task_runs t ON t.id = taken.task_id",
    )
    .bind(taker)
    .bind(now_dt.to_rfc3339())
    .bind(&stale)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(task_id, fence, claimed_by, outcome, exit_code, output, run_id, attempt, input)| {
                crate::models::RunnerResult {
                    task_id,
                    claimed_by,
                    fence,
                    outcome,
                    exit_code,
                    output,
                    run_id,
                    attempt: attempt.unwrap_or(1).saturating_sub(1).max(0),
                    input,
                }
            },
        )
        .collect())
}

/// Drop runners' results an engine has applied: each by its task and fence,
/// so that a newer word on the same task, kept since, is not dropped with it.
pub async fn drop_runner_results(pool: &Pool, applied: &[(String, i64)]) -> Result<()> {
    if applied.is_empty() {
        return Ok(());
    }
    let ids: Vec<&str> = applied.iter().map(|(id, _)| id.as_str()).collect();
    let fences: Vec<i64> = applied.iter().map(|(_, fence)| *fence).collect();
    sqlx::query(
        "DELETE FROM runner_results r
         USING unnest($1::text[], $2::bigint[]) AS a(task_id, fence)
         WHERE r.task_id = a.task_id AND r.fence = a.fence",
    )
    .bind(&ids)
    .bind(&fences)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod migration_lock_tests {
    //! The migration lock, live, each test on a database of its own: the
    //! lock's key is the database's, and a concurrent index build waits only
    //! on the sessions of its own database, so neither reaches the other live
    //! tests, which share TEST_DATABASE_URL's database.

    use super::*;
    use sqlx::migrate::{Migrate, Migrator};
    use sqlx::{Connection, PgConnection};

    /// A database made for one test, and dropped, with whatever is still
    /// connected to it, when the test ends, by a panic too.
    pub(super) struct OwnDatabase {
        admin_url: String,
        pub(super) name: String,
        pub(super) url: String,
    }

    impl OwnDatabase {
        pub(super) async fn new() -> Option<Self> {
            let admin_url = match std::env::var("TEST_DATABASE_URL") {
                Ok(u) if !u.trim().is_empty() => u,
                _ => {
                    eprintln!("TEST_DATABASE_URL unset - skipping a live-datastore test");
                    return None;
                }
            };
            let name = format!("mig_lock_{}", Uuid::new_v4().simple());
            let mut admin = PgConnection::connect(&admin_url).await.unwrap();
            let created = sqlx::query(&format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await;
            let _ = admin.close().await;
            if let Err(e) = created {
                // insufficient_privilege: a role without CREATEDB.
                if e.as_database_error().and_then(|db| db.code()).as_deref() == Some("42501") {
                    eprintln!("TEST_DATABASE_URL's role may not create a database - skipping");
                    return None;
                }
                panic!("CREATE DATABASE {name}: {e}");
            }
            let (head, query) = admin_url
                .split_once('?')
                .unwrap_or((admin_url.as_str(), ""));
            let (server, _) = head.rsplit_once('/').unwrap();
            let url = if query.is_empty() {
                format!("{server}/{name}")
            } else {
                format!("{server}/{name}?{query}")
            };
            Some(Self {
                admin_url,
                name,
                url,
            })
        }

        async fn connect(&self) -> PgConnection {
            PgConnection::connect(&self.url).await.unwrap()
        }
    }

    impl Drop for OwnDatabase {
        fn drop(&mut self) {
            let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
            // A thread and a runtime of its own: a drop cannot await, and the
            // test's runtime may be the one unwinding.
            let _ = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                        let _ =
                            sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                                .execute(&mut admin)
                                .await;
                        let _ = admin.close().await;
                    }
                });
            })
            .join();
        }
    }

    /// Whether the index named `index` is valid, or `None` when there is none.
    async fn index_valid(conn: &mut PgConnection, index: &str) -> Option<bool> {
        sqlx::query_scalar(
            "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
              WHERE c.relname = $1",
        )
        .bind(index)
        .fetch_optional(conn)
        .await
        .unwrap()
    }

    async fn unlock(conn: &mut PgConnection, key: i64) {
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .fetch_one(conn)
            .await
            .unwrap();
        assert!(released, "the lock was this session's");
    }

    #[test]
    fn the_key_s_crc_is_crc_32_iso_hdlc() {
        // The CRC catalogue's check value for CRC-32/ISO-HDLC.
        assert_eq!(crc32_iso_hdlc(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32_iso_hdlc(b""), 0);
    }

    /// sqlx's migration lock and this one are one lock: while either is
    /// held the other is not to be had, so an engine of an earlier release
    /// and one of this release migrate in turn.
    #[tokio::test]
    async fn sqlx_s_migration_lock_and_this_one_exclude_each_other() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let (mut sqlx_s, mut ours) = (db.connect().await, db.connect().await);
        let key = migration_lock_key(&db.name);

        sqlx_s.lock().await.unwrap();
        assert!(
            !try_migration_lock(&mut ours, key).await.unwrap(),
            "sqlx's lock holds this one off"
        );
        sqlx_s.unlock().await.unwrap();
        assert!(
            try_migration_lock(&mut ours, key).await.unwrap(),
            "free once sqlx's is let go"
        );

        // sqlx's waits in its statement while this one is held.
        let sqlx_waits = tokio::spawn(async move {
            sqlx_s.lock().await.unwrap();
            sqlx_s
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!sqlx_waits.is_finished(), "this lock holds sqlx's off");
        unlock(&mut ours, key).await;
        let mut sqlx_s = tokio::time::timeout(Duration::from_secs(10), sqlx_waits)
            .await
            .expect("sqlx's lock is taken once this one is let go")
            .unwrap();
        assert!(
            !try_migration_lock(&mut ours, key).await.unwrap(),
            "sqlx's holds again"
        );
        sqlx_s.unlock().await.unwrap();
        let _ = sqlx_s.close().await;
        let _ = ours.close().await;
    }

    /// What became of a concurrent index build that the migration lock's
    /// holder ran while another session waited for the lock.
    struct BuiltWhileWaited {
        build: std::result::Result<(), sqlx::Error>,
        index_valid: Option<bool>,
        waiter: Result<()>,
    }

    /// The holder takes the migration lock, another session comes to wait
    /// for it (as [`lock_migrations`] waits, or, with `sqlx_s_way`, as sqlx's
    /// migrator does), and the holder then builds an index concurrently, as a
    /// `-- no-transaction` migration does, and lets the lock go.
    async fn build_an_index_while_another_waits(
        db: &OwnDatabase,
        sqlx_s_way: bool,
    ) -> BuiltWhileWaited {
        let mut holder = db.connect().await;
        sqlx::query("CREATE TABLE things (id int)")
            .execute(&mut holder)
            .await
            .unwrap();
        sqlx::query("INSERT INTO things SELECT generate_series(1, 1000)")
            .execute(&mut holder)
            .await
            .unwrap();
        let key = lock_migrations(&mut holder).await.unwrap();
        let mut waiter = db.connect().await;
        let waits = tokio::spawn(async move {
            if sqlx_s_way {
                waiter.lock().await?;
            } else {
                lock_migrations(&mut waiter).await?;
            }
            Ok::<_, anyhow::Error>(waiter)
        });
        // Longer than deadlock_timeout (1 s by default): the waiter's own
        // deadlock check has run, and found nothing, before the build begins,
        // as when an engine starts while another is migrating. A cycle the
        // build then closes is the build's to break.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!waits.is_finished(), "the waiter waits for the lock");
        let build = tokio::time::timeout(
            Duration::from_secs(20),
            sqlx::query("CREATE INDEX CONCURRENTLY things_id ON things (id)").execute(&mut holder),
        )
        .await
        .expect("the index build ends within 20 s")
        .map(|_| ());
        unlock(&mut holder, key).await;
        let waiter = match tokio::time::timeout(Duration::from_secs(10), waits)
            .await
            .expect("the waiter ends within 10 s of the lock's release")
            .unwrap()
        {
            Ok(waiter) => waiter.close().await.map_err(anyhow::Error::from),
            Err(e) => Err(e),
        };
        let index_valid = index_valid(&mut holder, "things_id").await;
        let _ = holder.close().await;
        BuiltWhileWaited {
            build,
            index_valid,
            waiter,
        }
    }

    /// The deadlock this lock is polled to avoid: with another engine
    /// waiting for the migration lock, the holder's concurrent index build
    /// completes, and the index is valid. The waiter then takes the lock.
    #[tokio::test]
    async fn an_index_built_while_another_engine_waits_for_the_lock_completes() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let outcome = build_an_index_while_another_waits(&db, false).await;
        outcome.build.expect("the index build completes");
        assert_eq!(outcome.index_valid, Some(true), "the index is valid");
        outcome
            .waiter
            .expect("the waiter takes the lock once it is let go");
    }

    /// The same, with the waiter waiting as sqlx's migrator does, in one
    /// `SELECT pg_advisory_lock`: Postgres finds the build and the waiter
    /// waiting on each other and fails the build, which leaves the index
    /// INVALID. Ignored, for it shows the defect rather than guards against
    /// one: it is what makes the test above a test.
    #[tokio::test]
    #[ignore = "shows the deadlock sqlx's own lock meets; run by hand"]
    async fn an_index_built_while_another_engine_waits_sqlx_s_way_deadlocks() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let outcome = build_an_index_while_another_waits(&db, true).await;
        let err = outcome
            .build
            .expect_err("the index build is the deadlock's victim");
        eprintln!("the index build failed: {err}");
        let code = err
            .as_database_error()
            .and_then(|e| e.code().map(|c| c.into_owned()));
        assert_eq!(code.as_deref(), Some("40P01"), "deadlock_detected");
        assert_eq!(
            outcome.index_valid,
            Some(false),
            "the build left the index INVALID"
        );
        outcome
            .waiter
            .expect("the waiter takes the lock once it is let go");
    }

    /// Two engines migrating a fresh database at once, over a migration that
    /// builds an index concurrently: both succeed, one after the other, and
    /// the index is valid.
    #[tokio::test]
    async fn two_engines_migrating_at_once_over_a_concurrent_index_build_both_succeed() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let dir =
            std::env::temp_dir().join(format!("dagron_migration_lock_{}", Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        // The first migration outlasts deadlock_timeout, so the engine that
        // waits has waited past its own deadlock check when the index build
        // begins, as when an engine starts while another is migrating.
        std::fs::write(
            dir.join("1_things.sql"),
            "CREATE TABLE things (id int);\nSELECT pg_sleep(1.5);\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("2_things_id.sql"),
            "-- no-transaction\n\
             CREATE INDEX CONCURRENTLY IF NOT EXISTS things_id ON things (id);\n",
        )
        .unwrap();
        let (url, source) = (db.url.as_str(), dir.as_path());
        let engine = || async move {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(url)
                .await
                .unwrap();
            let mut migrator = Migrator::new(source).await.unwrap();
            let migrated = migrate(&pool, &mut migrator).await;
            pool.close().await;
            migrated
        };
        let (a, b) = tokio::join!(engine(), engine());
        // A third engine finds nothing to migrate and keeps its pool open, as
        // an engine does once it has migrated: the lock is to be had all the
        // same, so the connection that took it let it go.
        let third = PgPoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await
            .unwrap();
        let c = migrate(&third, &mut Migrator::new(source).await.unwrap()).await;
        let _ = std::fs::remove_dir_all(&dir);
        a.expect("the first engine migrates");
        b.expect("the second engine migrates");
        c.expect("the third engine finds nothing to migrate");
        let mut conn = db.connect().await;
        let key = migration_lock_key(&db.name);
        assert!(
            try_migration_lock(&mut conn, key).await.unwrap(),
            "the engine let the migration lock go"
        );
        unlock(&mut conn, key).await;
        third.close().await;
        assert_eq!(
            index_valid(&mut conn, "things_id").await,
            Some(true),
            "the index is valid"
        );
        let applied: Vec<(i64, bool)> =
            sqlx::query_as("SELECT version, success FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(applied, vec![(1, true), (2, true)]);
        let _ = conn.close().await;
    }
}

#[cfg(test)]
mod run_end_notify_tests {
    //! migrations_pg/107: a task that ends tells its run's listeners,
    //! whoever ended it. Each test on a database of its own, made from
    //! TEST_DATABASE_URL: the reap's slice here walks every running run,
    //! which on the shared database are other tests'.

    use super::migration_lock_tests::OwnDatabase;
    use super::*;

    /// The triggers 107 makes.
    const TRIGGERS: [&str; 3] = [
        "task_runs_ended_notify",
        "task_runs_made_over_notify",
        "task_runs_gone_notify",
    ];

    async fn fresh() -> Option<(OwnDatabase, Pool)> {
        let db = OwnDatabase::new().await?;
        let pool = init_pool(&db.url).await.unwrap();
        Some((db, pool))
    }

    async fn sql(pool: &Pool, text: &str, binds: &[&str]) {
        let mut q = sqlx::query(text);
        for b in binds {
            q = q.bind(*b);
        }
        q.execute(pool)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
    }

    /// Every notification sent before this was asked, taken. A marker is
    /// sent and taken: notifications reach a listener in the order their
    /// transactions committed, so what was sent before it has been taken
    /// with it. The runs named, the marker left out.
    async fn settle(pool: &Pool, w: &mut Waker) -> Vec<String> {
        let marker = format!("settle-{}", Uuid::new_v4());
        sql(pool, "SELECT pg_notify('task_events', $1)", &[&marker]).await;
        let mut runs = Vec::new();
        for _ in 0..500 {
            match w.take_woken().await {
                Woken::Runs(named) => runs.extend(named),
                Woken::All => panic!("the waker lost count of what it heard"),
            }
            if let Some(at) = runs.iter().position(|r| *r == marker) {
                runs.remove(at);
                return runs;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the marker never came");
    }

    async fn run_status(pool: &Pool, run: &str) -> String {
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(run)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The runs heard of since the last tick, reaped as a tick reaps them:
    /// `run` among them or not, and not ended.
    async fn tick_leaves(pool: &Pool, w: &mut Waker, run: &str, case: &str) {
        let named = settle(pool, w).await;
        let ended = reap_woken(pool, &Woken::Runs(named)).await.unwrap();
        assert!(
            !ended.iter().any(|(id, _)| id == run),
            "{case}: ended early: {ended:?}"
        );
        assert_eq!(run_status(pool, run).await, "running", "{case}");
    }

    /// With 107: `run` is heard of, and the reap of what was heard ends it
    /// so. Without: it is not, and is left for the slice.
    async fn tick_ends(
        pool: &Pool,
        w: &mut Waker,
        run: &str,
        status: RunStatus,
        case: &str,
        notified: bool,
    ) {
        let named = settle(pool, w).await;
        assert_eq!(
            named.iter().any(|id| id == run),
            notified,
            "{case}: {run} heard of: {named:?}"
        );
        let ended = reap_woken(pool, &Woken::Runs(named)).await.unwrap();
        if notified {
            assert!(
                ended.iter().any(|(id, s)| id == run && *s == status),
                "{case}: {run} should have ended {status}: {ended:?}"
            );
        } else {
            assert_eq!(run_status(pool, run).await, "running", "{case}");
            // What the slice of the reap finds: before 107, these ends.
            let (swept, _) = reap_slice(pool, None, 10_000).await.unwrap();
            assert_eq!(
                swept,
                vec![(run.to_string(), status)],
                "{case}: the slice's"
            );
        }
    }

    async fn run_of(pool: &Pool, yaml: &str) -> (String, HashMap<String, String>) {
        let run = create_run(pool, &DagGraph::from_yaml(yaml).unwrap(), yaml)
            .await
            .unwrap();
        let tasks: Vec<(String, String)> =
            sqlx::query_as("SELECT name, id FROM task_runs WHERE run_id = $1")
                .bind(&run)
                .fetch_all(pool)
                .await
                .unwrap();
        (run, tasks.into_iter().collect())
    }

    async fn claim_named(pool: &Pool, run: &str, name: &str) -> TaskRun {
        advance_ready_tasks(pool).await.unwrap();
        claim_ready(pool, "w", 100)
            .await
            .unwrap()
            .into_iter()
            .find(|t| t.run_id == run && t.name == name)
            .unwrap_or_else(|| panic!("{name} of {run} is not ready"))
    }

    /// The ends found to send no notification, each driven by the
    /// dagron-core function that makes it, with 107's triggers (`notified`)
    /// and without them. With them each run is heard of and ended by the
    /// reap of what was heard, and the slice, walked whole after, finds no
    /// run to end: `scheduler_runs_reaped_by_sweep_total` would count none.
    /// Without them none is heard of, and the slice ends each.
    async fn the_quiet_ends(pool: &Pool, notified: bool) {
        let mut waker = Waker::connect(pool).await.unwrap();
        assert_eq!(waker.take_woken().await, Woken::All);
        let w = &mut waker;
        {
            let case = "a dependent skipped after a failure";
            let yaml = "name: chain\ntasks:\n  - name: a\n    command: [\"false\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
            let (run, _) = run_of(pool, yaml).await;
            tick_leaves(pool, w, &run, case).await;
            let a = claim_named(pool, &run, "a").await;
            assert!(mark_task_failed(pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_leaves(pool, w, &run, case).await;
            advance_ready_tasks(pool).await.unwrap();
            tick_ends(pool, w, &run, RunStatus::Failed, case, notified).await;
        }
        {
            let case = "a trigger rule's skip";
            let yaml = "name: rule\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n    depends_on: [a]\n    trigger_rule: one_failed\n";
            let (run, _) = run_of(pool, yaml).await;
            tick_leaves(pool, w, &run, case).await;
            let a = claim_named(pool, &run, "a").await;
            assert!(mark_task_succeeded(pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_leaves(pool, w, &run, case).await;
            advance_ready_tasks(pool).await.unwrap();
            tick_ends(pool, w, &run, RunStatus::Succeeded, case, notified).await;
        }
        let fan = "name: fan\ntasks:\n  - name: list\n    command: [\"true\"]\n  - name: process\n    command: [\"handle\", \"{{ item }}\"]\n    with_output_of: list\n    depends_on: [list]\n";
        for (case, output) in [
            ("a fan-out of nothing", "[]"),
            ("a fan-out's barrier", r#"["x","y"]"#),
        ] {
            let (run, _) = run_of(pool, fan).await;
            tick_leaves(pool, w, &run, case).await;
            let list = claim_named(pool, &run, "list").await;
            assert!(mark_task_succeeded(
                pool,
                &list.id,
                "w",
                list.version + 1,
                Some(output.into())
            )
            .await
            .unwrap());
            advance_ready_tasks(pool).await.unwrap();
            tick_leaves(pool, w, &run, case).await;
            reconcile_fanouts(pool).await.unwrap();
            if output != "[]" {
                tick_leaves(pool, w, &run, case).await;
                for t in claim_ready(pool, "w", 10).await.unwrap() {
                    assert!(mark_task_succeeded(pool, &t.id, "w", t.version + 1, None)
                        .await
                        .unwrap());
                }
                tick_leaves(pool, w, &run, case).await;
                reconcile_fanouts(pool).await.unwrap();
            }
            tick_ends(pool, w, &run, RunStatus::Succeeded, case, notified).await;
        }
        for case in [
            "a time sensor",
            "an HTTP sensor",
            "a dataset sensor",
            "an external job",
            "an external job failed",
        ] {
            let yaml = match case {
                "a time sensor" => "name: s\ntasks:\n  - { name: s, type: wait, wait: { for: 1h } }\n",
                "an HTTP sensor" => "name: s\ntasks:\n  - { name: s, type: wait, wait: { url: \"https://example.com/ready\" } }\n",
                "a dataset sensor" => "name: s\ntasks:\n  - { name: s, type: wait, wait: { dataset: \"s3://lake/x\" } }\n",
                _ => "name: s\ntasks:\n  - { name: s, command: [\"true\"] }\n",
            };
            let (run, _) = run_of(pool, yaml).await;
            tick_leaves(pool, w, &run, case).await;
            let s = claim_named(pool, &run, "s").await;
            let fence = s.version + 1;
            let now = chrono::Utc::now().to_rfc3339();
            let status = match case {
                "a time sensor" => {
                    assert!(park_wait(pool, &s.id, fence, "2000-01-01T00:00:00+00:00")
                        .await
                        .unwrap());
                    tick_leaves(pool, w, &run, case).await;
                    assert_eq!(reconcile_waits(pool).await.unwrap().done.len(), 1);
                    RunStatus::Succeeded
                }
                "an HTTP sensor" => {
                    assert!(
                        park_wait_url(pool, &s.id, fence, "https://example.com/ready", &now)
                            .await
                            .unwrap()
                    );
                    tick_leaves(pool, w, &run, case).await;
                    assert!(resolve_url_wait(pool, &s.id).await.unwrap());
                    RunStatus::Succeeded
                }
                "a dataset sensor" => {
                    assert!(park_wait_dataset(pool, &s.id, fence, "s3://lake/x")
                        .await
                        .unwrap());
                    tick_leaves(pool, w, &run, case).await;
                    record_external_dataset_event(pool, "s3://lake/x")
                        .await
                        .unwrap();
                    assert_eq!(reconcile_dataset_waits(pool).await.unwrap().done.len(), 1);
                    RunStatus::Succeeded
                }
                _ => {
                    assert!(
                        park_external(pool, &s.id, fence, "http", "job-1", None, &now, None)
                            .await
                            .unwrap()
                    );
                    tick_leaves(pool, w, &run, case).await;
                    if case == "an external job" {
                        assert!(resolve_external(pool, &s.id, "job-1", Some("DONE"))
                            .await
                            .unwrap());
                        RunStatus::Succeeded
                    } else {
                        assert!(fail_external(pool, &s.id, "job-1", "OOM").await.unwrap());
                        RunStatus::Failed
                    }
                }
            };
            tick_ends(pool, w, &run, status, case, notified).await;
        }
        {
            let case = "a sub-workflow's trigger";
            let child = "name: child\ntasks:\n  - { name: c, command: [\"true\"] }\n";
            let parent =
                "name: parent\ntasks:\n  - { name: trigger, type: workflow, workflow: child }\n";
            let (run, _) = run_of(pool, parent).await;
            tick_leaves(pool, w, &run, case).await;
            let trigger = claim_named(pool, &run, "trigger").await;
            let (child_run, _) = run_of(pool, child).await;
            assert!(
                park_subworkflow(pool, &trigger.id, trigger.version + 1, &child_run)
                    .await
                    .unwrap()
            );
            tick_leaves(pool, w, &run, case).await;
            let c = claim_named(pool, &child_run, "c").await;
            assert!(mark_task_succeeded(pool, &c.id, "w", c.version + 1, None)
                .await
                .unwrap());
            // The child's own end notifies, as it did before 107.
            tick_ends(pool, w, &child_run, RunStatus::Succeeded, case, true).await;
            assert_eq!(reconcile_subworkflows(pool).await.unwrap().done.len(), 1);
            tick_ends(pool, w, &run, RunStatus::Succeeded, case, notified).await;
        }
        // Every run here has ended, by what was heard or by the slice.
        assert_eq!(
            reap_slice(pool, None, 10_000).await.unwrap(),
            (Vec::new(), None)
        );
    }

    /// With 107, each quiet end is heard of, and the slice finds nothing.
    #[tokio::test]
    async fn each_quiet_end_wakes_its_run() {
        let Some((_db, pool)) = fresh().await else {
            return;
        };
        the_quiet_ends(&pool, true).await;
        pool.close().await;
    }

    /// Without 107's triggers, as before it, none is heard of, and the
    /// slice ends each: what was found before 107, and what the test above
    /// would see were the triggers lost.
    #[tokio::test]
    async fn without_107_the_quiet_ends_are_the_slice_s() {
        let Some((_db, pool)) = fresh().await else {
            return;
        };
        for t in TRIGGERS {
            sql(&pool, &format!("DROP TRIGGER {t} ON task_runs"), &[]).await;
        }
        the_quiet_ends(&pool, false).await;
        pool.close().await;
    }

    /// Notifications of `run` a listener is sent once `act` has committed.
    async fn sent_for<F, Fut>(pool: &Pool, run: &str, act: F) -> usize
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut listener = PgListener::connect_with(pool).await.unwrap();
        listener.listen(EVENT_CHANNEL).await.unwrap();
        act().await;
        let marker = format!("count-{}", Uuid::new_v4());
        sql(pool, "SELECT pg_notify('task_events', $1)", &[&marker]).await;
        let mut n = 0;
        loop {
            let one = tokio::time::timeout(Duration::from_secs(10), listener.recv())
                .await
                .expect("the marker never came")
                .unwrap();
            match one.payload() {
                p if p == marker => return n,
                p if p == run => n += 1,
                _ => {}
            }
        }
    }

    /// What 107 adds to what is sent: a task that ends in a transaction
    /// that notifies already adds nothing (Postgres folds a transaction's
    /// equal notifications into one); a cancel of a run's two thousand
    /// tasks is one notification; an end that notified nobody is one; an
    /// end whose function notifies after its commit, in a transaction of
    /// its own, is two. And the payload is the run, on the engine's channel.
    /// With `ops` alone: the cancel is `cancel_run`.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn what_107_sends() {
        let Some((_db, pool)) = fresh().await else {
            return;
        };
        let one = "name: one\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let (run, _) = run_of(&pool, one).await;
        let a = claim_named(&pool, &run, "a").await;
        let n = sent_for(&pool, &run, || async {
            assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
        })
        .await;
        assert_eq!(n, 1, "an end that notified in its transaction");

        let (big, _) = run_of(&pool, one).await;
        sql(
            &pool,
            "INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
             SELECT 'big-' || n || '-' || $1, $1, 'b' || n, 'running', 0, '{}'
             FROM generate_series(1, 2000) AS n",
            &[&big],
        )
        .await;
        let n = sent_for(&pool, &big, || async {
            assert!(cancel_run(&pool, &big).await.unwrap().is_some());
        })
        .await;
        assert_eq!(n, 1, "a cancel of two thousand tasks");

        let (quiet, tasks) = run_of(&pool, one).await;
        let n = sent_for(&pool, &quiet, || async {
            sql(
                &pool,
                "UPDATE task_runs SET status = 'skipped' WHERE id = $1",
                &[&tasks["a"]],
            )
            .await;
        })
        .await;
        assert_eq!(n, 1, "an end by a statement that notifies nobody");

        // Not finished to finished alone: a task already over, written
        // over again, and a task's move that is not an end, send nothing.
        let n = sent_for(&pool, &quiet, || async {
            sql(
                &pool,
                "UPDATE task_runs SET status = 'failed' WHERE id = $1",
                &[&tasks["a"]],
            )
            .await;
        })
        .await;
        assert_eq!(n, 0, "an end written over an end");
        let (moving, tasks) = run_of(&pool, one).await;
        let n = sent_for(&pool, &moving, || async {
            sql(
                &pool,
                "UPDATE task_runs SET status = 'ready' WHERE id = $1",
                &[&tasks["a"]],
            )
            .await;
        })
        .await;
        assert_eq!(n, 0, "a move that is not an end");
        // A task not over taken away, and a task made over.
        let n = sent_for(&pool, &moving, || async {
            sql(&pool, "DELETE FROM task_runs WHERE id = $1", &[&tasks["a"]]).await;
            sql(
                &pool,
                "INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
                 VALUES ($1 || '-done', $1, 'done', 'succeeded', 0, '{}')",
                &[&moving],
            )
            .await;
        })
        .await;
        assert_eq!(n, 2, "a task not over taken away, and one made over");
        pool.close().await;
    }

    /// 107's text names the channel the waker and dagron-api listen on.
    #[test]
    fn the_triggers_notify_the_engine_s_channel() {
        let sql = include_str!("../../migrations_pg/107_task_runs_end_notify.sql");
        assert!(sql.contains(&format!("pg_notify('{EVENT_CHANNEL}', NEW.run_id)")));
        assert!(sql.contains(&format!("pg_notify('{EVENT_CHANNEL}', OLD.run_id)")));
        for t in TRIGGERS {
            assert!(sql.contains(&format!("CREATE TRIGGER {t} ")), "{t}");
        }
    }
}
