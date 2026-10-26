//! SQLite backend (v0/v1, default). Single-writer, optimistic-concurrency claim.
//!
//! Correctness comes from CAS on `version`: this path is already safe for the
//! v2 multi-worker model, it just contends harder than the Postgres
//! `FOR UPDATE SKIP LOCKED` path. SQLite has no `LISTEN/NOTIFY`, so the
//! reconcile loop wakes on a fixed-interval timer, and the file's triggers
//! name the runs a change may have ended (see [`Waker`]).

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
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

    // Two processes open one file on one node, and an engine and dagron-api
    // started together race to open it, which SQLite does not always wait
    // out:
    //  - a connection's pragmas (WAL) need a write lock on a file another
    //    process is writing while it is still in rollback mode, as it is when
    //    just made, and SQLite refuses that at once rather than deadlock
    //    (`database is locked`, past the busy timeout);
    //  - SQLite has no advisory lock to hold migrations to one process at a
    //    time, and the one that loses fails on `_sqlx_migrations`' key or on a
    //    table the other made. Each migration is its own transaction (none here
    //    opts out), so the loser's is rolled back whole.
    // So the open, connection and migrations, is tried again a few times.
    let mut attempt: u64 = 1;
    loop {
        match open(&opts).await {
            Ok(pool) => return Ok(pool),
            Err(e) if attempt < 5 => {
                tracing::warn!(attempt, error = %e, "opening the SQLite datastore; trying again");
                tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One connection to the file, migrated.
async fn open(opts: &SqliteConnectOptions) -> Result<Pool> {
    // One connection: SQLite is single-writer, and `claim_ready` reads-then-writes
    // in a deferred transaction. With multiple pool connections that read→write
    // upgrade can lose the write lock to a sibling connection and fail *instantly*
    // with SQLITE_BUSY (a busy timeout cannot rescue a lock upgrade). Serializing
    // all access through one connection removes that race entirely; WAL still lets
    // outside readers (ops queries, probes) run without blocking the writer. The
    // Postgres backend is the path for real write concurrency.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts.clone())
        .await?;
    migrate(&pool).await?;
    Ok(pool)
}

async fn migrate(pool: &Pool) -> Result<()> {
    // The base and enterprise (900+) sets share sqlx's single `_sqlx_migrations`
    // table. ignore_missing lets the base migrator tolerate the enterprise rows it
    // does not own; without it an enterprise DB (900 already applied) fails the base
    // migrator on every reboot with "previously applied but missing in the resolved
    // migrations" — before the enterprise migrator runs.
    let mut base = sqlx::migrate!("./migrations");
    base.set_ignore_missing(true);
    base.run(pool).await?;
    #[cfg(feature = "enterprise")]
    {
        // enterprise migrations share sqlx's single `_sqlx_migrations` table with the
        // base set; their versions are offset above the base range (900+) and
        // ignore_missing lets this migrator tolerate the base migrations it does
        // not own. Mirrors the Postgres path in db/postgres.rs.
        let mut ee = sqlx::migrate!("./migrations_ee");
        ee.set_ignore_missing(true);
        ee.run(pool).await?;
    }
    Ok(())
}

/// Readiness probe: one round trip proving a pooled connection can be acquired
/// and the datastore answers. Used by the ops API's `/readyz` (LOW_LATENCY R-1).
pub async fn ping(pool: &Pool) -> Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}

/// The indexes a build left unusable. None on SQLite: its index builds run
/// inside their migration's transaction, and one that fails leaves nothing.
pub async fn invalid_indexes(_pool: &Pool) -> Result<Vec<String>> {
    Ok(Vec::new())
}

/// Inserts a workflow_definition + workflow_run + all task_runs + dependency edges
/// in a single transaction. Returns the new run_id.
pub async fn create_run(pool: &Pool, dag: &DagGraph, yaml_spec: &str) -> Result<String> {
    create_run_inner(pool, dag, yaml_spec, None, None, None).await
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
    create_run_inner(pool, dag, yaml_spec, None, None, Some(trigger)).await
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
    create_run_inner(
        pool,
        dag,
        yaml_spec,
        Some((source_name, position)),
        None,
        trigger,
    )
    .await
}

/// [`create_run`] that deletes the dead letter `dead_letter_id` **in the same
/// transaction**: the redrive primitive. The run and the delete commit together
/// or not at all, so a refusal after the claim (the run cap, any error) rolls
/// the delete back as well and leaves the dead letter exactly as it was, with
/// the same id and history, ready to redrive again. The free-disk floor refuses
/// before the claim, with the same result. `Ok(None)` when the row is already
/// gone (redriven or discarded first), and then nothing is created.
///
/// The delete is the claim. SQLite has one writer, so a concurrent redrive of
/// the same id waits for the holder's transaction and then finds the row gone:
/// a dead letter becomes at most one run.
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
    match create_run_inner(pool, dag, yaml_spec, None, Some(dead_letter_id), trigger).await {
        Ok(run_id) => Ok(Some(run_id)),
        Err(e) if e.is::<super::DeadLetterGone>() => Ok(None),
        Err(e) => Err(e),
    }
}

/// The run writer behind [`create_run`], [`create_run_triggered`],
/// [`create_run_with_offset`] and `create_run_from_dead_letter`, with the
/// free-disk floor from `DAGRON_MIN_FREE_BYTES`. Besides the run, `offset`
/// commits a source cursor, `dead_letter` deletes that dead letter first, as
/// the claim, and `trigger` records what started the run, all in the one
/// transaction.
/// [`create_run_triggered`] for a run a gateway's caller submitted: who that
/// was is kept on the run as well (`triggered_by`), in the same insert. As
/// the Postgres backend's, for dagron-api on SQLite.
pub async fn create_run_triggered_by(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    triggered_by: &str,
    trigger: &crate::models::RunTrigger,
) -> Result<String> {
    create_run_full(pool, dag, yaml_spec, None, None, Some(triggered_by), Some(trigger), None, min_free_bytes()).await
}

/// [`create_run_triggered_by`] for a submit made under an `Idempotency-Key`:
/// the caller's claim on the key is resolved to the run in the same
/// transaction, so the run and the key's answer commit together, or neither
/// does. `Ok(None)` when the claim is no longer this submit's (taken over once
/// it went stale, or released): then nothing is written. As the Postgres
/// backend's, for dagron-api on SQLite.
pub async fn create_run_claimed(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    triggered_by: Option<&str>,
    trigger: Option<&crate::models::RunTrigger>,
    claim: &super::IdempotencyClaim<'_>,
) -> Result<Option<String>> {
    match create_run_full(pool, dag, yaml_spec, None, None, triggered_by, trigger, Some(claim), min_free_bytes()).await {
        Ok(run_id) => Ok(Some(run_id)),
        Err(e) if e.is::<super::IdempotencyClaimLost>() => Ok(None),
        Err(e) => Err(e),
    }
}

async fn create_run_inner(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    offset: Option<(&str, &str)>,
    dead_letter: Option<&str>,
    trigger: Option<&crate::models::RunTrigger>,
) -> Result<String> {
    create_run_inner_with_floor(
        pool,
        dag,
        yaml_spec,
        offset,
        dead_letter,
        trigger,
        min_free_bytes(),
    )
    .await
}

/// `DAGRON_MIN_FREE_BYTES` — the free-disk admission floor in bytes, `0` = off
/// (the default; the `edge` profile sets 64 MiB). Read once: boot-immutable
/// like every other knob, and the create path must not pay an env lookup per
/// run. Tests inject a floor through [`create_run_inner_with_floor`] rather
/// than the environment, for the same reason `lease_secs()` is never tested
/// by setting the variable after first use.
fn min_free_bytes() -> u64 {
    static FLOOR: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *FLOOR.get_or_init(|| {
        std::env::var("DAGRON_MIN_FREE_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0)
    })
}

/// Refuse admission when the datastore's filesystem has under `floor` bytes
/// free. Constrained hosts: a flash device that fills up corrupts the WAL
/// rather than refusing writes, so the floor keeps headroom for the runs
/// already in flight and for the checkpoint — new runs are the one write it
/// is safe to say no to.
///
/// Probes the directory holding the database file, taken from *this pool's*
/// connect options — never a process-global path — so a second pool in the
/// same process (tests, the crash-recovery harness) probes its own
/// filesystem. FAILS OPEN with one warning if the probe itself errors: a
/// probe that cannot run is not evidence that the disk is full, and refusing
/// every run over a permissions mistake would be a worse outage than the one
/// the floor prevents.
fn check_disk_floor(pool: &Pool, floor: u64) -> Result<()> {
    if floor == 0 {
        return Ok(());
    }
    let opts = pool.connect_options();
    let dir = match opts.get_filename().parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        // A bare filename (`workflow.db`) lives in the working directory.
        _ => std::path::PathBuf::from("."),
    };
    match super::free_bytes(&dir) {
        Ok(free) if free < floor => {
            Err(anyhow::Error::new(crate::models::DatastoreLowOnDisk { free, floor }))
        }
        Ok(_) => Ok(()),
        Err(e) => {
            static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
            if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(
                    dir = %dir.display(), error = %e, floor,
                    "free-disk probe failed — the DAGRON_MIN_FREE_BYTES floor fails open (warned once)"
                );
            }
            Ok(())
        }
    }
}

/// [`create_run_inner`] with the free-disk floor injected — the seam the
/// floor is tested through. `floor == 0` skips the probe entirely.
async fn create_run_inner_with_floor(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    offset: Option<(&str, &str)>,
    dead_letter: Option<&str>,
    trigger: Option<&crate::models::RunTrigger>,
    floor: u64,
) -> Result<String> {
    create_run_full(pool, dag, yaml_spec, offset, dead_letter, None, trigger, None, floor).await
}

/// Every way a run is made: from a source's position (`offset`), a dead
/// letter's redrive (`dead_letter`), a gateway's caller (`triggered_by`,
/// `trigger`), under an `Idempotency-Key` (`claim`), below a free-disk
/// `floor`.
#[allow(clippy::too_many_arguments)]
async fn create_run_full(
    pool: &Pool,
    dag: &DagGraph,
    yaml_spec: &str,
    offset: Option<(&str, &str)>,
    dead_letter: Option<&str>,
    triggered_by: Option<&str>,
    trigger: Option<&crate::models::RunTrigger>,
    claim: Option<&super::IdempotencyClaim<'_>>,
    floor: u64,
) -> Result<String> {
    // Before the transaction, before any id is minted: a refused run must
    // leave nothing behind — no definition row, no moved source cursor, no
    // consumed dead letter.
    check_disk_floor(pool, floor)?;
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

    // The caller's `Idempotency-Key` claim (`create_run_claimed`), held by the
    // transaction's first statement: a write that changes nothing, so the
    // transaction holds SQLite's write lock from here, and no other writer
    // (an engine on the same file) can come between this check and the run's
    // rows, where a read first would have to upgrade its lock and could lose.
    // A claim that is not this submit's any more ends it here, with nothing
    // written.
    if let Some(claim) = claim {
        let ours = sqlx::query(
            "UPDATE run_idempotency SET created_at = created_at
              WHERE principal = ? AND idempotency_key = ? AND run_id IS NULL AND created_at = ?",
        )
        .bind(claim.principal)
        .bind(claim.key)
        .bind(claim.claimed_at)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if ours != 1 {
            return Err(anyhow::Error::new(super::IdempotencyClaimLost));
        }
    }

    // Redrive claim (`create_run_from_dead_letter`), the transaction's first
    // statement. Everything below that refuses the run rolls it back with the
    // rest, so the dead letter is only ever gone together with a created run.
    if let Some(id) = dead_letter {
        let claimed = sqlx::query("DELETE FROM dead_letters WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if claimed == 0 {
            return Err(anyhow::Error::new(super::DeadLetterGone));
        }
    }

    // Per-workflow concurrency cap (#21): refuse to start a run if this workflow
    // (by name) already has `max_active_runs` runs in flight. Checked inside the
    // write transaction, so on single-writer SQLite it is race-free; the count
    // filters the small set of `running` runs and PK-joins their definitions.
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
    // The dependency a waiting run's tasks are held by, which none of them has.
    let held_by = i64::from(waits);

    sqlx::query(
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES (?, ?, ?, ?)",
    )
    .bind(&def_id)
    .bind(&dag.spec.name)
    .bind(yaml_spec)
    .bind(&now)
    .execute(&mut *tx)
    .await?;

    // Clock confidence (migration 041): whatever this process currently knows
    // about its own wall clock rides the row. `unknown` is a stored value, not
    // NULL, so "never assessed" stays distinguishable from "written before
    // the column existed". See `crate::clock`.
    let clock = crate::clock::current();
    // The parameters the run was created with (migration 052), for templates
    // resolved after submit. Stored even when empty: `{}` says "none", NULL
    // says "written before the column existed".
    let parameters = serde_json::to_string(&dag.parameters)?;
    // A `notify.git` run's `pending` status is owed from the moment it exists
    // (migration 054); an engine with a forge token posts it.
    let start_pending_at = dag
        .spec
        .notify
        .as_ref()
        .and_then(|n| n.git.as_ref())
        .map(|_| &now);
    sqlx::query(
        "INSERT INTO workflow_runs
           (id, definition_id, status, created_at, deadline_at, alert_deadline_at, result_from, environment,
            clock_confidence, clock_offset_ms, clock_source, concurrency_key, parameters, start_pending_at,
            queued_at, triggered_by)
         VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    .bind(&parameters)
    .bind(start_pending_at)
    .bind(waits.then_some(&now))
    .bind(triggered_by)
    .execute(&mut *tx)
    .await?;

    // What started the run (migration 057), with it or not at all.
    if let Some(t) = trigger {
        sqlx::query(
            "INSERT INTO run_triggers
               (run_id, kind, principal, actor, trust, source, forge_host, repo, ref, sha, base_sha, pr,
                event_id, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run_id: &str,
        now: &str,
        rows: &[TaskRow],
    ) -> Result<()> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
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
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        edges: &[(String, String)],
    ) -> Result<()> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
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

    // The key's answer rides it too: the claim held above now names the run.
    if let Some(claim) = claim {
        let resolved = sqlx::query(
            "UPDATE run_idempotency SET run_id = ?
              WHERE principal = ? AND idempotency_key = ? AND run_id IS NULL AND created_at = ?",
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

/// The lease sweep's statement ([`recover_expired_leases`]). A const, as each
/// sweep's read is here, so the plan tests read the text it runs.
pub(crate) const EXPIRED_LEASES: &str = "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL
         WHERE status = 'running'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < ?
           AND stop_requested_at IS NULL
           AND session_id IS NULL";

/// Reclaim tasks whose worker lease expired — the core crash-recovery primitive.
///
/// A task that was asked to stop is not one of them: it must not run again.
/// Its lapsed lease is [`claim_teardowns`]'s to take.
pub async fn recover_expired_leases(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    let r = sqlx::query(EXPIRED_LEASES).bind(&now).execute(pool).await?;
    Ok(r.rows_affected())
}

/// Advance pending tasks whose dependencies are all terminal
/// (`remaining_deps == 0`) to `ready` or `skipped`, per their `trigger_rule`.
/// Returns the number of tasks transitioned.
///
/// (LOW_LATENCY B-1) Two paths share the work. The **fast path** is one
/// set-based UPDATE pair covering the default shape — `trigger_rule =
/// 'all_success'`, not an approval gate, no runtime `when` — which is nearly
/// every task: ready when every dependency succeeded, skipped otherwise, with
/// skipped rows' dependents decremented atomically with the skip itself. The
/// old per-candidate implementation issued a dependency-status SELECT plus an
/// UPDATE per task, serially — a 200-wide fan-out becoming ready cost ~400
/// sequential round trips on the dispatch critical path. Everything not
/// *provably* default-shaped (custom rules, approvals, anything whose stored
/// input merely *contains* a `"when"` key — the LIKE filter is deliberately
/// conservative) takes the **slow path**: the original per-task evaluation,
/// byte-for-byte, so exotic semantics cannot drift.
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
        let any: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM task_runs WHERE status = 'pending' AND remaining_deps = 0 LIMIT 1",
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

/// One set-based pass over the default-shaped frontier — the Postgres twin's
/// rationale applies verbatim (see [`advance_ready_tasks`] and
/// `db/postgres.rs`); only the parameter syntax and the skip-decrement shape
/// differ (SQLite has no array binds, so skipped rows decrement their
/// dependents one terminal task at a time — exactly the old per-task cost, on
/// what is the rare path).
async fn advance_default_shaped(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    let readied = sqlx::query(
        // `fanout_of IS NULL` keeps a runtime fan-out barrier out of the fast
        // path, the same way `is_approval = 0` does: a barrier must never
        // become `ready`, because `ready` is exactly what `claim_ready` picks
        // up — and the barrier's command is the *template* for its instances,
        // not something to run. Excluding it here rather than adding a
        // predicate to the claim means the hot claim path is untouched.
        // Only the ready branch needs it; a barrier whose dependencies failed
        // is skipped by the branch below like any other task.
        "UPDATE task_runs SET status = 'ready', scheduled_at = ?
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

    // The skip and its dependent-decrements are ONE transaction — a skip is
    // terminal, and the old per-task path made the pair atomic. Split across
    // auto-commit statements, a crash between them would leave the skipped
    // rows terminal but their dependents' `remaining_deps` inflated forever:
    // nothing revisits a terminal task, so the run stalls permanently.
    let mut tx = pool.begin().await?;
    let skipped: Vec<String> = sqlx::query_scalar(
        "UPDATE task_runs SET status = 'skipped', finished_at = ?
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
    for id in &skipped {
        sqlx::query(
            "UPDATE task_runs SET remaining_deps = remaining_deps - 1
             WHERE status = 'pending' AND id IN (
                 SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
             )",
        )
        .bind(id)
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
    // (id, run_id, trigger_rule, is_approval, input) for every task whose deps
    // are all terminal.
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
            let rows = if fanout_of.is_some() {
                // A runtime fan-out barrier (`with_output_of:`) parks instead
                // of going `ready`: `status = 'running'` with claim and lease
                // NULL, the shape every other park uses, so the claim scan
                // skips it and lease recovery provably cannot reclaim it
                // (`recover_expired_leases` filters `lease_expires_at IS NOT
                // NULL`). `reconcile_fanouts` picks it up from here, reads the
                // producer's output, and inserts the instances.
                //
                // `scheduled_at` marks when it began waiting, matching the
                // approval park below — it is what makes "how long has this
                // been parked" answerable.
                let now = chrono::Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE task_runs
                     SET status = 'running', scheduled_at = ?,
                         claimed_by = NULL, lease_expires_at = NULL
                     WHERE id = ? AND status = 'pending'",
                )
                .bind(&now)
                .bind(&task_id)
                .execute(pool)
                .await?
                .rows_affected()
            } else if is_approval != 0 {
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
                // Stamp `scheduled_at` with the moment the task became
                // claimable — the Postgres twin's rationale applies verbatim:
                // it refines the claim order into readiness-FIFO (a run's
                // tasks all share one creation timestamp) and makes ready-age
                // honest for the dispatch-latency histogram, the stale-ready
                // alarm, and spillover reclassing. Retries and approval parks
                // already stamp their own `scheduled_at` on transition.
                let now = chrono::Utc::now().to_rfc3339();
                sqlx::query(
                    "UPDATE task_runs SET status = 'ready', scheduled_at = ?
                     WHERE id = ? AND status = 'pending'",
                )
                .bind(&now)
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
/// Pooled tasks currently occupying a slot.
///
/// The five park shapes are excluded because a parked row costs nothing while
/// it waits: a timer, an endpoint, a dataset cursor, a child run and a runtime
/// fan-out barrier all consume no capacity that a pool exists to ration.
///
/// The barrier is the one where omitting the exclusion is not merely a
/// mis-count. Its instances inherit its `pool`, so a parked barrier holding a
/// cap-1 pool's only slot blocks the very rows it is waiting for: the barrier
/// deadlocks against its own children and the run ends at `run_timeout_secs`.
/// Every future park shape belongs on this list, and this is why.
///
/// **`defer:` is deliberately NOT excluded**, and that asymmetry is the point.
/// The other five wait on something free; a deferred row waits on a job that is
/// burning someone's cluster the entire time. Counting it is the only thing
/// that lets `pool: spark` with a capacity of 4 mean "at most four concurrent
/// Spark jobs" — which is the bound an operator actually wants. Excluding it
/// would make the cap apply to *submissions*, which take seconds, and leave
/// concurrent remote cost unbounded by anything.
///
/// So for a deferred task `pool:` rations remote jobs, not worker slots. That
/// is a different meaning from the same field on a normal task, and it is
/// documented as such in `docs/EXTERNAL_JOBS.md`.
const POOL_RUNNING_COUNT: &str = "SELECT COUNT(*) FROM task_runs
     WHERE status = 'running' AND pool = ?
       AND wake_at IS NULL AND wait_url IS NULL
       AND wait_dataset IS NULL AND sub_run_id IS NULL
       AND fanout_of IS NULL";

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

/// The claim's read of the ready tasks ([`claim_ready_filtered`]), whose plan
/// a sweep's index must not take.
pub(crate) const READY_CANDIDATES: &str =
    "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at, pool, external_epoch
         FROM task_runs
         WHERE status = 'ready'
           AND (scheduled_at IS NULL OR scheduled_at <= ?1)
           AND (?2 = '' OR instr(?2, ',' || runner_class || ',') > 0)
           AND (?4 = '' OR pool IS NULL OR instr(?4, ',' || pool || ',') = 0)
           AND (?5 = 0 OR gang_id IS NULL)
         ORDER BY priority DESC, scheduled_at
         LIMIT ?3";

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

    // ONE wall-clock read for the whole claim: the `scheduled_at <= now` gate
    // and the lease expiry derive from the same instant, so a clock step
    // between two reads can never hand a task a lease already expired (or a
    // window too long) relative to the gate that admitted it. A duration must
    // never come from two wall-clock reads (clock discipline on units whose
    // clock is being set underneath them).
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_exp = (now_dt + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339();

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

    let candidates: Vec<TaskRun> = sqlx::query_as::<_, TaskRun>(READY_CANDIDATES)
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
                 log = NULL, log_fence = NULL, log_gen = NULL,
                 version = version + 1,
                 session_id = NULL
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
    // One wall-clock read for gate and lease alike — see `claim_ready_filtered`.
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_exp = (now_dt + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339();
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

    let claimed = sqlx::query_as::<_, TaskRun>(
        "UPDATE task_runs
         SET status = 'running',
             claimed_by = ?,
             lease_expires_at = ?,
             attempt = attempt + 1,
             log = NULL, log_fence = NULL, log_gen = NULL,
             version = version + 1,
             session_id = NULL
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
         SET lease_expires_at = CASE WHEN stop_requested_at IS NULL THEN ?3
                                     ELSE lease_expires_at END,
             stop_requested_at = COALESCE(stop_requested_at, ?1),
             output = COALESCE(output, 'cancelled: gang member failed')
         WHERE gang_id IS NOT NULL
           AND gang_id = (SELECT gang_id FROM task_runs WHERE id = ?2)
           AND id != ?2
           AND status = 'running' AND claimed_by IS NOT NULL",
    )
    .bind(&now)
    .bind(member_id)
    .bind(stop_lease())
    .execute(&mut *tx)
    .await?
    .rows_affected();
    let cancelled = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', claimed_by = NULL, lease_expires_at = NULL,
             version = version + 1, finished_at = ?1,
             output = COALESCE(output, 'cancelled: gang member failed')
         WHERE gang_id IS NOT NULL
           AND gang_id = (SELECT gang_id FROM task_runs WHERE id = ?2)
           AND id != ?2
           AND (status IN ('pending', 'ready') OR (status = 'running' AND claimed_by IS NULL))",
    )
    .bind(&now)
    .bind(member_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(asked + cancelled)
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
         WHERE id = ? AND claimed_by = ? AND version = ? AND stop_requested_at IS NULL",
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
        // Asked to stop, and over by itself first: cancelled, not succeeded.
        if settle_stopping(&mut tx, task_id, worker_id, fence, &now).await? {
            tx.commit().await?;
            tracing::info!(task_id, "the task ended after it was asked to stop: cancelled");
            return Ok(false);
        }
        tx.commit().await?;
        tracing::warn!(task_id, "stale completion ignored — task already reclaimed");
        return Ok(false);
    }

    // Decrement remaining_deps; dependents this statement does not promote
    // below are flipped by advance_ready_tasks.
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
        "UPDATE task_runs SET status = 'ready', scheduled_at = ?
         WHERE status = 'pending' AND remaining_deps = 0
           AND trigger_rule = 'all_success' AND is_approval = 0 AND fanout_of IS NULL
           AND (input IS NULL OR input NOT LIKE '%\"when\"%')
           AND id IN (
               SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
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
        "SELECT status, substr(output, -?) FROM task_runs WHERE run_id = ? AND name = ?",
    )
    .bind(tail as i64)
    .bind(run_id)
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(status, output)| (status, output.unwrap_or_default())))
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
         WHERE id = ? AND claimed_by = ? AND version = ? AND stop_requested_at IS NULL",
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
        // Asked to stop, and over by itself first: cancelled, not failed.
        if settle_stopping(&mut tx, task_id, worker_id, fence, &now).await? {
            tx.commit().await?;
            tracing::info!(task_id, "the task ended after it was asked to stop: cancelled");
            return Ok(false);
        }
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

/// Add a failure reason to a running task's streamed log, so a reason the
/// engine writes to `output` is not hidden by the log the views prefer. Only
/// when a log exists: a task with none already shows `output`, and starting a
/// log with just the reason would hide the rest of it.
pub async fn note_task_log(pool: &Pool, task_id: &str, fence: i64, note: &str) -> Result<()> {
    sqlx::query(
        "UPDATE task_runs SET log = log || ?
         WHERE id = ? AND version = ? AND status = 'running' AND log IS NOT NULL",
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
/// Returns `None`: the Postgres backend returns the run of the row it
/// changed, for the caller to tell subscribers of, and SQLite has none.
pub async fn append_task_output(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    chunk: &str,
    reset: bool,
) -> Result<Option<String>> {
    let sql = if reset {
        "UPDATE task_runs SET log = ?1, log_fence = ?3, log_gen = COALESCE(log_gen, 0) + 1
         WHERE id = ?2 AND version = ?3 AND status = 'running'"
    } else {
        "UPDATE task_runs SET log = COALESCE(log, '') || ?1, log_fence = ?3
         WHERE id = ?2 AND version = ?3 AND status = 'running'"
    };
    sqlx::query(sql)
        .bind(chunk)
        .bind(task_id)
        .bind(fence)
        .execute(pool)
        .await?;
    Ok(None)
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
/// outbox's trigger writes (066), so that no engine's clock matters.
const SWEEP_NOW: &str = "strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')";

/// [`SWEEP_NOW`], `secs` on: a parameter's or a column's number of seconds.
fn sweep_clock_in(secs: &str) -> String {
    format!("strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ({secs}) || ' seconds')")
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
        "UPDATE task_runs SET log = ?1, log_fence = ?3, log_gen = COALESCE(log_gen, 0) + 1,
             log_writings = COALESCE(log_writings, 0) + 1
         WHERE id = ?2 AND version = ?3 AND status = 'running'
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
    sqlx::query("DELETE FROM log_segments WHERE task_id = ?1 AND fence <= ?2")
        .bind(task_id)
        .bind(fence)
        .execute(&mut *tx)
        .await?;
    let swept = writings > 1;
    if swept {
        sqlx::query(&format!(
            "INSERT INTO log_sweeps (task_id, run_id, reason, marked_at, due_at)
             VALUES (?1, ?2, 'superseded', {SWEEP_NOW}, {SWEEP_NOW})
             ON CONFLICT (task_id) DO UPDATE SET reason = 'superseded', run_id = excluded.run_id,
                 marked_at = excluded.marked_at, due_at = excluded.due_at,
                 stamp = stamp + 1, tries = 0, last_error = NULL"
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
    sqlx::query("UPDATE task_runs SET log_writings = 1 WHERE id = ?1 AND log_writings IS NULL")
        .bind(task_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Take up to `limit` records of `log_sweeps` that are due, the earliest
/// first, for `lease_secs`: each is due again then, unless it is finished
/// before, and its stamp moves on, so that whoever held it before cannot
/// finish it. One writer at a time takes them here, so none is passed over.
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
    let rows: Vec<(String, Option<String>, String, i64, i64)> = sqlx::query_as(&format!(
        "UPDATE log_sweeps SET due_at = {}, stamp = stamp + 1
         WHERE task_id IN (SELECT task_id FROM log_sweeps WHERE {which}due_at <= {SWEEP_NOW}
                           ORDER BY due_at LIMIT ?1)
         RETURNING task_id, run_id, reason, stamp, tries",
        sweep_clock_in("?2")
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
    let mut out = Vec::new();
    for chunk in tasks.chunks(500) {
        let marks = (1..=chunk.len())
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT id, {LOG_FENCE}, {LOG_GEN} FROM task_runs WHERE id IN ({marks})");
        let mut q = sqlx::query_as::<_, (String, i64, i64)>(&sql);
        for task in chunk {
            q = q.bind(task);
        }
        out.extend(q.fetch_all(pool).await?);
    }
    Ok(out)
}

/// Finish claimed records of `log_sweeps`, each by `(task, stamp)` as its
/// claim left it: `retire`'s go (a removal found nothing left), and
/// `relook`'s are due again in `relook_secs` (a removal took something, and
/// a writer that has not stopped may make more). A record whose stamp moved
/// on since (the task was recorded again, or another sweeper claimed it) is
/// left as it is.
pub async fn finish_log_sweeps(
    pool: &Pool,
    retire: &[(String, i64)],
    relook: &[(String, i64)],
    relook_secs: i64,
) -> Result<()> {
    let pairs = |n: usize, from: usize| {
        (0..n)
            .map(|i| format!("(?{}, ?{})", from + 2 * i, from + 2 * i + 1))
            .collect::<Vec<_>>()
            .join(", ")
    };
    for chunk in retire.chunks(250) {
        let sql = format!(
            "DELETE FROM log_sweeps WHERE (task_id, stamp) IN (VALUES {})",
            pairs(chunk.len(), 1)
        );
        let mut q = sqlx::query(&sql);
        for (task, stamp) in chunk {
            q = q.bind(task).bind(stamp);
        }
        q.execute(pool).await?;
    }
    for chunk in relook.chunks(250) {
        let sql = format!(
            "UPDATE log_sweeps SET due_at = {}, stamp = stamp + 1, tries = 0, last_error = NULL
             WHERE (task_id, stamp) IN (VALUES {})",
            sweep_clock_in("?1"),
            pairs(chunk.len(), 2)
        );
        let mut q = sqlx::query(&sql).bind(relook_secs);
        for (task, stamp) in chunk {
            q = q.bind(task).bind(stamp);
        }
        q.execute(pool).await?;
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
        "UPDATE log_sweeps SET due_at = {}, stamp = stamp + 1, tries = tries + 1, last_error = ?4
         WHERE task_id = ?1 AND stamp = ?2",
        sweep_clock_in("?3")
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
        "SELECT COUNT(*), COALESCE(SUM(tries > 0), 0), MIN(marked_at) FROM log_sweeps",
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
            .map(|n| format!("?{n}"))
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
        "SELECT COALESCE((SELECT MAX(seq) + 1 FROM log_segments WHERE task_id = ?1 AND fence = ?2), 0), \
                CAST(COALESCE(octet_length(log), 0) AS BIGINT), \
                CAST(COALESCE(log_gen, 0) AS BIGINT) \
         FROM task_runs WHERE id = ?1",
    )
    .bind(task_id)
    .bind(fence)
    .fetch_optional(pool)
    .await?)
}

/// A task attempt's row `log`: the open part of a store that keeps it there.
pub async fn read_row_log(pool: &Pool, task_id: &str, fence: i64) -> Result<Option<String>> {
    let log: Option<Option<String>> =
        sqlx::query_scalar("SELECT log FROM task_runs WHERE id = ?1 AND version = ?2")
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
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%Y-%m-%dT%H:%M:%SZ', 'now')) ON CONFLICT DO NOTHING",
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
            "UPDATE task_runs SET log = SUBSTR(log, ?3 + 1) WHERE id = ?1 AND version = ?2",
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
        "SELECT id, {LOG_FENCE}, {LOG_GEN}, log, output, {SEGMENTS_COLUMN} FROM task_runs WHERE run_id = ?1"
    ))
    .bind(run_id)
    .fetch_all(pool)
    .await?)
}

/// One task row of a run as a reader of logs wants it ([`TaskLogRow`]).
pub async fn task_log_row(pool: &Pool, run_id: &str, task_id: &str) -> Result<Option<TaskLogRow>> {
    Ok(sqlx::query_as(&format!(
        "SELECT id, {LOG_FENCE}, {LOG_GEN}, log, output, {SEGMENTS_COLUMN} FROM task_runs \
         WHERE run_id = ?1 AND id = ?2"
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
    let mut out = Vec::new();
    for chunk in runs.chunks(500) {
        let marks = (1..=chunk.len())
            .map(|n| format!("?{n}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("{LOG_TAILS_SELECT} WHERE run_id IN ({marks})) t {LOG_TAILS_ORDER}");
        let mut q = sqlx::query_as::<_, LogTail>(&sql);
        for run in chunk {
            q = q.bind(run);
        }
        out.extend(q.fetch_all(pool).await?);
    }
    Ok(out)
}

/// One task as the live relay reads it ([`LogTail`]): what a stream reads
/// before it reads that task's log itself.
pub async fn log_tail_of(pool: &Pool, task_id: &str) -> Result<Option<LogTail>> {
    Ok(
        sqlx::query_as::<_, LogTail>(&format!("{LOG_TAILS_SELECT} WHERE id = ?1) t"))
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
    // One statement a task: SQLite has one connection, and a pass reads a
    // few tasks at most.
    let mut out = Vec::new();
    for r in reads {
        let row: Option<(i64, i64, Vec<u8>)> = sqlx::query_as(
            "SELECT c.closed, octet_length(t.log),
                    substr(CAST(t.log AS BLOB), max(?4 - c.closed - ?5, 0) + 1, ?6)
             FROM task_runs t,
                  (SELECT COALESCE(SUM(bytes), 0) AS closed FROM log_segments
                    WHERE task_id = ?1 AND fence = ?2) c
             WHERE t.id = ?1 AND t.log IS NOT NULL AND COALESCE(t.log_fence, t.version) = ?2
               AND COALESCE(t.log_gen, 0) = ?3",
        )
        .bind(&r.task_id)
        .bind(r.fence)
        .bind(r.gen)
        .bind(r.pos)
        .bind(r.base)
        .bind(r.max)
        .fetch_optional(pool)
        .await?;
        if let Some((closed, row_bytes, bytes)) = row {
            out.push(RowBytes {
                task_id: r.task_id.clone(),
                closed,
                row_bytes,
                bytes,
            });
        }
    }
    Ok(out)
}

/// At most `max` bytes of a task row's `output` from byte `from`: what stands
/// in for the log of a task that kept none. `None` where it has none.
pub async fn log_output_bytes(
    pool: &Pool,
    task_id: &str,
    from: i64,
    max: i64,
) -> Result<Option<Vec<u8>>> {
    let bytes: Option<Option<Vec<u8>>> = sqlx::query_scalar("SELECT substr(CAST(output AS BLOB), ?2 + 1, ?3) FROM task_runs WHERE id = ?1 AND output IS NOT NULL")
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
        "SELECT seq, bytes FROM log_segments WHERE task_id = ?1 AND fence = ?2 ORDER BY seq",
    )
    .bind(task_id)
    .bind(fence)
    .fetch_all(pool)
    .await?)
}

/// Tell the subscribers of `runs` that something of theirs changed. SQLite
/// has no `LISTEN/NOTIFY` and so no subscribers: nothing is done. The
/// reconcile loop does not need telling here: the file's triggers name the
/// runs a change may have ended (migrations/079, [`Waker::take_woken`]).
pub async fn notify_runs(_pool: &Pool, _runs: &[String]) -> Result<()> {
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

/// Make a batch of log appends, one after another: SQLite takes one writer
/// at a time whatever the pool holds, so a second at once would only wait
/// for the first, and it has no subscribers to tell. The Postgres backend
/// makes several at once and tells of their runs (see `db::postgres`).
///
/// Best-effort, as a log's tail is: an append that fails is logged and its
/// text is gone. Returns how many were made, those that reached no row among
/// them.
pub async fn append_task_outputs(pool: &Pool, appends: Vec<LogAppend>) -> usize {
    let mut written = 0;
    for append in appends {
        match append_task_output(
            pool,
            &append.task_id,
            append.fence,
            &append.text,
            append.reset,
        )
        .await
        {
            Ok(_) => written += 1,
            Err(e) => {
                tracing::warn!(task_id = %append.task_id, error = %e, "live-log append failed")
            }
        }
    }
    written
}

/// How many log appends may be in flight at once: one, for the reason
/// [`append_task_outputs`] gives.
pub fn log_append_lanes(_pool: &Pool) -> usize {
    1
}

// ── Runtime fan-out (`with_output_of:`) ──────────────────────────────────────

/// Statuses meaning an instance will not change again.
const FANOUT_TERMINAL: [&str; 4] = ["succeeded", "failed", "skipped", "cancelled"];

/// The fan-out sweep's read of the parked barriers ([`reconcile_fanouts`]):
/// those with no instance of their generation still to end, a batch at a
/// time. A barrier whose instances still run is the common case, and the
/// sweep has nothing to do for it.
///
/// Exact: the expansion stamps each instance with the version its CAS wrote,
/// and an instance of an older generation is one the sweep drops
/// (`fanout_epoch < version`), so a barrier left out here is one the sweep
/// would read and pass over. The statuses are [`FANOUT_TERMINAL`].
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
             LIMIT ?";

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
             WHERE dependent_id = ?
               AND dependency_id IN (
                   SELECT id FROM task_runs
                   WHERE fanout_parent = ? AND fanout_epoch < ?
               )",
        )
        .bind(&task_id)
        .bind(&task_id)
        .bind(version)
        .execute(pool)
        .await?;
        sqlx::query("DELETE FROM task_runs WHERE fanout_parent = ? AND fanout_epoch < ?")
            .bind(&task_id)
            .bind(version)
            .execute(pool)
            .await?;

        let instances_present: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = ?")
                .bind(&task_id)
                .fetch_one(pool)
                .await?;

        // ── phase 2: join ───────────────────────────────────────────────────
        if instances_present > 0 {
            let statuses: Vec<String> =
                sqlx::query_scalar("SELECT status FROM task_runs WHERE fanout_parent = ?")
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
             WHERE d.dependent_id = ?",
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
        let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = ?")
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
             WHERE id = ? AND version = ? AND status = 'running' AND claimed_by IS NULL
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
                 VALUES (?, ?, ?, 'ready', 0, ?, ?, 'all_success', ?, 0, ?, ?, ?, ?, ?)",
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
                "INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES (?, ?)",
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
        "UPDATE task_runs SET status = ?, finished_at = ?, output = ?
         WHERE id = ? AND status = 'running' AND fanout_of IS NOT NULL AND claimed_by IS NULL",
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
         WHERE id IN (SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?)
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
    decided_by: Option<&str>,
    comment: Option<&str>,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) =
        if approve { ("succeeded", "approved") } else { ("failed", "rejected") };
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = ?, finished_at = ?, output = ?,
                decided_by = ?, decision_comment = ?, log = ?
         WHERE id = ? AND run_id = ? AND status = 'awaiting_approval'",
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
/// `reject` — a gate fails safe). Returns the `(task_id, approved, deadline)`
/// decisions, the deadline being when the gate timed out (RFC 3339).
/// Idempotent: `resolve_approval`'s guard means an already-resolved gate is a
/// no-op, so a re-sweep does nothing. Not `ops`-gated — the reconcile loop
/// calls this every tick in every build.
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
            continue; // no start marker → can't measure the deadline
        };
        let deadline = started.with_timezone(&chrono::Utc)
            + chrono::TimeDelta::seconds(timeout);
        if now < deadline {
            continue;
        }
        let approve = on_timeout.as_deref() == Some("approve"); // default: reject
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
        let parent: Option<(String,)> = sqlx::query_as(SUB_RUN_PARENT)
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

/// One hop of [`sub_workflow_depth`]: the parked parent of a run.
pub(crate) const SUB_RUN_PARENT: &str = "SELECT run_id FROM task_runs WHERE sub_run_id = ? LIMIT 1";

pub async fn park_subworkflow(
    pool: &Pool,
    task_id: &str,
    fence: i64,
    child_run_id: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET sub_run_id = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ? AND stop_requested_at IS NULL",
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
         WHERE id = ? AND status = 'running' AND version = ? AND stop_requested_at IS NULL",
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
        "UPDATE task_runs SET status = ?, finished_at = ?,
                output = COALESCE(
                    (SELECT wr.output FROM workflow_runs wr WHERE wr.id = task_runs.sub_run_id),
                    ?)
         WHERE id = ? AND status = 'running' AND sub_run_id = ?",
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
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
/// The guard is on the **specific** `child_run_id` the sweep observed terminal,
/// not merely on "parked". A concurrent HA sweep working a stale snapshot could
/// otherwise re-arm this trigger after another scheduler already requeued it,
/// the worker re-dispatched it, and it re-parked on a *newer* child — clobbering
/// that child's parking and orphaning its run. Matching the observed child means
/// a stale re-arm finds no row and is a no-op.
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
             scheduled_at = ?,
             sub_run_id = NULL,
             claimed_by = NULL,
             lease_expires_at = NULL,
             finished_at = NULL,
             output = ?,
             version = version + 1
         WHERE id = ? AND status = 'running' AND sub_run_id = ?",
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
/// sweep observed, so a stale HA sweep cannot fail a trigger that has already
/// moved on to a newer iteration.
pub async fn fail_parked_subworkflow(
    pool: &Pool,
    task_id: &str,
    child_run_id: &str,
    reason: &str,
) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = 'failed', finished_at = ?, output = ?,
                sub_run_id = NULL
         WHERE id = ? AND status = 'running' AND sub_run_id = ?",
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// The sub-workflow sweep's read of the parked triggers whose child run has
/// ended ([`reconcile_subworkflows`]), a batch at a time. A trigger whose
/// child is gone is left parked, as a trigger whose child still runs is: the
/// join reads neither.
pub(crate) const PARKED_SUBWORKFLOWS: &str = "SELECT t.id, t.sub_run_id, c.status, c.output
         FROM task_runs t JOIN workflow_runs c ON c.id = t.sub_run_id
         WHERE t.status = 'running' AND t.sub_run_id IS NOT NULL
           AND c.status IN ('succeeded', 'failed', 'cancelled')
         LIMIT ?";

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
    let parked: Vec<(String, String, String, Option<String>)> = sqlx::query_as(PARKED_SUBWORKFLOWS)
        .bind(super::SWEEP_BATCH)
        .fetch_all(pool)
        .await?;
    let full = parked.len() as i64 >= super::SWEEP_BATCH;

    let mut resolved = Vec::new();
    for (task_id, child_run_id, child_status, child_output) in parked {
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
                        sqlx::query_scalar("SELECT attempt FROM task_runs WHERE id = ?")
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
                            requeue_parked_subworkflow(
                                pool,
                                &task_id,
                                &child_run_id,
                                child_output.as_deref(),
                                &retry_at,
                            )
                            .await?;
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
         SET wake_at = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ? AND stop_requested_at IS NULL",
    )
    .bind(wake_at)
    .bind(task_id)
    .bind(fence)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// The time sensors' read of the due waits ([`reconcile_waits`]): the
/// longest due first, a batch at a time.
pub(crate) const DUE_WAITS: &str = "SELECT id, wake_at FROM task_runs
         WHERE status = 'running' AND wake_at IS NOT NULL AND wake_at <= ?1
         ORDER BY wake_at
         LIMIT ?2";

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
        resolved.push((task_id, wake_at));
    }
    Ok(super::Pass {
        done: resolved,
        full,
    })
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
         WHERE id = ? AND status = 'running' AND version = ? AND stop_requested_at IS NULL",
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

/// The HTTP sensors' read of the ones due a poll ([`claim_due_url_waits`]).
/// No `next_poll_at IS NULL` branch: [`park_wait_url`] always sets it, and
/// migrations/077 set it on any sensor parked without one, so the branch
/// only made the read take every parked sensor.
pub(crate) const DUE_URL_WAITS: &str = "SELECT id, wait_url, next_poll_at FROM task_runs
         WHERE status = 'running' AND wait_url IS NOT NULL AND next_poll_at <= ?1
         ORDER BY next_poll_at
         LIMIT ?2";

/// **Claim** up to `limit` parked HTTP wait sensors (#27 follow-on) due for a
/// poll, the longest due first (the cap bounds how much probing one tick can
/// do), and return the ones this scheduler won: `(task_id, wait_url, due at)`.
///
/// Claimed as the external polls are ([`claim_due_external_polls`]): a
/// sensor read and not claimed was polled once by every scheduler that read
/// it. The claim moves `next_poll_at` on to `claim_secs` from now, guarded on
/// the value read, and is the crash bound too: a sensor whose scheduler dies
/// mid-GET is due again then. The GET's answer moves it again
/// ([`repark_url_wait`]) or resolves the sensor ([`resolve_url_wait`]). The
/// caller makes `claim_secs` longer than its GET's timeout, so a sensor is
/// never claimed while a GET of it is in flight.
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
            "UPDATE task_runs SET next_poll_at = ?
             WHERE id = ? AND status = 'running' AND wait_url IS NOT NULL
               AND next_poll_at = ?",
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

// ── deferred external jobs (`defer:`; the fifth park shape) ──────────────────

/// Park a task on a remote job it just submitted: drop the claim and the lease,
/// keep `status = 'running'`, and record the handle.
///
/// Fence-guarded like every other park, so a worker whose lease was already
/// reclaimed cannot park a row another worker now owns.
///
/// **The NULL lease is the entire crash-recovery argument.**
/// [`recover_expired_leases`] filters `lease_expires_at IS NOT NULL`, so from
/// this moment the row is provably outside the set it can reclaim: every
/// scheduler can die and the row is untouched, and any replica's next
/// [`due_external_polls`] resumes the poll. Nothing resubmits, because nothing
/// re-ran.
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
         SET external_kind = ?, external_handle = ?, external_endpoint = ?,
             next_poll_at = ?, external_deadline_at = ?,
             claimed_by = NULL, lease_expires_at = NULL
         WHERE id = ? AND status = 'running' AND version = ? AND stop_requested_at IS NULL",
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

/// The external polls' read of the parked jobs due one
/// ([`claim_due_external_polls`]).
pub(crate) const DUE_EXTERNAL_POLLS: &str =
    "SELECT id, name, run_id, external_kind, external_handle, external_endpoint,
                external_epoch, external_deadline_at, input, next_poll_at
         FROM task_runs
         WHERE status = 'running' AND external_handle IS NOT NULL
           AND (next_poll_at IS NULL OR next_poll_at <= ?1)
         ORDER BY next_poll_at
         LIMIT ?2";

/// **Claim** up to `limit` parked external jobs due for a poll, oldest deadline
/// first, and return only the rows this scheduler won.
///
/// Claiming, not merely listing — and this is the difference between a poller
/// and an outage. Every scheduler sweeps, so an unclaimed `SELECT … WHERE
/// next_poll_at <= now` hands the same due row to all of them, and N schedulers
/// make N calls to the vendor for one job. The `wait.url` sweep has that shape
/// and gets away with it only because nobody runs hundreds of HTTP sensors;
/// this is the feature whose selling point is hundreds of parked jobs.
///
/// The claim is a CAS on `next_poll_at`, the shape
/// [`claim_due_dataset_triggers`] uses on its cursor: push the value forward to
/// `now + claim_secs` guarded on the value we observed, and poll only the rows
/// whose UPDATE reported a row. A scheduler that loses the race sees
/// `rows_affected == 0` and moves on.
///
/// `IS` rather than `=` for the guard: a row parked with no interval carries
/// `next_poll_at IS NULL`, and `= NULL` is never true, so an `=` guard would
/// make exactly those rows unclaimable — forever, silently, by every scheduler.
///
/// The claim doubles as the crash bound. A scheduler that dies mid-request
/// leaves `next_poll_at` at `now + claim_secs`, so the row simply becomes due
/// again then. There is no separate claim to expire and nothing to reap.
pub async fn claim_due_external_polls(
    pool: &Pool,
    limit: i64,
    claim_secs: u64,
) -> Result<Vec<crate::models::ExternalPark>> {
    let now = chrono::Utc::now().to_rfc3339();
    let until = crate::dag::delayed_retry_at(claim_secs);
    // Candidates first, then CAS each one. Two statements rather than an
    // `UPDATE … RETURNING` because the guard has to compare against the value
    // this scheduler observed, which means reading it first.
    let rows = sqlx::query_as::<_, (
        String, String, String, String, String, Option<String>, i64, Option<String>, Option<String>, Option<String>,
    )>(DUE_EXTERNAL_POLLS)
    .bind(&now)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut won = Vec::with_capacity(rows.len());
    for (id, name, run_id, kind, handle, endpoint, epoch, deadline, input, observed) in rows {
        let claimed = sqlx::query(
            "UPDATE task_runs SET next_poll_at = ?
             WHERE id = ? AND status = 'running' AND external_handle = ?
               AND next_poll_at IS ?",
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

/// Push a parked external job's next poll out after a "still running" verdict
/// (or a transport error). Guarded on the **observed** handle, not merely on
/// "parked": a concurrent HA sweep working a stale snapshot could otherwise
/// push back the poll of a row that has since resolved, failed and been
/// re-armed onto a newer job — silently delaying the new one. Matching the
/// handle means a stale sweep finds no row and is a no-op.
pub async fn repark_external(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    next_poll_at: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs SET next_poll_at = ?
         WHERE id = ? AND status = 'running' AND external_handle = ?",
    )
    .bind(next_poll_at)
    .bind(task_id)
    .bind(handle)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Resolve a parked external job whose remote work succeeded: succeed the task
/// and advance its dependents, in one transaction.
///
/// Clearing `external_handle` in the same statement is the whole idempotency
/// argument — it is what takes the row out of the parked set, so a concurrent
/// re-sweep on another scheduler matches nothing and only one scheduler ever
/// resolves it. Same discipline as [`resolve_url_wait`] and
/// [`requeue_parked_subworkflow`].
///
/// `external_epoch` is deliberately **not** bumped: the job this row waited for
/// is the job that succeeded, and there is no next submission to name.
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
         SET status = 'succeeded', finished_at = ?, output = COALESCE(?, output),
             external_handle = NULL, next_poll_at = NULL, external_deadline_at = NULL
         WHERE id = ? AND status = 'running' AND external_handle = ?",
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Fail a parked external job — the remote work failed, or its
/// `defer.max_wait_secs` ceiling elapsed — advancing dependents exactly as a
/// normal failure does, and **bumping `external_epoch`**.
///
/// The epoch bump is what makes a retry a retry. The remote job is named
/// `dagron-<id>-<epoch>`, and `task_runs.id` is stable across lease recovery,
/// so without the bump a retried submit would reuse the failed job's name, the
/// remote system would answer AlreadyExists, and the step would adopt the
/// corpse of the attempt that just failed — a task that can never leave a
/// failure. With it, a retry gets a fresh name and a fresh job, while a
/// *recovery* (which does not come through here) keeps its epoch and adopts.
///
/// Guarded on the observed handle for the same reason [`repark_external`] is.
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
         SET status = 'failed', finished_at = ?, output = ?,
             external_handle = {handle_sql}, next_poll_at = NULL, external_deadline_at = NULL,
             external_epoch = external_epoch + 1
         WHERE id = ? AND status = 'running' AND external_handle = ?"
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
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = ?
         ) AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

// ── cancel teardown (reaching the remote job a cancelled run left running) ───

/// **Claim** up to `limit` rows that owe a remote teardown.
///
/// A row owes teardown when it still holds an `external_handle` but is no
/// longer in the parked shape. That single predicate is the whole design:
/// [`resolve_external`] and [`fail_external`] NULL the handle because the
/// remote job is already terminal and nothing is owed, so anything *else* that
/// flips a row terminal — a cancelled run, a cancelled task, a run that blew
/// its deadline — leaves the handle behind, and the handle is the debt.
///
/// Row-state driven on purpose, rather than stamped inside the cancel
/// transaction. The product's primary cancel path is inlined SQL in
/// `dagron-api`, a Postgres-only binary that by design cannot depend on the
/// engine crate and holds no `Seams` — so a teardown the cancel *performs*
/// would be a teardown the SDK and the MCP server never trigger. A cancel that
/// merely leaves evidence is one every caller performs for free.
///
/// The claim is the [`claim_outbox_batch`]-style lease the event outbox uses,
/// not a leader-gated sweep: leadership gates only the ops loops, so every
/// replica sweeps here, and vendor-side idempotency would bound correctness but
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
    // Built rather than bound as one array: SQLite has no array type, so the
    // placeholder count is the list length. The values are still BOUND, never
    // interpolated — they arrive from container labels, which is exactly the
    // kind of input that must not reach the SQL text.
    let placeholders = std::iter::repeat_n("?", ids.len()).collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT id FROM task_runs \
          WHERE id IN ({placeholders}) \
            AND status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')"
    );
    let mut q = sqlx::query_scalar::<_, String>(&sql);
    for id in ids {
        q = q.bind(id);
    }
    Ok(q.fetch_all(pool).await?.into_iter().collect())
}

/// not call volume.
pub async fn claim_due_external_cancels(
    pool: &Pool,
    limit: i64,
    lease_secs: u64,
) -> Result<Vec<crate::models::ExternalCancel>> {
    let now = chrono::Utc::now().to_rfc3339();
    let lease_until = crate::dag::delayed_retry_at(lease_secs);
    let mut tx = pool.begin().await?;
    let rows = sqlx::query_as::<_, (
        String, String, String, String, Option<String>, i64, i64, Option<String>, Option<String>,
    )>(DUE_EXTERNAL_CANCELS)
    .bind(&now)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    for r in &rows {
        sqlx::query("UPDATE task_runs SET next_poll_at = ? WHERE id = ?")
            .bind(&lease_until)
            .bind(&r.0)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(rows
        .into_iter()
        .map(|(id, run_id, kind, handle, endpoint, epoch, attempts, finished_at, input)| {
            crate::models::ExternalCancel {
                id,
                run_id,
                external_kind: kind,
                external_handle: handle,
                external_endpoint: endpoint,
                external_epoch: epoch,
                external_cancel_attempts: attempts,
                input,
                finished_at,
            }
        })
        .collect())
}

/// The read of the rows that owe a remote teardown
/// ([`claim_due_external_cancels`]), whose plan a sweep's index must not take.
pub(crate) const DUE_EXTERNAL_CANCELS: &str =
    "SELECT id, run_id, external_kind, external_handle, external_endpoint,
                external_epoch, external_cancel_attempts, finished_at, input
         FROM task_runs
         WHERE external_handle IS NOT NULL
           AND status <> 'running'
           AND (next_poll_at IS NULL OR next_poll_at <= ?1)
         ORDER BY next_poll_at
         LIMIT ?2";

/// Settle a row's teardown debt: drop the handle so it stops being swept.
///
/// Both outcomes come through here — the remote job was torn down, and the
/// engine gave up on tearing it down. The difference is not in the row, which
/// owes nothing either way; it is in whether the caller counted an orphan and
/// said so. Guarded on the observed handle so a stale sweep cannot clear a
/// debt that belongs to a newer submission.
pub async fn clear_external_handle(pool: &Pool, task_id: &str, handle: &str) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs
         SET external_handle = NULL, next_poll_at = NULL, external_deadline_at = NULL
         WHERE id = ? AND external_handle = ?",
    )
    .bind(task_id)
    .bind(handle)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows > 0)
}

/// Record a failed teardown attempt and schedule the retry, returning the new
/// attempt count so the caller can decide whether to give up.
///
/// Only a *failed* attempt increments. A sweep that found no poller owning the
/// kind has not tried anything — see [`release_external_cancel_claim`].
pub async fn record_external_cancel_failure(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    next_at: &str,
) -> Result<i64> {
    sqlx::query(
        "UPDATE task_runs
         SET external_cancel_attempts = external_cancel_attempts + 1, next_poll_at = ?
         WHERE id = ? AND external_handle = ?",
    )
    .bind(next_at)
    .bind(task_id)
    .bind(handle)
    .execute(pool)
    .await?;
    let n: Option<i64> = sqlx::query_scalar(
        "SELECT external_cancel_attempts FROM task_runs WHERE id = ? AND external_handle = ?",
    )
    .bind(task_id)
    .bind(handle)
    .fetch_optional(pool)
    .await?;
    Ok(n.unwrap_or_default())
}

/// Hand the row back without consuming an attempt: this engine has no poller
/// for the kind, so it tried nothing and has learned nothing.
///
/// That case is real rather than theoretical — a `RUNNER_CLASSES` pool runs the
/// same binary with different seams, so one replica can own a kind another
/// cannot. Consuming an attempt here would let the replicas that *cannot* tear
/// a job down exhaust the budget belonging to the one that can.
pub async fn release_external_cancel_claim(
    pool: &Pool,
    task_id: &str,
    handle: &str,
    next_at: &str,
) -> Result<bool> {
    let rows = sqlx::query(
        "UPDATE task_runs SET next_poll_at = ? WHERE id = ? AND external_handle = ?",
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

/// Record an **external** dataset event (not in this build: producers outside
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

/// Drop every subscription of the registered workflows in `names`, which
/// declare no `on_datasets:` (any more): one statement for all of them, by
/// the table's key, whichever scheduler synced their rows, and whether or
/// not it has restarted since. Returns the rows dropped.
pub async fn clear_dataset_triggers(pool: &Pool, names: &[String]) -> Result<u64> {
    if names.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(
        "DELETE FROM dataset_triggers
         WHERE workflow_name IN (SELECT value FROM json_each(?1))",
    )
    .bind(serde_json::to_string(names)?)
    .execute(pool)
    .await?
    .rows_affected())
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
         WHERE id = ? AND status = 'running' AND version = ? AND stop_requested_at IS NULL",
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

/// The dataset sensors' read of the ones whose dataset has moved
/// ([`reconcile_dataset_waits`]), a batch at a time.
pub(crate) const PARKED_DATASET_WAITS: &str = "SELECT id, wait_dataset FROM task_runs t
         WHERE status = 'running' AND wait_dataset IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM dataset_events e
               WHERE e.uri = t.wait_dataset AND e.id > COALESCE(t.wait_dataset_cursor, 0)
           )
         LIMIT ?";

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

/// The datasets one run read and wrote, for its OpenLineage event:
/// `(waited_on, produced)`, each distinct and sorted.
///
/// `waited_on` is every `wait: { dataset: … }` sensor in the run, as the URI it
/// expanded to. `produced` is every ledger row the run appended — a `produces:`
/// entry is recorded only when its task succeeded, so a failed run lists what it
/// did write. A workflow's `on_datasets:` are not here: they live in its spec,
/// not in any row of the run.
pub async fn run_dataset_io(pool: &Pool, run_id: &str) -> Result<(Vec<String>, Vec<String>)> {
    let waited_on: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT wait_dataset FROM task_runs
         WHERE run_id = ? AND wait_dataset IS NOT NULL ORDER BY wait_dataset",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    let produced: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT uri FROM dataset_events WHERE run_id = ? ORDER BY uri")
            .bind(run_id)
            .fetch_all(pool)
            .await?;
    Ok((waited_on, produced))
}

/// Reset a failed task to `ready` for a later retry attempt.
///
/// Sets `scheduled_at` to `retry_at` (a future RFC-3339 timestamp) so that
/// `claim_ready` will not pick it up until the backoff window has elapsed.
/// The `attempt` counter is NOT touched here. It is incremented only by the
/// claim, and given back only by [`requeue_quota_wait`], under the fence of the
/// claim it undoes.
/// Guards with `claimed_by = worker_id AND version = fence` to reject stale
/// retries from reclaimed workers.
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
    // would be a race with the next claim, which increments it. The number
    // is the task's count for its life (`attempt_base + attempt`), which a
    // rerun or a clear does not set back: it is what `task_attempts` is
    // keyed by.
    let claimed: Option<(i64,)> = sqlx::query_as(
        // The fault columns describe why this row is *currently* failed. A row
        // about to run again is not failed, so the previous attempt's verdict
        // is cleared here — otherwise a task that failed with `gpu-ecc`,
        // retried on its class budget of 5 and then succeeded keeps
        // `fault_class = 'gpu-ecc'` forever, and every roll-up counts a
        // success as an infrastructure failure. That is precisely the case the
        // wide infra budget exists to create, so the metric would be most
        // wrong exactly where it is most load-bearing. The `inc_fault` counter
        // already recorded the attempt when it happened; nothing is lost.
        "UPDATE task_runs
         SET status = 'ready',
             scheduled_at = ?,
             claimed_by = NULL,
             lease_expires_at = NULL,
             output = ?,
             fault_class = NULL,
             fault_detail = NULL,
             fault_confidence = NULL
         WHERE id = ? AND claimed_by = ? AND version = ? AND stop_requested_at IS NULL
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
    // instant the attempt's output stops existing anywhere else, and a crash
    // between the two would either lose it or leave a row for an attempt that
    // was never superseded.
    record_attempt(&mut tx, task_id, attempt, end, error.as_deref(), policy).await?;

    tx.commit().await?;
    Ok(true)
}

/// Keep a bounded tail of the attempt that `retry_task` is overwriting, and
/// evict whatever that pushes out of the window.
///
/// A no-op when retention is off (`DAGRON_ATTEMPT_LOG_BYTES=0`) — no insert, no
/// delete, nothing added to the transaction, so an operator who turns it off
/// gets back exactly the consumption of the code before this existed.
///
/// `INSERT OR REPLACE` rather than a plain insert: lease recovery increments
/// `attempt` on every claim, but a re-armed external job can legitimately
/// re-report the same number, and a retention row is not worth failing a
/// scheduler transition over.
async fn record_attempt(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
        "INSERT OR REPLACE INTO task_attempts
             (task_id, attempt, reason, output, truncated, finished_at)
         VALUES (?, ?, ?, ?, ?, ?)",
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
        sqlx::query("DELETE FROM task_attempts WHERE task_id = ? AND attempt <= ?")
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
/// `task_runs.output`, whole. A reader wanting the full history reads both, and
/// the API routes that do say so.
#[cfg(feature = "ops")]
pub async fn list_task_attempts(pool: &Pool, task_id: &str) -> Result<Vec<TaskAttempt>> {
    // Newest first with a hard cap, then reversed — the caller wants oldest
    // first, but the rows worth keeping when there are too many are the recent
    // ones. Capped because `DAGRON_ATTEMPT_LOG_KEEP=0` disables eviction, and
    // an unbounded retention policy must not imply an unbounded response.
    let mut rows = sqlx::query_as::<_, TaskAttempt>(
        "SELECT task_id, attempt, reason, status, scheduled_at, output, truncated, finished_at
         FROM task_attempts WHERE task_id = ? ORDER BY attempt DESC LIMIT ?",
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
        sqlx::query_scalar("SELECT attempt_base FROM task_runs WHERE id = ?")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(base.unwrap_or(0))
}

// ── Quota back-pressure ──────────────────────────────────────────────────────

/// Hand a claimed task back to the queue because its pod was refused by a full
/// Kubernetes namespace `ResourceQuota` (migration 051/066). That is
/// back-pressure, not a failed attempt: nothing ran.
///
/// The row returns to `ready` with a future `scheduled_at`, and `attempt` is
/// decremented. The claim incremented it for a dispatch that started nothing,
/// so every reader of `attempt` — the retry budget, `repeat:`, checkpoint
/// resume, the `task_attempts` key — sees the pre-claim value again. That is
/// sound only because the caller vouches that no workload was created for this
/// dispatch; a backend that cannot promise it must not call this.
///
/// Fenced like [`retry_task`], plus `status = 'running'`: the statement undoes
/// exactly one claim, so it lands at most once per claim. A second call with
/// the same fence, or one from a worker whose lease was reclaimed, returns
/// [`QuotaRequeue::Stale`] and gives nothing back.
///
/// `version` is left alone, as in `retry_task`, and the wait marker is built on
/// that. `quota_wait_version` is stamped with the row's version, so the row is
/// waiting on quota exactly while `status = 'ready' AND quota_wait_version =
/// version`. The next claim bumps `version` and the marker stops matching;
/// nothing has to clear it. A later refusal continues this episode only when
/// the marker is one behind its fence — the claim being undone took the row
/// straight out of the wait. Anything else in between (an attempt that started,
/// a dispatch lost to lease recovery, an operator reset) put another `version`
/// bump between the marker and the fence, and the refusal opens a new episode
/// with a new clock.
///
/// At the bound ([`QuotaRequeue::Expired`]) **nothing is written**: the row is
/// still `running` and claimed, so the caller fails it under the same fence.
/// `output`, `log`, the fault columns and `task_attempts` are untouched on
/// every path — a wait has no result to record.
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
         WHERE id = ? AND claimed_by = ? AND version = ? AND status = 'running'
           AND stop_requested_at IS NULL",
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
             scheduled_at = ?,
             claimed_by = NULL,
             lease_expires_at = NULL,
             attempt = MAX(attempt - 1, 0),
             quota_wait_since = ?,
             quota_waits = ?,
             quota_wait_version = version,
             quota_wait_detail = ?
         WHERE id = ? AND claimed_by = ? AND version = ? AND status = 'running'
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
/// refused. Feeds the engine's periodic summary line and, through
/// `status_counts`, the two `/metrics` gauges.
///
/// The predicate is the waiting rule itself, never `quota_wait_since IS NOT
/// NULL`: the columns outlive the wait (see [`requeue_quota_wait`]). Not
/// `ops`-gated — the summary line runs in a lean build too.
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
         SET fault_class = ?, fault_detail = ?, fault_confidence = ?
         WHERE id = ? AND claimed_by = ? AND version = ?",
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
        "SELECT fault_class, fault_detail, fault_confidence FROM task_runs WHERE id = ?",
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
        // `status = 'failed'` as well as a non-NULL class: belt and braces with
        // the clear in `retry_task`, so a row that somehow keeps a stale
        // verdict still cannot be counted as a failure it is not.
        "SELECT fault_class, COUNT(*) AS n, COALESCE(SUM(attempt), 0) AS attempts
         FROM task_runs
         WHERE fault_class IS NOT NULL AND status = 'failed'
           AND finished_at >= ? AND finished_at < ?
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
    Ok(sqlx::query(RECLASS_NEAR_DEADLINE)
        .bind(to_class)
        .bind(from_class)
        .bind(&horizon)
        .execute(pool)
        .await?
        .rows_affected())
}

/// [`reclass_ready_tasks_near_deadline`]' statement.
pub(crate) const RECLASS_NEAR_DEADLINE: &str = "UPDATE task_runs
         SET runner_class = ?
         WHERE status = 'ready'
           AND runner_class = ?
           AND gang_id IS NULL
           AND run_id IN (
               SELECT id FROM workflow_runs
               WHERE status = 'running'
                 AND COALESCE(deadline_at, alert_deadline_at) IS NOT NULL
                 AND COALESCE(deadline_at, alert_deadline_at) <= ?
           )";

// ── Running again: one implementation ───────────────────────────────────────
//
// A rerun, a retry and a clear each put tasks of a run back to run again, and
// so does a rerun of a run whole ([`rerun_all`]). Each, in one transaction,
// keeps what it replaces ([`keep_replaced`]), asks the concurrency key when it
// sets a run that had ended running again ([`run_cap`], [`refuse_if_full`])
// and writes one outbox event ([`insert_run_again`]). The Postgres backend
// says why, and what the two doors that ask for them used to do apart.

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
    /// `first`, and what they are bound to, in order. Numbered, like the
    /// statements it goes into: sqlx counts a bare `?` apart from the
    /// numbered ones, so the two cannot share a statement.
    fn condition(self, first: usize) -> (String, Vec<&'a str>) {
        match self {
            Reset::BrokenCone { run_id } => (
                format!("t.run_id = ?{first} AND t.status IN ('failed', 'cancelled', 'skipped')"),
                vec![run_id],
            ),
            Reset::Whole { run_id } => (
                format!(
                    "t.run_id = ?{first} AND t.fanout_parent IS NULL
                     AND t.status IN ('succeeded', 'failed', 'skipped', 'cancelled')"
                ),
                vec![run_id],
            ),
            Reset::Task { task_id } => (
                format!("t.id = ?{first} AND t.status IN ('failed', 'cancelled')"),
                vec![task_id],
            ),
            Reset::Cone { run_id, task_id } => (
                format!(
                    "t.id IN (
                         WITH RECURSIVE cone(id) AS (
                             SELECT id FROM task_runs WHERE id = ?{first} AND run_id = ?{}
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
/// (what is replacing it), numbered by the task's count for its life
/// (`attempt_base + attempt`). A task that never ran has no attempt to keep.
/// A decided approval gate is kept as one attempt ([`DECIDED_GATE`]).
/// Mirrors the Postgres backend, which says why the tail is cut in characters
/// here.
///
/// A no-op when retention is off (`DAGRON_ATTEMPT_LOG_BYTES=0`).
#[cfg(feature = "ops")]
async fn keep_replaced(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
         SELECT t.id, t.attempt_base + MAX(t.attempt, 1), ?1, t.status,
                substr(COALESCE(NULLIF(t.log, ''), t.output), -?2),
                COALESCE(length(COALESCE(NULLIF(t.log, ''), t.output)) > ?2, 0),
                t.scheduled_at, COALESCE(t.finished_at, ?3)
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
        .bind(i64::try_from(policy.bytes).unwrap_or(i64::MAX))
        .bind(now);
    for value in &bound {
        kept = kept.bind(*value);
    }
    kept.execute(&mut **tx).await?;

    if policy.keep > 0 {
        let (condition, bound) = reset.condition(2);
        let sql = format!(
            "DELETE FROM task_attempts
             WHERE EXISTS (
                 SELECT 1 FROM task_runs t
                 WHERE t.id = task_attempts.task_id AND (t.attempt > 0 OR {DECIDED_GATE})
                   AND task_attempts.attempt <= t.attempt_base + MAX(t.attempt, 1) - ?1
                   AND {condition})"
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

/// `run_id`'s cap, or `None` for a workflow with no cap (or no such run).
/// No lock, where Postgres takes one: a SQLite write transaction is the only
/// writer there is.
#[cfg(feature = "ops")]
async fn run_cap(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
) -> Result<Option<RunCap>> {
    let run: Option<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT d.name, d.spec, wr.concurrency_key
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.id = ?",
    )
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((name, spec, key)) = run else {
        return Ok(None);
    };
    Ok(crate::dag::DagGraph::stored_max_active_runs(&spec).map(|max| RunCap { name, key, max }))
}

/// Count `run_id`'s deadlines again from now ([`super::deadlines_again`]), in
/// the transaction that sets it running again after it ended, and let its SLA
/// alert fire again: the run is held to its `run_timeout_secs` and its
/// `deadline` from when it runs again, as a run let in after a wait is
/// ([`admit_waiting_runs`]). Each of the four that set an ended run running
/// again calls it once it has.
#[cfg(feature = "ops")]
async fn rearm_deadlines(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, run_id: &str) -> Result<()> {
    let spec: Option<String> = sqlx::query_scalar(
        "SELECT d.spec FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.id = ?",
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
         SET deadline_at = COALESCE(?1, deadline_at),
             alert_deadline_at = COALESCE(?2, alert_deadline_at),
             alert_fired_at = NULL
         WHERE id = ?3",
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
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
) -> Result<()> {
    let owing: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_runs
         WHERE run_id = ? AND external_handle IS NOT NULL AND status <> 'running'",
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
/// waits is in flight from the moment it is made.
async fn runs_in_flight(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    name: &str,
    key: Option<&str>,
    except: Option<&str>,
) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(RUNS_IN_FLIGHT)
        .bind(name)
        .bind(key)
        .bind(except)
        .fetch_one(&mut **tx)
        .await?)
}

/// [`runs_in_flight`]' statement.
pub(crate) const RUNS_IN_FLIGHT: &str = "SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
                                  THEN 1 ELSE 0 END), 0)
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.status = 'running' AND d.name = ?1
           AND wr.concurrency_key IS ?2
           AND wr.id IS NOT ?3";

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
async fn supersede_runs(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    name: &str,
    key: Option<&str>,
    by: &str,
    own: Option<&crate::models::RunTrigger>,
    now: &str,
) -> Result<u64> {
    let before: Vec<String> = sqlx::query_scalar(SUPERSEDED_RUNS)
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
        let ours = sqlx::query(
            "UPDATE workflow_runs
             SET stopping_as = 'cancelled', stop_requested_at = ?1, start_pending_at = NULL,
                 output = ?3
             WHERE id = ?2 AND status = 'running' AND stopping_as IS NULL",
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
        asked += 1;
    }
    Ok(asked)
}

/// [`supersede_runs`]' read of the runs a new run takes the place of.
pub(crate) const SUPERSEDED_RUNS: &str = "SELECT wr.id FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.status = 'running' AND wr.stopping_as IS NULL AND d.name = ?1
           AND wr.concurrency_key IS ?2
           AND (NOT ?3 OR EXISTS (
               SELECT 1 FROM run_triggers t
               WHERE t.run_id = wr.id
                 AND t.forge_host IS ?4
                 AND t.repo IS ?5
                 AND t.pr IS ?6
                 AND t.ref IS ?7
           ))
         ORDER BY wr.created_at, wr.id";

/// Write the event that says tasks of `run_id` were put back to run again
/// ([`RunAgain::event`](crate::models::RunAgain::event)), in the transaction
/// that puts them back.
#[cfg(feature = "ops")]
async fn insert_run_again(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
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
         VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
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
/// and an event is written (see "Running again", above). No NOTIFY (SQLite).
///
/// `false` if the task is not in a retryable (`failed`/`cancelled`) state.
/// [`MaxActiveRunsReached`](crate::models::MaxActiveRunsReached) if its run had
/// ended and its key is full: then nothing is written.
#[cfg(feature = "ops")]
pub async fn retry_task_from_ui(pool: &Pool, task_id: &str) -> Result<bool> {
    retry_task_from_ui_by(pool, task_id, None).await
}

/// [`retry_task_from_ui`], saying who asked for it (`reset_by`): the person
/// dagron-api was asked by, whom a gate put back to ask again does not take
/// an approval from where it is `not_triggerer`.
#[cfg(feature = "ops")]
pub async fn retry_task_from_ui_by(pool: &Pool, task_id: &str, who: Option<&str>) -> Result<bool> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let run_id: Option<String> = sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = ?")
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(run_id) = run_id else {
        tx.commit().await?;
        return Ok(false);
    };
    let cap = run_cap(&mut tx, &run_id).await?;
    keep_replaced(&mut tx, Reset::Task { task_id }, crate::models::RunAgain::Retry.reason(), &now).await?;

    let updated = sqlx::query(
        "UPDATE task_runs
         SET status = 'ready',
             claimed_by = NULL,
             lease_expires_at = NULL,
             scheduled_at = ?3,
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
             decided_by = NULL, decision_comment = NULL, cache_hit = 0,
             reset_by = ?2,
             version = version + 1
         WHERE id = ?1 AND status IN ('failed', 'cancelled')",
    )
    .bind(task_id)
    .bind(who)
    .bind(&now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if updated == 0 {
        // Not retryable: what was kept above goes with it.
        tx.rollback().await?;
        return Ok(false);
    }

    let rearmed = sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END
         WHERE id = ? AND status IN ('failed', 'cancelled')",
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
/// `params` override of the reset tasks' input). The caller commits. `who`
/// is the person dagron-api was asked by, recorded on each task put back
/// (`reset_by`). As the Postgres backend's.
#[cfg(feature = "ops")]
pub async fn rerun_from_failed_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    by: crate::models::RunAgain,
    who: Option<&str>,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let cap = run_cap(tx, run_id).await?;

    // Re-arm the run as the atomic gate: only the tx that flips the run
    // failed/cancelled → running proceeds to reset its tasks. A miss (run absent,
    // not rerunnable, or a lost race) → `None`, so no caller reports a false
    // success. SQLite serializes writers, so the loser sees 'running' here.
    // A run that was cancelled as it waited for its key was never let in, and
    // its tasks are put back below with the dependencies they have: it does
    // not wait now, and `admit_waiting_runs` must not take it for one that
    // does. The same holds for the two other statements that set a run
    // running again.
    let armed = sqlx::query(
        "UPDATE workflow_runs
         SET status = 'running', finished_at = NULL, output = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END
         WHERE id = ? AND status IN ('failed', 'cancelled')",
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

    // Reset the broken cone: failed, cancelled, and rule-skipped tasks (a task
    // skipped because a dependency failed should get another chance on rerun).
    // Done in two statements so the remaining_deps recompute below sees the
    // *post-reset* statuses — a skipped→pending row must be counted as
    // outstanding, which a single self-referential UPDATE cannot guarantee.
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
             -- the one that just failed. The remote name is
             -- `dagron-<id>-<epoch>` and `id` is stable across every reset, so
             -- without the bump the resubmit reuses the failed job's name, the
             -- remote answers AlreadyExists, and the step adopts a corpse — a
             -- task that can never leave its failure. Same reason
             -- `checkpoint_uri` is cleared on the Clear path.
             external_kind = NULL,
             external_handle = NULL,
             external_endpoint = NULL,
             external_deadline_at = NULL,
             next_poll_at = NULL,
             external_epoch = external_epoch + 1,
             -- What parked the row last time, and who decided it, as a whole
             -- rerun's reset has them go (see there).
             sub_run_id = NULL, wake_at = NULL, wait_url = NULL,
             wait_dataset = NULL, wait_dataset_cursor = NULL,
             decided_by = NULL, decision_comment = NULL, cache_hit = 0,
             reset_by = ?3,
             scheduled_at = ?1,
             version = version + 1
         WHERE run_id = ?2 AND status IN ('failed', 'cancelled', 'skipped')",
    )
    .bind(&now)
    .bind(run_id)
    .bind(who)
    .execute(&mut **tx)
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
    .execute(&mut **tx)
    .await?;

    insert_run_again(tx, run_id, by, reset, None, &now).await?;
    Ok(Some(reset))
}

/// Rerun a run whole (`from: all`): every task of a run that has ended,
/// succeeded, failed or cancelled, is put back, what succeeded included, and
/// the run runs again from its first tasks. `None`, `Some(n)` and a full
/// concurrency key as for [`rerun_from_failed`]. As the Postgres backend's,
/// which says the rest.
#[cfg(feature = "ops")]
pub async fn rerun_all(pool: &Pool, run_id: &str) -> Result<Option<u64>> {
    let mut tx = pool.begin().await?;
    let reset = rerun_all_in(&mut tx, run_id, None).await?;
    tx.commit().await?;
    Ok(reset)
}

/// [`rerun_all`] inside the caller's transaction, which the caller commits.
/// `who` is the person dagron-api was asked by (`reset_by`).
#[cfg(feature = "ops")]
pub async fn rerun_all_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    who: Option<&str>,
) -> Result<Option<u64>> {
    let now = chrono::Utc::now().to_rfc3339();
    let by = crate::models::RunAgain::RerunAll;
    let cap = run_cap(tx, run_id).await?;

    // The gate, as a rerun from the failed tasks has it, and open to a run
    // that succeeded too. It counts the rerun of every job.
    let armed = sqlx::query(
        "UPDATE workflow_runs
         SET status = 'running', finished_at = NULL, output = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END,
             whole_reruns = whole_reruns + 1
         WHERE id = ? AND status IN ('succeeded', 'failed', 'cancelled')",
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

    // A runtime fan-out's instances go, edges first, as the barrier's next
    // expansion would drop them: they are made again from its producer's
    // output, which runs again first.
    sqlx::query(
        "DELETE FROM task_dependencies
         WHERE dependency_id IN (
                   SELECT id FROM task_runs WHERE run_id = ?1 AND fanout_parent IS NOT NULL)
            OR dependent_id IN (
                   SELECT id FROM task_runs WHERE run_id = ?1 AND fanout_parent IS NOT NULL)",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM task_runs WHERE run_id = ? AND fanout_parent IS NOT NULL")
        .bind(run_id)
        .execute(&mut **tx)
        .await?;

    // Every other task, as a clear resets one.
    let reset = sqlx::query(
        "UPDATE task_runs
         SET status = 'pending',
             attempt_base = attempt_base
                 + CASE WHEN attempt = 0 AND is_approval = 1 AND status IN ('succeeded', 'failed')
                        THEN 1 ELSE attempt END,
             attempt = 0,
             claimed_by = NULL, lease_expires_at = NULL,
             output = NULL, log = NULL, log_fence = NULL, log_gen = NULL, finished_at = NULL, scheduled_at = ?1, version = version + 1,
             checkpoint_uri = NULL, checkpoint_marker = NULL,
             external_kind = NULL, external_handle = NULL, external_endpoint = NULL,
             external_deadline_at = NULL, next_poll_at = NULL,
             external_epoch = external_epoch + 1,
             fault_class = NULL, fault_detail = NULL, fault_confidence = NULL,
             sub_run_id = NULL, wake_at = NULL, wait_url = NULL,
             wait_dataset = NULL, wait_dataset_cursor = NULL,
             decided_by = NULL, decision_comment = NULL, cache_hit = 0,
             reset_by = ?3
         WHERE run_id = ?2 AND fanout_parent IS NULL
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
         WHERE run_id = ? AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?;

    insert_run_again(tx, run_id, by, reset, None, &now).await?;
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
    let cap = run_cap(&mut tx, run_id).await?;

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
             SELECT id FROM task_runs WHERE id = ?1 AND run_id = ?2
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
             output = NULL, log = NULL, log_fence = NULL, log_gen = NULL, finished_at = NULL, scheduled_at = ?3, version = version + 1,
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
             decided_by = NULL, decision_comment = NULL, cache_hit = 0,
             reset_by = ?4
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
         WHERE run_id = ? AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await?;

    // Re-arm a run that had finished so the reconcile loop resumes.
    let rearmed = sqlx::query(
        "UPDATE workflow_runs SET status = 'running', finished_at = NULL, output = NULL,
             queued_at = CASE WHEN admitted_at IS NULL THEN NULL ELSE queued_at END
         WHERE id = ? AND status IN ('succeeded', 'failed', 'cancelled')",
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
    let ids: Vec<(String,)> = sqlx::query_as(FAILED_RUNS_FOR_RERUN)
        .bind(max_attempts)
        .bind(cooldown_cutoff)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(ids.into_iter().map(|(id,)| id).collect())
}

/// [`list_failed_runs_for_rerun`]' statement.
#[cfg(feature = "enterprise")]
pub(crate) const FAILED_RUNS_FOR_RERUN: &str = "SELECT wr.id
         FROM workflow_runs wr
         LEFT JOIN run_reruns rr ON rr.run_id = wr.id
         WHERE wr.status = 'failed'
           AND COALESCE(rr.attempts, 0) < ?
           AND (rr.last_rerun_at IS NULL OR rr.last_rerun_at < ?)
         ORDER BY wr.finished_at DESC
         LIMIT ?";

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
    let n: i64 = sqlx::query_scalar(INCOMPLETE_RUNS)
        .bind(stall_cutoff)
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// [`count_incomplete_runs`]' statement.
#[cfg(feature = "enterprise")]
pub(crate) const INCOMPLETE_RUNS: &str =
    "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running' AND created_at < ?";

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
    let n: i64 = sqlx::query_scalar(ACTIVE_RUNS_COUNT)
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// [`count_active_runs`]' statement.
pub(crate) const ACTIVE_RUNS_COUNT: &str =
    "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running'";

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
/// The multi-run generalization of [`is_run_complete`]: every running run,
/// a slice of [`REAP_ALL_SLICE`] at a time ([`reap_woken`] with
/// [`super::Woken::All`]), so it reads the running runs alone, never the
/// history. Kept for its callers, which are all tests: dagron-core's and
/// dagron-engine's (`api.rs`, `cdevents.rs`). The loop reaps by
/// [`reap_woken`] and [`reap_slice`].
pub async fn reap_completed_runs(pool: &Pool) -> Result<Vec<(String, RunStatus)>> {
    reap_woken(pool, &super::Woken::All).await
}

/// The runs the reap may end, word for word idx_workflow_runs_reapable's
/// WHERE (migrations/079): SQLite lets a read use a partial index only if it
/// carries the index's terms as written.
pub(crate) const REAPABLE: &str =
    "status = 'running' AND stopping_as IS NULL AND (queued_at IS NULL OR admitted_at IS NOT NULL)";

/// [`reap_slice`]'s read: the next reapable runs after a cursor, in id order.
pub(crate) fn reap_slice_sql() -> String {
    format!("SELECT id FROM workflow_runs WHERE {REAPABLE} AND id > ?1 ORDER BY id LIMIT ?2")
}

/// The named reap's read of the tasks of the runs in a JSON array ?1. The
/// CROSS JOIN keeps the array the outer loop, so each run is found by its
/// key: written as `wr.id IN (SELECT value FROM json_each(?1))` it plans on
/// the enterprise build as its own index's `status=?` and reads every
/// running run.
pub(crate) const NAMED_REAP_TASKS: &str =
    "SELECT wr.id AS run_id, tr.status AS status, tr.allow_failure AS allow_failure, COUNT(*) AS cnt
       FROM json_each(?1) AS j
       CROSS JOIN workflow_runs AS wr ON wr.id = j.value
       JOIN task_runs AS tr ON tr.run_id = wr.id
      WHERE wr.status = 'running' AND wr.stopping_as IS NULL
        AND (wr.queued_at IS NULL OR wr.admitted_at IS NOT NULL)
      GROUP BY wr.id, tr.status, tr.allow_failure";

/// The named reap's read of those runs that have no task at all.
pub(crate) const NAMED_REAP_EMPTY: &str = "SELECT wr.id
       FROM json_each(?1) AS j
       CROSS JOIN workflow_runs AS wr ON wr.id = j.value
      WHERE wr.status = 'running' AND wr.stopping_as IS NULL
        AND (wr.queued_at IS NULL OR wr.admitted_at IS NOT NULL)
        AND NOT EXISTS (SELECT 1 FROM task_runs tr WHERE tr.run_id = wr.id)";

/// Finalize what is complete among `runs`, each found by its key. The
/// terminal `UPDATE` is guarded with `status = 'running'`, so when two
/// reaps race only one finalizes a given run. Returns the runs newly
/// transitioned to terminal.
async fn reap(pool: &Pool, runs: &[String]) -> Result<Vec<(String, RunStatus)>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        run_id: String,
        status: String,
        allow_failure: i64,
        cnt: i64,
    }

    let named = serde_json::to_string(runs)?;
    // Group by (status, allow_failure) so a `failed` task with allow_failure=1
    // counts as terminal but does not fail the run (fast-win #11).
    let rows: Vec<Row> = sqlx::query_as::<_, Row>(NAMED_REAP_TASKS)
        .bind(&named)
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
    // A run with no task at all (each one a `when:` of its parameters left
    // out) has nothing to wait for: it is complete, and succeeded. A run's
    // tasks are made in the transaction that makes it, so none is a run
    // whose tasks are yet to come. One that waits for its turn at a full cap
    // (`when_full: queue`) ends once it is let in, in the order it came.
    let empty: Vec<String> = sqlx::query_scalar(NAMED_REAP_EMPTY)
        .bind(&named)
        .fetch_all(pool)
        .await?;
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
             SET status = ?, finished_at = ?, completion_pending_at = NULL, start_pending_at = NULL
             WHERE id = ? AND status = 'running'",
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
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    status: &str,
    now: &str,
) -> Result<()> {
    let payload = serde_json::json!({ "run_id": run_id, "status": status }).to_string();
    sqlx::query(
        "INSERT INTO event_outbox
           (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
         VALUES (?, ?, 'run.completed', ?, 'pending', 0, ?, ?)",
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
/// event is owed to the engine's end-of-run sinks. The claim clears the stamp, so each run is returned to exactly one caller, which then
/// runs the work the reaper's own process runs for a run that finished.
///
/// A stamped run that is `running` again (rerun before any tick claimed it) is
/// left alone: its end was superseded, and its next end is owed instead.
pub async fn claim_pending_completions(
    pool: &Pool,
    limit: i64,
) -> Result<Vec<(String, RunStatus)>> {
    let rows: Vec<(String, String)> = sqlx::query_as(PENDING_COMPLETIONS)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|(id, status)| Ok((id, status.parse()?)))
        .collect()
}

/// [`claim_pending_completions`]' statement. It reads
/// idx_workflow_runs_completion_pending (migrations/078).
pub(crate) const PENDING_COMPLETIONS: &str = "UPDATE workflow_runs SET completion_pending_at = NULL
         WHERE id IN (
             SELECT id FROM workflow_runs
             WHERE completion_pending_at IS NOT NULL
               AND status IN ('succeeded', 'failed', 'cancelled')
             ORDER BY completion_pending_at
             LIMIT ?
         )
         RETURNING id, status";

/// Claim up to `limit` running runs whose start is still to be announced:
/// created with a `notify.git` block (`start_pending_at`), and no `pending`
/// commit status posted for them yet. The claim clears the stamp, so each run
/// is announced by exactly one caller. A run that has already ended is never
/// returned: its end cleared the stamp.
pub async fn claim_pending_starts(pool: &Pool, limit: i64) -> Result<Vec<String>> {
    let ids: Vec<String> = sqlx::query_scalar(PENDING_STARTS)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(ids)
}

/// [`claim_pending_starts`]' statement, whose plan a sweep's index must not
/// take.
pub(crate) const PENDING_STARTS: &str = "UPDATE workflow_runs SET start_pending_at = NULL
         WHERE id IN (
             SELECT id FROM workflow_runs
             WHERE start_pending_at IS NOT NULL AND status = 'running'
             ORDER BY start_pending_at
             LIMIT ?
         )
         RETURNING id";

/// A run's status, or `None` when there is no such run.
pub async fn run_status(pool: &Pool, run_id: &str) -> Result<Option<RunStatus>> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = ?")
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
         WHERE tr.id = ?",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
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

/// How many tasks `run_id` has: every row of it, as it is now. A run grows
/// where a task fans out over what another printed.
pub async fn run_task_count(pool: &Pool, run_id: &str) -> Result<i64> {
    let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = ?")
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
         FROM run_triggers WHERE run_id = ?",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(
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
        )| {
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
        sqlx::query_scalar("SELECT parameters FROM workflow_runs WHERE id = ?")
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
          WHERE e.name = ?",
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
             SELECT e.id, ?1, 'pending', 0, e.created_at
             FROM event_outbox e, outbox_epoch w
             WHERE e.status = 'pending' AND e.created_at < w.per_sink_since
               AND (?2 = '[]' OR e.event_type IN (SELECT value FROM json_each(?2)))
               AND NOT EXISTS (SELECT 1 FROM outbox_sinks s WHERE s.sink = ?1)
             ON CONFLICT (outbox_id, sink) DO NOTHING",
        )
        .bind(sink.name)
        .bind(super::outbox_types_json(sink))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO outbox_sinks (sink, since, cursor, registered_at) VALUES (?1, ?2, ?2, ?2)
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
            "UPDATE outbox_sinks SET wants_tasks_until = ?1
             WHERE sink = ?2 AND (wants_tasks_until IS NULL OR wants_tasks_until < ?1)",
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
    // Read first, and written only where there is something to write: an
    // INSERT takes the database's write lock even where it inserts nothing,
    // and a sink drains every few seconds.
    let found: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.id, e.created_at FROM event_outbox e
         WHERE e.status = 'pending' AND e.created_at >= ?2
           AND ((?3 = '[]' AND e.event_type NOT LIKE 'task.%')
                OR e.event_type IN (SELECT value FROM json_each(?3)))
           AND NOT EXISTS (SELECT 1 FROM outbox_deliveries d WHERE d.outbox_id = e.id AND d.sink = ?1)
         ORDER BY e.created_at, e.id
         LIMIT ?4",
    )
    .bind(sink.name)
    .bind(&from)
    .bind(super::outbox_types_json(sink))
    .bind(super::OUTBOX_ENROLL_ROWS)
    .fetch_all(pool)
    .await?;
    let enrolled: Vec<String> = if found.is_empty() {
        Vec::new()
    } else {
        sqlx::query_scalar(
            "INSERT OR IGNORE INTO outbox_deliveries (outbox_id, sink, status, attempts, next_attempt_at)
             SELECT e.id, ?1, 'pending', 0, e.created_at FROM event_outbox e
             WHERE e.id IN (SELECT value FROM json_each(?2))
             RETURNING next_attempt_at",
        )
        .bind(sink.name)
        .bind(
            serde_json::to_string(&found.iter().map(|(id, _)| id).collect::<Vec<_>>())
                .expect("a list of strings serialises"),
        )
        .fetch_all(pool)
        .await?
    };
    // The cursor goes by what was found, not by what was inserted: another
    // process enrolling the same sink may have inserted some of them first,
    // and a batch it filled is still a full one.
    let n = enrolled.len() as i64;
    let latest = found.iter().map(|(_, at)| at.clone()).max();
    let found = found.len() as i64;
    let next = super::outbox_next_cursor(latest, found, now);
    if let Some(next) = next.filter(|next| super::outbox_cursor_moves(&cursor, next, found)) {
        sqlx::query("UPDATE outbox_sinks SET cursor = ?1 WHERE sink = ?2 AND cursor < ?1")
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
            "SELECT since, cursor, wants_tasks_until FROM outbox_sinks WHERE sink = ?",
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
/// pending events, deferring each by `lease_secs` (bump `next_attempt_at`) so
/// a concurrent worker of the same sink won't take the same event
/// mid-delivery. At-least-once: a crashed
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
    use sqlx::Row;
    outbox_enroll(pool, sink).await?;
    let now = chrono::Utc::now();
    let now_s = now.to_rfc3339();
    let lease_until =
        (now + chrono::TimeDelta::seconds(lease_secs.clamp(0, MAX_SPOOL_SECS))).to_rfc3339();

    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT e.id, e.run_id, e.event_type, e.payload, d.attempts, e.created_at
         FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
         WHERE d.sink = ? AND d.status = 'pending' AND d.next_attempt_at <= ?
         ORDER BY d.next_attempt_at LIMIT ?",
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
    for row in &out {
        sqlx::query(
            "UPDATE outbox_deliveries SET next_attempt_at = ? WHERE sink = ? AND outbox_id = ?",
        )
        .bind(&lease_until)
        .bind(sink.name)
        .bind(&row.event.id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(out)
}

/// Delete up to `limit` task events (`task.*`) written before `before`
/// (RFC 3339), with every sink's delivery of them, oldest first. Returns how
/// many. See the Postgres backend's.
pub async fn prune_task_events(pool: &Pool, before: &str, limit: i64) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM event_outbox
         WHERE created_at < ?1 AND event_type LIKE 'task.%'
         ORDER BY created_at LIMIT ?2",
    )
    .bind(before)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    if ids.is_empty() {
        return Ok(0);
    }
    let ids = serde_json::to_string(&ids).expect("a list of strings serialises");
    sqlx::query(
        "DELETE FROM outbox_deliveries WHERE outbox_id IN (SELECT value FROM json_each(?1))",
    )
    .bind(&ids)
    .execute(&mut *tx)
    .await?;
    let n = sqlx::query("DELETE FROM event_outbox WHERE id IN (SELECT value FROM json_each(?1))")
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
        "UPDATE outbox_deliveries SET status = 'delivered', delivered_at = ?
         WHERE sink = ? AND outbox_id = ? AND status = 'pending'",
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
        "UPDATE outbox_deliveries SET attempts = attempts + 1, last_error = ?, next_attempt_at = ?
         WHERE sink = ? AND outbox_id = ? AND status = 'pending'",
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
        "UPDATE outbox_deliveries SET status = 'dead', attempts = attempts + 1, last_error = ?
         WHERE sink = ? AND outbox_id = ? AND status = 'pending'",
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
                COALESCE(SUM(d.status = 'pending'), 0),
                COALESCE(SUM(d.status = 'dead'), 0),
                MAX(CASE WHEN d.status = 'delivered' THEN d.delivered_at END)
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
/// Identical lease discipline — a per-row lease — so a worker that dies mid-upload simply lets the lease lapse and the rows are
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
/// Bytes, not characters (`LENGTH` of the payload as a BLOB): characters
/// under-count anything non-ASCII, and the budget is protecting a disk.
pub async fn outbox_pending_bytes(pool: &Pool, sink: &str) -> Result<i64> {
    // SUM over no rows is NULL, not 0.
    let total: Option<i64> = sqlx::query_scalar(
        "SELECT SUM(LENGTH(CAST(e.payload AS BLOB)))
         FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
         WHERE d.sink = ? AND d.status = 'pending'",
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
         WHERE d.sink = ? AND d.status = 'pending'",
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
            "UPDATE outbox_deliveries SET status = 'shed', last_error = ?
             WHERE sink = ? AND status = 'pending'
               AND outbox_id IN (SELECT id FROM event_outbox WHERE created_at < ?)
               AND next_attempt_at <= ?",
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
            "SELECT e.id, LENGTH(CAST(e.payload AS BLOB)) AS bytes
             FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
             WHERE d.sink = ? AND d.status = 'pending' AND d.next_attempt_at <= ?
             ORDER BY bytes DESC, e.created_at ASC LIMIT ?"
        } else {
            "SELECT e.id, LENGTH(CAST(e.payload AS BLOB)) AS bytes
             FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
             WHERE d.sink = ? AND d.status = 'pending' AND d.next_attempt_at <= ?
             ORDER BY e.created_at ASC LIMIT ?"
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
                    "UPDATE outbox_deliveries SET status = 'shed', last_error = ?
                     WHERE sink = ? AND outbox_id = ? AND status = 'pending'",
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
          WHERE d.spec = ? AND r.created_at >= ?
          ORDER BY r.created_at DESC, r.id DESC
          LIMIT 1",
    )
    .bind(spec)
    .bind(since)
    .fetch_optional(pool)
    .await?;
    Ok(id)
}

#[cfg(test)]
mod outbox_spool_tests {
    use super::*;
    use crate::dag::DagGraph;
    use crate::db::{OutboxSink, OUTBOX_LATE_COMMIT_SECS};

    /// The fleet uplink's sink, which took over the single-consumer outbox.
    const FLEET: OutboxSink<'static> = OutboxSink {
        name: "fleet",
        event_types: &[],
        adopts_legacy: true,
    };

    /// Own fixture rather than `mod tests`'s: this module is the outbox spool's
    /// contract and stays readable on its own.
    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_spool_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        // The uplink was configured long before any event these tests write,
        // some of which they write as hours or a week old.
        register_outbox_sink(&pool, &FLEET).await.unwrap();
        sqlx::query("UPDATE outbox_sinks SET since = '2000-01-01T00:00:00+00:00', cursor = '2000-01-01T00:00:00+00:00' WHERE sink = 'fleet'")
            .execute(&pool)
            .await
            .unwrap();
        (pool, path)
    }

    /// Move the watermark to `age_secs` ago: what was written before it is
    /// what the single-consumer outbox held when this release came up.
    async fn upgraded(pool: &Pool, age_secs: i64) {
        let at = (chrono::Utc::now() - chrono::TimeDelta::seconds(age_secs)).to_rfc3339();
        sqlx::query("UPDATE outbox_epoch SET per_sink_since = ?")
            .bind(&at)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Seed one pending outbox row with an exact payload and age, and hand it
    /// to the fleet's sink.
    async fn seed(pool: &Pool, id: &str, payload: &str, age_secs: i64) {
        write_event(pool, id, "run.completed", payload, age_secs).await;
        outbox_enroll(pool, &FLEET).await.unwrap();
    }

    /// Write one event to the outbox, `age_secs` old, as the engine does.
    async fn write_event(pool: &Pool, id: &str, event_type: &str, payload: &str, age_secs: i64) {
        let created = (chrono::Utc::now() - chrono::TimeDelta::seconds(age_secs)).to_rfc3339();
        sqlx::query(
            "INSERT INTO event_outbox
               (id, run_id, event_type, payload, status, attempts, next_attempt_at, created_at)
             VALUES (?, 'run-1', ?, ?, 'pending', 0, ?, ?)",
        )
        .bind(id)
        .bind(event_type)
        .bind(payload)
        .bind(&created)
        .bind(&created)
        .execute(pool)
        .await
        .unwrap();
    }

    /// The fleet sink's state of the event `id`.
    async fn status_of(pool: &Pool, id: &str) -> String {
        sqlx::query_scalar(
            "SELECT status FROM outbox_deliveries WHERE sink = 'fleet' AND outbox_id = ?",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// The ids of the events `sink` claims now.
    async fn claimed(pool: &Pool, sink: &OutboxSink<'_>) -> Vec<String> {
        claim_outbox_batch(pool, sink, 100, 30)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect()
    }

    /// 10.6: each sink is handed every event, and keeps its own state of it.
    /// The webhook delivering an event leaves it the uplink's to deliver; one
    /// sink dead-lettering it costs no other; and an event leased by one is
    /// free for the other.
    #[tokio::test]
    async fn every_sink_is_handed_every_event_and_keeps_its_own_state_of_it() {
        let (pool, path) = temp_pool().await;
        let webhook = OutboxSink {
            name: "webhook",
            event_types: &[],
            adopts_legacy: true,
        };
        register_outbox_sink(&pool, &webhook).await.unwrap();
        write_event(&pool, "e-1", "run.completed", "{}", 0).await;
        write_event(&pool, "e-2", "run.rerun", "{}", 0).await;

        assert_eq!(claimed(&pool, &webhook).await, ["e-1", "e-2"]);
        assert!(claimed(&pool, &webhook).await.is_empty(), "leased");
        assert_eq!(
            claimed(&pool, &FLEET).await,
            ["e-1", "e-2"],
            "the webhook's lease is not the fleet's"
        );

        mark_outbox_delivered(&pool, "webhook", "e-1")
            .await
            .unwrap();
        mark_outbox_dead(&pool, "webhook", "e-2", "HTTP 410")
            .await
            .unwrap();
        assert_eq!(status_of(&pool, "e-1").await, "pending");
        assert_eq!(status_of(&pool, "e-2").await, "pending");
        mark_outbox_delivered(&pool, "fleet", "e-2").await.unwrap();
        assert_eq!(status_of(&pool, "e-2").await, "delivered");
        assert_eq!(
            outbox_sink_counts(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|(s, p, d, _)| (s, p, d))
                .collect::<Vec<_>>(),
            [("fleet".to_string(), 1, 0), ("webhook".to_string(), 0, 1)]
        );
        // What the single-consumer outbox kept of the event is untouched.
        let legacy: Vec<String> = sqlx::query_scalar("SELECT status FROM event_outbox ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(legacy, ["pending", "pending"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A sink is handed the events written once it was configured, of its
    /// types; one that takes over the single-consumer outbox is handed what
    /// that still held pending when this release came up, and never what it
    /// delivered, nor what was written between the upgrade and the sink
    /// being configured.
    #[tokio::test]
    async fn a_sink_is_handed_what_was_written_once_it_was_configured_of_its_types() {
        let (pool, path) = temp_pool().await;
        write_event(&pool, "before", "run.completed", "{}", 60).await;
        write_event(&pool, "delivered", "run.completed", "{}", 50).await;
        sqlx::query("UPDATE event_outbox SET status = 'delivered' WHERE id = 'delivered'")
            .execute(&pool)
            .await
            .unwrap();
        upgraded(&pool, 40).await;
        write_event(&pool, "between", "run.completed", "{}", 30).await;

        let ends = OutboxSink {
            name: "engine.forge",
            event_types: &["run.completed"],
            adopts_legacy: false,
        };
        register_outbox_sink(&pool, &ends).await.unwrap();
        let webhook = OutboxSink {
            name: "webhook",
            event_types: &[],
            adopts_legacy: true,
        };
        register_outbox_sink(&pool, &webhook).await.unwrap();
        // Again, as another replica would: it adopts nothing twice.
        register_outbox_sink(&pool, &webhook).await.unwrap();
        write_event(&pool, "after", "run.completed", "{}", 0).await;
        write_event(&pool, "rerun", "run.rerun", "{}", 0).await;
        assert_eq!(
            claimed(&pool, &ends).await,
            ["after"],
            "only its type, only from when it was configured"
        );

        let mut got = claimed(&pool, &webhook).await;
        got.sort();
        assert_eq!(
            got,
            ["after", "before", "rerun"],
            "never what the old outbox delivered, nor what came between"
        );
        assert!(claimed(&pool, &webhook).await.is_empty(), "each once");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An event that commits after one written later than it is still
    /// handed to a sink whose cursor has passed it, inside
    /// `OUTBOX_LATE_COMMIT_SECS`; and the cursor moves on, so that the
    /// outbox is not read back to its beginning.
    #[tokio::test]
    async fn an_event_that_commits_late_is_still_handed_and_the_cursor_moves_on() {
        let (pool, path) = temp_pool().await;
        let ends = OutboxSink {
            name: "engine.notify",
            event_types: &["run.completed"],
            adopts_legacy: false,
        };
        register_outbox_sink(&pool, &ends).await.unwrap();
        sqlx::query("UPDATE outbox_sinks SET since = '2020-01-01T00:00:00+00:00'")
            .execute(&pool)
            .await
            .unwrap();
        write_event(&pool, "on-time", "run.completed", "{}", 0).await;
        assert_eq!(claimed(&pool, &ends).await, ["on-time"]);

        // Written a minute ago, committed only now.
        write_event(&pool, "late", "run.completed", "{}", 60).await;
        assert_eq!(claimed(&pool, &ends).await, ["late"]);
        // Past the window, it is not looked for.
        write_event(
            &pool,
            "too-late",
            "run.completed",
            "{}",
            3 * OUTBOX_LATE_COMMIT_SECS,
        )
        .await;
        assert!(claimed(&pool, &ends).await.is_empty());
        let behind = |cursor: String| {
            (chrono::Utc::now()
                - chrono::DateTime::parse_from_rfc3339(&cursor)
                    .unwrap()
                    .with_timezone(&chrono::Utc))
            .num_seconds()
        };
        let cursor_of = |sink: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, String>("SELECT cursor FROM outbox_sinks WHERE sink = ?")
                    .bind(sink)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        assert!(
            behind(cursor_of("engine.notify").await) < 5,
            "at the newest event it was handed"
        );

        // A sink none of whose events were written still moves on, to what
        // has surely committed, so that it does not read again all the others
        // wrote meanwhile.
        let none = OutboxSink {
            name: "engine.none",
            event_types: &["run.never"],
            adopts_legacy: false,
        };
        register_outbox_sink(&pool, &none).await.unwrap();
        sqlx::query("UPDATE outbox_sinks SET since = '2020-01-01T00:00:00+00:00', cursor = '2020-01-01T00:00:00+00:00' WHERE sink = 'engine.none'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(claimed(&pool, &none).await.is_empty());
        let behind_none = behind(cursor_of("engine.none").await);
        assert!(
            (OUTBOX_LATE_COMMIT_SECS - 5..=OUTBOX_LATE_COMMIT_SECS + 5).contains(&behind_none),
            "{behind_none}s behind"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The event types of the events written for `run`, in the order written.
    async fn types_of(pool: &Pool, run: &str) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT event_type || ':' || COALESCE(json_extract(payload, '$.name'), '')
             FROM event_outbox WHERE run_id = ? ORDER BY created_at, rowid",
        )
        .bind(run)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// Run a two-task run to its end, `b` after `a`, and return its id.
    async fn run_two(pool: &Pool) -> String {
        let yaml = "name: two\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(pool, &dag, yaml).await.unwrap();
        for _ in 0..2 {
            advance_ready_tasks(pool).await.unwrap();
            for t in claim_ready(pool, "w", 10).await.unwrap() {
                assert!(mark_task_succeeded(pool, &t.id, "w", t.version + 1, None)
                    .await
                    .unwrap());
            }
        }
        assert_eq!(reap_completed_runs(pool).await.unwrap().len(), 1);
        run
    }

    /// 10.22: while a sink that asks for them claims, each task's start and
    /// end is an event, in the order they happened, and the sink is handed
    /// them with the run's end; a sink of every type is not. With no such
    /// sink, or one that has stopped claiming, a task writes nothing.
    #[tokio::test]
    async fn a_task_s_start_and_end_are_events_only_while_a_sink_asks_for_them() {
        let (pool, path) = temp_pool().await;
        let quiet = run_two(&pool).await;
        assert_eq!(
            types_of(&pool, &quiet).await,
            ["run.completed:"],
            "no sink asks"
        );

        register_outbox_sink(&pool, &crate::db::CI_SINK)
            .await
            .unwrap();
        let told = run_two(&pool).await;
        assert_eq!(
            types_of(&pool, &told).await,
            [
                "task.started:a",
                "task.finished:a",
                "task.started:b",
                "task.finished:b",
                "run.completed:"
            ]
        );
        let handed: Vec<String> = claim_outbox_batch(&pool, &crate::db::CI_SINK, 100, 30)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.run_id == told)
            .map(|e| e.event_type)
            .collect();
        assert_eq!(handed.len(), 5, "{handed:?}");
        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM event_outbox WHERE run_id = ? AND event_type = 'task.finished' ORDER BY created_at LIMIT 1",
        )
        .bind(&told)
        .fetch_one(&pool)
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(
            (
                v["name"].as_str(),
                v["status"].as_str(),
                v["attempt"].as_i64()
            ),
            (Some("a"), Some("succeeded"), Some(1))
        );

        // The uplink takes every event but a task's.
        let fleet: Vec<String> = claim_outbox_batch_for_uplink(&pool, &FLEET, 100, 30)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.event.run_id == told)
            .map(|r| r.event.event_type)
            .collect();
        assert_eq!(fleet, ["run.completed"]);

        // A sink that has not claimed for a day is written none.
        sqlx::query("UPDATE outbox_sinks SET wants_tasks_until = '2020-01-01T00:00:00+00:00' WHERE sink = 'ci'")
            .execute(&pool)
            .await
            .unwrap();
        let lapsed = run_two(&pool).await;
        assert_eq!(types_of(&pool, &lapsed).await, ["run.completed:"]);
        // And its next claim asks for them again.
        claim_outbox_batch(&pool, &crate::db::CI_SINK, 100, 30)
            .await
            .unwrap();
        let again = run_two(&pool).await;
        assert_eq!(types_of(&pool, &again).await.len(), 5);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Task events a day old are deleted with every sink's delivery of them;
    /// a later one, and any other event, are kept.
    #[tokio::test]
    async fn task_events_are_pruned_a_day_on_with_their_deliveries() {
        let (pool, path) = temp_pool().await;
        register_outbox_sink(&pool, &crate::db::CI_SINK)
            .await
            .unwrap();
        let run = run_two(&pool).await;
        claim_outbox_batch(&pool, &crate::db::CI_SINK, 100, 30)
            .await
            .unwrap();
        // Two of the run's task events, as though written two days ago.
        sqlx::query(
            "UPDATE event_outbox SET created_at = '2020-01-01T00:00:00+00:00'
             WHERE run_id = ? AND event_type LIKE 'task.%' AND json_extract(payload, '$.name') = 'a'",
        )
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
        let day_ago = (chrono::Utc::now() - chrono::TimeDelta::days(1)).to_rfc3339();
        assert_eq!(
            prune_task_events(&pool, &day_ago, 1).await.unwrap(),
            1,
            "a batch at a time"
        );
        assert_eq!(prune_task_events(&pool, &day_ago, 100).await.unwrap(), 1);
        assert_eq!(prune_task_events(&pool, &day_ago, 100).await.unwrap(), 0);
        assert_eq!(
            types_of(&pool, &run).await,
            ["task.started:b", "task.finished:b", "run.completed:"]
        );
        let orphans: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outbox_deliveries d WHERE NOT EXISTS (SELECT 1 FROM event_outbox e WHERE e.id = d.outbox_id)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(orphans, 0, "their deliveries go with them");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A task that comes to wait on an approval is an event of its own.
    #[tokio::test]
    async fn a_task_waiting_on_an_approval_is_an_event() {
        let (pool, path) = temp_pool().await;
        register_outbox_sink(&pool, &crate::db::CI_SINK)
            .await
            .unwrap();
        let yaml = "name: gate\ntasks:\n  - name: gate\n    type: approval\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        assert_eq!(types_of(&pool, &run).await, ["task.waiting:gate"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The renewal of a task sink's lease on its events is written once an
    /// hour, not on every claim.
    #[test]
    fn a_task_sink_s_lease_on_its_events_is_renewed_within_its_last_hour_alone() {
        let now = chrono::Utc::now();
        let at = |secs: i64| (now + chrono::TimeDelta::seconds(secs)).to_rfc3339();
        assert!(crate::db::outbox_task_events_renew(None, now).is_some());
        assert!(crate::db::outbox_task_events_renew(Some(&at(-5)), now).is_some());
        assert!(crate::db::outbox_task_events_renew(Some(&at(23 * 3600 - 60)), now).is_some());
        assert!(crate::db::outbox_task_events_renew(Some(&at(23 * 3600 + 60)), now).is_none());
    }

    /// The uplink claim is the webhook claim plus `created_at`: same lease, same
    /// at-least-once behaviour, and the timestamp the plane needs to record when
    /// a week-old event actually happened.
    #[tokio::test]
    async fn uplink_claim_carries_created_at_and_leases_the_row() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: up\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        assert!(mark_task_succeeded(&pool, &claimed[0].id, "w", claimed[0].version + 1, None)
            .await
            .unwrap());
        assert_eq!(reap_completed_runs(&pool).await.unwrap().len(), 1);

        let batch = claim_outbox_batch_for_uplink(&pool, &FLEET, 10, 30).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event.run_id, run_id);
        assert_eq!(batch[0].event.event_type, "run.completed");
        assert!(
            chrono::DateTime::parse_from_rfc3339(&batch[0].created_at).is_ok(),
            "created_at rides the row as RFC 3339: {}",
            batch[0].created_at
        );
        assert!(
            claim_outbox_batch_for_uplink(&pool, &FLEET, 10, 30)
                .await
                .unwrap()
                .is_empty(),
            "a leased row is not re-claimed inside its lease window"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The two gauges the spool budget is read from, and what they ignore.
    #[tokio::test]
    async fn pending_bytes_and_age_measure_only_undelivered_rows() {
        let (pool, path) = temp_pool().await;
        assert_eq!(
            outbox_pending_bytes(&pool, "fleet").await.unwrap(),
            0,
            "an empty spool sums to 0, not NULL"
        );
        assert_eq!(
            outbox_oldest_pending_age_secs(&pool, "fleet")
                .await
                .unwrap(),
            0
        );

        seed(&pool, "e-old", &"x".repeat(100), 3_600).await;
        seed(&pool, "e-new", &"y".repeat(50), 10).await;
        assert_eq!(outbox_pending_bytes(&pool, "fleet").await.unwrap(), 150);
        let age = outbox_oldest_pending_age_secs(&pool, "fleet").await.unwrap();
        assert!((3_595..=3_605).contains(&age), "oldest pending row is about an hour old: {age}");

        // A delivered row has already left the spool; it costs nothing.
        mark_outbox_delivered(&pool, "fleet", "e-old").await.unwrap();
        assert_eq!(outbox_pending_bytes(&pool, "fleet").await.unwrap(), 50);
        let age = outbox_oldest_pending_age_secs(&pool, "fleet").await.unwrap();
        assert!(age < 60, "only the fresh row is left: {age}");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The byte budget with the default policy: oldest out first, and only as
    /// many rows as it takes to fit.
    #[tokio::test]
    async fn byte_budget_sheds_oldest_first_until_it_fits() {
        let (pool, path) = temp_pool().await;
        seed(&pool, "e-1", &"a".repeat(100), 300).await;
        seed(&pool, "e-2", &"b".repeat(100), 200).await;
        seed(&pool, "e-3", &"c".repeat(100), 100).await;

        let shed = shed_outbox(&pool, "fleet", 250, 0, "oldest").await.unwrap();
        assert_eq!(shed, 1, "dropping one 100-byte row brings 300 under 250");
        assert_eq!(status_of(&pool, "e-1").await, OUTBOX_STATUS_SHED);
        assert_eq!(status_of(&pool, "e-2").await, "pending");
        assert_eq!(status_of(&pool, "e-3").await, "pending");
        assert_eq!(outbox_pending_bytes(&pool, "fleet").await.unwrap(), 200);

        let reason: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM outbox_deliveries WHERE outbox_id = 'e-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let reason = reason.expect("a shed row records why it was dropped");
        assert!(reason.contains("250-byte"), "{reason}");
        assert!(reason.contains("oldest"), "{reason}");

        // A shed row is out of the drain's reach for good.
        let ids: Vec<String> = claim_outbox_batch_for_uplink(&pool, &FLEET, 10, 30)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.event.id)
            .collect();
        assert_eq!(ids, vec!["e-2".to_string(), "e-3".to_string()]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `largest` reclaims the same bytes from fewer rows — the policy for a
    /// spool that one oversized capture is wedging.
    #[tokio::test]
    async fn largest_policy_drops_the_big_payload_not_the_small_old_ones() {
        let (pool, path) = temp_pool().await;
        seed(&pool, "e-small-old", &"a".repeat(10), 900).await;
        seed(&pool, "e-huge", &"b".repeat(500), 300).await;
        seed(&pool, "e-small-new", &"c".repeat(10), 10).await;

        let shed = shed_outbox(&pool, "fleet", 100, 0, "LARGEST")
            .await
            .unwrap();
        assert_eq!(shed, 1, "one row carries the whole overshoot");
        assert_eq!(status_of(&pool, "e-huge").await, OUTBOX_STATUS_SHED);
        assert_eq!(status_of(&pool, "e-small-old").await, "pending");
        assert_eq!(status_of(&pool, "e-small-new").await, "pending");
        assert_eq!(outbox_pending_bytes(&pool, "fleet").await.unwrap(), 20);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The age budget, and the fact that `0` on either budget means "off" — a
    /// default configuration must never silently drop anything.
    #[tokio::test]
    async fn age_budget_parks_stale_rows_and_zero_budgets_shed_nothing() {
        let (pool, path) = temp_pool().await;
        seed(&pool, "e-week", &"a".repeat(10), 7 * 24 * 3_600 + 60).await;
        seed(&pool, "e-fresh", &"b".repeat(10), 30).await;

        assert_eq!(
            shed_outbox(&pool, "fleet", 0, 0, "oldest").await.unwrap(),
            0,
            "no budget, no loss"
        );
        assert_eq!(status_of(&pool, "e-week").await, "pending");

        let shed = shed_outbox(&pool, "fleet", 0, 7 * 24 * 3_600, "oldest")
            .await
            .unwrap();
        assert_eq!(shed, 1);
        assert_eq!(status_of(&pool, "e-week").await, OUTBOX_STATUS_SHED);
        assert_eq!(status_of(&pool, "e-fresh").await, "pending");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An absurd budget must clamp, not panic: `TimeDelta::seconds` is a
    /// panicking constructor and `DAGRON_FLEET_SPOOL_MAX_AGE_SECS` is an
    /// operator-supplied number.
    #[tokio::test]
    async fn an_absurd_age_budget_clamps_instead_of_panicking() {
        let (pool, path) = temp_pool().await;
        seed(&pool, "e-1", "payload", 60).await;
        assert_eq!(
            shed_outbox(&pool, "fleet", 0, i64::MAX, "oldest")
                .await
                .unwrap(),
            0
        );
        assert_eq!(status_of(&pool, "e-1").await, "pending");
        // The same clamp on the claim's lease.
        assert_eq!(
            claim_outbox_batch_for_uplink(&pool, &FLEET, 10, i64::MAX)
                .await
                .unwrap()
                .len(),
            1
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The ack heuristic: the newest run from these exact bytes, and nothing
    /// from before the work item was handed out.
    #[tokio::test]
    async fn latest_run_for_spec_is_newest_since_the_handout() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: fleet-item\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();

        let before = create_run(&pool, &dag, yaml).await.unwrap();
        let handed_out = chrono::Utc::now().to_rfc3339();
        // create_run stamps RFC 3339 with sub-second precision, but two runs in
        // the same microsecond would make "newest" ambiguous; step past it.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let after = create_run(&pool, &dag, yaml).await.unwrap();
        assert_ne!(before, after);

        let found = latest_run_for_spec(&pool, yaml, &handed_out).await.unwrap();
        assert_eq!(found.as_deref(), Some(after.as_str()), "the run created since the hand-out");

        let other = "name: other\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        assert_eq!(
            latest_run_for_spec(&pool, other, &handed_out).await.unwrap(),
            None,
            "a different spec is not this work item's run"
        );
        let future = (chrono::Utc::now() + chrono::TimeDelta::seconds(60)).to_rfc3339();
        assert_eq!(
            latest_run_for_spec(&pool, yaml, &future).await.unwrap(),
            None,
            "nothing was created after the hand-out, so the ack carries no run id"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
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
    let rows = match status {
        Some(s) => {
            // The status is bound as a parameter below (never interpolated), so
            // this is not injectable; validate against the enum anyway to reject
            // garbage early and satisfy SQL-injection scanners on the format!.
            let _: RunStatus = s
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid run status filter '{s}'"))?;
            sqlx::query_as::<_, RunSummary>(&list_runs_sql(true))
                .bind(s)
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
        None => {
            sqlx::query_as::<_, RunSummary>(&list_runs_sql(false))
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
    };
    Ok(rows)
}

/// [`list_runs`]' read, of one status where `by_status`.
#[cfg(feature = "ops")]
pub(crate) fn list_runs_sql(by_status: bool) -> String {
    format!(
        "SELECT wr.id AS id, wd.name AS name, wr.status AS status,
                       wr.created_at AS created_at, wr.finished_at AS finished_at,
                       wr.stopping_as AS stopping_as,
                       wr.queued_at AS queued_at, wr.admitted_at AS admitted_at
                FROM workflow_runs wr
                JOIN workflow_definitions wd ON wd.id = wr.definition_id{} ORDER BY wr.created_at DESC LIMIT ?",
        if by_status { " WHERE wr.status = ?" } else { "" }
    )
}

/// Fetch one run by id (or `None`). Backs `GET /runs/:id`.
#[cfg(feature = "ops")]
pub async fn get_run(pool: &Pool, run_id: &str) -> Result<Option<crate::models::WorkflowRun>> {
    use crate::models::WorkflowRun;
    let row = sqlx::query_as::<_, WorkflowRun>(
        "SELECT id, definition_id, status, input, output, created_at, finished_at,
                clock_confidence, clock_offset_ms, clock_source,
                stopping_as, stop_requested_at, queued_at, admitted_at
         FROM workflow_runs WHERE id = ?",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Stamp every **running** run `drifted` (clock discipline): the detector
/// caught a wall-clock step, or found the clock behind the datastore at boot,
/// so the timestamps these runs will finish under are not evidence. Runs
/// already terminal keep the confidence they were created with — their
/// record is closed, and rewriting it would be the detector inventing
/// history. Returns the number of runs touched. Not `ops`-gated: the lean
/// daemon runs the detector too.
pub async fn mark_runs_clock_drifted(pool: &Pool, offset_ms: i64, source: &str) -> Result<u64> {
    let r = sqlx::query(CLOCK_DRIFTED)
        .bind(offset_ms)
        .bind(source)
        .execute(pool)
        .await?;
    Ok(r.rows_affected())
}

/// [`mark_runs_clock_drifted`]' statement.
pub(crate) const CLOCK_DRIFTED: &str = "UPDATE workflow_runs
         SET clock_confidence = 'drifted', clock_offset_ms = ?, clock_source = ?
         WHERE status = 'running'";

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
        sqlx::query_as("SELECT COALESCE(NULLIF(log, ''), output) FROM task_runs WHERE run_id = ? AND id = ?")
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
        sqlx::query_as("SELECT id, COALESCE(NULLIF(log, ''), output) FROM task_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(id, log)| (id, log.unwrap_or_default())).collect())
}

/// All task rows of a run, ordered by name. Backs `GET /runs/:id`, and the
/// engine's fetch ahead of a run's images, in every build.
pub async fn list_tasks(pool: &Pool, run_id: &str) -> Result<Vec<TaskRun>> {
    let rows = sqlx::query_as::<_, TaskRun>(
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
         FROM task_runs WHERE run_id = ? ORDER BY name",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Cancel a still-running run. Returns what it did to the run's tasks, or
/// `None` when there was nothing to cancel (no such run, or one already over).
/// Backs `POST /runs/:id/cancel`.
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
/// changes nothing and answers with what is still stopping.
#[cfg(feature = "ops")]
pub async fn cancel_run(pool: &Pool, run_id: &str) -> Result<Option<crate::models::Cancelled>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let asked = sqlx::query(
        "UPDATE workflow_runs
         SET stopping_as = COALESCE(stopping_as, 'cancelled'),
             stop_requested_at = COALESCE(stop_requested_at, ?),
             start_pending_at = NULL
         WHERE id = ? AND status = 'running'",
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

    let overdue: Vec<(String, String)> = sqlx::query_as(OVERDUE_RUNS)
        .bind(&now)
        .bind(&now)
        .fetch_all(&mut *tx)
        .await?;

    for (run_id, _) in &overdue {
        stop_run_tasks(&mut tx, run_id, &now).await?;
        finish_stopped_run(&mut tx, run_id, &now).await?;
    }

    tx.commit().await?;
    Ok(overdue)
}

/// The deadline sweep's statement ([`cancel_overdue_runs`]).
pub(crate) const OVERDUE_RUNS: &str = "UPDATE workflow_runs
         SET stopping_as = 'failed', stop_requested_at = ?, start_pending_at = NULL,
             output = 'run deadline exceeded (run_timeout_secs)'
         WHERE status = 'running' AND stopping_as IS NULL
           AND deadline_at IS NOT NULL AND deadline_at < ?
           AND (queued_at IS NULL OR admitted_at IS NOT NULL)
         RETURNING id, deadline_at";

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
/// One write transaction, which on SQLite is the only writer there is. Reads
/// a partial index of the runs that wait, which is empty when none does.
pub async fn admit_waiting_runs(pool: &Pool) -> Result<Vec<String>> {
    let keys: Vec<(String, Option<String>)> =
        sqlx::query_as(WAITING_RUN_KEYS).fetch_all(pool).await?;
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut admitted = Vec::new();
    let mut tx = pool.begin().await?;
    for (name, key) in &keys {
        let (in_flight, waiting) = runs_in_flight(&mut tx, name, key.as_deref(), None).await?;
        let mut running = in_flight - waiting;
        let next: Vec<(String, String, String, Option<String>)> = sqlx::query_as(ADMISSION_NEXT)
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
            if running >= cap {
                break;
            }
            let let_in = sqlx::query(
                "UPDATE workflow_runs SET admitted_at = ?1, deadline_at = ?2
                 WHERE id = ?3 AND status = 'running' AND stopping_as IS NULL
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
                "UPDATE task_runs SET remaining_deps = remaining_deps - 1, scheduled_at = ?1
                 WHERE run_id = ?2 AND status = 'pending' AND remaining_deps > 0",
            )
            .bind(&now)
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
            running += 1;
            admitted.push(run_id.clone());
        }
    }
    tx.commit().await?;
    Ok(admitted)
}

/// How many waiting runs of one key a pass of [`admit_waiting_runs`] reads.
/// More than any cap lets in at once; what is left is the next pass's.
const ADMIT_AT_ONCE: i64 = 64;

/// [`admit_waiting_runs`]' read of the next runs of one key to let in.
pub(crate) const ADMISSION_NEXT: &str = "SELECT wr.id, d.spec, wr.queued_at, wr.deadline_at
             FROM workflow_runs wr
             JOIN workflow_definitions d ON d.id = wr.definition_id
             WHERE wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
               AND wr.status = 'running' AND wr.stopping_as IS NULL
               AND d.name = ?1 AND wr.concurrency_key IS ?2
             ORDER BY wr.queued_at, wr.id
             LIMIT ?3";

/// The admission sweep's read of the keys that have runs waiting
/// ([`admit_waiting_runs`]).
pub(crate) const WAITING_RUN_KEYS: &str = "SELECT DISTINCT d.name, wr.concurrency_key
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
           AND wr.status = 'running' AND wr.stopping_as IS NULL";

/// The read of the stopped runs [`end_stopped_runs`] ends.
pub(crate) const STRANDED_STOPPED_RUNS: &str = "SELECT id FROM workflow_runs
         WHERE stopping_as IS NOT NULL AND status = 'running'
           AND NOT EXISTS (
               SELECT 1 FROM task_runs t
               WHERE t.run_id = workflow_runs.id
                 AND t.status = 'running' AND t.stop_requested_at IS NOT NULL
           )";

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
    let stranded: Vec<String> = sqlx::query_scalar(STRANDED_STOPPED_RUNS)
        .fetch_all(pool)
        .await?;

    let mut ended = 0;
    for run_id in &stranded {
        let now = chrono::Utc::now().to_rfc3339();
        let mut tx = pool.begin().await?;
        stop_run_tasks(&mut tx, run_id, &now).await?;
        sqlx::query(
            "UPDATE task_runs SET stop_requested_at = NULL
             WHERE run_id = ? AND status <> 'running' AND stop_requested_at IS NOT NULL",
        )
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        if finish_stopped_run(&mut tx, run_id, &now).await?.is_some() {
            ended += 1;
        }
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
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    now: &str,
) -> Result<crate::models::Cancelled> {
    let lease = stop_lease();
    let cancelled = sqlx::query(
        "UPDATE task_runs
         SET status = 'cancelled', finished_at = ?, claimed_by = NULL, lease_expires_at = NULL
         WHERE run_id = ?
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
         SET lease_expires_at = CASE WHEN stop_requested_at IS NULL THEN ?3
                                     ELSE lease_expires_at END,
             stop_requested_at = COALESCE(stop_requested_at, ?1)
         WHERE run_id = ?2 AND status = 'running' AND claimed_by IS NOT NULL",
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
async fn finish_stopped_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    now: &str,
) -> Result<Option<String>> {
    let ended: Option<String> = sqlx::query_scalar(
        "UPDATE workflow_runs
         SET status = stopping_as, finished_at = ?1, completion_pending_at = NULL,
             start_pending_at = NULL, stopping_as = NULL, stop_requested_at = NULL
         WHERE id = ?2 AND status = 'running' AND stopping_as IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM task_runs
               WHERE run_id = ?2 AND status = 'running' AND stop_requested_at IS NOT NULL
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
/// written, for any other row. SQLite has one writer, so two of these cannot
/// each miss the other's task, as the Postgres twin must guard against.
async fn settle_stopping(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &str,
    worker_id: &str,
    fence: i64,
    now: &str,
) -> Result<bool> {
    let run_id: Option<String> = sqlx::query_scalar(
        "UPDATE task_runs
         SET status = 'cancelled', finished_at = ?, claimed_by = NULL, lease_expires_at = NULL,
             stop_requested_at = NULL
         WHERE id = ? AND claimed_by = ? AND version = ?
           AND status = 'running' AND stop_requested_at IS NOT NULL
         RETURNING run_id",
    )
    .bind(now)
    .bind(task_id)
    .bind(worker_id)
    .bind(fence)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(run_id) = run_id else {
        return Ok(false);
    };
    finish_stopped_run(tx, &run_id, now).await?;
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
        "UPDATE task_runs SET lease_expires_at = ?
         WHERE id = ? AND claimed_by = ? AND version = ?
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

/// The teardown claim's read of the stopping tasks whose lease has lapsed
/// ([`claim_teardowns`]).
pub(crate) const DUE_TEARDOWNS: &str = "SELECT id, run_id, name, status, attempt, remaining_deps,
                input, output, claimed_by, lease_expires_at, version,
                scheduled_at, finished_at, pool, stop_requested_at
         FROM task_runs
         WHERE status = 'running' AND stop_requested_at IS NOT NULL
           AND lease_expires_at IS NOT NULL AND lease_expires_at < ?1
           AND session_id IS NULL
           AND (?2 = '' OR instr(?2, ',' || runner_class || ',') > 0)
         ORDER BY lease_expires_at";

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
    let class_set = if classes.is_empty() {
        String::new()
    } else {
        format!(",{},", classes.join(","))
    };
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_exp = (now_dt + chrono::TimeDelta::seconds(super::lease_secs())).to_rfc3339();
    let mut tx = pool.begin().await?;

    // Every candidate is read and the engine's own are passed over here, not
    // in the statement: SQLite has no array to bind them as, and the stopping
    // tasks whose lease has lapsed are few. The limit is applied after.
    let candidates: Vec<TaskRun> = sqlx::query_as::<_, TaskRun>(DUE_TEARDOWNS)
        .bind(&now)
        .bind(&class_set)
        .fetch_all(&mut *tx)
        .await?;
    let candidates = candidates
        .into_iter()
        .filter(|task| !held.contains(&task.id))
        .take(usize::try_from(limit).unwrap_or(0));

    let mut claimed = Vec::new();
    for task in candidates {
        let rows = sqlx::query(
            "UPDATE task_runs
             SET claimed_by = ?, lease_expires_at = ?, version = version + 1
             WHERE id = ? AND version = ?
               AND status = 'running' AND stop_requested_at IS NOT NULL",
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
        }
    }
    tx.commit().await?;
    Ok(claimed)
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
    let mut rows = Vec::with_capacity(held.len());
    // Built rather than bound as one array, as in `live_task_ids`: SQLite has
    // no array type. In chunks, under its limit on bound values.
    for chunk in held.chunks(500) {
        let placeholders = std::iter::repeat_n("?", chunk.len()).collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT id, version, claimed_by, status, stop_requested_at \
               FROM task_runs WHERE id IN ({placeholders})"
        );
        let mut q = sqlx::query_as::<_, (String, i64, Option<String>, String, Option<String>)>(&sql);
        for (id, _) in chunk {
            q = q.bind(id);
        }
        rows.extend(q.fetch_all(pool).await?);
    }
    Ok(super::not_held(worker_id, held, rows))
}

/// The alert sweep's statement ([`fire_deadline_alerts`]).
pub(crate) const DUE_DEADLINE_ALERTS: &str = "UPDATE workflow_runs SET alert_fired_at = ?
         WHERE status = 'running' AND alert_deadline_at IS NOT NULL
           AND alert_deadline_at < ? AND alert_fired_at IS NULL
         RETURNING id, alert_deadline_at";

/// Emit a soft SLA deadline alert (spec `deadline`) for each still-running run
/// past its `alert_deadline_at` that hasn't alerted yet (#20). Fire-once and
/// idempotent: the guarded UPDATE stamps `alert_fired_at` and `RETURNING` makes
/// it winner-take-all across schedulers, so each run alerts exactly once. Unlike
/// the run-timeout sweep this does NOT cancel — it appends a
/// `run.deadline_exceeded` event to the transactional outbox (drained by the
/// outbox delivery worker) in the same transaction. Returns the alerted runs,
/// each `(run_id, alert_deadline_at)`.
pub async fn fire_deadline_alerts(pool: &Pool) -> Result<Vec<(String, String)>> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;

    let fired: Vec<(String, String)> = sqlx::query_as(DUE_DEADLINE_ALERTS)
        .bind(&now)
        .bind(&now)
        .fetch_all(&mut *tx)
        .await?;

    for (run_id, _) in &fired {
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
/// finished are read with the history, and those that have not for a scrape.
#[cfg(feature = "ops")]
async fn recent_runs(pool: &Pool, unfinished: bool) -> Result<Vec<crate::models::RecentRuns>> {
    let since = (chrono::Utc::now()
        - chrono::TimeDelta::seconds(crate::models::RECENT_RUNS_WINDOW_SECS))
    .to_rfc3339();
    let recent: Vec<(String, Option<String>, String, i64)> =
        sqlx::query_as(&recent_runs_sql(unfinished))
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

/// [`recent_runs`]' read. Of the unfinished, it carries
/// idx_workflow_runs_live's WHERE word for word (migrations/078).
#[cfg(feature = "ops")]
pub(crate) fn recent_runs_sql(unfinished: bool) -> String {
    format!(
        "SELECT wd.name, wr.environment, wr.status, COUNT(*)
         FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.status {} ({}) AND wr.created_at >= ?
         GROUP BY wd.name, wr.environment, wr.status",
        if unfinished { "IN" } else { "NOT IN" },
        super::sql_list(&crate::models::LIVE_RUN_STATUSES)
    )
}

/// Of [`status_counts_history`], the runs and tasks that have finished, by
/// status, alone: all dagron-api's `GET /api/metrics` serves of the history.
#[cfg(feature = "ops")]
pub async fn status_counts_finished(pool: &Pool) -> Result<crate::models::MetricsHistory> {
    let runs: Vec<(String, i64)> = sqlx::query_as(&finished_runs_by_status_sql())
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

/// [`status_counts_finished`]' read of the runs.
#[cfg(feature = "ops")]
pub(crate) fn finished_runs_by_status_sql() -> String {
    format!(
        "SELECT status, COUNT(*) FROM workflow_runs WHERE status NOT IN ({}) GROUP BY status",
        super::sql_list(&crate::models::LIVE_RUN_STATUSES)
    )
}

/// Of [`status_counts_live`], the runs and tasks that have not finished, by
/// status, and the dead letters, alone: all dagron-api's `GET /api/metrics`
/// serves of what has not finished. Three reads, where the whole is nine.
#[cfg(feature = "ops")]
pub async fn status_counts_unfinished(pool: &Pool) -> Result<crate::models::MetricsSnapshot> {
    let runs: Vec<(String, i64)> = sqlx::query_as(&live_runs_by_status_sql())
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

/// [`status_counts_unfinished`]' read of the runs. It carries
/// idx_workflow_runs_live's WHERE word for word (migrations/078).
#[cfg(feature = "ops")]
pub(crate) fn live_runs_by_status_sql() -> String {
    format!(
        "SELECT status, COUNT(*) FROM workflow_runs WHERE status IN ({}) GROUP BY status",
        super::sql_list(&crate::models::LIVE_RUN_STATUSES)
    )
}

/// The runs-waiting gauge's read ([`status_counts_live`]), of
/// idx_workflow_runs_waiting.
#[cfg(feature = "ops")]
pub(crate) const RUNS_WAITING: &str = "SELECT COUNT(*), MIN(queued_at) FROM workflow_runs
         WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running'";

/// The `/metrics` gauges of what has not finished: runs and
/// tasks in the statuses of work still to do, by status; the dead letters;
/// the recent runs still to end; and the rest of the snapshot, each bounded
/// by what is waiting, not by what has run. Read for a scrape.
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
    let (runs_waiting, runs_waiting_oldest_since): (i64, Option<String>) =
        sqlx::query_as(RUNS_WAITING).fetch_one(pool).await?;
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
         WHERE wr.id = ?",
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
    let ids: Vec<String> = sqlx::query_scalar(ARCHIVABLE_RUNS)
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

/// [`archivable_runs`]' read of the runs to archive.
#[cfg(feature = "ops")]
pub(crate) const ARCHIVABLE_RUNS: &str = "SELECT id FROM workflow_runs
         WHERE status IN ('succeeded','failed','cancelled')
           AND finished_at IS NOT NULL AND finished_at < ?
         ORDER BY finished_at LIMIT ?";

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
         WHERE wr.id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let tasks = sqlx::query("SELECT * FROM task_runs WHERE run_id = ? ORDER BY name")
        .bind(id)
        .fetch_all(pool)
        .await?;
    let outbox = sqlx::query("SELECT * FROM event_outbox WHERE run_id = ? ORDER BY created_at")
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
         WHERE task_id IN (SELECT id FROM task_runs WHERE run_id = ?)
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
    let [deps, tasks, runs] = gc_old_runs_sql();

    // Children first to respect the FK edges (dependencies → tasks → run).
    sqlx::query(&deps).bind(cutoff).execute(&mut *tx).await?;
    sqlx::query(&tasks).bind(cutoff).execute(&mut *tx).await?;
    let deleted = sqlx::query(&runs)
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

/// [`gc_old_runs`]' three deletes: the dependencies, the tasks and the runs.
#[cfg(feature = "ops")]
pub(crate) fn gc_old_runs_sql() -> [String; 3] {
    // A run whose tasks still owe a remote teardown is NOT collectable. Deleting
    // it drops the only record of a job still running on someone's cluster, and
    // turns the visible leak the orphan counter exists to advertise into a
    // silent one. The debt settles itself — the teardown sweep gives up after
    // its bound and NULLs the handle — so this only ever defers collection by
    // that window, which is far inside any sane GC cutoff.
    const OWES_TEARDOWN: &str = "EXISTS (
             SELECT 1 FROM task_runs owing
             WHERE owing.run_id = wr.id AND owing.external_handle IS NOT NULL
         )";
    [
        format!(
            "DELETE FROM task_dependencies
         WHERE dependent_id IN (
             SELECT tr.id FROM task_runs tr
             JOIN workflow_runs wr ON wr.id = tr.run_id
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < ?
               AND NOT {OWES_TEARDOWN}
         )"
        ),
        format!(
            "DELETE FROM task_runs
         WHERE run_id IN (
             SELECT wr.id FROM workflow_runs wr
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < ?
               AND NOT {OWES_TEARDOWN}
         )"
        ),
        format!(
            "DELETE FROM workflow_runs
         WHERE id IN (
             SELECT wr.id FROM workflow_runs wr
             WHERE wr.status IN ('succeeded','failed','cancelled')
               AND wr.finished_at IS NOT NULL AND wr.finished_at < ?
               AND NOT {OWES_TEARDOWN}
         )"
        ),
    ]
}

// ── Sweep slots ────────────────────────────────────────────────────────────────
//
// SQLite is one engine (dagron-api only opens the same file), so every sweep
// is its own and there is no table of slots to write a tick: Postgres's
// shares a sweep among the engines of one installation.

/// Every sweep asked for is this engine's: each slot at turn 0, with no
/// cursor, which tells the engine to keep its own.
pub async fn claim_sweep_slots(
    _pool: &Pool,
    _holder: &str,
    sweeps: &[(&str, f64)],
) -> Result<Vec<super::SweepSlot>> {
    Ok(sweeps
        .iter()
        .map(|(name, _)| super::SweepSlot {
            name: name.to_string(),
            turn: 0,
            cursor: None,
        })
        .collect())
}

/// Nothing to write: the engine keeps its own cursor.
pub async fn set_sweep_cursor(
    _pool: &Pool,
    _name: &str,
    _turn: i64,
    _cursor: Option<&str>,
) -> Result<bool> {
    Ok(false)
}

/// Nothing to hand back: the engine takes its next batch on its next tick.
pub async fn hand_back_sweep_slot(_pool: &Pool, _name: &str, _turn: i64) -> Result<bool> {
    Ok(false)
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

/// Reconcile-loop waker. SQLite has no `LISTEN/NOTIFY`: the file's triggers
/// (migrations/079) write each run a change may have ended to `run_wakes`,
/// and this takes them. The loop still sleeps a fixed interval between
/// ticks.
pub struct Waker {
    pool: Pool,
    /// Whether the first take has emptied `run_wakes` (see [`Self::take_woken`]).
    started: bool,
    /// While takes fail: when that was last logged, and the failures since.
    failing: Option<(std::time::Instant, u64)>,
}

/// The most runs one take names, as Postgres' `MAX_NAMED_RUNS`; the rest
/// are the next tick's.
const MAX_NAMED_RUNS: i64 = 4096;

/// While takes keep failing, how often that is logged again: a take is
/// tried every tick, twice a second by default.
const TAKE_FAILED_WARN_EVERY: Duration = Duration::from_secs(60);

impl Waker {
    pub async fn connect(pool: &Pool) -> Result<Self> {
        Ok(Self {
            pool: pool.clone(),
            started: false,
            failing: None,
        })
    }

    /// A take that failed says `All`. It is logged when takes begin to fail,
    /// and then at most once each [`TAKE_FAILED_WARN_EVERY`] while they go on
    /// failing, with how many failed since; [`Self::took`] says when they
    /// recover.
    fn failed(&mut self, what: &str, e: &sqlx::Error) -> super::Woken {
        let now = std::time::Instant::now();
        match &mut self.failing {
            None => {
                tracing::warn!(error = %e, "{what}: each tick reaps every running run until a take succeeds");
                self.failing = Some((now, 0));
            }
            Some((logged, since)) if now.duration_since(*logged) >= TAKE_FAILED_WARN_EVERY => {
                tracing::warn!(error = %e, failed = *since + 1, "{what}: still failing");
                *logged = now;
                *since = 0;
            }
            Some((_, since)) => *since += 1,
        }
        super::Woken::All
    }

    /// A take that succeeded, after some that failed.
    fn took(&mut self) {
        if self.failing.take().is_some() {
            tracing::info!("run_wakes taken again");
        }
    }

    /// Sleep for the full poll interval — there is no early-wake source. The
    /// count is of notifications taken, as the Postgres waker returns it, and
    /// here is always 0.
    pub async fn wait(&mut self, interval: Duration) -> Result<usize> {
        tokio::time::sleep(interval).await;
        Ok(0)
    }

    /// The runs the file says may have ended since the last take.
    ///
    /// The first take empties `run_wakes` and says `All`: what a crash between
    /// a take and the end it led to left, and what an engine from before 079
    /// never took, are then looked at with every other running run. A take
    /// that finds nothing writes nothing: an EXISTS is a read under WAL, where
    /// a DELETE would take the write lock each idle tick, beside dagron-api.
    /// The rows are deleted as they are taken. A wake written after the take
    /// is a new row, the next tick's. A reap that fails after a take ends the
    /// loop (its `?`), and the restart's `All` covers what was taken. An error
    /// here says `All`, as a Postgres waker that lost its connection does; the
    /// rows it could not take stay for the next. Errors that go on are logged
    /// once a minute, not each tick ([`Self::failed`]).
    pub async fn take_woken(&mut self) -> super::Woken {
        if !self.started {
            return match sqlx::query("DELETE FROM run_wakes")
                .execute(&self.pool)
                .await
            {
                Ok(_) => {
                    self.started = true;
                    self.took();
                    super::Woken::All
                }
                Err(e) => self.failed("could not empty run_wakes", &e),
            };
        }
        let any: std::result::Result<bool, sqlx::Error> =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM run_wakes)")
                .fetch_one(&self.pool)
                .await;
        match any {
            Ok(false) => {
                self.took();
                return super::Woken::Runs(Vec::new());
            }
            Ok(true) => {}
            Err(e) => return self.failed("could not read run_wakes", &e),
        }
        match sqlx::query_scalar::<_, String>(
            "DELETE FROM run_wakes
              WHERE run_id IN (SELECT run_id FROM run_wakes LIMIT ?1)
              RETURNING run_id",
        )
        .bind(MAX_NAMED_RUNS)
        .fetch_all(&self.pool)
        .await
        {
            Ok(ids) => {
                self.took();
                super::Woken::Runs(ids)
            }
            Err(e) => self.failed("could not take run_wakes", &e),
        }
    }
}

/// How many changes behind the newest a reader of `run_changes` may be and
/// still find every change it has not read: migrations/081 takes away, each
/// 1,024th change, the rows this many changes or more behind it.
#[cfg(feature = "ops")]
pub const RUN_CHANGES_KEPT: i64 = 65_536;

/// What [`run_changes_after`] found past a cursor.
#[cfg(feature = "ops")]
#[derive(Debug, PartialEq, Eq)]
pub enum RunChanges {
    /// The changes past it, oldest first, each its `seq` and its run: as
    /// many as were asked for at most, and more may follow.
    Since(Vec<(i64, String)>),
    /// The cursor is more than [`RUN_CHANGES_KEPT`] changes behind the
    /// newest, `newest`, so rows past it may be gone; or past it, as when
    /// the file was put back from an older copy, so changes it has not read
    /// may be numbered below it. Read every live run again
    /// ([`live_stream_runs`]).
    Behind { newest: i64 },
}

/// The newest `seq` written to `run_changes`, or 0 where none has been.
#[cfg(feature = "ops")]
const RUN_CHANGES_NEWEST: &str =
    "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'run_changes'), 0)";

/// [`run_changes_after`]'s read of the rows past a cursor: by the rowid.
#[cfg(feature = "ops")]
pub(crate) const RUN_CHANGES_SINCE: &str =
    "SELECT seq, run_id FROM run_changes WHERE seq > ?1 ORDER BY seq LIMIT ?2";

/// The runs a change has touched since `after`, a `seq` this reader has read
/// up to, at most `limit` of them. The newest `seq` and the rows are read in
/// one snapshot, so that what is past the cursor is not pruned between the
/// two. Nothing is written: any number of readers keep their own cursors.
#[cfg(feature = "ops")]
pub async fn run_changes_after(pool: &Pool, after: i64, limit: i64) -> Result<RunChanges> {
    let mut tx = pool.begin().await?;
    let newest: i64 = sqlx::query_scalar(RUN_CHANGES_NEWEST)
        .fetch_one(&mut *tx)
        .await?;
    let read = if after < newest - RUN_CHANGES_KEPT || after > newest {
        RunChanges::Behind { newest }
    } else {
        RunChanges::Since(
            sqlx::query_as(RUN_CHANGES_SINCE)
                .bind(after)
                .bind(limit)
                .fetch_all(&mut *tx)
                .await?,
        )
    };
    tx.commit().await?;
    Ok(read)
}

/// Every run a live stream shows: not finished, or finished at `since` or
/// after; and the newest `seq` of `run_changes` read in the same snapshot,
/// which a reader then reads past. What a reader reads at its start, and
/// when it has fallen behind ([`RunChanges::Behind`]): a read of every run
/// kept, as each read of dagron-api's stream was before 081.
#[cfg(feature = "ops")]
pub async fn live_stream_runs(pool: &Pool, since: &str) -> Result<(i64, Vec<String>)> {
    let mut tx = pool.begin().await?;
    let newest: i64 = sqlx::query_scalar(RUN_CHANGES_NEWEST)
        .fetch_one(&mut *tx)
        .await?;
    let runs: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM workflow_runs WHERE finished_at IS NULL OR finished_at >= ?1",
    )
    .bind(since)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((newest, runs))
}

/// The runs a slice of `All` reads at once: as the engine's REAP_SLICE.
const REAP_ALL_SLICE: i64 = 500;

/// Finalize the runs the waker named. `All` walks every slice of the
/// reapable runs from the start (at a start, after a take that failed, and
/// each tick with REAP_INTERVAL_MS=0): it reads the running runs alone,
/// never the history.
pub async fn reap_woken(pool: &Pool, woken: &super::Woken) -> Result<Vec<(String, RunStatus)>> {
    match woken {
        super::Woken::All => {
            let mut ended = Vec::new();
            let mut after: Option<String> = None;
            loop {
                let (swept, next) = reap_slice(pool, after.as_deref(), REAP_ALL_SLICE).await?;
                ended.extend(swept);
                match next {
                    Some(n) => after = Some(n),
                    None => break,
                }
            }
            Ok(ended)
        }
        super::Woken::Runs(runs) if runs.is_empty() => Ok(Vec::new()),
        super::Woken::Runs(runs) => reap(pool, runs).await,
    }
}

/// Finalize what is complete among the next `limit` reapable runs after
/// `after`, in the order of their ids, and say where the next slice starts:
/// `None` when this one reached the end, so the next begins again. As
/// Postgres', on idx_workflow_runs_reapable (migrations/079).
///
/// The net under [`reap_woken`]: a run whose wake was lost is finalized
/// when its slice comes round. A slice is bounded, so a pass costs the same
/// whatever the history holds.
pub async fn reap_slice(
    pool: &Pool,
    after: Option<&str>,
    limit: i64,
) -> Result<(Vec<(String, RunStatus)>, Option<String>)> {
    let runs: Vec<String> = sqlx::query_scalar(&reap_slice_sql())
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
    Ok((reap(pool, &runs).await?, next))
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
        // The end-of-run sink [`ends_owed`] reads, configured before any run.
        register_outbox_sink(&pool, &ENDS).await.unwrap();
        (pool, path)
    }

    /// An end-of-run sink of the tests' own ([`ends_owed`]).
    const ENDS: crate::db::OutboxSink<'static> = crate::db::OutboxSink {
        name: "test.ends",
        event_types: &["run.completed"],
        adopts_legacy: false,
    };

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
        retry_task(&pool, &task.id, "worker-A", fence, Some("preempted".into()), chrono::Utc::now().to_rfc3339(), AttemptEnd::Failed)
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

        // One member fails. Its siblings are held by a worker: they are asked
        // to stop, and stay running until that worker says their work is gone.
        let victim = &gang[0];
        mark_task_failed(&pool, &victim.id, "w", victim.version + 1, Some("rank died".into()))
            .await
            .unwrap();
        assert_eq!(cancel_gang_siblings(&pool, &victim.id).await.unwrap(), 2);
        let read = |id: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
                    "SELECT status, stop_requested_at, output FROM task_runs WHERE id = ?",
                )
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        let (sibling, other) = (&gang[1], &gang[2]);
        let (status, asked, _) = read(sibling.id.clone()).await;
        assert_eq!(status, "running", "its work has not stopped yet");
        assert!(asked.is_some(), "and it has been asked to");
        assert!(
            renew_task_lease(&pool, &sibling.id, "w", sibling.version + 1, 30).await.unwrap(),
            "its claim holds while its work is being stopped"
        );
        assert_eq!(
            tasks_to_stop(&pool, "w", &[(sibling.id.clone(), sibling.version + 1)]).await.unwrap(),
            [sibling.id.clone()],
            "its engine is told at its next sweep"
        );
        // Whatever it reports now, it is cancelled; and so is the other, on
        // its worker's word.
        assert!(!mark_task_succeeded(&pool, &sibling.id, "w", sibling.version + 1, None).await.unwrap());
        assert!(settle_stopped_task(&pool, &other.id, "w", other.version + 1).await.unwrap());
        for id in [&sibling.id, &other.id] {
            let (status, asked, output) = read(id.clone()).await;
            assert_eq!(status, "cancelled");
            assert_eq!(asked, None);
            assert_eq!(output.as_deref(), Some("cancelled: gang member failed"));
        }

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
        assert!(
            reconcile_subworkflows(&pool).await.unwrap().done.is_empty(),
            "child still running → no resolution"
        );

        // Child succeeds → the trigger resolves succeeded and `down` advances.
        sqlx::query("UPDATE workflow_runs SET status='succeeded' WHERE id = ?").bind(&child_run).execute(&pool).await.unwrap();
        let resolved = reconcile_subworkflows(&pool).await.unwrap().done;
        assert_eq!(resolved, vec![(trigger.id.clone(), true)]);
        advance_ready_tasks(&pool).await.unwrap();
        let down: (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
            .bind(&parent_run).fetch_one(&pool).await.unwrap();
        assert_eq!(down.0, "ready", "downstream advanced once the sub-workflow succeeded");

        // A child with no result of its own falls back to the constant, so a
        // sub-workflow that returns nothing still reads as having succeeded.
        let out: (Option<String>,) =
            sqlx::query_as("SELECT output FROM task_runs WHERE id = ?")
                .bind(&trigger.id).fetch_one(&pool).await.unwrap();
        assert_eq!(out.0.as_deref(), Some("sub-workflow succeeded"));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The durable agent loop, end to end: a `repeat:` on a sub-workflow trigger
    /// runs one child run per iteration and stops when the condition holds.
    ///
    /// The property that matters is the one that is easy to get wrong:
    /// **downstream work stays blocked while the loop runs.** Resolving a
    /// trigger decrements its dependents, so a loop that resolved-then-re-armed
    /// would release the rest of the DAG on the first turn and again on every
    /// turn after it. This asserts the dependent is still `pending` mid-loop.
    #[tokio::test]
    async fn a_repeating_sub_workflow_trigger_runs_one_child_per_iteration() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: turn\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ('wf-turn','turn',?,?,?)")
            .bind(child_yaml).bind("t").bind("t").execute(&pool).await.unwrap();

        let parent_yaml = "name: conversation\ntasks:\n  - { name: turn, type: workflow, workflow: turn, repeat: { until: \"{{ output }} == done\", max_iterations: 5 } }\n  - name: after\n    command: [\"true\"]\n    depends_on: [turn]\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        let parent_run = create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let mut child_runs = Vec::new();

        // ── turn 1: the child says "continue" ───────────────────────────────
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "turn").expect("trigger claimed");
        let trigger_id = trigger.id.clone();
        let child1 = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger_id, trigger.version + 1, &child1).await.unwrap());
        sqlx::query("UPDATE workflow_runs SET status='succeeded', output='continue' WHERE id = ?")
            .bind(&child1).execute(&pool).await.unwrap();
        child_runs.push(child1);

        // Not a resolution — the loop continues, so nothing is reported.
        assert!(
            reconcile_subworkflows(&pool).await.unwrap().done.is_empty(),
            "a continuing iteration is not a resolution"
        );

        // Re-armed: claimable again, and out of the parked set so the next sweep
        // cannot start a second child run for the same turn.
        let (status, sub, out): (String, Option<String>, Option<String>) =
            sqlx::query_as("SELECT status, sub_run_id, output FROM task_runs WHERE id = ?")
                .bind(&trigger_id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "ready", "the trigger is armed for the next turn");
        assert_eq!(sub, None, "cleared, or the sweep would see this turn twice");
        assert_eq!(out.as_deref(), Some("continue"), "the last turn's result is visible mid-loop");

        // The rest of the DAG has NOT been released.
        let (after_status, deps): (String, i64) = sqlx::query_as(
            "SELECT status, remaining_deps FROM task_runs WHERE name='after' AND run_id = ?",
        )
        .bind(&parent_run).fetch_one(&pool).await.unwrap();
        assert_eq!(after_status, "pending", "downstream must not start mid-loop");
        assert_eq!(deps, 1, "a continuing iteration must not decrement a dependent");

        // ── turn 2: the child says "done" ───────────────────────────────────
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "turn").expect("re-claimed for turn 2");
        let child2 = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert_ne!(child2, child_runs[0], "each turn is its own child run");
        assert!(park_subworkflow(&pool, &trigger.id, trigger.version + 1, &child2).await.unwrap());
        sqlx::query("UPDATE workflow_runs SET status='succeeded', output='done' WHERE id = ?")
            .bind(&child2).execute(&pool).await.unwrap();

        assert_eq!(
            reconcile_subworkflows(&pool).await.unwrap().done,
            vec![(trigger_id.clone(), true)],
            "the condition holds — now it is a resolution"
        );
        advance_ready_tasks(&pool).await.unwrap();
        let (after_status,): (String,) = sqlx::query_as(
            "SELECT status FROM task_runs WHERE name='after' AND run_id = ?",
        )
        .bind(&parent_run).fetch_one(&pool).await.unwrap();
        assert_eq!(after_status, "ready", "downstream advances once, when the loop is over");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A loop that never converges fails, and says what it last saw. Succeeding
    /// would hand `after` a result the conversation never reached.
    #[tokio::test]
    async fn a_loop_that_never_converges_fails_the_trigger() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: turn\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        let parent_yaml = "name: conversation\ntasks:\n  - { name: turn, type: workflow, workflow: turn, repeat: { until: \"{{ output }} == done\", max_iterations: 1 } }\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "turn").expect("trigger claimed");
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger.id, trigger.version + 1, &child).await.unwrap());
        sqlx::query("UPDATE workflow_runs SET status='succeeded', output='still thinking' WHERE id = ?")
            .bind(&child).execute(&pool).await.unwrap();

        assert_eq!(
            reconcile_subworkflows(&pool).await.unwrap().done,
            vec![(trigger.id.clone(), false)],
            "the only iteration allowed did not converge"
        );
        let (status, out): (String, Option<String>) =
            sqlx::query_as("SELECT status, output FROM task_runs WHERE id = ?")
                .bind(&trigger.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "failed");
        let out = out.unwrap_or_default();
        assert!(out.contains("not satisfied after 1 iterations"), "got: {out}");
        assert!(out.contains("still thinking"), "the last turn's output has to be in the reason");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A failed child fails the trigger even under `repeat:`. The loop operator
    /// decides when a conversation is over, not whether a failure counts —
    /// retrying a broken turn is `max_attempts`, and conflating the two would
    /// make a loop silently paper over every failure inside it.
    #[tokio::test]
    async fn a_failed_turn_fails_the_loop_rather_than_repeating_it() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: turn\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        let parent_yaml = "name: conversation\ntasks:\n  - { name: turn, type: workflow, workflow: turn, repeat: { until: \"{{ output }} == done\", max_iterations: 9 } }\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "turn").expect("trigger claimed");
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger.id, trigger.version + 1, &child).await.unwrap());
        sqlx::query("UPDATE workflow_runs SET status='failed', output='the tool exploded' WHERE id = ?")
            .bind(&child).execute(&pool).await.unwrap();

        assert_eq!(
            reconcile_subworkflows(&pool).await.unwrap().done,
            vec![(trigger.id.clone(), false)],
            "iterations remained, but a failed turn is not something to repeat"
        );
        let (status, out): (String, Option<String>) =
            sqlx::query_as("SELECT status, output FROM task_runs WHERE id = ?")
                .bind(&trigger.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "failed");
        assert_eq!(out.as_deref(), Some("the tool exploded"), "the turn's reason carries up");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The *resolution* needs the same guard, and for a sharper reason: it is
    /// the one that ends the loop. A stale HA sweep resolving a trigger that has
    /// already moved on would succeed the task while a turn was still running —
    /// releasing every downstream task and orphaning that turn's run.
    ///
    /// Before `repeat:` a trigger parked exactly once, so `sub_run_id IS NOT
    /// NULL` was enough: a duplicate sweep found the row already resolved and
    /// did nothing. Re-parking is what made the old guard too loose.
    #[tokio::test]
    async fn resolve_only_matches_the_observed_child() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: turn
tasks:
  - { name: c, command: [\"true\"] }
";
        let parent_yaml = "name: parent
tasks:
  - { name: trigger, type: workflow, workflow: turn }
  - name: down
    command: [\"true\"]
    depends_on: [trigger]
";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        let parent_run = create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "trigger").expect("trigger claimed");
        let trigger_id = trigger.id.clone();
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger_id, trigger.version + 1, &child).await.unwrap());

        // A resolution naming a child this trigger is not parked on is inert.
        assert!(
            !resolve_subworkflow(&pool, &trigger_id, "a-newer-child", true).await.unwrap(),
            "a stale resolve must not end a loop that has moved on"
        );
        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM task_runs WHERE id = ?")
                .bind(&trigger_id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "running", "still parked");
        // …and downstream is untouched, which is the damage a stale resolve does.
        let (down, deps): (String, i64) = sqlx::query_as(
            "SELECT status, remaining_deps FROM task_runs WHERE name='down' AND run_id = ?",
        )
        .bind(&parent_run).fetch_one(&pool).await.unwrap();
        assert_eq!(down, "pending");
        assert_eq!(deps, 1, "a stale resolve must not release a dependent");

        // The observed child resolves it.
        assert!(resolve_subworkflow(&pool, &trigger_id, &child, true).await.unwrap());
        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM task_runs WHERE id = ?")
                .bind(&trigger_id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "succeeded");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Re-arm and fail both match only the child the sweep observed — not any
    /// parked child. Under Postgres HA two schedulers sweep concurrently; a
    /// stale one could otherwise re-arm a trigger that another scheduler already
    /// requeued and a worker re-parked on a *newer* child, clobbering that
    /// child's parking and orphaning its run. The guard is `sub_run_id = <the
    /// observed child>`, so a stale re-arm finds no row and is a no-op.
    #[tokio::test]
    async fn requeue_and_fail_only_match_the_observed_child() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: turn\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        let parent_yaml = "name: conversation\ntasks:\n  - { name: turn, type: workflow, workflow: turn, repeat: { until: \"{{ output }} == done\", max_iterations: 5 } }\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "turn").expect("trigger claimed");
        let trigger_id = trigger.id.clone();
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger_id, trigger.version + 1, &child).await.unwrap());

        let now = chrono::Utc::now().to_rfc3339();

        // A re-arm naming a child this trigger is NOT parked on is a no-op, and
        // leaves the real parking untouched.
        assert!(
            !requeue_parked_subworkflow(&pool, &trigger_id, "a-newer-child", Some("stale"), &now)
                .await
                .unwrap(),
            "a re-arm for the wrong child must not touch the parking"
        );
        // A fail for the wrong child is equally inert.
        assert!(
            !fail_parked_subworkflow(&pool, &trigger_id, "a-newer-child", "stale")
                .await
                .unwrap(),
            "a fail for the wrong child must not touch the parking"
        );
        let (status, sub): (String, Option<String>) =
            sqlx::query_as("SELECT status, sub_run_id FROM task_runs WHERE id = ?")
                .bind(&trigger_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "running", "still parked after both stale calls");
        assert_eq!(sub.as_deref(), Some(child.as_str()), "still on its real child");

        // The observed child re-arms it.
        assert!(
            requeue_parked_subworkflow(&pool, &trigger_id, &child, Some("continue"), &now)
                .await
                .unwrap(),
            "the observed child re-arms the trigger"
        );
        let (status, sub): (String, Option<String>) =
            sqlx::query_as("SELECT status, sub_run_id FROM task_runs WHERE id = ?")
                .bind(&trigger_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "ready");
        assert_eq!(sub, None, "cleared out of the parked set");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A sub-workflow's `result_from` result reaches the parent.
    ///
    /// Without this a sub-workflow is a procedure, not a function: the run has a
    /// single return value and the parent threw it away for the constant
    /// "sub-workflow succeeded", so `{{ tasks.<trigger>.output }}` could not
    /// carry what the child decided — and `repeat: { until: … }` on the trigger
    /// had nothing to read but a string that never changes.
    #[tokio::test]
    async fn a_sub_workflows_result_reaches_its_parent_task() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: child\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        let parent_yaml = "name: parent\ntasks:\n  - { name: trigger, type: workflow, workflow: child }\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "trigger").expect("trigger claimed");
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child_run = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger.id, trigger.version + 1, &child_run).await.unwrap());

        // The child finishes with a result — what `result_from` writes onto the
        // run row when the run succeeds.
        sqlx::query("UPDATE workflow_runs SET status='succeeded', output='done' WHERE id = ?")
            .bind(&child_run).execute(&pool).await.unwrap();
        assert_eq!(
            reconcile_subworkflows(&pool).await.unwrap().done,
            vec![(trigger.id.clone(), true)]
        );

        let out: (Option<String>,) =
            sqlx::query_as("SELECT output FROM task_runs WHERE id = ?")
                .bind(&trigger.id).fetch_one(&pool).await.unwrap();
        assert_eq!(
            out.0.as_deref(),
            Some("done"),
            "the child's result is the parent task's output, not a constant"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The same propagation on the failure side. A child that died on its
    /// `run_timeout_secs` deadline records why on the run row; the parent's
    /// failure summary should say that rather than the word "failed".
    #[tokio::test]
    async fn a_failed_sub_workflow_carries_its_reason_up() {
        let (pool, path) = temp_pool().await;

        let child_yaml = "name: child\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        let parent_yaml = "name: parent\ntasks:\n  - { name: trigger, type: workflow, workflow: child }\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let trigger = claimed.iter().find(|t| t.name == "trigger").expect("trigger claimed");
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child_run = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &trigger.id, trigger.version + 1, &child_run).await.unwrap());

        sqlx::query(
            "UPDATE workflow_runs SET status='failed', output='run deadline exceeded (run_timeout_secs)' WHERE id = ?",
        )
        .bind(&child_run).execute(&pool).await.unwrap();
        assert_eq!(
            reconcile_subworkflows(&pool).await.unwrap().done,
            vec![(trigger.id.clone(), false)]
        );

        let (status, out): (String, Option<String>) =
            sqlx::query_as("SELECT status, output FROM task_runs WHERE id = ?")
                .bind(&trigger.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "failed");
        assert_eq!(out.as_deref(), Some("run deadline exceeded (run_timeout_secs)"));

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
        assert!(
            reconcile_waits(&pool).await.unwrap().done.is_empty(),
            "future deadline → not resolved"
        );

        // Deadline in the past → the sweep resolves it and `down` advances.
        sqlx::query("UPDATE task_runs SET wake_at = '2000-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&pause.id).execute(&pool).await.unwrap();
        assert_eq!(
            reconcile_waits(&pool)
                .await
                .unwrap()
                .done
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![pause.id.clone()]
        );
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
        assert!(
            reconcile_waits(&pool).await.unwrap().done.is_empty(),
            "url sensor is not a time sensor"
        );

        // It is claimed for a poll, with when it was due.
        let due = claim_due_url_waits(&pool, 32, 30).await.unwrap();
        assert_eq!(
            due,
            vec![(
                probe.id.clone(),
                "https://example.com/ready".to_string(),
                now.clone()
            )]
        );

        // A miss re-parks it far in the future → no longer due.
        let future = (chrono::Utc::now() + chrono::TimeDelta::hours(1)).to_rfc3339();
        assert!(repark_url_wait(&pool, &probe.id, &future).await.unwrap());
        assert!(
            claim_due_url_waits(&pool, 32, 30).await.unwrap().is_empty(),
            "re-parked → not due until next_poll_at"
        );

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
        assert!(
            reconcile_dataset_waits(&pool)
                .await
                .unwrap()
                .done
                .is_empty(),
            "park-time history never resolves the sensor"
        );

        // A fresh update (external-event flavor) resolves it; `down` advances.
        record_external_dataset_event(&pool, &uri).await.unwrap();
        assert_eq!(
            reconcile_dataset_waits(&pool).await.unwrap().done,
            vec![(sense.id.clone(), uri.clone())]
        );
        assert!(
            reconcile_dataset_waits(&pool)
                .await
                .unwrap()
                .done
                .is_empty(),
            "second sweep is a no-op"
        );
        advance_ready_tasks(&pool).await.unwrap();
        let (dstatus,): (String,) =
            sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
                .bind(&cons_run).fetch_one(&pool).await.unwrap();
        assert_eq!(dstatus, "ready", "downstream advanced once the dataset updated");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// What a run's OpenLineage event names: that run's sensors and the ledger
    /// rows it appended, each once, and nothing another run or an external event
    /// wrote. The read is by run, so it must use the run index, not scan.
    #[tokio::test]
    async fn run_dataset_io_reads_one_runs_sensors_and_outputs() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: mart\ntasks:\n  - { name: sense, type: wait, wait: { dataset: \"s3://raw/events\" } }\n  - { name: build, command: [\"true\"], produces: [\"ch://a/marts/daily\", \"ch://a/marts/agg\"] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        let other = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let mine = |name: &str| {
            claimed
                .iter()
                .find(|t| t.run_id == run && t.name == name)
                .unwrap_or_else(|| panic!("{name} claimed"))
                .clone()
        };
        let (sense, build) = (mine("sense"), mine("build"));
        assert!(
            park_wait_dataset(&pool, &sense.id, sense.version + 1, "s3://raw/events")
                .await
                .unwrap()
        );
        assert!(
            mark_task_succeeded(&pool, &build.id, "w", build.version + 1, None)
                .await
                .unwrap()
        );
        let both = [
            "ch://a/marts/daily".to_string(),
            "ch://a/marts/agg".to_string(),
        ];
        record_dataset_updates(&pool, "mart", &build.id, "build", &both)
            .await
            .unwrap();
        // A second update of the same dataset is one output, not two.
        record_dataset_updates(&pool, "mart", &build.id, "build", &both[..1])
            .await
            .unwrap();

        // Noise the read must not pick up: another run's output, and an
        // external event, which belongs to no run.
        let (other_build,): (String,) =
            sqlx::query_as("SELECT id FROM task_runs WHERE run_id = ? AND name = 'build'")
                .bind(&other)
                .fetch_one(&pool)
                .await
                .unwrap();
        record_dataset_updates(
            &pool,
            "mart",
            &other_build,
            "build",
            &["ch://a/elsewhere".to_string()],
        )
        .await
        .unwrap();
        record_external_dataset_event(&pool, "ch://a/marts/daily")
            .await
            .unwrap();

        let (waited_on, produced) = run_dataset_io(&pool, &run).await.unwrap();
        assert_eq!(waited_on, vec!["s3://raw/events".to_string()]);
        assert_eq!(
            produced,
            vec![
                "ch://a/marts/agg".to_string(),
                "ch://a/marts/daily".to_string()
            ]
        );

        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
            "EXPLAIN QUERY PLAN SELECT DISTINCT uri FROM dataset_events WHERE run_id = ? ORDER BY uri",
        )
        .bind(&run)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            plan.iter()
                .any(|(_, _, _, detail)| detail.contains("idx_dataset_events_run_id")),
            "the by-run read must use its index: {plan:?}"
        );

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

    /// Clearing a producer and re-running must fan out over the NEW output.
    ///
    /// A barrier's instances are its dependencies, so no reset path's
    /// downstream cone reaches them — clearing the producer resets the producer
    /// and the barrier and leaves the old instances attached, terminal. Without
    /// a generation stamp the next sweep sees instances, takes the join branch,
    /// and resolves the barrier against the previous attempt's results: the new
    /// partitions are never processed and the run reports success.
    #[tokio::test]
    async fn clearing_a_producer_re_fans_out_over_its_new_output() {
        let _g = crate::env_lock();
        let (pool, path, run_id, barrier) = parked_fanout(r#"["a","b"]"#).await;
        let producer: String = sqlx::query_scalar(
            "SELECT id FROM task_runs WHERE run_id = ? AND name = 'list'",
        )
        .bind(&run_id).fetch_one(&pool).await.unwrap();

        reconcile_fanouts(&pool).await.unwrap();
        let first = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(first.len(), 2);
        for t in &first {
            mark_task_succeeded(&pool, &t.id, "w1", t.version + 1, None).await.unwrap();
        }
        reconcile_fanouts(&pool).await.unwrap();

        // Clear the producer: it and the barrier reset, the instances do not.
        assert!(clear_task_with_downstream(&pool, &run_id, &producer).await.unwrap().is_some());

        // Re-run the producer with a DIFFERENT list.
        advance_ready_tasks(&pool).await.unwrap();
        let c = claim_ready(&pool, "w1", 10).await.unwrap();
        let p = c.iter().find(|t| t.name == "list").expect("the producer re-runs");
        mark_task_succeeded(&pool, &p.id, "w1", p.version + 1, Some(r#"["x","y","z"]"#.into()))
            .await
            .unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Expanded { instances: 3 },
            "it re-read the producer instead of joining the previous attempt's rows"
        );
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM task_runs WHERE fanout_parent = ? ORDER BY name",
        )
        .bind(&barrier).fetch_all(&pool).await.unwrap();
        assert_eq!(
            names,
            vec!["process.0", "process.1", "process.2"],
            "three new instances, and the two stale ones are gone"
        );
        // The stale rows took their dependency edges with them, or the barrier
        // would still be waiting on rows that no longer exist.
        let edges: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_dependencies WHERE dependent_id = ?",
        )
        .bind(&barrier).fetch_one(&pool).await.unwrap();
        assert_eq!(edges, 4, "one per new instance, plus the producer");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A pooled runtime fan-out barrier must not squat its pool's budget while
    /// it is parked — the fifth park shape has to be on the same exclusion list
    /// as the other four.
    ///
    /// Getting this wrong does not merely mis-count: the instances inherit the
    /// barrier's `pool`, so in a cap-1 pool the parked barrier holds the only
    /// slot, its own instances can never be claimed, and the barrier waits for
    /// them forever. That is a deadlock that ends at `run_timeout_secs`, not a
    /// slow run.
    #[tokio::test]
    async fn a_parked_fan_out_barrier_does_not_squat_its_pools_budget() {
        let _g = crate::env_lock();
        let (pool, path) = temp_pool().await;
        let caps: std::collections::BTreeMap<String, i64> =
            std::iter::once(("db".to_string(), 1)).collect();

        let yaml = "name: p\ntasks:\n\
                    \x20 - { name: list, command: [\"ls\"], pool: db }\n\
                    \x20 - name: process\n    command: [\"handle\", \"{{ item }}\"]\n    \
                    pool: db\n    depends_on: [list]\n    with_output_of: list\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // `list` runs and prints two partitions.
        let claimed = claim_ready_classes(&pool, "w", 1, &[], &caps).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].name, "list");
        mark_task_succeeded(&pool, &claimed[0].id, "w", claimed[0].version + 1, Some(r#"["a","b"]"#.into()))
            .await
            .unwrap();

        // The barrier parks, holding no worker — and must not hold the slot.
        advance_ready_tasks(&pool).await.unwrap();
        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(events.len(), 1, "the barrier expanded");

        let claimed = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "the pool's one slot goes to an instance; a parked barrier squatting it \
             would deadlock the barrier against its own children"
        );
        assert!(claimed[0].name.starts_with("process."), "got {}", claimed[0].name);

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

    /// `concurrency_key` scopes the cap: one run per key at a time, but distinct
    /// keys admit side by side, and the refusal names the key.
    #[tokio::test]
    async fn concurrency_key_caps_runs_per_key() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: deploy\nmax_active_runs: 1\nconcurrency_key: \"{{ stack }}\"\nparameters: { stack: \"\" }\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let with = |stack: &str| {
            let mut p = std::collections::BTreeMap::new();
            p.insert("stack".to_string(), stack.to_string());
            DagGraph::from_yaml_with_params(yaml, &p).unwrap()
        };

        let prod = create_run(&pool, &with("prod"), yaml).await.unwrap();
        // A second prod run is refused, and the error carries the key.
        let err = create_run(&pool, &with("prod"), yaml).await.expect_err("prod is busy");
        let cap = err.downcast_ref::<crate::models::MaxActiveRunsReached>().expect("typed refusal");
        assert_eq!(cap.key.as_deref(), Some("prod"));
        assert_eq!(cap.active, 1);
        // Another stack is unaffected.
        create_run(&pool, &with("staging"), yaml).await.expect("staging is a different key");

        // The key is stored on the run, and freeing prod's slot admits prod again.
        let stored: Option<String> =
            sqlx::query_scalar("SELECT concurrency_key FROM workflow_runs WHERE id = ?")
                .bind(&prod)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored.as_deref(), Some("prod"));
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
            .bind(&prod)
            .execute(&pool)
            .await
            .unwrap();
        create_run(&pool, &with("prod"), yaml).await.expect("prod slot freed");

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

        let swept = cancel_overdue_runs(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
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

    // ---- the IaC library's promotion chain -------------------------------------
    //
    // A task whose dependency failed is *skipped*, never failed, and `none_failed`
    // reads a skip as fine. So a stage whose plan failed, whose review was
    // rejected or whose apply failed ends with its last task skipped, which a
    // next stage waiting on that task with `none_failed` cannot tell from "nothing
    // to change". These tests run the shipped examples through the real scheduler.

    /// How a scripted task finishes in [`drive`].
    #[cfg(feature = "ops")]
    enum Step {
        Done(&'static str),
        Fail,
        Approve,
        Reject,
    }

    /// The shipped `iac-library`, merged into `workflow` the way `use:` does it
    /// (the API resolves `use:`; the engine only ever sees the merged spec).
    #[cfg(feature = "ops")]
    fn merged_iac(workflow: &str, only: Option<&str>) -> String {
        let lib: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../../../../examples/iac/iac-library.yaml")).unwrap();
        let mut templates = lib["templates"].as_sequence().unwrap().clone();
        if let Some(name) = only {
            templates.retain(|t| t["name"].as_str() == Some(name));
        }
        let mut wf: serde_yaml::Value = serde_yaml::from_str(workflow).unwrap();
        let m = wf.as_mapping_mut().unwrap();
        m.remove("use");
        m.insert("templates".into(), templates.into());
        serde_yaml::to_string(&wf).unwrap()
    }

    /// Run a run to quiescence, finishing each task the way `script` says. A task
    /// that becomes claimable without an entry is one that should not have run,
    /// so it panics instead of being quietly completed.
    #[cfg(feature = "ops")]
    async fn drive(
        pool: &Pool,
        run_id: &str,
        script: &[(&str, Step)],
    ) -> std::collections::HashMap<String, crate::models::TaskStatus> {
        use crate::models::TaskStatus;
        let step_of = |name: &str| script.iter().find(|(n, _)| *n == name).map(|(_, s)| s);
        loop {
            advance_ready_tasks(pool).await.unwrap();
            let mut progressed = false;
            for t in claim_ready(pool, "w", 50).await.unwrap() {
                let ok = match step_of(&t.name) {
                    Some(Step::Done(out)) => {
                        mark_task_succeeded(pool, &t.id, "w", t.version + 1, Some(out.to_string())).await.unwrap()
                    }
                    Some(Step::Fail) => {
                        mark_task_failed(pool, &t.id, "w", t.version + 1, Some("boom".into())).await.unwrap()
                    }
                    _ => panic!("task '{}' ran, but an earlier stage had not completed", t.name),
                };
                assert!(ok, "could not finish '{}'", t.name);
                progressed = true;
            }
            for t in list_tasks(pool, run_id).await.unwrap() {
                if t.status == TaskStatus::AwaitingApproval {
                    let approve = match step_of(&t.name) {
                        Some(Step::Approve) => true,
                        Some(Step::Reject) => false,
                        _ => panic!("gate '{}' parked, but an earlier stage had not completed", t.name),
                    };
                    assert!(resolve_approval(pool, run_id, &t.id, approve, Some("alice"), None).await.unwrap());
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        list_tasks(pool, run_id).await.unwrap().into_iter().map(|t| (t.name, t.status)).collect()
    }

    #[cfg(feature = "ops")]
    async fn iac_run(workflow: &str, only: Option<&str>, promote_through: &str) -> (Pool, std::path::PathBuf, String) {
        let (pool, path) = temp_pool().await;
        let yaml = merged_iac(workflow, only);
        let overrides = std::collections::BTreeMap::from([("promote_through".to_string(), promote_through.to_string())]);
        // Expanding reads DAGRON_MAX_TASKS_PER_RUN, which a fan-out test lowers
        // to 4 for the process: the examples are larger than that. Held for
        // the expansion alone, not across an await.
        let dag = {
            let _g = crate::env_lock();
            DagGraph::from_yaml_with_params(&yaml, &overrides).expect("the merged example must validate")
        };
        let run_id = create_run(&pool, &dag, &yaml).await.unwrap();
        (pool, path, run_id)
    }

    #[cfg(feature = "ops")]
    fn tf_checks_pass() -> Vec<(&'static str, Step)> {
        ["checks.fmt", "checks.init", "checks.validate", "checks.lint", "checks.compliance"]
            .into_iter()
            .map(|n| (n, Step::Done("")))
            .collect()
    }

    #[cfg(feature = "ops")]
    const PROMOTE_TF: &str = include_str!("../../../../examples/iac/promote_terraform.yaml");
    #[cfg(feature = "ops")]
    const PROMOTE_PULUMI: &str = include_str!("../../../../examples/iac/promote_pulumi.yaml");

    /// dev's plan fails: nothing after it may run, and no later stage may be
    /// left looking finished.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn iac_a_failed_plan_does_not_promote() {
        use crate::models::TaskStatus::*;
        let (pool, path, run_id) = iac_run(PROMOTE_TF, None, "prod").await;
        let mut script = tf_checks_pass();
        script.extend([("dev.plan", Step::Fail), ("notify_failure", Step::Done(""))]);
        let st = drive(&pool, &run_id, &script).await;
        assert_eq!(st["dev.plan"], Failed);
        for t in ["dev.done", "staging.plan", "staging.done", "prod.plan", "prod.done"] {
            assert_eq!(st[t], Skipped, "{t}");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An approver rejects staging: prod must not be planned, let alone offered.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn iac_a_rejected_review_does_not_promote() {
        use crate::models::TaskStatus::*;
        let (pool, path, run_id) = iac_run(PROMOTE_TF, None, "prod").await;
        let mut script = tf_checks_pass();
        script.extend([
            ("dev.plan", Step::Done("")),
            ("dev.changed", Step::Done("changes")),
            ("dev.apply", Step::Done("")),
            ("dev.done", Step::Done("")),
            ("staging.plan", Step::Done("")),
            ("staging.changed", Step::Done("changes")),
            ("staging.review", Step::Reject),
            ("notify_failure", Step::Done("")),
        ]);
        let st = drive(&pool, &run_id, &script).await;
        assert_eq!(st["staging.review"], Failed);
        for t in ["staging.apply", "staging.done", "prod.plan", "prod.done"] {
            assert_eq!(st[t], Skipped, "{t}");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An apply that fails is the same: the next stage waits for it.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn iac_a_failed_apply_does_not_promote() {
        use crate::models::TaskStatus::*;
        let (pool, path, run_id) = iac_run(PROMOTE_TF, None, "staging").await;
        let mut script = tf_checks_pass();
        script.extend([
            ("dev.plan", Step::Done("")),
            ("dev.changed", Step::Done("changes")),
            ("dev.apply", Step::Fail),
            ("notify_failure", Step::Done("")),
        ]);
        let st = drive(&pool, &run_id, &script).await;
        assert_eq!(st["dev.apply"], Failed);
        for t in ["dev.done", "staging.plan", "staging.done"] {
            assert_eq!(st[t], Skipped, "{t}");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The other direction: a stage with nothing to change is complete, and an
    /// approved one is too, so promotion still reaches prod.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn iac_a_stage_that_changes_nothing_still_promotes() {
        use crate::models::TaskStatus::*;
        let (pool, path, run_id) = iac_run(PROMOTE_TF, None, "prod").await;
        let mut script = tf_checks_pass();
        script.extend([
            ("dev.plan", Step::Done("")),
            ("dev.changed", Step::Done("no_changes")),
            ("dev.done", Step::Done("")),
            ("staging.plan", Step::Done("")),
            ("staging.changed", Step::Done("changes")),
            ("staging.review", Step::Approve),
            ("staging.apply", Step::Done("")),
            ("staging.done", Step::Done("")),
            ("prod.plan", Step::Done("")),
            ("prod.changed", Step::Done("no_changes")),
            ("prod.done", Step::Done("")),
        ]);
        let st = drive(&pool, &run_id, &script).await;
        assert_eq!(st["dev.apply"], Skipped, "nothing to apply");
        assert_eq!(st["staging.apply"], Succeeded);
        for t in ["dev.done", "staging.done", "prod.done"] {
            assert_eq!(st[t], Succeeded, "{t}");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `pulumi_stage` has the same contract.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn iac_pulumi_a_failed_plan_does_not_promote() {
        use crate::models::TaskStatus::*;
        let (pool, path, run_id) = iac_run(PROMOTE_PULUMI, Some("pulumi_stage"), "prod").await;
        let st = drive(&pool, &run_id, &[("dev.plan", Step::Fail)]).await;
        assert_eq!(st["dev.plan"], Failed);
        for t in ["dev.done", "staging.plan", "staging.done", "prod.plan", "prod.done"] {
            assert_eq!(st[t], Skipped, "{t}");
        }
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

        assert_eq!(
            fire_deadline_alerts(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![run_id.clone()]
        );
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
    /// log; a stale fence and a terminal row are both refused.
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
        let out: Option<String> = sqlx::query_scalar("SELECT log FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("hello\nworld\n"));

        // A stale fence (wrong version) writes nothing.
        append_task_output(&pool, &task.id, fence + 5, "STALE\n", false).await.unwrap();
        let out: Option<String> = sqlx::query_scalar("SELECT log FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("hello\nworld\n"), "stale fence must not append");

        // A terminal task refuses appends (guarded on status='running').
        assert!(mark_task_succeeded(&pool, &task.id, "w", fence, Some("final".into())).await.unwrap());
        append_task_output(&pool, &task.id, fence, "LATE\n", false).await.unwrap();
        let out: Option<String> = sqlx::query_scalar("SELECT log FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(out.as_deref(), Some("hello
world
"), "terminal row is immutable to appends");
        // The streamed log and the task's value are separate columns: completion
        // sets `output` and leaves the log as streamed.
        let value: Option<String> = sqlx::query_scalar("SELECT output FROM task_runs WHERE id = ?")
            .bind(&task.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some("final"));

        let _ = run_id;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A failure reason joins a streamed log (which the log views read first), and
    /// never starts one: a task without a log already shows its `output`.
    #[tokio::test]
    async fn note_task_log_appends_only_to_an_existing_log() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: t
tasks:
  - name: a
    command: [\"true\"]
";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let task = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let fence = task.version + 1;
        let log = |pool: Pool, id: String| async move {
            sqlx::query_scalar::<_, Option<String>>("SELECT log FROM task_runs WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        note_task_log(&pool, &task.id, fence, "reason").await.unwrap();
        assert_eq!(log(pool.clone(), task.id.clone()).await, None, "no log, nothing started");

        append_task_output(&pool, &task.id, fence, "partial
", true).await.unwrap();
        note_task_log(&pool, &task.id, fence, "task timed out after 5s").await.unwrap();
        assert_eq!(
            log(pool.clone(), task.id.clone()).await.as_deref(),
            Some("partial
task timed out after 5s
")
        );

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

    /// A run its parameters leave no task in (each task's `when:` false for
    /// them) is ended by the reaper as one with nothing to wait for: it
    /// succeeds, with its `run.completed` event, where it stayed `running`
    /// and a caller waiting on it waited for ever. A run with a task still
    /// to run is not ended by it.
    #[tokio::test]
    async fn a_run_its_parameters_leave_no_task_in_succeeds() {
        let (pool, path) = temp_pool().await;
        register_outbox_sink(&pool, &crate::db::WEBHOOK_SINK)
            .await
            .unwrap();
        let yaml = "name: tags-only\nparameters: { event: push }\ntasks:\n  \
            - { name: release, command: [\"true\"], when: \"{{ event }} == tag\" }\n";
        let dag =
            DagGraph::from_yaml_with_params(yaml, &std::collections::BTreeMap::new()).unwrap();
        assert!(dag.spec.tasks.is_empty(), "the when leaves the task out");
        let empty = create_run(&pool, &dag, yaml).await.unwrap();

        let other = "name: other\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let busy = create_run(&pool, &DagGraph::from_yaml(other).unwrap(), other)
            .await
            .unwrap();

        let finalized = reap_completed_runs(&pool).await.unwrap();
        assert_eq!(finalized.len(), 1, "{finalized:?}");
        assert_eq!(finalized[0].0, empty);
        assert!(matches!(finalized[0].1, RunStatus::Succeeded));
        let status = |id: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, String>("SELECT status FROM workflow_runs WHERE id = ?")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(status(empty.clone()).await, "succeeded");
        assert_eq!(
            status(busy).await,
            "running",
            "a run with a task to run goes on"
        );
        let events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_outbox WHERE run_id = ? AND event_type = 'run.completed'",
        )
        .bind(&empty)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(events, 1, "its end is announced as any run's");
        assert!(
            reap_completed_runs(&pool).await.unwrap().is_empty(),
            "ended once"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Transactional outbox: finalizing a run emits exactly one pending
    /// `run.completed` event (atomic with the finalize), and the drain API can
    /// claim → deliver it, after which it is not re-claimed.
    #[tokio::test]
    async fn reap_emits_outbox_event_then_drains() {
        let (pool, path) = temp_pool().await;
        register_outbox_sink(&pool, &crate::db::WEBHOOK_SINK)
            .await
            .unwrap();
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

        let batch = claim_outbox_batch(&pool, &crate::db::WEBHOOK_SINK, 10, 30)
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_type, "run.completed");
        assert_eq!(batch[0].run_id, run_id);

        // Leased rows aren't re-claimed within the lease window.
        assert!(
            claim_outbox_batch(&pool, &crate::db::WEBHOOK_SINK, 10, 30)
                .await
                .unwrap()
                .is_empty(),
            "a leased event is not re-claimed"
        );

        mark_outbox_delivered(&pool, "webhook", &batch[0].id)
            .await
            .unwrap();
        let delivered: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_deliveries WHERE sink = 'webhook' AND status = 'delivered'")
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
    /// and is a no-op (`None`) the second time.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn cancel_run_is_idempotent() {
        let (pool, path) = temp_pool().await;
        let run_id = seed_run(&pool).await;

        assert_eq!(
            cancel_run(&pool, &run_id).await.unwrap(),
            Some(crate::models::Cancelled { cancelled: 2, stopping: 0 }),
            "both tasks called off, and neither had started"
        );
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status.to_string(), "cancelled");
        let tasks = list_tasks(&pool, &run_id).await.unwrap();
        assert!(
            tasks.iter().all(|t| t.status == crate::models::TaskStatus::Cancelled),
            "all tasks cancelled"
        );

        assert_eq!(
            cancel_run(&pool, &run_id).await.unwrap(),
            None,
            "second cancel is a no-op"
        );
        assert_eq!(cancel_run(&pool, "no-such-run").await.unwrap(), None);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `seed_run`'s run with its first task claimed by `w`: the run, the task
    /// and the claim's fence.
    #[cfg(feature = "ops")]
    async fn running_run(pool: &Pool) -> (String, TaskRun, i64) {
        let run_id = seed_run(pool).await;
        advance_ready_tasks(pool).await.unwrap();
        let task = claim_ready(pool, "w", 10).await.unwrap().remove(0);
        let fence = task.version + 1;
        (run_id, task, fence)
    }

    /// A task's status and whether it is asked to stop.
    #[cfg(feature = "ops")]
    async fn stopping(pool: &Pool, task_id: &str) -> (String, bool) {
        let (status, asked): (String, Option<String>) =
            sqlx::query_as("SELECT status, stop_requested_at FROM task_runs WHERE id = ?")
                .bind(task_id)
                .fetch_one(pool)
                .await
                .unwrap();
        (status, asked.is_some())
    }

    /// A cancel asks. The task a worker holds stays running, with its claim,
    /// until that worker says its work is gone, and the run stays running
    /// with it: no event, no end-of-run work, nothing that reads as over.
    /// Then both are cancelled, once. It used to write `cancelled` on both in
    /// the cancel's own statement, while the container ran on.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_cancel_asks_a_running_task_to_stop_and_the_run_ends_when_it_has() {
        use crate::models::Cancelled;
        let (pool, path) = temp_pool().await;
        let (run_id, task, fence) = running_run(&pool).await;

        assert_eq!(
            cancel_run(&pool, &run_id).await.unwrap(),
            Some(Cancelled { cancelled: 1, stopping: 1 }),
            "the task that had not started is cancelled; the one a worker holds is asked"
        );
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Running, "its work has not stopped");
        assert_eq!(run.stopping_as.as_deref(), Some("cancelled"));
        assert!(run.stop_requested_at.is_some());
        assert_eq!(run.finished_at, None);
        let tasks = list_tasks(&pool, &run_id).await.unwrap();
        let (a, b) = (&tasks[0], &tasks[1]);
        assert_eq!((a.name.as_str(), a.status.to_string().as_str()), ("a", "running"));
        assert!(a.stop_requested_at.is_some());
        assert_eq!(a.claimed_by.as_deref(), Some("w"), "its claim is its worker's still");
        assert_eq!((b.name.as_str(), b.status.to_string().as_str()), ("b", "cancelled"));
        assert_eq!(b.stop_requested_at, None);
        assert!(completed_events(&pool, &run_id).await.is_empty(), "no end is announced");
        assert!(ends_owed(&pool).await.is_empty());

        // The worker's claim holds, and its engine learns at its next sweep.
        assert!(renew_task_lease(&pool, &task.id, "w", fence, 30).await.unwrap());
        assert_eq!(
            tasks_to_stop(&pool, "w", &[(task.id.clone(), fence)]).await.unwrap(),
            [task.id.clone()]
        );

        // A second cancel changes nothing, and says what is still stopping.
        assert_eq!(
            cancel_run(&pool, &run_id).await.unwrap(),
            Some(Cancelled { cancelled: 0, stopping: 1 })
        );
        // Only the claim that holds it can say its work is gone.
        assert!(!settle_stopped_task(&pool, &task.id, "another", fence).await.unwrap());
        assert!(!settle_stopped_task(&pool, &task.id, "w", fence + 1).await.unwrap());
        assert_eq!(stopping(&pool, &task.id).await, ("running".into(), true));

        assert!(settle_stopped_task(&pool, &task.id, "w", fence).await.unwrap());
        assert_eq!(stopping(&pool, &task.id).await, ("cancelled".into(), false));
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Cancelled);
        assert_eq!((run.stopping_as, run.stop_requested_at), (None, None));
        assert!(run.finished_at.is_some());
        assert_eq!(completed_events(&pool, &run_id).await, ["cancelled"]);
        assert_eq!(
            ends_owed(&pool).await,
            vec![(run_id.clone(), RunStatus::Cancelled)]
        );

        // And once: nothing is left to settle or to cancel.
        assert!(!settle_stopped_task(&pool, &task.id, "w", fence).await.unwrap());
        assert_eq!(cancel_run(&pool, &run_id).await.unwrap(), None);
        assert_eq!(completed_events(&pool, &run_id).await, ["cancelled"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A task that was asked to stop takes no other result. One that ends by
    /// itself first is cancelled where it would have succeeded, failed, been
    /// queued for a retry or handed back to its queue; and it is not parked.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_task_asked_to_stop_takes_no_other_result() {
        let (pool, path) = temp_pool().await;
        let now = chrono::Utc::now().to_rfc3339();

        let (run, t, fence) = running_run(&pool).await;
        cancel_run(&pool, &run).await.unwrap();
        assert!(!mark_task_succeeded(&pool, &t.id, "w", fence, Some("done".into())).await.unwrap());
        assert_eq!(stopping(&pool, &t.id).await, ("cancelled".into(), false));
        assert_eq!(completed_events(&pool, &run).await, ["cancelled"], "and its run ends with it");

        let (run, t, fence) = running_run(&pool).await;
        cancel_run(&pool, &run).await.unwrap();
        assert!(!mark_task_failed(&pool, &t.id, "w", fence, Some("boom".into())).await.unwrap());
        assert_eq!(stopping(&pool, &t.id).await, ("cancelled".into(), false));

        let (run, t, fence) = running_run(&pool).await;
        cancel_run(&pool, &run).await.unwrap();
        assert!(
            !retry_task(&pool, &t.id, "w", fence, Some("boom".into()), now.clone(), AttemptEnd::Failed)
                .await
                .unwrap(),
            "it does not run again"
        );
        assert_eq!(stopping(&pool, &t.id).await, ("cancelled".into(), false));

        let (run, t, fence) = running_run(&pool).await;
        cancel_run(&pool, &run).await.unwrap();
        assert_eq!(
            requeue_quota_wait(&pool, &t.id, "w", fence, "the namespace is full", 3600).await.unwrap(),
            QuotaRequeue::Stale
        );
        assert_eq!(stopping(&pool, &t.id).await, ("cancelled".into(), false));
        assert_eq!(completed_events(&pool, &run).await, ["cancelled"]);

        // Not parked, and not handed back to be claimed again: it stays as it
        // is, for its worker or a teardown to settle.
        let (run, t, fence) = running_run(&pool).await;
        cancel_run(&pool, &run).await.unwrap();
        assert!(!park_wait(&pool, &t.id, fence, &now).await.unwrap());
        assert!(!park_external(&pool, &t.id, fence, "http", "job-1", None, &now, None).await.unwrap());
        assert!(!release_subworkflow_task(&pool, &t.id, fence).await.unwrap());
        assert_eq!(stopping(&pool, &t.id).await, ("running".into(), true));
        assert!(completed_events(&pool, &run).await.is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run past its deadline whose task a worker holds is failed once that
    /// task has stopped, and is not asked twice. A cancel in between does not
    /// make it a cancelled run.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_run_past_its_deadline_fails_once_its_running_task_has_stopped() {
        let (pool, path) = temp_pool().await;
        let yaml =
            "name: slow\nrun_timeout_secs: 60\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let task = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let fence = task.version + 1;
        sqlx::query(
            "UPDATE workflow_runs SET deadline_at = '2020-01-01T00:00:00+00:00' WHERE id = ?",
        )
        .bind(&run_id)
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            cancel_overdue_runs(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![run_id.clone()]
        );
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty(), "it is asked once");
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Running);
        assert_eq!(run.stopping_as.as_deref(), Some("failed"));
        assert_eq!(stopping(&pool, &task.id).await, ("running".into(), true));
        assert!(completed_events(&pool, &run_id).await.is_empty());

        // A cancel now finds it stopping already, and leaves it to fail.
        assert_eq!(
            cancel_run(&pool, &run_id).await.unwrap(),
            Some(crate::models::Cancelled { cancelled: 0, stopping: 1 })
        );

        assert!(settle_stopped_task(&pool, &task.id, "w", fence).await.unwrap());
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.output.as_deref(), Some("run deadline exceeded (run_timeout_secs)"));
        assert_eq!(stopping(&pool, &task.id).await, ("cancelled".into(), false));
        assert_eq!(completed_events(&pool, &run_id).await, ["failed"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A cancelled run holds its concurrency key until its work has stopped:
    /// the next run of the key is refused while it has, and admitted after.
    /// The key used to be free from the cancel on, with the old run's
    /// container still running beside the new one's.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_cancelled_run_holds_its_concurrency_key_until_its_work_has_stopped() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: deploy\nmax_active_runs: 1\nconcurrency_key: stack-a\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let first = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let task = claim_ready(&pool, "w", 10).await.unwrap().remove(0);

        cancel_run(&pool, &first).await.unwrap();
        let refused = create_run(&pool, &dag, yaml).await.unwrap_err();
        assert!(
            refused.downcast_ref::<crate::models::MaxActiveRunsReached>().is_some(),
            "{refused}"
        );

        assert!(settle_stopped_task(&pool, &task.id, "w", task.version + 1).await.unwrap());
        create_run(&pool, &dag, yaml).await.expect("the key is free once the work is gone");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A task that is asked to stop is given a whole lease to answer in. One
    /// that had outlived its lease, as a task may with no lease heartbeat,
    /// would otherwise be claimed for a teardown in the tick that asked it,
    /// ahead of the worker that is stopping it. Asked again, it keeps the
    /// lease it has: one cut short on purpose stays short.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_task_asked_to_stop_is_given_a_lease_to_answer_in() {
        let (pool, path) = temp_pool().await;
        let lease = |id: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, Option<String>>(
                    "SELECT lease_expires_at FROM task_runs WHERE id = ?",
                )
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
                .expect("a claimed task has a lease")
            }
        };
        let (run_id, task, fence) = running_run(&pool).await;
        // It has outlived its lease: nothing renewed it.
        sqlx::query("UPDATE task_runs SET lease_expires_at = '2020-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&task.id)
            .execute(&pool)
            .await
            .unwrap();

        let asked_at = chrono::Utc::now().to_rfc3339();
        cancel_run(&pool, &run_id).await.unwrap();
        assert!(lease(task.id.clone()).await > asked_at, "a lease from the request on");
        assert!(
            claim_teardowns(&pool, "w2", 10, &[], &[]).await.unwrap().is_empty(),
            "its worker has that long to say the work is gone"
        );
        assert!(settle_stopped_task(&pool, &task.id, "w", fence).await.unwrap(), "and its word counts");

        // Cut short because its teardown was not seen through, then asked
        // again: the lease stays as it was cut.
        let (run_id, task, fence) = running_run(&pool).await;
        cancel_run(&pool, &run_id).await.unwrap();
        assert!(release_stopping_task(&pool, &task.id, "w", fence, -1).await.unwrap());
        let cut = lease(task.id.clone()).await;
        cancel_run(&pool, &run_id).await.unwrap();
        assert_eq!(lease(task.id.clone()).await, cut);
        assert_eq!(claim_teardowns(&pool, "w2", 10, &[], &[]).await.unwrap().len(), 1);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An engine from before the stop request finishes a task that was asked
    /// to stop as it always did, and ends no run: the run would read running
    /// for good. The sweep ends it, clears the request the task still carried,
    /// and cancels a task such an engine had queued to run again.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_run_an_older_engine_left_with_nothing_stopping_is_ended_by_the_sweep() {
        let (pool, path) = temp_pool().await;

        // It marks the task succeeded, by a statement that knows no request.
        let (run_id, task, _) = running_run(&pool).await;
        cancel_run(&pool, &run_id).await.unwrap();
        assert_eq!(end_stopped_runs(&pool).await.unwrap(), 0, "a task is still stopping");
        sqlx::query("UPDATE task_runs SET status = 'succeeded', claimed_by = NULL WHERE id = ?")
            .bind(&task.id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(get_run(&pool, &run_id).await.unwrap().unwrap().status, RunStatus::Running);
        assert_eq!(end_stopped_runs(&pool).await.unwrap(), 1);
        assert_eq!(get_run(&pool, &run_id).await.unwrap().unwrap().status, RunStatus::Cancelled);
        assert_eq!(stopping(&pool, &task.id).await, ("succeeded".into(), false));
        assert_eq!(completed_events(&pool, &run_id).await, ["cancelled"]);
        assert_eq!(end_stopped_runs(&pool).await.unwrap(), 0, "once");

        // It queues the task for a retry: that task must not run again.
        let (run_id, task, _) = running_run(&pool).await;
        cancel_run(&pool, &run_id).await.unwrap();
        sqlx::query(
            "UPDATE task_runs SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL
             WHERE id = ?",
        )
        .bind(&task.id)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(end_stopped_runs(&pool).await.unwrap(), 1);
        assert_eq!(stopping(&pool, &task.id).await, ("cancelled".into(), false));
        assert_eq!(get_run(&pool, &run_id).await.unwrap().unwrap().status, RunStatus::Cancelled);
        assert!(claim_ready(&pool, "w", 10).await.unwrap().is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A stopping task whose lease has lapsed has nobody watching its work:
    /// its worker died, or could not see its teardown through. It is not run
    /// again. It is claimed for a teardown alone, under a new claim and with
    /// no attempt spent, and that claim's word settles it.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_stopping_task_whose_lease_lapsed_is_claimed_for_a_teardown_alone() {
        let (pool, path) = temp_pool().await;
        let (run_id, task, fence) = running_run(&pool).await;
        cancel_run(&pool, &run_id).await.unwrap();
        assert!(
            claim_teardowns(&pool, "w2", 10, &[], &[]).await.unwrap().is_empty(),
            "its worker's lease still holds"
        );

        // The worker could not see its teardown through: it says so, and its
        // lease is cut short.
        assert!(!release_stopping_task(&pool, &task.id, "another", fence, -1).await.unwrap());
        assert!(release_stopping_task(&pool, &task.id, "w", fence, -1).await.unwrap());

        assert_eq!(recover_expired_leases(&pool).await.unwrap(), 0, "it is not run again");
        assert!(claim_ready(&pool, "w2", 10).await.unwrap().is_empty());
        assert!(
            claim_teardowns(&pool, "w2", 10, &["some-other-class".to_string()], &[])
                .await
                .unwrap()
                .is_empty(),
            "nor torn down by an engine that does not serve its class"
        );
        assert!(
            claim_teardowns(&pool, "w", 10, &[], &[task.id.clone()]).await.unwrap().is_empty(),
            "nor by the engine that still has its execution out, whatever its lease says"
        );

        let torn = claim_teardowns(&pool, "w2", 10, &[], &[]).await.unwrap();
        assert_eq!(torn.len(), 1);
        assert_eq!(torn[0].id, task.id);
        assert_eq!(torn[0].version, fence, "the version from before this claim, as a claim returns");
        let (attempt, by): (i64, Option<String>) =
            sqlx::query_as("SELECT attempt, claimed_by FROM task_runs WHERE id = ?")
                .bind(&task.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((attempt, by.as_deref()), (1, Some("w2")), "no attempt is spent on a teardown");
        assert!(claim_teardowns(&pool, "w3", 10, &[], &[]).await.unwrap().is_empty(), "one claim");

        // The worker that held it is too late, and the teardown's word counts.
        assert!(!settle_stopped_task(&pool, &task.id, "w", fence).await.unwrap());
        assert!(settle_stopped_task(&pool, &task.id, "w2", fence + 1).await.unwrap());
        assert_eq!(stopping(&pool, &task.id).await, ("cancelled".into(), false));
        assert_eq!(completed_events(&pool, &run_id).await, ["cancelled"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The ends an end-of-run sink is handed now, as `(run, status)` in the
    /// order they were written, each then done: the work a run's end owes,
    /// whichever path ended it. It is configured with the database
    /// (`temp_pool`), so it is handed every end.
    async fn ends_owed(pool: &Pool) -> Vec<(String, RunStatus)> {
        let mut out = Vec::new();
        for ev in claim_outbox_batch(pool, &ENDS, 100, 60).await.unwrap() {
            let v: serde_json::Value = serde_json::from_str(&ev.payload).unwrap();
            out.push((
                ev.run_id.clone(),
                v["status"].as_str().unwrap().parse().unwrap(),
            ));
            mark_outbox_delivered(pool, ENDS.name, &ev.id)
                .await
                .unwrap();
        }
        out
    }

    /// The statuses of the `run.completed` events in the outbox for `run_id`,
    /// oldest first. Each payload must name the run.
    async fn completed_events(pool: &Pool, run_id: &str) -> Vec<String> {
        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM event_outbox WHERE run_id = ? AND event_type = 'run.completed'
             ORDER BY created_at",
        )
        .bind(run_id)
        .fetch_all(pool)
        .await
        .unwrap();
        payloads
            .iter()
            .map(|p| {
                let v: serde_json::Value = serde_json::from_str(p).unwrap();
                assert_eq!(v["run_id"], run_id, "{p}");
                v["status"].as_str().unwrap().to_string()
            })
            .collect()
    }

    /// A cancel ends the run fully: one `run.completed` event with
    /// status `cancelled`, written with the cancel, and its end-of-run work
    /// owed once to each end-of-run sink. And nothing is stamped for the
    /// loop to claim (`completion_pending_at`), as a release before 10.7 did. Before this, a cancelled run emitted
    /// nothing, and a commit status it had set stayed "pending" forever.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_cancelled_run_emits_its_end_once_and_owes_its_end_of_run_work_once() {
        let (pool, path) = temp_pool().await;
        let run_id = seed_run(&pool).await;
        assert!(
            ends_owed(&pool).await.is_empty(),
            "a running run owes nothing"
        );

        assert!(cancel_run(&pool, &run_id).await.unwrap().is_some());
        assert!(cancel_run(&pool, &run_id).await.unwrap().is_none());
        assert_eq!(
            completed_events(&pool, &run_id).await,
            ["cancelled"],
            "one event, from the first cancel only"
        );

        assert_eq!(
            ends_owed(&pool).await,
            vec![(run_id.clone(), RunStatus::Cancelled)]
        );
        assert!(
            ends_owed(&pool).await
                .is_empty(),
            "claimed once"
        );
        assert!(claim_pending_completions(&pool, 10).await.unwrap().is_empty(), "nothing stamped");
        // The reaper never sees it: it reads running runs only.
        assert!(reap_completed_runs(&pool).await.unwrap().is_empty());
        assert_eq!(completed_events(&pool, &run_id).await, ["cancelled"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The run_timeout_secs sweep ends a run as a cancel does, with status
    /// `failed`, which is what the run is.
    #[tokio::test]
    async fn a_run_failed_by_its_deadline_emits_its_end_and_owes_its_end_of_run_work() {
        let (pool, path) = temp_pool().await;
        let yaml =
            "name: slow\nrun_timeout_secs: 60\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        assert!(
            cancel_overdue_runs(&pool).await.unwrap().is_empty(),
            "not overdue yet"
        );
        sqlx::query(
            "UPDATE workflow_runs SET deadline_at = '2020-01-01T00:00:00+00:00' WHERE id = ?",
        )
        .bind(&run_id)
        .execute(&pool)
        .await
        .unwrap();

        assert_eq!(
            cancel_overdue_runs(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![run_id.clone()]
        );
        assert!(
            cancel_overdue_runs(&pool).await.unwrap().is_empty(),
            "it fails once"
        );
        assert_eq!(completed_events(&pool, &run_id).await, ["failed"]);
        assert_eq!(
            ends_owed(&pool).await,
            vec![(run_id.clone(), RunStatus::Failed)]
        );
        assert!(ends_owed(&pool).await
            .is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run the reaper finalizes owes its end to the end-of-run sinks as a
    /// cancelled one does. And a cancelled run rerun before its end was done
    /// has each end handed, in turn: the engine does an end only while it is
    /// still the run's (`run_end::owed`), so the superseded cancel's work
    /// never runs.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_finished_run_owes_no_claim_and_a_rerun_supersedes_a_pending_end() {
        let (pool, path) = temp_pool().await;
        let run_id = seed_run(&pool).await;

        assert!(cancel_run(&pool, &run_id).await.unwrap().is_some());
        assert!(
            rerun_from_failed(&pool, &run_id).await.unwrap().is_some(),
            "re-armed"
        );
        assert_eq!(
            ends_owed(&pool).await,
            vec![(run_id.clone(), RunStatus::Cancelled)],
            "handed, while the run is running again"
        );
        assert_eq!(
            run_status(&pool, &run_id).await.unwrap(),
            Some(RunStatus::Running)
        );

        for _round in 0..2 {
            advance_ready_tasks(&pool).await.unwrap();
            for t in claim_ready(&pool, "w", 10).await.unwrap() {
                assert!(mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None)
                    .await
                    .unwrap());
            }
        }
        assert_eq!(
            reap_completed_runs(&pool).await.unwrap(),
            vec![(run_id.clone(), RunStatus::Succeeded)]
        );
        assert_eq!(
            ends_owed(&pool).await,
            vec![(run_id.clone(), RunStatus::Succeeded)],
            "the reaper's end is owed like any other"
        );
        assert_eq!(
            completed_events(&pool, &run_id).await,
            ["cancelled", "succeeded"]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    const NOTIFY_GIT: &str = "name: ci\n\
        parameters: { commit_sha: abc }\n\
        notify:\n  git: { provider: github, repo: acme/etl, sha: \"{{ commit_sha }}\" }\n\
        tasks:\n  - name: a\n    command: [\"true\"]\n";

    /// A run created with a `notify.git` block owes its commit a `pending`
    /// status, claimed once; a run without one owes nothing, so the claim
    /// never touches the runs that cannot use it.
    #[tokio::test]
    async fn a_notify_git_run_owes_its_start_once() {
        let (pool, path) = temp_pool().await;
        let plain = "name: plain\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        create_run(&pool, &DagGraph::from_yaml(plain).unwrap(), plain)
            .await
            .unwrap();
        let run_id = create_run(&pool, &DagGraph::from_yaml(NOTIFY_GIT).unwrap(), NOTIFY_GIT)
            .await
            .unwrap();

        assert_eq!(
            claim_pending_starts(&pool, 10).await.unwrap(),
            vec![run_id.clone()]
        );
        assert!(
            claim_pending_starts(&pool, 10).await.unwrap().is_empty(),
            "announced once"
        );
        assert_eq!(
            run_status(&pool, &run_id).await.unwrap(),
            Some(RunStatus::Running)
        );
        assert_eq!(run_status(&pool, "no-such-run").await.unwrap(), None);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run that ends before any tick announced its start never gets the
    /// `pending` status: posted after the end, it would be the status the
    /// commit shows. Every end clears the stamp: the reaper, a cancel, and the
    /// deadline sweep.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_run_that_ends_first_is_never_announced() {
        let (pool, path) = temp_pool().await;
        let dag = DagGraph::from_yaml(NOTIFY_GIT).unwrap();

        // One the reaper ends,
        create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        for t in claim_ready(&pool, "w", 10).await.unwrap() {
            assert!(mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None)
                .await
                .unwrap());
        }
        assert_eq!(reap_completed_runs(&pool).await.unwrap().len(), 1);

        // one a cancel ends,
        let cancelled = create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        assert!(cancel_run(&pool, &cancelled).await.unwrap().is_some());

        // and one the deadline sweep ends.
        let overdue = create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET deadline_at = '2020-01-01T00:00:00+00:00' WHERE id = ?",
        )
        .bind(&overdue)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            cancel_overdue_runs(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![overdue.clone()]
        );

        let stamped: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_runs WHERE start_pending_at IS NOT NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stamped, 0, "every end cleared its start");
        assert!(claim_pending_starts(&pool, 10).await.unwrap().is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The claim takes at most `limit` runs, oldest stamp first, so a mass
    /// cancel a release before 10.7 stamped is worked off over several ticks
    /// rather than in one. Nothing stamps a run now: the stamps are written
    /// here as that release wrote them.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn the_claim_is_bounded_and_oldest_first() {
        let (pool, path) = temp_pool().await;
        let mut runs = Vec::new();
        for _ in 0..3 {
            let run_id = seed_run(&pool).await;
            assert!(cancel_run(&pool, &run_id).await.unwrap().is_some());
            runs.push(run_id);
        }
        // Distinct stamps, oldest first, whatever the clock's resolution.
        for (i, run_id) in runs.iter().enumerate() {
            sqlx::query("UPDATE workflow_runs SET completion_pending_at = ? WHERE id = ?")
                .bind(format!("2026-10-04T00:00:0{i}+00:00"))
                .bind(run_id)
                .execute(&pool)
                .await
                .unwrap();
        }

        let first: Vec<String> = claim_pending_completions(&pool, 2)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(first.len(), 2);
        assert!(
            first.contains(&runs[0]) && first.contains(&runs[1]),
            "{first:?}"
        );
        assert_eq!(
            claim_pending_completions(&pool, 2).await.unwrap(),
            vec![(runs[2].clone(), RunStatus::Cancelled)]
        );
        assert!(claim_pending_completions(&pool, 2)
            .await
            .unwrap()
            .is_empty());

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
        assert!(resolve_approval(&pool, &run_id, &tasks["gate"].id, true, Some("alice"), Some("looks good")).await.unwrap());
        advance_ready_tasks(&pool).await.unwrap();
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].status, crate::models::TaskStatus::Succeeded);
        assert_eq!(tasks["deploy"].status, crate::models::TaskStatus::Ready);
        // Re-approving an already-resolved gate is a no-op (guarded).
        assert_eq!(tasks["gate"].decided_by.as_deref(), Some("alice"));
        assert_eq!(tasks["gate"].decision_comment.as_deref(), Some("looks good"));
        // The decision also lands in the gate's log, so the run's merged log shows
        // who decided and why; `output` stays the bare verdict `when:` reads.
        let log: Option<String> = sqlx::query_scalar("SELECT log FROM task_runs WHERE id = ?")
            .bind(&tasks["gate"].id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let log = log.expect("a decided gate carries a log");
        assert!(log.starts_with("approved by alice at "), "{log}");
        assert!(log.ends_with("\ncomment: looks good"), "{log}");
        assert_eq!(tasks["gate"].output.as_deref(), Some("approved"));
        assert!(!resolve_approval(&pool, &run_id, &tasks["gate"].id, true, Some("mallory"), None).await.unwrap());
        // The losing second decision must not overwrite the first.
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].decided_by.as_deref(), Some("alice"));

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
        assert!(resolve_approval(&pool, &run_id, &tasks["gate"].id, false, None, None).await.unwrap());
        advance_ready_tasks(&pool).await.unwrap();
        let tasks = by_name(&list_tasks(&pool, &run_id).await.unwrap());
        assert_eq!(tasks["gate"].status, crate::models::TaskStatus::Failed);
        assert_eq!(tasks["deploy"].status, crate::models::TaskStatus::Skipped, "all_success dependent skipped");
        let log = |id: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, Option<String>>("SELECT log FROM task_runs WHERE id = ?")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    .expect("a decided gate carries a log")
            }
        };
        // No decider on this path (the engine API has no caller identity).
        let rejected = log(tasks["gate"].id.clone()).await;
        assert!(
            rejected.starts_with("rejected by unknown at "),
            "{rejected}"
        );
        assert!(
            !rejected.contains('\n'),
            "no comment, no comment line: {rejected}"
        );

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
        let resolved: Vec<(String, bool)> = resolve_expired_approvals(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, approved, _)| (id, approved))
            .collect();
        assert_eq!(resolved, vec![(gate2.clone(), false)], "default reject");
        let g2 = by_name(&list_tasks(&pool, &run2).await.unwrap());
        assert_eq!(g2["gate"].decided_by.as_deref(), Some("timeout"));
        assert!(g2["gate"].decision_comment.as_deref().unwrap_or("").contains("60s"));
        let timed_out = log(gate2.clone()).await;
        assert!(
            timed_out.starts_with("rejected by timeout at "),
            "{timed_out}"
        );
        assert!(
            timed_out
                .ends_with("\ncomment: approval_timeout_secs (60s) elapsed; defaulted to reject"),
            "{timed_out}"
        );
        let st: String = sqlx::query_scalar("SELECT status FROM task_runs WHERE id = ?")
            .bind(&gate2)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(st, "failed");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `examples/terraform/lifecycle.yaml` through the scheduler: the teardown is
    /// planned only after the deploy applied or had nothing to apply. A rejected
    /// apply, a failed check and a failed plan all leave the deploy's apply
    /// skipped, and none of them may reach the teardown's DESTROY gate.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn terraform_lifecycle_example_tears_down_only_after_the_deploy() {
        /// Run the example to a standstill: every task succeeds except `fail`,
        /// every `*.changed` prints `changed`, every gate is approved except
        /// `reject`. Returns the tasks that ran and the gates that asked.
        async fn drive(fail: &str, changed: &str, reject: &str) -> (Vec<String>, Vec<String>) {
            let yaml = include_str!("../../../../examples/terraform/lifecycle.yaml");
            let (pool, path) = temp_pool().await;
            let dag = DagGraph::from_yaml_with_params(yaml, &Default::default()).unwrap();
            let run_id = create_run(&pool, &dag, yaml).await.unwrap();
            let (mut ran, mut asked) = (Vec::new(), Vec::new());
            for _ in 0..100 {
                let moved = advance_ready_tasks(&pool).await.unwrap();
                let claimed = claim_ready(&pool, "w", 50).await.unwrap();
                for t in &claimed {
                    ran.push(t.name.clone());
                    if t.name == fail {
                        mark_task_failed(&pool, &t.id, "w", t.version + 1, None)
                            .await
                            .unwrap();
                    } else {
                        let out = t.name.ends_with(".changed").then(|| changed.to_string());
                        mark_task_succeeded(&pool, &t.id, "w", t.version + 1, out)
                            .await
                            .unwrap();
                    }
                }
                let parked: Vec<TaskRun> = list_tasks(&pool, &run_id)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|t| t.status == crate::models::TaskStatus::AwaitingApproval)
                    .collect();
                for g in &parked {
                    asked.push(g.name.clone());
                    let approve = g.name != reject;
                    assert!(
                        resolve_approval(&pool, &run_id, &g.id, approve, Some("alice"), None)
                            .await
                            .unwrap()
                    );
                }
                if moved == 0 && claimed.is_empty() && parked.is_empty() {
                    break;
                }
            }
            pool.close().await;
            let _ = std::fs::remove_file(&path);
            (ran, asked)
        }
        let ran_teardown = |ran: &[String]| ran.iter().any(|n| n.starts_with("teardown."));

        // Both approved: apply, then plan the destroy and ask again.
        let (ran, asked) = drive("", "changes", "").await;
        assert_eq!(asked, ["deploy.review", "teardown.review"]);
        assert!(ran.iter().any(|n| n == "teardown.execute"), "{ran:?}");
        // Nothing to change on the deploy: no apply gate, and the teardown still follows.
        let (ran, asked) = drive("", "no_changes", "").await;
        assert!(asked.is_empty(), "{asked:?}");
        assert!(ran.iter().any(|n| n == "teardown.plan"), "{ran:?}");
        // A rejected apply ends it: no teardown plan, no DESTROY gate.
        let (ran, asked) = drive("", "changes", "deploy.review").await;
        assert_eq!(asked, ["deploy.review"]);
        assert!(!ran_teardown(&ran), "{ran:?}");
        // So do a failed check and a failed deploy plan.
        for fail in ["fmt", "deploy.plan"] {
            let (ran, asked) = drive(fail, "changes", "").await;
            assert!(
                asked.is_empty() && !ran_teardown(&ran),
                "{fail}: ran {ran:?}, asked {asked:?}"
            );
        }
        // A rejected teardown destroys nothing.
        let (ran, asked) = drive("", "changes", "teardown.review").await;
        assert_eq!(asked, ["deploy.review", "teardown.review"]);
        assert!(!ran.iter().any(|n| n == "teardown.execute"), "{ran:?}");
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

        // The per-workflow views: the seeded run is recent and unfinished, and
        // a dead letter is grouped under the source that produced it.
        assert_eq!(snap.recent_runs.len(), 1);
        assert_eq!(snap.recent_runs[0].workflow, "demo");
        assert_eq!(snap.recent_runs[0].environment, None);
        assert_eq!(snap.recent_runs[0].status, "running");
        assert_eq!(snap.recent_runs[0].count, 1);
        let active: i64 = snap
            .active_tasks
            .iter()
            .filter(|(workflow, _, _)| workflow == "demo")
            .map(|(_, _, n)| *n)
            .sum();
        let unfinished: i64 = snap
            .tasks_by_status
            .iter()
            .filter(|(s, _)| matches!(s.as_str(), "pending" | "ready" | "running" | "awaiting_approval"))
            .map(|(_, n)| *n)
            .sum();
        assert_eq!(active, unfinished, "one workflow owns every unfinished task");
        assert!(active >= 2);
        assert!(snap.dead_letters_by_source.is_empty());
        record_dead_letter(&pool, "not yaml", "parse error", "kafka", 5).await.unwrap();
        let snap = status_counts(&pool).await.unwrap();
        assert_eq!(snap.dead_letters_by_source.len(), 1);
        assert_eq!(snap.dead_letters_by_source[0].source, "kafka");
        assert_eq!(snap.dead_letters_by_source[0].count, 1);
        assert!(snap.dead_letters_by_source[0].oldest_age_secs(chrono::Utc::now()) < 60);
        let (workflow, created_at) = run_metric_facts(&pool, &run_id).await.unwrap().expect("run exists");
        assert_eq!(workflow, "demo");
        assert!(chrono::DateTime::parse_from_rfc3339(&created_at).is_ok(), "{created_at}");
        assert!(run_metric_facts(&pool, "no-such-run").await.unwrap().is_none());

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

    /// Redrive is all-or-nothing. A refused run leaves the dead letter exactly
    /// as it was, still redrivable, whether the refusal comes after the claim
    /// (the run cap, inside the transaction) or before it (the disk floor). A
    /// created run consumes it, and a consumed id is `None` with no second run.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn redrive_consumes_the_dead_letter_only_with_its_run() {
        let (pool, path) = temp_pool().await;
        let yaml =
            "name: capped\nmax_active_runs: 1\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let blocker = create_run(&pool, &dag, yaml).await.unwrap();
        let id = record_dead_letter(&pool, yaml, "transient", "redis", 3).await.unwrap();
        let parked = serde_json::to_value(get_dead_letter(&pool, &id).await.unwrap()).unwrap();
        let runs = |pool: Pool| async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs")
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        let err = create_run_from_dead_letter(&pool, &dag, yaml, &id, None).await.unwrap_err();
        assert!(err.downcast_ref::<crate::models::MaxActiveRunsReached>().is_some(), "{err}");
        let err = create_run_inner_with_floor(&pool, &dag, yaml, None, Some(&id), None, u64::MAX)
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<crate::models::DatastoreLowOnDisk>().is_some(), "{err}");
        assert_eq!(
            serde_json::to_value(get_dead_letter(&pool, &id).await.unwrap()).unwrap(),
            parked,
            "a refused redrive keeps the row: same id, payload and history"
        );
        assert_eq!(runs(pool.clone()).await, 1, "only the blocker");

        // The cause clears; the same id redrives.
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
            .bind(&blocker)
            .execute(&pool)
            .await
            .unwrap();
        let run_id = create_run_from_dead_letter(&pool, &dag, yaml, &id, None)
            .await
            .unwrap()
            .expect("the row was there to claim");
        assert!(get_dead_letter(&pool, &id).await.unwrap().is_none(), "consumed with its run");
        let spec: String = sqlx::query_scalar(
            "SELECT d.spec FROM workflow_runs r
             JOIN workflow_definitions d ON d.id = r.definition_id WHERE r.id = ?",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(spec, yaml, "the run is the parked payload");

        assert!(create_run_from_dead_letter(&pool, &dag, yaml, &id, None)
            .await
            .unwrap()
            .is_none());
        assert_eq!(runs(pool.clone()).await, 2, "a consumed id creates nothing");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run that is not trusted is confined where it is made
    /// (`crate::confine`). One that names what it may not use is not made,
    /// and leaves nothing behind. One that is made has rows without what it
    /// is not given. A trusted run of the same workflow has all of it.
    #[tokio::test]
    async fn a_run_that_is_not_trusted_is_confined_where_it_is_made() {
        use crate::models::{trigger_kind, RunTrigger};
        let (pool, path) = temp_pool().await;
        let fork = RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            trust: Some("untrusted".to_string()),
            ..Default::default()
        };
        let own = RunTrigger {
            trust: Some("trusted".to_string()),
            ..fork.clone()
        };
        let rows = |pool: Pool, table: &'static str| async move {
            sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        // Refused: the engine itself would post where the workflow says.
        let calls = "name: calls\nnotify:\n  webhook: { url: \"http://127.0.0.1:9/\" }\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(calls).unwrap();
        let err = create_run_triggered(&pool, &dag, calls, &fork)
            .await
            .unwrap_err();
        let refused = err
            .downcast_ref::<crate::confine::ConfinedRefused>()
            .unwrap_or_else(|| panic!("{err}"));
        assert_eq!(
            (refused.class.as_str(), refused.what.as_str()),
            ("untrusted", "`notify`")
        );
        // So is a task that has the engine start another workflow, and a
        // record that names no class, which a CI service's is not trusted for.
        let starts =
            "name: starts\ntasks:\n  - name: call\n    type: workflow\n    workflow: deploy\n";
        let unsaid = RunTrigger {
            trust: None,
            ..fork.clone()
        };
        let err = create_run_triggered(
            &pool,
            &DagGraph::from_yaml(starts).unwrap(),
            starts,
            &unsaid,
        )
        .await
        .unwrap_err();
        assert!(
            err.downcast_ref::<crate::confine::ConfinedRefused>()
                .is_some(),
            "{err}"
        );
        for table in [
            "workflow_definitions",
            "workflow_runs",
            "task_runs",
            "run_triggers",
        ] {
            assert_eq!(rows(pool.clone(), table).await, 0, "{table}");
        }
        // A trusted run's workflow is its author's to write.
        create_run_triggered(&pool, &dag, calls, &own)
            .await
            .unwrap();

        // Made, without what it is not given.
        let shared = "name: shared\ntasks:\n  - name: a\n    command: [\"true\"]\n    cache: { key: one }\n    pool: builders\n    priority: 9\n";
        let dag = DagGraph::from_yaml(shared).unwrap();
        let row = |pool: Pool, run: String| async move {
            let (pool_named, priority, input) =
                sqlx::query_as::<_, (Option<String>, i64, Option<String>)>(
                    "SELECT pool, priority, input FROM task_runs WHERE run_id = ?",
                )
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap();
            let stored: crate::dag::TaskSpec =
                serde_json::from_str(&input.expect("the task's spec is stored")).unwrap();
            (pool_named, priority, stored)
        };
        let confined = create_run_triggered(&pool, &dag, shared, &fork)
            .await
            .unwrap();
        let (pool_named, priority, stored) = row(pool.clone(), confined.clone()).await;
        // Its own pool, not the one it named.
        assert_eq!((pool_named.as_deref(), priority), (Some("untrusted"), 0));
        assert!(stored.cache.is_none());
        assert_eq!(stored.pool.as_deref(), Some("untrusted"));
        assert_eq!(run_task_count(&pool, &confined).await.unwrap(), 1);
        assert_eq!(run_task_count(&pool, "no-such-run").await.unwrap(), 0);
        assert_eq!(stored.priority, 0);
        // What was written is kept as it was written.
        let kept: String = sqlx::query_scalar(
            "SELECT d.spec FROM workflow_definitions d JOIN workflow_runs r ON r.definition_id = d.id WHERE r.id = ?",
        )
        .bind(&confined)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(kept, shared);

        let trusted = create_run_triggered(&pool, &dag, shared, &own)
            .await
            .unwrap();
        let (pool_named, priority, stored) = row(pool.clone(), trusted).await;
        assert_eq!((pool_named.as_deref(), priority), (Some("builders"), 9));
        assert!(stored.cache.is_some());
        // And so has a run with no record, which is this installation's own.
        let plain = create_run(&pool, &dag, shared).await.unwrap();
        let (pool_named, _, stored) = row(pool.clone(), plain).await;
        assert_eq!(pool_named.as_deref(), Some("builders"));
        assert!(stored.cache.is_some());

        // A cap on active runs counts by a workflow's name and key, which
        // are its author's to write. A fork's run of a workflow named as one
        // of the installation's holds none of that workflow's slots, and is
        // capped among its own kind.
        let capped = "name: deploy\nmax_active_runs: 1\nconcurrency_key: main\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(capped).unwrap();
        let theirs = create_run_triggered(&pool, &dag, capped, &fork)
            .await
            .unwrap();
        let key = |pool: Pool, run: String| async move {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT concurrency_key FROM workflow_runs WHERE id = ?",
            )
            .bind(run)
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        assert_eq!(
            key(pool.clone(), theirs).await.as_deref(),
            Some("untrusted:main")
        );
        // The installation's own run is admitted while the fork's is active,
        let ours = create_run_triggered(&pool, &dag, capped, &own)
            .await
            .unwrap();
        assert_eq!(key(pool.clone(), ours).await.as_deref(), Some("main"));
        // and each is refused a second of its kind.
        for record in [&fork, &own] {
            let err = create_run_triggered(&pool, &dag, capped, record)
                .await
                .unwrap_err();
            assert!(
                err.downcast_ref::<crate::models::MaxActiveRunsReached>()
                    .is_some(),
                "{err}"
            );
        }

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    /// What started a run is written with it, and read back as it was
    /// written, by each way a run is made. A run made with none has none. A
    /// run that is refused leaves none behind. And the record goes when its
    /// run goes.
    #[tokio::test]
    async fn a_run_s_trigger_record_is_written_with_it_and_goes_with_it() {
        use crate::models::{trigger_kind, RunTrigger};
        let (pool, path) = temp_pool().await;
        let yaml =
            "name: recorded\nmax_active_runs: 2\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let records = |pool: Pool| async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM run_triggers")
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        // Every field, as a CI service's record has them.
        let whole = RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            actor: Some("octocat".to_string()),
            trust: Some("untrusted".to_string()),
            source: None,
            forge_host: Some("github.com".to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some("refs/pull/7/head".to_string()),
            sha: Some("a1".to_string()),
            base_sha: Some("b2".to_string()),
            pr: Some(7),
            event_id: Some("e-1".to_string()),
        };
        let run = create_run_triggered(&pool, &dag, yaml, &whole)
            .await
            .unwrap();
        assert_eq!(
            run_trigger(&pool, &run).await.unwrap().as_ref(),
            Some(&whole)
        );

        // With a source's cursor, which is the engine's own to write.
        let message = RunTrigger::engine(trigger_kind::SOURCE, "source:stream").of_event("42");
        let from_source = create_run_with_offset(&pool, &dag, yaml, "stream", "42", Some(&message))
            .await
            .unwrap();
        assert_eq!(
            run_trigger(&pool, &from_source).await.unwrap(),
            Some(message)
        );

        // A run made with none has none, and neither has a run that is not there.
        let plain = create_run(&pool, &dag, yaml).await.unwrap();
        assert_eq!(run_trigger(&pool, &plain).await.unwrap(), None);
        assert_eq!(run_trigger(&pool, "no-such-run").await.unwrap(), None);
        assert_eq!(records(pool.clone()).await, 2);

        // Refused: two of the installation's own are running, and two is the
        // cap. (The first run is a fork's, which is counted apart.) No run,
        // no record.
        let own = RunTrigger {
            trust: Some("trusted".to_string()),
            ..whole.clone()
        };
        let err = create_run_triggered(&pool, &dag, yaml, &own)
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<crate::models::MaxActiveRunsReached>()
                .is_some(),
            "{err}"
        );
        assert_eq!(
            records(pool.clone()).await,
            2,
            "a refused run leaves no record"
        );

        // The record goes with its run, and no other does.
        for sql in [
            "DELETE FROM task_dependencies WHERE dependent_id IN (SELECT id FROM task_runs WHERE run_id = ?)",
            "DELETE FROM task_runs WHERE run_id = ?",
            "DELETE FROM workflow_runs WHERE id = ?",
        ] {
            sqlx::query(sql).bind(&run).execute(&pool).await.unwrap();
        }
        assert_eq!(run_trigger(&pool, &run).await.unwrap(), None);
        assert_eq!(records(pool.clone()).await, 1);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run made from a dead letter is recorded as whoever asked for it, of
    /// that dead letter.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn a_redriven_run_is_recorded_as_its_redrive() {
        use crate::models::RunTrigger;
        let (pool, path) = temp_pool().await;
        let yaml = "name: parked\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let id = record_dead_letter(&pool, yaml, "transient", "redis", 3)
            .await
            .unwrap();
        let asked = RunTrigger::ops_api().from_source(format!("dead-letter:{id}"));
        let run = create_run_from_dead_letter(&pool, &dag, yaml, &id, Some(&asked))
            .await
            .unwrap()
            .expect("the row was there to claim");
        assert_eq!(run_trigger(&pool, &run).await.unwrap(), Some(asked));

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

    /// The fault verdict is fenced exactly like the transition it accompanies:
    /// a worker whose lease was reclaimed cannot relabel the newer attempt's
    /// failure. Without this a slow rank-0 straggler's "nccl-timeout" would
    /// overwrite the reclaiming attempt's real "gpu-ecc" — and the retry budget
    /// reads the class, so the mislabel is not cosmetic.
    #[tokio::test]
    async fn fault_attribution_is_fenced_and_reads_back() {
        use crate::fault::{Confidence, FaultClass};
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w1", 10).await.unwrap();
        let task_id = claimed[0].id.clone();
        let fence = claimed[0].version + 1;

        // Unclassified until something looks — distinct from "unknown".
        assert_eq!(get_task_fault(&pool, &task_id).await.unwrap(), None);

        assert!(
            record_task_fault(
                &pool, &task_id, "w1", fence,
                FaultClass::GpuEcc, Some("Xid 48: Double Bit ECC Error"), Confidence::High
            ).await.unwrap(),
            "the claim holder's fenced write lands"
        );
        let (class, detail, conf) = get_task_fault(&pool, &task_id).await.unwrap().unwrap();
        assert_eq!(class, "gpu-ecc");
        assert_eq!(conf.as_deref(), Some("high"));
        assert!(detail.unwrap().contains("Xid 48"));

        // A stale worker (wrong fence) and a foreign worker (wrong claim) both
        // no-op rather than overwriting the verdict.
        assert!(
            !record_task_fault(
                &pool, &task_id, "w1", fence + 1,
                FaultClass::NcclTimeout, None, Confidence::Low
            ).await.unwrap(),
            "a stale fence is rejected"
        );
        assert!(
            !record_task_fault(
                &pool, &task_id, "w2", fence,
                FaultClass::NcclTimeout, None, Confidence::Low
            ).await.unwrap(),
            "a worker that does not hold the claim is rejected"
        );
        assert_eq!(
            get_task_fault(&pool, &task_id).await.unwrap().unwrap().0,
            "gpu-ecc",
            "the verdict survived both stale writes"
        );

        // The roll-up the cost question is asked with.
        mark_task_failed(&pool, &task_id, "w1", fence, Some("boom".into())).await.unwrap();
        let counts = fault_class_counts(&pool, "1970-01-01T00:00:00+00:00", "2999-01-01T00:00:00+00:00")
            .await
            .unwrap();
        assert_eq!(counts, vec![("gpu-ecc".to_string(), 1, 1)]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A verdict must not outlive the failure it describes. The infra classes
    /// carry budgets above 1 precisely so the task gets another attempt — so
    /// "failed with gpu-ecc, retried, succeeded" is the *common* path for them,
    /// and a roll-up that still counts it as a gpu-ecc failure is most wrong
    /// exactly where it is most load-bearing.
    #[tokio::test]
    async fn a_retried_then_succeeded_task_is_not_counted_as_a_failure() {
        use crate::fault::{Confidence, FaultClass};
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();

        // Attempt 1: fails, attributed to a device fault.
        advance_ready_tasks(&pool).await.unwrap();
        let c1 = claim_ready(&pool, "w1", 10).await.unwrap();
        let task_id = c1[0].id.clone();
        let f1 = c1[0].version + 1;
        record_task_fault(
            &pool, &task_id, "w1", f1,
            FaultClass::GpuEcc, Some("Xid 48"), Confidence::High,
        )
        .await
        .unwrap();
        let retry_at = chrono::Utc::now().to_rfc3339();
        assert!(retry_task(&pool, &task_id, "w1", f1, Some("boom".into()), retry_at, AttemptEnd::Failed).await.unwrap());

        // The verdict is gone the moment the row is queued to run again.
        assert_eq!(
            get_task_fault(&pool, &task_id).await.unwrap(),
            None,
            "a row about to run again is not a failed row"
        );

        // Attempt 2: succeeds, on another node, as the infra budget intends.
        let c2 = claim_ready(&pool, "w2", 10).await.unwrap();
        assert_eq!(c2.len(), 1, "the task is claimable again");
        let f2 = c2[0].version + 1;
        assert!(
            mark_task_succeeded(&pool, &task_id, "w2", f2, Some("ok".into())).await.unwrap()
        );

        let counts =
            fault_class_counts(&pool, "1970-01-01T00:00:00+00:00", "2999-01-01T00:00:00+00:00")
                .await
                .unwrap();
        assert!(
            counts.is_empty(),
            "a task that recovered is not an infrastructure failure: {counts:?}"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Every path that resets a task for another attempt must drop the previous
    /// attempt's verdict — not just the ordinary retry. `rerun_from_failed` and
    /// `clear_task_with_downstream` were missed when `retry_task` was fixed, so
    /// a rerun left a stale class on a row that could then fail for a wholly
    /// different reason, and the roll-up reported the old one.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn every_reset_path_drops_the_previous_attempts_verdict() {
        use crate::fault::{Confidence, FaultClass};
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        // Attribute a failure, then rerun the run from its failure frontier.
        advance_ready_tasks(&pool).await.unwrap();
        let c = claim_ready(&pool, "w1", 10).await.unwrap();
        let task_id = c[0].id.clone();
        let fence = c[0].version + 1;
        record_task_fault(
            &pool, &task_id, "w1", fence,
            FaultClass::GpuEcc, Some("Xid 48"), Confidence::High,
        ).await.unwrap();
        mark_task_failed(&pool, &task_id, "w1", fence, Some("boom".into())).await.unwrap();
        assert!(get_task_fault(&pool, &task_id).await.unwrap().is_some(), "recorded");

        // `rerun_from_failed` re-arms the *run* as its atomic gate, so the run
        // has to be finalized first — without this it returns `None`, resets
        // nothing, and the assertion below would be testing a no-op.
        reap_completed_runs(&pool).await.unwrap();
        let reset = rerun_from_failed(&pool, &run_id).await.unwrap();
        assert_eq!(reset, Some(1), "the failed task was actually reset");
        assert_eq!(
            get_task_fault(&pool, &task_id).await.unwrap(),
            None,
            "rerun_from_failed must not carry the old verdict forward"
        );

        // And the same for an operator's Clear task.
        advance_ready_tasks(&pool).await.unwrap();
        let c = claim_ready(&pool, "w2", 10).await.unwrap();
        let fence = c[0].version + 1;
        record_task_fault(
            &pool, &task_id, "w2", fence,
            FaultClass::NanLoss, Some("Found NaN in loss"), Confidence::High,
        ).await.unwrap();
        mark_task_failed(&pool, &task_id, "w2", fence, Some("boom".into())).await.unwrap();
        assert!(get_task_fault(&pool, &task_id).await.unwrap().is_some());

        let cleared = clear_task_with_downstream(&pool, &run_id, &task_id).await.unwrap();
        assert_eq!(cleared, Some(1), "the task was actually cleared");
        assert_eq!(
            get_task_fault(&pool, &task_id).await.unwrap(),
            None,
            "clear_task_with_downstream must not carry the old verdict forward"
        );

        // Nothing stale is left for the roll-up to count.
        let counts =
            fault_class_counts(&pool, "1970-01-01T00:00:00+00:00", "2999-01-01T00:00:00+00:00")
                .await
                .unwrap();
        assert!(counts.is_empty(), "no stale attribution survives a reset: {counts:?}");

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

        register_outbox_sink(&pool, &crate::db::WEBHOOK_SINK).await.unwrap();
        enqueue_outbox_event(&pool, &run_id, "backfill.catchup", "{\"k\":1}").await.unwrap();
        let batch = claim_outbox_batch(&pool, &crate::db::WEBHOOK_SINK, 10, 30).await.unwrap();
        assert_eq!(batch.len(), 1, "the enqueued event is claimable");
        assert_eq!(batch[0].event_type, "backfill.catchup");
        assert_eq!(batch[0].run_id, run_id);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// (LOW_LATENCY B-3) The chunked multi-row `create_run` writes exactly the
    /// rows and dependency edges the per-row version did — including across a
    /// chunk boundary (>1,000 task rows in one insert set) and fan-in
    /// `remaining_deps` seeding.
    #[tokio::test]
    async fn batched_create_run_preserves_rows_edges_and_dep_counts() {
        let (pool, path) = temp_pool().await;
        // 1 root + 1,100 dependents (crosses the 1,000-row chunk) + a joiner
        // depending on three of them (fan-in dep_count = 3).
        let mut yaml = String::from("name: big\ntasks:\n  - { name: root, command: [\"true\"] }\n");
        for i in 0..1100 {
            yaml.push_str(&format!(
                "  - {{ name: w{i}, command: [\"true\"], depends_on: [root] }}\n"
            ));
        }
        yaml.push_str("  - { name: join, command: [\"true\"], depends_on: [w0, w1, w2] }\n");
        let dag = DagGraph::from_yaml(&yaml).unwrap();
        let run_id = create_run(&pool, &dag, &yaml).await.unwrap();

        let tasks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = ?")
            .bind(&run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(tasks, 1102, "root + 1100 dependents + join");
        let edges: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_dependencies d
             JOIN task_runs t ON t.id = d.dependent_id WHERE t.run_id = ?",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(edges, 1103, "1100 root edges + 3 join edges");
        let join_deps: i64 = sqlx::query_scalar(
            "SELECT remaining_deps FROM task_runs WHERE run_id = ? AND name = 'join'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(join_deps, 3, "fan-in seeds remaining_deps to in-degree");
        let root_deps: i64 = sqlx::query_scalar(
            "SELECT remaining_deps FROM task_runs WHERE run_id = ? AND name = 'root'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(root_deps, 0);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// (LOW_LATENCY B-2) Advance-on-mark: a default-shaped dependent whose last
    /// dependency just succeeded is `ready` — with a fresh `scheduled_at`
    /// readiness stamp — straight out of `mark_task_succeeded`, before any
    /// `advance_ready_tasks` sweep runs. A dependent with another, failed
    /// dependency is NOT promoted (the mark re-checks every dependency).
    #[tokio::test]
    async fn mark_succeeded_promotes_default_shaped_dependents_in_tx() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: prom\ntasks:\n  \
            - { name: a, command: [\"true\"] }\n  \
            - { name: x, command: [\"false\"] }\n  \
            - { name: b, command: [\"true\"], depends_on: [a] }\n  \
            - { name: mixed, command: [\"true\"], depends_on: [a, x], trigger_rule: all_done }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let a = claimed.iter().find(|t| t.name == "a").unwrap();
        let x = claimed.iter().find(|t| t.name == "x").unwrap();
        assert!(mark_task_failed(&pool, &x.id, "w", x.version + 1, Some("boom".into())).await.unwrap());
        assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, Some("ok".into())).await.unwrap());

        // No advance sweep has run since the marks: b must already be ready.
        let (b_status, b_sched): (String, Option<String>) = sqlx::query_as(
            "SELECT status, scheduled_at FROM task_runs WHERE run_id = ? AND name = 'b'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(b_status, "ready", "promoted inside the mark transaction");
        assert!(b_sched.is_some(), "readiness stamp present");
        // `mixed` has a non-default rule (all_done) — never touched by the
        // promotion; the next advance sweep readies it via the slow path.
        let mixed_status: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'mixed'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(mixed_status, "pending", "non-default shapes wait for the sweep");
        advance_ready_tasks(&pool).await.unwrap();
        let mixed_status: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'mixed'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(mixed_status, "ready", "all_done readies once every dep is terminal");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// (LOW_LATENCY B-1) The set-based fast path and the per-task slow path
    /// agree: a command merely *containing* the literal string "when" is a
    /// conservative LIKE-filter false positive that routes to the slow path
    /// and still advances correctly, while a real runtime `when` evaluates on
    /// the slow path (true → ready, false → skipped, and the skip cascades).
    #[tokio::test]
    async fn when_filter_routes_conservatively_and_evaluates() {
        use crate::models::TaskStatus;
        let (pool, path) = temp_pool().await;
        let yaml = "name: whens\ntasks:\n  \
            - { name: a, command: [\"true\"] }\n  \
            - { name: fake, command: [\"echo\", \"when\"], depends_on: [a] }\n  \
            - { name: gated_yes, command: [\"true\"], depends_on: [a], when: \"{{ tasks.a.output }} == go\" }\n  \
            - { name: gated_no, command: [\"true\"], depends_on: [a], when: \"{{ tasks.a.output }} == stop\" }\n  \
            - { name: downstream, command: [\"true\"], depends_on: [gated_no] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let a = claim_ready(&pool, "w", 10).await.unwrap().into_iter().find(|t| t.name == "a").unwrap();
        assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, Some("go".into())).await.unwrap());

        for _ in 0..4 {
            advance_ready_tasks(&pool).await.unwrap();
        }
        let status = |name: &str| {
            let pool = pool.clone();
            let run_id = run_id.clone();
            let name = name.to_string();
            async move {
                let s: String = sqlx::query_scalar(
                    "SELECT status FROM task_runs WHERE run_id = ? AND name = ?",
                )
                .bind(&run_id)
                .bind(&name)
                .fetch_one(&pool)
                .await
                .unwrap();
                s.parse::<TaskStatus>().unwrap()
            }
        };
        assert_eq!(status("fake").await, TaskStatus::Ready, "LIKE false positive still advances");
        assert_eq!(status("gated_yes").await, TaskStatus::Ready, "when true → ready");
        assert_eq!(status("gated_no").await, TaskStatus::Skipped, "when false → skipped");
        assert_eq!(status("downstream").await, TaskStatus::Skipped, "skip cascades");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── Clock discipline + constrained-host floor (edge profile) ────────────

    /// The three clock columns of one run, as stored.
    async fn clock_stamp(pool: &Pool, run_id: &str) -> (Option<String>, Option<i64>, Option<String>) {
        sqlx::query_as(
            "SELECT clock_confidence, clock_offset_ms, clock_source FROM workflow_runs WHERE id = ?",
        )
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// `lease_expires_at` of one task row, parsed — `None` means no lease held.
    async fn lease_of(pool: &Pool, task_id: &str) -> chrono::DateTime<chrono::Utc> {
        let s: Option<String> =
            sqlx::query_scalar("SELECT lease_expires_at FROM task_runs WHERE id = ?")
                .bind(task_id)
                .fetch_one(pool)
                .await
                .unwrap();
        chrono::DateTime::parse_from_rfc3339(&s.expect("a claimed task holds a lease"))
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    /// Every run is stamped with the process's clock confidence at creation:
    /// `unknown` as a stored value when nothing has assessed the clock, and
    /// whatever the detector last published otherwise — offset and source
    /// included. Read back through raw SQL so the stamp is verified in the
    /// lean build too (`get_run` is `ops`-gated and covered separately).
    #[tokio::test]
    async fn create_run_stamps_the_published_clock_confidence() {
        let _guard = crate::clock::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (pool, path) = temp_pool().await;
        let yaml = "name: clk\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();

        crate::clock::publish(crate::clock::ClockStatus::unknown());
        let r = create_run(&pool, &dag, yaml).await.unwrap();
        assert_eq!(
            clock_stamp(&pool, &r).await,
            (Some("unknown".into()), None, None),
            "unknown is a value, not NULL"
        );

        crate::clock::publish(crate::clock::ClockStatus::drifted(-4_200, "step"));
        let r = create_run(&pool, &dag, yaml).await.unwrap();
        assert_eq!(
            clock_stamp(&pool, &r).await,
            (Some("drifted".into()), Some(-4_200), Some("step".into()))
        );

        crate::clock::publish(crate::clock::ClockStatus::synced("sync-file"));
        let r = create_run(&pool, &dag, yaml).await.unwrap();
        assert_eq!(
            clock_stamp(&pool, &r).await,
            (Some("synced".into()), None, Some("sync-file".into()))
        );

        crate::clock::publish(crate::clock::ClockStatus::unknown());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `get_run` carries the three clock fields (the engine's `GET /runs/{id}`
    /// reads them from here), and a row written before migration 041 — NULL
    /// in every column — reads as `None`, never as a guessed `unknown`.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn get_run_round_trips_clock_fields() {
        let _guard = crate::clock::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (pool, path) = temp_pool().await;
        let yaml = "name: clk2\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        crate::clock::publish(crate::clock::ClockStatus::drifted(90_000, "behind-datastore"));
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        crate::clock::publish(crate::clock::ClockStatus::unknown());

        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!(run.clock_confidence.as_deref(), Some("drifted"));
        assert_eq!(run.clock_offset_ms, Some(90_000));
        assert_eq!(run.clock_source.as_deref(), Some("behind-datastore"));

        sqlx::query(
            "UPDATE workflow_runs SET clock_confidence = NULL, clock_offset_ms = NULL, clock_source = NULL WHERE id = ?",
        )
        .bind(&run_id)
        .execute(&pool)
        .await
        .unwrap();
        let run = get_run(&pool, &run_id).await.unwrap().unwrap();
        assert_eq!((run.clock_confidence, run.clock_offset_ms, run.clock_source), (None, None, None));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A caught clock step re-stamps only the runs still in flight: a finished
    /// run's record is closed and keeps the confidence it was written under.
    #[tokio::test]
    async fn mark_runs_clock_drifted_touches_running_runs_only() {
        let _guard = crate::clock::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let (pool, path) = temp_pool().await;
        let yaml = "name: drift\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        crate::clock::publish(crate::clock::ClockStatus::synced("sync-file"));
        let running = create_run(&pool, &dag, yaml).await.unwrap();
        let finished = create_run(&pool, &dag, yaml).await.unwrap();
        crate::clock::publish(crate::clock::ClockStatus::unknown());
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded', finished_at = ? WHERE id = ?")
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(&finished)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(mark_runs_clock_drifted(&pool, -1_500, "step").await.unwrap(), 1);
        assert_eq!(
            clock_stamp(&pool, &running).await,
            (Some("drifted".into()), Some(-1_500), Some("step".into()))
        );
        assert_eq!(
            clock_stamp(&pool, &finished).await,
            (Some("synced".into()), None, Some("sync-file".into())),
            "a closed record is never rewritten"
        );

        // Nothing running → nothing touched, and no error.
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded'").execute(&pool).await.unwrap();
        assert_eq!(mark_runs_clock_drifted(&pool, 7, "step").await.unwrap(), 0);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The boot plausibility probe reports the newest run's `created_at`, and
    /// `None` on an empty store (a fresh unit has nothing to be behind).
    #[tokio::test]
    async fn latest_run_created_at_reports_the_newest_run() {
        let (pool, path) = temp_pool().await;
        assert_eq!(latest_run_created_at(&pool).await.unwrap(), None);

        let yaml = "name: newest\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let first = create_run(&pool, &dag, yaml).await.unwrap();
        let second = create_run(&pool, &dag, yaml).await.unwrap();
        // Push the second run unmistakably into the future (two creates can
        // share a millisecond) so the probe has a definite answer.
        let future = (chrono::Utc::now() + chrono::TimeDelta::seconds(3_600)).to_rfc3339();
        sqlx::query("UPDATE workflow_runs SET created_at = ? WHERE id = ?")
            .bind(&future)
            .bind(&second)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(latest_run_created_at(&pool).await.unwrap().as_deref(), Some(future.as_str()));
        let first_at: String = sqlx::query_scalar("SELECT created_at FROM workflow_runs WHERE id = ?")
            .bind(&first)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(future > first_at, "text order is time order for to_rfc3339() stamps");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The free-disk floor (`DAGRON_MIN_FREE_BYTES`), exercised through the
    /// injecting helper: `0` is off, an unreachable floor refuses with the
    /// typed error carrying both numbers and leaves nothing behind (no
    /// definition, no run, no moved source cursor), and a one-byte floor
    /// admits. The env knob itself is a `OnceLock` read, which is why the
    /// test injects instead of setting it.
    #[tokio::test]
    async fn disk_floor_refuses_below_and_admits_above() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: floor\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let counts = |pool: Pool| async move {
            let defs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_definitions")
                .fetch_one(&pool)
                .await
                .unwrap();
            let runs: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs").fetch_one(&pool).await.unwrap();
            (defs, runs)
        };

        assert!(
            create_run_inner_with_floor(&pool, &dag, yaml, None, None, None, 0)
                .await
                .is_ok(),
            "0 = off"
        );

        let err =
            create_run_inner_with_floor(&pool, &dag, yaml, None, None, None, u64::MAX).await.unwrap_err();
        let low = err
            .downcast_ref::<crate::models::DatastoreLowOnDisk>()
            .expect("a typed refusal, not a bare string");
        assert_eq!(low.floor, u64::MAX);
        assert!(low.free < low.floor);
        assert!(err.to_string().contains("DAGRON_MIN_FREE_BYTES"), "{err}");

        assert!(
            create_run_inner_with_floor(&pool, &dag, yaml, None, None, None, 1).await.is_ok(),
            "one byte of headroom admits"
        );
        assert_eq!(counts(pool.clone()).await, (2, 2), "the refused run left no rows behind");

        // The exactly-once path is refused the same way, before its cursor moves.
        let err = create_run_inner_with_floor(&pool, &dag, yaml, Some(("src", "9")), None, None, u64::MAX)
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<crate::models::DatastoreLowOnDisk>().is_some());
        assert_eq!(source_offset(&pool, "src").await.unwrap(), None, "cursor untouched");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Single-read lease derivation: the lease a claim hands out is exactly
    /// one `LEASE_SECS` window past the instant that gated `scheduled_at`, on
    /// both claim paths — never a second wall-clock read taken later.
    #[tokio::test]
    async fn claim_lease_is_one_window_past_the_gate_instant() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: lease\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: g\n    command: [\"true\"]\n    gang: { size: 2 }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let window = chrono::TimeDelta::seconds(crate::db::lease_secs());

        let before = chrono::Utc::now();
        let ordinary = claim_ready_classes_nongang(&pool, "w", 10, &[], &Default::default()).await.unwrap();
        let after = chrono::Utc::now();
        assert_eq!(ordinary.len(), 1, "the non-gang task");
        let lease = lease_of(&pool, &ordinary[0].id).await;
        assert!(lease >= before + window && lease <= after + window, "{lease} vs [{before}, {after}] + window");

        let before = chrono::Utc::now();
        let gang = claim_ready_gang(&pool, "w", 2, &[], &Default::default()).await.unwrap();
        let after = chrono::Utc::now();
        assert_eq!(gang.len(), 2, "the whole gang");
        for member in &gang {
            let lease = lease_of(&pool, &member.id).await;
            assert!(lease >= before + window && lease <= after + window, "{lease} vs [{before}, {after}] + window");
        }

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The external-job park: submit → park → poll → resolve, and the crash
    /// property the whole shape exists for.
    #[tokio::test]
    async fn external_park_poll_and_resolve() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: submit, command: [\"true\"], defer: { kind: spark-k8s } }\n  - name: down\n    command: [\"true\"]\n    depends_on: [submit]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let sub = claimed.iter().find(|t| t.name == "submit").expect("submit claimed");
        let fence = sub.version + 1;
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(
            &pool, &sub.id, fence, "spark-k8s", "app-7", Some("https://spark.local"), &now, None
        )
        .await
        .unwrap());

        // Parked shape: running, NULL lease, and none of the other four park
        // reasons set — exactly one park reason per row.
        let (status, lease): (String, Option<String>) =
            sqlx::query_as("SELECT status, lease_expires_at FROM task_runs WHERE id = ?")
                .bind(&sub.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "running");
        assert!(lease.is_none(), "the NULL lease is the crash-recovery argument");
        let (other_parks,): (i64,) = sqlx::query_as(
            "SELECT (wake_at IS NOT NULL) + (wait_url IS NOT NULL)
                  + (wait_dataset IS NOT NULL) + (sub_run_id IS NOT NULL)
             FROM task_runs WHERE id = ?",
        )
        .bind(&sub.id).fetch_one(&pool).await.unwrap();
        assert_eq!(other_parks, 0, "exactly one park reason per row");

        // THE crash property: every scheduler dies, the sweep that reclaims
        // expired leases runs, and the parked row is untouched — so nothing
        // resubmits, because nothing re-ran.
        assert_eq!(recover_expired_leases(&pool).await.unwrap(), 0);
        let (still,): (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE id = ?")
            .bind(&sub.id).fetch_one(&pool).await.unwrap();
        assert_eq!(still, "running", "lease recovery cannot reclaim a parked row");
        assert!(
            reconcile_waits(&pool).await.unwrap().done.is_empty(),
            "not a time sensor"
        );
        assert!(
            claim_due_url_waits(&pool, 32, 30).await.unwrap().is_empty(),
            "not an http sensor"
        );

        // Due for a poll, carrying everything the poller needs off the row alone.
        let due = claim_due_external_polls(&pool, 32, 30).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, sub.id);
        assert_eq!(due[0].run_id, run);
        assert_eq!(due[0].external_kind, "spark-k8s");
        assert_eq!(due[0].external_handle, "app-7");
        assert_eq!(due[0].external_endpoint.as_deref(), Some("https://spark.local"));
        assert_eq!(due[0].external_epoch, 0);
        assert!(
            due[0].input.as_deref().is_some_and(|i| i.contains("spark-k8s")),
            "the persisted spec rides along, so the poller reads the defer block that actually ran"
        );

        // The claim itself already pushed next_poll_at forward, so a second
        // sweep — on this scheduler or any other — sees nothing.
        assert!(
            claim_due_external_polls(&pool, 32, 30).await.unwrap().is_empty(),
            "claiming advances next_poll_at, so the row is not handed out twice"
        );

        // "Still running" pushes the next poll out further.
        let future = (chrono::Utc::now() + chrono::TimeDelta::hours(1)).to_rfc3339();
        assert!(repark_external(&pool, &sub.id, "app-7", &future).await.unwrap());
        assert!(claim_due_external_polls(&pool, 32, 30).await.unwrap().is_empty(), "re-parked → not due");
        assert!(
            !repark_external(&pool, &sub.id, "stale-handle", &future).await.unwrap(),
            "a stale sweep matching another handle is a no-op"
        );

        // Success resolves it and advances the dependent.
        assert!(resolve_external(&pool, &sub.id, "app-7", Some("COMPLETED")).await.unwrap());
        assert!(
            !resolve_external(&pool, &sub.id, "app-7", Some("COMPLETED")).await.unwrap(),
            "second resolve is a no-op — clearing the handle takes the row out of the parked set"
        );
        advance_ready_tasks(&pool).await.unwrap();
        let (dstatus,): (String,) =
            sqlx::query_as("SELECT status FROM task_runs WHERE name='down' AND run_id = ?")
                .bind(&run).fetch_one(&pool).await.unwrap();
        assert_eq!(dstatus, "ready", "downstream advanced on the remote job's success");
        let (sstatus, out, epoch): (String, Option<String>, i64) =
            sqlx::query_as("SELECT status, output, external_epoch FROM task_runs WHERE id = ?")
                .bind(&sub.id).fetch_one(&pool).await.unwrap();
        assert_eq!(sstatus, "succeeded");
        assert_eq!(out.as_deref(), Some("COMPLETED"));
        assert_eq!(epoch, 0, "a success names no next submission, so the epoch stands");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// A remote failure fails the task, advances dependents like any failure,
    /// and bumps the epoch so a retry submits a NEW job instead of adopting the
    /// one that just failed.
    #[tokio::test]
    async fn external_failure_bumps_the_epoch_so_a_retry_cannot_adopt_a_corpse() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: submit, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let sub = &claimed[0];
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(&pool, &sub.id, sub.version + 1, "http", "run-1", None, &now, None)
            .await.unwrap());

        assert!(fail_external(&pool, &sub.id, "run-1", "remote job FAILED: OOM").await.unwrap());
        assert!(
            !fail_external(&pool, &sub.id, "run-1", "again").await.unwrap(),
            "second failure is a no-op"
        );
        let (status, out, handle, epoch): (String, Option<String>, Option<String>, i64) =
            sqlx::query_as(
                "SELECT status, output, external_handle, external_epoch FROM task_runs WHERE id = ?",
            )
            .bind(&sub.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "failed");
        assert_eq!(out.as_deref(), Some("remote job FAILED: OOM"), "the remote reason is the task's");
        assert!(handle.is_none(), "handle cleared — the row leaves the parked set");
        assert_eq!(epoch, 1, "a fresh name for the next submit");

        // …and a rerun bumps it again rather than reusing the failed job's name.
        #[cfg(feature = "ops")]
        {
            // `fail_external` fails the task, not the run — finalize it the way
            // the reconcile loop would so `rerun_from_failed` has something to
            // re-arm.
            sqlx::query("UPDATE workflow_runs SET status = 'failed' WHERE id = ?")
                .bind(&run).execute(&pool).await.unwrap();
            rerun_from_failed(&pool, &run).await.unwrap().expect("rerun armed");
            let (rstatus, repoch, rhandle): (String, i64, Option<String>) = sqlx::query_as(
                "SELECT status, external_epoch, external_handle FROM task_runs WHERE id = ?",
            )
            .bind(&sub.id).fetch_one(&pool).await.unwrap();
            assert_eq!(rstatus, "pending");
            assert_eq!(repoch, 2, "every re-arm of an already-submitted row takes a fresh epoch");
            assert!(rhandle.is_none());
        }
        #[cfg(not(feature = "ops"))]
        let _ = &run;

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// A parked external job holds its pool slot, unlike the four free park
    /// shapes — it is burning someone's cluster the whole time it waits, and
    /// `pool:` is the only knob that can bound that.
    #[tokio::test]
    async fn a_parked_external_job_still_occupies_its_pool_slot() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], pool: spark, defer: { kind: http } }\n  - { name: b, command: [\"true\"], pool: spark }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let caps = std::collections::BTreeMap::from([("spark".to_string(), 1i64)]);
        let first = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        assert_eq!(first.len(), 1, "capacity 1 admits one");
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(&pool, &first[0].id, first[0].version + 1, "http", "j-1", None, &now, None)
            .await.unwrap());

        let second = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        assert!(
            second.is_empty(),
            "the parked remote job still holds the slot — otherwise `pool:` would cap \
             submissions (seconds) rather than concurrent remote jobs (hours)"
        );

        // Once it resolves, the slot frees.
        assert!(resolve_external(&pool, &first[0].id, "j-1", None).await.unwrap());
        let third = claim_ready_classes(&pool, "w", 10, &[], &caps).await.unwrap();
        assert_eq!(third.len(), 1, "slot freed on resolve");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// Two schedulers sweeping the same tick must not both call the vendor for
    /// one job. The claim is what prevents it — and a row parked with no
    /// interval at all (`next_poll_at IS NULL`) is the case an `=` guard would
    /// silently make unclaimable forever, so it is the one tested.
    #[tokio::test]
    async fn a_due_external_job_is_claimed_by_exactly_one_scheduler() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http } }\n  - { name: b, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // Park both with next_poll_at NULL — immediately due, and the null-safe
        // guard's test case.
        for t in claim_ready(&pool, "w", 10).await.unwrap() {
            assert!(park_external(
                &pool, &t.id, t.version + 1, "http", &format!("job-{}", t.name), None, "", None
            )
            .await
            .unwrap());
            sqlx::query("UPDATE task_runs SET next_poll_at = NULL WHERE id = ?")
                .bind(&t.id).execute(&pool).await.unwrap();
        }

        let first = claim_due_external_polls(&pool, 32, 30).await.unwrap();
        assert_eq!(first.len(), 2, "both rows are due and this sweep wins both");

        let second = claim_due_external_polls(&pool, 32, 30).await.unwrap();
        assert!(
            second.is_empty(),
            "a second scheduler sweeping the same tick gets nothing — otherwise every \
             scheduler calls the vendor for every due job"
        );

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// Cancelling a run leaves the remote job running, and the row says so: the
    /// handle survives the cancel, which is the whole debt mechanism.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_cancelled_run_leaves_its_parked_rows_owing_teardown() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let t = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(&pool, &t.id, t.version + 1, "http", "job-1", None, &now, None)
            .await.unwrap());

        // Nothing owes teardown while the job is parked and being polled.
        assert!(claim_due_external_cancels(&pool, 16, 30).await.unwrap().is_empty());

        assert!(cancel_run(&pool, &run).await.unwrap().is_some());

        let owing = claim_due_external_cancels(&pool, 16, 30).await.unwrap();
        assert_eq!(owing.len(), 1, "the cancelled row still holds its handle — that IS the debt");
        assert_eq!(owing[0].external_handle, "job-1");
        assert_eq!(owing[0].external_cancel_attempts, 0);
        assert!(owing[0].finished_at.is_some(), "the give-up clock needs a start");

        // Claimed, so a second scheduler sweeping the same tick gets nothing.
        assert!(
            claim_due_external_cancels(&pool, 16, 30).await.unwrap().is_empty(),
            "the teardown claim is exclusive — otherwise every replica calls the vendor"
        );

        // A failed attempt consumes budget; a no-poller release does not.
        let later = chrono::Utc::now().to_rfc3339();
        assert_eq!(
            record_external_cancel_failure(&pool, &t.id, "job-1", &later).await.unwrap(),
            1
        );
        assert!(release_external_cancel_claim(&pool, &t.id, "job-1", &later).await.unwrap());
        let again = claim_due_external_cancels(&pool, 16, 30).await.unwrap();
        assert_eq!(
            again[0].external_cancel_attempts, 1,
            "releasing the claim must not spend the budget of a replica that could do the work"
        );

        // Settling clears the debt for good.
        assert!(clear_external_handle(&pool, &t.id, "job-1").await.unwrap());
        assert!(claim_due_external_cancels(&pool, 16, 30).await.unwrap().is_empty());

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// A job that finished on its own owes nothing — resolve/fail already NULLed
    /// the handle, so the teardown sweep must not issue a redundant vendor
    /// cancel for every completed job.
    #[tokio::test]
    async fn a_job_that_finished_by_itself_owes_no_teardown() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http } }\n  - { name: b, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let now = chrono::Utc::now().to_rfc3339();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        for t in &claimed {
            assert!(park_external(
                &pool, &t.id, t.version + 1, "http", &format!("j-{}", t.name), None, &now, None
            ).await.unwrap());
        }
        let a = claimed.iter().find(|t| t.name == "a").unwrap();
        let b = claimed.iter().find(|t| t.name == "b").unwrap();
        assert!(resolve_external(&pool, &a.id, "j-a", Some("DONE")).await.unwrap());
        assert!(fail_external(&pool, &b.id, "j-b", "OOM").await.unwrap());

        assert!(
            claim_due_external_cancels(&pool, 16, 30).await.unwrap().is_empty(),
            "a terminal remote job owes nothing; sweeping it would cancel every completed job"
        );

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// `defer.max_wait_secs` elapsing on a task with a `defer.http.cancel` block
    /// fails the task but keeps the handle, so the row owes a teardown and the
    /// sweep is handed the spec that says how to send it. The epoch still
    /// bumps, so a retry gets a fresh remote name rather than adopting the job
    /// that is about to be stopped.
    #[tokio::test]
    async fn max_wait_with_a_cancel_block_keeps_the_handle_and_owes_a_teardown() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http, http: { url: \"https://h/j/{{ handle }}\", succeed_when: \"s == DONE\", cancel: { url: \"https://h/j/{{ handle }}\" } } } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let t = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(&pool, &t.id, t.version + 1, "http", "job-1", None, &now, None)
            .await.unwrap());

        assert!(fail_external_keep_handle(&pool, &t.id, "job-1", "max_wait elapsed").await.unwrap());
        assert!(
            !fail_external_keep_handle(&pool, &t.id, "job-1", "again").await.unwrap(),
            "guarded on status = running, so a second sweep cannot re-fail it"
        );

        let owing = claim_due_external_cancels(&pool, 16, 30).await.unwrap();
        assert_eq!(owing.len(), 1, "the kept handle IS the debt");
        assert_eq!(owing[0].external_handle, "job-1");
        assert!(
            owing[0].input.as_deref().is_some_and(|j| j.contains("\"cancel\"")),
            "the sweep needs the spec to know how to cancel: {:?}",
            owing[0].input
        );
        let epoch: i64 = sqlx::query_scalar("SELECT external_epoch FROM task_runs WHERE id = ?")
            .bind(&t.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(epoch, 1, "a retry must not adopt the job being torn down");

        // Torn down: the debt settles like any other.
        assert!(clear_external_handle(&pool, &t.id, "job-1").await.unwrap());
        assert!(claim_due_external_cancels(&pool, 16, 30).await.unwrap().is_empty());

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// GC must not collect a run still owing teardown — deleting it drops the
    /// only record of a job running on someone's cluster, turning the visible
    /// leak into a silent one.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn gc_refuses_a_run_that_still_owes_a_teardown() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let t = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(&pool, &t.id, t.version + 1, "http", "job-1", None, &now, None)
            .await.unwrap());
        assert!(cancel_run(&pool, &run).await.unwrap().is_some());

        // Well past any cutoff, but the debt is outstanding.
        let future = (chrono::Utc::now() + chrono::TimeDelta::days(365)).to_rfc3339();
        assert_eq!(
            gc_old_runs(&pool, &future).await.unwrap(),
            0,
            "a run owing teardown is not collectable"
        );
        let (still,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM workflow_runs WHERE id = ?")
            .bind(&run).fetch_one(&pool).await.unwrap();
        assert_eq!(still, 1);

        // Settle the debt and it collects normally.
        assert!(clear_external_handle(&pool, &t.id, "job-1").await.unwrap());
        assert_eq!(gc_old_runs(&pool, &future).await.unwrap(), 1, "settled → collectable");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// A whole rerun must not drop a handle the teardown sweep has still to
    /// reach. A cancel ends a run whose only task is a parked deferred job at
    /// once, with the job still running; a rerun then would reset the handle
    /// with the rest of the task's result, and the job would run on unswept.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_whole_rerun_waits_for_an_owed_teardown() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let t = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(
            &pool,
            &t.id,
            t.version + 1,
            "http",
            "job-1",
            None,
            &now,
            None
        )
        .await
        .unwrap());
        assert!(cancel_run(&pool, &run).await.unwrap().is_some());

        let err = rerun_all(&pool, &run).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<crate::models::TeardownOwed>()
                .expect("refused for the debt")
                .tasks,
            1
        );
        let (status, whole): (String, i64) =
            sqlx::query_as("SELECT status, whole_reruns FROM workflow_runs WHERE id = ?")
                .bind(&run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            (status.as_str(), whole),
            ("cancelled", 0),
            "nothing was written"
        );
        let owed = claim_due_external_cancels(&pool, 16, 30).await.unwrap();
        assert_eq!(owed.len(), 1);
        assert_eq!(
            owed[0].external_handle, "job-1",
            "the sweep still sees the job"
        );

        // Settle the debt and the run reruns.
        assert!(clear_external_handle(&pool, &t.id, "job-1").await.unwrap());
        assert_eq!(rerun_all(&pool, &run).await.unwrap(), Some(1));

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// A worker whose task was cancelled mid-flight must not resurrect the row
    /// — which would take it out of the teardown set and lose the handle.
    ///
    /// `cancel_run` does not bump `version`, so the guard doing the work here is
    /// `claimed_by`, which the cancel NULLs. Asserted rather than reasoned
    /// about, because the whole teardown design rests on a cancelled row staying
    /// cancelled.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_late_worker_result_cannot_resurrect_a_cancelled_deferred_row() {
        let (pool, path) = temp_pool().await;

        let yaml = "name: p\ntasks:\n  - { name: a, command: [\"true\"], defer: { kind: http } }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let t = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let fence = t.version + 1;

        assert!(cancel_run(&pool, &run).await.unwrap().is_some());

        // The worker finishes and reports success with the fence it still holds.
        assert!(
            !mark_task_succeeded(&pool, &t.id, "w", fence, Some("done".into())).await.unwrap(),
            "it was asked to stop: what it reports is not applied"
        );
        let (status,): (String,) = sqlx::query_as("SELECT status FROM task_runs WHERE id = ?")
            .bind(&t.id).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "cancelled", "a cancelled row stays cancelled");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// A deferred task's `produces:` must reach the ledger. It succeeds in the
    /// sweep rather than on the worker-result path, so without the recorder
    /// wired there the pair validates, runs, and records nothing — leaving every
    /// downstream sensor and `on_datasets:` consumer parked forever.
    ///
    /// This pins the datastore half: the park row carries the spec and the task
    /// name the recorder needs, and recording after a resolve produces exactly
    /// one lineage row.
    #[tokio::test]
    async fn a_resolved_deferred_task_can_record_its_produces() {
        let (pool, path) = temp_pool().await;

        let uri = "clickhouse://analytics/marts/daily/2026-09-14";
        let yaml = format!(
            "name: p\ntasks:\n  - name: rollup\n    command: [\"true\"]\n    defer: {{ kind: http }}\n    produces: [\"{uri}\"]\n"
        );
        let dag = DagGraph::from_yaml(&yaml).unwrap();
        create_run(&pool, &dag, &yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let t = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let now = chrono::Utc::now().to_rfc3339();
        assert!(park_external(&pool, &t.id, t.version + 1, "http", "job-1", None, &now, None)
            .await.unwrap());

        // Everything the recorder needs rides on the claimed row.
        let due = claim_due_external_polls(&pool, 8, 30).await.unwrap();
        assert_eq!(due[0].name, "rollup", "the task name is on the row, not a second query");
        let produces: Vec<String> = serde_json::from_str::<crate::dag::TaskSpec>(
            due[0].input.as_deref().expect("the spec rides along"),
        )
        .unwrap()
        .produces;
        assert_eq!(produces, vec![uri.to_string()]);

        // Resolve, then record — the order the sweep uses, guarded on the
        // resolve having landed.
        assert!(resolve_external(&pool, &t.id, "job-1", Some("DONE")).await.unwrap());
        record_dataset_updates(&pool, "p", &t.id, &due[0].name, &produces).await.unwrap();

        // Straight at the ledger rather than through `list_dataset_events`,
        // which is ops-gated — this property holds in every feature world.
        let (n, task_name): (i64, String) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(MAX(task_name), '') FROM dataset_events WHERE uri = ?",
        )
        .bind(uri)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(n, 1, "exactly one lineage row for one resolve");
        assert_eq!(task_name, "rollup", "attributed to the task that produced it");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// `datasets_mode: all` is open, and fires exactly once both upstreams are
    /// fresh — with both cursors advancing together.
    ///
    /// That last part is what the gate's documented fallback could not do. A
    /// `wait: { dataset: … }` sensor stamps its cursor when the TASK PARKS
    /// (`park_wait_dataset`), which is after the trigger fired and the run got
    /// that far; a subscription stamps its cursor once at registration
    /// (`sync_dataset_triggers`) and advances it only when a fire consumes it.
    /// So an update to the partner dataset that lands between those two
    /// moments — the common case in a nightly mart where both upstreams arrive
    /// within minutes — is invisible to the sensor, which then waits for the
    /// NEXT one, a day later.
    #[tokio::test]
    async fn all_of_fan_in_fires_once_both_upstreams_are_fresh() {
        let (pool, path) = temp_pool().await;

        let a = "clickhouse://raw/orders".to_string();
        let b = "clickhouse://raw/customers".to_string();

        sync_dataset_triggers(&pool, "mart", &[a.clone(), b.clone()], "all").await.unwrap();
        assert!(
            claim_due_dataset_triggers(&pool).await.unwrap().is_empty(),
            "nothing fresh yet"
        );

        record_dataset_updates(&pool, "producer", "t-a", "load_a", std::slice::from_ref(&a))
            .await
            .unwrap();
        assert!(
            claim_due_dataset_triggers(&pool).await.unwrap().is_empty(),
            "all-of must NOT fire on one upstream — this is the whole difference from any-of"
        );

        record_dataset_updates(&pool, "producer", "t-b", "load_b", std::slice::from_ref(&b))
            .await
            .unwrap();
        let fires = claim_due_dataset_triggers(&pool).await.unwrap();
        assert_eq!(fires.len(), 1, "both upstreams fresh → one fire");
        assert_eq!(fires[0].workflow_name, "mart");
        assert_eq!(
            fires[0].advanced.len(),
            2,
            "one fire consumes BOTH cursors, so neither upstream's update is replayed \
             and neither is lost"
        );

        // Claimed, so a second scheduler sweeping the same tick gets nothing.
        assert!(claim_due_dataset_triggers(&pool).await.unwrap().is_empty());

        // any-of is the other half, and still fires on one.
        sync_dataset_triggers(&pool, "loose", &[a.clone(), b.clone()], "any").await.unwrap();
        record_dataset_updates(&pool, "producer", "t-a2", "load_a", &[a]).await.unwrap();
        let any = claim_due_dataset_triggers(&pool).await.unwrap();
        assert_eq!(any.len(), 1, "any-of fires on the first fresh upstream");
        assert_eq!(any[0].workflow_name, "loose");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// The fleet sweep's whole question, and the two ways a task can fail to be
    /// live. A workload for either is leftover.
    #[tokio::test]
    async fn live_task_ids_answers_only_for_non_terminal_tasks() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: demo\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "worker-A", 10).await.unwrap();
        assert_eq!(claimed.len(), 2, "both tasks are claimable");
        let (running, finishing) = (&claimed[0], &claimed[1]);

        // Both running: both live, so a workload for either is legitimate.
        let ids = vec![running.id.clone(), finishing.id.clone()];
        let live = live_task_ids(&pool, &ids).await.unwrap();
        assert_eq!(live.len(), 2, "a running task is live: {live:?}");

        // One goes terminal. Its workload is now leftover.
        assert!(mark_task_succeeded(&pool, &finishing.id, "worker-A", finishing.version + 1, None)
            .await
            .unwrap());
        let live = live_task_ids(&pool, &ids).await.unwrap();
        assert!(live.contains(&running.id), "still running");
        assert!(!live.contains(&finishing.id), "succeeded is terminal, so not live");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An id with no row at all is not live either — retention collects a run
    /// long before anyone notices its pod. Same answer, and deliberately so:
    /// the caller has already scoped its listing to one installation, which is
    /// what makes "no such row" safe to read as "not mine any more" rather than
    /// "belongs to a different dagron".
    #[tokio::test]
    async fn an_unknown_task_id_is_not_live() {
        let (pool, path) = temp_pool().await;
        let ghost = vec![Uuid::new_v4().to_string()];
        assert!(live_task_ids(&pool, &ghost).await.unwrap().is_empty());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// No ids means no query — the sweep saw nothing, and an empty `IN ()` is
    /// a syntax error rather than an empty answer.
    #[tokio::test]
    async fn live_task_ids_of_nothing_is_empty_without_asking() {
        let (pool, path) = temp_pool().await;
        assert!(live_task_ids(&pool, &[]).await.unwrap().is_empty());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The ids come off container labels, so the binding has to be binding.
    /// Interpolating them would make a label a SQL injection vector.
    #[tokio::test]
    async fn a_task_id_is_bound_not_interpolated() {
        let (pool, path) = temp_pool().await;
        let nasty = vec!["'); DROP TABLE task_runs; --".to_string()];
        assert!(live_task_ids(&pool, &nasty).await.unwrap().is_empty());
        // The table is still there, which is the actual assertion.
        assert!(live_task_ids(&pool, &["x".to_string()]).await.unwrap().is_empty());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }



    // ── Runtime fan-out over a task's output (`with_output_of:`) ─────────────

    const FANOUT_YAML: &str = r#"
name: partitions
tasks:
  - name: list
    command: ["sh", "-c", "echo ..."]
  - name: process
    command: ["handle", "{{ item }}"]
    with_output_of: list
    depends_on: [list]
  - name: summarize
    command: ["summarize"]
    depends_on: [process]
"#;

    /// Drive a run to the point where `process` is parked, with `list` having
    /// produced `output`. Returns (pool, path, run_id, barrier task id).
    pub(super) async fn parked_fanout(output: &str) -> (Pool, std::path::PathBuf, String, String) {
        let (pool, path) = temp_pool().await;
        let dag = DagGraph::from_yaml(FANOUT_YAML).unwrap();
        let run_id = create_run(&pool, &dag, FANOUT_YAML).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let c = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(c.len(), 1, "only `list` is claimable — the barrier must not be");
        assert_eq!(c[0].name, "list");
        let fence = c[0].version + 1;
        mark_task_succeeded(&pool, &c[0].id, "w1", fence, Some(output.to_string()))
            .await
            .unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        let barrier: (String, String, Option<String>) = sqlx::query_as(
            "SELECT id, status, claimed_by FROM task_runs WHERE run_id = ? AND name = 'process'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(barrier.1, "running", "the barrier parks rather than going ready");
        assert!(barrier.2.is_none(), "parked means no claim — and so no lease to recover");
        (pool, path, run_id, barrier.0)
    }

    /// The whole shape: a producer's JSON array becomes rows that did not exist
    /// when the run was created, and the task that was already wired to the
    /// barrier waits for all of them.
    #[tokio::test]
    async fn a_task_fans_out_over_its_producers_output_mid_run() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, run_id, barrier) = parked_fanout(r#"["a","b","c"]"#).await;

        // Nothing claimable while the barrier is unresolved: the instances do
        // not exist yet and the barrier itself must never be dispatched.
        assert!(claim_ready(&pool, "w1", 10).await.unwrap().is_empty());

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Expanded { instances: 3 },
            "three rows appeared mid-run"
        );

        let names: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM task_runs WHERE fanout_parent = ? ORDER BY name",
        )
        .bind(&barrier)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(names, vec!["process.0", "process.1", "process.2"]);

        // `{{ item }}` is substituted per instance, same as an expansion-time
        // fan-out — the whole point of the feature.
        let specs: Vec<String> =
            sqlx::query_scalar("SELECT input FROM task_runs WHERE fanout_parent = ? ORDER BY name")
                .bind(&barrier)
                .fetch_all(&pool)
                .await
                .unwrap();
        for (i, want) in ["a", "b", "c"].iter().enumerate() {
            let spec: crate::dag::TaskSpec = serde_json::from_str(&specs[i]).unwrap();
            assert_eq!(spec.command, vec!["handle".to_string(), want.to_string()]);
            assert!(spec.with_output_of.is_none(), "an instance must not fan out again");
        }

        // The instances are claimable; the barrier still is not.
        let claimed = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(claimed.len(), 3, "the instances are ready with nothing left to wait for");
        assert!(claimed.iter().all(|t| t.id != barrier));

        // Downstream is still blocked — it was wired to the barrier and the
        // barrier now waits for the instances.
        let downstream: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'summarize'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(downstream, "pending");

        for t in &claimed {
            mark_task_succeeded(&pool, &t.id, "w1", t.version + 1, Some("ok".into()))
                .await
                .unwrap();
        }
        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Joined { succeeded: true, instances: 3 }
        );

        advance_ready_tasks(&pool).await.unwrap();
        let downstream: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'summarize'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(downstream, "ready", "the dependent advanced once every instance was done");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The other shape the old exact-name lookup could not reach: a producer
    /// reached through a `templates:` call, whose rows are named
    /// `<call>.<task>`. Reading the consumer's dependencies covers it for the
    /// same reason it covers a fanned-out producer — `depends_on: [discover]`
    /// expands to the call's exit rows, and those are what to read.
    #[tokio::test]
    async fn a_fan_out_reads_a_producer_reached_through_a_template_call() {
        let _g = crate::env_lock();
        let (pool, path) = temp_pool().await;
        let yaml = r#"
name: templated
templates:
  - name: finder
    tasks:
      - { name: scan, command: ["scan"] }
      - { name: emit, command: ["emit"], depends_on: [scan] }
tasks:
  - { name: discover, template: finder }
  - name: process
    command: ["handle", "{{ item }}"]
    depends_on: [discover]
    with_output_of: discover
"#;
        let dag = DagGraph::from_yaml(yaml).expect("a templated producer is accepted");
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();

        // Drive the sub-DAG to done; `emit` is the call's exit, so it is the
        // row `process` depends on and therefore the one that is read.
        for _ in 0..3 {
            advance_ready_tasks(&pool).await.unwrap();
            for t in claim_ready(&pool, "w1", 10).await.unwrap() {
                let out = (t.name == "discover.emit").then(|| r#"["p","q"]"#.to_string());
                mark_task_succeeded(&pool, &t.id, "w1", t.version + 1, out).await.unwrap();
            }
        }
        advance_ready_tasks(&pool).await.unwrap();

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Expanded { instances: 2 },
            "read the exit of the sub-DAG it depends on"
        );
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM task_runs WHERE run_id = ? AND name LIKE 'process.%' ORDER BY name",
        )
        .bind(&run_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(names, vec!["process.0", "process.1"]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── Chaining: a fan-out over a producer that is ITSELF fanned out ────────

    const CHAINED_YAML: &str = r#"
name: chained
tasks:
  - name: regions
    command: ["list-partitions", "{{ item }}"]
    with_items: ["us", "eu"]
  - name: process
    command: ["handle", "{{ item }}"]
    depends_on: [regions]
    with_output_of: regions
  - name: report
    command: ["report"]
    depends_on: [process]
"#;

    /// Run `regions.0` and `regions.1` to success with the given outputs, then
    /// advance so the `process` barrier parks. Returns (pool, path, run, barrier).
    async fn parked_chain(outs: [&str; 2]) -> (Pool, std::path::PathBuf, String, String) {
        let (pool, path) = temp_pool().await;
        let dag = DagGraph::from_yaml(CHAINED_YAML).unwrap();
        let run_id = create_run(&pool, &dag, CHAINED_YAML).await.unwrap();

        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(claimed.len(), 2, "both producer instances, and not the barrier");
        for t in &claimed {
            let which = if t.name == "regions.0" { 0 } else { 1 };
            mark_task_succeeded(&pool, &t.id, "w1", t.version + 1, Some(outs[which].to_string()))
                .await
                .unwrap();
        }
        advance_ready_tasks(&pool).await.unwrap();

        let barrier: (String, String) = sqlx::query_as(
            "SELECT id, status FROM task_runs WHERE run_id = ? AND name = 'process'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(barrier.1, "running", "the barrier parks");
        (pool, path, run_id, barrier.0)
    }

    /// The whole point: `regions` fans out, each instance prints its own list,
    /// and `process` fans out over the union of what they all found.
    #[tokio::test]
    async fn a_fan_out_chains_off_a_producer_that_was_itself_fanned_out() {
        let _g = crate::env_lock();
        let (pool, path, run_id, barrier) = parked_chain([r#"["a","b"]"#, r#"["c"]"#]).await;

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Expanded { instances: 3 },
            "2 + 1 — the union of both producer instances' lists"
        );

        // Order is `depends_on` order, i.e. instance order: regions.0's items
        // first. A string sort over the producer names would give .0,.1,.10,.2
        // and silently scramble which item is which.
        let specs: Vec<String> = sqlx::query_scalar(
            "SELECT input FROM task_runs WHERE fanout_parent = ? ORDER BY name",
        )
        .bind(&barrier)
        .fetch_all(&pool)
        .await
        .unwrap();
        let args: Vec<String> = specs
            .iter()
            .map(|j| serde_json::from_str::<crate::dag::TaskSpec>(j).unwrap().command[1].clone())
            .collect();
        assert_eq!(args, vec!["a", "b", "c"], "us's partitions, then eu's");

        // And the task downstream of the barrier waits for all three.
        let claimed = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(claimed.len(), 3);
        for t in &claimed {
            mark_task_succeeded(&pool, &t.id, "w1", t.version + 1, None).await.unwrap();
        }
        reconcile_fanouts(&pool).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let report: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'report'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(report, "ready", "downstream never had to know there were three");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// One producer instance finding nothing is not an error — it contributes
    /// no items and the others still fan out. A region with no partitions is a
    /// result, not a failure.
    #[tokio::test]
    async fn a_producer_instance_that_found_nothing_contributes_nothing() {
        let _g = crate::env_lock();
        let (pool, path, _run, barrier) = parked_chain([r#"[]"#, r#"["c","d"]"#]).await;

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(events[0].1, crate::models::FanoutOutcome::Expanded { instances: 2 });
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = ?")
            .bind(&barrier)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 2);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Every producer empty is the same answer one empty producer gives: the
    /// barrier succeeds with nothing under it and downstream still runs.
    #[tokio::test]
    async fn a_chain_where_every_producer_found_nothing_still_succeeds() {
        let _g = crate::env_lock();
        let (pool, path, run_id, _b) = parked_chain([r#"[]"#, r#"[]"#]).await;

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Joined { succeeded: true, instances: 0 }
        );
        advance_ready_tasks(&pool).await.unwrap();
        let report: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'report'",
        )
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(report, "ready");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// When one of several producers prints something that is not a list, the
    /// message has to name THAT ROW. "fans out over 'regions'" would name three
    /// tasks and send the reader to check all of them.
    #[tokio::test]
    async fn a_bad_producer_in_a_chain_is_named_by_its_row_not_the_authored_task() {
        let _g = crate::env_lock();
        let (pool, path, _run, _b) = parked_chain([r#"["a"]"#, r#"{"oops": 1}"#]).await;

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        let crate::models::FanoutOutcome::Failed { reason } = &events[0].1 else {
            panic!("expected a failure, got {:?}", events[0].1);
        };
        assert!(reason.contains("regions.1"), "names the offending instance: {reason}");
        assert!(!reason.contains("over 'regions'"), "not the authored task: {reason}");
        assert!(reason.contains("an object"), "and what it found: {reason}");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// At expansion an empty `with_items:` is an authoring mistake and is
    /// refused. At run time "there was nothing to process" is a result, and a
    /// run that fails because a query returned no rows is a bad answer.
    #[tokio::test]
    async fn an_empty_list_succeeds_with_no_instances_rather_than_failing() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, run_id, barrier) = parked_fanout("[]").await;

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Joined { succeeded: true, instances: 0 }
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = ?")
                .bind(&barrier).fetch_one(&pool).await.unwrap(),
            0
        );

        advance_ready_tasks(&pool).await.unwrap();
        let downstream: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'summarize'",
        )
        .bind(&run_id).fetch_one(&pool).await.unwrap();
        assert_eq!(downstream, "ready", "downstream still runs — there was simply no work");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Output that is not a JSON array fails the barrier with a message naming
    /// what it got. Silently fanning out over nothing would look like success.
    #[tokio::test]
    async fn output_that_is_not_a_json_array_fails_the_fan_out_loudly() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, _run, barrier) = parked_fanout("partition-a\npartition-b").await;

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        let crate::models::FanoutOutcome::Failed { reason } = &events[0].1 else {
            panic!("expected a failure, got {:?}", events[0].1);
        };
        assert!(reason.contains("not valid JSON"), "says what was wrong: {reason}");

        let (status, output): (String, Option<String>) =
            sqlx::query_as("SELECT status, output FROM task_runs WHERE id = ?")
                .bind(&barrier).fetch_one(&pool).await.unwrap();
        assert_eq!(status, "failed");
        assert!(output.unwrap().contains("not valid JSON"), "the reason is on the row");

        // Re-sweeping a resolved barrier does nothing — it is no longer parked.
        assert!(reconcile_fanouts(&pool).await.unwrap().done.is_empty());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A JSON value that parses but is not a list is the subtler mistake, and
    /// the message has to say what it *got* or the author re-reads a correct
    /// command looking for a syntax error.
    #[tokio::test]
    async fn a_json_object_is_refused_by_what_it_is_not_by_what_was_wanted() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, _run, _b) = parked_fanout(r#"{"partitions": ["a"]}"#).await;
        let events = reconcile_fanouts(&pool).await.unwrap().done;
        let crate::models::FanoutOutcome::Failed { reason } = &events[0].1 else {
            panic!("expected a failure");
        };
        assert!(reason.contains("an object"), "names the kind it found: {reason}");
        assert!(reason.contains("JSON array"), "and the kind it wanted");
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The admission decision this feature owes the rest of the system: a run's
    /// task count is otherwise fixed before the run exists, which is what lets
    /// `budget:` refuse a blow-up at submit. Exceeding the ceiling here has to
    /// fail the task, not insert the rows and find out.
    #[tokio::test]
    async fn a_runtime_fan_out_past_the_task_ceiling_fails_instead_of_inserting() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let restore = std::env::var("DAGRON_MAX_TASKS_PER_RUN").ok();
        unsafe { std::env::set_var("DAGRON_MAX_TASKS_PER_RUN", "4") };

        // The run already holds 3 tasks; 4 more instances would make 7 > 4.
        let (pool, path, _run, barrier) = parked_fanout(r#"["a","b","c","d"]"#).await;
        let events = reconcile_fanouts(&pool).await.unwrap().done;
        let crate::models::FanoutOutcome::Failed { reason } = &events[0].1 else {
            panic!("expected a budget refusal, got {:?}", events[0].1);
        };
        assert!(reason.contains("DAGRON_MAX_TASKS_PER_RUN"), "names the knob: {reason}");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = ?")
                .bind(&barrier).fetch_one(&pool).await.unwrap(),
            0,
            "nothing was inserted — the ceiling is checked before the write, not after"
        );

        match restore {
            Some(v) => unsafe { std::env::set_var("DAGRON_MAX_TASKS_PER_RUN", v) },
            None => unsafe { std::env::remove_var("DAGRON_MAX_TASKS_PER_RUN") },
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A failing instance fails the barrier, which is what stops downstream —
    /// the whole reason the barrier stays as the join point.
    #[tokio::test]
    async fn a_failed_instance_fails_the_barrier_and_blocks_downstream() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, run_id, _b) = parked_fanout(r#"["a","b"]"#).await;
        reconcile_fanouts(&pool).await.unwrap();

        let claimed = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(claimed.len(), 2);
        mark_task_succeeded(&pool, &claimed[0].id, "w1", claimed[0].version + 1, None)
            .await
            .unwrap();
        mark_task_failed(&pool, &claimed[1].id, "w1", claimed[1].version + 1, Some("boom".into()))
            .await
            .unwrap();

        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Joined { succeeded: false, instances: 2 }
        );
        advance_ready_tasks(&pool).await.unwrap();
        let downstream: String = sqlx::query_scalar(
            "SELECT status FROM task_runs WHERE run_id = ? AND name = 'summarize'",
        )
        .bind(&run_id).fetch_one(&pool).await.unwrap();
        assert_eq!(downstream, "skipped", "a failed fan-out does not silently unblock its dependents");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Two schedulers sweeping the same tick both see zero instances. Without
    /// the CAS on the barrier's version they would both insert a full set, and
    /// the run would quietly do everything twice.
    #[tokio::test]
    async fn two_sweeps_of_the_same_barrier_expand_it_once() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, _run, barrier) = parked_fanout(r#"["a","b"]"#).await;

        let first = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(first[0].1, crate::models::FanoutOutcome::Expanded { instances: 2 });
        // The second sweep finds instances and takes the join path instead —
        // which, with nothing terminal yet, is a no-op.
        let second = reconcile_fanouts(&pool).await.unwrap().done;
        assert!(second.is_empty(), "no second expansion, and no premature join");

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = ?")
                .bind(&barrier).fetch_one(&pool).await.unwrap(),
            2,
            "two instances, not four"
        );
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── Per-attempt output retention (migration 044) ─────────────────────────
    //
    // `retry_task` overwrites `task_runs.output`, which is why a `repeat:` loop
    // has only ever shown its last iteration and a retried task has only ever
    // shown the attempt that passed. These prove the tail survives, that the
    // bound actually bounds, and that the opt-out is a real opt-out.

    /// `DAGRON_ATTEMPT_LOG_*` are process-global and every `retry_task` reads
    /// them, so these serialize against each other **and** against
    /// `attempt_log`'s own env tests — one lock, shared, or the two modules
    /// race over the same two keys.
    use crate::env_lock as attempt_env;

    fn set_attempt_env(bytes: Option<&str>, keep: Option<&str>) {
        for (k, v) in [("DAGRON_ATTEMPT_LOG_BYTES", bytes), ("DAGRON_ATTEMPT_LOG_KEEP", keep)] {
            match v {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }

    /// Create a one-task run and return (pool, path, task_id). The caller then
    /// drives claim → retry cycles, which is exactly what a `repeat:` loop is.
    async fn looping_task() -> (Pool, std::path::PathBuf, String) {
        let (pool, path) = temp_pool().await;
        let yaml = "name: r\ntasks:\n  - name: poll\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let id = claim_ready(&pool, "w1", 10).await.unwrap()[0].id.clone();
        (pool, path, id)
    }

    /// Run `n` iterations, each printing `output(i)`, re-claiming between them
    /// the way the engine does.
    async fn iterate(pool: &Pool, id: &str, n: usize, output: impl Fn(usize) -> String) {
        for i in 1..=n {
            let fence = sqlx::query_scalar::<_, i64>("SELECT version FROM task_runs WHERE id = ?")
                .bind(id)
                .fetch_one(pool)
                .await
                .unwrap();
            assert!(
                retry_task(
                    pool,
                    id,
                    "w1",
                    fence,
                    Some(output(i)),
                    chrono::Utc::now().to_rfc3339(),
                    AttemptEnd::Iteration,
                )
                .await
                .unwrap(),
                "iteration {i} was accepted"
            );
            // The engine's next tick: the row becomes claimable and `attempt`
            // increments, which is what numbers the next iteration.
            sqlx::query("UPDATE task_runs SET scheduled_at = NULL WHERE id = ?")
                .bind(id)
                .execute(pool)
                .await
                .unwrap();
            advance_ready_tasks(pool).await.unwrap();
            claim_ready(pool, "w1", 10).await.unwrap();
        }
    }

    #[tokio::test]
    async fn every_iteration_of_a_loop_is_readable_not_just_the_last() {
        let _g = attempt_env();
        set_attempt_env(None, None); // defaults
        let (pool, path, id) = looping_task().await;

        iterate(&pool, &id, 3, |i| format!("pass {i}")).await;

        let history = list_task_attempts(&pool, &id).await.unwrap();
        assert_eq!(
            history.iter().map(|a| a.attempt).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the three superseded iterations, oldest first"
        );
        assert_eq!(
            history.iter().map(|a| a.output.as_deref().unwrap()).collect::<Vec<_>>(),
            vec!["pass 1", "pass 2", "pass 3"],
            "each iteration kept its own output — this is the whole point"
        );
        assert!(history.iter().all(|a| a.reason == "iteration"), "and says it was a loop pass");
        assert!(history.iter().all(|a| !a.truncated), "short output is not truncated");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn the_live_attempt_stays_on_the_row_and_is_not_copied_into_the_history() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path, id) = looping_task().await;

        iterate(&pool, &id, 2, |i| format!("pass {i}")).await;
        // Attempt 3 is now claimed and running — its output belongs on the row.
        let fence = sqlx::query_scalar::<_, i64>("SELECT version FROM task_runs WHERE id = ?")
            .bind(&id).fetch_one(&pool).await.unwrap();
        append_task_output(&pool, &id, fence, "pass 3 so far", true).await.unwrap();

        let history = list_task_attempts(&pool, &id).await.unwrap();
        assert_eq!(history.len(), 2, "only the superseded ones");
        assert!(
            history.iter().all(|a| a.attempt < 3),
            "the running attempt is not in the history — duplicating it would double \
             the storage of the one attempt that is already stored whole"
        );
        let live: Option<String> =
            sqlx::query_scalar("SELECT log FROM task_runs WHERE id = ?")
                .bind(&id).fetch_one(&pool).await.unwrap();
        assert_eq!(live.as_deref(), Some("pass 3 so far"), "and the live tail is untouched");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn the_kept_tail_is_bounded_and_says_so() {
        let _g = attempt_env();
        set_attempt_env(Some("16"), None);
        let (pool, path, id) = looping_task().await;

        iterate(&pool, &id, 1, |_| "0123456789abcdefghij".to_string()).await;

        let a = list_task_attempts(&pool, &id).await.unwrap().pop().unwrap();
        assert_eq!(a.output.as_deref(), Some("456789abcdefghij"), "the tail, not the head");
        assert!(a.truncated, "recorded, so a reader knows there was more");

        set_attempt_env(None, None);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn the_window_drops_the_oldest_iterations_rather_than_growing() {
        let _g = attempt_env();
        set_attempt_env(None, Some("3"));
        let (pool, path, id) = looping_task().await;

        iterate(&pool, &id, 8, |i| format!("pass {i}")).await;

        let history = list_task_attempts(&pool, &id).await.unwrap();
        assert_eq!(
            history.iter().map(|a| a.attempt).collect::<Vec<_>>(),
            vec![6, 7, 8],
            "eight iterations, three retained — a loop cannot grow the table without bound"
        );

        set_attempt_env(None, None);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn retention_off_writes_nothing_at_all() {
        let _g = attempt_env();
        set_attempt_env(Some("0"), None);
        let (pool, path, id) = looping_task().await;

        iterate(&pool, &id, 4, |i| format!("pass {i}")).await;

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_attempts")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(rows, 0, "an operator who turns this off pays nothing for it");

        set_attempt_env(None, None);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `DAGRON_ATTEMPT_LOG_KEEP=0` turns off eviction, so the rows are
    /// unbounded — but the *response* must not be. The read keeps the newest
    /// `MAX_READ` and still hands them back oldest-first.
    #[tokio::test]
    async fn an_unbounded_retention_window_does_not_mean_an_unbounded_read() {
        let _g = attempt_env();
        set_attempt_env(None, Some("0")); // unlimited retention
        let (pool, path, id) = looping_task().await;

        let n = crate::attempt_log::MAX_READ + 5;
        iterate(&pool, &id, n, |i| format!("pass {i}")).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_attempts")
                .fetch_one(&pool).await.unwrap(),
            n as i64,
            "eviction really is off — otherwise this proves nothing"
        );

        let history = list_task_attempts(&pool, &id).await.unwrap();
        assert_eq!(history.len(), crate::attempt_log::MAX_READ, "the read is capped");
        assert_eq!(
            history.first().unwrap().attempt,
            6,
            "the newest are kept — a loop is diagnosed from where it stopped"
        );
        assert_eq!(history.last().unwrap().attempt, n as i64, "and still oldest-first");

        set_attempt_env(None, None);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The FK from `task_attempts` to `task_runs` must cascade, because
    /// `foreign_keys` is ON for this backend and the retention sweep deletes
    /// task rows directly. Without `ON DELETE CASCADE` every purge of a run
    /// that looped is a foreign-key violation — GC wedges, and only for
    /// deployments that actually use the feature.
    #[tokio::test]
    async fn purging_a_run_that_looped_does_not_wedge_on_the_attempt_history() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path, id) = looping_task().await;
        let run_id: String = sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = ?")
            .bind(&id).fetch_one(&pool).await.unwrap();

        iterate(&pool, &id, 3, |i| format!("pass {i}")).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_attempts")
                .fetch_one(&pool).await.unwrap(),
            3,
            "history exists — otherwise this test proves nothing"
        );

        sqlx::query("UPDATE workflow_runs SET status = 'succeeded', finished_at = ? WHERE id = ?")
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(&run_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'succeeded', claimed_by = NULL WHERE id = ?")
            .bind(&id).execute(&pool).await.unwrap();

        assert_eq!(
            purge_runs_by_id(&pool, &[run_id.clone()]).await.unwrap(),
            1,
            "the purge completes rather than erroring on the foreign key"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_attempts")
                .fetch_one(&pool).await.unwrap(),
            0,
            "and takes the history with it — a history without its task is not a record"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The archive document is written immediately before the purge, so it is
    /// the attempt history's last chance to exist anywhere. A looped run
    /// archived without it loses exactly what retention added.
    #[tokio::test]
    async fn the_archive_document_carries_the_attempt_history() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path, id) = looping_task().await;
        let run_id: String = sqlx::query_scalar("SELECT run_id FROM task_runs WHERE id = ?")
            .bind(&id).fetch_one(&pool).await.unwrap();

        iterate(&pool, &id, 2, |i| format!("pass {i}")).await;

        let doc = archive_doc_for_run(&pool, &run_id).await.unwrap().unwrap();
        let attempts = doc["task_attempts"].as_array().unwrap();
        assert_eq!(attempts.len(), 2, "both superseded passes are in the archive");
        assert_eq!(attempts[0]["output"], "pass 1");
        assert_eq!(attempts[1]["reason"], "iteration");
        // Additive: the keys the compactor reads are untouched.
        assert_eq!(doc["format"], "dagron.run-archive.v1");
        assert!(doc["tasks"].as_array().unwrap().len() >= 1);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The retention write shares the transition's transaction, so a retry that
    /// loses the CAS must leave no trace — otherwise a reclaimed worker's stale
    /// attempt writes history for a task it no longer owns.
    #[tokio::test]
    async fn a_stale_retry_records_no_attempt() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path, id) = looping_task().await;

        let fence = sqlx::query_scalar::<_, i64>("SELECT version FROM task_runs WHERE id = ?")
            .bind(&id).fetch_one(&pool).await.unwrap();
        assert!(
            !retry_task(&pool, &id, "someone-else", fence, Some("ghost".into()),
                        chrono::Utc::now().to_rfc3339(), AttemptEnd::Iteration)
                .await.unwrap(),
            "wrong worker — the CAS refuses it"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_attempts")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(rows, 0, "no row for a transition that did not happen");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── Quota back-pressure (migration 051) ──────────────────────────────────
    //
    // A pod refused by a full namespace quota is handed back, not failed. These
    // prove the give-back is exact and fenced, that reaching the bound writes
    // nothing by itself, and that the wait marker dies with the claim that ends
    // the wait — without any reset path having to clear it.

    const REFUSAL: &str = "not started: pod refused by namespace ResourceQuota 'tenant-quota' \
        (requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4)";

    /// `task_runs` as these tests need to see it: the raw columns, not the
    /// guarded projection `list_tasks` serves.
    #[derive(sqlx::FromRow)]
    struct QuotaRow {
        status: String,
        attempt: i64,
        version: i64,
        claimed_by: Option<String>,
        lease_expires_at: Option<String>,
        scheduled_at: Option<String>,
        output: Option<String>,
        quota_wait_since: Option<String>,
        quota_waits: i64,
        quota_wait_version: Option<i64>,
        quota_wait_detail: Option<String>,
    }

    async fn quota_row(pool: &Pool, id: &str) -> QuotaRow {
        sqlx::query_as(
            "SELECT status, attempt, version, claimed_by, lease_expires_at, scheduled_at, output,
                    quota_wait_since, quota_waits, quota_wait_version, quota_wait_detail
             FROM task_runs WHERE id = ?",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A one-task run, advanced to `ready`: (pool, path, run_id, task_id).
    async fn quota_task() -> (Pool, std::path::PathBuf, String, String) {
        let (pool, path) = temp_pool().await;
        let yaml = "name: q\ntasks:\n  - name: pod\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let id: String = sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = ?")
            .bind(&run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        (pool, path, run_id, id)
    }

    /// The engine's next dispatch of the task: its backoff elapses and it is
    /// claimed. Returns the fence of that claim.
    async fn claim_due(pool: &Pool, id: &str, worker: &str) -> i64 {
        sqlx::query("UPDATE task_runs SET scheduled_at = NULL WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await
            .unwrap();
        let claimed = claim_ready(pool, worker, 10).await.unwrap();
        assert_eq!(claimed.len(), 1, "the task is claimable once it is due");
        assert_eq!(claimed[0].id, id);
        claimed[0].version + 1
    }

    /// Move an episode's clock back, as if its first refusal were that long ago.
    async fn backdate_quota_wait(pool: &Pool, id: &str, secs: i64) -> String {
        let since = (chrono::Utc::now() - chrono::TimeDelta::seconds(secs)).to_rfc3339();
        sqlx::query("UPDATE task_runs SET quota_wait_since = ? WHERE id = ?")
            .bind(&since)
            .bind(id)
            .execute(pool)
            .await
            .unwrap();
        since
    }

    fn rfc3339(t: &Option<String>) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(t.as_deref().expect("timestamp is set"))
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[tokio::test]
    async fn a_quota_requeue_gives_the_attempt_back() {
        let (pool, path, _run, id) = quota_task().await;
        let fence = claim_due(&pool, &id, "w1").await;
        // What an earlier attempt left on the row has to survive the wait.
        sqlx::query("UPDATE task_runs SET output = 'from the attempt before' WHERE id = ?")
            .bind(&id).execute(&pool).await.unwrap();
        let claimed = quota_row(&pool, &id).await;
        assert_eq!((claimed.status.as_str(), claimed.attempt), ("running", 1));

        let before = chrono::Utc::now();
        let outcome = requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 3600).await.unwrap();
        let QuotaRequeue::Requeued { waits, waited_secs, retry_in_secs } = outcome else {
            panic!("a first refusal inside the bound is a requeue, got {outcome:?}");
        };
        assert_eq!((waits, waited_secs), (1, 0));
        assert!((3..=6).contains(&retry_in_secs), "5 s give or take the jitter, got {retry_in_secs}");

        let row = quota_row(&pool, &id).await;
        assert_eq!(row.status, "ready");
        assert!(row.claimed_by.is_none() && row.lease_expires_at.is_none(), "the claim is gone");
        assert_eq!(row.attempt, 0, "the attempt the claim consumed is given back");
        assert_eq!(row.version, fence, "version is left alone, as retry_task leaves it");
        assert_eq!(row.quota_wait_version, Some(row.version), "which is what marks the row as waiting");
        assert_eq!(row.quota_waits, 1);
        assert_eq!(row.quota_wait_detail.as_deref(), Some(REFUSAL));
        let since = rfc3339(&row.quota_wait_since);
        assert!(since >= before && since <= chrono::Utc::now(), "the clock starts at this refusal");
        assert!(
            rfc3339(&row.scheduled_at) >= since + chrono::TimeDelta::milliseconds(3_750),
            "due a backoff after the refusal, not at it"
        );
        assert_eq!(
            row.output.as_deref(),
            Some("from the attempt before"),
            "a wait has no result, so it must not overwrite one"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_attempts")
                .fetch_one(&pool).await.unwrap(),
            0,
            "and it is not an attempt, so it leaves no attempt row"
        );
        assert!(
            claim_ready(&pool, "w1", 10).await.unwrap().is_empty(),
            "not claimable until the backoff elapses"
        );

        // Ten refusals in a row are one episode, and still cost no attempt.
        for cycle in 2..=10 {
            let fence = claim_due(&pool, &id, "w1").await;
            let detail = format!("{REFUSAL} #{cycle}");
            let outcome = requeue_quota_wait(&pool, &id, "w1", fence, &detail, 3600).await.unwrap();
            assert!(
                matches!(outcome, QuotaRequeue::Requeued { waits, .. } if waits == cycle),
                "refusal {cycle}: {outcome:?}"
            );
        }
        let after = quota_row(&pool, &id).await;
        assert_eq!(after.attempt, 0, "ten refusals consumed no attempt");
        assert_eq!(after.quota_waits, 10);
        assert_eq!(after.quota_wait_since, row.quota_wait_since, "one episode, one clock");
        assert_eq!(after.quota_wait_version, Some(after.version));
        assert!(
            after.quota_wait_detail.as_deref().is_some_and(|d| d.ends_with("#10")),
            "the detail is the last refusal's"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The give-back undoes exactly one claim. Anyone who does not hold that
    /// claim — another worker, an older or newer fence, the same call a second
    /// time, a worker whose lease was recovered — gets nothing back.
    #[tokio::test]
    async fn a_quota_requeue_is_fenced() {
        let (pool, path, _run, id) = quota_task().await;
        let fence = claim_due(&pool, &id, "w1").await;

        for (worker, f) in [("w2", fence), ("w1", fence + 1), ("w1", fence - 1)] {
            assert_eq!(
                requeue_quota_wait(&pool, &id, worker, f, REFUSAL, 3600).await.unwrap(),
                QuotaRequeue::Stale,
                "{worker} with fence {f} does not hold the claim"
            );
        }
        let row = quota_row(&pool, &id).await;
        assert_eq!(
            (row.status.as_str(), row.attempt, row.quota_waits, row.claimed_by.as_deref()),
            ("running", 1, 0, Some("w1")),
            "a stale call changes nothing"
        );

        // The holder's call lands, storing a bounded detail…
        let long = "x".repeat(1000);
        assert!(matches!(
            requeue_quota_wait(&pool, &id, "w1", fence, &long, 3600).await.unwrap(),
            QuotaRequeue::Requeued { waits: 1, .. }
        ));
        // …and lands once: the same fence again has nothing left to undo.
        assert_eq!(
            requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 3600).await.unwrap(),
            QuotaRequeue::Stale
        );
        let row = quota_row(&pool, &id).await;
        assert_eq!((row.attempt, row.quota_waits), (0, 1), "one claim, one attempt given back");
        assert_eq!(
            row.quota_wait_detail.map(|d| d.chars().count()),
            Some(crate::quota_wait::DETAIL_MAX_CHARS)
        );
        assert!(
            !renew_task_lease(&pool, &id, "w1", fence, 300).await.unwrap(),
            "the claim triple is gone, so the heartbeat stops with it"
        );

        // A claim whose lease lapsed and was recovered is no longer this
        // worker's to hand back. Lease recovery keeps the attempt, and a late
        // requeue must not take it.
        let fence = claim_due(&pool, &id, "w1").await;
        sqlx::query("UPDATE task_runs SET lease_expires_at = '1970-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&id).execute(&pool).await.unwrap();
        assert_eq!(recover_expired_leases(&pool).await.unwrap(), 1);
        assert_eq!(
            requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 3600).await.unwrap(),
            QuotaRequeue::Stale
        );
        let row = quota_row(&pool, &id).await;
        assert_eq!((row.status.as_str(), row.attempt, row.quota_waits), ("ready", 1, 1));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// At the bound the datastore reports and steps back: the row is still the
    /// caller's, so the caller can fail it with a reason under the same fence.
    #[tokio::test]
    async fn a_quota_wait_expires_without_writing() {
        let (pool, path, _run, id) = quota_task().await;
        let fence = claim_due(&pool, &id, "w1").await;
        assert!(matches!(
            requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap(),
            QuotaRequeue::Requeued { .. }
        ));
        // The episode began fifteen minutes ago, against a ten-minute bound.
        let since = backdate_quota_wait(&pool, &id, 900).await;

        let fence = claim_due(&pool, &id, "w1").await;
        let held = quota_row(&pool, &id).await;
        let outcome = requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap();
        let QuotaRequeue::Expired { waits, waited_secs } = outcome else {
            panic!("past the bound on a continuing episode, got {outcome:?}");
        };
        assert_eq!(waits, 2, "the refusal that reached the bound is counted");
        assert!((900..=905).contains(&waited_secs), "measured from the first refusal, got {waited_secs}");

        let row = quota_row(&pool, &id).await;
        assert_eq!(
            (row.status.as_str(), row.claimed_by.as_deref(), row.attempt),
            ("running", Some("w1"), held.attempt),
            "still running and still claimed"
        );
        assert_eq!(row.lease_expires_at, held.lease_expires_at);
        assert_eq!(row.scheduled_at, held.scheduled_at);
        assert_eq!((row.quota_waits, row.quota_wait_since), (1, Some(since)), "nothing was written");

        assert!(
            mark_task_failed(&pool, &id, "w1", fence, Some("gave up waiting for namespace quota".into()))
                .await
                .unwrap(),
            "the fence still holds for the failure that follows"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The marker is never cleared; a claim outruns it. Once the task has been
    /// dispatched again it is not waiting, however it comes back to `ready`,
    /// and its next refusal starts a new episode with a new clock.
    #[tokio::test]
    async fn a_claim_ends_the_quota_wait_episode() {
        let (pool, path, run_id, id) = quota_task().await;
        // `list_tasks` is an ops read; the lean build checks the rest.
        #[cfg(not(feature = "ops"))]
        let _ = &run_id;

        let fence = claim_due(&pool, &id, "w1").await;
        assert!(matches!(
            requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap(),
            QuotaRequeue::Requeued { waits: 1, .. }
        ));
        assert_eq!(quota_wait_stats(&pool).await.unwrap().waiting, 1);
        #[cfg(feature = "ops")]
        {
            let task = &list_tasks(&pool, &run_id).await.unwrap()[0];
            assert!(task.quota_wait_since.is_some(), "a waiter says since when");
            assert_eq!(task.quota_wait_detail.as_deref(), Some(REFUSAL));
            assert_eq!(task.quota_waits, 1);
        }
        // Were the old episode to continue, the next refusal would be past the
        // ten-minute bound at once.
        backdate_quota_wait(&pool, &id, 3600).await;

        // The next dispatch starts, and fails for an ordinary reason.
        let fence = claim_due(&pool, &id, "w1").await;
        assert_eq!(quota_wait_stats(&pool).await.unwrap().waiting, 0, "a running task is not waiting");
        assert!(retry_task(
            &pool, &id, "w1", fence, Some("boom".into()),
            chrono::Utc::now().to_rfc3339(), AttemptEnd::Failed,
        )
        .await
        .unwrap());

        // `ready` again, old columns and all — but the claim moved `version`.
        let row = quota_row(&pool, &id).await;
        assert_eq!((row.status.as_str(), row.quota_waits), ("ready", 1), "the columns are never cleared");
        assert_ne!(row.quota_wait_version, Some(row.version));
        assert_eq!(quota_wait_stats(&pool).await.unwrap(), QuotaWaitStats::default());
        #[cfg(feature = "ops")]
        {
            let task = &list_tasks(&pool, &run_id).await.unwrap()[0];
            assert_eq!(
                (task.quota_wait_since.as_deref(), task.quota_wait_detail.as_deref(), task.quota_waits),
                (None, None, 0),
                "a task backing off after a real failure does not read as waiting on quota"
            );
        }

        let fence = claim_due(&pool, &id, "w1").await;
        let outcome = requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap();
        assert!(
            matches!(outcome, QuotaRequeue::Requeued { waits: 1, waited_secs: 0, .. }),
            "a new episode, not the hour-old one: {outcome:?}"
        );

        // The same when the dispatch in between died and its lease was recovered.
        backdate_quota_wait(&pool, &id, 3600).await;
        claim_due(&pool, &id, "w1").await;
        sqlx::query("UPDATE task_runs SET lease_expires_at = '1970-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&id).execute(&pool).await.unwrap();
        assert_eq!(recover_expired_leases(&pool).await.unwrap(), 1);
        assert_eq!(quota_wait_stats(&pool).await.unwrap().waiting, 0, "recovered, not waiting");

        let fence = claim_due(&pool, &id, "w1").await;
        let outcome = requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap();
        assert!(
            matches!(outcome, QuotaRequeue::Requeued { waits: 1, waited_secs: 0, .. }),
            "a new episode after lease recovery: {outcome:?}"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The reason the wait columns need no clearing: every way an operator
    /// brings a cancelled task back bumps `version`, so the marker a cancelled
    /// waiter still carries cannot match again. If one of these ever stopped
    /// bumping it, the revived task would read as waiting — and its next
    /// refusal would expire on the spot, against a clock from before the cancel.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn no_reset_path_resurrects_a_quota_wait() {
        let (pool, path, run_id, id) = quota_task().await;

        // `retry_task_from_ui` goes last: it re-arms only a failed run, so
        // after it there is no running run left to cancel.
        for reset in [
            "rerun_from_failed",
            "rerun_all",
            "clear_task_with_downstream",
            "retry_task_from_ui",
        ] {
            let fence = claim_due(&pool, &id, "w1").await;
            let outcome = requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap();
            assert!(
                matches!(outcome, QuotaRequeue::Requeued { waits: 1, waited_secs: 0, .. }),
                "before {reset}: a fresh episode, got {outcome:?}"
            );
            // A live waiter, already past the bound if its episode were to continue.
            backdate_quota_wait(&pool, &id, 3600).await;
            assert_eq!(quota_wait_stats(&pool).await.unwrap().waiting, 1);

            assert!(cancel_run(&pool, &run_id).await.unwrap().is_some());
            assert_eq!(quota_wait_stats(&pool).await.unwrap().waiting, 0, "cancelled is not waiting");

            match reset {
                "rerun_from_failed" => {
                    assert_eq!(rerun_from_failed(&pool, &run_id).await.unwrap(), Some(1))
                }
                "rerun_all" => assert_eq!(rerun_all(&pool, &run_id).await.unwrap(), Some(1)),
                "clear_task_with_downstream" => {
                    assert_eq!(clear_task_with_downstream(&pool, &run_id, &id).await.unwrap(), Some(1))
                }
                _ => assert!(retry_task_from_ui(&pool, &id).await.unwrap()),
            }
            advance_ready_tasks(&pool).await.unwrap();

            let row = quota_row(&pool, &id).await;
            assert_eq!(row.status, "ready", "{reset} brought the task back");
            assert_ne!(row.quota_wait_version, Some(row.version), "{reset} must bump version");
            assert_eq!(
                quota_wait_stats(&pool).await.unwrap().waiting,
                0,
                "{reset} must not bring the old wait back with it"
            );
            let task = &list_tasks(&pool, &run_id).await.unwrap()[0];
            assert!(task.quota_wait_since.is_none(), "{reset}: the task view agrees");
        }

        // And the last path's revived task starts a fresh episode like the rest.
        let fence = claim_due(&pool, &id, "w1").await;
        let outcome = requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 600).await.unwrap();
        assert!(
            matches!(outcome, QuotaRequeue::Requeued { waits: 1, waited_secs: 0, .. }),
            "after retry_task_from_ui: a fresh episode, got {outcome:?}"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The gauge counts rows that are waiting *now*. A task that waited and was
    /// then dispatched, or failed, keeps its wait columns and must not be in it.
    #[tokio::test]
    async fn quota_wait_stats_counts_only_live_waiters() {
        let (pool, path) = temp_pool().await;
        assert_eq!(quota_wait_stats(&pool).await.unwrap(), QuotaWaitStats::default());

        let yaml = "name: q\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n  - name: d\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        // All four are refused once.
        let mut ids = std::collections::HashMap::new();
        for task in claim_ready(&pool, "w1", 10).await.unwrap() {
            let outcome =
                requeue_quota_wait(&pool, &task.id, "w1", task.version + 1, REFUSAL, 86_400)
                    .await
                    .unwrap();
            assert!(matches!(outcome, QuotaRequeue::Requeued { .. }), "{}: {outcome:?}", task.name);
            ids.insert(task.name, task.id);
        }
        assert_eq!(ids.len(), 4);
        assert_eq!(quota_wait_stats(&pool).await.unwrap().waiting, 4);
        // Hold all four in backoff for the length of the test, so each claim
        // below takes only the row it makes due.
        sqlx::query("UPDATE task_runs SET scheduled_at = '2999-01-01T00:00:00+00:00'")
            .execute(&pool).await.unwrap();

        // `a` has waited five minutes; `c` and `d` longer still, but they are
        // about to stop waiting.
        let oldest = backdate_quota_wait(&pool, &ids["a"], 300).await;
        backdate_quota_wait(&pool, &ids["c"], 9_000).await;
        backdate_quota_wait(&pool, &ids["d"], 9_000).await;

        // `c` is dispatched and runs; `d` is dispatched and fails.
        claim_due(&pool, &ids["c"], "w1").await;
        let fence = claim_due(&pool, &ids["d"], "w1").await;
        assert!(mark_task_failed(&pool, &ids["d"], "w1", fence, Some("boom".into())).await.unwrap());
        for name in ["c", "d"] {
            assert_eq!(quota_row(&pool, &ids[name]).await.quota_waits, 1, "{name} still carries its wait");
        }

        let stats = quota_wait_stats(&pool).await.unwrap();
        assert_eq!(stats.waiting, 2, "a and b; not the running row, not the failed one");
        assert_eq!(stats.oldest_since.as_deref(), Some(oldest.as_str()), "a's clock, not c's or d's");
        assert!((300..=305).contains(&stats.oldest_age_secs(chrono::Utc::now())));

        #[cfg(feature = "ops")]
        {
            let snap = status_counts(&pool).await.unwrap();
            assert_eq!(snap.quota_waiting, 2, "/metrics reads the same count");
            assert_eq!(snap.quota_wait_oldest_since, stats.oldest_since);
        }

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// `attempt` numbers the attempts that ran. A refusal between two real
    /// failures must not shift the second one's number, or overwrite the
    /// first one's history row.
    #[tokio::test]
    async fn a_quota_wait_between_two_failures_does_not_shift_the_attempt_history() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path, _run, id) = quota_task().await;
        async fn history(pool: &Pool, id: &str) -> Vec<(i64, String, Option<String>)> {
            sqlx::query_as(
                "SELECT attempt, reason, output FROM task_attempts WHERE task_id = ? ORDER BY attempt",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap()
        }

        // Attempt 1 starts and fails for real.
        let fence = claim_due(&pool, &id, "w1").await;
        assert!(retry_task(
            &pool, &id, "w1", fence, Some("boom 1".into()),
            chrono::Utc::now().to_rfc3339(), AttemptEnd::Failed,
        )
        .await
        .unwrap());

        // Its retry is refused for quota: attempt 2 never started.
        let fence = claim_due(&pool, &id, "w1").await;
        assert_eq!(quota_row(&pool, &id).await.attempt, 2, "the claim counted it");
        assert!(matches!(
            requeue_quota_wait(&pool, &id, "w1", fence, REFUSAL, 3600).await.unwrap(),
            QuotaRequeue::Requeued { .. }
        ));
        let row = quota_row(&pool, &id).await;
        assert_eq!(row.attempt, 1, "back to the one attempt that ran");
        assert_eq!(row.output.as_deref(), Some("boom 1"), "whose output is still the row's");
        assert_eq!(
            history(&pool, &id).await,
            vec![(1, "failed".to_string(), Some("boom 1".to_string()))],
            "and whose history row is intact"
        );

        // The dispatch that does start is attempt 2, and is recorded as 2.
        let fence = claim_due(&pool, &id, "w1").await;
        assert!(retry_task(
            &pool, &id, "w1", fence, Some("boom 2".into()),
            chrono::Utc::now().to_rfc3339(), AttemptEnd::Failed,
        )
        .await
        .unwrap());
        assert_eq!(
            history(&pool, &id).await,
            vec![
                (1, "failed".to_string(), Some("boom 1".to_string())),
                (2, "failed".to_string(), Some("boom 2".to_string())),
            ]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── Running again keeps what it replaces (migration 056) ─────────────────
    //
    // A rerun, a retry and a clear each wiped the attempt that was on the row.
    // These prove the attempt is kept first, that a task's attempts are
    // numbered through its life, that a run which had ended asks its
    // concurrency key before it runs again, and that each writes its event.

    /// The events that say tasks of `run_id` were put back to run again.
    #[cfg(feature = "ops")]
    async fn run_again_events(pool: &Pool, run_id: &str) -> Vec<(String, serde_json::Value)> {
        sqlx::query_as::<_, (String, String)>(
            "SELECT event_type, payload FROM event_outbox
             WHERE run_id = ? AND event_type IN ('run.rerun', 'run.auto_rerun')
             ORDER BY created_at, rowid",
        )
        .bind(run_id)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(kind, payload)| (kind, serde_json::from_str(&payload).unwrap()))
        .collect()
    }

    /// End `run_id` as failed at its task `a`, which ran once and printed
    /// `output`, with every other task cancelled before it ran: the shape a
    /// failure leaves.
    #[cfg(feature = "ops")]
    async fn fail_at(pool: &Pool, run_id: &str, a: &str, output: &str) {
        sqlx::query("UPDATE task_runs SET status = 'cancelled' WHERE run_id = ? AND id <> ?")
            .bind(run_id)
            .bind(a)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE task_runs SET status = 'failed', attempt = 1, output = ?,
                 scheduled_at = '2026-01-01T00:00:00Z', finished_at = '2026-01-01T00:00:05Z'
             WHERE id = ?",
        )
        .bind(output)
        .bind(a)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET status = 'failed', finished_at = '2026-01-01T00:00:05Z' WHERE id = ?",
        )
        .bind(run_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A run of `a → b` under `head` (the spec's lines before its tasks), and
    /// its two tasks' ids.
    #[cfg(feature = "ops")]
    async fn chain_run(pool: &Pool, head: &str) -> (String, String, String) {
        let yaml = format!(
            "{head}tasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [\"a\"]\n"
        );
        let dag = DagGraph::from_yaml(&yaml).unwrap();
        let run_id = create_run(pool, &dag, &yaml).await.unwrap();
        let tasks = list_tasks(pool, &run_id).await.unwrap();
        let id = |name: &str| tasks.iter().find(|t| t.name == name).unwrap().id.clone();
        (run_id, id("a"), id("b"))
    }

    /// `(attempt, attempt_base, status)` of a task's row.
    #[cfg(feature = "ops")]
    async fn round(pool: &Pool, id: &str) -> (i64, i64, String) {
        sqlx::query_as("SELECT attempt, attempt_base, status FROM task_runs WHERE id = ?")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A rerun keeps the attempt it wipes: how it ended, when, and what it
    /// printed. The task's next attempts are numbered on from it, though the
    /// row's own count starts again so that the task has its retries back. A
    /// task that never ran has nothing to keep.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_rerun_keeps_the_attempts_it_replaces_and_numbers_on_from_them() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let (run, a, b) = chain_run(&pool, "name: chain\n").await;
        fail_at(&pool, &run, &a, "boom").await;

        assert_eq!(rerun_from_failed(&pool, &run).await.unwrap(), Some(2));
        let kept = list_task_attempts(&pool, &a).await.unwrap();
        assert_eq!(kept.len(), 1, "{kept:?}");
        let first = &kept[0];
        assert_eq!(
            (first.attempt, first.reason.as_str(), first.status.as_deref(), first.output.as_deref()),
            (1, "rerun", Some("failed"), Some("boom"))
        );
        assert_eq!(first.scheduled_at.as_deref(), Some("2026-01-01T00:00:00Z"));
        assert_eq!(first.finished_at, "2026-01-01T00:00:05Z", "when the attempt ended, not now");
        assert!(!first.truncated);
        assert!(list_task_attempts(&pool, &b).await.unwrap().is_empty(), "b never ran");

        assert_eq!(round(&pool, &a).await, (0, 1, "pending".to_string()));
        assert_eq!(task_attempt_base(&pool, &a).await.unwrap(), 1);
        assert_eq!(task_attempt_base(&pool, "no-such-task").await.unwrap(), 0);

        // The round after: a worker takes it, fails, and its own retry keeps
        // that attempt as the task's second, not as a second "attempt 1".
        advance_ready_tasks(&pool).await.unwrap();
        assert_eq!(claim_ready(&pool, "w1", 10).await.unwrap()[0].id, a);
        let fence: i64 = sqlx::query_scalar("SELECT version FROM task_runs WHERE id = ?")
            .bind(&a)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(retry_task(
            &pool, &a, "w1", fence, Some("boom again".into()),
            chrono::Utc::now().to_rfc3339(), AttemptEnd::Failed,
        )
        .await
        .unwrap());
        let kept = list_task_attempts(&pool, &a).await.unwrap();
        assert_eq!(
            kept.iter().map(|k| (k.attempt, k.reason.as_str(), k.output.as_deref())).collect::<Vec<_>>(),
            vec![(1, "rerun", Some("boom")), (2, "failed", Some("boom again"))]
        );
        assert_eq!(kept[1].status, None, "a worker's retry: the task was not terminal");

        assert_eq!(
            run_again_events(&pool, &run).await,
            vec![(
                "run.rerun".to_string(),
                serde_json::json!({ "run_id": run, "by": "rerun", "reset_tasks": 2 })
            )],
            "one event, for the one rerun"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A retry keeps the failed attempt it replaces, sets a run that had ended
    /// running again, and leaves the task's count where it was. A clear keeps
    /// an attempt that succeeded, and says so.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_retry_and_a_clear_keep_what_they_replace() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let (run, a, b) = chain_run(&pool, "name: chain\n").await;
        fail_at(&pool, &run, &a, "boom").await;
        sqlx::query("UPDATE workflow_runs SET status = 'cancelled' WHERE id = ?")
            .bind(&run)
            .execute(&pool)
            .await
            .unwrap();

        assert!(retry_task_from_ui(&pool, &a).await.unwrap());
        assert_eq!(round(&pool, &a).await, (1, 0, "ready".to_string()), "its count goes on");
        assert_eq!(round(&pool, &b).await.2, "cancelled", "only the task asked for");
        let run_row = get_run(&pool, &run).await.unwrap().unwrap();
        assert_eq!(run_row.status.to_string(), "running", "a run whose task is to run is running");
        let kept = list_task_attempts(&pool, &a).await.unwrap();
        assert_eq!(
            kept.iter().map(|k| (k.attempt, k.reason.as_str(), k.status.as_deref())).collect::<Vec<_>>(),
            vec![(1, "retry", Some("failed"))]
        );
        assert!(!retry_task_from_ui(&pool, &a).await.unwrap(), "not failed any more");
        assert!(!retry_task_from_ui(&pool, "no-such-task").await.unwrap());

        // Its second attempt succeeds, and is then cleared.
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', attempt = 2, output = 'ok',
                 finished_at = '2026-01-01T00:01:00Z' WHERE id = ?",
        )
        .bind(&a)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(clear_task_with_downstream(&pool, &run, &a).await.unwrap(), Some(2));
        assert_eq!(round(&pool, &a).await, (0, 2, "pending".to_string()));
        let kept = list_task_attempts(&pool, &a).await.unwrap();
        assert_eq!(
            kept.iter()
                .map(|k| (k.attempt, k.reason.as_str(), k.status.as_deref(), k.output.as_deref()))
                .collect::<Vec<_>>(),
            vec![(1, "retry", Some("failed"), Some("boom")), (2, "clear", Some("succeeded"), Some("ok"))]
        );

        assert_eq!(
            run_again_events(&pool, &run).await,
            vec![
                (
                    "run.rerun".to_string(),
                    serde_json::json!({ "run_id": run, "by": "retry", "reset_tasks": 1, "task_id": a })
                ),
                (
                    "run.rerun".to_string(),
                    serde_json::json!({ "run_id": run, "by": "clear", "reset_tasks": 2, "task_id": a })
                ),
            ]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A retried task is queued when it is retried, as a rerun's and a
    /// clear's are: its time is kept, and it is claimed at once.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_retried_task_is_queued_when_it_is_retried() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let (run, a, _) = chain_run(&pool, "name: chain\n").await;
        fail_at(&pool, &run, &a, "boom").await;
        let before = chrono::Utc::now();

        assert!(retry_task_from_ui(&pool, &a).await.unwrap());
        let queued = task_queued_at(&pool, &a)
            .await
            .unwrap()
            .expect("a retried task's time is kept");
        let queued = chrono::DateTime::parse_from_rfc3339(&queued).unwrap();
        assert!(
            queued >= before,
            "queued at the retry, not before it: {queued}"
        );
        let claimed = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(
            claimed.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            vec![a.as_str()]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run that had ended asks its concurrency key before it runs again, as
    /// a new run of the key would: while the key is full a rerun, a retry and
    /// a clear are each refused, and write nothing. A task retried in a run
    /// that is running already asks nothing.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_run_set_running_again_asks_its_concurrency_key() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let head = "name: one-at-a-time\nmax_active_runs: 1\n";
        let (older, a, _) = chain_run(&pool, head).await;
        fail_at(&pool, &older, &a, "boom").await;
        let (newer, _, _) = chain_run(&pool, head).await;

        let full = |e: anyhow::Error| {
            let m = e
                .downcast_ref::<crate::models::MaxActiveRunsReached>()
                .unwrap_or_else(|| panic!("not a full key: {e:?}"));
            (m.name.clone(), m.max, m.active)
        };
        let want = ("one-at-a-time".to_string(), 1, 1);
        assert_eq!(full(rerun_from_failed(&pool, &older).await.unwrap_err()), want);
        assert_eq!(full(rerun_all(&pool, &older).await.unwrap_err()), want);
        assert_eq!(full(retry_task_from_ui(&pool, &a).await.unwrap_err()), want);
        assert_eq!(full(clear_task_with_downstream(&pool, &older, &a).await.unwrap_err()), want);

        assert_eq!(get_run(&pool, &older).await.unwrap().unwrap().status.to_string(), "failed");
        assert_eq!(round(&pool, &a).await, (1, 0, "failed".to_string()), "nothing was reset");
        assert!(list_task_attempts(&pool, &a).await.unwrap().is_empty(), "and nothing kept");
        assert!(run_again_events(&pool, &older).await.is_empty(), "and nothing announced");

        // The key's run ends: the older one may run again.
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
            .bind(&newer)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(rerun_from_failed(&pool, &older).await.unwrap(), Some(2));

        // It is the key's one run now. A task of it that fails and is retried
        // asks nothing: the run is not being set running, it is running.
        sqlx::query("UPDATE task_runs SET status = 'failed', attempt = 1 WHERE id = ?")
            .bind(&a)
            .execute(&pool)
            .await
            .unwrap();
        assert!(retry_task_from_ui(&pool, &a).await.unwrap());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The engine's own rerun keeps what it replaces as a person's does, and
    /// writes the event it always wrote, now with the rerun or not at all.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn the_engine_s_own_rerun_writes_its_event_with_the_rerun() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let (run, a, _) = chain_run(&pool, "name: chain\n").await;
        fail_at(&pool, &run, &a, "boom").await;

        let reset = rerun_from_failed_as(&pool, &run, crate::models::RunAgain::AutoRerun)
            .await
            .unwrap();
        assert_eq!(reset, Some(2));
        assert_eq!(list_task_attempts(&pool, &a).await.unwrap()[0].reason, "rerun");
        assert_eq!(
            run_again_events(&pool, &run).await,
            vec![(
                "run.auto_rerun".to_string(),
                serde_json::json!({
                    "run_id": run, "reset_tasks": 2, "reason": "auto_rerun_incomplete"
                })
            )]
        );
        // Already running: no second rerun, and no second event.
        let again = rerun_from_failed_as(&pool, &run, crate::models::RunAgain::AutoRerun)
            .await
            .unwrap();
        assert_eq!(again, None);
        assert_eq!(run_again_events(&pool, &run).await.len(), 1);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The bound still bounds: a replaced attempt's output is kept as a tail,
    /// the window drops the oldest attempts, and retention switched off keeps
    /// nothing.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_replaced_attempt_is_a_bounded_tail_within_the_window() {
        let _g = attempt_env();
        set_attempt_env(Some("4"), Some("1"));
        let (pool, path) = temp_pool().await;
        let (run, a, _) = chain_run(&pool, "name: chain\n").await;

        fail_at(&pool, &run, &a, "abcdefghij").await;
        assert_eq!(rerun_from_failed(&pool, &run).await.unwrap(), Some(2));
        let kept = list_task_attempts(&pool, &a).await.unwrap();
        assert_eq!((kept[0].output.as_deref(), kept[0].truncated), (Some("ghij"), true));

        // A second round, replaced in its turn: one attempt is kept, the newest.
        fail_at(&pool, &run, &a, "xy").await;
        assert_eq!(rerun_from_failed(&pool, &run).await.unwrap(), Some(2));
        let kept = list_task_attempts(&pool, &a).await.unwrap();
        assert_eq!(
            kept.iter().map(|k| (k.attempt, k.output.as_deref(), k.truncated)).collect::<Vec<_>>(),
            vec![(2, Some("xy"), false)]
        );

        set_attempt_env(Some("0"), None);
        fail_at(&pool, &run, &a, "unkept").await;
        assert_eq!(rerun_from_failed(&pool, &run).await.unwrap(), Some(2));
        assert_eq!(list_task_attempts(&pool, &a).await.unwrap().len(), 1, "retention is off");
        assert_eq!(round(&pool, &a).await, (0, 3, "pending".to_string()), "the count goes on all the same");

        set_attempt_env(None, None);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── A rerun of a run whole (`from: all`) ─────────────────────────────────

    /// A rerun of a run whole puts back every task, what succeeded included,
    /// in a run that succeeded as in one that failed. It keeps what it
    /// replaces, drops a checkpoint with the rest of a task's result, and
    /// says `"from": "all"` in its event. A run that has not ended, or does
    /// not exist, is not rerun.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_rerun_of_a_run_whole_puts_back_what_succeeded() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let (run, a, b) = chain_run(&pool, "name: chain\n").await;
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', attempt = 1, output = 'ok',
                 checkpoint_uri = 'file:///cp', scheduled_at = '2026-01-01T00:00:00Z',
                 finished_at = '2026-01-01T00:00:05Z'
             WHERE run_id = ?",
        )
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET status = 'succeeded', output = 'ok',
                 finished_at = '2026-01-01T00:00:05Z' WHERE id = ?",
        )
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
        let version = |id: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>("SELECT version FROM task_runs WHERE id = ?")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        let before = version(a.clone()).await;

        assert_eq!(
            rerun_from_failed(&pool, &run).await.unwrap(),
            None,
            "from the failed: none failed"
        );
        assert_eq!(rerun_all(&pool, &run).await.unwrap(), Some(2));
        let row = get_run(&pool, &run).await.unwrap().unwrap();
        assert_eq!(row.status.to_string(), "running");
        assert_eq!((row.finished_at, row.output), (None, None));
        for id in [&a, &b] {
            assert_eq!(round(&pool, id).await, (0, 1, "pending".to_string()));
            let kept = list_task_attempts(&pool, id).await.unwrap();
            assert_eq!(
                kept.iter()
                    .map(|k| (
                        k.attempt,
                        k.reason.as_str(),
                        k.status.as_deref(),
                        k.output.as_deref()
                    ))
                    .collect::<Vec<_>>(),
                vec![(1, "rerun", Some("succeeded"), Some("ok"))]
            );
        }
        assert_eq!(
            version(a.clone()).await,
            before + 1,
            "a stale worker is fenced off"
        );
        let (deps, checkpoint): (i64, Option<String>) =
            sqlx::query_as("SELECT remaining_deps, checkpoint_uri FROM task_runs WHERE id = ?")
                .bind(&b)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            (deps, checkpoint),
            (1, None),
            "b waits for a again, and resumes nothing"
        );
        assert_eq!(
            run_again_events(&pool, &run).await,
            vec![(
                "run.rerun".to_string(),
                serde_json::json!({ "run_id": run, "by": "rerun", "reset_tasks": 2, "from": "all" })
            )]
        );

        // It runs again from its first task.
        advance_ready_tasks(&pool).await.unwrap();
        assert_eq!(round(&pool, &a).await.2, "ready");
        assert_eq!(round(&pool, &b).await.2, "pending");

        // Running: not rerun again, and nothing more written.
        assert_eq!(rerun_all(&pool, &run).await.unwrap(), None);
        assert_eq!(rerun_all(&pool, "no-such-run").await.unwrap(), None);
        assert_eq!(run_again_events(&pool, &run).await.len(), 1);

        // A run that failed: what failed, what was cancelled and what
        // succeeded are all put back.
        let (failed, a, b) = chain_run(&pool, "name: chain\n").await;
        fail_at(&pool, &failed, &a, "boom").await;
        assert_eq!(rerun_all(&pool, &failed).await.unwrap(), Some(2));
        assert_eq!(round(&pool, &a).await, (0, 1, "pending".to_string()));
        assert_eq!(round(&pool, &b).await, (0, 0, "pending".to_string()));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A runtime fan-out run whole again: its instances go, and are made again
    /// from what its producer prints this time. A failed instance of the run
    /// before holds nothing back, as it would were it kept until the barrier
    /// is expanded again.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_rerun_of_a_run_whole_fans_out_again() {
        let _g = crate::env_lock();
        let (pool, path, run_id, barrier) = parked_fanout(r#"["a","b"]"#).await;
        reconcile_fanouts(&pool).await.unwrap();
        let instances = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(instances.len(), 2);
        mark_task_succeeded(
            &pool,
            &instances[0].id,
            "w1",
            instances[0].version + 1,
            None,
        )
        .await
        .unwrap();
        mark_task_failed(
            &pool,
            &instances[1].id,
            "w1",
            instances[1].version + 1,
            None,
        )
        .await
        .unwrap();
        reconcile_fanouts(&pool).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        reap_completed_runs(&pool).await.unwrap();
        assert_eq!(
            get_run(&pool, &run_id)
                .await
                .unwrap()
                .unwrap()
                .status
                .to_string(),
            "failed"
        );

        // `list`, `process` and `summarize`: the two instances are not among them.
        assert_eq!(rerun_all(&pool, &run_id).await.unwrap(), Some(3));
        let instances: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE fanout_parent = ?")
                .bind(&barrier)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(instances, 0, "the instances went");
        let edges: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_dependencies WHERE dependent_id = ?")
                .bind(&barrier)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(edges, 1, "the barrier waits for its producer alone");

        advance_ready_tasks(&pool).await.unwrap();
        let c = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(c.len(), 1, "the producer runs first, and alone");
        assert_eq!(c[0].name, "list");
        mark_task_succeeded(
            &pool,
            &c[0].id,
            "w1",
            c[0].version + 1,
            Some(r#"["x","y","z"]"#.into()),
        )
        .await
        .unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let events = reconcile_fanouts(&pool).await.unwrap().done;
        assert_eq!(
            events[0].1,
            crate::models::FanoutOutcome::Expanded { instances: 3 },
            "expanded again over the producer's new output"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run whole again asks its approval gates again, from nothing: the
    /// decision it asked for last time is kept as the gate's attempt, and is
    /// not left on the gate for an approver to read as this one's. Who asked
    /// for the rerun is recorded on what it put back.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_rerun_of_a_run_whole_asks_its_gates_again() {
        let _g = attempt_env();
        set_attempt_env(None, None);
        let (pool, path) = temp_pool().await;
        let yaml = "name: gate\ntasks:\n  - name: gate\n    type: approval\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = create_run(&pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let gate = list_tasks(&pool, &run_id).await.unwrap()[0].clone();
        assert_eq!(gate.status, crate::models::TaskStatus::AwaitingApproval);
        let decided = |by: &'static str| {
            let (pool, run_id, gate) = (pool.clone(), run_id.clone(), gate.id.clone());
            async move {
                assert!(resolve_approval(
                    &pool,
                    &run_id,
                    &gate,
                    true,
                    Some(by),
                    Some("looks right")
                )
                .await
                .unwrap());
                reap_completed_runs(&pool).await.unwrap();
                assert_eq!(
                    get_run(&pool, &run_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        .to_string(),
                    "succeeded"
                );
            }
        };
        // `(decided_by, decision_comment, reset_by)` of the gate.
        let decision = |gate: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
                    "SELECT decided_by, decision_comment, reset_by FROM task_runs WHERE id = ?",
                )
                .bind(gate)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        decided("alice").await;

        let mut tx = pool.begin().await.unwrap();
        assert_eq!(
            rerun_all_in(&mut tx, &run_id, Some("carol")).await.unwrap(),
            Some(1)
        );
        tx.commit().await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let gate = list_tasks(&pool, &run_id).await.unwrap()[0].clone();
        assert_eq!(
            gate.status,
            crate::models::TaskStatus::AwaitingApproval,
            "asked again"
        );
        assert_eq!(
            decision(gate.id.clone()).await,
            (None, None, Some("carol".to_string())),
            "last time's decision is not this one's"
        );
        assert!(claim_ready(&pool, "w", 10).await.unwrap().is_empty());

        // Each decision it replaced is kept, numbered as an attempt.
        decided("bob").await;
        assert_eq!(rerun_all(&pool, &run_id).await.unwrap(), Some(1));
        assert_eq!(decision(gate.id.clone()).await, (None, None, None));
        assert_eq!(round(&pool, &gate.id).await, (0, 2, "pending".to_string()));
        let kept = list_task_attempts(&pool, &gate.id).await.unwrap();
        assert_eq!(
            kept.iter()
                .map(|k| (k.attempt, k.reason.as_str(), k.status.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                (1, "rerun", Some("succeeded")),
                (2, "rerun", Some("succeeded"))
            ]
        );
        let said = |k: &crate::models::TaskAttempt| k.output.clone().unwrap_or_default();
        assert!(said(&kept[0]).contains("alice"), "{}", said(&kept[0]));
        assert!(said(&kept[1]).contains("bob"), "{}", said(&kept[1]));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run set running again after it ended is held to its deadlines from
    /// then, as a new run is from when it is made: a rerun of a run older
    /// than its `run_timeout_secs` runs, where the deadline it was made with
    /// would have failed it at the next sweep, and its SLA alert may fire
    /// again. A rerun of every job is counted (`whole_reruns`).
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_run_set_running_again_counts_its_deadlines_from_then() {
        let (pool, path) = temp_pool().await;
        let head = "name: timed\nrun_timeout_secs: 60\ndeadline:\n  in: 30m\n";
        // `(deadline_at, alert_deadline_at, alert_fired_at, whole_reruns)`.
        let deadlines = |run: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>, i64)>(
                    "SELECT deadline_at, alert_deadline_at, alert_fired_at, whole_reruns
                     FROM workflow_runs WHERE id = ?",
                )
                .bind(run)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        // A run that ended a day ago, its deadlines long past and its alert fired.
        let ended_long_ago = |run: String, status: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query(
                    "UPDATE task_runs SET status = ?, attempt = 1, output = 'done',
                         finished_at = '2026-01-01T00:00:05Z'
                     WHERE run_id = ?",
                )
                .bind(status)
                .bind(&run)
                .execute(&pool)
                .await
                .unwrap();
                sqlx::query(
                    "UPDATE workflow_runs SET status = ?, finished_at = '2026-01-01T00:00:05Z',
                         deadline_at = '2026-01-01T00:01:00+00:00',
                         alert_deadline_at = '2026-01-01T00:30:00+00:00',
                         alert_fired_at = '2026-01-01T00:30:01+00:00'
                     WHERE id = ?",
                )
                .bind(status)
                .bind(&run)
                .execute(&pool)
                .await
                .unwrap();
            }
        };
        let after = |at: Option<String>, secs: i64| {
            let at =
                chrono::DateTime::parse_from_rfc3339(at.as_deref().expect("a deadline")).unwrap();
            let from_now = at.with_timezone(&chrono::Utc) - chrono::Utc::now();
            assert!(
                from_now > chrono::TimeDelta::seconds(secs - 10)
                    && from_now <= chrono::TimeDelta::seconds(secs),
                "{at} is not {secs} s from now"
            );
        };

        let (run, a, _) = chain_run(&pool, head).await;
        ended_long_ago(run.clone(), "succeeded").await;
        assert_eq!(rerun_all(&pool, &run).await.unwrap(), Some(2));
        assert!(
            cancel_overdue_runs(&pool).await.unwrap().is_empty(),
            "not overdue"
        );
        assert_eq!(
            get_run(&pool, &run)
                .await
                .unwrap()
                .unwrap()
                .status
                .to_string(),
            "running"
        );
        let (deadline, alert, fired, whole) = deadlines(run.clone()).await;
        after(deadline, 60);
        after(alert, 1800);
        assert_eq!((fired, whole), (None, 1));

        // The other three that set an ended run running again do the same.
        ended_long_ago(run.clone(), "succeeded").await;
        assert_eq!(
            clear_task_with_downstream(&pool, &run, &a).await.unwrap(),
            Some(2)
        );
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty());
        let (deadline, _, fired, whole) = deadlines(run.clone()).await;
        after(deadline, 60);
        assert_eq!(
            (fired, whole),
            (None, 1),
            "a clear is no rerun of every job"
        );
        ended_long_ago(run.clone(), "failed").await;
        assert_eq!(rerun_from_failed(&pool, &run).await.unwrap(), Some(2));
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty());
        after(deadlines(run.clone()).await.0, 60);
        ended_long_ago(run.clone(), "failed").await;
        assert!(retry_task_from_ui(&pool, &a).await.unwrap());
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty());
        after(deadlines(run.clone()).await.0, 60);

        // A run with no deadline has none after it either.
        let (plain, _, _) = chain_run(&pool, "name: plain\n").await;
        ended_long_ago(plain.clone(), "succeeded").await;
        sqlx::query(
            "UPDATE workflow_runs SET deadline_at = NULL, alert_deadline_at = NULL WHERE id = ?",
        )
        .bind(&plain)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(rerun_all(&pool, &plain).await.unwrap(), Some(2));
        assert_eq!(deadlines(plain).await, (None, None, None, 1));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A task put back is parked again from nothing. What parked it last
    /// time (a child run, a time, a dataset) goes with the rest of its
    /// result: left on the row, the sweeps would resolve the task from its
    /// last child run or its last deadline in the time between its claim and
    /// its park, and release what depends on it.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_task_put_back_is_parked_again_from_nothing() {
        let (pool, path) = temp_pool().await;
        // The child run the task waited on last time, which succeeded.
        let (child, _, _) = chain_run(&pool, "name: child\n").await;
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded', output = 'old' WHERE id = ?")
            .bind(&child)
            .execute(&pool)
            .await
            .unwrap();
        let (run, a, b) = chain_run(&pool, "name: parent\n").await;
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', attempt = 1, output = 'done',
                 sub_run_id = CASE WHEN id = ?2 THEN ?3 END,
                 wake_at = CASE WHEN id = ?2 THEN '2026-01-01T00:00:00Z' END,
                 wait_dataset = CASE WHEN id = ?2 THEN 'orders' END,
                 wait_dataset_cursor = CASE WHEN id = ?2 THEN 0 END,
                 cache_hit = 1
             WHERE run_id = ?1",
        )
        .bind(&run)
        .bind(&a)
        .bind(&child)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
            .bind(&run)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(rerun_all(&pool, &run).await.unwrap(), Some(2));
        let parks: (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            i64,
        ) = sqlx::query_as(
            "SELECT sub_run_id, wake_at, wait_dataset, wait_dataset_cursor, cache_hit
             FROM task_runs WHERE id = ?",
        )
        .bind(&a)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(parks, (None, None, None, None, 0));

        // Claimed, and not yet parked again: no sweep takes it for parked.
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        assert!(claimed.iter().any(|t| t.id == a), "a runs again");
        assert!(reconcile_subworkflows(&pool).await.unwrap().done.is_empty());
        assert!(reconcile_waits(&pool).await.unwrap().done.is_empty());
        assert!(reconcile_dataset_waits(&pool)
            .await
            .unwrap()
            .done
            .is_empty());
        assert_eq!(round(&pool, &a).await.2, "running");
        assert_eq!(
            round(&pool, &b).await.2,
            "pending",
            "nothing it holds back is released"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

/// A run that waits for its workflow's cap (`when_full`), and one that takes
/// another's place.
#[cfg(test)]
mod wait_tests {
    use super::*;
    use crate::dag::DagGraph;
    use crate::models::{trigger_kind, MaxActiveRunsReached, RunTrigger};

    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_wait_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    /// A workflow `deploy` of one task, with `head` above its tasks.
    fn spec(head: &str) -> String {
        format!("name: deploy\n{head}tasks:\n  - name: a\n    command: [\"true\"]\n")
    }

    async fn make(pool: &Pool, yaml: &str) -> String {
        create_run(pool, &DagGraph::from_yaml(yaml).unwrap(), yaml).await.unwrap()
    }

    async fn make_for(pool: &Pool, yaml: &str, by: &RunTrigger) -> String {
        create_run_triggered(pool, &DagGraph::from_yaml(yaml).unwrap(), yaml, by).await.unwrap()
    }

    /// What a run reads: its status, whether it waits, and what it is to be.
    async fn reads(pool: &Pool, run: &str) -> (String, bool, Option<String>) {
        let (status, queued, admitted, stopping): (String, Option<String>, Option<String>, Option<String>) =
            sqlx::query_as("SELECT status, queued_at, admitted_at, stopping_as FROM workflow_runs WHERE id = ?")
                .bind(run)
                .fetch_one(pool)
                .await
                .unwrap();
        let waits = status == "running" && queued.is_some() && admitted.is_none();
        (status, waits, stopping)
    }

    fn waiting() -> (String, bool, Option<String>) {
        ("running".to_string(), true, None)
    }

    fn running() -> (String, bool, Option<String>) {
        ("running".to_string(), false, None)
    }

    fn ended(status: &str) -> (String, bool, Option<String>) {
        (status.to_string(), false, None)
    }

    /// A run's tasks, by name: each one's status and what it still waits for.
    async fn tasks(pool: &Pool, run: &str) -> Vec<(String, String, i64)> {
        sqlx::query_as("SELECT name, status, remaining_deps FROM task_runs WHERE run_id = ? ORDER BY name")
            .bind(run)
            .fetch_all(pool)
            .await
            .unwrap()
    }

    /// End a run as one whose every task succeeded.
    async fn finish(pool: &Pool, run: &str) {
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', finished_at = ?, claimed_by = NULL,
                    lease_expires_at = NULL, remaining_deps = 0
             WHERE run_id = ?",
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(run)
        .execute(pool)
        .await
        .unwrap();
        reap_completed_runs(pool).await.unwrap();
        assert_eq!(reads(pool, run).await, ended("succeeded"));
    }

    /// Say when a run began to wait, so that an order does not hang on two
    /// readings of a clock.
    async fn queued_at(pool: &Pool, run: &str, at: &str) {
        sqlx::query("UPDATE workflow_runs SET queued_at = ? WHERE id = ?")
            .bind(at)
            .bind(run)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn completed_events(pool: &Pool, run: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox WHERE run_id = ? AND event_type = 'run.completed'")
            .bind(run)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A run that arrives at a full cap is made, and waits: it reads
    /// `running`, nothing of it advances, and no deadline of its own ends it.
    /// When the run before it has ended it is let in, its roots advance, and
    /// its budget is counted from there.
    #[tokio::test]
    async fn a_run_over_its_cap_waits_and_is_let_in_when_the_one_before_has_ended() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: deploy\nmax_active_runs: 1\nwhen_full: queue\nrun_timeout_secs: 600\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
        let first = make(&pool, yaml).await;
        let second = make(&pool, yaml).await;
        assert_eq!(reads(&pool, &first).await, running(), "the first has the cap to itself");
        assert_eq!(reads(&pool, &second).await, waiting());

        // Nothing of the one that waits advances, and nothing of it is claimed.
        advance_ready_tasks(&pool).await.unwrap();
        let held = [("a".to_string(), "pending".to_string(), 1), ("b".to_string(), "pending".to_string(), 2)];
        assert_eq!(tasks(&pool, &second).await, held);
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        assert_eq!(claimed.iter().map(|t| t.run_id.as_str()).collect::<Vec<_>>(), [first.as_str()]);
        assert!(admit_waiting_runs(&pool).await.unwrap().is_empty(), "the first has not ended");

        // Its budget is for what it runs. Say it has waited an hour, with ten
        // minutes to run in: the deadline it was made with is long past, and
        // ends nothing.
        let hour_ago = chrono::Utc::now() - chrono::TimeDelta::hours(1);
        sqlx::query("UPDATE workflow_runs SET queued_at = ?, deadline_at = ? WHERE id = ?")
            .bind(hour_ago.to_rfc3339())
            .bind((hour_ago + chrono::TimeDelta::seconds(600)).to_rfc3339())
            .bind(&second)
            .execute(&pool)
            .await
            .unwrap();
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty());
        assert_eq!(reads(&pool, &second).await, waiting());

        // A third waits behind the second, and is not let in ahead of it.
        let third = make(&pool, yaml).await;
        assert_eq!(reads(&pool, &third).await, waiting());

        finish(&pool, &first).await;
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![second.clone()]);
        assert_eq!(reads(&pool, &second).await, running());
        assert_eq!(reads(&pool, &third).await, waiting());
        let deadline: Option<String> = sqlx::query_scalar("SELECT deadline_at FROM workflow_runs WHERE id = ?")
            .bind(&second)
            .fetch_one(&pool)
            .await
            .unwrap();
        let left = chrono::DateTime::parse_from_rfc3339(deadline.as_deref().unwrap()).unwrap()
            - chrono::Utc::now().fixed_offset();
        assert!(
            (540..=600).contains(&left.num_seconds()),
            "ten minutes from when it was let in, not from when it was made: {deadline:?}"
        );
        assert!(cancel_overdue_runs(&pool).await.unwrap().is_empty());
        // And it is a deadline again: past it, the run fails.
        sqlx::query("UPDATE workflow_runs SET deadline_at = '2020-01-01T00:00:00+00:00' WHERE id = ?")
            .bind(&second)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            cancel_overdue_runs(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![second.clone()]
        );
        assert_eq!(reads(&pool, &second).await, ended("failed"));
        // The third's turn has come with that.
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![third.clone()]);
        assert!(admit_waiting_runs(&pool).await.unwrap().is_empty(), "a run is let in once");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// What a run that is let in has is what a run that never waited has: its
    /// roots advance and are claimed, and what depends on them waits for them.
    #[tokio::test]
    async fn a_run_that_is_let_in_runs_as_one_that_never_waited() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: deploy\nmax_active_runs: 1\nwhen_full: queue\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
        let first = make(&pool, yaml).await;
        let second = make(&pool, yaml).await;
        finish(&pool, &first).await;
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![second.clone()]);
        assert_eq!(
            tasks(&pool, &second).await,
            [("a".to_string(), "pending".to_string(), 0), ("b".to_string(), "pending".to_string(), 1)]
        );

        advance_ready_tasks(&pool).await.unwrap();
        let a = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        assert_eq!((a.run_id.as_str(), a.name.as_str()), (second.as_str(), "a"));
        mark_task_succeeded(&pool, &a.id, "w", a.version + 1, None).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let b = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        assert_eq!(b.name, "b");
        mark_task_succeeded(&pool, &b.id, "w", b.version + 1, None).await.unwrap();
        reap_completed_runs(&pool).await.unwrap();
        assert_eq!(reads(&pool, &second).await, ended("succeeded"));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run its parameters leave no task in, that came to a full cap, waits
    /// for its turn as any other: it ends once it is let in, and not before.
    #[tokio::test]
    async fn a_run_of_no_task_at_a_full_cap_ends_once_it_is_let_in() {
        let (pool, path) = temp_pool().await;
        let busy = make(&pool, &spec("max_active_runs: 1\nwhen_full: queue\n")).await;
        let empty = make(
            &pool,
            "name: deploy\nmax_active_runs: 1\nwhen_full: queue\nparameters: { event: push }\n\
             tasks:\n  - name: a\n    command: [\"true\"]\n    when: \"{{ event }} == tag\"\n",
        )
        .await;
        assert!(tasks(&pool, &empty).await.is_empty(), "the when leaves its task out");
        assert_eq!(reads(&pool, &empty).await, waiting());

        assert!(reap_completed_runs(&pool).await.unwrap().is_empty());
        assert_eq!(reads(&pool, &empty).await, waiting(), "not ended before its turn");

        // The run before it ends (`finish` reaps it), and its turn comes.
        finish(&pool, &busy).await;
        assert_eq!(reads(&pool, &empty).await, waiting(), "until it is let in");
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![empty.clone()]);
        let ended_now = reap_completed_runs(&pool).await.unwrap();
        assert_eq!(ended_now.len(), 1);
        assert_eq!(ended_now[0].0, empty);
        assert_eq!(reads(&pool, &empty).await, ended("succeeded"));

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Runs are let in in the order they came, as many as the cap has room
    /// for, and each key of a workflow has a queue of its own.
    #[tokio::test]
    async fn runs_are_let_in_oldest_first_and_as_many_as_the_cap_has_room_for() {
        let (pool, path) = temp_pool().await;
        let yaml = spec("max_active_runs: 2\nwhen_full: queue\n");
        let one = make(&pool, &yaml).await;
        let two = make(&pool, &yaml).await;
        let mut behind = Vec::new();
        // Made newest first, and said to have come in the other order.
        for at in ["2026-10-13T10:00:03+00:00", "2026-10-13T10:00:02+00:00", "2026-10-13T10:00:01+00:00"] {
            let run = make(&pool, &yaml).await;
            assert_eq!(reads(&pool, &run).await, waiting());
            queued_at(&pool, &run, at).await;
            behind.push(run);
        }
        // Another key of the same workflow is not behind any of them.
        let keyed = spec("max_active_runs: 2\nwhen_full: queue\nconcurrency_key: other\n");
        assert_eq!(reads(&pool, &make(&pool, &keyed).await).await, running());

        assert!(admit_waiting_runs(&pool).await.unwrap().is_empty(), "the cap is full");
        finish(&pool, &one).await;
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![behind[2].clone()], "room for one");
        finish(&pool, &two).await;
        finish(&pool, &behind[2]).await;
        assert_eq!(
            admit_waiting_runs(&pool).await.unwrap(),
            vec![behind[1].clone(), behind[0].clone()],
            "room for two, in the order they came"
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A workflow that says nothing of a full cap refuses, as it did, and one
    /// that queues refuses once as many runs wait as may. A run that waits is
    /// in the way of a workflow that refuses, as a run that runs is.
    #[tokio::test]
    async fn a_queue_is_no_longer_than_it_may_be() {
        let (pool, path) = temp_pool().await;
        let queues = spec("max_active_runs: 1\nwhen_full: queue\n");
        let refuses = spec("max_active_runs: 1\n");
        let refused = |e: anyhow::Error| e.downcast_ref::<MaxActiveRunsReached>().cloned().expect("a typed refusal");
        let dag = |yaml: &str| DagGraph::from_yaml(yaml).unwrap();

        let first = make(&pool, &queues).await;
        let may = crate::dag::max_waiting_runs();
        for _ in 0..may {
            make(&pool, &queues).await;
        }
        let full = refused(create_run(&pool, &dag(&queues), &queues).await.unwrap_err());
        assert_eq!((full.active, full.waiting, full.max), (may + 1, may, 1));
        assert!(full.to_string().contains("no more may wait"), "{full}");
        let said = refused(create_run(&pool, &dag(&refuses), &refuses).await.unwrap_err());
        assert_eq!((said.active, said.waiting), (may + 1, 0), "a workflow that refuses says only that");
        assert!(!said.to_string().contains("wait"), "{said}");

        // With the first ended and nothing let in yet, the ones that wait are
        // still before a new run: it is not made ahead of them.
        finish(&pool, &first).await;
        assert!(create_run(&pool, &dag(&refuses), &refuses).await.is_err());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run that supersedes cancels the runs of its key before it. One whose
    /// work is still running holds the key until that is gone, and the new
    /// run waits for it. One that waits ends at once, and is told as ended.
    #[tokio::test]
    async fn a_run_that_supersedes_cancels_what_is_before_it_and_waits_for_its_work_to_stop() {
        let (pool, path) = temp_pool().await;
        let yaml = spec("max_active_runs: 1\nwhen_full: supersede\n");
        let first = make(&pool, &yaml).await;
        advance_ready_tasks(&pool).await.unwrap();
        let held = claim_ready(&pool, "w", 10).await.unwrap().remove(0);

        // The first has work a worker holds: it is asked to stop, and the
        // second waits for it.
        let second = make(&pool, &yaml).await;
        assert_eq!(reads(&pool, &first).await, ("running".to_string(), false, Some("cancelled".to_string())));
        assert_eq!(reads(&pool, &second).await, waiting());
        let output: Option<String> = sqlx::query_scalar("SELECT output FROM workflow_runs WHERE id = ?")
            .bind(&first)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(output, Some(format!("superseded by run {second}")));
        assert_eq!(completed_events(&pool, &first).await, 0, "it has not ended");

        // A third takes the second's place too. The second had started
        // nothing: it is over here, and its end is told.
        let third = make(&pool, &yaml).await;
        assert_eq!(reads(&pool, &second).await, ended("cancelled"));
        assert_eq!(tasks(&pool, &second).await, [("a".to_string(), "cancelled".to_string(), 1)]);
        assert_eq!(completed_events(&pool, &second).await, 1);
        assert_eq!(reads(&pool, &third).await, waiting());
        assert!(admit_waiting_runs(&pool).await.unwrap().is_empty(), "the first's work is still there");

        // The first's work is gone: it ends, and the third is let in.
        assert!(settle_stopped_task(&pool, &held.id, "w", held.version + 1).await.unwrap());
        assert_eq!(reads(&pool, &first).await, ended("cancelled"));
        assert_eq!(completed_events(&pool, &first).await, 1);
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![third.clone()]);
        assert_eq!(reads(&pool, &third).await, running());

        // Nothing of the third is held by a worker: a fourth ends it where it
        // is made, and waits for nothing.
        advance_ready_tasks(&pool).await.unwrap();
        let fourth = make(&pool, &yaml).await;
        assert_eq!(reads(&pool, &third).await, ended("cancelled"));
        assert_eq!(reads(&pool, &fourth).await, running());
        assert_eq!(tasks(&pool, &fourth).await, [("a".to_string(), "pending".to_string(), 0)]);

        // Another key's run is none of its business.
        let other = spec("max_active_runs: 1\nwhen_full: supersede\nconcurrency_key: other\n");
        let beside = make(&pool, &other).await;
        make(&pool, &yaml).await;
        assert_eq!(reads(&pool, &beside).await, running());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run that is not trusted writes its own key, so it takes the place of
    /// its own source's runs alone: another pull request's stays, and is in
    /// its way. A trusted run of the same workflow and key is counted apart
    /// from both.
    #[tokio::test]
    async fn a_run_that_is_not_trusted_supersedes_its_own_source_s_runs_alone() {
        let (pool, path) = temp_pool().await;
        let yaml = spec("max_active_runs: 1\nwhen_full: supersede\nconcurrency_key: pr-7\n");
        let from = |pr: i64, trust: &str| RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            trust: Some(trust.to_string()),
            forge_host: Some("github.com".to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some(format!("refs/pull/{pr}/head")),
            pr: Some(pr),
            ..Default::default()
        };

        let theirs = make_for(&pool, &yaml, &from(7, "untrusted")).await;
        // Pull request 8 names 7's key. It cancels nothing of 7's, and waits.
        let intruder = make_for(&pool, &yaml, &from(8, "untrusted")).await;
        assert_eq!(reads(&pool, &theirs).await, running());
        assert_eq!(reads(&pool, &intruder).await, waiting());

        // A trusted run of the same name and key is no confined run's to
        // cancel, and none of them is in its way.
        let trusted = make_for(&pool, &yaml, &from(7, "trusted")).await;
        assert_eq!(reads(&pool, &trusted).await, running());
        assert_eq!(reads(&pool, &theirs).await, running());

        // Pull request 7's next push takes its own run's place, and leaves
        // 8's, which is before it.
        let next = make_for(&pool, &yaml, &from(7, "untrusted")).await;
        assert_eq!(reads(&pool, &theirs).await, ended("cancelled"));
        assert_eq!(reads(&pool, &intruder).await, waiting());
        assert_eq!(reads(&pool, &next).await, waiting());
        assert_eq!(reads(&pool, &trusted).await, running());
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![intruder.clone()], "oldest first");

        // The same pull request on another forge, or under another ref, is
        // another source.
        let elsewhere = RunTrigger { forge_host: Some("git.example.com".to_string()), ..from(8, "untrusted") };
        make_for(&pool, &yaml, &elsewhere).await;
        assert_eq!(reads(&pool, &intruder).await, running());

        let keys: Vec<Option<String>> =
            sqlx::query_scalar("SELECT DISTINCT concurrency_key FROM workflow_runs ORDER BY concurrency_key")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(keys, [Some("pr-7".to_string()), Some("untrusted:pr-7".to_string())]);

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run cancelled as it waits ends at once and is told as ended, and
    /// nothing is let in for it. Run again, it does not wait: it asks its key
    /// as a run that is set running again does, and its tasks have the
    /// dependencies they have.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_run_cancelled_as_it_waits_is_over_and_run_again_does_not_wait() {
        let (pool, path) = temp_pool().await;
        let yaml = spec("max_active_runs: 1\nwhen_full: queue\n");
        let first = make(&pool, &yaml).await;
        let second = make(&pool, &yaml).await;

        let did = cancel_run(&pool, &second).await.unwrap().expect("a run that waits is one to cancel");
        assert_eq!((did.cancelled, did.stopping), (1, 0));
        assert_eq!(reads(&pool, &second).await, ended("cancelled"));
        assert_eq!(completed_events(&pool, &second).await, 1);
        assert!(admit_waiting_runs(&pool).await.unwrap().is_empty());

        // Its key is full while the first runs, and it is refused as any run
        // set running again is: it does not go back to waiting.
        let refused = rerun_from_failed(&pool, &second).await.unwrap_err();
        assert!(refused.downcast_ref::<MaxActiveRunsReached>().is_some(), "{refused}");
        assert_eq!(reads(&pool, &second).await, ended("cancelled"));

        finish(&pool, &first).await;
        assert_eq!(rerun_from_failed(&pool, &second).await.unwrap(), Some(1));
        assert_eq!(reads(&pool, &second).await, running(), "it is let in by being run again");
        assert_eq!(tasks(&pool, &second).await, [("a".to_string(), "pending".to_string(), 0)]);
        assert!(admit_waiting_runs(&pool).await.unwrap().is_empty(), "and nothing is taken off it twice");
        assert_eq!(tasks(&pool, &second).await, [("a".to_string(), "pending".to_string(), 0)]);
        advance_ready_tasks(&pool).await.unwrap();
        assert_eq!(claim_ready(&pool, "w", 10).await.unwrap().len(), 1);

        // A run that waited and was let in keeps the record that it did.
        let third = make(&pool, &yaml).await;
        assert_eq!(reads(&pool, &third).await, waiting());
        finish(&pool, &second).await;
        assert_eq!(admit_waiting_runs(&pool).await.unwrap(), vec![third.clone()]);
        let read = get_run(&pool, &third).await.unwrap().unwrap();
        assert!(read.queued_at.is_some() && read.admitted_at.is_some() && !read.waits());

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

// ── Path caches ──────────────────────────────────────────────────────────────
// The same as the Postgres backend's; see there.

pub async fn path_cache_entries(
    pool: &Pool,
    scope: &str,
    prefix: &str,
    version: &str,
    limit: i64,
) -> Result<Vec<crate::models::PathCacheEntry>> {
    let rows: Vec<(String, String, String, Option<i64>)> = sqlx::query_as(
        "SELECT id, cache_key, object_key, size_bytes FROM path_caches
         WHERE scope = ? AND prefix = ? AND version = ? AND state = 'ready'
         ORDER BY created_at DESC, id DESC LIMIT ?",
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

pub async fn path_cache_used(pool: &Pool, id: &str) -> Result<()> {
    sqlx::query("UPDATE path_caches SET last_used_at = ? WHERE id = ? AND state = 'ready'")
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn path_cache_offered(pool: &Pool, entry: &crate::models::NewPathCache) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO path_caches
           (id, repo, scope, prefix, version, object_key, state, task_id, created_at, last_used_at)
         VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
    )
    .bind(&entry.id)
    .bind(&entry.repo)
    .bind(&entry.scope)
    .bind(&entry.prefix)
    .bind(&entry.version)
    .bind(&entry.object_key)
    .bind(&entry.task_id)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(())
}

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
                OR p.created_at < ?)
         ORDER BY p.created_at LIMIT ?",
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

pub async fn path_cache_ready(
    pool: &Pool,
    id: &str,
    cache_key: &str,
    size_bytes: i64,
) -> Result<Option<Vec<String>>> {
    let mut tx = pool.begin().await?;
    let row: Option<(String, String, String)> = sqlx::query_as(
        "SELECT scope, prefix, version FROM path_caches WHERE id = ? AND state = 'pending'",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((scope, prefix, version)) = row else {
        return Ok(None);
    };
    let replaced: Vec<(String,)> = sqlx::query_as(
        "DELETE FROM path_caches
         WHERE scope = ? AND prefix = ? AND version = ? AND cache_key = ? AND state = 'ready'
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
        "UPDATE path_caches SET state = 'ready', cache_key = ?, size_bytes = ?, task_id = NULL,
           created_at = ?, last_used_at = ?
         WHERE id = ?",
    )
    .bind(cache_key)
    .bind(size_bytes)
    .bind(&now)
    .bind(&now)
    .bind(id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(replaced.into_iter().map(|(k,)| k).collect()))
}

pub async fn path_cache_drop(pool: &Pool, id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("DELETE FROM path_caches WHERE id = ? RETURNING object_key")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(k,)| k))
}

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
         WHERE r.kept > ? OR r.last_used_at < ?
         ORDER BY r.last_used_at LIMIT ?",
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
         VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
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
/// no longer pending.
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
            SET state = 'ready', size_bytes = ?, sha256 = ?, expires_at = ?, created_at = ?,
                provenance = ?
          WHERE id = ? AND state = 'pending'
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
    .bind(provenance)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// A run's artifacts whose uploads are complete, oldest first.
pub async fn workflow_artifacts_of_run(
    pool: &Pool,
    run_id: &str,
) -> Result<Vec<crate::models::WorkflowArtifact>> {
    let rows: Vec<ArtifactRow> = sqlx::query_as(
        "SELECT id, artifact_id, run_id, task_id, name, object_key, size_bytes, sha256,
                created_at, expires_at
           FROM workflow_artifacts WHERE run_id = ? AND state = 'ready'
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
        sqlx::query_scalar("SELECT scheduled_at FROM task_runs WHERE id = ?")
            .bind(task_id)
            .fetch_optional(pool)
            .await?;
    Ok(at.flatten())
}

/// Drop an artifact's row: its object key, to delete its bytes by; `None`
/// when another had dropped it.
pub async fn workflow_artifact_drop(pool: &Pool, id: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("DELETE FROM workflow_artifacts WHERE id = ? RETURNING object_key")
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
        "SELECT id, object_key FROM workflow_artifacts WHERE expires_at < ?
          ORDER BY expires_at LIMIT ?",
    )
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

#[cfg(test)]
mod workflow_artifact_tests {
    use super::*;
    use crate::models::WorkflowArtifact;

    fn artifact(
        id: &str,
        number: i64,
        run: &str,
        name: &str,
        expires_at: &str,
    ) -> WorkflowArtifact {
        WorkflowArtifact {
            id: id.into(),
            artifact_id: number,
            run_id: run.into(),
            task_id: "t".into(),
            name: name.into(),
            object_key: format!("{run}/{id}"),
            size_bytes: None,
            sha256: None,
            created_at: "2026-10-15T00:00:00+00:00".into(),
            expires_at: expires_at.into(),
        }
    }

    #[tokio::test]
    async fn an_artifact_is_its_runs_by_name_once_complete_and_goes_when_it_expires() {
        let path = std::env::temp_dir().join(format!("dagron-wa-{}.db", uuid::Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        let later = "2099-01-01T00:00:00+00:00";
        workflow_artifact_begin(&pool, &artifact("a", 1, "r1", "dist", later))
            .await
            .unwrap();
        workflow_artifact_begin(&pool, &artifact("b", 2, "r1", "dist", later))
            .await
            .unwrap();
        workflow_artifact_begin(
            &pool,
            &artifact("c", 3, "r2", "dist", "2000-01-01T00:00:00+00:00"),
        )
        .await
        .unwrap();
        // Pending: not listed.
        assert!(workflow_artifacts_of_run(&pool, "r1")
            .await
            .unwrap()
            .is_empty());
        assert!(workflow_artifact_ready(
            &pool,
            "a",
            10,
            "sha256:aa",
            later,
            Some("{\"payload\":\"\"}")
        )
        .await
        .unwrap());
        // Its provenance, where one was signed, with it.
        let provenance: Option<String> =
            sqlx::query_scalar("SELECT provenance FROM workflow_artifacts WHERE id = 'a'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(provenance.as_deref(), Some("{\"payload\":\"\"}"));
        // A second of the name in the run is not made ready; one of another
        // run is its own.
        assert!(
            !workflow_artifact_ready(&pool, "b", 11, "sha256:bb", later, None)
                .await
                .unwrap()
        );
        assert!(
            !workflow_artifact_ready(&pool, "a", 10, "sha256:aa", later, None)
                .await
                .unwrap()
        );
        let listed = workflow_artifacts_of_run(&pool, "r1").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            (
                listed[0].artifact_id,
                listed[0].size_bytes,
                listed[0].sha256.as_deref()
            ),
            (1, Some(10), Some("sha256:aa"))
        );
        // What expired goes, pending or not.
        let now = chrono::Utc::now().to_rfc3339();
        assert_eq!(
            workflow_artifacts_expired(&pool, &now, 10).await.unwrap(),
            [("c".to_string(), "r2/c".to_string())]
        );
        assert_eq!(
            workflow_artifact_drop(&pool, "c").await.unwrap().as_deref(),
            Some("r2/c")
        );
        assert_eq!(workflow_artifact_drop(&pool, "c").await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(test)]
mod path_cache_tests {
    use super::*;

    async fn pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("dagron-path-caches-{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    fn offered(id: &str, repo: &str, scope: &str) -> crate::models::NewPathCache {
        crate::models::NewPathCache {
            id: id.into(),
            repo: repo.into(),
            scope: scope.into(),
            prefix: "cargo".into(),
            version: "v1".into(),
            object_key: format!("obj-{id}"),
            task_id: format!("task-{id}"),
        }
    }

    #[tokio::test]
    async fn an_entry_is_read_once_ready_and_takes_the_place_of_its_key() {
        let (pool, path) = pool().await;
        path_cache_offered(&pool, &offered("a", "r", "r@main"))
            .await
            .unwrap();
        // Pending: nothing to restore yet, and its task's row is gone, so the
        // sweep sees it.
        assert!(path_cache_entries(&pool, "r@main", "cargo", "v1", 5)
            .await
            .unwrap()
            .is_empty());
        let settled = path_caches_settled(&pool, "0", 10).await.unwrap();
        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].task_status, None);
        assert_eq!(
            path_cache_ready(&pool, "a", "cargo-k1", 10).await.unwrap(),
            Some(vec![])
        );
        // Twice: the second finds it no longer pending.
        assert_eq!(
            path_cache_ready(&pool, "a", "cargo-k1", 10).await.unwrap(),
            None
        );
        let got = path_cache_entries(&pool, "r@main", "cargo", "v1", 5)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].cache_key, "cargo-k1");
        // The same key saved again takes its place, and says what to forget.
        path_cache_offered(&pool, &offered("b", "r", "r@main"))
            .await
            .unwrap();
        assert_eq!(
            path_cache_ready(&pool, "b", "cargo-k1", 12).await.unwrap(),
            Some(vec!["obj-a".to_string()])
        );
        let got = path_cache_entries(&pool, "r@main", "cargo", "v1", 5)
            .await
            .unwrap();
        assert_eq!(
            got.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            vec!["b"]
        );
        // Another scope, prefix or version reads nothing of it.
        assert!(path_cache_entries(&pool, "r@dev", "cargo", "v1", 5)
            .await
            .unwrap()
            .is_empty());
        assert!(path_cache_entries(&pool, "r@main", "npm", "v1", 5)
            .await
            .unwrap()
            .is_empty());
        assert!(path_cache_entries(&pool, "r@main", "cargo", "v2", 5)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            path_cache_drop(&pool, "b").await.unwrap(),
            Some("obj-b".to_string())
        );
        assert_eq!(path_cache_drop(&pool, "b").await.unwrap(), None);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn the_least_recently_used_past_a_repositorys_quota_are_evicted() {
        let (pool, path) = pool().await;
        for (id, scope, size) in [
            ("old", "r@main", 40),
            ("mid", "r@dev", 40),
            ("new", "r@main", 40),
        ] {
            path_cache_offered(&pool, &offered(id, "r", scope))
                .await
                .unwrap();
            path_cache_ready(&pool, id, &format!("cargo-{id}"), size)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        path_cache_offered(&pool, &offered("other", "s", "s@main"))
            .await
            .unwrap();
        path_cache_ready(&pool, "other", "cargo-other", 100)
            .await
            .unwrap();
        // 100 bytes a repository: r's newest two fit, its oldest does not;
        // s's one entry fits alone.
        assert_eq!(
            path_cache_victims(&pool, 100, "0", 10).await.unwrap(),
            vec!["old".to_string()]
        );
        // Reading the oldest makes it the newest, and the next one goes.
        path_cache_used(&pool, "old").await.unwrap();
        assert_eq!(
            path_cache_victims(&pool, 100, "0", 10).await.unwrap(),
            vec!["mid".to_string()]
        );
        // Unused since a time after all of them: every one goes.
        let later = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        assert_eq!(
            path_cache_victims(&pool, i64::MAX, &later, 10)
                .await
                .unwrap()
                .len(),
            4
        );
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

// ── Task identity ────────────────────────────────────────────────────────────
// The same as the Postgres backend's; see there.

pub async fn oidc_request_offered(pool: &Pool, request: &crate::models::OidcRequest) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM oidc_requests WHERE task_id = ? OR expires_at < ?")
        .bind(&request.task_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO oidc_requests
           (token_sha256, run_id, task_id, task_name, attempt, audiences, environment, expires_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
             WHERE r.token_sha256 = ?",
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

#[cfg(test)]
mod oidc_request_tests {
    use super::*;

    fn request(token: &str, task: &str, expires_at: &str) -> crate::models::OidcRequest {
        crate::models::OidcRequest {
            token_sha256: token.into(),
            run_id: "run".into(),
            task_id: task.into(),
            task_name: "deploy".into(),
            attempt: 1,
            audiences: vec!["sts.amazonaws.com".into()],
            environment: Some("production".into()),
            expires_at: expires_at.into(),
        }
    }

    #[tokio::test]
    async fn a_task_has_the_credential_of_its_latest_attempt_and_none_past_its_time() {
        let path = std::env::temp_dir().join(format!("dagron-oidc-{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        let later = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        oidc_request_offered(&pool, &request("a", "t1", &later))
            .await
            .unwrap();
        let (got, status) = oidc_request(&pool, "a").await.unwrap().unwrap();
        assert_eq!(got, request("a", "t1", &later));
        assert_eq!(status, None, "no task row here");
        // A second attempt's credential takes the place of the first's.
        oidc_request_offered(&pool, &request("b", "t1", &later))
            .await
            .unwrap();
        assert!(oidc_request(&pool, "a").await.unwrap().is_none());
        assert!(oidc_request(&pool, "b").await.unwrap().is_some());
        // Another task's past its time goes when the next is written.
        let earlier = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        oidc_request_offered(&pool, &request("c", "t2", &earlier))
            .await
            .unwrap();
        oidc_request_offered(&pool, &request("d", "t3", &later))
            .await
            .unwrap();
        assert!(oidc_request(&pool, "c").await.unwrap().is_none());
        assert!(oidc_request(&pool, "zzz").await.unwrap().is_none());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

// ── Remote runner sessions ────────────────────────────────────────────────────
//
// Runners dial dagron-api, which is Postgres's alone, so a SQLite engine never
// has a session or a runner's result. These are its twins of the engine's two
// calls (see db/postgres.rs), the same statements, so that the engine is one
// loop on either backend.

/// How long an engine that took a runner's result has to apply it before
/// another may take it.
const RUNNER_RESULT_RETAKE_SECS: i64 = 60;

/// Take back the tasks of sessions whose lease has lapsed. See the Postgres
/// twin.
pub async fn recover_expired_sessions(pool: &Pool) -> Result<u64> {
    let now = chrono::Utc::now().to_rfc3339();
    // Most sweeps find no lapsed session (and a SQLite engine never has one):
    // a read, and nothing written.
    let any: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM runner_sessions WHERE lease_expires_at < ?1)",
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
                 AND s.lease_expires_at < ?1
           )
           AND NOT EXISTS (SELECT 1 FROM runner_results r WHERE r.task_id = task_runs.id)";
    let requeued = sqlx::query(&format!(
        "UPDATE task_runs
         SET status = 'ready', claimed_by = NULL, lease_expires_at = NULL, session_id = NULL
         WHERE {HELD_BY_EXPIRED} AND stop_requested_at IS NULL"
    ))
    .bind(&now)
    .execute(pool)
    .await?
    .rows_affected();
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
        "DELETE FROM runner_sessions
         WHERE lease_expires_at < ?1
           AND NOT EXISTS (
               SELECT 1 FROM task_runs t
               WHERE t.session_id = runner_sessions.id AND t.status = 'running'
                 AND t.claimed_by = runner_sessions.holder
           )",
    )
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(requeued + cancelled)
}

/// Take up to `limit` of what remote runners said of their tasks. See the
/// Postgres twin.
pub async fn take_runner_results(
    pool: &Pool,
    taker: &str,
    limit: i64,
) -> Result<Vec<crate::models::RunnerResult>> {
    let now_dt = chrono::Utc::now();
    let stale = (now_dt - chrono::TimeDelta::seconds(RUNNER_RESULT_RETAKE_SECS)).to_rfc3339();
    let mut tx = pool.begin().await?;
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT task_id FROM runner_results
         WHERE taken_at IS NULL OR taken_at < ?1
         ORDER BY created_at
         LIMIT ?2",
    )
    .bind(&stale)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    let mut taken = Vec::with_capacity(ids.len());
    for id in &ids {
        #[allow(clippy::type_complexity)]
        let row: Option<(
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
            "UPDATE runner_results SET taken_by = ?1, taken_at = ?2
             WHERE task_id = ?3 AND (taken_at IS NULL OR taken_at < ?4)
             RETURNING task_id, fence, claimed_by, status, exit_code, output,
                       (SELECT run_id FROM task_runs WHERE id = ?3),
                       (SELECT attempt FROM task_runs WHERE id = ?3),
                       (SELECT input FROM task_runs WHERE id = ?3)",
        )
        .bind(taker)
        .bind(now_dt.to_rfc3339())
        .bind(id)
        .bind(&stale)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((task_id, fence, claimed_by, outcome, exit_code, output, run_id, attempt, input)) =
            row
        {
            taken.push(crate::models::RunnerResult {
                task_id,
                claimed_by,
                fence,
                outcome,
                exit_code,
                output,
                run_id,
                attempt: attempt.unwrap_or(1).saturating_sub(1).max(0),
                input,
            });
        }
    }
    tx.commit().await?;
    Ok(taken)
}

/// Drop runners' results an engine has applied, each by its task and fence.
pub async fn drop_runner_results(pool: &Pool, applied: &[(String, i64)]) -> Result<()> {
    for (task_id, fence) in applied {
        sqlx::query("DELETE FROM runner_results WHERE task_id = ?1 AND fence = ?2")
            .bind(task_id)
            .bind(fence)
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// The engine's two calls for remote runners, on SQLite: the same statements
/// as Postgres's, so that one loop runs on either. A SQLite engine never has a
/// session of its own; these stand one in by hand.
#[cfg(test)]
mod runner_session_tests {
    use super::*;
    use crate::dag::DagGraph;

    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_sessions_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    /// A ready task claimed as a session claims it: under the session's holder,
    /// pointing at the session. Returns its id and fence.
    async fn held_by_session(pool: &Pool, session: &str, lease: &str) -> (String, i64) {
        let yaml = "name: remote\ntasks:\n  - name: t\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        create_run(pool, &dag, yaml).await.unwrap();
        advance_ready_tasks(pool).await.unwrap();
        let holder = format!("runner:r1/{session}");
        let claimed = claim_ready(pool, &holder, 1).await.unwrap();
        let task = &claimed[0];
        sqlx::query(
            "INSERT INTO runner_sessions
                 (id, runner_id, holder, labels, capacity, protocol, secrets, lease_expires_at, started_at)
             VALUES (?1, 'r1', ?2, 'default', 1, 0, 'server', ?3, ?3)",
        )
        .bind(session)
        .bind(&holder)
        .bind(lease)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("UPDATE task_runs SET session_id = ?1 WHERE id = ?2")
            .bind(session)
            .bind(&task.id)
            .execute(pool)
            .await
            .unwrap();
        (task.id.clone(), task.version + 1)
    }

    async fn status(pool: &Pool, id: &str) -> (String, Option<String>) {
        sqlx::query_as("SELECT status, session_id FROM task_runs WHERE id = ?")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_expired_session_s_task_is_taken_back_and_a_live_one_s_is_left() {
        let (pool, path) = temp_pool().await;
        let past = "2000-01-01T00:00:00+00:00";
        let future = "2999-01-01T00:00:00+00:00";
        let (expired, _) = held_by_session(&pool, "s-expired", past).await;
        let (live, _) = held_by_session(&pool, "s-live", future).await;
        // The engine's own sweep passes a session's task by, however old the
        // row's lease.
        sqlx::query("UPDATE task_runs SET lease_expires_at = ?1")
            .bind(past)
            .execute(&pool)
            .await
            .unwrap();
        recover_expired_leases(&pool).await.unwrap();
        assert_eq!(status(&pool, &expired).await.0, "running");
        assert_eq!(recover_expired_sessions(&pool).await.unwrap(), 1);
        assert_eq!(status(&pool, &expired).await, ("ready".to_string(), None));
        assert_eq!(status(&pool, &live).await.0, "running");
        let left: Vec<String> = sqlx::query_scalar("SELECT id FROM runner_sessions ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(left, ["s-live"], "the expired session, holding nothing, is dropped");
        // A claim of the engine's own clears what a session left on the row.
        let claimed = claim_ready(&pool, "engine", 10).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(status(&pool, &expired).await, ("running".to_string(), None));
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_runner_s_word_is_taken_once_and_left_by_the_sweep_until_it_is_applied() {
        let (pool, path) = temp_pool().await;
        let past = "2000-01-01T00:00:00+00:00";
        let (task, fence) = held_by_session(&pool, "s1", past).await;
        sqlx::query(
            "INSERT INTO runner_results (task_id, fence, claimed_by, status, output, created_at)
             VALUES (?1, ?2, 'runner:r1/s1', 'succeeded', 'done', ?3)",
        )
        .bind(&task)
        .bind(fence)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
        // Expired, but its word waits: the sweep leaves it for the word.
        assert_eq!(recover_expired_sessions(&pool).await.unwrap(), 0);
        assert_eq!(status(&pool, &task).await.0, "running");
        let taken = take_runner_results(&pool, "engine-1", 10).await.unwrap();
        assert_eq!(taken.len(), 1);
        let word = &taken[0];
        assert_eq!((word.fence, word.attempt, word.outcome.as_str()), (fence, 0, "succeeded"));
        assert_eq!(word.claimed_by, "runner:r1/s1");
        assert!(word.run_id.is_some() && word.input.is_some());
        assert!(take_runner_results(&pool, "engine-2", 10).await.unwrap().is_empty());
        // Applied, as the engine's loop applies it.
        assert!(mark_task_succeeded(&pool, &task, "runner:r1/s1", fence, word.output.clone())
            .await
            .unwrap());
        drop_runner_results(&pool, &[(task.clone(), fence)]).await.unwrap();
        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runner_results")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(left, 0);
        recover_expired_sessions(&pool).await.unwrap();
        assert_eq!(status(&pool, &task).await.0, "succeeded");
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

/// The plans SQLite takes for the sweeps' reads, and for the reads beside
/// them, on the SQLite this build bundles and with no statistics, which is
/// how the engine runs (migrations/076).
#[cfg(test)]
mod sweep_plan_tests {
    use super::*;

    /// migrations/076's indexes.
    const SWEEP_INDEXES: [&str; 7] = [
        "idx_task_runs_running_lease_expires_at",
        "idx_task_runs_fanout_parked",
        "idx_task_runs_wake_due",
        "idx_task_runs_wait_url_due",
        "idx_task_runs_wait_dataset_parked",
        "idx_task_runs_sub_run_parked",
        "idx_task_runs_external_running",
    ];

    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_plans_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    /// Each line of the plan SQLite takes for `sql`. Unbound parameters are
    /// NULL, which a plan does not depend on. Prepared afresh each time: a
    /// plan is made when its statement is prepared, and a cached one would
    /// keep it across a change of the indexes.
    async fn plan(pool: &Pool, sql: &str) -> Vec<String> {
        let rows: Vec<(i64, i64, i64, String)> =
            sqlx::query_as(&format!("EXPLAIN QUERY PLAN {sql}"))
                .persistent(false)
                .fetch_all(pool)
                .await
                .unwrap();
        rows.into_iter().map(|(_, _, _, detail)| detail).collect()
    }

    /// Whether a line of `plan` reads `index`, by its whole name: one name
    /// can begin another's.
    fn reads(plan: &[String], index: &str) -> bool {
        plan.iter()
            .any(|line| line.split_whitespace().any(|word| word == index))
    }

    /// Each sweep's read takes the index made for it, and none reads
    /// idx_task_runs_status, which they all took before: every running row,
    /// and a sort of them where the sweep orders.
    #[tokio::test]
    async fn sweep_reads_plan_on_their_partial_indexes() {
        let (pool, path) = temp_pool().await;
        for (sweep, sql, index) in [
            (
                "leases",
                EXPIRED_LEASES,
                "idx_task_runs_running_lease_expires_at",
            ),
            ("fanouts", PARKED_FANOUTS, "idx_task_runs_fanout_parked"),
            ("waits", DUE_WAITS, "idx_task_runs_wake_due"),
            ("url waits", DUE_URL_WAITS, "idx_task_runs_wait_url_due"),
            (
                "dataset waits",
                PARKED_DATASET_WAITS,
                "idx_task_runs_wait_dataset_parked",
            ),
            (
                "subworkflows",
                PARKED_SUBWORKFLOWS,
                "idx_task_runs_sub_run_parked",
            ),
            (
                "external polls",
                DUE_EXTERNAL_POLLS,
                "idx_task_runs_external_running",
            ),
            (
                "teardowns",
                DUE_TEARDOWNS,
                "idx_task_runs_running_lease_expires_at",
            ),
        ] {
            let plan = plan(&pool, sql).await;
            assert!(reads(&plan, index), "{sweep} should read {index}: {plan:?}");
            assert!(!reads(&plan, "idx_task_runs_status"), "{sweep}: {plan:?}");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The reads beside the sweeps' plan as they did before migrations/076:
    /// an index led by `status` and of running rows can match any read of
    /// running rows, and must not take one from the index it has.
    #[tokio::test]
    async fn plans_that_must_not_move() {
        let (pool, path) = temp_pool().await;
        let reads_of = [
            ("the claim", READY_CANDIDATES, None),
            (
                "a sub-workflow's parent",
                SUB_RUN_PARENT,
                Some("idx_task_runs_sub_run_id"),
            ),
            (
                "a run's running tasks",
                "SELECT name, status FROM task_runs WHERE run_id = ? AND status = 'running'",
                Some("idx_task_runs_run_id_name"),
            ),
            (
                "a pool's running tasks",
                POOL_RUNNING_COUNT,
                Some("idx_task_runs_pool_running"),
            ),
            (
                "the external cancels",
                DUE_EXTERNAL_CANCELS,
                Some("idx_task_runs_external"),
            ),
        ];
        let mut with_074 = Vec::new();
        for (what, sql, index) in reads_of {
            let plan = plan(&pool, sql).await;
            if let Some(index) = index {
                assert!(reads(&plan, index), "{what} should read {index}: {plan:?}");
            }
            with_074.push(plan);
        }
        for index in SWEEP_INDEXES {
            sqlx::query(&format!("DROP INDEX {index}"))
                .execute(&pool)
                .await
                .unwrap();
        }
        for ((what, sql, _), with_074) in reads_of.into_iter().zip(with_074) {
            assert_eq!(plan(&pool, sql).await, with_074, "{what}'s plan moved");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// migrations/080's index of the running runs, by their key.
    const IN_FLIGHT_INDEX: &str = "idx_workflow_runs_in_flight";

    /// The enterprise build's own indexes of every run, led by `status`.
    const STATUS_INDEXES: [&str; 2] = [
        "idx_workflow_runs_status_created_at",
        "idx_workflow_runs_status_finished_at",
    ];

    /// migrations/078's six, led by `status`, and 079's index of the runs
    /// the reap may end.
    const RUN_SWEEP_INDEXES: [&str; 7] = [
        "idx_workflow_runs_deadline",
        "idx_workflow_runs_alert_deadline",
        "idx_workflow_runs_stopping",
        "idx_workflow_runs_waiting",
        "idx_workflow_runs_start_pending",
        "idx_workflow_runs_completion_pending",
        "idx_workflow_runs_reapable",
    ];

    /// The six as 010, 015, 053, 054, 055 and 060 made them, before 078.
    const OLD_RUN_SWEEP_INDEXES: [&str; 6] = [
        "CREATE INDEX idx_workflow_runs_deadline ON workflow_runs(deadline_at)
             WHERE deadline_at IS NOT NULL AND status = 'running'",
        "CREATE INDEX idx_workflow_runs_alert_deadline ON workflow_runs(alert_deadline_at)
             WHERE alert_deadline_at IS NOT NULL AND alert_fired_at IS NULL AND status = 'running'",
        "CREATE INDEX idx_workflow_runs_completion_pending
             ON workflow_runs (completion_pending_at) WHERE completion_pending_at IS NOT NULL",
        "CREATE INDEX idx_workflow_runs_start_pending
             ON workflow_runs (start_pending_at) WHERE start_pending_at IS NOT NULL",
        "CREATE INDEX idx_workflow_runs_stopping ON workflow_runs (id) WHERE stopping_as IS NOT NULL",
        "CREATE INDEX idx_workflow_runs_waiting ON workflow_runs (id)
             WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running'",
    ];

    /// The line of `plan` that reads `table`, by its alias or its name.
    fn line_of<'a>(plan: &'a [String], table: &str) -> &'a str {
        plan.iter()
            .find(|line| {
                let mut words = line.split_whitespace();
                matches!(words.next(), Some("SEARCH" | "SCAN")) && words.next() == Some(table)
            })
            .unwrap_or_else(|| panic!("no line reads {table}: {plan:?}"))
    }

    async fn exec(pool: &Pool, sql: &str) {
        sqlx::query(sql)
            .persistent(false)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Drop each index and make it again from its own text, so that it is
    /// the newest of the table's: with no statistics, SQLite takes the
    /// newest of two loops that tie.
    async fn make_newest(pool: &Pool, indexes: &[&str]) {
        for index in indexes {
            let sql: String = sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            )
            .bind(index)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("{index}: {e}"));
            exec(pool, &format!("DROP INDEX {index}")).await;
            exec(pool, &sql).await;
        }
    }

    /// Indexes of every run led by `status`, the newest of the table's: on
    /// the enterprise build its own, made again; elsewhere two of the same
    /// shape, by hand.
    /// Their names.
    async fn status_indexes_newest(pool: &Pool) -> [&'static str; 2] {
        if cfg!(feature = "enterprise") {
            make_newest(pool, &STATUS_INDEXES).await;
            STATUS_INDEXES
        } else {
            exec(
                pool,
                "CREATE INDEX idx_test_status_created ON workflow_runs (status, created_at)",
            )
            .await;
            exec(
                pool,
                "CREATE INDEX idx_test_status_finished ON workflow_runs (status, finished_at)",
            )
            .await;
            ["idx_test_status_created", "idx_test_status_finished"]
        }
    }

    /// The run-level sweeps and the reads beside them that read the same
    /// indexes: each with its index, and the term of its second key that
    /// the index's line carries.
    fn run_sweeps() -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
        #[allow(unused_mut)]
        let mut sweeps = vec![
            (
                "deadlines",
                OVERDUE_RUNS,
                "idx_workflow_runs_deadline",
                "deadline_at<?",
            ),
            (
                "alerts",
                DUE_DEADLINE_ALERTS,
                "idx_workflow_runs_alert_deadline",
                "alert_deadline_at<?",
            ),
            (
                "stopped runs",
                STRANDED_STOPPED_RUNS,
                "idx_workflow_runs_stopping",
                "stopping_as>?",
            ),
            (
                "admissions",
                WAITING_RUN_KEYS,
                "idx_workflow_runs_waiting",
                "queued_at>?",
            ),
            (
                "pending starts",
                PENDING_STARTS,
                "idx_workflow_runs_start_pending",
                "start_pending_at>?",
            ),
            (
                "pending completions",
                PENDING_COMPLETIONS,
                "idx_workflow_runs_completion_pending",
                "completion_pending_at>?",
            ),
            // Its plan, on 3.46.0, reads the runs that wait in the order of
            // `queued_at` and the definition by its key, with no sort, on
            // either build.
            (
                "admission's next runs",
                ADMISSION_NEXT,
                "idx_workflow_runs_waiting",
                "queued_at>?",
            ),
        ];
        #[cfg(feature = "ops")]
        sweeps.push((
            "runs waiting",
            RUNS_WAITING,
            "idx_workflow_runs_waiting",
            "queued_at>?",
        ));
        sweeps
    }

    /// Each run sweep reads its index, by `status` and its second key, and
    /// reads none of `rivals`, nor migrations/080's index of the running
    /// runs.
    async fn assert_run_sweeps(pool: &Pool, rivals: &[&str], when: &str) {
        for (sweep, sql, index, term) in run_sweeps() {
            let plan = plan(pool, sql).await;
            assert!(
                reads(&plan, index),
                "{when}: {sweep} should read {index}: {plan:?}"
            );
            let line = plan.iter().find(|l| l.contains(index)).unwrap();
            assert!(
                line.contains("status=?") && line.contains(term),
                "{when}: {sweep} should read {index} by status and {term}: {plan:?}"
            );
            for rival in rivals.iter().chain(&[IN_FLIGHT_INDEX]) {
                assert!(
                    !reads(&plan, rival),
                    "{when}: {sweep} reads {rival}: {plan:?}"
                );
            }
            if sweep == "pending starts" {
                assert!(
                    !plan.iter().any(|l| l.starts_with("USE TEMP B-TREE")),
                    "{when}: {sweep} sorts: {plan:?}"
                );
            }
        }
    }

    /// The reap's reads: the slice by idx_workflow_runs_reapable from its
    /// cursor, in its order; the named reads by the keys they are handed.
    /// None reads `rivals`, but that the slice may read `slice_tie`, an
    /// index of running runs led by (status, id) that its loop uses by the
    /// same terms: the same walk, in the same order.
    async fn assert_reap_reads(pool: &Pool, rivals: &[&str], slice_tie: Option<&str>, when: &str) {
        let slice = plan(pool, &reap_slice_sql()).await;
        let line = line_of(&slice, "workflow_runs");
        let tied = slice_tie.is_some_and(|tie| reads(&slice, tie));
        assert!(
            (line.contains("USING INDEX idx_workflow_runs_reapable") || tied)
                && line.contains("status=? AND id>?"),
            "{when}: the slice: {slice:?}"
        );
        assert!(
            !slice.iter().any(|l| l.starts_with("USE TEMP B-TREE")),
            "{when}: the slice sorts: {slice:?}"
        );
        assert!(
            !reads(&slice, "sqlite_autoindex_workflow_runs_1"),
            "{when}: {slice:?}"
        );
        for (what, sql) in [
            ("tasks", NAMED_REAP_TASKS),
            ("empty runs", NAMED_REAP_EMPTY),
        ] {
            let plan = plan(pool, sql).await;
            assert!(
                plan[0].starts_with("SCAN j "),
                "{when}: the named {what}: {plan:?}"
            );
            // By its key: through the primary key, or an index with `id` in
            // its keys, which holds one row of each id.
            let wr = line_of(&plan, "wr");
            assert!(
                wr.starts_with("SEARCH") && wr.contains("id=?"),
                "{when}: the named {what} should find each run by its key: {plan:?}"
            );
            let tr = line_of(&plan, "tr");
            assert!(
                tr.contains("idx_task_runs_run_id_name") || tr.contains("idx_task_runs_run_id "),
                "{when}: the named {what}: {plan:?}"
            );
            for rival in rivals.iter().chain(&[IN_FLIGHT_INDEX]) {
                assert!(
                    !plan
                        .iter()
                        .any(|l| l.contains(rival) && !l.contains("id=?")),
                    "{when}: the named {what} reads {rival}: {plan:?}"
                );
            }
        }
        for rival in rivals
            .iter()
            .chain(&[IN_FLIGHT_INDEX])
            .filter(|r| Some(**r) != slice_tie)
        {
            assert!(
                !reads(&slice, rival),
                "{when}: the slice reads {rival}: {slice:?}"
            );
        }
        // The deadline sweep carries the reapable WHERE, and keeps its own:
        // its loop uses `deadline_at` besides.
        let overdue = plan(pool, OVERDUE_RUNS).await;
        assert!(
            reads(&overdue, "idx_workflow_runs_deadline")
                && !reads(&overdue, "idx_workflow_runs_reapable"),
            "{when}: {overdue:?}"
        );
    }

    /// The run-level sweeps read the partial indexes made for each, led by
    /// `status` (migrations/078), on both builds. On the enterprise build,
    /// before 078, its own index of (status, created_at) took them from the
    /// partials, which were led by the column each compares: its loop uses
    /// `status`, as theirs did not, and it read every running run. Checked
    /// in the order of a fresh enterprise file (its own index the newest) and
    /// of an upgraded one (078 the newest).
    #[tokio::test]
    async fn workflow_runs_sweeps_keep_their_partials() {
        let (pool, path) = temp_pool().await;
        assert_run_sweeps(&pool, &STATUS_INDEXES, "as migrated").await;
        make_newest(&pool, &RUN_SWEEP_INDEXES).await;
        assert_run_sweeps(&pool, &STATUS_INDEXES, "078 the newest").await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The run sweeps and the reap's reads keep their indexes beside an
    /// index of every run led by `status`, the newer or the older: on OSS
    /// too, made by hand, so a PR's own run proves what the enterprise
    /// build's index would do.
    #[tokio::test]
    async fn run_sweeps_hold_against_a_status_index_of_either_age() {
        let (pool, path) = temp_pool().await;
        let rivals = status_indexes_newest(&pool).await;
        assert_run_sweeps(&pool, &rivals, "a status index the newest").await;
        assert_reap_reads(&pool, &rivals, None, "a status index the newest").await;
        make_newest(&pool, &RUN_SWEEP_INDEXES).await;
        assert_run_sweeps(&pool, &rivals, "ours the newest").await;
        assert_reap_reads(&pool, &rivals, None, "ours the newest").await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The live reads (`/metrics`' live tier and dagron-api's health count)
    /// read idx_workflow_runs_live, by `status`, and the recent runs by
    /// `created_at` besides. On the enterprise build they tie with its own
    /// index of (status, created_at), with the same terms and the same rows,
    /// and the newer of the two is taken: so either, there, until the live index is made the
    /// newest. `also` are the indexes that may tie so, each read with the
    /// live read's own terms.
    #[cfg(feature = "ops")]
    async fn assert_live_reads(pool: &Pool, also: &[&str], when: &str) {
        for (what, sql, term) in [
            (
                "the live runs by status",
                live_runs_by_status_sql(),
                "status=?",
            ),
            (
                "the recent live runs",
                recent_runs_sql(true),
                "created_at>?",
            ),
            (
                "the health count",
                crate::db::LIVE_RUNS_COUNT.to_string(),
                "status=?",
            ),
        ] {
            let plan = plan(pool, &sql).await;
            let line = line_of(
                &plan,
                if sql.contains(" wr") {
                    "wr"
                } else {
                    "workflow_runs"
                },
            );
            let index = line
                .split_whitespace()
                .skip_while(|w| *w != "INDEX")
                .nth(1)
                .unwrap_or("");
            assert!(
                index == "idx_workflow_runs_live" || also.contains(&index),
                "{when}: {what} should read idx_workflow_runs_live: {plan:?}"
            );
            assert!(
                line.contains("status=?") && line.contains(term),
                "{when}: {what}: {plan:?}"
            );
        }
        // What has finished is not the live index's to read.
        for (what, sql) in [
            ("the recent finished runs", recent_runs_sql(false)),
            ("the finished runs by status", finished_runs_by_status_sql()),
        ] {
            let plan = plan(pool, &sql).await;
            assert!(
                !reads(&plan, "idx_workflow_runs_live"),
                "{when}: {what}: {plan:?}"
            );
        }
        // The list as LIVE_RUN_STATUSES would write it were it reordered:
        // the index's WHERE is no longer carried as written, so it is not
        // read. This is what holds the three to the index's text.
        let reversed = plan(
            pool,
            "SELECT COUNT(*) FROM workflow_runs WHERE status IN ('running', 'pending')",
        )
        .await;
        assert!(
            !reads(&reversed, "idx_workflow_runs_live"),
            "{when}: {reversed:?}"
        );
    }

    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn live_reads_plan_on_the_live_index() {
        let (pool, path) = temp_pool().await;
        let tie: &[&str] = if cfg!(feature = "enterprise") {
            &STATUS_INDEXES[..1]
        } else {
            &[]
        };
        assert_live_reads(&pool, tie, "as migrated").await;
        make_newest(&pool, &["idx_workflow_runs_live"]).await;
        assert_live_reads(&pool, &[], "the live index the newest").await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The reap reads the runs it is handed by their keys and the slice by
    /// its own index, on both builds and beside an index led by `status` of
    /// either age.
    #[tokio::test]
    async fn reap_reads_plan_on_their_keys() {
        let (pool, path) = temp_pool().await;
        assert_reap_reads(&pool, &STATUS_INDEXES, None, "as migrated").await;
        let rivals = status_indexes_newest(&pool).await;
        assert_reap_reads(&pool, &rivals, None, "a status index the newest").await;
        make_newest(&pool, &["idx_workflow_runs_reapable"]).await;
        assert_reap_reads(&pool, &rivals, None, "the reapable index the newest").await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The reads of runs beside the sweeps: each its name and its text.
    fn reads_beside_the_sweeps() -> Vec<(&'static str, String)> {
        #[allow(unused_mut)]
        let mut reads_of: Vec<(&str, String)> = vec![
            ("the runs in flight", RUNS_IN_FLIGHT.to_string()),
            ("the runs superseded", SUPERSEDED_RUNS.to_string()),
            (
                "the reclass near a deadline",
                RECLASS_NEAR_DEADLINE.to_string(),
            ),
            ("the count of active runs", ACTIVE_RUNS_COUNT.to_string()),
            ("the drift mark", CLOCK_DRIFTED.to_string()),
            ("the parked sub-workflows", PARKED_SUBWORKFLOWS.to_string()),
        ];
        #[cfg(feature = "ops")]
        {
            let [deps, tasks, runs] = gc_old_runs_sql();
            reads_of.extend([
                // Planned with the status it is bound with: SQLite plans a
                // bound statement again where a partial index could serve it.
                (
                    "the running runs listed",
                    list_runs_sql(true).replace("wr.status = ?", "wr.status = 'running'"),
                ),
                ("the finished runs by status", finished_runs_by_status_sql()),
                ("the recent finished runs", recent_runs_sql(false)),
                ("the runs to archive", ARCHIVABLE_RUNS.to_string()),
                ("GC's dependencies", deps),
                ("GC's tasks", tasks),
                ("GC's runs", runs),
            ]);
        }
        #[cfg(feature = "enterprise")]
        reads_of.extend([
            ("the incomplete runs", INCOMPLETE_RUNS.to_string()),
            (
                "the failed runs to rerun",
                FAILED_RUNS_FOR_RERUN.to_string(),
            ),
        ]);
        reads_of
    }

    /// The reads of runs beside the sweeps plan as they did before
    /// migrations/078 and 079, on either build: each is planned with them,
    /// and again with the six as they were and without the live and
    /// reapable indexes. What 078 moves on purpose (the sweeps, the waiting
    /// gauge, admission's next runs, the live reads) is held by the tests
    /// above instead. 080, there in both, is held by `what_080_moves` and
    /// `in_flight_reads_plan_on_their_index`.
    #[tokio::test]
    async fn run_plans_that_must_not_move() {
        let (pool, path) = temp_pool().await;
        let reads_of = reads_beside_the_sweeps();
        let mut with_078 = Vec::new();
        for (_, sql) in &reads_of {
            with_078.push(plan(&pool, sql).await);
        }
        for index in RUN_SWEEP_INDEXES.iter().chain(&["idx_workflow_runs_live"]) {
            exec(&pool, &format!("DROP INDEX {index}")).await;
        }
        for sql in OLD_RUN_SWEEP_INDEXES {
            exec(&pool, sql).await;
        }
        for ((what, sql), with_078) in reads_of.iter().zip(with_078) {
            assert_eq!(plan(&pool, sql).await, with_078, "{what}'s plan moved");
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The index a line of a plan reads, by its name: the word after
    /// `INDEX`, or "" for a line that reads none.
    fn index_of(line: &str) -> &str {
        line.split_whitespace()
            .skip_while(|w| *w != "INDEX")
            .nth(1)
            .unwrap_or("")
    }

    /// The indexes of `workflow_runs` a read by `status = 'running'` alone
    /// may use by that term alone: led by `status`, with no WHERE or with
    /// that one. Each is a read of the running runs, the same rows.
    async fn running_by_status_alone(pool: &Pool) -> Vec<String> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'index' AND tbl_name = 'workflow_runs' AND sql IS NOT NULL",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        rows.into_iter()
            .filter(|(_, sql)| {
                let sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
                let (head, partial) = sql.split_once(" WHERE ").unwrap_or((&sql, ""));
                let columns = head
                    .split_once(" ON workflow_runs")
                    .map_or("", |(_, c)| c.trim());
                columns.starts_with("(status") && matches!(partial, "" | "status = 'running'")
            })
            .map(|(name, _)| name)
            .collect()
    }

    /// The reads 080 is for. The run cap's count and supersede's read take
    /// idx_workflow_runs_in_flight by `status` and `concurrency_key`: a
    /// strict superset of the terms of any index of `status` alone, so no
    /// such index takes them, of either age; supersede's order is the
    /// index's but for its last term. The count of active runs reads the
    /// running runs by `status`, through 080's or through an index that
    /// ties with it (`running_by_status_alone`): on the enterprise build,
    /// its own.
    async fn assert_in_flight_reads(pool: &Pool, when: &str) {
        for (what, sql) in [
            ("the runs in flight", RUNS_IN_FLIGHT),
            ("the runs superseded", SUPERSEDED_RUNS),
        ] {
            let plan = plan(pool, sql).await;
            let line = line_of(&plan, "wr");
            assert!(
                line.contains(&format!(
                    "USING INDEX {IN_FLIGHT_INDEX} (status=? AND concurrency_key=?)"
                )),
                "{when}: {what} should read {IN_FLIGHT_INDEX} by status and key: {plan:?}"
            );
            assert!(
                !plan.iter().any(|l| l == "USE TEMP B-TREE FOR ORDER BY"),
                "{when}: {what} sorts: {plan:?}"
            );
        }
        let ties = running_by_status_alone(pool).await;
        let plan = plan(pool, ACTIVE_RUNS_COUNT).await;
        let line = line_of(&plan, "workflow_runs");
        assert!(
            line.starts_with("SEARCH workflow_runs USING COVERING INDEX")
                && line.ends_with("(status=?)")
                && ties.iter().any(|t| t == index_of(line)),
            "{when}: the count of active runs should read the running runs alone ({ties:?}): {plan:?}"
        );
        assert!(
            ties.iter().any(|t| t == IN_FLIGHT_INDEX),
            "{when}: {IN_FLIGHT_INDEX} reads by status alone: {ties:?}"
        );
    }

    /// The run cap's count, supersede's read and the count of active runs
    /// read the running runs (migrations/080), where on OSS each read every
    /// run kept: as migrated (on the enterprise build, its own index the
    /// newest, a fresh file's order), beside an index of every run led by
    /// `status` the newest, and with 080 the newest (an upgraded enterprise
    /// file's order). And with 080 the newest,
    /// the sweeps, the reap and the live reads keep theirs.
    #[tokio::test]
    async fn in_flight_reads_plan_on_their_index() {
        let (pool, path) = temp_pool().await;
        assert_in_flight_reads(&pool, "as migrated").await;
        let rivals = status_indexes_newest(&pool).await;
        assert_in_flight_reads(&pool, "a status index the newest").await;
        make_newest(&pool, &[IN_FLIGHT_INDEX]).await;
        assert_in_flight_reads(&pool, "080 the newest").await;
        assert_run_sweeps(&pool, &rivals, "080 the newest").await;
        assert_reap_reads(&pool, &rivals, None, "080 the newest").await;
        #[cfg(feature = "ops")]
        {
            // The live index the newest, as an upgraded file has it, so
            // that a tie with a rival led by `status` is not what is read.
            make_newest(&pool, &["idx_workflow_runs_live"]).await;
            make_newest(&pool, &[IN_FLIGHT_INDEX]).await;
            assert_live_reads(&pool, &[], "080 the newest").await;
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// What 080 moves among the reads beside the sweeps, on either build and
    /// in either order: each plans as it did without 080, or reads the
    /// running runs by its index where it read them by an index of `status`
    /// alone or read every run. Nothing else in its plan moves. On OSS that
    /// is the reclass near a deadline, the drift mark and the running runs
    /// listed; on the enterprise build, where 080 is the newer of it and the
    /// build's own index, those it ties with that index for.
    #[tokio::test]
    async fn what_080_moves() {
        let (pool, path) = temp_pool().await;
        let ours = [
            "the runs in flight",
            "the runs superseded",
            "the count of active runs",
        ];
        let reads_of: Vec<_> = reads_beside_the_sweeps()
            .into_iter()
            .filter(|(what, _)| !ours.contains(what))
            .collect();
        for when in ["as migrated", "080 the newest"] {
            if when == "080 the newest" {
                make_newest(&pool, &[IN_FLIGHT_INDEX]).await;
            }
            let ties = running_by_status_alone(&pool).await;
            let mut with = Vec::new();
            for (_, sql) in &reads_of {
                with.push(plan(&pool, sql).await);
            }
            let made: String = sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            )
            .bind(IN_FLIGHT_INDEX)
            .fetch_one(&pool)
            .await
            .unwrap();
            exec(&pool, &format!("DROP INDEX {IN_FLIGHT_INDEX}")).await;
            let mut moved = Vec::new();
            for ((what, sql), with) in reads_of.iter().zip(with) {
                let without = plan(&pool, sql).await;
                assert_eq!(
                    with.len(),
                    without.len(),
                    "{when}: {what}: {with:?} {without:?}"
                );
                for (now, before) in with.iter().zip(&without) {
                    if now == before {
                        continue;
                    }
                    let table = now.split_whitespace().nth(1).unwrap_or("");
                    let read_all = *before == format!("SCAN {table}");
                    let by_status = before.ends_with("(status=?)")
                        && ties.iter().any(|t| t == index_of(before));
                    assert!(
                        now.ends_with(&format!("INDEX {IN_FLIGHT_INDEX} (status=?)"))
                            && before.split_whitespace().nth(1) == Some(table)
                            && (read_all || by_status),
                        "{when}: {what}: `{before}` became `{now}`"
                    );
                    moved.push(*what);
                }
            }
            if cfg!(not(feature = "enterprise")) && when == "as migrated" {
                let mut want = vec!["the reclass near a deadline", "the drift mark"];
                if cfg!(feature = "ops") {
                    want.push("the running runs listed");
                }
                assert_eq!(moved, want, "{when}: what 080 moves on OSS");
            }
            exec(&pool, &made).await;
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An index added later, of a shape that could take these reads, takes
    /// none of them from its own: each made the newest in turn, and every
    /// plan the tests above hold planned again. One led by `status`, with no
    /// WHERE or the live index's own, may take a live read from it: its loop
    /// has the same terms, ties, and the newer index is taken, which is the
    /// same read of the same rows (`assert_live_reads` holds the terms).
    /// That alone is let be, as is the enterprise build's own index, which
    /// ties with the live index as migrated (`live_reads_plan_on_the_live_index`).
    #[tokio::test]
    async fn run_plans_hold_against_new_indexes() {
        let (pool, path) = temp_pool().await;
        for (name, columns, partial) in [
            ("idx_test_status", "(status)", ""),
            ("idx_test_status_created", "(status, created_at)", ""),
            ("idx_test_status_id", "(status, id)", ""),
            ("idx_test_running", "(status)", " WHERE status = 'running'"),
            (
                "idx_test_live",
                "(status, created_at)",
                " WHERE status IN ('pending', 'running')",
            ),
            (
                "idx_test_running_id",
                "(status, id)",
                " WHERE status = 'running' AND stopping_as IS NULL",
            ),
            (
                "idx_test_reapable_id",
                "(id)",
                &*format!(" WHERE {REAPABLE}"),
            ),
            ("idx_test_created", "(created_at)", ""),
            ("idx_test_deadline", "(deadline_at)", ""),
        ] {
            exec(
                &pool,
                &format!("CREATE INDEX {name} ON workflow_runs {columns}{partial}"),
            )
            .await;
            let when = format!("beside {name}");
            let rivals = [name, STATUS_INDEXES[0], STATUS_INDEXES[1]];
            assert_run_sweeps(&pool, &rivals, &when).await;
            let tie = columns.starts_with("(status, id)").then_some(name);
            assert_reap_reads(&pool, &rivals, tie, &when).await;
            assert_in_flight_reads(&pool, &when).await;
            #[cfg(feature = "ops")]
            {
                // Only a rival led by `status`, of no WHERE or the live
                // index's own, may take a live read, and on the enterprise
                // build its own index, which ties as migrated.
                let live: String = sqlx::query_scalar(
                    "SELECT sql FROM sqlite_master WHERE name = 'idx_workflow_runs_live'",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                let words = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
                let live = words(&live);
                let live_where = live.split_once(" WHERE ").expect("a partial index").1;
                let mut tie = Vec::new();
                if columns.starts_with("(status")
                    && (partial.is_empty()
                        || partial.strip_prefix(" WHERE ").map(words).as_deref()
                            == Some(live_where))
                {
                    tie.push(name);
                }
                if cfg!(feature = "enterprise") {
                    tie.push(STATUS_INDEXES[0]);
                }
                assert_live_reads(&pool, &tie, &when).await;
            }
            exec(&pool, &format!("DROP INDEX {name}")).await;
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The indexes of `workflow_runs`, as written. Each plan the tests
    /// above hold was checked beside these: one added is to be checked
    /// beside them too.
    #[tokio::test]
    async fn workflow_runs_indexes_are_known() {
        let (pool, path) = temp_pool().await;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'index' AND tbl_name = 'workflow_runs' AND sql IS NOT NULL
             ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let found: Vec<(String, String)> = rows
            .into_iter()
            .map(|(name, sql)| (name, sql.split_whitespace().collect::<Vec<_>>().join(" ")))
            .collect();
        #[allow(unused_mut)]
        let mut known = vec![
            ("idx_runs_untriaged", "CREATE INDEX idx_runs_untriaged ON workflow_runs(status, created_at) WHERE triage_state IS NULL"),
            ("idx_workflow_runs_alert_deadline", "CREATE INDEX idx_workflow_runs_alert_deadline ON workflow_runs (status, alert_deadline_at) WHERE alert_deadline_at IS NOT NULL AND alert_fired_at IS NULL AND status = 'running'"),
            ("idx_workflow_runs_completion_pending", "CREATE INDEX idx_workflow_runs_completion_pending ON workflow_runs (status, completion_pending_at) WHERE completion_pending_at IS NOT NULL"),
            ("idx_workflow_runs_concurrency_key", "CREATE INDEX idx_workflow_runs_concurrency_key ON workflow_runs (concurrency_key) WHERE status = 'running' AND concurrency_key IS NOT NULL"),
            ("idx_workflow_runs_deadline", "CREATE INDEX idx_workflow_runs_deadline ON workflow_runs (status, deadline_at) WHERE deadline_at IS NOT NULL AND status = 'running'"),
            ("idx_workflow_runs_in_flight", "CREATE INDEX idx_workflow_runs_in_flight ON workflow_runs (status, concurrency_key, created_at) WHERE status = 'running'"),
            ("idx_workflow_runs_live", "CREATE INDEX idx_workflow_runs_live ON workflow_runs (status, created_at) WHERE status IN ('pending', 'running')"),
            ("idx_workflow_runs_reapable", "CREATE INDEX idx_workflow_runs_reapable ON workflow_runs (status, id) WHERE status = 'running' AND stopping_as IS NULL AND (queued_at IS NULL OR admitted_at IS NOT NULL)"),
            ("idx_workflow_runs_schedule", "CREATE INDEX idx_workflow_runs_schedule ON workflow_runs(schedule_id) WHERE schedule_id IS NOT NULL"),
            ("idx_workflow_runs_start_pending", "CREATE INDEX idx_workflow_runs_start_pending ON workflow_runs (status, start_pending_at) WHERE start_pending_at IS NOT NULL"),
            ("idx_workflow_runs_stopping", "CREATE INDEX idx_workflow_runs_stopping ON workflow_runs (status, stopping_as) WHERE stopping_as IS NOT NULL"),
            ("idx_workflow_runs_waiting", "CREATE INDEX idx_workflow_runs_waiting ON workflow_runs (status, queued_at, id) WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running'"),
        ];
        #[cfg(feature = "enterprise")]
        known.extend([
            ("idx_workflow_runs_status_created_at", "CREATE INDEX idx_workflow_runs_status_created_at ON workflow_runs (status, created_at)"),
            ("idx_workflow_runs_status_finished_at", "CREATE INDEX idx_workflow_runs_status_finished_at ON workflow_runs (status, finished_at)"),
        ]);
        known.sort();
        let known: Vec<(String, String)> = known
            .into_iter()
            .map(|(n, s)| (n.to_string(), s.to_string()))
            .collect();
        assert_eq!(
            found, known,
            "a new index on workflow_runs: re-run the plan tests above with it in place, and add it here"
        );
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A file migrated before 078 and 079, on the enterprise build with its
    /// own migrations applied beside it, takes them at its next open: sqlx
    /// applies a version that is missing beneath one applied
    /// (`ignore_missing`), as 076 and 077 reached enterprise files. Then
    /// 078's indexes are the newest of the table's, as on any upgraded
    /// enterprise file, and the plans hold.
    #[tokio::test]
    async fn a_file_from_before_078_takes_078_and_079() {
        let (pool, path) = temp_pool().await;
        for index in RUN_SWEEP_INDEXES.iter().chain(&["idx_workflow_runs_live"]) {
            exec(&pool, &format!("DROP INDEX {index}")).await;
        }
        for sql in OLD_RUN_SWEEP_INDEXES {
            exec(&pool, sql).await;
        }
        for trigger in [
            "run_wakes_task_ended",
            "run_wakes_task_made_over",
            "run_wakes_task_gone",
            "run_wakes_run_made",
            "run_wakes_run_changed",
        ] {
            exec(&pool, &format!("DROP TRIGGER {trigger}")).await;
        }
        exec(&pool, "DROP TABLE run_wakes").await;
        exec(
            &pool,
            "DELETE FROM _sqlx_migrations WHERE version IN (78, 79)",
        )
        .await;
        pool.close().await;

        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        let applied: Vec<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version >= 76 AND success ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut want = vec![76, 77, 78, 79, 80, 81];
        if cfg!(feature = "enterprise") {
            want.extend([900, 902, 903]);
        }
        assert_eq!(applied, want);
        let triggers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'run_wakes_%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(triggers, 5);
        assert_run_sweeps(&pool, &STATUS_INDEXES, "upgraded").await;
        assert_reap_reads(&pool, &STATUS_INDEXES, None, "upgraded").await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A file migrated before 080 and 081, on the enterprise build with its
    /// own migrations applied beside it, takes them at its next open, as 078
    /// and 079 reached such files. 080's index is then the newer of it and
    /// the build's own, and the plans hold.
    #[tokio::test]
    async fn a_file_from_before_080_takes_080_and_081() {
        let (pool, path) = temp_pool().await;
        exec(&pool, &format!("DROP INDEX {IN_FLIGHT_INDEX}")).await;
        for trigger in [
            "run_changes_run_made",
            "run_changes_run_moved",
            "run_changes_task_made",
            "run_changes_task_moved",
            "run_changes_task_gone",
            "run_changes_bounded",
        ] {
            exec(&pool, &format!("DROP TRIGGER {trigger}")).await;
        }
        exec(&pool, "DROP TABLE run_changes").await;
        exec(
            &pool,
            "DELETE FROM _sqlx_migrations WHERE version IN (80, 81)",
        )
        .await;
        pool.close().await;

        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        let applied: Vec<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE version >= 78 AND success ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut want = vec![78, 79, 80, 81];
        if cfg!(feature = "enterprise") {
            want.extend([900, 902, 903]);
        }
        assert_eq!(applied, want);
        let triggers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'run_changes_%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(triggers, 6);
        assert_in_flight_reads(&pool, "upgraded").await;
        assert_run_sweeps(&pool, &STATUS_INDEXES, "upgraded").await;
        assert_reap_reads(&pool, &STATUS_INDEXES, None, "upgraded").await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

/// The sweeps' reads: the slots, which SQLite grants all, the claim of the
/// HTTP sensors, the reads that leave out what has nothing due, the batches,
/// the set-based clear of the dataset triggers, and the gauges in two tiers.
#[cfg(test)]
// `crate::env_lock` is held across awaits on purpose, as the fan-out tests
// above hold it: no test that lowers DAGRON_MAX_TASKS_PER_RUN runs meanwhile.
#[allow(clippy::await_holding_lock)]
mod sweep_tests {
    use super::*;

    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_sweeps_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    /// SQLite is one engine: every slot asked for is granted, at turn 0 and
    /// with no cursor, and there is nothing to write back.
    #[tokio::test]
    async fn sweep_slots_on_sqlite_grant_every_name() {
        let (pool, path) = temp_pool().await;
        let slots = claim_sweep_slots(&pool, "e1", &[("leases", 0.45), ("waits", 0.45)])
            .await
            .unwrap();
        let names: Vec<&str> = slots.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["leases", "waits"]);
        assert!(slots.iter().all(|s| s.turn == 0 && s.cursor.is_none()));
        // A second engine would be granted them too: there is none.
        let again = claim_sweep_slots(&pool, "e2", &[("leases", 0.45)])
            .await
            .unwrap();
        assert_eq!(again.len(), 1);
        assert!(claim_sweep_slots(&pool, "e1", &[])
            .await
            .unwrap()
            .is_empty());
        assert!(!set_sweep_cursor(&pool, "reap", 0, Some("r1"))
            .await
            .unwrap());
        assert!(!hand_back_sweep_slot(&pool, "waits", 0).await.unwrap());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A due HTTP sensor is claimed by one sweep: a second sweep of the same
    /// moment takes nothing, its `next_poll_at` has moved to the claim's end,
    /// and once that has passed it is due again.
    #[tokio::test]
    async fn a_due_url_wait_is_claimed_by_exactly_one_sweep() {
        let (pool, path) = temp_pool().await;
        let url = "https://example.com/ready";
        let yaml = format!(
            "name: p\ntasks:\n  - {{ name: probe, type: wait, wait: {{ url: \"{url}\" }} }}\n"
        );
        let dag = DagGraph::from_yaml(&yaml).unwrap();
        create_run(&pool, &dag, &yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let probe = claim_ready(&pool, "w", 10).await.unwrap().remove(0);
        let due = "2026-01-01T00:00:00+00:00";
        let parked = park_wait_url(&pool, &probe.id, probe.version + 1, url, due).await;
        assert!(parked.unwrap());

        let first = claim_due_url_waits(&pool, 32, 30).await.unwrap();
        assert_eq!(
            first,
            vec![(probe.id.clone(), url.to_string(), due.to_string())]
        );
        assert!(
            claim_due_url_waits(&pool, 32, 30).await.unwrap().is_empty(),
            "a second sweep takes nothing: one GET per due sensor"
        );
        let until: String = sqlx::query_scalar("SELECT next_poll_at FROM task_runs WHERE id = ?")
            .bind(&probe.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(until > chrono::Utc::now().to_rfc3339(), "{until}");

        // The engine died mid-GET: once the claim's end has passed, it is due.
        sqlx::query("UPDATE task_runs SET next_poll_at = ? WHERE id = ?")
            .bind("2026-01-02T00:00:00+00:00")
            .bind(&probe.id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(claim_due_url_waits(&pool, 32, 30).await.unwrap().len(), 1);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The barriers [`PARKED_FANOUTS`] reads, as the sweep binds it.
    async fn barriers_read(pool: &Pool) -> Vec<String> {
        let rows: Vec<ParkedFanout> = sqlx::query_as(PARKED_FANOUTS)
            .bind(crate::db::SWEEP_BATCH)
            .fetch_all(pool)
            .await
            .unwrap();
        rows.into_iter().map(|row| row.0).collect()
    }

    /// The instances of a barrier: `(id, status, version)`, by name.
    async fn instances(pool: &Pool, barrier: &str) -> Vec<(String, String, i64)> {
        sqlx::query_as(
            "SELECT id, status, version FROM task_runs WHERE fanout_parent = ? ORDER BY name",
        )
        .bind(barrier)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    fn expanded(barrier: &str, instances: usize) -> (String, crate::models::FanoutOutcome) {
        let outcome = crate::models::FanoutOutcome::Expanded { instances };
        (barrier.to_string(), outcome)
    }

    /// A barrier with no instances is read and expanded once; one whose
    /// instances of its generation still run is not read at all, and its
    /// rows are left as they were; once they have all ended it is read, and
    /// joins once.
    #[tokio::test]
    async fn a_barrier_whose_instances_still_run_is_not_read() {
        // `reconcile_fanouts` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path, _run, barrier) = super::tests::parked_fanout(r#"["a","b"]"#).await;

        assert_eq!(barriers_read(&pool).await, vec![barrier.clone()]);
        let pass = reconcile_fanouts(&pool).await.unwrap();
        assert!(!pass.full);
        assert_eq!(pass.done, vec![expanded(&barrier, 2)]);
        assert!(barriers_read(&pool).await.is_empty(), "expanded once");

        let claimed = claim_ready(&pool, "w1", 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        let before = instances(&pool, &barrier).await;
        assert!(
            barriers_read(&pool).await.is_empty(),
            "one runs, one is ready"
        );
        assert!(reconcile_fanouts(&pool).await.unwrap().done.is_empty());
        assert_eq!(
            instances(&pool, &barrier).await,
            before,
            "its rows are untouched"
        );

        let fence = claimed[0].version + 1;
        mark_task_succeeded(&pool, &claimed[0].id, "w1", fence, None)
            .await
            .unwrap();
        let rest = claim_ready(&pool, "w1", 1).await.unwrap();
        assert!(barriers_read(&pool).await.is_empty(), "one still runs");
        let fence = rest[0].version + 1;
        mark_task_succeeded(&pool, &rest[0].id, "w1", fence, None)
            .await
            .unwrap();
        assert_eq!(barriers_read(&pool).await, vec![barrier.clone()]);
        let joined = crate::models::FanoutOutcome::Joined {
            succeeded: true,
            instances: 2,
        };
        assert_eq!(
            reconcile_fanouts(&pool).await.unwrap().done,
            vec![(barrier.clone(), joined)]
        );
        assert!(barriers_read(&pool).await.is_empty(), "joined once");
        assert!(reconcile_fanouts(&pool).await.unwrap().done.is_empty());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A barrier a reset has moved past its instances is read: they are of a
    /// generation before, and do not hold it. Through the reset the API makes
    /// (the producer cleared, and the barrier with it, while the instances,
    /// the barrier's dependencies, are left as they were), the sweep drops
    /// them and expands the barrier again over the producer's new output,
    /// once.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn a_barrier_reset_past_its_instances_is_cleaned_and_expanded_once() {
        let _g = crate::env_lock();
        let (pool, path, run, barrier) = super::tests::parked_fanout(r#"["a","b"]"#).await;
        let producer: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = ? AND name = 'list'")
                .bind(&run)
                .fetch_one(&pool)
                .await
                .unwrap();
        reconcile_fanouts(&pool).await.unwrap();
        let first = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(first.len(), 2);
        let stale: Vec<String> = first.iter().map(|t| t.id.clone()).collect();
        for t in &first {
            mark_task_succeeded(&pool, &t.id, "w1", t.version + 1, None)
                .await
                .unwrap();
        }
        assert_eq!(
            reconcile_fanouts(&pool).await.unwrap().done.len(),
            1,
            "joined"
        );

        assert!(clear_task_with_downstream(&pool, &run, &producer)
            .await
            .unwrap()
            .is_some());
        advance_ready_tasks(&pool).await.unwrap();
        let again = claim_ready(&pool, "w1", 10).await.unwrap();
        let p = again
            .iter()
            .find(|t| t.id == producer)
            .expect("the producer runs again");
        let output = Some(r#"["x","y","z"]"#.to_string());
        mark_task_succeeded(&pool, &p.id, "w1", p.version + 1, output)
            .await
            .unwrap();
        advance_ready_tasks(&pool).await.unwrap();

        assert_eq!(
            barriers_read(&pool).await,
            vec![barrier.clone()],
            "the instances before do not hold it"
        );
        assert_eq!(
            reconcile_fanouts(&pool).await.unwrap().done,
            vec![expanded(&barrier, 3)]
        );
        let now = instances(&pool, &barrier).await;
        assert_eq!(now.len(), 3, "{now:?}");
        assert!(now
            .iter()
            .all(|(id, status, _)| !stale.contains(id) && status == "ready"));
        assert!(barriers_read(&pool).await.is_empty());
        assert!(
            reconcile_fanouts(&pool).await.unwrap().done.is_empty(),
            "expanded once"
        );
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An instance of a generation before that still runs does not hold its
    /// barrier either: the sweep drops it and expands the barrier again,
    /// once. No reset leaves one under a parked barrier today (a reset
    /// barrier waits for its instances, its dependencies, before it parks
    /// again), but the read does not lean on that.
    #[tokio::test]
    async fn an_instance_of_a_generation_before_does_not_hold_its_barrier() {
        let _g = crate::env_lock();
        let (pool, path, _run, barrier) = super::tests::parked_fanout(r#"["a","b"]"#).await;
        reconcile_fanouts(&pool).await.unwrap();
        let running = claim_ready(&pool, "w1", 10).await.unwrap();
        assert_eq!(running.len(), 2);
        let stale: Vec<String> = running.iter().map(|t| t.id.clone()).collect();

        // The barrier's version moves on, as a reset moves it.
        sqlx::query("UPDATE task_runs SET version = version + 1 WHERE id = ?")
            .bind(&barrier)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(barriers_read(&pool).await, vec![barrier.clone()]);
        assert_eq!(
            reconcile_fanouts(&pool).await.unwrap().done,
            vec![expanded(&barrier, 2)]
        );
        let now = instances(&pool, &barrier).await;
        assert_eq!(now.len(), 2, "{now:?}");
        assert!(now
            .iter()
            .all(|(id, status, _)| !stale.contains(id) && status == "ready"));
        assert!(barriers_read(&pool).await.is_empty());
        assert!(
            reconcile_fanouts(&pool).await.unwrap().done.is_empty(),
            "expanded once"
        );
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The triggers [`PARKED_SUBWORKFLOWS`] reads, as the sweep binds it.
    async fn triggers_read(pool: &Pool) -> Vec<String> {
        let rows: Vec<(String, String, String, Option<String>)> =
            sqlx::query_as(PARKED_SUBWORKFLOWS)
                .bind(crate::db::SWEEP_BATCH)
                .fetch_all(pool)
                .await
                .unwrap();
        rows.into_iter().map(|row| row.0).collect()
    }

    /// The sub-workflow sweep reads a trigger only once its child has ended:
    /// not while the child runs, not where the child is gone (the trigger
    /// stays parked), and the trigger of an ended child is resolved once. A
    /// `repeat:` trigger is read through the same join, and re-arms rather
    /// than resolves: that case is
    /// `a_repeating_sub_workflow_trigger_runs_one_child_per_iteration`.
    #[tokio::test]
    async fn a_trigger_is_read_only_once_its_child_has_ended() {
        let (pool, path) = temp_pool().await;
        let child_yaml = "name: child\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        let parent_yaml = "name: parent\ntasks:\n  \
                           - { name: a, type: workflow, workflow: child }\n  \
                           - { name: b, type: workflow, workflow: child }\n";
        let dag = DagGraph::from_yaml(parent_yaml).unwrap();
        create_run(&pool, &dag, parent_yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 10).await.unwrap();
        let by = |name: &str| claimed.iter().find(|t| t.name == name).unwrap().clone();
        let (a, b) = (by("a"), by("b"));
        let child_dag = DagGraph::from_yaml(child_yaml).unwrap();
        let child = create_run(&pool, &child_dag, child_yaml).await.unwrap();
        assert!(park_subworkflow(&pool, &a.id, a.version + 1, &child)
            .await
            .unwrap());
        // `b`'s child is gone: it was never made, or it was purged.
        assert!(park_subworkflow(&pool, &b.id, b.version + 1, "gone")
            .await
            .unwrap());

        assert!(
            triggers_read(&pool).await.is_empty(),
            "a child runs, the other is gone"
        );
        assert!(reconcile_subworkflows(&pool).await.unwrap().done.is_empty());

        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
            .bind(&child)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(triggers_read(&pool).await, vec![a.id.clone()]);
        let pass = reconcile_subworkflows(&pool).await.unwrap();
        assert_eq!(pass.done, vec![(a.id.clone(), true)]);
        assert!(!pass.full);
        let again = reconcile_subworkflows(&pool).await.unwrap();
        assert!(again.done.is_empty(), "resolved once");

        let (status, sub): (String, Option<String>) =
            sqlx::query_as("SELECT status, sub_run_id FROM task_runs WHERE id = ?")
                .bind(&b.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "running", "left parked");
        assert_eq!(sub.as_deref(), Some("gone"));
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// More time sensors are due than a batch reads: the first pass resolves
    /// a batch, the longest due first, and says it was full; the next takes
    /// the rest.
    #[tokio::test]
    async fn waits_past_the_batch_are_taken_by_the_next_pass() {
        // A run of 300 tasks: `create_run` reads DAGRON_MAX_TASKS_PER_RUN.
        let _g = crate::env_lock();
        let (pool, path) = temp_pool().await;
        let mut yaml = String::from("name: many\ntasks:\n");
        for i in 0..300 {
            yaml.push_str(&format!(
                "  - {{ name: w{i}, type: wait, wait: {{ for: 1h }} }}\n"
            ));
        }
        let dag = DagGraph::from_yaml(&yaml).unwrap();
        create_run(&pool, &dag, &yaml).await.unwrap();
        advance_ready_tasks(&pool).await.unwrap();
        let claimed = claim_ready(&pool, "w", 300).await.unwrap();
        assert_eq!(claimed.len(), 300);
        for (i, t) in claimed.iter().enumerate() {
            // Due a second apart, the first the longest.
            let wake_at = format!("2026-01-01T00:{:02}:{:02}+00:00", i / 60, i % 60);
            assert!(park_wait(&pool, &t.id, t.version + 1, &wake_at)
                .await
                .unwrap());
        }

        let first = reconcile_waits(&pool).await.unwrap();
        assert_eq!(first.done.len() as i64, crate::db::SWEEP_BATCH);
        assert!(first.full, "a full batch says so");
        assert_eq!(
            first.done[0].1, "2026-01-01T00:00:00+00:00",
            "the longest due first"
        );
        let second = reconcile_waits(&pool).await.unwrap();
        assert_eq!(second.done.len(), 44);
        assert!(!second.full);
        assert!(reconcile_waits(&pool).await.unwrap().done.is_empty());
        let left: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE status = 'running'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(left, 0);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The trigger rows of a workflow.
    async fn trigger_rows(pool: &Pool, name: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM dataset_triggers WHERE workflow_name = ?")
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A workflow whose spec no longer subscribes to a dataset has its trigger
    /// rows cleared by name, whoever synced them and whenever: the engine
    /// keeps nothing of who it synced. A workflow that still subscribes keeps
    /// its own.
    #[tokio::test]
    async fn a_workflow_that_drops_on_datasets_is_cleared_by_any_engine() {
        let (pool, path) = temp_pool().await;
        let uris = vec!["s3://bucket/a".to_string(), "s3://bucket/b".to_string()];
        sync_dataset_triggers(&pool, "dropped", &uris, "any")
            .await
            .unwrap();
        sync_dataset_triggers(&pool, "kept", &uris[..1], "any")
            .await
            .unwrap();
        assert_eq!(trigger_rows(&pool, "dropped").await, 2);

        let names = ["dropped".to_string(), "never-synced".to_string()];
        assert_eq!(clear_dataset_triggers(&pool, &names).await.unwrap(), 2);
        assert_eq!(trigger_rows(&pool, "dropped").await, 0);
        assert_eq!(
            trigger_rows(&pool, "kept").await,
            1,
            "a subscribed workflow is untouched"
        );
        assert_eq!(clear_dataset_triggers(&pool, &[]).await.unwrap(), 0);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Each status's count over a whole table, in one read.
    #[cfg(feature = "ops")]
    async fn whole(pool: &Pool, table: &str) -> Vec<(String, i64)> {
        let mut rows: Vec<(String, i64)> = sqlx::query_as(&format!(
            "SELECT status, COUNT(*) FROM {table} GROUP BY status"
        ))
        .fetch_all(pool)
        .await
        .unwrap();
        rows.sort();
        rows
    }

    /// The gauges in two tiers add up to one read of the whole datastore, and
    /// no status is in both.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn status_counts_is_live_plus_history() {
        let (pool, path) = temp_pool().await;
        let yaml = "name: mix\ntasks:\n  - { name: a, command: [\"true\"] }\n  \
                    - { name: b, command: [\"true\"], depends_on: [a] }\n  \
                    - { name: c, command: [\"true\"] }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        for _ in 0..3 {
            create_run(&pool, &dag, yaml).await.unwrap();
        }
        advance_ready_tasks(&pool).await.unwrap();
        // Some tasks run, one finishes, one fails, and one run has ended.
        let claimed = claim_ready(&pool, "w", 4).await.unwrap();
        let fence = claimed[0].version + 1;
        mark_task_succeeded(&pool, &claimed[0].id, "w", fence, None)
            .await
            .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'failed' WHERE id = ?")
            .bind(&claimed[1].id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET status = 'succeeded'
             WHERE id = (SELECT id FROM workflow_runs ORDER BY id LIMIT 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        // And a run still to end that was made two days ago: no recent run.
        let old = create_run(&pool, &dag, yaml).await.unwrap();
        sqlx::query("UPDATE workflow_runs SET created_at = ? WHERE id = ?")
            .bind((chrono::Utc::now() - chrono::TimeDelta::days(2)).to_rfc3339())
            .bind(&old)
            .execute(&pool)
            .await
            .unwrap();

        let live = status_counts_live(&pool).await.unwrap();
        let history = status_counts_history(&pool).await.unwrap();
        for (status, _) in &live.runs_by_status {
            assert!(crate::models::LIVE_RUN_STATUSES.contains(&status.as_str()));
            assert!(!history.runs_by_status.iter().any(|(s, _)| s == status));
        }
        for (status, _) in &live.tasks_by_status {
            assert!(crate::models::LIVE_TASK_STATUSES.contains(&status.as_str()));
            assert!(!history.tasks_by_status.iter().any(|(s, _)| s == status));
        }
        // The recent runs still to end are read for a scrape, the rest with
        // the history, and the two are the one read of the last day's runs
        // the history made before.
        assert!(!live.recent_runs.is_empty() && !history.recent_runs.is_empty());
        for row in &live.recent_runs {
            assert!(
                crate::models::LIVE_RUN_STATUSES.contains(&row.status.as_str()),
                "{row:?}"
            );
        }
        for row in &history.recent_runs {
            assert!(
                !crate::models::LIVE_RUN_STATUSES.contains(&row.status.as_str()),
                "{row:?}"
            );
        }
        let cells = |rows: &[crate::models::RecentRuns]| {
            let mut cells: Vec<(String, Option<String>, String, i64)> = rows
                .iter()
                .map(|r| {
                    (
                        r.workflow.clone(),
                        r.environment.clone(),
                        r.status.clone(),
                        r.count,
                    )
                })
                .collect();
            cells.sort();
            cells
        };
        let since = (chrono::Utc::now()
            - chrono::TimeDelta::seconds(crate::models::RECENT_RUNS_WINDOW_SECS))
        .to_rfc3339();
        let mut one_read: Vec<(String, Option<String>, String, i64)> = sqlx::query_as(
            "SELECT wd.name, wr.environment, wr.status, COUNT(*)
             FROM workflow_runs wr
             JOIN workflow_definitions wd ON wd.id = wr.definition_id
             WHERE wr.created_at >= ?
             GROUP BY wd.name, wr.environment, wr.status",
        )
        .bind(&since)
        .fetch_all(&pool)
        .await
        .unwrap();
        one_read.sort();
        assert_eq!(
            cells(&[live.recent_runs.clone(), history.recent_runs.clone()].concat()),
            one_read
        );
        assert_eq!(
            one_read.iter().map(|cell| cell.3).sum::<i64>(),
            3,
            "the run made two days ago is not a recent one"
        );

        let merged = status_counts(&pool).await.unwrap();
        let (mut runs, mut tasks) = (
            merged.runs_by_status.clone(),
            merged.tasks_by_status.clone(),
        );
        runs.sort();
        tasks.sort();
        assert_eq!(runs, whole(&pool, "workflow_runs").await);
        assert_eq!(tasks, whole(&pool, "task_runs").await);
        assert!(tasks.iter().any(|(s, _)| s == "failed"));
        assert!(runs.iter().any(|(s, _)| s == "succeeded"));
        assert_eq!(cells(&merged.recent_runs), one_read);
        assert_eq!(merged.history_age_secs, 0.0);

        // dagron-api's two: the counts by status and the dead letters, as
        // the whole tiers have them, and nothing else read.
        let unfinished = status_counts_unfinished(&pool).await.unwrap();
        assert_eq!(unfinished.runs_by_status, live.runs_by_status);
        assert_eq!(unfinished.tasks_by_status, live.tasks_by_status);
        assert_eq!(unfinished.dead_letters, live.dead_letters);
        assert!(unfinished.active_tasks.is_empty() && !live.active_tasks.is_empty());
        let finished = status_counts_finished(&pool).await.unwrap();
        assert_eq!(finished.runs_by_status, history.runs_by_status);
        assert_eq!(finished.tasks_by_status, history.tasks_by_status);
        assert!(finished.recent_runs.is_empty());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    // ── The file's wakes ────────────────────────────────────────────────────

    /// The runs `run_wakes` holds, in order.
    async fn wakes(pool: &Pool) -> Vec<String> {
        sqlx::query_scalar("SELECT run_id FROM run_wakes ORDER BY run_id")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    async fn clear_wakes(pool: &Pool) {
        sqlx::query("DELETE FROM run_wakes")
            .execute(pool)
            .await
            .unwrap();
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

    /// A run of `yaml`, and its tasks by name.
    async fn run_of(pool: &Pool, yaml: &str) -> (String, HashMap<String, String>) {
        let run = create_run(pool, &DagGraph::from_yaml(yaml).unwrap(), yaml)
            .await
            .unwrap();
        let tasks: Vec<(String, String)> =
            sqlx::query_as("SELECT name, id FROM task_runs WHERE run_id = ?")
                .bind(&run)
                .fetch_all(pool)
                .await
                .unwrap();
        (run, tasks.into_iter().collect())
    }

    /// A waker past its first take, as the engine's is after its start.
    async fn started(pool: &Pool) -> Waker {
        let mut waker = Waker::connect(pool).await.unwrap();
        assert_eq!(waker.take_woken().await, crate::db::Woken::All);
        waker
    }

    /// Each write that can make a run over writes its wake, in its own
    /// transaction; one that cannot writes none.
    #[tokio::test]
    async fn wakes_follow_the_writes_that_may_end_a_run() {
        let (pool, path) = temp_pool().await;
        let two = "name: w\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n";
        // A run made running and not waiting wakes; then the table is let be.
        let (run, tasks) = run_of(&pool, two).await;
        assert_eq!(wakes(&pool).await, std::slice::from_ref(&run), "a run made");
        clear_wakes(&pool).await;

        sql(
            &pool,
            "UPDATE task_runs SET status = 'ready' WHERE id = ?",
            &[&tasks["a"]],
        )
        .await;
        assert!(wakes(&pool).await.is_empty(), "pending to ready");
        for status in ["succeeded", "failed", "skipped", "cancelled"] {
            sql(
                &pool,
                "UPDATE task_runs SET status = 'running' WHERE id = ?",
                &[&tasks["a"]],
            )
            .await;
            sql(
                &pool,
                "UPDATE task_runs SET status = ? WHERE id = ?",
                &[status, &tasks["a"]],
            )
            .await;
            assert_eq!(
                wakes(&pool).await,
                std::slice::from_ref(&run),
                "running to {status}"
            );
            clear_wakes(&pool).await;
        }
        sql(
            &pool,
            "UPDATE task_runs SET status = 'failed' WHERE id = ?",
            &[&tasks["a"]],
        )
        .await;
        assert!(wakes(&pool).await.is_empty(), "from one end to another");

        // A task made already over.
        sql(
            &pool,
            "INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
             VALUES ('t-over', ?, 'over', 'succeeded', 0, '{}')",
            &[&run],
        )
        .await;
        assert_eq!(
            wakes(&pool).await,
            std::slice::from_ref(&run),
            "a task made over"
        );
        clear_wakes(&pool).await;

        // A task not over taken from a running run; then the finished tasks
        // of a finished run, as GC takes them, which wake nothing.
        sql(&pool, "DELETE FROM task_runs WHERE id = ?", &[&tasks["b"]]).await;
        assert_eq!(
            wakes(&pool).await,
            std::slice::from_ref(&run),
            "an unfinished task gone"
        );
        clear_wakes(&pool).await;
        sql(
            &pool,
            "UPDATE workflow_runs SET status = 'failed' WHERE id = ?",
            &[&run],
        )
        .await;
        sql(&pool, "DELETE FROM task_runs WHERE run_id = ?", &[&run]).await;
        assert!(wakes(&pool).await.is_empty(), "a finished run's tasks gone");

        // A finished run running again, its wait cleared, as a rerun does.
        sql(
            &pool,
            "UPDATE workflow_runs SET status = 'running', queued_at = NULL WHERE id = ?",
            &[&run],
        )
        .await;
        assert_eq!(
            wakes(&pool).await,
            std::slice::from_ref(&run),
            "running again"
        );
        clear_wakes(&pool).await;

        // A stop asked for wakes nothing; a stop taken back wakes it.
        sql(
            &pool,
            "UPDATE workflow_runs SET stopping_as = 'cancelled' WHERE id = ?",
            &[&run],
        )
        .await;
        assert!(wakes(&pool).await.is_empty(), "a stop asked for");
        sql(
            &pool,
            "UPDATE workflow_runs SET stopping_as = NULL WHERE id = ?",
            &[&run],
        )
        .await;
        assert_eq!(
            wakes(&pool).await,
            std::slice::from_ref(&run),
            "a stop taken back"
        );
        clear_wakes(&pool).await;

        // The stamps of the other sweeps are not the reap's.
        sql(
            &pool,
            "UPDATE workflow_runs SET alert_fired_at = 'now', start_pending_at = 'now' WHERE id = ?",
            &[&run],
        )
        .await;
        assert!(wakes(&pool).await.is_empty(), "a sweep's stamp");

        // A run made waiting for its turn wakes nothing until it is let in.
        let definition: String =
            sqlx::query_scalar("SELECT definition_id FROM workflow_runs WHERE id = ?")
                .bind(&run)
                .fetch_one(&pool)
                .await
                .unwrap();
        sql(
            &pool,
            "INSERT INTO workflow_runs (id, definition_id, status, created_at, queued_at)
             VALUES ('r-queued', ?, 'running', 'now', 'now')",
            &[&definition],
        )
        .await;
        assert!(wakes(&pool).await.is_empty(), "a run made waiting");
        sql(
            &pool,
            "UPDATE workflow_runs SET admitted_at = 'now' WHERE id = 'r-queued'",
            &[],
        )
        .await;
        assert_eq!(wakes(&pool).await, ["r-queued"], "let in");
        clear_wakes(&pool).await;

        // A wake rolls back with the write that made it.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE task_runs SET status = 'cancelled' WHERE id = 't-over'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'running' WHERE id = 't-over'")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE id = 't-over'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(wakes(&pool).await.is_empty(), "rolled back");
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The first take says `All` and empties the table, whatever an older
    /// engine left there; the next names what changed since.
    #[tokio::test]
    async fn the_first_take_is_all_and_empties_the_table() {
        let (pool, path) = temp_pool().await;
        for left in ["left-1", "left-2", "left-3"] {
            sql(&pool, "INSERT INTO run_wakes (run_id) VALUES (?)", &[left]).await;
        }
        let one = "name: w\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let (run, _) = run_of(&pool, one).await;
        let mut waker = started(&pool).await;
        assert!(wakes(&pool).await.is_empty());
        assert_eq!(waker.take_woken().await, crate::db::Woken::Runs(Vec::new()));

        advance_ready_tasks(&pool).await.unwrap();
        let a = claim_ready(&pool, "w", 1).await.unwrap().remove(0);
        assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, None)
            .await
            .unwrap());
        assert_eq!(waker.take_woken().await, crate::db::Woken::Runs(vec![run]));
        assert!(wakes(&pool).await.is_empty(), "taken is deleted");
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// One take names at most [`MAX_NAMED_RUNS`]; the rest are the next's.
    #[tokio::test]
    async fn a_take_is_bounded() {
        let (pool, path) = temp_pool().await;
        let mut waker = started(&pool).await;
        sql(
            &pool,
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 4097)
             INSERT INTO run_wakes (run_id) SELECT printf('r-%05d', i) FROM n",
            &[],
        )
        .await;
        let crate::db::Woken::Runs(first) = waker.take_woken().await else {
            panic!("a take that worked names its runs");
        };
        assert_eq!(first.len(), 4096);
        let crate::db::Woken::Runs(second) = waker.take_woken().await else {
            panic!("a take that worked names its runs");
        };
        assert_eq!(second.len(), 1);
        assert!(!first.contains(&second[0]));
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A take that cannot read the table says `All`, and a reap of all is
    /// what the tick then runs. Takes that go on failing are not logged
    /// each tick, and one that succeeds ends the run of failures.
    #[tokio::test]
    async fn a_take_that_fails_says_all() {
        let (pool, path) = temp_pool().await;
        let mut waker = started(&pool).await;
        sql(&pool, "ALTER TABLE run_wakes RENAME TO run_wakes_away", &[]).await;
        assert_eq!(waker.take_woken().await, crate::db::Woken::All);
        assert!(reap_woken(&pool, &crate::db::Woken::All)
            .await
            .unwrap()
            .is_empty());
        // Logged at the first failure; those after it, within the minute,
        // are counted for the next line, not logged each tick.
        let logged = waker.failing.expect("the failure is kept").0;
        assert_eq!(waker.take_woken().await, crate::db::Woken::All);
        assert_eq!(waker.take_woken().await, crate::db::Woken::All);
        assert_eq!(waker.failing, Some((logged, 2)));
        sql(&pool, "ALTER TABLE run_wakes_away RENAME TO run_wakes", &[]).await;
        assert_eq!(waker.take_woken().await, crate::db::Woken::Runs(Vec::new()));
        assert_eq!(waker.failing, None, "takes recovered");
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A take that finds nothing takes no write lock: it returns while
    /// another process holds one, well inside the busy timeout. One that
    /// finds a wake waits for the lock, as any write does, and a bare
    /// `DELETE` of the empty table waits too.
    #[tokio::test]
    async fn the_idle_take_writes_nothing() {
        let (pool, path) = temp_pool().await;
        let other = init_pool(path.to_str().unwrap()).await.unwrap();
        let mut waker = started(&pool).await;
        let mut holder = other.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *holder)
            .await
            .unwrap();

        let idle = tokio::time::timeout(Duration::from_secs(1), waker.take_woken()).await;
        assert_eq!(
            idle.expect("an idle take waits for no lock"),
            crate::db::Woken::Runs(Vec::new())
        );
        // A third process's DELETE of the empty table waits all the same.
        let third = init_pool(path.to_str().unwrap()).await.unwrap();
        let bare = {
            let third = third.clone();
            tokio::spawn(async move { sqlx::query("DELETE FROM run_wakes").execute(&third).await })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !bare.is_finished(),
            "a DELETE of nothing still takes the write lock"
        );
        sqlx::query("ROLLBACK").execute(&mut *holder).await.unwrap();
        assert_eq!(bare.await.unwrap().unwrap().rows_affected(), 0);
        third.close().await;

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *holder)
            .await
            .unwrap();
        sqlx::query("INSERT INTO run_wakes (run_id) VALUES ('r-held')")
            .execute(&mut *holder)
            .await
            .unwrap();
        sqlx::query("COMMIT").execute(&mut *holder).await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *holder)
            .await
            .unwrap();
        let take = tokio::spawn(async move { waker.take_woken().await });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!take.is_finished(), "a take of a wake waits for the lock");
        sqlx::query("COMMIT").execute(&mut *holder).await.unwrap();
        assert_eq!(
            take.await.unwrap(),
            crate::db::Woken::Runs(vec!["r-held".to_string()])
        );
        drop(holder);
        other.close().await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A run's status as the reap left it.
    async fn run_status(pool: &Pool, run: &str) -> String {
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = ?")
            .bind(run)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Take what the file says and reap it, as a tick does: `run` is not
    /// ended by it.
    async fn tick_leaves(pool: &Pool, waker: &mut Waker, run: &str, case: &str) {
        let woken = waker.take_woken().await;
        assert!(
            matches!(woken, crate::db::Woken::Runs(_)),
            "{case}: {woken:?}"
        );
        let ended = reap_woken(pool, &woken).await.unwrap();
        assert!(
            !ended.iter().any(|(id, _)| id == run),
            "{case}: ended early: {ended:?}"
        );
        assert_eq!(run_status(pool, run).await, "running", "{case}");
    }

    /// The take names `run`, and the reap of what it named ends it so.
    async fn tick_ends(pool: &Pool, waker: &mut Waker, run: &str, status: RunStatus, case: &str) {
        let woken = waker.take_woken().await;
        let crate::db::Woken::Runs(named) = &woken else {
            panic!("{case}: {woken:?}");
        };
        assert!(
            named.iter().any(|id| id == run),
            "{case}: {run} not named: {named:?}"
        );
        let ended = reap_woken(pool, &woken).await.unwrap();
        assert!(
            ended.iter().any(|(id, s)| id == run && *s == status),
            "{case}: {run} should have ended {status}: {ended:?}"
        );
    }

    /// Claim the one task of `run` named `name` that is ready, as worker `w`.
    async fn claim_named(pool: &Pool, run: &str, name: &str) -> TaskRun {
        advance_ready_tasks(pool).await.unwrap();
        let claimed = claim_ready(pool, "w", 100).await.unwrap();
        claimed
            .into_iter()
            .find(|t| t.run_id == run && t.name == name)
            .unwrap_or_else(|| panic!("{name} of {run} is not ready"))
    }

    /// Every way a run ends today, each driven by the dagron-core function
    /// that ends it, reaches the reap by its wake alone: no case reads a
    /// slice or reaps every run. A path added later that ends a run joins
    /// this list.
    #[tokio::test]
    async fn each_end_reaches_the_reap_through_its_wake() {
        let _g = crate::env_lock();
        let (pool, path) = temp_pool().await;
        let mut waker = started(&pool).await;
        let w = &mut waker;
        let one = "name: one\ntasks:\n  - name: a\n    command: [\"true\"]\n";

        // A task's own end, through each mark.
        for (case, mark, status) in [
            ("succeeded", 0, RunStatus::Succeeded),
            ("succeeded from cache", 1, RunStatus::Succeeded),
            ("failed", 2, RunStatus::Failed),
        ] {
            let (run, _) = run_of(&pool, one).await;
            tick_leaves(&pool, w, &run, case).await;
            let a = claim_named(&pool, &run, "a").await;
            let fence = a.version + 1;
            let marked = match mark {
                0 => mark_task_succeeded(&pool, &a.id, "w", fence, None).await,
                1 => mark_task_succeeded_cached(&pool, &a.id, "w", fence, None).await,
                _ => mark_task_failed(&pool, &a.id, "w", fence, Some("no".into())).await,
            };
            assert!(marked.unwrap(), "{case}");
            tick_ends(&pool, w, &run, status, case).await;
        }
        {
            let case = "failed, and allowed to";
            let yaml = "name: lax\ntasks:\n  - name: a\n    command: [\"false\"]\n    allow_failure: true\n";
            let (run, _) = run_of(&pool, yaml).await;
            tick_leaves(&pool, w, &run, case).await;
            let a = claim_named(&pool, &run, "a").await;
            assert!(mark_task_failed(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
        }
        {
            let case = "a runner's word";
            let (run, _) = run_of(&pool, one).await;
            tick_leaves(&pool, w, &run, case).await;
            advance_ready_tasks(&pool).await.unwrap();
            let a = claim_ready(&pool, "runner:r1/s1", 1)
                .await
                .unwrap()
                .remove(0);
            sql(
                &pool,
                "INSERT INTO runner_results (task_id, fence, claimed_by, status, output, created_at)
                 VALUES (?, ?, 'runner:r1/s1', 'succeeded', 'done', 'now')",
                &[&a.id, &(a.version + 1).to_string()],
            )
            .await;
            let word = take_runner_results(&pool, "engine", 10)
                .await
                .unwrap()
                .remove(0);
            assert!(mark_task_succeeded(
                &pool,
                &word.task_id,
                &word.claimed_by,
                word.fence,
                word.output
            )
            .await
            .unwrap());
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
        }
        {
            // On Postgres this end is the slice's: the skip sends no
            // notification.
            let case = "a dependent skipped after a failure";
            let yaml = "name: chain\ntasks:\n  - name: a\n    command: [\"false\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
            let (run, _) = run_of(&pool, yaml).await;
            tick_leaves(&pool, w, &run, case).await;
            let a = claim_named(&pool, &run, "a").await;
            assert!(mark_task_failed(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_leaves(&pool, w, &run, case).await;
            advance_ready_tasks(&pool).await.unwrap();
            tick_ends(&pool, w, &run, RunStatus::Failed, case).await;
        }
        {
            let case = "a trigger rule's skip";
            let yaml = "name: rule\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: c\n    command: [\"true\"]\n    depends_on: [a]\n    trigger_rule: one_failed\n";
            let (run, _) = run_of(&pool, yaml).await;
            tick_leaves(&pool, w, &run, case).await;
            let a = claim_named(&pool, &run, "a").await;
            assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_leaves(&pool, w, &run, case).await;
            advance_ready_tasks(&pool).await.unwrap();
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
        }
        {
            let case = "a gang's siblings";
            let yaml = "name: g\ntasks:\n  - name: train\n    command: [\"true\"]\n    gang: { size: 2 }\n";
            let (run, _) = run_of(&pool, yaml).await;
            tick_leaves(&pool, w, &run, case).await;
            advance_ready_tasks(&pool).await.unwrap();
            let gang = claim_ready_gang(&pool, "w", 2, &[], &Default::default())
                .await
                .unwrap();
            assert_eq!(gang.len(), 2);
            let (victim, sibling) = (&gang[0], &gang[1]);
            assert!(
                mark_task_failed(&pool, &victim.id, "w", victim.version + 1, None)
                    .await
                    .unwrap()
            );
            tick_leaves(&pool, w, &run, case).await;
            assert_eq!(cancel_gang_siblings(&pool, &victim.id).await.unwrap(), 1);
            assert!(
                settle_stopped_task(&pool, &sibling.id, "w", sibling.version + 1)
                    .await
                    .unwrap()
            );
            tick_ends(&pool, w, &run, RunStatus::Failed, case).await;
        }
        // A fan-out's barrier, of no instance and of some, and made again
        // over a new output after its instances were.
        let fan = "name: fan\ntasks:\n  - name: list\n    command: [\"true\"]\n  - name: process\n    command: [\"handle\", \"{{ item }}\"]\n    with_output_of: list\n    depends_on: [list]\n";
        for (case, output) in [
            ("a fan-out of nothing", "[]"),
            ("a fan-out", r#"["x","y"]"#),
        ] {
            let (run, _) = run_of(&pool, fan).await;
            tick_leaves(&pool, w, &run, case).await;
            let list = claim_named(&pool, &run, "list").await;
            assert!(mark_task_succeeded(
                &pool,
                &list.id,
                "w",
                list.version + 1,
                Some(output.into())
            )
            .await
            .unwrap());
            advance_ready_tasks(&pool).await.unwrap();
            tick_leaves(&pool, w, &run, case).await;
            reconcile_fanouts(&pool).await.unwrap();
            if output != "[]" {
                tick_leaves(&pool, w, &run, case).await;
                for t in claim_ready(&pool, "w", 10).await.unwrap() {
                    assert!(mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None)
                        .await
                        .unwrap());
                }
                tick_leaves(&pool, w, &run, case).await;
                reconcile_fanouts(&pool).await.unwrap();
            }
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
        }
        {
            let case = "a fan-out made again";
            let (run, tasks) = run_of(&pool, fan).await;
            tick_leaves(&pool, w, &run, case).await;
            let list = claim_named(&pool, &run, "list").await;
            assert!(mark_task_succeeded(
                &pool,
                &list.id,
                "w",
                list.version + 1,
                Some(r#"["x","y"]"#.into())
            )
            .await
            .unwrap());
            advance_ready_tasks(&pool).await.unwrap();
            reconcile_fanouts(&pool).await.unwrap();
            assert_eq!(claim_ready(&pool, "w", 10).await.unwrap().len(), 2);
            tick_leaves(&pool, w, &run, case).await;
            // The barrier's version moves on while its instances run, as a
            // reset moves it: the next expansion takes them away, tasks not
            // over, and that is the run's wake.
            sql(
                &pool,
                "UPDATE task_runs SET version = version + 1 WHERE id = ?",
                &[&tasks["process"]],
            )
            .await;
            reconcile_fanouts(&pool).await.unwrap();
            assert_eq!(
                w.take_woken().await,
                crate::db::Woken::Runs(vec![run.clone()]),
                "{case}"
            );
            for t in claim_ready(&pool, "w", 10).await.unwrap() {
                assert!(mark_task_succeeded(&pool, &t.id, "w", t.version + 1, None)
                    .await
                    .unwrap());
            }
            tick_leaves(&pool, w, &run, case).await;
            reconcile_fanouts(&pool).await.unwrap();
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
        }
        // A gate's decision, an operator's and its timeout's.
        let gate =
            "name: gate\ntasks:\n  - { name: gate, type: approval, approval_timeout_secs: 60 }\n";
        for (case, approve, status) in [
            ("a gate approved", Some(true), RunStatus::Succeeded),
            ("a gate rejected", Some(false), RunStatus::Failed),
            ("a gate timed out", None, RunStatus::Failed),
        ] {
            let (run, tasks) = run_of(&pool, gate).await;
            advance_ready_tasks(&pool).await.unwrap();
            tick_leaves(&pool, w, &run, case).await;
            match approve {
                Some(approve) => assert!(resolve_approval(
                    &pool,
                    &run,
                    &tasks["gate"],
                    approve,
                    Some("alice"),
                    None
                )
                .await
                .unwrap()),
                None => {
                    let past = (chrono::Utc::now() - chrono::TimeDelta::seconds(120)).to_rfc3339();
                    sql(
                        &pool,
                        "UPDATE task_runs SET scheduled_at = ? WHERE id = ?",
                        &[&past, &tasks["gate"]],
                    )
                    .await;
                    assert_eq!(resolve_expired_approvals(&pool).await.unwrap().len(), 1);
                }
            }
            tick_ends(&pool, w, &run, status, case).await;
        }
        // A sub-workflow's trigger: the parent ends at the take after its
        // child's, and a trigger failed outright.
        let child = "name: child\ntasks:\n  - { name: c, command: [\"true\"] }\n";
        sql(
            &pool,
            "INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ('wf-child', 'child', ?, 't', 't')",
            &[child],
        )
        .await;
        let parent =
            "name: parent\ntasks:\n  - { name: trigger, type: workflow, workflow: child }\n";
        for (case, fails) in [("a sub-workflow", false), ("a sub-workflow failed", true)] {
            let (run, _) = run_of(&pool, parent).await;
            tick_leaves(&pool, w, &run, case).await;
            let trigger = claim_named(&pool, &run, "trigger").await;
            let (child_run, _) = run_of(&pool, child).await;
            assert!(
                park_subworkflow(&pool, &trigger.id, trigger.version + 1, &child_run)
                    .await
                    .unwrap()
            );
            tick_leaves(&pool, w, &run, case).await;
            if fails {
                assert!(
                    fail_parked_subworkflow(&pool, &trigger.id, &child_run, "gone")
                        .await
                        .unwrap()
                );
                tick_ends(&pool, w, &run, RunStatus::Failed, case).await;
            } else {
                let c = claim_named(&pool, &child_run, "c").await;
                assert!(mark_task_succeeded(&pool, &c.id, "w", c.version + 1, None)
                    .await
                    .unwrap());
                tick_ends(&pool, w, &child_run, RunStatus::Succeeded, case).await;
                assert_eq!(reconcile_subworkflows(&pool).await.unwrap().done.len(), 1);
                tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
            }
        }
        // The sensors: time, HTTP, an external job's two ends, a dataset.
        for case in [
            "a time sensor",
            "an HTTP sensor",
            "an external job",
            "an external job failed",
            "a dataset sensor",
        ] {
            let yaml = match case {
                "a time sensor" => "name: s\ntasks:\n  - { name: s, type: wait, wait: { for: 1h } }\n",
                "an HTTP sensor" => "name: s\ntasks:\n  - { name: s, type: wait, wait: { url: \"https://example.com/ready\" } }\n",
                "a dataset sensor" => "name: s\ntasks:\n  - { name: s, type: wait, wait: { dataset: \"s3://lake/x\" } }\n",
                _ => "name: s\ntasks:\n  - { name: s, command: [\"true\"] }\n",
            };
            let (run, _) = run_of(&pool, yaml).await;
            tick_leaves(&pool, w, &run, case).await;
            let s = claim_named(&pool, &run, "s").await;
            let fence = s.version + 1;
            let now = chrono::Utc::now().to_rfc3339();
            let status = match case {
                "a time sensor" => {
                    assert!(park_wait(&pool, &s.id, fence, "2000-01-01T00:00:00+00:00")
                        .await
                        .unwrap());
                    tick_leaves(&pool, w, &run, case).await;
                    assert_eq!(reconcile_waits(&pool).await.unwrap().done.len(), 1);
                    RunStatus::Succeeded
                }
                "an HTTP sensor" => {
                    assert!(
                        park_wait_url(&pool, &s.id, fence, "https://example.com/ready", &now)
                            .await
                            .unwrap()
                    );
                    tick_leaves(&pool, w, &run, case).await;
                    assert!(resolve_url_wait(&pool, &s.id).await.unwrap());
                    RunStatus::Succeeded
                }
                "a dataset sensor" => {
                    assert!(park_wait_dataset(&pool, &s.id, fence, "s3://lake/x")
                        .await
                        .unwrap());
                    tick_leaves(&pool, w, &run, case).await;
                    record_external_dataset_event(&pool, "s3://lake/x")
                        .await
                        .unwrap();
                    assert_eq!(reconcile_dataset_waits(&pool).await.unwrap().done.len(), 1);
                    RunStatus::Succeeded
                }
                _ => {
                    assert!(
                        park_external(&pool, &s.id, fence, "http", "job-1", None, &now, None)
                            .await
                            .unwrap()
                    );
                    tick_leaves(&pool, w, &run, case).await;
                    if case == "an external job" {
                        assert!(resolve_external(&pool, &s.id, "job-1", Some("DONE"))
                            .await
                            .unwrap());
                        RunStatus::Succeeded
                    } else {
                        assert!(fail_external(&pool, &s.id, "job-1", "OOM").await.unwrap());
                        RunStatus::Failed
                    }
                }
            };
            tick_ends(&pool, w, &run, status, case).await;
        }
        {
            // A run its parameters leave no task in, made, and one that
            // waited at a full cap and was let in.
            let case = "a run of no task";
            let none = "name: tags\nparameters: { event: push }\ntasks:\n  - { name: release, command: [\"true\"], when: \"{{ event }} == tag\" }\n";
            let dag = DagGraph::from_yaml_with_params(none, &Default::default()).unwrap();
            let run = create_run(&pool, &dag, none).await.unwrap();
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;

            let case = "a run of no task let in at its cap";
            let capped = "name: deploy\nmax_active_runs: 1\nwhen_full: queue\n";
            let busy = format!("{capped}tasks:\n  - name: a\n    command: [\"true\"]\n");
            let (first, _) = run_of(&pool, &busy).await;
            let empty = format!(
                "{capped}parameters: {{ event: push }}\ntasks:\n  - name: a\n    command: [\"true\"]\n    when: \"{{{{ event }}}} == tag\"\n"
            );
            let dag = DagGraph::from_yaml_with_params(&empty, &Default::default()).unwrap();
            let waits = create_run(&pool, &dag, &empty).await.unwrap();
            tick_leaves(&pool, w, &waits, case).await;
            let a = claim_named(&pool, &first, "a").await;
            assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_ends(&pool, w, &first, RunStatus::Succeeded, case).await;
            assert_eq!(
                admit_waiting_runs(&pool).await.unwrap(),
                vec![waits.clone()]
            );
            tick_ends(&pool, w, &waits, RunStatus::Succeeded, case).await;
        }
        #[cfg(feature = "ops")]
        {
            let case = "a rerun";
            let (run, _) = run_of(&pool, one).await;
            tick_leaves(&pool, w, &run, case).await;
            let a = claim_named(&pool, &run, "a").await;
            assert!(mark_task_failed(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_ends(&pool, w, &run, RunStatus::Failed, case).await;
            assert!(rerun_from_failed(&pool, &run).await.unwrap().is_some());
            tick_leaves(&pool, w, &run, case).await;
            let a = claim_named(&pool, &run, "a").await;
            assert!(mark_task_succeeded(&pool, &a.id, "w", a.version + 1, None)
                .await
                .unwrap());
            tick_ends(&pool, w, &run, RunStatus::Succeeded, case).await;
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A wake is a reason to look, not an end: the reap of what it names
    /// leaves a run with work to do, one being stopped (that end is
    /// `end_stopped_runs`'), and one already ended as they were.
    #[tokio::test]
    async fn a_wake_does_not_end_what_is_not_over() {
        let (pool, path) = temp_pool().await;
        let mut waker = started(&pool).await;
        let two = "name: w\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n";
        let (pending, _) = run_of(&pool, two).await;
        let (stopping, _) = run_of(&pool, two).await;
        let (cancelled, _) = run_of(&pool, two).await;
        assert!(waker.take_woken().await != crate::db::Woken::All);
        sql(
            &pool,
            "UPDATE workflow_runs SET stopping_as = 'cancelled' WHERE id = ?",
            &[&stopping],
        )
        .await;
        sql(
            &pool,
            "UPDATE task_runs SET status = 'cancelled' WHERE run_id = ?",
            &[&stopping],
        )
        .await;
        sql(
            &pool,
            "UPDATE workflow_runs SET status = 'cancelled' WHERE id = ?",
            &[&cancelled],
        )
        .await;
        sql(
            &pool,
            "UPDATE task_runs SET status = 'cancelled' WHERE run_id = ?",
            &[&cancelled],
        )
        .await;
        sql(
            &pool,
            "UPDATE task_runs SET status = 'succeeded' WHERE run_id = ? AND name = 'a'",
            &[&pending],
        )
        .await;
        let woken = waker.take_woken().await;
        let crate::db::Woken::Runs(named) = &woken else {
            panic!("{woken:?}");
        };
        let mut named = named.clone();
        named.sort();
        let mut all = vec![pending.clone(), stopping.clone(), cancelled.clone()];
        all.sort();
        assert_eq!(named, all);
        assert!(reap_woken(&pool, &woken).await.unwrap().is_empty());
        assert_eq!(run_status(&pool, &pending).await, "running");
        assert_eq!(run_status(&pool, &stopping).await, "running");
        assert_eq!(run_status(&pool, &cancelled).await, "cancelled");
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The slice is the net under a wake that is lost: slices from the
    /// start end such a run, never name one that waits for its turn, walk
    /// every reapable run, and say `None` at the end.
    #[tokio::test]
    async fn a_lost_wake_is_ended_by_the_slice() {
        let (pool, path) = temp_pool().await;
        let mut waker = started(&pool).await;
        let one = "name: w\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let mut runs = Vec::new();
        for _ in 0..5 {
            runs.push(run_of(&pool, one).await.0);
        }
        let capped = "name: cap\nmax_active_runs: 1\nwhen_full: queue\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let (_holding, _) = run_of(&pool, capped).await;
        let (waits, _) = run_of(&pool, capped).await;
        assert!(waker.take_woken().await != crate::db::Woken::All);
        // The last task of runs[2] ends, and its wake is lost.
        sql(
            &pool,
            "UPDATE task_runs SET status = 'succeeded' WHERE run_id = ?",
            &[&runs[2]],
        )
        .await;
        clear_wakes(&pool).await;
        assert_eq!(waker.take_woken().await, crate::db::Woken::Runs(Vec::new()));

        let (mut after, mut walked, mut passes) = (None::<String>, 0, 0);
        let mut ended = Vec::new();
        loop {
            let ids: Vec<String> = sqlx::query_scalar(&reap_slice_sql())
                .bind(after.as_deref().unwrap_or(""))
                .bind(2_i64)
                .fetch_all(&pool)
                .await
                .unwrap();
            assert!(!ids.contains(&waits), "a run that waits is not the slice's");
            walked += ids.len();
            let (swept, next) = reap_slice(&pool, after.as_deref(), 2).await.unwrap();
            ended.extend(swept);
            passes += 1;
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        // Five runs and the one holding the cap, two a slice, and a last
        // slice that finds none and says so.
        assert_eq!((walked, passes), (6, 4));
        assert_eq!(ended, vec![(runs[2].clone(), RunStatus::Succeeded)]);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Another process on the file, as dagron-api is, writes as it does:
    /// the engine's waker hears each write that may end a run, and the reap
    /// of what it names ends each.
    #[tokio::test]
    async fn writes_from_another_pool_are_heard() {
        let (pool, path) = temp_pool().await;
        let api = init_pool(path.to_str().unwrap()).await.unwrap();
        let mut waker = started(&pool).await;

        // A gate approved by control.rs's SQL, the run's last task.
        let gate = "name: gate\ntasks:\n  - { name: gate, type: approval }\n";
        let (gated, tasks) = run_of(&pool, gate).await;
        advance_ready_tasks(&pool).await.unwrap();
        // A run made by dagron-api's own insert, with no task.
        let definition: String =
            sqlx::query_scalar("SELECT definition_id FROM workflow_runs WHERE id = ?")
                .bind(&gated)
                .fetch_one(&pool)
                .await
                .unwrap();
        // A task ended by hand.
        let one = "name: one\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let (by_hand, by_hand_tasks) = run_of(&pool, one).await;
        assert!(waker.take_woken().await != crate::db::Woken::All);
        assert!(reap_woken(
            &pool,
            &crate::db::Woken::Runs(vec![gated.clone(), by_hand.clone()])
        )
        .await
        .unwrap()
        .is_empty());

        sql(
            &api,
            "UPDATE task_runs SET status = $1, finished_at = $2, output = $3,
                    decided_by = $4, decision_comment = $5, log = $6
             WHERE id = $7 AND run_id = $8 AND status = 'awaiting_approval'",
            &[
                "succeeded",
                "now",
                "approved",
                "alice",
                "ok",
                "approved",
                &tasks["gate"],
                &gated,
            ],
        )
        .await;
        sql(
            &api,
            "INSERT INTO workflow_runs (id, definition_id, status, created_at, deadline_at, result_from, environment)
             VALUES ('r-api', $1, 'running', 'now', NULL, NULL, NULL)",
            &[&definition],
        )
        .await;
        sql(
            &api,
            "UPDATE task_runs SET status = 'succeeded' WHERE id = ?",
            &[&by_hand_tasks["a"]],
        )
        .await;

        let woken = waker.take_woken().await;
        let crate::db::Woken::Runs(named) = &woken else {
            panic!("{woken:?}");
        };
        let mut named = named.clone();
        named.sort();
        let mut want = vec![gated.clone(), by_hand.clone(), "r-api".to_string()];
        want.sort();
        assert_eq!(named, want);
        let mut ended = reap_woken(&pool, &woken).await.unwrap();
        ended.sort_by(|a, b| a.0.cmp(&b.0));
        let mut ends = vec![
            (gated, RunStatus::Succeeded),
            (by_hand, RunStatus::Succeeded),
            ("r-api".to_string(), RunStatus::Succeeded),
        ];
        ends.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(ended, ends);
        api.close().await;
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}

/// migrations/081's `run_changes`: what the file writes there for dagron-api's
/// live stream, its bound, and the reads of it.
#[cfg(all(test, feature = "ops"))]
mod run_changes_tests {
    use super::*;

    const TWO: &str = "name: changes\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";

    async fn temp_pool() -> (Pool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("module54_changes_{}.db", Uuid::new_v4()));
        let pool = init_pool(path.to_str().unwrap()).await.unwrap();
        (pool, path)
    }

    async fn sql(pool: &Pool, text: &str, binds: &[&str]) {
        let mut q = sqlx::query(text).persistent(false);
        for b in binds {
            q = q.bind(*b);
        }
        q.execute(pool)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
    }

    async fn newest(pool: &Pool) -> i64 {
        sqlx::query_scalar(RUN_CHANGES_NEWEST)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The runs written past `after`, in order.
    async fn past(pool: &Pool, after: i64) -> Vec<String> {
        match run_changes_after(pool, after, 1_000_000).await.unwrap() {
            RunChanges::Since(rows) => rows.into_iter().map(|(_, run)| run).collect(),
            RunChanges::Behind { newest } => panic!("behind {newest}"),
        }
    }

    /// Each change the stream compared before 081 writes its run, once
    /// however often it is touched before it is read; a change it did not
    /// compare writes nothing.
    #[tokio::test]
    async fn the_changes_the_stream_sends_are_written_and_no_others() {
        let (pool, path) = temp_pool().await;
        let at = newest(&pool).await;
        let run = create_run(&pool, &DagGraph::from_yaml(TWO).unwrap(), TWO)
            .await
            .unwrap();
        assert_eq!(
            past(&pool, at).await,
            vec![run.clone()],
            "a run made, with its tasks"
        );
        let cases: [(&str, &str, bool); 9] = [
            ("UPDATE task_runs SET output = 'x' WHERE run_id = ?1", "a task's output", false),
            ("UPDATE task_runs SET status = status WHERE run_id = ?1", "a status written unchanged", false),
            ("UPDATE workflow_runs SET output = 'x' WHERE id = ?1", "a run's output", false),
            ("UPDATE task_runs SET status = 'ready' WHERE run_id = ?1 AND name = 'a'", "a task's status", true),
            ("UPDATE task_runs SET attempt = attempt + 1 WHERE run_id = ?1 AND name = 'a'", "a task's attempt", true),
            ("UPDATE OR IGNORE task_runs SET status = 'running' WHERE run_id = ?1 AND name = 'a'", "a move under OR IGNORE", true),
            ("DELETE FROM task_runs WHERE run_id = ?1 AND name = 'b'", "a task not over taken away", true),
            ("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?1", "a run's status", true),
            ("DELETE FROM task_runs WHERE run_id = ?1", "a task over taken away (GC's)", false),
        ];
        for (i, (text, what, written)) in cases.into_iter().enumerate() {
            if i == 6 {
                // What a fan-out's re-expansion takes away first.
                sql(
                    &pool,
                    "DELETE FROM task_dependencies
                     WHERE dependent_id IN (SELECT id FROM task_runs WHERE run_id = ?1)",
                    &[&run],
                )
                .await;
            }
            if i == 8 {
                sql(
                    &pool,
                    "UPDATE task_runs SET status = 'succeeded' WHERE run_id = ?1",
                    &[&run],
                )
                .await;
            }
            let at = newest(&pool).await;
            sql(&pool, text, &[&run]).await;
            let got = past(&pool, at).await;
            if written {
                assert_eq!(got, vec![run.clone()], "{what}");
            } else {
                assert!(got.is_empty(), "{what}: {got:?}");
            }
        }
        // One row a run, at its latest change.
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_changes WHERE run_id = ?")
            .bind(&run)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 1);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A change rolled back leaves nothing, and the next change's `seq` is
    /// past every one a reader has read: what a reader reads in `seq` order
    /// is what was committed, in order.
    #[tokio::test]
    async fn a_change_rolled_back_leaves_nothing_and_seq_stays_in_order() {
        let (pool, path) = temp_pool().await;
        let run = create_run(&pool, &DagGraph::from_yaml(TWO).unwrap(), TWO)
            .await
            .unwrap();
        let at = newest(&pool).await;
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("UPDATE task_runs SET status = 'ready' WHERE run_id = ?")
            .bind(&run)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(newest(&pool).await, at);
        assert!(past(&pool, at).await.is_empty());
        sql(
            &pool,
            "UPDATE task_runs SET status = 'ready' WHERE run_id = ?1",
            &[&run],
        )
        .await;
        match run_changes_after(&pool, at, 10).await.unwrap() {
            RunChanges::Since(rows) => {
                assert_eq!(rows.len(), 1);
                assert!(rows[0].0 > at);
            }
            other => panic!("{other:?}"),
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The table is bounded, by the file alone: past RUN_CHANGES_KEPT
    /// changes, each 1,024th takes away what is that far behind it, and
    /// what is within it is all there. A cursor further behind is told so,
    /// and one past the newest.
    #[tokio::test]
    async fn the_table_is_bounded_and_a_reader_far_behind_is_told() {
        let (pool, path) = temp_pool().await;
        sql(
            &pool,
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200000)
             INSERT INTO run_changes (run_id) SELECT 'r' || i FROM n",
            &[],
        )
        .await;
        let top = newest(&pool).await;
        assert_eq!(top, 200_000);
        let (rows, lowest): (i64, i64) =
            sqlx::query_as("SELECT COUNT(*), MIN(seq) FROM run_changes")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(rows <= RUN_CHANGES_KEPT + 1024, "{rows} rows");
        assert!(lowest <= top - RUN_CHANGES_KEPT + 1, "lowest {lowest}");
        assert_eq!(
            run_changes_after(&pool, top - RUN_CHANGES_KEPT - 1, 1)
                .await
                .unwrap(),
            RunChanges::Behind { newest: top }
        );
        // A cursor past the newest: the file is older than what was read.
        assert_eq!(
            run_changes_after(&pool, top + 1, 1).await.unwrap(),
            RunChanges::Behind { newest: top }
        );
        assert_eq!(
            run_changes_after(&pool, top, 1).await.unwrap(),
            RunChanges::Since(Vec::new())
        );
        let edge = top - RUN_CHANGES_KEPT;
        match run_changes_after(&pool, edge, RUN_CHANGES_KEPT + 1)
            .await
            .unwrap()
        {
            RunChanges::Since(rows) => {
                assert_eq!(
                    rows.len() as i64,
                    RUN_CHANGES_KEPT,
                    "every change past the edge"
                );
                assert_eq!(rows[0].0, edge + 1);
            }
            other => panic!("{other:?}"),
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The reads of it, and the triggers' own, by their keys: the pump's
    /// read past its cursor never reads the table whole, and a change finds
    /// the run's row by its unique key.
    #[tokio::test]
    async fn the_reads_of_run_changes_are_by_their_keys() {
        let (pool, path) = temp_pool().await;
        for (sql, want) in [
            (RUN_CHANGES_SINCE, "USING INTEGER PRIMARY KEY (rowid>?)"),
            (
                "DELETE FROM run_changes WHERE run_id = ?",
                "INDEX sqlite_autoindex_run_changes_1 (run_id=?)",
            ),
            (
                "DELETE FROM run_changes WHERE seq <= ?",
                "USING INTEGER PRIMARY KEY (rowid<?)",
            ),
        ] {
            let plan: Vec<(i64, i64, i64, String)> =
                sqlx::query_as(&format!("EXPLAIN QUERY PLAN {sql}"))
                    .persistent(false)
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            let plan: Vec<String> = plan.into_iter().map(|r| r.3).collect();
            assert!(
                plan.len() == 1
                    && plan[0].starts_with("SEARCH run_changes ")
                    && plan[0].ends_with(want),
                "{sql}: {plan:?}"
            );
        }
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The read of every live run and its cursor are of one snapshot: a
    /// change after it is past the cursor.
    #[tokio::test]
    async fn a_read_of_every_live_run_hands_a_cursor_past_which_is_what_came_after() {
        let (pool, path) = temp_pool().await;
        let run = create_run(&pool, &DagGraph::from_yaml(TWO).unwrap(), TWO)
            .await
            .unwrap();
        let since = (chrono::Utc::now() - chrono::TimeDelta::seconds(30)).to_rfc3339();
        let (cursor, live) = live_stream_runs(&pool, &since).await.unwrap();
        assert_eq!(live, vec![run.clone()]);
        assert_eq!(cursor, newest(&pool).await);
        assert!(past(&pool, cursor).await.is_empty());
        sql(
            &pool,
            "UPDATE task_runs SET status = 'ready' WHERE run_id = ?1",
            &[&run],
        )
        .await;
        assert_eq!(past(&pool, cursor).await, [run]);
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
