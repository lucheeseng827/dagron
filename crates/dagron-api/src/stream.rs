//! Shared Postgres `task_events` listener → broadcast pump.
//!
//! ONE listener connection feeds a `tokio::sync::broadcast` channel that every
//! SSE client subscribes to (and filters by run_id). This is the N-replica-safe
//! fan-out — a per-client listener would burn one DB connection per browser tab.
//!
//! SQLite has no LISTEN/NOTIFY: there the pump polls,
//! and sends the same event for each run whose status, or a task's, changed,
//! read from the runs the file's triggers write to `run_changes`, past a
//! cursor of its own.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "postgres")]
use sqlx::postgres::PgListener;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::db::Pool;
use crate::state::TaskEvent;

/// NOTIFY channel the engine fires on every task-state change (payload = run_id).
#[cfg(feature = "postgres")]
const EVENT_CHANNEL: &str = "task_events";

/// Spawn the background listener. Connects a dedicated `PgListener`, subscribes
/// to `task_events`, and forwards each notification's run_id payload onto `tx`.
///
/// Resilient: `PgListener` auto-reconnects on transient drops; on a hard error
/// we log, back off briefly, and rebuild the listener. A zero-receiver broadcast
/// just drops sends, so it is safe to run with no clients connected.
#[cfg(feature = "postgres")]
pub fn spawn_listener(pool: Pool, tx: broadcast::Sender<TaskEvent>, ready: Arc<AtomicBool>) {
    tokio::spawn(async move {
        loop {
            match run_listener(&pool, &tx, &ready).await {
                Ok(()) => {
                    // run_listener only returns Ok on a clean shutdown path; in
                    // practice it loops forever. Treat as a signal to stop —
                    // and the bridge is dark from here on, so the flag must
                    // not keep advertising it as live.
                    ready.store(false, Ordering::Release);
                    info!("task_events listener stopped");
                    return;
                }
                Err(err) => {
                    // The flag comes down for the whole reconnect window, so
                    // /readyz reports the replica unready while its live-event
                    // bridge is dark (LOW_LATENCY R-1).
                    ready.store(false, Ordering::Release);
                    warn!(error = ?err, "task_events listener errored; reconnecting in 1s");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

#[cfg(feature = "postgres")]
async fn run_listener(
    pool: &Pool,
    tx: &broadcast::Sender<TaskEvent>,
    ready: &AtomicBool,
) -> anyhow::Result<()> {
    // DATABASE_LISTEN_URL: on a shared state-store cell the query pools point
    // at PgBouncer (transaction mode), which cannot serve a session-scoped
    // LISTEN — this listener must hit the direct Postgres endpoint
    // (the split-DSN setup). Unset = share the pool config.
    let mut listener = match std::env::var("DATABASE_LISTEN_URL") {
        Ok(url) if !url.trim().is_empty() => PgListener::connect(url.trim()).await?,
        _ => PgListener::connect_with(pool).await?,
    };
    listener.listen(EVENT_CHANNEL).await?;
    ready.store(true, Ordering::Release);
    info!(channel = EVENT_CHANNEL, "SSE listener subscribed");

    loop {
        // recv() returns Err on a connection error that PgListener could not
        // transparently recover — propagate so the outer loop rebuilds it.
        let notification = listener.recv().await?;
        let run_id = notification.payload().to_string();
        // send() errs only when there are no receivers — that's fine, drop it.
        let _ = tx.send(TaskEvent { run_id });
    }
}

/// SQLite's pump: every `DAGRON_SQLITE_POLL_MS` (500 by default), the runs
/// written to `run_changes` since the last read are sent, each once, as a
/// NOTIFY would have sent them. The file's triggers
/// (dagron-core's migrations/081) write a run there when it is made, its
/// status changes, or a task of it is made, moves (its status or attempt)
/// or, not over, is taken away; the pump keeps a cursor, the last `seq` it
/// read, and reads past it by its key, never every run.
///
/// At its start, and when its cursor is so far behind that rows past it may
/// have been taken away, or past the newest `seq`, as when the file was put
/// back from an older copy (`RunChanges::Behind`), it reads every run not
/// finished, or finished in the last half minute, and sends each, as each
/// read did before 081; the cursor is the newest `seq` of the same
/// snapshot, so nothing written after the read is missed. It writes
/// nothing, so two dagron-api processes on one file each read every change.
#[cfg(feature = "sqlite")]
pub fn spawn_listener(pool: Pool, tx: broadcast::Sender<TaskEvent>, ready: Arc<AtomicBool>) {
    let every = Duration::from_millis(
        std::env::var("DAGRON_SQLITE_POLL_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(500u64)
            .max(50),
    );
    tokio::spawn(async move {
        let mut cursor = None;
        let mut ticks = tokio::time::interval(every);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            poll_ms = every.as_millis() as u64,
            "SSE pump reading the SQLite datastore's run changes"
        );
        loop {
            ticks.tick().await;
            match changed_runs(&pool, &mut cursor).await {
                Ok(runs) => {
                    ready.store(true, Ordering::Release);
                    for run_id in runs {
                        let _ = tx.send(TaskEvent { run_id });
                    }
                }
                Err(err) => {
                    ready.store(false, Ordering::Release);
                    warn!(error = ?err, "SSE pump could not read the datastore; retrying");
                }
            }
        }
    });
}

/// The most rows of `run_changes` one read takes.
#[cfg(feature = "sqlite")]
const CHANGES_PER_READ: i64 = 4096;

/// The most reads one poll makes: what is past them is the next poll's.
#[cfg(feature = "sqlite")]
const READS_PER_POLL: usize = 8;

/// How long a finished run is still sent by a read of every live run: long
/// enough that a client sees a run end, short enough that the read stays
/// small.
#[cfg(feature = "sqlite")]
const FINISHED_SHOWN: chrono::TimeDelta = chrono::TimeDelta::seconds(30);

/// The runs to send for one poll, each once, in the order they changed.
/// `cursor` is the last `seq` read: `None` before the first read, and after
/// the pump fell behind. An error leaves it where it was: the poll's reads
/// are made past a copy of it, kept only when every one of them was read,
/// so a read that fails sends nothing of the poll, and the next poll reads
/// it all again.
#[cfg(feature = "sqlite")]
async fn changed_runs(pool: &Pool, cursor: &mut Option<i64>) -> anyhow::Result<Vec<String>> {
    use dagron_core::db::{self, RunChanges};

    let mut at = *cursor;
    let mut runs = Vec::new();
    let mut sent = std::collections::HashSet::new();
    for _ in 0..READS_PER_POLL {
        let Some(after) = at else {
            let since = (chrono::Utc::now() - FINISHED_SHOWN).to_rfc3339();
            let (newest, live) = db::live_stream_runs(pool, &since).await?;
            at = Some(newest);
            for run in live {
                if sent.insert(run.clone()) {
                    runs.push(run);
                }
            }
            continue;
        };
        match db::run_changes_after(pool, after, CHANGES_PER_READ).await? {
            RunChanges::Behind { newest } => {
                warn!(
                    cursor = after,
                    newest,
                    "SSE pump lost its place in the run changes; reading every live run again"
                );
                at = None;
            }
            RunChanges::Since(changes) => {
                let full = changes.len() as i64 == CHANGES_PER_READ;
                for (seq, run) in changes {
                    at = Some(seq);
                    if sent.insert(run.clone()) {
                        runs.push(run);
                    }
                }
                if !full {
                    break;
                }
            }
        }
    }
    *cursor = at;
    Ok(runs)
}

#[cfg(all(test, feature = "sqlite"))]
mod sqlite_tests {
    use super::*;

    const YAML: &str = "name: pump\ntasks:\n  - name: a\n    command: [\"true\"]\n";

    async fn pool() -> Pool {
        let path = std::env::temp_dir().join(format!("dagron-api-pump-{}.db", uuid::Uuid::new_v4().simple()));
        crate::db::connect(path.to_str().unwrap()).await.unwrap()
    }

    /// Close `pool` and take its file away, with its WAL and shared memory.
    async fn done(pool: Pool) {
        let path = pool.connect_options().get_filename().to_owned();
        pool.close().await;
        for end in ["", "-wal", "-shm"] {
            let mut file = path.clone().into_os_string();
            file.push(end);
            let _ = std::fs::remove_file(file);
        }
    }

    async fn run_of(pool: &Pool) -> String {
        let dag = dagron_core::dag::DagGraph::from_yaml(YAML).unwrap();
        dagron_core::db::create_run(pool, &dag, YAML).await.unwrap()
    }

    async fn sql(pool: &Pool, text: &str, binds: &[&str]) {
        let mut q = sqlx::query(text);
        for b in binds {
            q = q.bind(*b);
        }
        q.execute(pool).await.unwrap();
    }

    async fn poll(pool: &Pool, cursor: &mut Option<i64>) -> Vec<String> {
        changed_runs(pool, cursor).await.unwrap()
    }

    /// The first read sends every live run; a read with nothing changed
    /// sends nothing; a task's move, a run's end and a run made are sent,
    /// each once a read however often it changed; a write the stream never
    /// sent for (a task's output) is not sent.
    #[tokio::test]
    async fn a_run_is_sent_again_only_when_it_or_a_task_moves() {
        let pool = pool().await;
        let run = run_of(&pool).await;
        let mut cursor = None;
        assert_eq!(poll(&pool, &mut cursor).await, vec![run.clone()]);
        assert!(poll(&pool, &mut cursor).await.is_empty(), "nothing moved");
        sql(
            &pool,
            "UPDATE task_runs SET output = 'x' WHERE run_id = $1",
            &[&run],
        )
        .await;
        assert!(
            poll(&pool, &mut cursor).await.is_empty(),
            "an output is not a move"
        );
        sql(
            &pool,
            "UPDATE task_runs SET status = 'running', attempt = 1 WHERE run_id = $1",
            &[&run],
        )
        .await;
        sql(
            &pool,
            "UPDATE task_runs SET status = 'succeeded' WHERE run_id = $1",
            &[&run],
        )
        .await;
        assert_eq!(
            poll(&pool, &mut cursor).await,
            vec![run.clone()],
            "a task moved, twice"
        );
        let now = chrono::Utc::now().to_rfc3339();
        sql(
            &pool,
            "UPDATE workflow_runs SET status = 'succeeded', finished_at = $1 WHERE id = $2",
            &[&now, &run],
        )
        .await;
        let other = run_of(&pool).await;
        assert_eq!(poll(&pool, &mut cursor).await, [run.clone(), other]);
        assert!(poll(&pool, &mut cursor).await.is_empty());
        done(pool).await;
    }

    /// A run long finished is not read at a start, as before.
    #[tokio::test]
    async fn a_start_reads_the_live_runs_alone() {
        let pool = pool().await;
        let old = run_of(&pool).await;
        let ago = (chrono::Utc::now() - chrono::TimeDelta::minutes(5)).to_rfc3339();
        sql(
            &pool,
            "UPDATE workflow_runs SET status = 'succeeded', finished_at = $1 WHERE id = $2",
            &[&ago, &old],
        )
        .await;
        let live = run_of(&pool).await;
        let mut cursor = None;
        assert_eq!(poll(&pool, &mut cursor).await, [live]);
        done(pool).await;
    }

    /// Two readers of one file each read every change, and a reader made
    /// again (dagron-api restarted) reads what is live and then what moves.
    #[tokio::test]
    async fn two_pumps_each_read_every_change_and_a_restart_misses_none() {
        let pool = pool().await;
        let run = run_of(&pool).await;
        let (mut a, mut b) = (None, None);
        poll(&pool, &mut a).await;
        poll(&pool, &mut b).await;
        sql(
            &pool,
            "UPDATE task_runs SET status = 'running', attempt = 1 WHERE run_id = $1",
            &[&run],
        )
        .await;
        assert_eq!(poll(&pool, &mut a).await, vec![run.clone()]);
        assert_eq!(
            poll(&pool, &mut b).await,
            vec![run.clone()],
            "a's read took nothing from b"
        );
        // Restarted between a change and its read: the start reads it.
        sql(
            &pool,
            "UPDATE task_runs SET status = 'succeeded' WHERE run_id = $1",
            &[&run],
        )
        .await;
        let mut again = None;
        assert_eq!(poll(&pool, &mut again).await, vec![run.clone()]);
        let other = run_of(&pool).await;
        assert_eq!(poll(&pool, &mut again).await, [other]);
        done(pool).await;
    }

    /// A pump further behind than the file keeps reads every live run
    /// again, and from there reads past its new cursor.
    #[tokio::test]
    async fn a_pump_that_fell_behind_reads_every_live_run_again() {
        let pool = pool().await;
        let run = run_of(&pool).await;
        let mut cursor = None;
        poll(&pool, &mut cursor).await;
        // More changes than the file keeps, of runs that are not there.
        sql(
            &pool,
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 70000)
             INSERT INTO run_changes (run_id) SELECT 'gone-' || i FROM n",
            &[],
        )
        .await;
        let kept: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_changes")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            kept <= dagron_core::db::RUN_CHANGES_KEPT + 1024,
            "{kept} rows kept"
        );
        assert_eq!(
            poll(&pool, &mut cursor).await,
            vec![run.clone()],
            "every live run, again"
        );
        let newest: i64 =
            sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'run_changes'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(cursor, Some(newest));
        sql(
            &pool,
            "UPDATE task_runs SET status = 'running', attempt = 1 WHERE run_id = $1",
            &[&run],
        )
        .await;
        assert_eq!(poll(&pool, &mut cursor).await, [run]);
        done(pool).await;
    }

    /// A cursor past the newest `seq`, as when the file was put back from
    /// an older copy, is read as one behind: every live run, and the
    /// cursor set back to the newest.
    #[tokio::test]
    async fn a_cursor_past_the_newest_reads_every_live_run_again() {
        let pool = pool().await;
        let run = run_of(&pool).await;
        let mut cursor = None;
        poll(&pool, &mut cursor).await;
        let newest = cursor.unwrap();
        cursor = Some(newest + 100);
        assert_eq!(
            poll(&pool, &mut cursor).await,
            vec![run.clone()],
            "every live run, again"
        );
        assert_eq!(cursor, Some(newest));
        sql(
            &pool,
            "UPDATE task_runs SET status = 'running', attempt = 1 WHERE run_id = $1",
            &[&run],
        )
        .await;
        assert_eq!(poll(&pool, &mut cursor).await, [run]);
        done(pool).await;
    }

    /// A read that fails part way through a poll sends nothing of it and
    /// leaves the cursor where it was, so the poll after it sends what the
    /// failed one had read: past a start's read, and past a full read. The
    /// failure is a row the read cannot take, a BLOB where a run's id is
    /// TEXT, taken away again after.
    #[tokio::test]
    async fn a_poll_that_fails_part_way_loses_nothing() {
        let pool = pool().await;
        let run = run_of(&pool).await;
        let bad = "INSERT INTO run_changes (run_id) VALUES (x'00')";
        let fixed = "DELETE FROM run_changes WHERE typeof(run_id) = 'blob'";
        // At a start: the newest `seq` is held back, so the read past the
        // start's finds the bad row.
        sql(&pool, bad, &[]).await;
        sql(
            &pool,
            "UPDATE sqlite_sequence SET seq = seq - 1 WHERE name = 'run_changes'",
            &[],
        )
        .await;
        let mut cursor = None;
        assert!(changed_runs(&pool, &mut cursor).await.is_err());
        assert_eq!(cursor, None, "a start not taken");
        sql(&pool, fixed, &[]).await;
        assert_eq!(poll(&pool, &mut cursor).await, vec![run.clone()]);
        // Past a full read: the run's change is in it, the bad row past it.
        sql(
            &pool,
            "UPDATE task_runs SET status = 'running', attempt = 1 WHERE run_id = $1",
            &[&run],
        )
        .await;
        sql(
            &pool,
            &format!(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {})
                 INSERT INTO run_changes (run_id) SELECT 'r-' || i FROM n",
                CHANGES_PER_READ - 1
            ),
            &[],
        )
        .await;
        sql(&pool, bad, &[]).await;
        let at = cursor;
        assert!(changed_runs(&pool, &mut cursor).await.is_err());
        assert_eq!(cursor, at, "the full read not taken");
        sql(&pool, fixed, &[]).await;
        let sent = poll(&pool, &mut cursor).await;
        assert_eq!(sent.len() as i64, CHANGES_PER_READ);
        assert_eq!(sent[0], run);
        done(pool).await;
    }

    /// A poll takes what changed in reads of a bounded size, and what is
    /// past them is the next poll's.
    #[tokio::test]
    async fn a_poll_reads_a_bounded_number_of_changes() {
        let pool = pool().await;
        let mut cursor = None;
        poll(&pool, &mut cursor).await;
        let most = CHANGES_PER_READ as usize * READS_PER_POLL;
        sql(
            &pool,
            &format!(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {})
                 INSERT INTO run_changes (run_id) SELECT 'r-' || i FROM n",
                most + 10
            ),
            &[],
        )
        .await;
        assert_eq!(poll(&pool, &mut cursor).await.len(), most);
        assert_eq!(poll(&pool, &mut cursor).await.len(), 10);
        done(pool).await;
    }

    /// The pump sends the run whose task moved, as a NOTIFY would have.
    #[tokio::test]
    async fn the_pump_sends_a_run_whose_task_moved() {
        std::env::set_var("DAGRON_SQLITE_POLL_MS", "50");
        let pool = pool().await;
        let run = run_of(&pool).await;
        let (tx, mut rx) = broadcast::channel(16);
        let ready = Arc::new(AtomicBool::new(false));
        spawn_listener(pool.clone(), tx, Arc::clone(&ready));
        // The first poll sends what it reads.
        let first = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!(first.run_id, run);
        assert!(ready.load(Ordering::Acquire));
        sqlx::query("UPDATE task_runs SET status = 'succeeded', attempt = 1 WHERE run_id = $1")
            .bind(&run)
            .execute(&pool)
            .await
            .unwrap();
        let moved = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!(moved.run_id, run);
        done(pool).await;
    }
}
