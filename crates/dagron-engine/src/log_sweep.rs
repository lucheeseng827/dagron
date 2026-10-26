//! The log store's GC (docs/LOG_PIPELINE.md §7.4): what the log store keeps
//! of a task goes once no reader can name it.
//!
//! Everything the store keeps of a task, every attempt and every writing of
//! its log, is under one name of its own (`<task>/`). What goes is what no
//! row names: the whole of a task whose row is gone, or, while its row is
//! there, the writings below the one its row is on (`LOG_FENCE`, `LOG_GEN`,
//! which only rise). The row decides, never why the task was recorded.
//!
//! A task to look at is recorded in the database, in the statement or the
//! transaction that makes it one (`log_sweeps`): a trigger on the deletion of
//! a task row whose log the store holds (`task_runs.log_writings`), and a
//! reset that begins a log again over a writing kept before
//! (`db::reset_task_log`). A crash cannot keep the one and lose the other.
//! Every engine with a store sweeps, leader or not: a record is taken by a
//! claim that passes over what another holds, for [`LEASE_SECS`], and is
//! finished only at the stamp its claim left, so a newer record of the task
//! is never lost and a claim that lapsed cannot finish.
//!
//! A superseded writing's record is due at once, taken before any other, and
//! the engine whose reset made it wakes its sweeper: a seal's writing before,
//! which can hold a secret that masking a chunk at a time split, goes within
//! moments, as it did when the batch removed it itself. A deleted task's is
//! due a minute on. A record goes only when a pass finds nothing left; one
//! that took something is looked at again [`RELOOK_SECS`] on, past the
//! longest a worker waits for an executor whose claim is gone, so an engine
//! still writing for a task that is gone is caught. A removal that fails is
//! tried again later, from a minute to six hours on, and is counted.
//!
//! The store's I/O is made in no transaction and holds no connection, and a
//! removal can be made again.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use dagron_artifact::log_segments::{Removed, SegmentStore, Writing};
use dagron_core::metrics::Metrics;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::db;

/// How often an engine with a log store sweeps it, in seconds; 0, never:
/// what is recorded waits for an engine that does.
pub(crate) const ENV_SWEEP_SECS: &str = "DAGRON_LOG_SWEEP_SECS";
const DEFAULT_SWEEP_SECS: u64 = 30;
/// Records a claim takes, and claims a pass makes at most.
const BATCH: i64 = 100;
const CLAIMS_A_PASS: usize = 50;
/// Removals made at once.
const AT_ONCE: usize = 8;
/// How long a claim holds its records: another sweeper takes them after.
const LEASE_SECS: i64 = 600;
/// When a record whose removal took something is looked at again: past the
/// longest a worker waits for an executor whose claim is gone (its stop's
/// grace and two minutes), so that what such an engine wrote since goes too.
const RELOOK_SECS: i64 = dagron_executor::executor::MAX_STOP_GRACE_SECS as i64 + 300;
/// A removal that failed is tried again this long after, twice as long at
/// each failure, up to the most.
const BACKOFF_FIRST_SECS: i64 = 60;
const BACKOFF_MOST_SECS: i64 = 6 * 3600;
/// A task whose removal failed this many times is named in a warning.
const WARN_AT_TRIES: i64 = 10;

/// An engine's sweeper of its log store.
pub(crate) struct Sweeper {
    pool: db::Pool,
    store: SegmentStore,
    metrics: Arc<Metrics>,
    wake: Arc<Notify>,
    every: Duration,
}

/// What a pass did: records let go, records to look at again, removals that
/// failed, and what was removed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Swept {
    pub retired: usize,
    pub relooked: usize,
    pub failed: usize,
    pub entries: u64,
    pub bytes: u64,
}

/// [`ENV_SWEEP_SECS`], or its default where unset.
fn sweep_secs(v: Option<&str>) -> anyhow::Result<u64> {
    match v.map(str::trim).filter(|v| !v.is_empty()) {
        None => Ok(DEFAULT_SWEEP_SECS),
        Some(v) => v
            .parse()
            .map_err(|_| anyhow!("{ENV_SWEEP_SECS}={v:?} is not a number of seconds")),
    }
}

/// The store's GC for this engine: a sweeper, where it has a log store and
/// sweeps; where it has none, a word at its start if records wait, for they
/// wait for an engine that has one. A setting that is no number stops it.
pub(crate) fn start(
    pool: &db::Pool,
    store: Option<SegmentStore>,
    metrics: Arc<Metrics>,
    wake: Arc<Notify>,
) -> anyhow::Result<()> {
    let every = sweep_secs(std::env::var(ENV_SWEEP_SECS).ok().as_deref())?;
    match store {
        Some(store) if every > 0 => {
            info!(every_secs = every, "the log store's GC sweeps it");
            let sweeper = Sweeper::new(
                pool.clone(),
                store,
                metrics,
                wake,
                Duration::from_secs(every),
            );
            tokio::spawn(sweeper.run());
        }
        Some(_) => info!(
            "{ENV_SWEEP_SECS}=0: this engine removes nothing from the log store; what is \
             recorded for it waits for an engine that does"
        ),
        None => {
            tokio::spawn(warn_if_waiting(pool.clone()));
        }
    }
    Ok(())
}

/// Said once, by an engine with no log store: records wait that only an
/// engine with one takes.
async fn warn_if_waiting(pool: db::Pool) {
    match db::log_sweeps_waiting(&pool).await {
        Ok(w) if w.waiting > 0 => warn!(
            waiting = w.waiting,
            "tasks wait for the log store's GC (log_sweeps): they are for an engine that has a \
             log store (DAGRON_LOG_STORE); this one removes nothing"
        ),
        Ok(_) => {}
        Err(e) => debug!(error = %e, "the log store's GC records could not be counted"),
    }
}

/// How long after its `tries`-th failure a removal is tried again.
fn backoff_secs(tries: i64) -> i64 {
    BACKOFF_FIRST_SECS
        .saturating_mul(1i64 << tries.clamp(0, 20))
        .min(BACKOFF_MOST_SECS)
}

impl Sweeper {
    pub(crate) fn new(
        pool: db::Pool,
        store: SegmentStore,
        metrics: Arc<Metrics>,
        wake: Arc<Notify>,
        every: Duration,
    ) -> Sweeper {
        Sweeper {
            pool,
            store,
            metrics,
            wake,
            every,
        }
    }

    /// A pass every `every`, and one as soon as a reset wakes it.
    pub(crate) async fn run(self) {
        loop {
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(self.every) => {}
            }
            if let Err(e) = self.pass().await {
                warn!(error = %format!("{e:#}"), "a pass of the log store's GC failed");
            }
        }
    }

    /// Take what is due, a batch at a time, superseded writings first, and
    /// remove what no row names of each. A pass goes on while a batch was
    /// full and something in it went: a store that does not answer is not
    /// asked again and again.
    pub(crate) async fn pass(&self) -> anyhow::Result<Swept> {
        let mut swept = Swept::default();
        let mut first_error = None;
        for _ in 0..CLAIMS_A_PASS {
            let mut claimed =
                db::claim_log_sweeps(&self.pool, BATCH, LEASE_SECS, db::ClaimFirst::Superseded)
                    .await?;
            let more = BATCH - claimed.len() as i64;
            if more > 0 {
                claimed.extend(
                    db::claim_log_sweeps(&self.pool, more, LEASE_SECS, db::ClaimFirst::Any).await?,
                );
            }
            if claimed.is_empty() {
                break;
            }
            let full = claimed.len() as i64 == BATCH;
            let ids: Vec<String> = claimed.iter().map(|s| s.task_id.clone()).collect();
            // The row decides: what its writing is, or that there is none.
            let current: HashMap<String, Writing> = db::log_writings_of(&self.pool, &ids)
                .await?
                .into_iter()
                .map(|(task, fence, gen)| (task, Writing { fence, gen }))
                .collect();
            let (mut retire, mut relook, mut failed) = (Vec::new(), Vec::new(), Vec::new());
            for (sweep, outcome) in self.remove(claimed, &current).await {
                match outcome {
                    Ok(removed) if removed.entries == 0 => {
                        retire.push((sweep.task_id, sweep.stamp))
                    }
                    Ok(removed) => {
                        swept.entries += removed.entries;
                        swept.bytes += removed.bytes;
                        self.metrics
                            .add_log_store_removed(removed.entries, removed.bytes);
                        relook.push((sweep.task_id, sweep.stamp));
                    }
                    Err(e) => failed.push((sweep, format!("{e:#}"))),
                }
            }
            db::finish_log_sweeps(&self.pool, &retire, &relook, RELOOK_SECS).await?;
            for (sweep, error) in &failed {
                db::defer_log_sweep(
                    &self.pool,
                    &sweep.task_id,
                    sweep.stamp,
                    backoff_secs(sweep.tries),
                    error,
                )
                .await?;
                if sweep.tries + 1 == WARN_AT_TRIES {
                    warn!(task_id = %sweep.task_id, tries = WARN_AT_TRIES, error = %error,
                        "the log store's GC has failed to remove this task's logs again and again");
                }
            }
            self.metrics.add_log_sweep_failures(failed.len());
            swept.retired += retire.len();
            swept.relooked += relook.len();
            swept.failed += failed.len();
            if first_error.is_none() {
                first_error = failed.into_iter().next().map(|(_, error)| error);
            }
            // On while a batch was full, and something of it went: there may
            // be more, and the store answers.
            if !full || retire.len() + relook.len() == 0 {
                break;
            }
        }
        if let Some(error) = first_error {
            warn!(failed = swept.failed, first = %error, "the log store's GC could not remove some tasks' logs; it tries again later");
        }
        if swept.entries > 0 {
            info!(
                tasks = swept.relooked,
                entries = swept.entries,
                bytes = swept.bytes,
                "the log store's GC removed what no reader names"
            );
        }
        Ok(swept)
    }

    /// Each claimed record's removal, [`AT_ONCE`] at a time: against its row's
    /// writing, or, with no row, the whole task. A removal that panicked has
    /// no outcome, and its record is taken again once its claim lapses.
    async fn remove(
        &self,
        claimed: Vec<db::LogSweep>,
        current: &HashMap<String, Writing>,
    ) -> Vec<(db::LogSweep, anyhow::Result<Removed>)> {
        let mut out = Vec::with_capacity(claimed.len());
        let mut waiting = claimed.into_iter();
        let mut running = tokio::task::JoinSet::new();
        loop {
            while running.len() < AT_ONCE {
                let Some(sweep) = waiting.next() else {
                    break;
                };
                let store = self.store.clone();
                let writing = current.get(&sweep.task_id).copied();
                running.spawn(async move {
                    let removed = store.remove_log(&sweep.task_id, writing).await;
                    (sweep, removed)
                });
            }
            match running.join_next().await {
                Some(Ok(done)) => out.push(done),
                Some(Err(e)) => warn!(error = %e, "a removal from the log store did not finish"),
                None => break,
            }
        }
        out
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::log_batch::{Kept, LogBatch};
    use dagron_artifact::log_segments::{kept_logs, LogKey, TaskRow};
    use dagron_executor::executor::LogChunk;
    use std::path::{Path, PathBuf};

    /// Segments close at this much text, so a short log has a few.
    const SEGMENT: u64 = 4096;

    /// A fresh SQLite database with a run of `n` tasks, each running at
    /// `fence`, in the order of their names.
    async fn running(n: usize, fence: i64) -> (db::Pool, tempfile::TempDir, String, Vec<String>) {
        let dir = tempfile::tempdir().unwrap();
        let pool = db::init_pool(dir.path().join("e.db").to_str().unwrap())
            .await
            .unwrap();
        let tasks: String = (0..n)
            .map(|k| format!("  - name: t{k}\n    command: [\"true\"]\n"))
            .collect();
        let yaml = format!("name: gc\ntasks:\n{tasks}");
        let dag = dagron_core::dag::DagGraph::from_yaml(&yaml).unwrap();
        let run = db::create_run(&pool, &dag, &yaml).await.unwrap();
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = ?1 ORDER BY name")
                .bind(&run)
                .fetch_all(&pool)
                .await
                .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'running', version = ?1 WHERE run_id = ?2")
            .bind(fence)
            .bind(&run)
            .execute(&pool)
            .await
            .unwrap();
        (pool, dir, run, ids)
    }

    /// The stores a test runs over: a file:// directory, one whose open part
    /// is the row, and an object store in memory where this build has object
    /// stores; each with the directory it keeps its files in, where it has
    /// one.
    fn stores(at: &Path) -> Vec<(SegmentStore, Option<PathBuf>)> {
        let (dir, row) = (at.join("dir"), at.join("row"));
        let mut all = vec![
            (SegmentStore::Dir(dir.clone()), Some(dir)),
            (SegmentStore::DirOpenInRow(row.clone()), Some(row)),
        ];
        all.extend(SegmentStore::in_memory().map(|store| (store, None)));
        all
    }

    fn gc(pool: &db::Pool, store: &SegmentStore) -> Sweeper {
        Sweeper::new(
            pool.clone(),
            store.clone(),
            Arc::new(Metrics::new()),
            Arc::new(Notify::new()),
            Duration::from_secs(30),
        )
    }

    /// A batch that writes to `store` as the engine's does, waking `wake`.
    fn batch(store: &SegmentStore, wake: &Arc<Notify>) -> LogBatch {
        LogBatch::new(Some(Kept::new(store.clone(), SEGMENT).waking(wake.clone())))
    }

    /// One chunk of `task`'s log, written at once, as the loop writes what
    /// waits for a task whose result is in.
    async fn write(
        batch: &mut LogBatch,
        pool: &db::Pool,
        task: &str,
        fence: i64,
        text: &str,
        first: bool,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(LogChunk {
            task_id: task.to_string(),
            fence,
            chunk: text.to_string(),
            first,
        })
        .unwrap();
        batch.take(&mut rx);
        batch.write_for(pool, [task].into_iter()).await;
    }

    fn lines(tag: &str, n: usize) -> String {
        (0..n)
            .map(|k| format!("{tag} {k:04} {}\n", "x".repeat(40)))
            .collect()
    }

    /// `task`'s log as a reader reads it.
    async fn read(pool: &db::Pool, store: &SegmentStore, run: &str, task: &str) -> String {
        let rows = db::task_log_rows(pool, run).await.unwrap();
        let (id, fence, gen, log, _, segments) = rows.into_iter().find(|r| r.0 == task).unwrap();
        let row = [TaskRow {
            task_id: &id,
            fence,
            gen,
            log: log.as_deref(),
            segments,
        }];
        let kept = kept_logs(store, &row).await.unwrap();
        kept.get(task).cloned().or(log).unwrap_or_default()
    }

    /// Whether the store holds anything of writing `(fence, gen)` of `task`:
    /// its directory, or, in an object store, its first segment.
    async fn holds(
        store: &SegmentStore,
        files: &Option<PathBuf>,
        task: &str,
        fence: i64,
        gen: i64,
    ) -> bool {
        match files {
            Some(dir) => dir
                .join(task)
                .join(match gen {
                    0 => fence.to_string(),
                    gen => format!("{fence}.{gen}"),
                })
                .exists(),
            None => store
                .get_segment(&LogKey::new(task, fence, gen).segment(0))
                .await
                .is_ok(),
        }
    }

    /// `task`'s record: why, its stamp, its tries, in how many seconds it is
    /// due by the database's clock, and its last error.
    async fn record(
        pool: &db::Pool,
        task: &str,
    ) -> Option<(String, i64, i64, f64, Option<String>)> {
        sqlx::query_as(
            "SELECT reason, stamp, tries, (julianday(due_at) - julianday('now')) * 86400.0, \
             last_error FROM log_sweeps WHERE task_id = ?1",
        )
        .bind(task)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    /// Every record due, as time passing would make it.
    async fn all_due(pool: &db::Pool) {
        sqlx::query("UPDATE log_sweeps SET due_at = '1970-01-01T00:00:00.000+00:00'")
            .execute(pool)
            .await
            .unwrap();
    }

    async fn writings(pool: &db::Pool, task: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT log_writings FROM task_runs WHERE id = ?1")
            .bind(task)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn woken(wake: &Notify) -> bool {
        tokio::time::timeout(Duration::from_millis(50), wake.notified())
            .await
            .is_ok()
    }

    #[test]
    fn a_setting_is_a_number_of_seconds() {
        assert_eq!(sweep_secs(None).unwrap(), 30);
        assert_eq!(sweep_secs(Some(" ")).unwrap(), 30);
        assert_eq!(sweep_secs(Some("0")).unwrap(), 0);
        assert_eq!(sweep_secs(Some("5")).unwrap(), 5);
        assert!(sweep_secs(Some("30s")).is_err());
        assert_eq!(
            (0..5).map(backoff_secs).collect::<Vec<_>>(),
            [60, 120, 240, 480, 960]
        );
        assert_eq!(backoff_secs(60), 6 * 3600, "never more than six hours");
        assert_eq!(RELOOK_SECS, 900, "past the worker's twelve minutes");
    }

    #[tokio::test]
    async fn a_first_writing_makes_no_record() {
        let base = tempfile::tempdir().unwrap();
        for (store, _) in stores(base.path()) {
            let (pool, _db, _run, tasks) = running(1, 2).await;
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, &tasks[0], 2, &lines("a", 200), true).await;
            write(&mut b, &pool, &tasks[0], 2, "more\n", false).await;
            assert_eq!(writings(&pool, &tasks[0]).await, Some(1), "{store:?}");
            assert_eq!(record(&pool, &tasks[0]).await, None, "{store:?}");
            assert!(!woken(&wake).await, "{store:?}");
        }
    }

    #[tokio::test]
    async fn a_seal_lets_the_writing_before_go() {
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, run, tasks) = running(1, 2).await;
            let task = tasks[0].as_str();
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, task, 2, &lines("to***", 200), true).await;
            assert!(holds(&store, &files, task, 2, 1).await, "{store:?}");
            // The seal: its whole log again, masked.
            write(&mut b, &pool, task, 2, "masked\n", true).await;
            assert!(woken(&wake).await, "{store:?}: the reset woke the GC");
            let (why, stamp, tries, due, _) = record(&pool, task).await.unwrap();
            assert_eq!((why.as_str(), tries), ("superseded", 0), "{store:?}");
            assert!(due <= 1.0, "{store:?}: due at once, {due}");
            assert!(
                holds(&store, &files, task, 2, 1).await,
                "{store:?}: nothing removed as it was written"
            );
            assert_eq!(read(&pool, &store, &run, task).await, "masked\n");

            let swept = gc(&pool, &store).pass().await.unwrap();
            assert_eq!((swept.relooked, swept.failed), (1, 0), "{store:?}");
            assert!(swept.entries > 0 && swept.bytes > 0, "{store:?}: {swept:?}");
            assert!(!holds(&store, &files, task, 2, 1).await, "{store:?}");
            assert_eq!(read(&pool, &store, &run, task).await, "masked\n");
            let (_, after, _, due, _) = record(&pool, task).await.unwrap();
            assert_eq!(after, stamp + 2, "{store:?}: claimed, then finished");
            assert!(
                (890.0..=905.0).contains(&due),
                "{store:?}: looked at again, {due}"
            );

            // Looked at again, and nothing is left: let go.
            all_due(&pool).await;
            let swept = gc(&pool, &store).pass().await.unwrap();
            assert_eq!((swept.retired, swept.entries), (1, 0), "{store:?}");
            assert_eq!(record(&pool, task).await, None, "{store:?}");
            assert_eq!(read(&pool, &store, &run, task).await, "masked\n");
        }
    }

    #[tokio::test]
    async fn a_new_attempt_lets_the_earlier_go() {
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, run, tasks) = running(1, 2).await;
            let task = tasks[0].as_str();
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, task, 2, &lines("first", 200), true).await;
            let rows = |fence: i64| {
                let pool = pool.clone();
                async move {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM log_segments WHERE task_id = ?1 AND fence = ?2",
                    )
                    .bind(task)
                    .bind(fence)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                }
            };
            assert!(rows(2).await > 0, "{store:?}");
            // A retry's claim: the next attempt, its log not begun.
            sqlx::query(
                "UPDATE task_runs SET version = 3, log = NULL, log_fence = NULL, log_gen = NULL \
                 WHERE id = ?1",
            )
            .bind(task)
            .execute(&pool)
            .await
            .unwrap();
            write(&mut b, &pool, task, 3, &lines("second", 10), true).await;
            assert_eq!(
                rows(2).await,
                0,
                "{store:?}: the reset took the attempt before's rows"
            );
            assert!(holds(&store, &files, task, 2, 1).await, "{store:?}");
            gc(&pool, &store).pass().await.unwrap();
            assert!(!holds(&store, &files, task, 2, 1).await, "{store:?}");
            if let Some(dir) = &files {
                let left: Vec<String> = std::fs::read_dir(dir.join(task))
                    .unwrap()
                    .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                    .collect();
                assert!(
                    left.iter().all(|n| n.starts_with("3.")),
                    "{store:?}: {left:?}"
                );
            }
            assert_eq!(read(&pool, &store, &run, task).await, lines("second", 10));
        }
    }

    /// A task deleted while it is recorded already is recorded as gone, with
    /// a stamp past any claim's, no failure left, and due no later than it
    /// was: one due now stays due now, one claimed or waiting out a failure
    /// is due a minute on. A finish at the stamp before changes nothing.
    #[tokio::test]
    async fn a_task_deleted_while_recorded_is_recorded_gone() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("logs");
        let store = SegmentStore::Dir(dir);
        let (pool, _db, _run, tasks) = running(2, 2).await;
        let wake = Arc::new(Notify::new());
        let mut b = batch(&store, &wake);
        for task in &tasks {
            // A first writing, then a seal: the writing before is recorded.
            write(&mut b, &pool, task, 2, "a line\n", true).await;
            write(&mut b, &pool, task, 2, "a line, sealed\n", true).await;
        }
        let sweep = |pool: &db::Pool, task: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (String, i64, i64, Option<String>, f64)>(
                    "SELECT reason, stamp, tries, last_error, \
                     (julianday(due_at) - julianday('now')) * 86400.0 \
                     FROM log_sweeps WHERE task_id = ?1",
                )
                .bind(task)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };
        // One claimed (due when its lease ends), one waiting out a failure,
        // due an hour on.
        let taken = db::claim_log_sweeps(&pool, 1, 600, db::ClaimFirst::Any)
            .await
            .unwrap()
            .pop()
            .expect("claimed");
        assert_eq!(taken.reason, "superseded");
        let (claimed, stamp) = (taken.task_id, taken.stamp);
        let waiting = tasks.iter().find(|t| **t != claimed).unwrap().clone();
        sqlx::query(
            "UPDATE log_sweeps SET tries = 3, last_error = 'refused', stamp = stamp + 1, \
             due_at = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', '+3600 seconds') \
             WHERE task_id = ?1",
        )
        .bind(&waiting)
        .execute(&pool)
        .await
        .unwrap();
        for task in [&claimed, &waiting] {
            sqlx::query("DELETE FROM task_runs WHERE id = ?1")
                .bind(task)
                .execute(&pool)
                .await
                .unwrap();
        }
        let (why, now_stamp, tries, error, due) = sweep(&pool, claimed.clone()).await;
        assert_eq!((why.as_str(), tries, error), ("gone", 0, None));
        assert_eq!(now_stamp, stamp + 1);
        assert!((55.0..=65.0).contains(&due), "due a minute on: {due}");
        let (why, _, tries, error, due) = sweep(&pool, waiting.clone()).await;
        assert_eq!((why.as_str(), tries, error), ("gone", 0, None));
        assert!(
            (55.0..=65.0).contains(&due),
            "due a minute on, not an hour: {due}"
        );
        // The claim's finish, at its stamp, is too late.
        db::finish_log_sweeps(&pool, &[(claimed.clone(), stamp)], &[], RELOOK_SECS)
            .await
            .unwrap();
        assert_eq!(sweep(&pool, claimed).await.1, stamp + 1, "still recorded");

        // One due now stays due now.
        let (pool, _db, _run, tasks) = running(1, 2).await;
        write(&mut b, &pool, &tasks[0], 2, "a line\n", true).await;
        write(&mut b, &pool, &tasks[0], 2, "again\n", true).await;
        sqlx::query("DELETE FROM task_runs WHERE id = ?1")
            .bind(&tasks[0])
            .execute(&pool)
            .await
            .unwrap();
        let (why, stamp, _, _, due) = sweep(&pool, tasks[0].clone()).await;
        assert_eq!((why.as_str(), stamp), ("gone", 2));
        assert!(due <= 0.0, "still due now: {due}");
    }

    /// Every way a task row is deleted records a task whose log the store
    /// holds, due a minute on, and no task that wrote nothing there; once due,
    /// its whole name in the store goes.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn every_delete_path_records_a_task_that_wrote() {
        for path in [
            "gc_old_runs",
            "purge_runs_by_id",
            "rerun_all",
            "reconcile_fanouts",
            "a DELETE by hand",
        ] {
            let base = tempfile::tempdir().unwrap();
            let dir = base.path().join("logs");
            let (store, files) = (SegmentStore::Dir(dir.clone()), Some(dir.clone()));
            let (pool, _db, run, tasks) = running(3, 2).await;
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, &tasks[0], 2, &lines("a", 200), true).await;
            let sql = |q: &'static str| {
                let pool = pool.clone();
                async move { sqlx::query(q).execute(&pool).await.unwrap() }
            };
            match path {
                "gc_old_runs" => {
                    sql("UPDATE workflow_runs SET status = 'succeeded', \
                         finished_at = '2020-01-01T00:00:00+00:00'")
                    .await;
                    let now = chrono::Utc::now().to_rfc3339();
                    assert_eq!(db::gc_old_runs(&pool, &now).await.unwrap(), 1);
                }
                "purge_runs_by_id" => {
                    sql("UPDATE workflow_runs SET status = 'succeeded'").await;
                    let purged = db::purge_runs_by_id(&pool, std::slice::from_ref(&run));
                    assert_eq!(purged.await.unwrap(), 1);
                }
                "rerun_all" => {
                    // A runtime fan-out's instances go as the run is rerun.
                    sqlx::query("UPDATE task_runs SET fanout_parent = ?1 WHERE id IN (?2, ?3)")
                        .bind(&tasks[2])
                        .bind(&tasks[0])
                        .bind(&tasks[1])
                        .execute(&pool)
                        .await
                        .unwrap();
                    sql("UPDATE workflow_runs SET status = 'succeeded'").await;
                    assert!(db::rerun_all(&pool, &run).await.unwrap().is_some());
                }
                "reconcile_fanouts" => {
                    // A barrier expanded again: its instances of the epoch
                    // before go.
                    sqlx::query(
                        "UPDATE task_runs SET fanout_parent = ?1, fanout_epoch = 0 \
                         WHERE id IN (?2, ?3)",
                    )
                    .bind(&tasks[2])
                    .bind(&tasks[0])
                    .bind(&tasks[1])
                    .execute(&pool)
                    .await
                    .unwrap();
                    sqlx::query(
                        "UPDATE task_runs SET fanout_of = 'up', claimed_by = NULL, version = 5 \
                         WHERE id = ?1",
                    )
                    .bind(&tasks[2])
                    .execute(&pool)
                    .await
                    .unwrap();
                    db::reconcile_fanouts(&pool).await.unwrap();
                }
                _ => {
                    sqlx::query("DELETE FROM task_runs WHERE id IN (?1, ?2)")
                        .bind(&tasks[0])
                        .bind(&tasks[1])
                        .execute(&pool)
                        .await
                        .unwrap();
                }
            }
            let recorded: Vec<(String, String, f64)> = sqlx::query_as(
                "SELECT task_id, reason, (julianday(due_at) - julianday('now')) * 86400.0 \
                 FROM log_sweeps",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(recorded.len(), 1, "{path}: {recorded:?}");
            let (task, why, due) = &recorded[0];
            assert_eq!((task, why.as_str()), (&tasks[0], "gone"), "{path}");
            assert!(
                (55.0..=65.0).contains(due),
                "{path}: due a minute on, {due}"
            );

            let swept = gc(&pool, &store).pass().await.unwrap();
            assert_eq!(swept, Swept::default(), "{path}: nothing is due");
            assert!(holds(&store, &files, &tasks[0], 2, 1).await, "{path}");
            all_due(&pool).await;
            let swept = gc(&pool, &store).pass().await.unwrap();
            assert_eq!(swept.relooked, 1, "{path}");
            assert!(!dir.join(&tasks[0]).exists(), "{path}: the whole task goes");
            all_due(&pool).await;
            assert_eq!(gc(&pool, &store).pass().await.unwrap().retired, 1, "{path}");
            assert_eq!(record(&pool, &tasks[0]).await, None, "{path}");
        }
    }

    #[tokio::test]
    async fn a_writer_that_comes_back_is_swept_again() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("logs");
        let store = SegmentStore::Dir(dir.clone());
        let (pool, _db, _run, tasks) = running(1, 2).await;
        let task = tasks[0].as_str();
        let wake = Arc::new(Notify::new());
        let mut b = batch(&store, &wake);
        write(&mut b, &pool, task, 2, &lines("a", 20), true).await;
        write(&mut b, &pool, task, 2, "masked\n", true).await;
        assert_eq!(gc(&pool, &store).pass().await.unwrap().relooked, 1);
        assert!(!dir.join(task).join("2.1").exists());
        // An engine that had the writing before open goes on writing it.
        let open = dir.join(task).join("2.1").join("000000.open");
        std::fs::create_dir_all(open.parent().unwrap()).unwrap();
        std::fs::write(&open, "late\n").unwrap();
        all_due(&pool).await;
        let swept = gc(&pool, &store).pass().await.unwrap();
        assert_eq!(
            (swept.relooked, swept.entries),
            (1, 2),
            "the file and its directory"
        );
        assert!(!open.exists());
        assert!(
            record(&pool, task).await.is_some(),
            "kept, to be looked at again"
        );
        all_due(&pool).await;
        assert_eq!(gc(&pool, &store).pass().await.unwrap().retired, 1);
        assert_eq!(record(&pool, task).await, None);
    }

    /// A record says only that a task is to be looked at: while its row is
    /// there, what its row names stays, whatever the record says.
    #[tokio::test]
    async fn the_row_decides() {
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, run, tasks) = running(1, 3).await;
            let task = tasks[0].as_str();
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, task, 3, &lines("now", 200), true).await;
            for (fence, gen) in [(2, 1), (3, 0)] {
                let key = LogKey::new(task, fence, gen);
                store.put_segment(&key, 0, "before\n").await.unwrap();
            }
            sqlx::query(
                "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at) \
                 VALUES (?1, 'gone', '1970-01-01T00:00:00.000+00:00', '1970-01-01T00:00:00.000+00:00')",
            )
            .bind(task)
            .execute(&pool)
            .await
            .unwrap();
            gc(&pool, &store).pass().await.unwrap();
            assert!(!holds(&store, &files, task, 2, 1).await, "{store:?}");
            assert!(!holds(&store, &files, task, 3, 0).await, "{store:?}");
            assert!(holds(&store, &files, task, 3, 1).await, "{store:?}");
            assert_eq!(read(&pool, &store, &run, task).await, lines("now", 200));
        }
    }

    #[tokio::test]
    async fn a_reset_during_a_pass_is_not_lost() {
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, run, tasks) = running(1, 2).await;
            let task = tasks[0].as_str();
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, task, 2, &lines("a", 200), true).await;
            write(&mut b, &pool, task, 2, &lines("b", 200), true).await;
            let claimed = db::claim_log_sweeps(&pool, 10, 600, db::ClaimFirst::Any)
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1);
            // While the claim's removal runs, the log is sealed again.
            write(&mut b, &pool, task, 2, "masked\n", true).await;
            db::finish_log_sweeps(&pool, &[(task.to_string(), claimed[0].stamp)], &[], 900)
                .await
                .unwrap();
            let (_, stamp, _, due, _) = record(&pool, task).await.expect("not lost");
            assert_eq!(stamp, claimed[0].stamp + 1);
            assert!(due <= 1.0, "{store:?}: still due at once, {due}");
            gc(&pool, &store).pass().await.unwrap();
            assert!(!holds(&store, &files, task, 2, 1).await, "{store:?}");
            assert!(!holds(&store, &files, task, 2, 2).await, "{store:?}");
            assert_eq!(read(&pool, &store, &run, task).await, "masked\n");
        }
    }

    #[tokio::test]
    async fn a_claim_that_lapsed_cannot_be_finished_by_its_first_holder() {
        let (pool, _db, _run, tasks) = running(1, 2).await;
        let task = tasks[0].as_str();
        let base = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(base.path().to_path_buf());
        let wake = Arc::new(Notify::new());
        let mut b = batch(&store, &wake);
        write(&mut b, &pool, task, 2, "one\n", true).await;
        write(&mut b, &pool, task, 2, "two\n", true).await;
        let claim = |lease: i64| {
            let pool = pool.clone();
            async move {
                db::claim_log_sweeps(&pool, 10, lease, db::ClaimFirst::Any)
                    .await
                    .unwrap()
            }
        };
        let first = claim(600).await;
        assert_eq!(first.len(), 1);
        assert!(claim(600).await.is_empty(), "held");
        all_due(&pool).await;
        let second = claim(600).await;
        assert_eq!(second[0].stamp, first[0].stamp + 1);
        let stale = [(task.to_string(), first[0].stamp)];
        db::finish_log_sweeps(&pool, &stale, &[], 900)
            .await
            .unwrap();
        db::finish_log_sweeps(&pool, &[], &stale, 900)
            .await
            .unwrap();
        assert!(
            !db::defer_log_sweep(&pool, task, first[0].stamp, 60, "late")
                .await
                .unwrap()
        );
        let (_, stamp, tries, _, error) = record(&pool, task).await.unwrap();
        assert_eq!((stamp, tries, error), (second[0].stamp, 0, None));
        db::finish_log_sweeps(&pool, &[(task.to_string(), second[0].stamp)], &[], 900)
            .await
            .unwrap();
        assert_eq!(record(&pool, task).await, None);
    }

    #[tokio::test]
    async fn superseded_records_are_taken_first() {
        let (pool, _db, _run, tasks) = running(1, 2).await;
        for k in 0..300 {
            sqlx::query(
                "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at) \
                 VALUES (?1, 'gone', '2020-01-01T00:00:00.000+00:00', ?2)",
            )
            .bind(format!("gone-{k:03}"))
            .bind(format!(
                "2020-01-01T00:{:02}:{:02}.000+00:00",
                k / 60,
                k % 60
            ))
            .execute(&pool)
            .await
            .unwrap();
        }
        let base = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(base.path().to_path_buf());
        let wake = Arc::new(Notify::new());
        let mut b = batch(&store, &wake);
        write(&mut b, &pool, &tasks[0], 2, "one\n", true).await;
        write(&mut b, &pool, &tasks[0], 2, "two\n", true).await;
        let first = db::claim_log_sweeps(&pool, BATCH, LEASE_SECS, db::ClaimFirst::Superseded)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(
            (first[0].task_id.as_str(), first[0].reason.as_str()),
            (tasks[0].as_str(), "superseded")
        );
        let any = db::claim_log_sweeps(&pool, BATCH - 1, LEASE_SECS, db::ClaimFirst::Any)
            .await
            .unwrap();
        let mut ids: Vec<&str> = any.iter().map(|s| s.task_id.as_str()).collect();
        ids.sort_unstable();
        let earliest: Vec<String> = (0..99).map(|k| format!("gone-{k:03}")).collect();
        assert_eq!(ids, earliest, "the earliest due");
        // And a pass takes all of what is due, a batch at a time.
        all_due(&pool).await;
        let swept = gc(&pool, &store).pass().await.unwrap();
        assert_eq!(swept.retired, 300, "{swept:?}");
        assert_eq!(swept.relooked, 1, "{swept:?}");
    }

    #[tokio::test]
    async fn a_removal_that_fails_backs_off_and_is_kept() {
        let (pool, _db, _run, _) = running(1, 2).await;
        let base = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(base.path().to_path_buf());
        let mark = |task: String, due: &'static str| {
            let pool = pool.clone();
            async move {
                sqlx::query(
                    "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at) \
                     VALUES (?1, 'gone', ?2, ?2)",
                )
                .bind(task)
                .bind(due)
                .execute(&pool)
                .await
                .unwrap();
            }
        };
        // A task whose name is a file, not a directory, cannot be removed;
        // the one beside it can.
        std::fs::write(base.path().join("stuck"), b"x").unwrap();
        mark("stuck".into(), "1970-01-01T00:00:00.000+00:00").await;
        let healthy = base.path().join("healthy").join("1.1");
        std::fs::create_dir_all(&healthy).unwrap();
        std::fs::write(healthy.join("000000.open"), b"y").unwrap();
        mark("healthy".into(), "1970-01-01T00:00:00.000+00:00").await;
        let swept = gc(&pool, &store).pass().await.unwrap();
        assert_eq!((swept.failed, swept.relooked), (1, 1), "{swept:?}");
        let (_, _, tries, due, error) = record(&pool, "stuck").await.unwrap();
        assert_eq!(tries, 1);
        assert!(error.is_some_and(|e| e.contains("stuck")));
        assert!((55.0..=65.0).contains(&due), "a minute on: {due}");
        assert!(!base.path().join("healthy").exists());
        sqlx::query("UPDATE log_sweeps SET due_at = '1970-01-01T00:00:00.000+00:00' WHERE task_id = 'stuck'")
            .execute(&pool)
            .await
            .unwrap();
        gc(&pool, &store).pass().await.unwrap();
        let (_, _, tries, due, _) = record(&pool, "stuck").await.unwrap();
        assert_eq!(tries, 2);
        assert!((115.0..=125.0).contains(&due), "two minutes on: {due}");

        // A batch of which nothing could be removed ends the pass: a store
        // that does not answer is not asked again at once.
        for k in 0..150 {
            let task = format!("down-{k:03}");
            std::fs::write(base.path().join(&task), b"x").unwrap();
            mark(task, "1970-01-01T00:00:00.000+00:00").await;
        }
        let metrics = Arc::new(Metrics::new());
        let gc = Sweeper::new(
            pool.clone(),
            store.clone(),
            metrics.clone(),
            Arc::new(Notify::new()),
            Duration::from_secs(30),
        );
        let swept = gc.pass().await.unwrap();
        assert_eq!(swept.failed, BATCH as usize);
        assert_eq!(
            metrics
                .log_sweep_failures
                .load(std::sync::atomic::Ordering::Relaxed),
            BATCH as u64
        );
        let untried: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM log_sweeps WHERE task_id LIKE 'down-%' AND tries = 0",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(untried, 50);
    }

    /// An attempt the engine learnt from the database, not began (a first
    /// reset that failed, here a first chunk that does not say it is one), is
    /// marked before it is written: once its row is deleted, it is recorded,
    /// and goes.
    #[tokio::test]
    async fn an_engine_that_learns_an_attempt_marks_it_first() {
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, _run, tasks) = running(1, 2).await;
            let task = tasks[0].as_str();
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, task, 2, &lines("a", 200), false).await;
            assert_eq!(writings(&pool, task).await, Some(1), "{store:?}");
            assert!(holds(&store, &files, task, 2, 0).await, "{store:?}");
            sqlx::query("DELETE FROM task_runs WHERE id = ?1")
                .bind(task)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(record(&pool, task).await.unwrap().0, "gone", "{store:?}");
            all_due(&pool).await;
            gc(&pool, &store).pass().await.unwrap();
            assert!(!holds(&store, &files, task, 2, 0).await, "{store:?}");
            assert_eq!(
                store.remove_log(task, None).await.unwrap(),
                Removed::default(),
                "{store:?}: nothing left of it"
            );
        }
    }

    #[tokio::test]
    async fn a_deleted_task_is_not_written_again_by_an_engine_that_had_not_learnt_it() {
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, _run, tasks) = running(1, 2).await;
            let task = tasks[0].as_str();
            sqlx::query("DELETE FROM task_runs WHERE id = ?1")
                .bind(task)
                .execute(&pool)
                .await
                .unwrap();
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            write(&mut b, &pool, task, 2, &lines("late", 200), false).await;
            if let Some(dir) = &files {
                assert!(!dir.join(task).exists(), "{store:?}");
            }
            assert_eq!(
                store.remove_log(task, None).await.unwrap(),
                Removed::default(),
                "{store:?}: nothing was written"
            );
            assert_eq!(record(&pool, task).await, None, "{store:?}");
        }
    }

    #[tokio::test]
    async fn no_store_records_nothing() {
        let (pool, _db, _run, tasks) = running(2, 2).await;
        let mut b = LogBatch::new(None);
        write(&mut b, &pool, &tasks[0], 2, "one\n", true).await;
        write(&mut b, &pool, &tasks[0], 2, "two\n", true).await;
        sqlx::query("UPDATE task_runs SET version = 3 WHERE id = ?1")
            .bind(&tasks[0])
            .execute(&pool)
            .await
            .unwrap();
        write(&mut b, &pool, &tasks[0], 3, "three\n", true).await;
        assert_eq!(writings(&pool, &tasks[0]).await, None);
        sqlx::query("DELETE FROM task_runs")
            .execute(&pool)
            .await
            .unwrap();
        let none: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM log_sweeps")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(none, 0);
    }

    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn the_gauges_and_counters_move() {
        let (pool, _db, _run, _) = running(1, 2).await;
        let base = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(base.path().to_path_buf());
        std::fs::write(base.path().join("stuck"), b"x").unwrap();
        let kept = base.path().join("healthy").join("1.1");
        std::fs::create_dir_all(&kept).unwrap();
        std::fs::write(kept.join("000000.open"), b"twelve bytes").unwrap();
        for task in ["stuck", "healthy"] {
            sqlx::query(
                "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at) \
                 VALUES (?1, 'gone', '2020-01-01T00:00:00.000+00:00', '2020-01-01T00:00:00.000+00:00')",
            )
            .bind(task)
            .execute(&pool)
            .await
            .unwrap();
        }
        let metrics = Arc::new(Metrics::new());
        let gc = Sweeper::new(
            pool.clone(),
            store,
            metrics.clone(),
            Arc::new(Notify::new()),
            Duration::from_secs(30),
        );
        let swept = gc.pass().await.unwrap();
        let load = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(swept.entries, 3, "the file and two directories");
        assert_eq!(load(&metrics.log_store_removed_objects), swept.entries);
        assert_eq!(load(&metrics.log_store_removed_bytes), 12);
        assert_eq!(load(&metrics.log_sweep_failures), 1);
        let snap = db::status_counts(&pool).await.unwrap();
        assert_eq!((snap.log_sweeps_waiting, snap.log_sweeps_failing), (2, 1));
        assert_eq!(
            snap.log_sweeps_oldest_marked_at.as_deref(),
            Some("2020-01-01T00:00:00.000+00:00")
        );
    }

    /// Across the seam with the head and tail a log is kept as: an
    /// attempt's log written as its head and the line
    /// it is held at, then begun again as the head, that line, a line saying
    /// what was left out, and the tail, and then the engine's note. It reads
    /// back as that text and the note; the writing before is recorded and
    /// goes, and the one after is whole. Where the gap and the tail are added
    /// to the writing instead, it stays the one writing, and nothing is
    /// recorded.
    #[tokio::test]
    async fn a_log_kept_as_its_head_and_tail_lets_the_whole_one_go() {
        let head = lines("head", 100);
        let held =
            "[dagron] this attempt's log reached 4 KiB (DAGRON_LOG_TASK_CAP_BYTES): what the \
                    task writes from here is held back, and the last 1 KiB of it \
                    (DAGRON_LOG_TAIL_BYTES) is added below when it ends\n";
        let gap =
            "[dagron] 250 KiB of this attempt's output (256000 bytes and 5000 lines, counted \
                   before masking) is left out here; what follows is its end\n";
        let tail = lines("tail", 100);
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, run, tasks) = running(2, 2).await;
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);

            // Begun again as head and tail.
            let task = tasks[0].as_str();
            write(&mut b, &pool, task, 2, &format!("{head}{held}"), true).await;
            assert!(holds(&store, &files, task, 2, 1).await, "{store:?}");
            let again = format!("{head}{held}{gap}{tail}");
            write(&mut b, &pool, task, 2, &again, true).await;
            db::note_task_log(&pool, task, 2, "a note").await.unwrap();
            let whole = format!("{again}a note\n");
            assert_eq!(read(&pool, &store, &run, task).await, whole, "{store:?}");
            assert!(woken(&wake).await, "{store:?}");
            assert_eq!(record(&pool, task).await.unwrap().0, "superseded");
            gc(&pool, &store).pass().await.unwrap();
            assert!(!holds(&store, &files, task, 2, 1).await, "{store:?}");
            assert!(holds(&store, &files, task, 2, 2).await, "{store:?}");
            assert_eq!(read(&pool, &store, &run, task).await, whole, "{store:?}");

            // The gap and the tail added to the one writing.
            let task = tasks[1].as_str();
            write(&mut b, &pool, task, 2, &format!("{head}{held}"), true).await;
            write(&mut b, &pool, task, 2, &format!("{gap}{tail}"), false).await;
            assert_eq!(
                read(&pool, &store, &run, task).await,
                format!("{head}{held}{gap}{tail}"),
                "{store:?}"
            );
            assert_eq!(writings(&pool, task).await, Some(1), "{store:?}");
            assert_eq!(record(&pool, task).await, None, "{store:?}");
            assert!(holds(&store, &files, task, 2, 1).await, "{store:?}");
        }
    }

    /// The same, of what a sink sends of a log past its cap, as the loop
    /// takes it: its head and the line it is held at as it is written, and
    /// its seal. A seal that writes it again, a secret cut in two in its
    /// beginning, begins it again with its end in it; the reset records the
    /// writing before, which a pass removes, leaving the one after whole. A
    /// seal that adds its end adds it to the one writing, and nothing is
    /// recorded. Each reads back as the log the chunks make, and the note.
    #[tokio::test]
    async fn a_sink_s_head_and_tail_are_kept_as_the_log_they_make() {
        use dagron_executor::executor::LogSink;
        use dagron_executor::redact::Redactor;
        let base = tempfile::tempdir().unwrap();
        for (store, files) in stores(base.path()) {
            let (pool, _db, run, tasks) = running(2, 2).await;
            let wake = Arc::new(Notify::new());
            let mut b = batch(&store, &wake);
            for (task, cut_in_two) in [(tasks[0].as_str(), true), (tasks[1].as_str(), false)] {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let redactor = Redactor::default().with_values(["s3cr3t-token-value"]);
                let sink = LogSink::new(tx, task.to_string(), 2, redactor)
                    .with_cap(8192)
                    .with_tail(2048);
                let mut whole = String::new();
                let mut write = |piece: &str| {
                    whole.push_str(piece);
                    sink.append(piece);
                };
                if cut_in_two {
                    write("key=s3cr3t-");
                    write("token-value\n");
                } else {
                    write("key=s3cr3t-token-value\n");
                }
                for k in 0..1000 {
                    write(&format!("line {k:04} {}\n", "x".repeat(40)));
                }
                // What the loop takes as the task writes, then at its end.
                let mut log = String::new();
                let mut resets = 0;
                for sealed in [false, true] {
                    if sealed {
                        sink.seal(&whole);
                    }
                    let chunks: Vec<LogChunk> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
                    let (tx, mut taken) = tokio::sync::mpsc::unbounded_channel();
                    for chunk in chunks {
                        if chunk.first {
                            log.clear();
                            resets += 1;
                        }
                        log.push_str(&chunk.chunk);
                        tx.send(chunk).unwrap();
                    }
                    b.take(&mut taken);
                    b.write_for(&pool, [task].into_iter()).await;
                }
                assert!(log.contains("is held back") && log.contains("is left out here"));
                assert!(!log.contains("s3cr3t"), "{store:?}");
                assert!(log.ends_with(&format!("line 0999 {}\n", "x".repeat(40))));
                db::note_task_log(&pool, task, 2, "a note").await.unwrap();
                let whole_log = format!("{log}a note\n");
                assert_eq!(
                    read(&pool, &store, &run, task).await,
                    whole_log,
                    "{store:?}"
                );
                if cut_in_two {
                    assert_eq!(resets, 2, "{store:?}: begun, and begun again");
                    assert!(woken(&wake).await, "{store:?}: the reset woke the GC");
                    assert_eq!(record(&pool, task).await.unwrap().0, "superseded");
                    assert!(holds(&store, &files, task, 2, 1).await, "{store:?}");
                    let swept = gc(&pool, &store).pass().await.unwrap();
                    assert_eq!(swept.failed, 0, "{store:?}");
                    assert!(!holds(&store, &files, task, 2, 1).await, "{store:?}");
                    assert!(holds(&store, &files, task, 2, 2).await, "{store:?}");
                    assert_eq!(
                        read(&pool, &store, &run, task).await,
                        whole_log,
                        "{store:?}"
                    );
                } else {
                    assert_eq!(resets, 1, "{store:?}: begun once");
                    assert_eq!(writings(&pool, task).await, Some(1), "{store:?}");
                    assert_eq!(record(&pool, task).await, None, "{store:?}");
                    assert!(holds(&store, &files, task, 2, 1).await, "{store:?}");
                }
            }
        }
    }
}
