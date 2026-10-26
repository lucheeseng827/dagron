//! What SQLite's run sweeps, the reap and the live counts cost a pass,
//! before migrations 078 and 079 and after, with 10k, 100k and 1M runs kept
//! and 1,000 running, five tasks each. Each figure is the median of 15
//! passes, on one connection, as the engine has one. The "before" of each
//! file is the same file with 078's six as they were, and without the live
//! and reapable indexes: so before and after are measured in one session, on
//! the same rows. The run of 10,000 tasks the bulk cancel needs is there
//! only while that cancel is timed.
//!
//! Run by hand, in the Linux builder, with the bundled SQLite built as a
//! release would build it:
//!
//!   cargo test -p dagron-core --features ops --test sqlite_run_sweeps_bench \
//!     --config profile.dev.package.libsqlite3-sys.opt-level=3 -- --ignored --nocapture
//!
//! and with `--features enterprise` for the enterprise build's file, where
//! its own migrations are beside them.
#![cfg(feature = "sqlite")]

use std::time::{Duration, Instant};

use dagron_core::db::{self, Pool, Woken};

const PASSES: usize = 15;
const RUNNING: i64 = 1_000;
const TASKS: i64 = 5;

/// The six as 010, 015, 053, 054, 055 and 060 made them.
const BEFORE: &str = "
DROP INDEX idx_workflow_runs_live;
DROP INDEX idx_workflow_runs_reapable;
DROP INDEX idx_workflow_runs_deadline;
DROP INDEX idx_workflow_runs_alert_deadline;
DROP INDEX idx_workflow_runs_stopping;
DROP INDEX idx_workflow_runs_waiting;
DROP INDEX idx_workflow_runs_start_pending;
DROP INDEX idx_workflow_runs_completion_pending;
CREATE INDEX idx_workflow_runs_deadline ON workflow_runs(deadline_at)
    WHERE deadline_at IS NOT NULL AND status = 'running';
CREATE INDEX idx_workflow_runs_alert_deadline ON workflow_runs(alert_deadline_at)
    WHERE alert_deadline_at IS NOT NULL AND alert_fired_at IS NULL AND status = 'running';
CREATE INDEX idx_workflow_runs_completion_pending
    ON workflow_runs (completion_pending_at) WHERE completion_pending_at IS NOT NULL;
CREATE INDEX idx_workflow_runs_start_pending
    ON workflow_runs (start_pending_at) WHERE start_pending_at IS NOT NULL;
CREATE INDEX idx_workflow_runs_stopping ON workflow_runs (id) WHERE stopping_as IS NOT NULL;
CREATE INDEX idx_workflow_runs_waiting ON workflow_runs (id)
    WHERE queued_at IS NOT NULL AND admitted_at IS NULL AND status = 'running';";

const TRIGGERS: [&str; 5] = [
    "run_wakes_task_ended",
    "run_wakes_task_made_over",
    "run_wakes_task_gone",
    "run_wakes_run_made",
    "run_wakes_run_changed",
];

/// The reads, as db/sqlite.rs has them (text copied: they are the crate's
/// own), each with what it binds, in order: the time now, an hour ago, a
/// batch, the workflow's name, NULL or 0.
fn reads() -> Vec<(&'static str, &'static str, &'static [&'static str])> {
    vec![
        ("deadlines", "UPDATE workflow_runs
         SET stopping_as = 'failed', stop_requested_at = ?, start_pending_at = NULL,
             output = 'run deadline exceeded (run_timeout_secs)'
         WHERE status = 'running' AND stopping_as IS NULL
           AND deadline_at IS NOT NULL AND deadline_at < ?
           AND (queued_at IS NULL OR admitted_at IS NOT NULL)
         RETURNING id, deadline_at", &["now", "now"]),
        ("alerts", "UPDATE workflow_runs SET alert_fired_at = ?
         WHERE status = 'running' AND alert_deadline_at IS NOT NULL
           AND alert_deadline_at < ? AND alert_fired_at IS NULL
         RETURNING id, alert_deadline_at", &["now", "now"]),
        ("stopped runs", "SELECT id FROM workflow_runs
         WHERE stopping_as IS NOT NULL AND status = 'running'
           AND NOT EXISTS (
               SELECT 1 FROM task_runs t
               WHERE t.run_id = workflow_runs.id
                 AND t.status = 'running' AND t.stop_requested_at IS NOT NULL
           )", &[]),
        ("admissions", "SELECT DISTINCT d.name, wr.concurrency_key
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
           AND wr.status = 'running' AND wr.stopping_as IS NULL", &[]),
        ("pending starts", "UPDATE workflow_runs SET start_pending_at = NULL
         WHERE id IN (
             SELECT id FROM workflow_runs
             WHERE start_pending_at IS NOT NULL AND status = 'running'
             ORDER BY start_pending_at
             LIMIT ?
         )
         RETURNING id", &["limit"]),
        ("pending completions", "UPDATE workflow_runs SET completion_pending_at = NULL
         WHERE id IN (
             SELECT id FROM workflow_runs
             WHERE completion_pending_at IS NOT NULL
               AND status IN ('succeeded', 'failed', 'cancelled')
             ORDER BY completion_pending_at
             LIMIT ?
         )
         RETURNING id, status", &["limit"]),
        ("live runs by status", "SELECT status, COUNT(*) FROM workflow_runs WHERE status IN ('pending', 'running') GROUP BY status", &[]),
        ("recent live runs", "SELECT wd.name, wr.environment, wr.status, COUNT(*)
         FROM workflow_runs wr
         JOIN workflow_definitions wd ON wd.id = wr.definition_id
         WHERE wr.status IN ('pending', 'running') AND wr.created_at >= ?
         GROUP BY wd.name, wr.environment, wr.status", &["since"]),
        ("health count", "SELECT COUNT(*) FROM workflow_runs WHERE status IN ('pending', 'running')", &[]),
        // Not a run sweep: timed for the row after it.
        ("count of active runs", "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running'", &[]),
        ("runs in flight (the cap)", "SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
                                  THEN 1 ELSE 0 END), 0)
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.status = 'running' AND d.name = ?1
           AND wr.concurrency_key IS ?2
           AND wr.id IS NOT ?3", &["bench", "null", "null"]),
        ("the runs superseded", "SELECT wr.id FROM workflow_runs wr
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
         ORDER BY wr.created_at, wr.id", &["bench", "null", "zero", "null", "null", "null", "null"]),
    ]
}

/// The reap before 079: every running run joined to its tasks, and the
/// runs with none.
const OLD_REAP: [&str; 2] = [
    "SELECT wr.id AS run_id, tr.status AS status, tr.allow_failure AS allow_failure, COUNT(*) AS cnt
     FROM workflow_runs wr
     JOIN task_runs tr ON tr.run_id = wr.id
     WHERE wr.status = 'running' AND wr.stopping_as IS NULL
     GROUP BY wr.id, tr.status, tr.allow_failure",
    "SELECT wr.id FROM workflow_runs wr
     WHERE wr.status = 'running' AND wr.stopping_as IS NULL
       AND (wr.queued_at IS NULL OR wr.admitted_at IS NOT NULL)
       AND NOT EXISTS (SELECT 1 FROM task_runs tr WHERE tr.run_id = wr.id)",
];

fn median(mut times: Vec<Duration>) -> f64 {
    times.sort();
    times[times.len() / 2].as_secs_f64() * 1e3
}

async fn exec(pool: &Pool, sql: &str) {
    sqlx::raw_sql(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// One statement's median pass, in a transaction rolled back, so a write
/// changes nothing for the next.
async fn time_read(pool: &Pool, sql: &str, binds: &[&str]) -> f64 {
    let now = chrono::Utc::now().to_rfc3339();
    let since = (chrono::Utc::now() - chrono::TimeDelta::hours(1)).to_rfc3339();
    let mut times = Vec::new();
    for _ in 0..PASSES {
        let mut tx = pool.begin().await.unwrap();
        let mut q = sqlx::query(sql).persistent(false);
        for b in binds {
            q = match *b {
                "now" => q.bind(now.clone()),
                "since" => q.bind(since.clone()),
                "bench" => q.bind("bench"),
                "null" => q.bind(Option::<String>::None),
                "zero" => q.bind(0_i64),
                _ => q.bind(256_i64),
            };
        }
        let at = Instant::now();
        q.fetch_all(&mut *tx).await.unwrap();
        times.push(at.elapsed());
        tx.rollback().await.unwrap();
    }
    median(times)
}

async fn time<F, Fut>(mut f: F) -> f64
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut times = Vec::new();
    for _ in 0..PASSES {
        let at = Instant::now();
        f().await;
        times.push(at.elapsed());
    }
    median(times)
}

/// The bytes of the file's pages in use: a dropped index's pages are free
/// pages of the file, which a new one takes before the file grows.
async fn used_bytes(pool: &Pool) -> f64 {
    let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(pool)
        .await
        .unwrap();
    let free: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(pool)
        .await
        .unwrap();
    let size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .unwrap();
    ((pages - free) * size) as f64
}

/// A file of `kept` finished runs, one task each, and RUNNING running runs
/// of TASKS tasks, none of them over: some with a deadline, an alert, a
/// start to announce, a stop asked for, or waiting for their turn; and some
/// finished ones stamped for their end's work.
async fn file_of(kept: i64) -> (Pool, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("bench1024_{kept}_{}.db", uuid::Uuid::new_v4()));
    let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    for sql in [
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ('d1', 'bench', 'name: bench', '2026-01-01T00:00:00+00:00')".to_string(),
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {kept})
             INSERT INTO workflow_runs (id, definition_id, status, created_at, finished_at, completion_pending_at)
             SELECT printf('h%08d', i), 'd1',
                    CASE i % 10 WHEN 0 THEN 'failed' WHEN 1 THEN 'cancelled' ELSE 'succeeded' END,
                    strftime('%Y-%m-%dT%H:%M:%S+00:00', '2026-01-01', '+' || (i % 20000000) || ' seconds'),
                    strftime('%Y-%m-%dT%H:%M:%S+00:00', '2026-01-01', '+' || (i % 20000000 + 60) || ' seconds'),
                    CASE WHEN i % 20000 = 0 THEN '2026-01-02T00:00:00+00:00' END
             FROM n"
        ),
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {kept})
             INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
             SELECT printf('ht%08d', i), printf('h%08d', i), 'a', 'succeeded', 0, '{{}}' FROM n"
        ),
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {RUNNING})
             INSERT INTO workflow_runs (id, definition_id, status, created_at, deadline_at,
                                        alert_deadline_at, start_pending_at, stopping_as, queued_at)
             SELECT printf('r%08d', i), 'd1', 'running', strftime('%Y-%m-%dT%H:%M:%S+00:00', 'now', '-' || i || ' seconds'),
                    CASE WHEN i % 10 = 0 THEN '2099-01-01T00:00:00+00:00' END,
                    CASE WHEN i % 10 = 1 THEN '2099-01-01T00:00:00+00:00' END,
                    CASE WHEN i % 20 = 2 THEN '2026-01-02T00:00:00+00:00' END,
                    CASE WHEN i % 100 = 3 THEN 'cancelled' END,
                    CASE WHEN i % 50 = 4 THEN '2026-01-02T00:00:00+00:00' END
             FROM n"
        ),
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {RUNNING} * {TASKS})
             INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
             SELECT printf('rt%08d', i), printf('r%08d', (i - 1) / {TASKS} + 1), 't' || (i % {TASKS}),
                    CASE i % {TASKS} WHEN 0 THEN 'pending' WHEN 1 THEN 'running' ELSE 'succeeded' END, 0, '{{}}'
             FROM n"
        ),
    ] {
        sqlx::query(&sql).execute(&mut *tx).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    tx.commit().await.unwrap();
    exec(
        &pool,
        "DELETE FROM run_wakes; PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .await;
    (pool, path)
}

/// The reap's costs as the loop pays them: the idle take, a named reap of
/// `n` running runs, one slice of 500, and `All`.
async fn reap_after(pool: &Pool, scale: i64) {
    let mut waker = db::Waker::connect(pool).await.unwrap();
    assert_eq!(waker.take_woken().await, Woken::All);
    let mut times = Vec::new();
    for _ in 0..PASSES {
        let at = Instant::now();
        assert_eq!(waker.take_woken().await, Woken::Runs(Vec::new()));
        times.push(at.elapsed());
    }
    let idle = median(times);
    println!("BENCH|{scale}|reap: the idle take||{idle:.3}");
    let ids: Vec<String> = (1..=4096).map(|i| format!("r{i:08}")).collect();
    for n in [10, 100, 4096] {
        let woken = Woken::Runs(ids[..n].to_vec());
        let ms = time(|| async {
            assert!(db::reap_woken(pool, &woken).await.unwrap().is_empty());
        })
        .await;
        println!("BENCH|{scale}|reap: {n} runs named||{ms:.3}");
    }
    let slice = time(|| async {
        db::reap_slice(pool, None, 500).await.unwrap();
    })
    .await;
    println!("BENCH|{scale}|reap: one slice of 500||{slice:.3}");
    let all = time(|| async {
        assert!(db::reap_woken(pool, &Woken::All).await.unwrap().is_empty());
    })
    .await;
    println!("BENCH|{scale}|reap: All||{all:.3}");
}

/// A cancel of every task of a run of 10,000, with the triggers and
/// without. The run is made for it and taken away after, with its tasks
/// and whatever wakes it left, so that no read is timed beside it: the
/// reap before 079 joined every running run to its tasks, and would have
/// counted its 10,000.
async fn bulk_cancel(pool: &Pool) -> f64 {
    exec(
        pool,
        "INSERT INTO workflow_runs (id, definition_id, status, created_at) VALUES ('big', 'd1', 'running', 'now');
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10000)
         INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
         SELECT printf('bt%06d', i), 'big', 'b' || i, 'running', 0, '{}' FROM n;
         DELETE FROM run_wakes;",
    )
    .await;
    let mut times = Vec::new();
    for _ in 0..PASSES {
        let mut tx = pool.begin().await.unwrap();
        let at = Instant::now();
        sqlx::query(
            "UPDATE task_runs SET status = 'cancelled' WHERE run_id = 'big' AND status = 'running'",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        times.push(at.elapsed());
        tx.rollback().await.unwrap();
    }
    exec(
        pool,
        "DELETE FROM task_runs WHERE run_id = 'big'; DELETE FROM workflow_runs WHERE id = 'big';
         DELETE FROM run_wakes; PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .await;
    median(times)
}

#[tokio::test]
#[ignore]
async fn sqlite_run_sweeps_bench() {
    println!();
    println!(
        "BENCH|scale|what|before ms|after ms (enterprise={})",
        cfg!(feature = "enterprise")
    );
    for scale in [10_000_i64, 100_000, 1_000_000] {
        let (pool, path) = file_of(scale).await;
        // The bulk cancel with the triggers first, so that the reads before
        // and after are both timed on a file that held the big run once and
        // holds it no more.
        let with = bulk_cancel(&pool).await;
        let mut after = Vec::new();
        for (what, sql, binds) in reads() {
            after.push((what, time_read(&pool, sql, binds).await));
        }
        let mut old_reap_after = 0.0;
        for sql in OLD_REAP {
            old_reap_after += time_read(&pool, sql, &[]).await;
        }
        reap_after(&pool, scale).await;

        // Before: 078's six as they were, no live or reapable index, and no
        // triggers; the WAL emptied, as file_of leaves it for the after.
        exec(&pool, BEFORE).await;
        for t in TRIGGERS {
            exec(&pool, &format!("DROP TRIGGER {t}")).await;
        }
        exec(&pool, "PRAGMA wal_checkpoint(TRUNCATE);").await;
        for ((what, sql, binds), (_, after_ms)) in reads().into_iter().zip(&after) {
            let before = time_read(&pool, sql, binds).await;
            println!("BENCH|{scale}|{what}|{before:.3}|{after_ms:.3}");
        }
        let mut old_reap = 0.0;
        for sql in OLD_REAP {
            old_reap += time_read(&pool, sql, &[]).await;
        }
        println!("BENCH|{scale}|reap: every running run joined (each tick, before)|{old_reap:.3}|{old_reap_after:.3}");
        let without = bulk_cancel(&pool).await;
        println!("BENCH|{scale}|bulk cancel of 10,000 tasks|{without:.3}|{with:.3}");

        if scale == 1_000_000 {
            // 078 and 079 on a file of a million runs: their time, and what
            // the file grows by.
            exec(
                &pool,
                "DROP TABLE run_wakes; PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .await;
            let size = used_bytes(&pool).await;
            let at = Instant::now();
            exec(
                &pool,
                include_str!("../migrations/078_run_sweep_indexes.sql"),
            )
            .await;
            let m078 = at.elapsed().as_secs_f64() * 1e3;
            let at = Instant::now();
            exec(&pool, include_str!("../migrations/079_run_wakes.sql")).await;
            let m079 = at.elapsed().as_secs_f64() * 1e3;
            exec(&pool, "PRAGMA wal_checkpoint(TRUNCATE);").await;
            let grown = used_bytes(&pool).await - size;
            println!("BENCH|{scale}|migration 078||{m078:.1}");
            println!("BENCH|{scale}|migration 079||{m079:.1}");
            println!(
                "BENCH|{scale}|the pages in use, {:.1} MB, grow by (MB)||{:.2}",
                size / 1e6,
                grown / 1e6
            );
        }
        pool.close().await;
        for ext in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{ext}", path.display()));
        }
    }
}
