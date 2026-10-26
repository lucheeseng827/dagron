//! What SQLite's reads of the running runs by `status = 'running'` and
//! dagron-api's live stream cost a pass, before migrations 080 and 081 and
//! after, with 10k, 100k and 1M runs kept and 1,000 running, five tasks
//! each; and what 081's triggers cost a write. Each figure is the median of
//! 15 passes, on one connection, as the engine and dagron-api each have one.
//! The "before" of each file is the same file without 080's index and 081's
//! triggers: before and after are measured in one session, on the same rows.
//!
//! Run by hand, in the Linux builder, with the bundled SQLite built as a
//! release would build it:
//!
//!   cargo test -p dagron-core --features ops --test sqlite_run_reads_bench \
//!     --config profile.dev.package.libsqlite3-sys.opt-level=3 -- --ignored --nocapture
//!
//! and with `--features enterprise` for the enterprise build's file, where
//! its own migrations are beside them.
#![cfg(all(feature = "sqlite", feature = "ops"))]

use std::time::{Duration, Instant};

use dagron_core::db::{self, Pool, RunChanges};

const PASSES: usize = 15;
const RUNNING: i64 = 1_000;
const TASKS: i64 = 5;

/// 081's triggers.
const TRIGGERS: [&str; 6] = [
    "run_changes_run_made",
    "run_changes_run_moved",
    "run_changes_task_made",
    "run_changes_task_moved",
    "run_changes_task_gone",
    "run_changes_bounded",
];

/// The reads, as db/sqlite.rs has them (text copied: they are the crate's
/// own), each with what it binds: the time now, a key or NULL, 0.
fn reads() -> Vec<(&'static str, &'static str, &'static [&'static str])> {
    const IN_FLIGHT: &str = "SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN wr.queued_at IS NOT NULL AND wr.admitted_at IS NULL
                                  THEN 1 ELSE 0 END), 0)
         FROM workflow_runs wr
         JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.status = 'running' AND d.name = ?1
           AND wr.concurrency_key IS ?2
           AND wr.id IS NOT ?3";
    const SUPERSEDED: &str = "SELECT wr.id FROM workflow_runs wr
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
    vec![
        ("count of active runs", "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running'", &[]),
        ("runs in flight, no key (500 running)", IN_FLIGHT, &["bench", "null", "null"]),
        ("runs in flight, a key (100 running)", IN_FLIGHT, &["bench", "key", "null"]),
        ("the runs superseded, no key", SUPERSEDED, &["bench", "null", "zero", "null", "null", "null", "null"]),
        ("the runs superseded, a key", SUPERSEDED, &["bench", "key", "zero", "null", "null", "null", "null"]),
        // What 080 takes besides, on OSS from a scan.
        ("the reclass near a deadline", "UPDATE task_runs
         SET runner_class = ?
         WHERE status = 'ready'
           AND runner_class = ?
           AND gang_id IS NULL
           AND run_id IN (
               SELECT id FROM workflow_runs
               WHERE status = 'running'
                 AND COALESCE(deadline_at, alert_deadline_at) IS NOT NULL
                 AND COALESCE(deadline_at, alert_deadline_at) <= ?
           )", &["key", "key", "now"]),
        ("the drift mark", "UPDATE workflow_runs
         SET clock_confidence = 'drifted', clock_offset_ms = ?, clock_source = ?
         WHERE status = 'running'", &["zero", "key"]),
        ("the running runs listed (50)", "SELECT wr.id AS id, wd.name AS name, wr.status AS status,
                       wr.created_at AS created_at, wr.finished_at AS finished_at,
                       wr.stopping_as AS stopping_as,
                       wr.queued_at AS queued_at, wr.admitted_at AS admitted_at
                FROM workflow_runs wr
                JOIN workflow_definitions wd ON wd.id = wr.definition_id WHERE wr.status = 'running' ORDER BY wr.created_at DESC LIMIT ?", &["fifty"]),
    ]
}

/// dagron-api's stream read before 081, every DAGRON_SQLITE_POLL_MS.
const OLD_PUMP: &str = "SELECT r.id, r.status || '|' || COALESCE((
             SELECT group_concat(state, ',') FROM (
                 SELECT t.status || ':' || t.attempt AS state FROM task_runs t
                 WHERE t.run_id = r.id ORDER BY t.id)), '')
         FROM workflow_runs r
         WHERE r.finished_at IS NULL OR r.finished_at >= $1";

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
    let since = (chrono::Utc::now() - chrono::TimeDelta::seconds(30)).to_rfc3339();
    let mut times = Vec::new();
    for _ in 0..PASSES {
        let mut tx = pool.begin().await.unwrap();
        let mut q = sqlx::query(sql).persistent(false);
        for b in binds {
            q = match *b {
                "now" => q.bind(now.clone()),
                "since" => q.bind(since.clone()),
                "bench" => q.bind("bench"),
                "key" => q.bind("k1"),
                "null" => q.bind(Option::<String>::None),
                "zero" => q.bind(0_i64),
                "fifty" => q.bind(50_i64),
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

/// The bytes of the file's pages in use.
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
/// of TASKS tasks, none of them over: half with no concurrency key, the
/// other half in five keys of 100; some with a deadline, an alert, a stop
/// asked for, or waiting for their turn. As sqlite_run_sweeps_bench makes its file,
/// but for the keys.
async fn file_of(kept: i64) -> (Pool, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("bench1025_{kept}_{}.db", uuid::Uuid::new_v4()));
    let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    for sql in [
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ('d1', 'bench', 'name: bench', '2026-01-01T00:00:00+00:00')".to_string(),
        format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {kept})
             INSERT INTO workflow_runs (id, definition_id, status, created_at, finished_at)
             SELECT printf('h%08d', i), 'd1',
                    CASE i % 10 WHEN 0 THEN 'failed' WHEN 1 THEN 'cancelled' ELSE 'succeeded' END,
                    strftime('%Y-%m-%dT%H:%M:%S+00:00', '2026-01-01', '+' || (i % 20000000) || ' seconds'),
                    strftime('%Y-%m-%dT%H:%M:%S+00:00', '2026-01-01', '+' || (i % 20000000 + 60) || ' seconds')
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
                                        alert_deadline_at, stopping_as, queued_at, concurrency_key)
             SELECT printf('r%08d', i), 'd1', 'running', strftime('%Y-%m-%dT%H:%M:%S+00:00', 'now', '-' || i || ' seconds'),
                    CASE WHEN i % 10 = 0 THEN '2099-01-01T00:00:00+00:00' END,
                    CASE WHEN i % 10 = 1 THEN '2099-01-01T00:00:00+00:00' END,
                    CASE WHEN i % 100 = 3 THEN 'cancelled' END,
                    CASE WHEN i % 50 = 4 THEN '2026-01-02T00:00:00+00:00' END,
                    CASE WHEN i % 2 = 1 THEN 'k' || ((i / 2) % 5) END
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

/// The pump after 081: a poll with nothing changed, and one of `n` runs
/// changed, each a task's move, made in its own statement before the poll
/// is timed and rolled back after.
async fn pump_after(pool: &Pool, scale: i64) {
    let since = (chrono::Utc::now() - chrono::TimeDelta::seconds(30)).to_rfc3339();
    let mut times = Vec::new();
    for _ in 0..PASSES {
        let at = Instant::now();
        db::live_stream_runs(pool, &since).await.unwrap();
        times.push(at.elapsed());
    }
    println!(
        "BENCH|{scale}|the stream's read at a start (every live run)||{:.3}",
        median(times)
    );
    let (cursor, _) = db::live_stream_runs(pool, &since).await.unwrap();
    for n in [0_i64, 10, 100] {
        let mut times = Vec::new();
        for _ in 0..PASSES {
            if n > 0 {
                sqlx::query(&format!(
                "UPDATE task_runs SET attempt = attempt + 1
                  WHERE name = 't1' AND run_id IN (SELECT printf('r%08d', i) FROM
                    (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {n}) SELECT i FROM n))"
            ))
            .execute(pool)
            .await
            .unwrap();
            }
            let at = Instant::now();
            let read = db::run_changes_after(pool, cursor, 4096).await.unwrap();
            times.push(at.elapsed());
            match read {
                RunChanges::Since(rows) => assert_eq!(rows.len() as i64, n),
                RunChanges::Behind { .. } => panic!("behind"),
            }
            // Back to the cursor's: the rows written are taken away, so the
            // next pass reads the same.
            sqlx::query("DELETE FROM run_changes WHERE seq > ?")
                .bind(cursor)
                .execute(pool)
                .await
                .unwrap();
        }
        println!(
            "BENCH|{scale}|the stream's poll, {n} runs changed||{:.3}",
            median(times)
        );
    }
}

/// A write's cost: a cancel of every task of a run of 10,000 running tasks
/// (one statement), a fan-out's 10,000 instances made (one statement), and
/// one task's end, each in a transaction rolled back. The run is made for it
/// and taken away after.
async fn writes(pool: &Pool) -> [f64; 3] {
    exec(
        pool,
        "INSERT INTO workflow_runs (id, definition_id, status, created_at) VALUES ('big', 'd1', 'running', 'now');
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10000)
         INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
         SELECT printf('bt%06d', i), 'big', 'b' || i, 'running', 0, '{}' FROM n;
         DELETE FROM run_wakes;",
    )
    .await;
    let mut out = [0.0; 3];
    for (slot, sql) in [
        "UPDATE task_runs SET status = 'cancelled' WHERE run_id = 'big' AND status = 'running'",
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10000)
         INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
         SELECT printf('fi%06d', i), 'big', 'f' || i, 'ready', 0, '{}' FROM n",
        "UPDATE task_runs SET status = 'succeeded' WHERE id = 'bt000001'",
    ]
    .into_iter()
    .enumerate()
    {
        let mut times = Vec::new();
        for _ in 0..PASSES {
            let mut tx = pool.begin().await.unwrap();
            let at = Instant::now();
            sqlx::query(sql).execute(&mut *tx).await.unwrap();
            times.push(at.elapsed());
            tx.rollback().await.unwrap();
        }
        out[slot] = median(times);
    }
    exec(
        pool,
        "DELETE FROM task_runs WHERE run_id = 'big'; DELETE FROM workflow_runs WHERE id = 'big';
         DELETE FROM run_wakes; PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .await;
    out
}

#[tokio::test]
#[ignore]
async fn sqlite_run_reads_bench() {
    println!();
    println!(
        "BENCH|scale|what|before ms|after ms (enterprise={})",
        cfg!(feature = "enterprise")
    );
    for scale in [10_000_i64, 100_000, 1_000_000] {
        let (pool, path) = file_of(scale).await;
        let writes_after = writes(&pool).await;
        let mut after = Vec::new();
        for (_, sql, binds) in reads() {
            after.push(time_read(&pool, sql, binds).await);
        }
        pump_after(&pool, scale).await;

        // Before: no 080, no 081's triggers.
        exec(&pool, "DROP INDEX idx_workflow_runs_in_flight;").await;
        for t in TRIGGERS {
            exec(&pool, &format!("DROP TRIGGER {t}")).await;
        }
        exec(&pool, "PRAGMA wal_checkpoint(TRUNCATE);").await;
        for ((what, sql, binds), after_ms) in reads().into_iter().zip(&after) {
            let before = time_read(&pool, sql, binds).await;
            println!("BENCH|{scale}|{what}|{before:.3}|{after_ms:.3}");
        }
        let pump = time_read(&pool, OLD_PUMP, &["since"]).await;
        println!("BENCH|{scale}|the stream's poll before 081 (every live run, with its tasks)|{pump:.3}|");
        let writes_before = writes(&pool).await;
        for (i, what) in [
            "a cancel of 10,000 tasks",
            "10,000 tasks made",
            "one task's end",
        ]
        .into_iter()
        .enumerate()
        {
            println!(
                "BENCH|{scale}|{what}|{:.3}|{:.3}",
                writes_before[i], writes_after[i]
            );
        }

        if scale == 1_000_000 {
            exec(
                &pool,
                "DROP TABLE run_changes; PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .await;
            let size = used_bytes(&pool).await;
            let at = Instant::now();
            exec(
                &pool,
                include_str!("../migrations/080_run_in_flight_index.sql"),
            )
            .await;
            let m080 = at.elapsed().as_secs_f64() * 1e3;
            let at = Instant::now();
            exec(&pool, include_str!("../migrations/081_run_changes.sql")).await;
            let m081 = at.elapsed().as_secs_f64() * 1e3;
            exec(&pool, "PRAGMA wal_checkpoint(TRUNCATE);").await;
            let grown = used_bytes(&pool).await - size;
            println!("BENCH|{scale}|migration 080||{m080:.1}");
            println!("BENCH|{scale}|migration 081||{m081:.1}");
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
