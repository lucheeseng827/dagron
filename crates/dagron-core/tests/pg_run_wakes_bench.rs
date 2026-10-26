//! What migrations_pg/107's trigger costs a write on Postgres: a cancel of
//! every task of a run of 10,000 running tasks in one statement, the bulk
//! cancel's shape; one task's end; and a lease renewed, which is not an end.
//! Each with the trigger and without it, on the same rows of one database,
//! the two taken in turn pass by pass. Each figure is the median of 25
//! passes, each in a transaction rolled back, so each pass finds the same
//! rows: a pass without the trigger disables it in its own transaction
//! first, untimed. A notification queued in a transaction rolled back is not
//! sent; its queueing is in the figure.
//!
//! Run by hand, in the Linux builder, against a database of its own made
//! from TEST_DATABASE_URL:
//!
//!   cargo test -p dagron-core --no-default-features --features postgres,ops \
//!     --test pg_run_wakes_bench -- --ignored --nocapture
#![cfg(feature = "postgres")]

use std::time::{Duration, Instant};

use dagron_core::db::{self, Pool};
use sqlx::{Connection, PgConnection};

const PASSES: usize = 25;

/// 107's triggers.
const TRIGGERS: [&str; 3] = [
    "task_runs_ended_notify",
    "task_runs_made_over_notify",
    "task_runs_gone_notify",
];

/// Drops the bench's database, with what is still connected to it, when the
/// bench ends, by a panic too.
struct DropDatabase {
    admin_url: String,
    name: String,
}

impl Drop for DropDatabase {
    fn drop(&mut self) {
        let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
        // A thread and a runtime of its own: a drop cannot await, and the
        // bench's runtime may be the one unwinding.
        let _ = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                        .execute(&mut admin)
                        .await;
                    let _ = admin.close().await;
                }
            });
        })
        .join();
    }
}

fn median(mut times: Vec<Duration>) -> f64 {
    times.sort();
    times[times.len() / 2].as_secs_f64() * 1e3
}

/// One pass of `sql`, in a transaction rolled back: with 107's triggers, or
/// with them disabled for the transaction.
async fn pass(pool: &Pool, sql: &str, with: bool) -> Duration {
    let mut tx = pool.begin().await.unwrap();
    if !with {
        for t in TRIGGERS {
            sqlx::query(&format!("ALTER TABLE task_runs DISABLE TRIGGER {t}"))
                .execute(&mut *tx)
                .await
                .unwrap();
        }
    }
    let at = Instant::now();
    sqlx::query(sql)
        .persistent(false)
        .execute(&mut *tx)
        .await
        .unwrap();
    let took = at.elapsed();
    tx.rollback().await.unwrap();
    took
}

#[tokio::test]
#[ignore]
async fn pg_run_wakes_bench() {
    let Ok(admin_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL unset - skipping");
        return;
    };
    let name = format!("bench_1026_{}", uuid::Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&admin_url).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .await
        .unwrap();
    let _ = admin.close().await;
    let _drop = DropDatabase {
        admin_url: admin_url.clone(),
        name: name.clone(),
    };
    let (head, query) = admin_url
        .split_once('?')
        .unwrap_or((admin_url.as_str(), ""));
    let (server, _) = head.rsplit_once('/').unwrap();
    let url = if query.is_empty() {
        format!("{server}/{name}")
    } else {
        format!("{server}/{name}?{query}")
    };
    let pool = db::init_pool(&url).await.unwrap();

    sqlx::raw_sql(
        "INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ('d1', 'bench', 'name: bench', 'now');
         INSERT INTO workflow_runs (id, definition_id, status, created_at) VALUES ('big', 'd1', 'running', 'now');
         INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input)
         SELECT 'bt' || n, 'big', 'b' || n, 'running', 0, '{}' FROM generate_series(1, 10000) AS n;
         ANALYZE task_runs;",
    )
    .execute(&pool)
    .await
    .unwrap();
    // 107 again, on a table of 10,000 tasks: its time at a start.
    let at = Instant::now();
    sqlx::raw_sql(include_str!(
        "../migrations_pg/107_task_runs_end_notify.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let m107 = at.elapsed().as_secs_f64() * 1e3;

    println!();
    println!("BENCH|what|without 107 ms|with 107 ms");
    for (what, sql) in [
        (
            "a cancel of 10,000 tasks",
            "UPDATE task_runs SET status = 'cancelled' WHERE run_id = 'big' AND status = 'running'",
        ),
        (
            "one task's end",
            "UPDATE task_runs SET status = 'succeeded' WHERE id = 'bt1'",
        ),
        (
            "a lease renewed (not an end)",
            "UPDATE task_runs SET lease_expires_at = 'later' WHERE id = 'bt1'",
        ),
    ] {
        let (mut with, mut without) = (Vec::new(), Vec::new());
        // One of each first, unrecorded, to warm what both read.
        pass(&pool, sql, true).await;
        pass(&pool, sql, false).await;
        for i in 0..PASSES * 2 {
            if i % 2 == 0 {
                with.push(pass(&pool, sql, true).await);
            } else {
                without.push(pass(&pool, sql, false).await);
            }
        }
        println!("BENCH|{what}|{:.3}|{:.3}", median(without), median(with));
    }
    println!("BENCH|migration 107 on 10,000 tasks||{m107:.1}");

    pool.close().await;
}
