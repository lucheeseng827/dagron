//! A task's output with a NUL byte in it.
//!
//! Postgres takes no NUL in a text value, and refuses the whole statement that
//! holds one. A task that wrote a single one lost the append its log went in,
//! and failed the statement that recorded its result, on which the engine
//! exited: one `printf '\0'` in a job did it. So neither what a worker reports
//! nor what it streams holds one, whatever its backend handed it. Each is kept
//! as U+FFFD, which is what a byte that is no text already is.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use dagron_core::metrics::Metrics;
use dagron_executor::executor::{ExecContext, ExecOutput, Executor, LogChunk};
use dagron_executor::worker::{DispatchPayload, TaskResult, WorkerPool};
use tokio::sync::mpsc;

/// A backend that streams a NUL to the live log, and hands one back: in its
/// output, or in the error it fails with, as the task's command says.
struct WritesNul;

#[async_trait]
impl Executor for WritesNul {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        if let Some(sink) = &ctx.log_sink {
            sink.append("before\n");
            sink.append("a\0b\n");
            sink.append("after\n");
        }
        match ctx.command[0].as_str() {
            "fails" => Err(anyhow::anyhow!("the tool said: bad\0input")),
            _ => Ok(ExecOutput {
                success: true,
                output: "before\na\0b\nafter\n".into(),
                dropped: 0,
            }),
        }
    }
}

/// One dispatch, the way the reconcile loop makes it. Returns the result and
/// what reached the live log.
async fn dispatch(pool: &WorkerPool, command: &str) -> (TaskResult, String) {
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<LogChunk>();
    pool.dispatch(DispatchPayload {
        task_id: "t1".into(),
        worker_id: "w1".into(),
        ctx: ExecContext::new(vec![command.into()], Some(5), None),
        attempt: 0,
        max_attempts: 1,
        retry_delay_secs: 0,
        retry_max_delay_secs: None,
        retry_on_timeout: true,
        fence: 7,
        spec: None,
        teardown: false,
        result_tx,
        log_tx: Some(log_tx),
    })
    .expect("the worker takes the task");

    let result = tokio::time::timeout(Duration::from_secs(10), result_rx.recv())
        .await
        .expect("the worker reports within 10 s")
        .expect("and reports exactly one result");
    let mut log = String::new();
    while let Ok(chunk) = log_rx.try_recv() {
        log.push_str(&chunk.chunk);
    }
    (result, log)
}

#[tokio::test]
async fn a_nul_a_task_wrote_reaches_neither_its_result_nor_its_log() {
    let pool = WorkerPool::new(1, Arc::new(WritesNul), Arc::new(Metrics::new()))
        .await
        .expect("a one-worker pool starts");

    // A task that succeeds: its output is what it wrote, the NUL as a mark.
    let (result, log) = dispatch(&pool, "succeeds").await;
    assert!(result.success);
    assert_eq!(
        result.output.as_deref(),
        Some("before\na\u{FFFD}b\nafter\n")
    );
    assert_eq!(log, "before\na\u{FFFD}b\nafter\n");

    // One that fails: the reason is its output, and the last line of its log.
    let (result, log) = dispatch(&pool, "fails").await;
    assert!(!result.success);
    assert_eq!(
        result.output.as_deref(),
        Some("the tool said: bad\u{FFFD}input")
    );
    assert_eq!(
        log,
        "before\na\u{FFFD}b\nafter\nthe tool said: bad\u{FFFD}input\n"
    );
}
