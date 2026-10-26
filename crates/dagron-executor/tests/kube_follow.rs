//! A task pod's log, on a real cluster, followed as the task writes it: a
//! line is in the task's log while its pod still runs, a task
//! that ran out of time keeps what it wrote, and a line longer than the
//! container runtime's own (which the kubelet hands over in parts) is whole.
//!
//! Skips, saying so, unless `DAGRON_KUBE_LIVE` names a namespace the
//! kubeconfig reaches, as `kube_workspace.rs` does. tests/k3s.sh runs this.
#![cfg(feature = "kubernetes")]

use std::time::{Duration, Instant};

use dagron_executor::executor::{
    ExecContext, Executor, LogChunk, LogSink, TaskIdentity, TimeoutError,
};
use dagron_executor::kube_executor::KubeExecutor;
use dagron_executor::redact::Redactor;
use tokio::sync::mpsc;

const TASK_IMAGE: &str = "alpine:3.20";

async fn executor() -> Option<KubeExecutor> {
    let Ok(namespace) = std::env::var("DAGRON_KUBE_LIVE") else {
        eprintln!("DAGRON_KUBE_LIVE names no namespace - skipping");
        return None;
    };
    dagron_executor::install_crypto_provider();
    Some(
        KubeExecutor::connect(TASK_IMAGE, &namespace)
            .await
            .expect("the executor reaches the namespace"),
    )
}

/// A task that runs `script`, within `secs`, with a log.
fn task(script: &str, secs: u64) -> (ExecContext, mpsc::UnboundedReceiver<LogChunk>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let task_id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(secs),
        Some(TASK_IMAGE.into()),
    );
    ctx.log_sink = Some(LogSink::new(tx, task_id.clone(), 1, Redactor::default()));
    ctx.identity = Some(TaskIdentity {
        task_id,
        run_id: "r-follow".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    (ctx, rx)
}

/// Everything the log was sent, in order.
fn log_of(rx: &mut mpsc::UnboundedReceiver<LogChunk>) -> String {
    let mut log = String::new();
    while let Ok(chunk) = rx.try_recv() {
        log.push_str(&chunk.chunk);
    }
    log
}

/// A task's first line is in its log while its pod still runs, seconds
/// before its last is written; and the log is its output, each line once.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_s_lines_are_in_its_log_as_it_writes_them() {
    let Some(exec) = executor().await else { return };
    let (ctx, mut rx) = task("echo first; sleep 12; echo second", 180);
    let run = exec.execute(&ctx);
    tokio::pin!(run);
    let first = tokio::select! {
        out = &mut run => panic!("the task ended before its first line was in its log: {out:?}"),
        chunk = rx.recv() => chunk.expect("a chunk"),
    };
    let at = Instant::now();
    assert_eq!(first.chunk, "first\n");
    assert!(first.first, "the attempt's first chunk is flagged");
    let out = run.await.expect("the task runs");
    assert!(out.success, "{}", out.output);
    assert!(
        at.elapsed() >= Duration::from_secs(5),
        "the first line came only {:?} before the task ended",
        at.elapsed()
    );
    assert_eq!(out.output, "first\nsecond\n");
    assert_eq!(
        format!("first\n{}", log_of(&mut rx)),
        out.output,
        "each line once"
    );
}

/// A task that ran out of time keeps the lines it wrote before it was
/// stopped, and none it wrote after: it is stopped at its deadline (once its
/// log has been read once more, for at most 10 s), not once its log is over. Its lines: it is read once more at its deadline, and a
/// line it had not ended then containerd writes to the kubelet's file only
/// once the container's output closes, or the line passes 16 KiB.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_that_timed_out_keeps_what_it_wrote() {
    let Some(exec) = executor().await else { return };
    let (ctx, mut rx) = task(
        "echo before; echo and after; sleep 40; echo late; sleep 300",
        25,
    );
    let began = Instant::now();
    let err = exec.execute(&ctx).await.expect_err("it times out");
    assert!(err.is::<TimeoutError>(), "{err:#}");
    assert!(
        began.elapsed() < Duration::from_secs(25 + 35),
        "ended {:?} after it began, for a deadline of 25 s and the pod's deletion",
        began.elapsed()
    );
    assert_eq!(log_of(&mut rx), "before\nand after\n");
}

/// A line longer than the container runtime's own, which it writes to the
/// kubelet's file in parts and the kubelet hands over with one time at its
/// start, is one line, whole, with the lines on either side.
#[tokio::test(flavor = "multi_thread")]
async fn a_line_longer_than_the_runtime_s_is_whole() {
    let Some(exec) = executor().await else { return };
    let (ctx, mut rx) = task(
        "echo before; head -c 40000 /dev/zero | tr '\\0' a; echo; echo after",
        180,
    );
    let out = exec.execute(&ctx).await.expect("the task runs");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        format!("before\n{}\nafter\n", "a".repeat(40_000))
    );
    assert_eq!(log_of(&mut rx), out.output);
}

/// A task that writes past its log's cap keeps its last lines: the log is
/// sent to its cap as the pod writes it and held there, and once the pod is
/// over its end is sent in one piece, under a line that says what was left
/// out. So too where it ran out of time.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_past_its_cap_keeps_its_last_lines() {
    let Some(exec) = executor().await else { return };
    let lines =
        "awk 'BEGIN { for (i = 1; i <= 5000; i++) printf \"line %04d of the task\\n\", i }'";
    let whole: String = (1..=5000)
        .map(|i| format!("line {i:04} of the task\n"))
        .collect();
    for (script, secs) in [
        (lines.to_string(), 180),
        (format!("{lines}; sleep 300"), 25),
    ] {
        // A log of 64 KiB, and 16 KiB of its end.
        let (mut ctx, _) = task(&script, secs);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task_id = ctx.identity.as_ref().unwrap().task_id.clone();
        ctx.log_sink = Some(
            LogSink::new(tx, task_id, 1, Redactor::default())
                .with_cap(64 * 1024)
                .with_tail(16 * 1024),
        );
        let ran = exec.execute(&ctx).await;
        match secs {
            180 => assert_eq!(ran.expect("the task runs").output, whole),
            _ => assert!(ran.expect_err("it times out").is::<TimeoutError>()),
        }
        let chunks: Vec<LogChunk> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let held = chunks
            .iter()
            .position(|c| c.chunk.contains("is held back"))
            .expect("the line the log is held at");
        assert_eq!(chunks.len(), held + 2, "{secs}: then one chunk, its end");
        let end = &chunks[held + 1];
        assert!(!end.first, "added to the log");
        let (gap, tail) = end.chunk.split_once('\n').unwrap();
        assert!(gap.ends_with("; what follows is its end"), "{gap}");
        assert!(tail.starts_with("line ") && tail.ends_with("line 5000 of the task\n"));
        assert!(tail.len() <= 16 * 1024 && whole.ends_with(tail));
        let (_, counts) = gap.split_once("output (").unwrap();
        let omitted: usize = counts.split_once(' ').unwrap().0.parse().unwrap();
        assert_eq!(64 * 1024 + omitted + tail.len(), whole.len());
        let head: String = chunks[..=held].iter().map(|c| c.chunk.as_str()).collect();
        assert!(head.starts_with(&whole[..64 * 1024]));
    }
}
