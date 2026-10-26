//! A task container's output, against a real Docker daemon, read as the task
//! writes it: from the container's own streams, attached before
//! it starts, and sent to the task's log as each line ends.
//!
//! So a line is in the log while the task still runs, a task that timed out
//! keeps what it wrote, and the log is every line the task wrote whatever
//! the daemon's log driver keeps: Podman's own, the journal, drops lines past
//! a rate limit, which the last test here passes only because what the task
//! writes is not read back from it.
//!
//! Needs a daemon, and skips, saying so, where none answers. The image is
//! pulled first, since the executor runs only images the daemon has.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bollard::container::ListContainersOptions;
use bollard::image::CreateImageOptions;
use bollard::Docker;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{
    ExecContext, Executor, LogChunk, LogSink, TaskIdentity, TimeoutError,
};
use dagron_executor::redact::Redactor;
use futures_util::TryStreamExt;
use tokio::sync::mpsc;

const IMAGE: &str = "alpine:3.20";

/// A daemon that answers, with the test image; `None`, said, where none does.
async fn daemon() -> Option<Docker> {
    let Ok(docker) = Docker::connect_with_local_defaults() else {
        eprintln!("no Docker client here - skipping");
        return None;
    };
    if let Err(e) = docker.ping().await {
        eprintln!("no Docker daemon answers ({e}) - skipping");
        return None;
    }
    docker
        .create_image(
            Some(CreateImageOptions {
                from_image: IMAGE,
                ..Default::default()
            }),
            None,
            None,
        )
        .try_collect::<Vec<_>>()
        .await
        .expect("pull the test image");
    Some(docker)
}

/// A task `task` that runs `script`, within `secs`, with a log.
fn task(task: &str, script: &str, secs: u64) -> (ExecContext, mpsc::UnboundedReceiver<LogChunk>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(secs),
        None,
    );
    ctx.log_sink = Some(LogSink::new(tx, task.into(), 1, Redactor::default()));
    ctx.identity = Some(TaskIdentity {
        task_id: task.into(),
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

/// The containers labelled with `task`'s id: what it left.
async fn containers_of(docker: &Docker, task: &str) -> usize {
    let filters = HashMap::from([(
        "label".to_string(),
        vec![format!("dagron.dev/task-id={task}")],
    )]);
    docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        }))
        .await
        .expect("list containers")
        .len()
}

/// A task's first line is in its log while the task runs, seconds before
/// its last is written; and the log is its output, each line once.
#[tokio::test]
async fn a_task_s_lines_are_in_its_log_as_it_writes_them() {
    let Some(_docker) = daemon().await else {
        return;
    };
    let exec = DockerExecutor::connect(IMAGE).await.unwrap();
    let (ctx, mut rx) = task("t-follow-live", "echo first; sleep 5; echo second", 60);
    let began = Instant::now();
    let run = exec.execute(&ctx);
    tokio::pin!(run);
    let first = tokio::select! {
        out = &mut run => panic!("the task ended before its first line was in its log: {out:?}"),
        chunk = rx.recv() => chunk.expect("a chunk"),
    };
    let at = began.elapsed();
    assert_eq!(first.chunk, "first\n");
    assert!(first.first, "the attempt's first chunk is flagged");
    let out = run.await.expect("the task runs");
    assert!(out.success, "{}", out.output);
    assert!(
        began.elapsed() >= at + Duration::from_secs(4),
        "the first line came {at:?} after the start, and the task ended {:?} after it",
        began.elapsed()
    );
    assert_eq!(out.output, "first\nsecond\n");
    assert_eq!(
        format!("first\n{}", log_of(&mut rx)),
        out.output,
        "each line once"
    );
}

/// A task that ran out of time keeps what it wrote before it was stopped,
/// and its container is gone.
#[tokio::test]
async fn a_task_that_timed_out_keeps_what_it_wrote() {
    let Some(docker) = daemon().await else { return };
    let exec = DockerExecutor::connect(IMAGE).await.unwrap();
    let (ctx, mut rx) = task(
        "t-follow-timeout",
        "echo before; printf 'no newline'; sleep 60",
        3,
    );
    let err = exec.execute(&ctx).await.expect_err("it times out");
    assert!(err.is::<TimeoutError>(), "{err:#}");
    assert_eq!(log_of(&mut rx), "before\nno newline");
    assert_eq!(containers_of(&docker, "t-follow-timeout").await, 0);
}

/// Every line of a task that writes fast to both streams is in its log,
/// whole and once, each stream's in its order: 35,000 lines in about a
/// second, more than the journal keeps of a user's containers in thirty.
#[tokio::test]
async fn every_line_of_a_task_that_writes_fast_is_in_its_log() {
    let Some(_docker) = daemon().await else {
        return;
    };
    let exec = DockerExecutor::connect(IMAGE).await.unwrap();
    let script = "awk 'BEGIN { for (i = 1; i <= 30000; i++) print \"out \" i }' & \
                  awk 'BEGIN { for (i = 1; i <= 5000; i++) print \"err \" i > \"/dev/stderr\" }'; \
                  wait";
    let (ctx, mut rx) = task("t-follow-fast", script, 120);
    let out = exec.execute(&ctx).await.expect("the task runs");
    assert!(out.success);
    let (mut outs, mut errs) = (0, 0);
    for line in out.output.lines() {
        match line.split_once(' ') {
            Some(("out", n)) if n.parse::<usize>() == Ok(outs + 1) => outs += 1,
            Some(("err", n)) if n.parse::<usize>() == Ok(errs + 1) => errs += 1,
            _ => panic!("line {line:?} after out {outs} and err {errs}"),
        }
    }
    assert_eq!((outs, errs), (30_000, 5_000));
    assert_eq!(log_of(&mut rx), out.output, "the log is the output");
}

/// A task that writes past its log's cap keeps its last lines: the log is
/// sent to its cap as the task writes it and held there, and once the task
/// is over its end is sent in one piece, under a line that says what was
/// left out. So too where it ran out of time.
#[tokio::test]
async fn a_task_past_its_cap_keeps_its_last_lines() {
    let Some(_docker) = daemon().await else {
        return;
    };
    let exec = DockerExecutor::connect(IMAGE).await.unwrap();
    let lines =
        "awk 'BEGIN { for (i = 1; i <= 5000; i++) printf \"line %04d of the task\\n\", i }'";
    let whole: String = (1..=5000)
        .map(|i| format!("line {i:04} of the task\n"))
        .collect();
    for (task_id, script, secs) in [
        ("t-follow-tail", lines.to_string(), 60),
        ("t-follow-tail-late", format!("{lines}; sleep 60"), 5),
    ] {
        // A log of 64 KiB, and 16 KiB of its end.
        let (mut ctx, _) = task(task_id, &script, secs);
        let (tx, mut rx) = mpsc::unbounded_channel();
        ctx.log_sink = Some(
            LogSink::new(tx, task_id.into(), 1, Redactor::default())
                .with_cap(64 * 1024)
                .with_tail(16 * 1024),
        );
        let ran = exec.execute(&ctx).await;
        match secs {
            60 => assert_eq!(ran.expect("the task runs").output, whole),
            _ => assert!(ran.expect_err("it times out").is::<TimeoutError>()),
        }
        let chunks: Vec<LogChunk> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let held = chunks
            .iter()
            .position(|c| c.chunk.contains("is held back"))
            .expect("the line the log is held at");
        assert_eq!(chunks.len(), held + 2, "{task_id}: then one chunk, its end");
        let end = &chunks[held + 1];
        assert!(!end.first, "{task_id}: added to the log");
        let (gap, tail) = end.chunk.split_once('\n').unwrap();
        assert!(
            gap.ends_with("; what follows is its end"),
            "{task_id}: {gap}"
        );
        assert!(tail.starts_with("line ") && tail.ends_with("line 5000 of the task\n"));
        assert!(
            tail.len() <= 16 * 1024 && whole.ends_with(tail),
            "{task_id}"
        );
        let (_, counts) = gap.split_once("output (").unwrap();
        let omitted: usize = counts.split_once(' ').unwrap().0.parse().unwrap();
        assert_eq!(64 * 1024 + omitted + tail.len(), whole.len(), "{task_id}");
        let head: String = chunks[..=held].iter().map(|c| c.chunk.as_str()).collect();
        assert!(head.starts_with(&whole[..64 * 1024]), "{task_id}");
    }
}
