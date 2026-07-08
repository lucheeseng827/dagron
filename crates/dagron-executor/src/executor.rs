use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{timeout, Duration};

// ── Shared types ─────────────────────────────────────────────────────────────

/// Output returned by every executor backend.
pub struct ExecOutput {
    pub success: bool,
    pub output: String,
}

/// One incremental chunk of a running task's output, streamed for live tailing
/// (fast-win #17). The executor emits these through a [`LogSink`] as output
/// arrives; the reconcile loop appends them to the task's stored output so the
/// UI/API can tail it before the task exits. `first` marks the first chunk of an
/// attempt so the loop resets any prior-attempt output before appending.
pub struct LogChunk {
    pub task_id: String,
    pub fence: i64,
    pub chunk: String,
    pub first: bool,
}

/// A per-task handle an executor uses to stream incremental output. Bound to one
/// `(task_id, fence)` by the worker; secrets are masked here (so streamed chunks
/// are redacted like the final output, #8) and the first chunk is flagged so the
/// loop can reset a retried task's prior output. Cheap to clone.
#[derive(Clone)]
pub struct LogSink {
    tx: UnboundedSender<LogChunk>,
    task_id: String,
    fence: i64,
    redactor: crate::redact::Redactor,
    started: Arc<AtomicBool>,
}

impl LogSink {
    /// Build a sink bound to one task attempt.
    pub fn new(
        tx: UnboundedSender<LogChunk>,
        task_id: String,
        fence: i64,
        redactor: crate::redact::Redactor,
    ) -> Self {
        Self { tx, task_id, fence, redactor, started: Arc::new(AtomicBool::new(false)) }
    }

    /// Stream one output chunk (redacted). Best-effort: a closed receiver (loop
    /// gone) or an empty chunk is silently dropped. Streaming redaction is
    /// chunk-wise, so a secret split across chunks may slip through the live view
    /// — the final stored output is always redacted whole, so it self-corrects.
    pub fn append(&self, chunk: &str) {
        if chunk.is_empty() {
            return;
        }
        let redacted = self.redactor.redact(chunk).into_owned();
        let first = !self.started.swap(true, Ordering::SeqCst);
        let _ = self.tx.send(LogChunk {
            task_id: self.task_id.clone(),
            fence: self.fence,
            chunk: redacted,
            first,
        });
    }
}

/// All inputs an executor needs to run one task.
pub struct ExecContext {
    pub command: Vec<String>,
    pub timeout_secs: Option<u64>,
    /// Docker image hint — used by DockerExecutor, ignored by LocalExecutor.
    pub docker_image: Option<String>,
    /// Environment variables for the task. Applied by every backend (subprocess
    /// env, container env, pod env).
    pub env: Vec<dagron_core::dag::EnvVar>,
    /// Per-task pod resource requests/limits. KubeExecutor only.
    pub resources: Option<dagron_core::dag::ResourceRequirements>,
    /// ServiceAccount (IRSA) for the task pod. KubeExecutor only.
    pub service_account: Option<String>,
    /// Optional live-log sink (#17). When set, the executor streams incremental
    /// output here as it arrives; when `None` the output is only returned in full
    /// at exit (the original behaviour). The worker wires this up per attempt.
    pub log_sink: Option<LogSink>,
}

impl ExecContext {
    /// Build a minimal context (no env / resources / service account) — used by
    /// tests and the internal no-op fallback path.
    pub fn new(command: Vec<String>, timeout_secs: Option<u64>, docker_image: Option<String>) -> Self {
        Self {
            command,
            timeout_secs,
            docker_image,
            env: Vec::new(),
            resources: None,
            service_account: None,
            log_sink: None,
        }
    }
}

// ── Executor trait ────────────────────────────────────────────────────────────

/// Pluggable execution backend. Swap between local subprocesses, Docker
/// containers, Kubernetes pods, or remote workers without touching the
/// reconcile loop.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput>;
}

// ── LocalExecutor ─────────────────────────────────────────────────────────────

/// Subprocess executor — the default backend (original behavior).
pub struct LocalExecutor;

#[async_trait]
impl Executor for LocalExecutor {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        // Stream line-by-line when a live-log sink is wired (#17); otherwise use
        // the byte-exact buffered path (unchanged behaviour).
        let (code, output) = match &ctx.log_sink {
            Some(sink) => {
                run_command_streaming(&ctx.command, ctx.timeout_secs, &ctx.env, sink).await?
            }
            None => run_command(&ctx.command, ctx.timeout_secs, &ctx.env).await?,
        };
        Ok(ExecOutput { success: code == 0, output })
    }
}

// ── Low-level subprocess runner ───────────────────────────────────────────────

/// Spawns `command[0]` with `command[1..]` as args.
/// `timeout_secs` caps execution; falls back to 25 s (inside the 30 s lease).
/// `env` is layered on top of the inherited environment.
/// `kill_on_drop` ensures the child is reaped if the future is dropped.
pub async fn run_command(
    command: &[String],
    timeout_secs: Option<u64>,
    env: &[dagron_core::dag::EnvVar],
) -> Result<(i32, String)> {
    if command.is_empty() {
        bail!("empty command");
    }
    let secs = timeout_secs.unwrap_or(25);
    if secs == 0 {
        bail!("timeout_secs must be >= 1 when provided");
    }
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..]).kill_on_drop(true);
    for e in env {
        cmd.env(&e.name, &e.value);
    }

    let output = timeout(Duration::from_secs(secs), cmd.output())
        .await
        .map_err(|_| anyhow::anyhow!("command timed out after {secs}s"))??;

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if !stderr.is_empty() {
        // Mask sensitive env values out of the live stderr log too (#8); the
        // stored stdout is redacted centrally in the worker.
        let redactor = crate::redact::Redactor::from_task_env(env);
        tracing::warn!(stderr = %redactor.redact(stderr.trim()), "subprocess stderr");
    }
    Ok((exit_code, stdout))
}

/// Streaming variant of [`run_command`] (#17): pipes stdout and forwards each
/// line to `sink` as it arrives (for live tailing) while accumulating the full
/// stdout to return at exit. stderr is drained concurrently (so a chatty child
/// can't deadlock on a full pipe) and logged redacted, matching `run_command`.
/// Line-buffered, so it appends a trailing newline per line — a cosmetic
/// difference from the byte-exact buffered path, acceptable for a log tail.
async fn run_command_streaming(
    command: &[String],
    timeout_secs: Option<u64>,
    env: &[dagron_core::dag::EnvVar],
    sink: &LogSink,
) -> Result<(i32, String)> {
    use std::process::Stdio;

    if command.is_empty() {
        bail!("empty command");
    }
    let secs = timeout_secs.unwrap_or(25);
    if secs == 0 {
        bail!("timeout_secs must be >= 1 when provided");
    }
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..])
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for e in env {
        cmd.env(&e.name, &e.value);
    }

    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    let combined = async {
        let mut lines = BufReader::new(stdout).lines();
        // Read stdout (streaming to the sink) and stderr concurrently so neither
        // pipe backpressures the child into a deadlock.
        let stdout_fut = async {
            let mut acc = String::new();
            while let Some(line) = lines.next_line().await? {
                acc.push_str(&line);
                acc.push('\n');
                sink.append(&format!("{line}\n"));
            }
            Ok::<String, anyhow::Error>(acc)
        };
        let stderr_fut = async {
            let mut s = String::new();
            BufReader::new(stderr).read_to_string(&mut s).await?;
            Ok::<String, anyhow::Error>(s)
        };
        let (out, err) = tokio::try_join!(stdout_fut, stderr_fut)?;
        let status = child.wait().await?;
        Ok::<_, anyhow::Error>((status, out, err))
    };

    let (status, stdout_s, stderr_s) = timeout(Duration::from_secs(secs), combined)
        .await
        .map_err(|_| anyhow::anyhow!("command timed out after {secs}s"))??;

    if !stderr_s.is_empty() {
        let redactor = crate::redact::Redactor::from_task_env(env);
        tracing::warn!(stderr = %redactor.redact(stderr_s.trim()), "subprocess stderr");
    }
    Ok((status.code().unwrap_or(-1), stdout_s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    /// LocalExecutor streams each stdout line to the sink as a chunk (first-flagged
    /// on the first) while still returning the full accumulated output (#17).
    #[tokio::test]
    async fn local_executor_streams_lines_to_sink() {
        let (tx, mut rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "task-1".to_string(), 7, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["printf".to_string(), "a\\nb\\n".to_string()],
            timeout_secs: Some(10),
            docker_image: None,
            env: vec![],
            resources: None,
            service_account: None,
            log_sink: Some(sink),
        };

        let out = LocalExecutor.execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output, "a\nb\n", "full output still returned for the final store");

        // The two lines streamed as two chunks; only the first carries `first`.
        let mut chunks = Vec::new();
        while let Ok(c) = rx.try_recv() {
            chunks.push(c);
        }
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk, "a\n");
        assert!(chunks[0].first, "first chunk flagged for the reset-on-attempt");
        assert_eq!(chunks[0].task_id, "task-1");
        assert_eq!(chunks[0].fence, 7);
        assert_eq!(chunks[1].chunk, "b\n");
        assert!(!chunks[1].first);
    }

    /// With no sink, the buffered path runs unchanged (byte-exact output).
    #[tokio::test]
    async fn local_executor_without_sink_is_buffered() {
        let ctx = ExecContext::new(vec!["printf".to_string(), "hi".to_string()], Some(10), None);
        let out = LocalExecutor.execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output, "hi", "no trailing newline added on the buffered path");
    }
}
