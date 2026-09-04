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
#[derive(Debug)]
pub struct ExecOutput {
    pub success: bool,
    pub output: String,
}

/// Marker error for a task killed by its `timeout_secs` deadline — as opposed to
/// a non-zero exit or a spawn/backend error. Every executor backend returns this
/// (wrapped in `anyhow`) when it aborts a task at the deadline, so the worker can
/// `downcast` and tell the reconcile loop the failure was a timeout. That lets a
/// task with `retry_on_timeout: false` (fast-win #24) skip the retry a deadline
/// kill would otherwise burn (such kills usually recur). Carries the deadline for
/// the message.
#[derive(Debug)]
pub struct TimeoutError {
    pub secs: u64,
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command timed out after {}s", self.secs)
    }
}

impl std::error::Error for TimeoutError {}

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

// ── Task wall clock ──────────────────────────────────────────────────────────

/// What a task gets when it names no `timeout_secs` of its own.
pub const DEFAULT_TASK_TIMEOUT_SECS: u64 = 25;

/// The wall clock a task actually gets: what it asked for, the default when it
/// asked for nothing, and never more than `DAGRON_MAX_TASK_TIMEOUT_SECS` where
/// that is set.
///
/// **Why a ceiling exists.** `timeout_secs` comes from the workflow, so before
/// this the longest a task could run was whatever its author typed. That is
/// correct when the engine and the workflows share an owner — it is your
/// hardware — and wrong the moment the operator of the engine is not the author
/// of every DAG on it. Caps on *how many* runs or tasks an installation admits
/// say nothing about worst-case compute while a single task can run for a week.
///
/// **Unset means unlimited**, so an existing deployment behaves exactly as it
/// did. Where a ceiling is set, a workflow asking for longer is clamped rather
/// than rejected, because the alternative is a DAG that validated yesterday
/// failing to admit today with nothing in the task itself having changed.
///
/// A present-but-unusable value (unparseable, or `0`, which would time every task
/// out instantly) is treated as unset and **said out loud once** — a ceiling that
/// silently is not a ceiling is the failure this function exists to prevent.
pub fn effective_timeout_secs(requested: Option<u64>) -> u64 {
    // Read once. The two halves below are split out and pure so they can be tested
    // for every input, which a function consulting a process-wide cache cannot be:
    // the first test to run would fix the ceiling for all the others.
    static CEILING: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    let ceiling = *CEILING
        .get_or_init(|| parse_ceiling(std::env::var("DAGRON_MAX_TASK_TIMEOUT_SECS").ok().as_deref()));
    clamp_to(requested, ceiling)
}

/// `DAGRON_MAX_TASK_TIMEOUT_SECS` as a ceiling, or `None` for no ceiling.
fn parse_ceiling(raw: Option<&str>) -> Option<u64> {
    let raw = raw?;
    if raw.trim().is_empty() {
        return None;
    }
    match raw.trim().parse::<u64>() {
        Ok(n) if n > 0 => Some(n),
        _ => {
            // Not silent. `0` would time every task out instantly and a typo would
            // leave the fleet uncapped — either way the operator set this variable
            // believing it did something.
            tracing::warn!(
                value = %raw,
                "DAGRON_MAX_TASK_TIMEOUT_SECS is not a positive integer; \
                 no task-duration ceiling is in force"
            );
            None
        }
    }
}

/// What the task asked for (or the default), never above the ceiling.
fn clamp_to(requested: Option<u64>, ceiling: Option<u64>) -> u64 {
    let wanted = requested.unwrap_or(DEFAULT_TASK_TIMEOUT_SECS);
    match ceiling {
        Some(max) => wanted.min(max),
        None => wanted,
    }
}

// ── Low-level subprocess runner ───────────────────────────────────────────────

/// Spawns `command[0]` with `command[1..]` as args.
/// `timeout_secs` caps execution; falls back to [`DEFAULT_TASK_TIMEOUT_SECS`]
/// (inside the 30 s lease) and is clamped by [`effective_timeout_secs`].
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
    let secs = effective_timeout_secs(timeout_secs);
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
        .map_err(|_| anyhow::Error::new(TimeoutError { secs }))??;

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if !stderr.is_empty() {
        // Mask sensitive env values out of the live stderr log too (#8); the
        // stored output is redacted centrally in the worker.
        let redactor = crate::redact::Redactor::from_task_env(env);
        tracing::warn!(stderr = %redactor.redact(stderr.trim()), "subprocess stderr");
    }
    Ok((exit_code, with_stderr_on_failure(exit_code, stdout, &stderr)))
}

/// How much of a failing task's stderr rides along in the stored output.
///
/// Unbounded would be wrong — a task that dies in a retry loop can emit
/// megabytes, and this string lands in a database column, once per attempt.
/// The **tail** rather than the head because the fatal error is the last thing
/// a process prints; the head is startup banners.
const STDERR_TAIL_LIMIT: usize = 16 * 1024;

/// Append a failing command's stderr to its stored output.
///
/// **Only on failure**, which is the whole care here. `output` is load-bearing
/// on the success path — `repeat.until` decides loop termination from it, the
/// memoization store caches it, and `produces:` lineage reads it — so a
/// successful task's output stays byte-identical to what it has always been.
/// A failing task's output is only ever an error message, and it was missing
/// the half that says what went wrong: stderr was logged and then discarded,
/// so `RuntimeError`, `CUDA error`, and every NCCL warning — all of which are
/// written to stderr — never reached the stored record or the fault
/// classifier that now reads it.
///
/// This also makes the local backend agree with the other two: DockerExecutor
/// interleaves both streams into `output`, and KubeExecutor stores the pod's
/// combined log. Local was the odd one out.
fn with_stderr_on_failure(exit_code: i32, stdout: String, stderr: &str) -> String {
    if exit_code == 0 || stderr.trim().is_empty() {
        return stdout;
    }
    let trimmed = stderr.trim_end();
    let tail = if trimmed.len() > STDERR_TAIL_LIMIT {
        // Cut on a char boundary, then forward to the next line break so the
        // first retained line is whole rather than starting mid-token.
        //
        // When the tail contains no line break at all — one enormous line, which
        // a JSON-logging framework produces — there is no boundary to forward
        // to and the retained text does start mid-line. That is deliberate:
        // truncating from the front of a single line keeps the end, and the end
        // is where the error is. Dropping it entirely to preserve a "whole
        // line" property would discard the only diagnostic there is.
        let start = trimmed.len() - STDERR_TAIL_LIMIT;
        let start = (start..trimmed.len())
            .find(|i| trimmed.is_char_boundary(*i))
            .unwrap_or(trimmed.len());
        let rest = &trimmed[start..];
        match rest.find('\n') {
            Some(nl) => &rest[nl + 1..],
            None => rest,
        }
    } else {
        trimmed
    };
    if stdout.trim().is_empty() {
        return tail.to_string();
    }
    let mut out = stdout;
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(tail);
    out
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
    let secs = effective_timeout_secs(timeout_secs);
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
        .map_err(|_| anyhow::Error::new(TimeoutError { secs }))??;

    if !stderr_s.is_empty() {
        let redactor = crate::redact::Redactor::from_task_env(env);
        tracing::warn!(stderr = %redactor.redact(stderr_s.trim()), "subprocess stderr");
    }
    let code = status.code().unwrap_or(-1);
    Ok((code, with_stderr_on_failure(code, stdout_s, &stderr_s)))
}

#[cfg(test)]
mod timeout_ceiling_tests {
    use super::{clamp_to, parse_ceiling, DEFAULT_TASK_TIMEOUT_SECS};

    /// Unset must mean unlimited. A self-hosted engine runs on its owner's
    /// hardware, and a ceiling appearing there because the cloud wanted one would
    /// break workflows that have been correct for as long as they have existed.
    #[test]
    fn no_ceiling_leaves_the_task_alone() {
        assert_eq!(parse_ceiling(None), None);
        assert_eq!(clamp_to(Some(86_400), None), 86_400);
        assert_eq!(clamp_to(None, None), DEFAULT_TASK_TIMEOUT_SECS);
    }

    /// The point of the whole change: what a workflow asks for is a request, not a
    /// grant, once an operator has set a ceiling.
    #[test]
    fn a_ceiling_binds_the_request_and_the_default() {
        assert_eq!(clamp_to(Some(86_400), Some(600)), 600);
        assert_eq!(clamp_to(Some(5), Some(600)), 5, "under the ceiling is untouched");
        assert_eq!(clamp_to(None, Some(10)), 10, "the default is clamped too");
        assert_eq!(clamp_to(None, Some(600)), DEFAULT_TASK_TIMEOUT_SECS);
    }

    /// A ceiling that silently is not a ceiling is the failure this exists to
    /// prevent, so every unusable value resolves to "no ceiling" — loudly, via the
    /// warning in `parse_ceiling` — rather than to something arbitrary. `0` matters
    /// most: read as a ceiling it would time out every task in the fleet instantly.
    #[test]
    fn an_unusable_ceiling_is_no_ceiling() {
        for raw in ["0", "abc", "-1", "12s", " ", ""] {
            assert_eq!(parse_ceiling(Some(raw)), None, "{raw:?} must not become a ceiling");
        }
        assert_eq!(parse_ceiling(Some("600")), Some(600));
        assert_eq!(parse_ceiling(Some("  600  ")), Some(600), "whitespace is trimmed");
    }
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

    /// A task that outruns its `timeout_secs` deadline returns an error that
    /// downcasts to [`TimeoutError`], so the reconcile loop can distinguish a
    /// deadline kill from a non-zero exit and honor `retry_on_timeout` (#24).
    /// Covers both the buffered and the streaming (log-sink) paths.
    #[tokio::test]
    async fn timeout_is_a_typed_timeout_error() {
        // Buffered path.
        let ctx = ExecContext::new(vec!["sleep".to_string(), "5".to_string()], Some(1), None);
        let err = LocalExecutor.execute(&ctx).await.expect_err("must time out");
        assert!(err.is::<TimeoutError>(), "buffered timeout must be a TimeoutError, got: {err}");

        // Streaming path (log sink wired).
        let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "t".to_string(), 1, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["sleep".to_string(), "5".to_string()],
            timeout_secs: Some(1),
            docker_image: None,
            env: vec![],
            resources: None,
            service_account: None,
            log_sink: Some(sink),
        };
        let err = LocalExecutor.execute(&ctx).await.expect_err("must time out");
        assert!(err.is::<TimeoutError>(), "streaming timeout must be a TimeoutError, got: {err}");
    }

    /// A plain non-zero exit is NOT a timeout — it stays a normal failure so the
    /// usual attempts-based retry still applies.
    #[tokio::test]
    async fn nonzero_exit_is_not_a_timeout() {
        let ctx = ExecContext::new(vec!["false".to_string()], Some(10), None);
        let out = LocalExecutor.execute(&ctx).await.expect("false exits cleanly, non-zero");
        assert!(!out.success, "`false` exits non-zero");
    }

    /// A successful task's stored output must stay byte-identical: `repeat.until`
    /// terminates loops from it, the memo store caches it, and `produces:`
    /// lineage reads it. Folding stderr in on success would change all three.
    #[tokio::test]
    async fn a_successful_command_still_stores_only_its_stdout() {
        let ctx = ExecContext {
            command: vec!["sh".into(), "-c".into(), "echo out; echo noise >&2".into()],
            timeout_secs: Some(10),
            docker_image: None,
            env: vec![],
            resources: None,
            service_account: None,
            log_sink: None,
        };
        let out = LocalExecutor.execute(&ctx).await.unwrap();
        assert!(out.success);
        assert_eq!(out.output.trim(), "out", "stderr must not leak into a success");
    }

    /// A failing task's stderr is the half that says what went wrong — and on a
    /// GPU fleet it is where every CUDA error, NCCL warning and Python traceback
    /// is written. It was logged and then discarded, so the stored record (and
    /// the fault classifier that reads it) never saw any of it.
    #[tokio::test]
    async fn a_failing_command_carries_its_stderr_into_the_stored_output() {
        let ctx = ExecContext {
            command: vec![
                "sh".into(),
                "-c".into(),
                "echo step 100; echo 'RuntimeError: CUDA out of memory' >&2; exit 1".into(),
            ],
            timeout_secs: Some(10),
            docker_image: None,
            env: vec![],
            resources: None,
            service_account: None,
            log_sink: None,
        };
        let out = LocalExecutor.execute(&ctx).await.unwrap();
        assert!(!out.success);
        assert!(out.output.contains("step 100"), "stdout is kept: {:?}", out.output);
        assert!(out.output.contains("CUDA out of memory"), "stderr is appended: {:?}", out.output);
        // And it is now classifiable, which is the point.
        let c = dagron_core::fault::classify_text(&out.output).unwrap();
        assert_eq!(c.class, dagron_core::fault::FaultClass::GpuOom);
    }

    /// The streaming path (#17) must behave identically — a task with live logs
    /// enabled is not a task with worse diagnostics.
    #[tokio::test]
    async fn the_streaming_path_appends_stderr_on_failure_too() {
        let (tx, _rx) = mpsc::unbounded_channel::<LogChunk>();
        let sink = LogSink::new(tx, "task-1".to_string(), 1, crate::redact::Redactor::default());
        let ctx = ExecContext {
            command: vec!["sh".into(), "-c".into(), "echo hi; echo 'Xid 79' >&2; exit 1".into()],
            timeout_secs: Some(10),
            docker_image: None,
            env: vec![],
            resources: None,
            service_account: None,
            log_sink: Some(sink),
        };
        let out = LocalExecutor.execute(&ctx).await.unwrap();
        assert!(!out.success);
        assert!(out.output.contains("hi"));
        assert!(out.output.contains("Xid 79"), "{:?}", out.output);
    }

    #[test]
    fn the_stderr_tail_is_bounded_and_starts_on_a_whole_line() {
        // A task dying in a loop can emit megabytes, once per attempt, into a
        // database column. The tail — not the head — because the fatal error is
        // the last thing a process prints.
        let noise = "startup banner line\n".repeat(4000);
        let err = format!("{noise}FATAL: Xid 79, GPU has fallen off the bus");
        let out = with_stderr_on_failure(1, String::new(), &err);
        assert!(out.len() <= STDERR_TAIL_LIMIT + 64, "bounded: {}", out.len());
        assert!(out.contains("Xid 79"), "the end is what is kept");
        assert!(out.starts_with("startup banner line"), "starts on a line: {:?}", &out[..40]);
    }

    #[test]
    fn a_single_oversized_line_keeps_its_end_rather_than_being_dropped() {
        // One line, no breaks, well over the limit. There is no line boundary
        // to cut on, so the tail starts mid-line — and that is the right
        // trade: the end of the line is where the error is.
        let err = format!("{}CUDA error: device-side assert triggered", "x".repeat(40_000));
        let out = with_stderr_on_failure(1, String::new(), &err);
        assert!(out.len() <= STDERR_TAIL_LIMIT + 64, "still bounded: {}", out.len());
        assert!(out.ends_with("device-side assert triggered"), "the end survives");
        // And it is still classifiable, which is the reason any of this is kept.
        assert_eq!(
            dagron_core::fault::classify_text(&out).unwrap().class,
            dagron_core::fault::FaultClass::UserCode
        );
    }

    #[test]
    fn stderr_is_not_appended_when_there_is_none_or_when_the_task_succeeded() {
        assert_eq!(with_stderr_on_failure(0, "out".into(), "noise"), "out");
        assert_eq!(with_stderr_on_failure(1, "out".into(), "   \n "), "out");
        // No stdout at all: the output is just the stderr, with no stray blank line.
        assert_eq!(with_stderr_on_failure(1, String::new(), "boom"), "boom");
    }
}
