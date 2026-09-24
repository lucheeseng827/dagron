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
    /// The **effective** trust envelope for this task — already raised to the
    /// operator's floor by the engine. KubeExecutor shapes the pod's security
    /// context from it; the Local and Docker executors cannot enforce it, which
    /// is why the engine refuses such a task before dispatch rather than
    /// letting it run believing itself sandboxed
    /// (`IsolationSpec::require_enforceable_by`).
    pub isolation: Option<dagron_core::isolation::IsolationSpec>,
    /// Optional live-log sink (#17). When set, the executor streams incremental
    /// output here as it arrives; when `None` the output is only returned in full
    /// at exit (the original behaviour). The worker wires this up per attempt.
    pub log_sink: Option<LogSink>,
    /// Which task row this execution belongs to, and which attempt of it.
    ///
    /// `None` for callers with no task row behind them — tests and the internal
    /// no-op fallback. A backend that creates a remote workload (a pod, a
    /// container) **labels it with this** so the workload can be found again by
    /// something other than the process that created it. See [`TaskIdentity`].
    pub identity: Option<TaskIdentity>,
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
            isolation: None,
            log_sink: None,
            identity: None,
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

    /// Every workload this installation owns that is old enough to judge.
    ///
    /// The **listing** half of the fleet sweep. It is split from the deleting
    /// half because only the engine can answer the question in between — "is
    /// this task still live?" is a database query, and an `Executor` has no
    /// database. Handing the executor a pool instead would put schema knowledge
    /// behind a trait anyone may implement.
    ///
    /// Default: an empty list, so an executor with nothing to sweep (the local
    /// process pool, whose children die with it) inherits a correct no-op
    /// rather than being forced to write one.
    async fn list_orphan_candidates(&self, _scope: &OrphanScope<'_>) -> Result<Vec<ManagedWorkload>> {
        Ok(Vec::new())
    }

    /// Delete one workload [`Executor::list_orphan_candidates`] returned.
    ///
    /// Takes the handle from the listing rather than re-deriving one, so the
    /// thing deleted is the thing judged. A backend that cannot delete says so
    /// with an `Err`; the sweep logs it and moves on, because one undeletable
    /// leftover must not stop the rest.
    async fn delete_workload(&self, _w: &ManagedWorkload) -> Result<()> {
        Ok(())
    }
}

/// What a fleet sweep is allowed to look at.
///
/// Both fields narrow, and neither has a safe default — which is why this is a
/// parameter rather than executor state read from the environment.
#[derive(Debug, Clone)]
pub struct OrphanScope<'a> {
    /// The [`LABEL_INSTALLATION`] value to select on. Required, and the reason
    /// the whole sweep is opt-in: see that constant.
    pub installation: &'a str,
    /// Workloads younger than this are never candidates.
    ///
    /// Not a tuning knob — a correctness one. The sweep asks the database
    /// which tasks are live, and a workload created *after* that answer was
    /// computed would look orphaned because its row had not been written yet.
    /// Listing before querying narrows the window; refusing to judge anything
    /// young closes it, without needing the two to be atomic across a
    /// datastore and an apiserver that share no transaction.
    pub min_age: std::time::Duration,
}

/// A workload a fleet sweep found, and enough identity to judge it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedWorkload {
    /// Backend-specific handle — a pod name, a container id. Whatever
    /// [`Executor::delete_workload`] needs to address exactly this one.
    pub handle: String,
    /// The task this workload says it belongs to. The sweep's whole question is
    /// whether that task is still live.
    pub task_id: String,
    /// For the operator reading the log line, not for the decision.
    pub run_id: Option<String>,
    /// Likewise — it says which attempt left this behind.
    pub attempt: Option<String>,
    /// The backend's own identity for this exact object, where it has one.
    ///
    /// A handle names a **slot**; this names the **occupant**. The distinction
    /// is only visible across time, which is exactly the gap a sweep opens: it
    /// lists, then asks the datastore, then deletes, and a name is not a
    /// promise that the three saw the same object. Kubernetes stamps every
    /// object with a UID and will refuse a delete whose precondition names a
    /// different one, so carrying it turns "delete the pod I judged" from a
    /// convention about how names are minted into something the apiserver
    /// enforces.
    ///
    /// `None` where the backend has no equivalent: a Docker container id is
    /// already the identity rather than a name for one, so deletion there
    /// stays id-based and nothing is lost.
    pub uid: Option<String>,
}

impl ManagedWorkload {
    /// Build a candidate from a workload's labels, or refuse it.
    ///
    /// The refusal is the point, and it belongs here rather than in each
    /// backend's listing. A label **selector** constrains only the labels it
    /// names, and says nothing at all about the rest, so a workload can match
    /// `managed-by` and `installation` exactly while carrying a `task-id` that
    /// is empty or not a usable label value. Nothing dagron creates looks like that —
    /// [`TaskIdentity::labels`] refuses to label partially — but a legacy
    /// install, another tool, or a hand-edited manifest can.
    ///
    /// Such a workload would sail through the sweep exactly like a real
    /// orphan: its task id matches no live row, because no row ever had that
    /// id, and the sweep acts on absence. It would then be **deleted**, which
    /// inverts the rule the rest of this module is built on — a workload that
    /// cannot prove it is stale is left alone. [`is_stale_attempt`] already
    /// refuses a missing or unparseable *attempt* for that reason; the task
    /// id, which the whole judgement rests on, deserves it more.
    ///
    /// `run_id` and `attempt` are not validated: they are for the operator
    /// reading the log line, and no decision reads them.
    /// `uid` is the backend's, passed in rather than read from a label: it is
    /// the backend's own identity for the object, not something dagron wrote.
    /// Whether its absence is anomalous is the caller's to decide, because the
    /// answer differs — see [`ManagedWorkload::uid`].
    pub fn from_labels<'a>(
        handle: String,
        uid: Option<String>,
        get: impl Fn(&str) -> Option<&'a String>,
    ) -> Option<Self> {
        let task_id = get(LABEL_TASK_ID)?;
        label_value(task_id)?;
        Some(Self {
            handle,
            task_id: task_id.clone(),
            run_id: get(LABEL_RUN_ID).cloned(),
            attempt: get(LABEL_ATTEMPT).cloned(),
            uid,
        })
    }
}

// ── Task identity, and why a remote workload must carry it ───────────────────

/// Who a remote workload belongs to.
///
/// Before this existed, `KubeExecutor` and `DockerExecutor` both named their
/// workload `sched-<random uuid>` and kept that name **only on the stack of the
/// `execute()` call that created it**. Two things follow, and both are bugs:
///
/// 1. `ARCHITECTURE.md` claims "the lease bounds *concurrent* execution to one
///    holder". Under these two backends it did not. A lease expires while the
///    pod is still running, another scheduler claims the task and creates a
///    *second* pod, and nothing connects either one to the task row — so both
///    run the command at once. The lease bounded which scheduler owned the
///    *row*, never which workloads were running.
/// 2. If the scheduler process dies mid-execution, the random name dies with
///    it. The pod is then unreachable: no label, no derivation, nothing to
///    select on. It runs to completion — or forever, if the command does not
///    exit — and no sweep can ever find it.
///
/// Labelling the workload with the task row fixes both: a dispatch can find and
/// delete a predecessor before starting, and a workload outlives the process
/// that made it without becoming anonymous.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIdentity {
    /// `task_runs.id` — stable across lease recovery, so it identifies the
    /// *task*, not one attempt at it.
    pub task_id: String,
    /// `task_runs.run_id`, for operators grepping by run.
    pub run_id: String,
    /// `task_runs.attempt` — increments on every claim, including a recovery.
    /// That ordering is what makes it the right discriminator here: a workload
    /// carrying a **lower** attempt for the same task is a predecessor, and a
    /// predecessor is what must be reaped before this one starts. A *higher*
    /// one is not — see [`is_stale_attempt`].
    pub attempt: i64,
    /// Which dagron installation this workload belongs to
    /// ([`LABEL_INSTALLATION`]). `None` when the operator has not set
    /// [`DAGRON_INSTALLATION`] — the workload is then labelled with everything
    /// else and the fleet sweep stays off, because a sweep that cannot tell
    /// this install's workloads from another's is a sweep that deletes the
    /// wrong ones.
    pub installation: Option<String>,
}

/// Whether a workload labelled with `labelled` is a predecessor this dispatch
/// may reap, given that this dispatch is attempt `mine`.
///
/// **Only a strictly lower attempt is stale**, and the two rejected cases are
/// each a way to destroy live work:
///
/// - *Equal* is this dispatch's own workload, on a same-attempt re-entry.
/// - *Higher* is a *newer* attempt, and it is very likely running. A scheduler
///   that stalls between claiming a row and creating the workload can resume
///   after its lease expired and another replica claimed the row — so attempt 1
///   can reach this code while attempt 2 is already executing. Reaping
///   "anything not mine" would kill the live successor and then start duplicate
///   work: exactly the fence violation these labels exist to prevent.
/// - *Missing or unparseable* is ignored on the same principle. A workload that
///   cannot prove it is stale is not deleted. An orphan that survives is a job
///   for a fleet-wide sweep, which can weigh liveness against the datastore; a
///   live workload deleted by a stale attempt is work already lost.
pub fn is_stale_attempt(labelled: Option<&str>, mine: i64) -> bool {
    labelled.and_then(|a| a.parse::<i64>().ok()).is_some_and(|a| a < mine)
}

/// Label key marking a workload as this project's, so a sweep can scope itself
/// and never touch a pod or container someone else created.
pub const LABEL_MANAGED_BY: &str = "dagron.dev/managed-by";
/// Label key carrying [`TaskIdentity::task_id`].
pub const LABEL_TASK_ID: &str = "dagron.dev/task-id";
/// Label key carrying [`TaskIdentity::run_id`].
pub const LABEL_RUN_ID: &str = "dagron.dev/run-id";
/// Label key carrying [`TaskIdentity::attempt`].
pub const LABEL_ATTEMPT: &str = "dagron.dev/attempt";
/// The value [`LABEL_MANAGED_BY`] always carries.
pub const MANAGED_BY: &str = "dagron";
/// Label key naming **which dagron installation** owns a workload.
///
/// [`LABEL_MANAGED_BY`] says "some dagron made this"; it does not say *which*.
/// That distinction is the whole reason a fleet-wide sweep is dangerous without
/// this label: two installations sharing one Kubernetes namespace both stamp
/// `managed-by=dagron`, so a sweep scoped only on that reads the other install's
/// pods, fails to find their task ids in its **own** database, concludes they
/// are orphans, and deletes running work belonging to someone else.
///
/// The per-task reap does not need it — it selects on a task id, which is a
/// UUID and therefore globally distinct. A fleet sweep has no such anchor: it
/// looks for workloads whose task is *absent*, and absence is exactly what a
/// foreign installation's workload looks like.
///
/// So the value is operator-supplied ([`DAGRON_INSTALLATION`]) and the sweep is
/// **off** without it. dagron cannot infer it: a namespace can hold two
/// installs, one install can span namespaces, and a pod carries no pointer back
/// to the database that created it.
pub const LABEL_INSTALLATION: &str = "dagron.dev/installation";
/// Environment variable supplying [`LABEL_INSTALLATION`]'s value.
pub const DAGRON_INSTALLATION: &str = "DAGRON_INSTALLATION";

/// Longest value a Kubernetes label may hold.
const MAX_LABEL_VALUE: usize = 63;

impl TaskIdentity {
    /// The labels a workload carries. Empty when the identity cannot be
    /// expressed as valid labels, which is a refusal to label *partially*:
    /// a pod carrying `managed-by` but no `task-id` would be selected by a
    /// sweep and match no task, which is precisely how a reaper deletes live
    /// work.
    pub fn labels(&self) -> std::collections::BTreeMap<String, String> {
        let mut out = std::collections::BTreeMap::new();
        let (Some(task), Some(run)) = (label_value(&self.task_id), label_value(&self.run_id))
        else {
            return out;
        };
        out.insert(LABEL_MANAGED_BY.to_string(), MANAGED_BY.to_string());
        out.insert(LABEL_TASK_ID.to_string(), task);
        out.insert(LABEL_RUN_ID.to_string(), run);
        out.insert(LABEL_ATTEMPT.to_string(), self.attempt.to_string());
        // Only when it expresses as a label. A workload silently carrying a
        // TRUNCATED installation could be selected by another install whose
        // name shares that prefix — so an unusable value contributes nothing
        // and leaves the sweep unable to claim this workload, which is the safe
        // direction.
        if let Some(inst) = self.installation.as_deref().and_then(label_value) {
            out.insert(LABEL_INSTALLATION.to_string(), inst);
        }
        out
    }

    /// Selector matching every workload this project owns for this task,
    /// whatever attempt made it. `None` when the id will not express as a label
    /// — and then nothing is selected, rather than a selector matching more
    /// than intended.
    pub fn task_selector(&self) -> Option<String> {
        let task = label_value(&self.task_id)?;
        Some(format!("{LABEL_MANAGED_BY}={MANAGED_BY},{LABEL_TASK_ID}={task}"))
    }
}

/// A value usable as a Kubernetes label (and therefore as a Docker one, whose
/// rules are looser).
///
/// Validates rather than sanitises. Two different task ids must never map to
/// one label — a sanitiser that strips offending characters can collide, and a
/// collision here means reaping another task's pod. dagron's own ids are
/// hyphenated v4 UUIDs and pass unchanged; anything else is refused and the
/// workload simply goes unlabelled, which is the pre-existing behaviour rather
/// than a new hazard.
pub fn label_value(raw: &str) -> Option<String> {
    if raw.is_empty() || raw.len() > MAX_LABEL_VALUE {
        return None;
    }
    let ok_edge = |c: char| c.is_ascii_alphanumeric();
    let ok_inner = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.';
    let mut chars = raw.chars();
    let first = chars.next()?;
    if !ok_edge(first) || !raw.chars().all(ok_inner) {
        return None;
    }
    if !raw.chars().next_back().is_some_and(ok_edge) {
        return None;
    }
    Some(raw.to_string())
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

/// Build the `Command` for a task, under whatever this pool permits.
///
/// One place, so the buffered and streaming paths cannot drift: a rule that
/// held on one of them and not the other would be a hole shaped like "whether a
/// live-log sink happened to be wired", and the streaming path is the one a
/// real task takes.
///
/// With neither pool knob set this is what it always was — spawn `command[0]`,
/// layer `env` over the inherited environment. See [`crate::pool`] for what the
/// knobs do and why the allowlist matches a resolved path rather than a word.
fn build_command(command: &[String], env: &[dagron_core::dag::EnvVar]) -> Result<Command> {
    let policy = crate::pool::policy();
    let program = policy.program(&command[0], |name| {
        crate::pool::resolve_on_path(name, std::env::var("PATH").ok().as_deref())
    })?;

    let mut cmd = Command::new(program);
    cmd.args(&command[1..]).kill_on_drop(true);
    match policy.child_env(env, |name| std::env::var(name).ok())? {
        // Isolated: the child gets exactly this, and nothing it was not given.
        Some(pairs) => {
            cmd.env_clear();
            for (name, value) in pairs {
                cmd.env(name, value);
            }
        }
        // Unisolated: inherit, and layer the task's env on top.
        None => {
            for e in env {
                cmd.env(&e.name, &e.value);
            }
        }
    }
    Ok(cmd)
}

/// Spawns `command[0]` with `command[1..]` as args.
/// `timeout_secs` caps execution; falls back to [`DEFAULT_TASK_TIMEOUT_SECS`]
/// (inside the 30 s lease) and is clamped by [`effective_timeout_secs`].
/// `env` is layered on top of the inherited environment — or replaces it, where
/// the pool isolates ([`crate::pool`]).
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
    let mut cmd = build_command(command, env)?;

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
    let mut cmd = build_command(command, env)?;
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

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

    fn ident() -> TaskIdentity {
        TaskIdentity {
            task_id: "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e".into(),
            run_id: "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f".into(),
            attempt: 3,
            installation: None,
        }
    }

    /// dagron's own ids are hyphenated v4 UUIDs, and they must pass unchanged —
    /// the whole reaper rests on the label round-tripping the task id exactly.
    #[test]
    fn a_task_uuid_is_a_valid_label_value() {
        let id = "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e";
        assert_eq!(label_value(id).as_deref(), Some(id));
    }

    /// Validate, never sanitise.
    ///
    /// A sanitiser that strips offending characters can map two different task
    /// ids onto one label, and a collision here means one task's reaper
    /// deleting another task's live pod. Refusing is the safe direction: the
    /// workload goes unlabelled, which is exactly the behaviour that shipped
    /// before this existed.
    #[test]
    fn an_unlabellable_id_is_refused_rather_than_mangled() {
        assert_eq!(label_value(""), None, "empty");
        assert_eq!(label_value(&"a".repeat(64)), None, "over the 63-byte k8s cap");
        assert_eq!(label_value("-leading"), None, "must start alphanumeric");
        assert_eq!(label_value("trailing-"), None, "must end alphanumeric");
        assert_eq!(label_value("has spaces"), None);
        assert_eq!(label_value("has/slash"), None);
        assert_eq!(label_value("has:colon"), None);
        // Exactly at the cap is fine — the boundary is inclusive.
        assert!(label_value(&"a".repeat(63)).is_some());
        // The inner set k8s allows.
        assert!(label_value("a-b_c.d9").is_some());
    }

    #[test]
    fn labels_carry_the_whole_identity() {
        let l = ident().labels();
        assert_eq!(l.get(LABEL_MANAGED_BY).map(String::as_str), Some(MANAGED_BY));
        assert_eq!(l.get(LABEL_TASK_ID).map(String::as_str), Some("0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"));
        assert_eq!(l.get(LABEL_RUN_ID).map(String::as_str), Some("9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f"));
        assert_eq!(l.get(LABEL_ATTEMPT).map(String::as_str), Some("3"), "attempt is the discriminator");
    }

    /// The fence, in one predicate. Only a strictly lower attempt is a
    /// predecessor — every other answer here is a way to delete live work.
    #[test]
    fn only_a_strictly_lower_attempt_is_reapable() {
        assert!(is_stale_attempt(Some("1"), 2), "an earlier attempt is a predecessor");
        assert!(is_stale_attempt(Some("1"), 9));
    }

    /// The bug this predicate replaced: the old check skipped only the EQUAL
    /// attempt, so attempt 1 waking up after its lease expired would delete
    /// attempt 2's live pod and then start duplicate work.
    #[test]
    fn a_newer_attempt_is_never_reaped_by_an_older_one() {
        assert!(!is_stale_attempt(Some("2"), 1), "attempt 2 is probably running right now");
        assert!(!is_stale_attempt(Some("2"), 2), "and this one is our own");
    }

    /// A workload that cannot prove it is stale is left alone. An orphan that
    /// survives is a job for a fleet-wide sweep; a live pod deleted on a guess
    /// is work already lost.
    #[test]
    fn an_unprovable_attempt_is_left_alone() {
        assert!(!is_stale_attempt(None, 5), "unlabelled");
        assert!(!is_stale_attempt(Some(""), 5), "empty");
        assert!(!is_stale_attempt(Some("one"), 5), "not a number");
        assert!(!is_stale_attempt(Some("3x"), 5), "not entirely a number");
        assert!(!is_stale_attempt(Some("99999999999999999999"), 5), "overflows i64");
    }

    /// Lexical comparison would call "10" stale against 9. It is not.
    #[test]
    fn the_comparison_is_numeric_not_lexical() {
        assert!(!is_stale_attempt(Some("10"), 9), "10 > 9, whatever string order says");
        assert!(is_stale_attempt(Some("9"), 10));
    }

    /// All-or-nothing, and this is the load-bearing case.
    ///
    /// A workload labelled `managed-by=dagron` with no `task-id` is selected by
    /// the reaper's `managed-by` clause and matches no task — which is precisely
    /// the shape that gets live work deleted. So an identity that cannot be
    /// fully expressed contributes no labels at all.
    #[test]
    fn a_partial_identity_contributes_no_labels_at_all() {
        let bad = TaskIdentity {
            task_id: "has/slash".into(),
            run_id: "ok".into(),
            attempt: 1,
            installation: None,
        };
        assert!(bad.labels().is_empty(), "no managed-by without a task-id");
        assert!(bad.task_selector().is_none(), "and nothing to select on");
    }

    /// The selector is scoped twice over: to this project, and to this task.
    /// Either clause alone would over-select.
    #[test]
    fn the_selector_names_both_owner_and_task() {
        let sel = ident().task_selector().expect("a uuid selects");
        assert!(sel.contains(&format!("{LABEL_MANAGED_BY}={MANAGED_BY}")), "{sel}");
        assert!(sel.contains(&format!("{LABEL_TASK_ID}=0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e")), "{sel}");
        assert!(!sel.contains(LABEL_ATTEMPT), "attempt must NOT narrow it — the point is to find OTHER attempts");
    }

    /// A context with no task row behind it carries no identity, and that path
    /// must keep working: it is what tests and the no-op fallback use.
    #[test]
    fn a_context_without_a_task_row_has_no_identity() {
        let ctx = ExecContext::new(vec!["true".into()], None, None);
        assert!(ctx.identity.is_none());
    }

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
            isolation: None,
            log_sink: Some(sink),
            identity: None,
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
            isolation: None,
            log_sink: Some(sink),
            identity: None,
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
            isolation: None,
            log_sink: None,
            identity: None,
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
            isolation: None,
            log_sink: None,
            identity: None,
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
            isolation: None,
            log_sink: Some(sink),
            identity: None,
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

    /// Without an installation the workload is still fully labelled — just not
    /// claimable by a fleet sweep. That is the whole opt-in: the per-dispatch
    /// reap keeps working, and nothing sweeps on a scope nobody set.
    #[test]
    fn no_installation_still_labels_everything_else() {
        let l = ident().labels();
        assert!(!l.contains_key(LABEL_INSTALLATION), "nothing to scope a sweep by");
        assert_eq!(l.get(LABEL_MANAGED_BY).map(String::as_str), Some(MANAGED_BY));
        assert!(l.contains_key(LABEL_TASK_ID), "the per-task reap still works");
    }

    #[test]
    fn an_installation_is_carried_when_it_is_set() {
        let mut i = ident();
        i.installation = Some("prod-eu-1".into());
        assert_eq!(i.labels().get(LABEL_INSTALLATION).map(String::as_str), Some("prod-eu-1"));
    }

    /// An unusable installation contributes NO label rather than a truncated or
    /// sanitised one.
    ///
    /// A truncated value is the dangerous outcome: `prod-eu-1-very-long…` and
    /// `prod-eu-2-very-long…` can share the 63-character prefix a sanitiser
    /// would produce, and then one installation's sweep selects the other's
    /// pods — the exact cross-installation deletion the label exists to
    /// prevent. Contributing nothing leaves the workload unsweepable, which
    /// costs a leftover rather than live work.
    #[test]
    fn an_unusable_installation_contributes_no_label_rather_than_a_truncated_one() {
        for bad in ["has/slash", "has space", &"a".repeat(64), "", "-leading"] {
            let mut i = ident();
            i.installation = Some(bad.to_string());
            assert!(
                !i.labels().contains_key(LABEL_INSTALLATION),
                "{bad:?} must not become a label"
            );
            assert!(
                i.labels().contains_key(LABEL_TASK_ID),
                "{bad:?} must not cost the task label either"
            );
        }
    }

    /// The installation is not part of the per-task selector. That selector
    /// already narrows to one task id — a UUID — and adding the installation
    /// would make the per-dispatch reap silently stop working the moment an
    /// operator renamed their installation, leaving predecessors alive.
    #[test]
    fn the_per_task_selector_does_not_depend_on_the_installation() {
        let mut i = ident();
        i.installation = Some("prod-eu-1".into());
        let with = i.task_selector().expect("selector");
        i.installation = None;
        assert_eq!(with, i.task_selector().expect("selector"), "same either way");
        assert!(!with.contains("installation"));
    }

    fn labels_of(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn a_well_labelled_workload_becomes_a_candidate() {
        let l = labels_of(&[
            (LABEL_MANAGED_BY, MANAGED_BY),
            (LABEL_TASK_ID, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"),
            (LABEL_RUN_ID, "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f"),
            (LABEL_ATTEMPT, "2"),
        ]);
        let w = ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).expect("candidate");
        assert_eq!(w.task_id, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e");
        assert_eq!(w.attempt.as_deref(), Some("2"));
    }

    /// The reason this validates rather than merely checking presence. A label
    /// SELECTOR constrains only the labels it names and says nothing about the
    /// rest — so a workload can match `managed-by` and `installation` exactly
    /// while carrying a task id that is empty or not a usable label value.
    ///
    /// Such a workload matches no live row, because no row ever had that id, and
    /// the sweep acts on absence — so without this it would be DELETED, which
    /// inverts the rule the rest of this module rests on.
    #[test]
    fn a_task_id_that_could_never_name_a_row_is_refused() {
        for bad in ["", "has/slash", "has space", &"a".repeat(64), "-leading"] {
            let l = labels_of(&[(LABEL_MANAGED_BY, MANAGED_BY), (LABEL_TASK_ID, bad)]);
            assert!(
                ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).is_none(),
                "{bad:?} must not become a deletion candidate"
            );
        }
    }

    #[test]
    fn a_workload_with_no_task_id_at_all_is_refused() {
        let l = labels_of(&[(LABEL_MANAGED_BY, MANAGED_BY), (LABEL_ATTEMPT, "1")]);
        assert!(ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).is_none());
    }

    /// `run_id` and `attempt` are for the log line, and no decision reads them,
    /// so a missing or odd value must not cost a real orphan its cleanup.
    #[test]
    fn the_informational_labels_are_not_validated() {
        let l = labels_of(&[
            (LABEL_TASK_ID, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"),
            (LABEL_RUN_ID, "not/a/label/value"),
        ]);
        let w = ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k)).expect("candidate");
        assert_eq!(w.run_id.as_deref(), Some("not/a/label/value"));
        assert_eq!(w.attempt, None);
    }

    /// The pair of rules read together: what a sweep may delete is a workload
    /// whose task id is usable AND whose attempt, where present, is provably
    /// older. Neither rule covers for the other.
    #[test]
    fn identity_round_trips_from_labels_back_into_a_candidate() {
        let mut i = ident();
        i.installation = Some("prod-eu-1".into());
        let l = i.labels();
        let w = ManagedWorkload::from_labels("pod-x".into(), Some("u-1".into()), |k| l.get(k))
            .expect("what dagron writes, the sweep can read back");
        assert_eq!(w.task_id, i.task_id);
        assert_eq!(w.attempt.as_deref(), Some("3"));
    }

    /// The uid is the backend's, not a label, and it rides through untouched —
    /// including its absence, which is normal for a backend whose handle is
    /// already an identity.
    #[test]
    fn the_backends_own_identity_rides_through_unread() {
        let l = labels_of(&[(LABEL_TASK_ID, "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e")]);
        let with = ManagedWorkload::from_labels("pod-x".into(), Some("u-9".into()), |k| l.get(k))
            .expect("candidate");
        assert_eq!(with.uid.as_deref(), Some("u-9"));

        let without =
            ManagedWorkload::from_labels("c0ffee".into(), None, |k| l.get(k)).expect("candidate");
        assert_eq!(without.uid, None);
    }
}
