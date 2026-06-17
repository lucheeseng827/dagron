use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

// ── Shared types ─────────────────────────────────────────────────────────────

/// Output returned by every executor backend.
pub struct ExecOutput {
    pub success: bool,
    pub output: String,
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
        let (code, output) = run_command(&ctx.command, ctx.timeout_secs, &ctx.env).await?;
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
        tracing::warn!(stderr = %stderr.trim(), "subprocess stderr");
    }
    Ok((exit_code, stdout))
}
