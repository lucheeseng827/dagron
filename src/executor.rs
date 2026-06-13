// SPDX-License-Identifier: Apache-2.0
//! Pluggable execution backend.
//!
//! [`Executor`] abstracts *how a task runs*. The OSS distribution ships the
//! zero-infra [`LocalExecutor`] (subprocess) reference implementation. Container
//! and remote backends are provided by separate distributions but implement the
//! exact same trait, so anything written against [`Executor`] is portable.

use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

/// Output returned by every executor backend.
pub struct ExecOutput {
    pub success: bool,
    pub output: String,
}

/// All inputs an executor needs to run one task.
pub struct ExecContext {
    pub command: Vec<String>,
    pub timeout_secs: Option<u64>,
}

/// Pluggable execution backend. Implement this to run tasks on a new substrate
/// (containers, remote workers, …) without touching the runner.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput>;
}

/// Subprocess executor — the default, zero-infra backend.
pub struct LocalExecutor;

#[async_trait]
impl Executor for LocalExecutor {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        let (code, output) = run_command(&ctx.command, ctx.timeout_secs).await?;
        Ok(ExecOutput {
            success: code == 0,
            output,
        })
    }
}

/// Spawns `command[0]` with `command[1..]` as args.
/// `timeout_secs` caps execution; falls back to 25 s. `kill_on_drop` ensures the
/// child is reaped if the future is dropped.
pub async fn run_command(command: &[String], timeout_secs: Option<u64>) -> Result<(i32, String)> {
    if command.is_empty() {
        bail!("empty command");
    }
    let secs = timeout_secs.unwrap_or(25);
    if secs == 0 {
        bail!("timeout_secs must be >= 1 when provided");
    }
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..]).kill_on_drop(true);

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
