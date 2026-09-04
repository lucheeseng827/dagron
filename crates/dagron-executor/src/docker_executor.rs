use anyhow::Result;
use async_trait::async_trait;
use bollard::{
    container::{
        Config, CreateContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions,
        WaitContainerOptions,
    },
    models::HostConfig,
    Docker,
};
use futures_util::{StreamExt, TryStreamExt};
use tokio::time::{timeout, Duration};

/// How long best-effort container cleanup may take before it is abandoned.
/// Matches `KubeExecutor::cleanup`, which bounds pod deletion the same way.
const CLEANUP_TIMEOUT_SECS: u64 = 10;
use uuid::Uuid;

use crate::executor::{ExecContext, ExecOutput, Executor};

/// Docker executor — each task runs in a freshly created container.
///
/// The container is started, waited on (with a hard timeout), logs are captured,
/// and the container is force-removed whether the task succeeds or times out.
/// This is the "spawnable worker in Docker container form" backend.
pub struct DockerExecutor {
    /// Default image when a task does not specify `docker_image`.
    pub default_image: String,
    docker: Docker,
}

impl DockerExecutor {
    /// Connect to the local Docker daemon and verify reachability.
    pub async fn connect(default_image: impl Into<String>) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| anyhow::anyhow!("Docker connect: {e}"))?;
        docker
            .ping()
            .await
            .map_err(|e| anyhow::anyhow!("Docker daemon unreachable: {e}"))?;
        Ok(Self { default_image: default_image.into(), docker })
    }
}

#[async_trait]
impl Executor for DockerExecutor {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        if ctx.command.is_empty() {
            anyhow::bail!("empty command");
        }
        let secs = crate::executor::effective_timeout_secs(ctx.timeout_secs);
        let image = ctx.docker_image.as_deref().unwrap_or(&self.default_image);
        // Short unique name — stays well within Docker's 64-char limit.
        let name = format!("sched-{}", Uuid::new_v4().simple());

        // ── Create container ────────────────────────────────────────────────
        let env: Vec<String> = ctx
            .env
            .iter()
            .map(|e| format!("{}={}", e.name, e.value))
            .collect();
        let config = Config::<String> {
            image: Some(image.to_string()),
            cmd: Some(ctx.command.clone()),
            env: if env.is_empty() { None } else { Some(env) },
            host_config: Some(HostConfig {
                auto_remove: Some(false), // we remove manually to capture logs first
                ..Default::default()
            }),
            ..Default::default()
        };
        self.docker
            .create_container(
                Some(CreateContainerOptions { name: name.as_str(), platform: None }),
                config,
            )
            .await?;

        // ── Start ───────────────────────────────────────────────────────────
        self.docker.start_container::<String>(&name, None).await?;

        // ── Wait (with hard timeout) ─────────────────────────────────────────
        let wait_result = timeout(Duration::from_secs(secs), async {
            self.docker
                .wait_container::<String>(&name, None::<WaitContainerOptions<String>>)
                .try_next()
                .await
        })
        .await;

        let exit_code: i64 = match wait_result {
            Ok(Ok(Some(response))) => response.status_code,
            Ok(Ok(None)) => {
                self.force_remove(&name).await;
                anyhow::bail!("container '{}' wait stream ended unexpectedly", name);
            }
            Ok(Err(e)) => {
                self.force_remove(&name).await;
                return Err(e.into());
            }
            Err(_) => {
                // Timed out — force-remove stops and deletes in one call. Return
                // the typed TimeoutError so the worker can gate retry-on-timeout (#24).
                self.force_remove(&name).await;
                return Err(anyhow::Error::new(crate::executor::TimeoutError { secs }));
            }
        };

        // ── Collect logs (container already stopped) ─────────────────────────
        // Bounded like `KubeExecutor`'s log read, for the same reason it is: this
        // is reached only once `wait_container` returned an exit code, so the
        // container has stopped and a stall here costs no task compute — it costs
        // the worker, which would sit in `logs.next()` forever and never reach the
        // `force_remove` below, leaking the stopped container with it.
        //
        // Whatever arrived before the deadline is kept. The exit code is already
        // known here, so truncated output beats never returning.
        let mut log_output = String::new();
        let mut logs = self.docker.logs::<String>(
            &name,
            Some(LogsOptions { stdout: true, stderr: true, ..Default::default() }),
        );
        let collected = timeout(Duration::from_secs(secs), async {
            while let Some(item) = logs.next().await {
                match item {
                    Ok(LogOutput::StdOut { message }) => {
                        let line = String::from_utf8_lossy(&message);
                        // Forward to the live-log tail (#17) if wired. The container has
                        // already stopped here, so for Docker this surfaces the captured
                        // output through the same append path rather than mid-run — true
                        // `follow: true` streaming is a documented follow-up; Local
                        // streams live.
                        if let Some(sink) = &ctx.log_sink {
                            sink.append(&line);
                        }
                        log_output.push_str(&line);
                    }
                    Ok(LogOutput::StdErr { message }) => {
                        let stderr_line = String::from_utf8_lossy(&message);
                        tracing::warn!(
                            container = %name,
                            stderr = %stderr_line.trim(),
                            "container stderr"
                        );
                        log_output.push_str(&stderr_line);
                    }
                    _ => {}
                }
            }
        })
        .await;
        if collected.is_err() {
            tracing::warn!(container = %name, secs, "log collection timed out; output may be truncated");
        }

        self.force_remove(&name).await;
        Ok(ExecOutput { success: exit_code == 0, output: log_output })
    }
}

impl DockerExecutor {
    /// Force-remove the named container, ignoring errors (best-effort cleanup).
    ///
    /// Bounded, for the reason `KubeExecutor::cleanup` is bounded: three of the
    /// four call sites run *after* `execute()` has already given up on the task,
    /// the timeout path among them. An unresponsive daemon here would hold the
    /// worker past the very ceiling that just expired — so the wall clock a task
    /// is promised would be `secs` plus however long the daemon takes to answer.
    ///
    /// A container left behind by a stalled remove is the smaller problem, and
    /// not a new one: this is best-effort either way, and the discarded `Result`
    /// already meant a failed remove leaks the container.
    async fn force_remove(&self, name: &str) {
        let _ = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.remove_container(
                name,
                Some(RemoveContainerOptions { force: true, ..Default::default() }),
            ),
        )
        .await;
    }
}
