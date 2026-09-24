use anyhow::Result;
use async_trait::async_trait;
use bollard::{
    container::{
        Config, CreateContainerOptions, ListContainersOptions, LogOutput, LogsOptions,
        RemoveContainerOptions, WaitContainerOptions,
    },
    models::HostConfig,
    Docker,
};
use futures_util::{StreamExt, TryStreamExt};
use tokio::time::{timeout, Duration};

/// How long best-effort container cleanup may take before it is abandoned.
/// Matches `KubeExecutor::cleanup`, which bounds pod deletion the same way.
const CLEANUP_TIMEOUT_SECS: u64 = 10;

/// One budget for the WHOLE predecessor reap, list and removals together.
///
/// Per-removal timeouts do not compose: they bound each call and therefore add
/// up, so N stale containers against a wedged dockerd delay `create_container`
/// by N × [`CLEANUP_TIMEOUT_SECS`] — an unbounded dispatch delay that grows
/// with exactly the mess the reap exists to clear. `ctx.timeout_secs` does not
/// cover this; it starts later, at the wait and log collection.
///
/// When the budget expires, dispatch proceeds. Cleanup is best-effort by
/// design: refusing to start because a leftover would not die turns a cleanup
/// failure into an outage.
const REAP_BUDGET_SECS: u64 = 20;
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
    /// Every container on this daemon carrying this installation's labels and
    /// old enough to judge.
    ///
    /// `all: true` on purpose — an exited container still holds its writable
    /// layer, so a leftover that stopped on its own is still disk this sweep
    /// should reclaim. Scoped on installation as well as `managed-by`, so a
    /// second dagron on the same daemon is invisible here rather than being
    /// mistaken for an orphan.
    async fn list_orphan_candidates(
        &self,
        scope: &crate::executor::OrphanScope<'_>,
    ) -> Result<Vec<crate::executor::ManagedWorkload>> {
        use crate::executor::{LABEL_INSTALLATION, LABEL_MANAGED_BY, MANAGED_BY};
        let Some(installation) = crate::executor::label_value(scope.installation) else {
            anyhow::bail!(
                "DAGRON_INSTALLATION={:?} is not usable as a label value, so the fleet sweep \
                 cannot scope itself and will not run. Use 1-63 alphanumerics, '-', '_' or '.', \
                 starting and ending alphanumeric.",
                scope.installation
            );
        };
        let mut filters = std::collections::HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![
                format!("{LABEL_MANAGED_BY}={MANAGED_BY}"),
                format!("{LABEL_INSTALLATION}={installation}"),
            ],
        );
        let list = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            })),
        )
        .await
        .map_err(|_| anyhow::anyhow!("listing containers for the fleet sweep timed out"))??;

        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().saturating_sub(scope.min_age.as_secs()) as i64)
            .unwrap_or(0);

        let mut out = Vec::new();
        for c in list {
            let Some(id) = c.id.clone() else { continue };
            // Young containers are not judged — see `OrphanScope::min_age`. A
            // missing creation time counts as young, because the alternative is
            // judging a container whose age is unknown.
            if c.created.is_none_or(|born| born > cutoff) {
                continue;
            }
            let labels = c.labels.unwrap_or_default();
            // Refused here, not filtered: the selector constrains only the
            // labels it names and says nothing about the rest — see
            // `from_labels`. No uid: a container id is already an identity
            // rather than a name for one, so deletion stays id-based.
            let Some(w) = crate::executor::ManagedWorkload::from_labels(id, None, |k| labels.get(k))
            else {
                continue;
            };
            out.push(w);
        }
        Ok(out)
    }

    /// Remove one container the sweep judged leftover.
    ///
    /// By **id**, taken from the listing, so the container removed is the one
    /// judged rather than whatever a name might resolve to now.
    ///
    /// `force: true` because a leftover is as likely to be still running as
    /// exited — the scheduler that would have stopped it is the one that died
    /// — and because removal is what reclaims the writable layer, which is the
    /// disk this sweep exists to give back.
    async fn delete_workload(&self, w: &crate::executor::ManagedWorkload) -> Result<()> {
        timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.remove_container(
                &w.handle,
                Some(RemoveContainerOptions { force: true, ..Default::default() }),
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("removing container '{}' timed out", w.handle))??;
        Ok(())
    }

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
        // Identity labels, so this container can be found by something other
        // than the process that created it — see `TaskIdentity`. An identity
        // that will not express as labels contributes none, rather than a
        // partial set a reaper could match on and misread.
        let labels = ctx.identity.as_ref().map(|i| i.labels()).filter(|l| !l.is_empty());

        let config = Config::<String> {
            image: Some(image.to_string()),
            cmd: Some(ctx.command.clone()),
            env: if env.is_empty() { None } else { Some(env) },
            labels: labels.map(|l| l.into_iter().collect()),
            host_config: Some(HostConfig {
                auto_remove: Some(false), // we remove manually to capture logs first
                ..Default::default()
            }),
            ..Default::default()
        };
        // Before creating: kill anything an earlier attempt of this same task
        // left running. Without this the lease bounds the row, not the work.
        self.reap_predecessors(ctx).await;

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
    /// Remove any container this project owns for the same task from an
    /// **earlier attempt**, before starting this one.
    ///
    /// The counterpart to `KubeExecutor::reap_predecessors`, and it exists for
    /// the same reason: a lease bounds which scheduler owns the task *row*, and
    /// bounded nothing about the container. When a lease expired mid-run the
    /// reclaiming scheduler started a second container while the first was
    /// still executing the same command.
    ///
    /// Best-effort — a daemon hiccup must not block dispatch, since refusing to
    /// run would turn a cleanup failure into an outage, and the state it leaves
    /// is no worse than the behaviour that preceded this method.
    async fn reap_predecessors(&self, ctx: &ExecContext) {
        let Some(id) = &ctx.identity else { return };
        let Some(task) = crate::executor::label_value(&id.task_id) else { return };

        let mut filters = std::collections::HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![
                format!("{}={}", crate::executor::LABEL_MANAGED_BY, crate::executor::MANAGED_BY),
                format!("{}={}", crate::executor::LABEL_TASK_ID, task),
            ],
        );
        // The whole reap runs inside one deadline, so a wedged daemon costs a
        // bounded dispatch delay rather than one per leftover.
        let budget = tokio::time::Instant::now() + Duration::from_secs(REAP_BUDGET_SECS);
        let listed = tokio::time::timeout_at(
            budget,
            self.docker.list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            })),
        )
        .await;
        let found = match listed {
            Ok(Ok(list)) => list,
            Ok(Err(e)) => {
                tracing::warn!(task_id = %id.task_id, error = %e, "could not list prior task containers — dispatching anyway");
                return;
            }
            Err(_) => {
                tracing::warn!(task_id = %id.task_id, "listing prior task containers timed out — dispatching anyway");
                return;
            }
        };
        for c in found {
            // Strictly lower only — see `is_stale_attempt`. A higher attempt is
            // a newer dispatch that is very likely alive, and this one may be a
            // stalled predecessor that only just woke up.
            let attempt = c
                .labels
                .as_ref()
                .and_then(|l| l.get(crate::executor::LABEL_ATTEMPT))
                .map(String::as_str);
            if !crate::executor::is_stale_attempt(attempt, id.attempt) {
                continue;
            }
            let Some(cid) = c.id.as_deref() else { continue };
            tracing::warn!(
                task_id = %id.task_id, container = %cid,
                stale_attempt = attempt.unwrap_or("<unlabelled>"), current_attempt = %id.attempt,
                "reaping a container left by an earlier attempt of this task — it was still running the same command"
            );
            if tokio::time::timeout_at(budget, self.force_remove(cid)).await.is_err() {
                tracing::warn!(
                    task_id = %id.task_id,
                    "predecessor cleanup budget expired with leftover containers still \
                     present — dispatching anyway, because refusing here would turn a \
                     cleanup failure into an outage"
                );
                return;
            }
        }
    }

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
