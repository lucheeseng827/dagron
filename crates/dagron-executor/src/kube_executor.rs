//! Kubernetes pod executor (v3) — `EXECUTOR=kubernetes`, `--features kubernetes`.
//!
//! The third `Executor` backend, alongside `LocalExecutor` (subprocess) and
//! `DockerExecutor` (container): each task runs as a one-shot **Pod**. The pod is
//! created with `restartPolicy: Never`, polled to a terminal phase under a hard
//! timeout, its logs are captured, and it is deleted whether it succeeded or
//! timed out — the same create / wait-with-timeout / collect-logs / cleanup shape
//! as `DockerExecutor`, just against the Kubernetes API instead of a Docker
//! socket. Swapping to it touches nothing in the reconcile loop (the `Executor`
//! trait is the seam); the `docker_image` field on `ExecContext` doubles as the
//! per-task container image override here.
//!
//! **Cluster-gated.** `kube`/`k8s-openapi` are a pure-Rust client, so this module
//! *compiles* without a cluster; it only *runs* against a live apiserver
//! (in-cluster service account or local kubeconfig, via `Client::try_default`).
//! The whole module is behind the `kubernetes` Cargo feature so a default build
//! carries none of it.

use anyhow::{bail, Result};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, LogParams, PostParams};
use kube::Client;
use tokio::time::{sleep, timeout, Duration};
use uuid::Uuid;

use crate::executor::{ExecContext, ExecOutput, Executor};

/// How often to poll a pod's phase while waiting for it to finish.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Kubernetes executor — each task runs in a freshly created one-shot pod.
pub struct KubeExecutor {
    /// Default image when a task does not specify `docker_image`.
    pub default_image: String,
    /// Namespace the task pods are created in.
    namespace: String,
    client: Client,
}

impl KubeExecutor {
    /// Build a client from the ambient config (in-cluster service account or
    /// local kubeconfig) and verify the apiserver + namespace are reachable.
    pub async fn connect(
        default_image: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .map_err(|e| anyhow::anyhow!("kube client init: {e}"))?;
        let namespace = namespace.into();

        // Reachability probe — analogous to DockerExecutor::connect's ping.
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        pods.list(&ListParams::default().limit(1)).await.map_err(|e| {
            anyhow::anyhow!("kube apiserver unreachable or namespace '{namespace}' inaccessible: {e}")
        })?;

        Ok(Self { default_image: default_image.into(), namespace, client })
    }

    /// Best-effort pod deletion (cleanup); errors and a stalled request are
    /// ignored so cleanup can never hang `execute()`.
    async fn cleanup(pods: &Api<Pod>, name: &str) {
        let _ = timeout(Duration::from_secs(10), pods.delete(name, &DeleteParams::default())).await;
    }
}

#[async_trait]
impl Executor for KubeExecutor {
    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        if ctx.command.is_empty() {
            bail!("empty command");
        }
        let secs = ctx.timeout_secs.unwrap_or(25);
        let image = ctx.docker_image.as_deref().unwrap_or(&self.default_image);
        // Short unique pod name (DNS-1123 label: lowercase alphanumeric + '-').
        let name = format!("sched-{}", Uuid::new_v4().simple());
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

        let pod = build_pod(&name, image, &ctx.command, ctx)?;
        pods.create(&PostParams::default(), &pod)
            .await
            .map_err(|e| anyhow::anyhow!("create pod '{name}': {e}"))?;

        // ── Wait for a terminal phase, bounded by the task timeout ───────────
        let waited = timeout(Duration::from_secs(secs), async {
            loop {
                let pod = pods
                    .get(&name)
                    .await
                    .map_err(|e| anyhow::anyhow!("poll pod '{name}': {e}"))?;
                if let Some(phase) = pod.status.and_then(|s| s.phase) {
                    if phase == "Succeeded" || phase == "Failed" {
                        return Ok::<String, anyhow::Error>(phase);
                    }
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await;

        let phase = match waited {
            Ok(Ok(phase)) => phase,
            Ok(Err(e)) => {
                Self::cleanup(&pods, &name).await;
                return Err(e);
            }
            Err(_) => {
                Self::cleanup(&pods, &name).await;
                bail!("pod '{name}' timed out after {secs}s");
            }
        };

        // ── Collect logs, then delete the pod ───────────────────────────────
        // kube's `logs()`/`LogParams` carry no request timeout (it's a transport
        // setting), so bound it with the task timeout to keep `execute()` from
        // hanging on a stalled read.
        let output = match timeout(Duration::from_secs(secs), pods.logs(&name, &LogParams::default()))
            .await
        {
            Ok(Ok(logs)) => logs,
            Ok(Err(e)) => {
                tracing::warn!(pod = %name, error = %e, "failed to read pod logs");
                String::new()
            }
            Err(_) => {
                tracing::warn!(pod = %name, "pod log read timed out");
                String::new()
            }
        };
        Self::cleanup(&pods, &name).await;

        Ok(ExecOutput { success: phase == "Succeeded", output })
    }
}

/// Build the one-shot task Pod manifest. Split out (and free of any client) so it
/// is unit-testable without a cluster.
///
/// `ctx` supplies the per-task knobs that make load-test pods realistic:
/// * `env` → container env vars (the parameterised ETL image reads these);
/// * `resources` → container `resources.requests/limits` so the k8s scheduler
///   packs/evicts/OOMKills pods like production;
/// * `service_account` → the IRSA seam, so the pod assumes an IAM role for S3.
fn build_pod(name: &str, image: &str, command: &[String], ctx: &ExecContext) -> Result<Pod> {
    let env: Vec<serde_json::Value> = ctx
        .env
        .iter()
        .map(|e| serde_json::json!({ "name": e.name, "value": e.value }))
        .collect();

    let resources = ctx.resources.as_ref().map(|r| {
        serde_json::json!({
            "requests": r.requests,
            "limits": r.limits,
        })
    });

    let mut container = serde_json::json!({
        "name": "task",
        "image": image,
        "command": command,
    });
    if !env.is_empty() {
        container["env"] = serde_json::Value::Array(env);
    }
    if let Some(resources) = resources {
        container["resources"] = resources;
    }

    let mut spec = serde_json::json!({
        "restartPolicy": "Never",
        "containers": [container],
    });
    if let Some(sa) = ctx.service_account.as_deref() {
        spec["serviceAccountName"] = serde_json::Value::String(sa.to_string());
    }

    let pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": { "app": "module-54-scheduler" },
        },
        "spec": spec,
    }))
    .map_err(|e| anyhow::anyhow!("build pod manifest: {e}"))?;
    Ok(pod)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manifest builder produces a valid one-shot Pod with the task's image
    /// and command — verifiable without a cluster.
    #[test]
    fn build_pod_sets_image_command_and_restart_policy() {
        let ctx = ExecContext::new(
            vec!["echo".to_string(), "hello".to_string()],
            None,
            Some("alpine:3.19".to_string()),
        );
        let pod = build_pod("sched-abc", "alpine:3.19", &ctx.command, &ctx).unwrap();

        let spec = pod.spec.expect("spec");
        assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
        let c = &spec.containers[0];
        assert_eq!(c.image.as_deref(), Some("alpine:3.19"));
        assert_eq!(c.command.as_ref().unwrap(), &vec!["echo".to_string(), "hello".to_string()]);
        assert_eq!(pod.metadata.name.as_deref(), Some("sched-abc"));
        // No per-task knobs set → no env, no resources, no service account.
        assert!(c.env.is_none());
        assert!(c.resources.is_none());
        assert!(spec.service_account_name.is_none());
    }

    /// Per-task env, resources, and the IRSA service account land on the pod —
    /// the knobs that make load-test task pods behave like production.
    #[test]
    fn build_pod_applies_env_resources_and_service_account() {
        use dagron_core::dag::{EnvVar, ResourceRequirements};
        use std::collections::BTreeMap;

        let mut requests = BTreeMap::new();
        requests.insert("cpu".to_string(), "250m".to_string());
        requests.insert("memory".to_string(), "256Mi".to_string());
        let mut limits = BTreeMap::new();
        limits.insert("memory".to_string(), "512Mi".to_string());

        let ctx = ExecContext {
            command: vec!["python".to_string(), "/app/etl.py".to_string()],
            timeout_secs: Some(120),
            docker_image: Some("etl-task:latest".to_string()),
            env: vec![EnvVar { name: "S3_BUCKET".into(), value: "dagron-lt".into() }],
            resources: Some(ResourceRequirements { requests, limits }),
            service_account: Some("dagron-etl".to_string()),
        };
        let pod = build_pod("sched-xyz", "etl-task:latest", &ctx.command, &ctx).unwrap();

        let spec = pod.spec.expect("spec");
        assert_eq!(spec.service_account_name.as_deref(), Some("dagron-etl"));
        let c = &spec.containers[0];

        let env = c.env.as_ref().expect("env");
        assert_eq!(env[0].name, "S3_BUCKET");
        assert_eq!(env[0].value.as_deref(), Some("dagron-lt"));

        let res = c.resources.as_ref().expect("resources");
        let reqs = res.requests.as_ref().expect("requests");
        assert_eq!(reqs["cpu"].0, "250m");
        assert_eq!(reqs["memory"].0, "256Mi");
        let lims = res.limits.as_ref().expect("limits");
        assert_eq!(lims["memory"].0, "512Mi");
    }
}
