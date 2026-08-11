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
                // Typed TimeoutError so the worker can gate retry-on-timeout (#24).
                return Err(anyhow::Error::new(crate::executor::TimeoutError { secs }));
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


/// How task pods are hardened, read once from the environment.
///
/// WHY THIS EXISTS: `build_pod` used to emit a Pod carrying only image, command,
/// env, resources and an optional serviceAccountName — no securityContext, no
/// runtimeClassName, no `automountServiceAccountToken`, no
/// `activeDeadlineSeconds`, no node placement. Meanwhile the customer-image
/// contract (`ee/images/runners/README.md`) told customers that "task pods run
/// with the engine's pod security context". They did not: that context is on the
/// engine's own Deployment and is not inherited by pods the engine creates, so a
/// customer image whose final layer is `USER root` ran as root.
///
/// A hosted plane can enforce this at admission (Kyverno). A self-hosted install
/// has no admission controller, which is exactly where the contract was least
/// true and most believed — so the engine has to be able to do it itself.
///
/// DEFAULTS PRESERVE TODAY'S BEHAVIOUR, with one exception noted on
/// `automount_sa_token`. Turning hardening on by default would break images that
/// legitimately need root or extra capabilities, and silently changing what a
/// working task is allowed to do is not a thing to do in a patch release. The
/// hosted plane sets these explicitly; self-hosted operators opt in.
#[derive(Debug, Clone, Default)]
pub struct PodHardening {
    /// `DAGRON_TASK_RUN_AS_USER` — numeric uid; also sets `runAsNonRoot` when > 0.
    pub run_as_user: Option<i64>,
    /// `DAGRON_TASK_READ_ONLY_ROOT_FS=1`
    pub read_only_root_fs: bool,
    /// `DAGRON_TASK_DROP_ALL_CAPABILITIES=1`
    pub drop_all_capabilities: bool,
    /// `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT=1`
    pub seccomp_runtime_default: bool,
    /// `DAGRON_TASK_ACTIVE_DEADLINE_SECS` — a task that hangs otherwise holds a
    /// node slot until the run-level timeout notices.
    pub active_deadline_secs: Option<i64>,
    /// `DAGRON_TASK_RUNTIME_CLASS` — e.g. `gvisor`, for untrusted images.
    pub runtime_class: Option<String>,
    /// `DAGRON_TASK_NODE_SELECTOR` — `k=v,k=v`.
    pub node_selector: Vec<(String, String)>,
    /// Mount a ServiceAccount token into task pods that did NOT ask for one.
    ///
    /// This is the single default that CHANGES: it is `false`, so a task with no
    /// `service_account:` gets no token. A task that declared no identity has no
    /// use for one, and on a cluster using IRSA that token is an IAM credential
    /// handed to arbitrary customer code. Set
    /// `DAGRON_TASK_AUTOMOUNT_SA_TOKEN=1` to restore the old behaviour.
    pub automount_sa_token: bool,
}

impl PodHardening {
    pub fn from_env() -> Self {
        fn flag(k: &str) -> bool {
            matches!(std::env::var(k).ok().as_deref(), Some("1" | "true" | "yes" | "on"))
        }
        Self {
            run_as_user: std::env::var("DAGRON_TASK_RUN_AS_USER").ok().and_then(|v| v.parse().ok()),
            read_only_root_fs: flag("DAGRON_TASK_READ_ONLY_ROOT_FS"),
            drop_all_capabilities: flag("DAGRON_TASK_DROP_ALL_CAPABILITIES"),
            seccomp_runtime_default: flag("DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT"),
            active_deadline_secs: std::env::var("DAGRON_TASK_ACTIVE_DEADLINE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&n: &i64| n > 0),
            runtime_class: std::env::var("DAGRON_TASK_RUNTIME_CLASS").ok().filter(|v| !v.is_empty()),
            node_selector: std::env::var("DAGRON_TASK_NODE_SELECTOR")
                .ok()
                .map(|v| {
                    v.split(',')
                        .filter_map(|kv| kv.split_once('='))
                        .map(|(k, val)| (k.trim().to_string(), val.trim().to_string()))
                        .filter(|(k, _)| !k.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            automount_sa_token: flag("DAGRON_TASK_AUTOMOUNT_SA_TOKEN"),
        }
    }
}

/// Apply hardening to a built Pod manifest.
///
/// Pure and `Value`-shaped, mirroring `ee/dagron-executor-ee`'s confidential
/// path so both shape the same manifest through the same kind of seam rather
/// than two divergent ones.
pub fn apply_hardening(pod: &mut serde_json::Value, h: &PodHardening, wants_sa: bool) {
    let Some(spec) = pod.get_mut("spec").and_then(serde_json::Value::as_object_mut) else {
        return;
    };

    if let Some(uid) = h.run_as_user {
        let mut sc = serde_json::json!({ "runAsUser": uid });
        if uid > 0 {
            sc["runAsNonRoot"] = serde_json::json!(true);
        }
        if h.seccomp_runtime_default {
            sc["seccompProfile"] = serde_json::json!({ "type": "RuntimeDefault" });
        }
        spec.insert("securityContext".to_string(), sc);
    } else if h.seccomp_runtime_default {
        spec.insert(
            "securityContext".to_string(),
            serde_json::json!({ "seccompProfile": { "type": "RuntimeDefault" } }),
        );
    }

    if let Some(secs) = h.active_deadline_secs {
        spec.insert("activeDeadlineSeconds".to_string(), serde_json::json!(secs));
    }
    if let Some(rc) = &h.runtime_class {
        spec.insert("runtimeClassName".to_string(), serde_json::json!(rc));
    }
    if !h.node_selector.is_empty() {
        let m: serde_json::Map<String, serde_json::Value> = h
            .node_selector
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v)))
            .collect();
        spec.insert("nodeSelector".to_string(), serde_json::Value::Object(m));
    }
    // Only withhold the token from tasks that never asked for an identity;
    // a task with `service_account:` wants IRSA and must keep its token.
    if !h.automount_sa_token && !wants_sa {
        spec.insert("automountServiceAccountToken".to_string(), serde_json::json!(false));
    }

    if h.read_only_root_fs || h.drop_all_capabilities {
        if let Some(containers) = spec.get_mut("containers").and_then(serde_json::Value::as_array_mut) {
            for c in containers.iter_mut() {
                let csc = c
                    .as_object_mut()
                    .and_then(|o| {
                        o.entry("securityContext")
                            .or_insert_with(|| serde_json::json!({}))
                            .as_object_mut()
                    });
                if let Some(csc) = csc {
                    csc.insert("allowPrivilegeEscalation".to_string(), serde_json::json!(false));
                    if h.read_only_root_fs {
                        csc.insert("readOnlyRootFilesystem".to_string(), serde_json::json!(true));
                    }
                    if h.drop_all_capabilities {
                        csc.insert("capabilities".to_string(), serde_json::json!({ "drop": ["ALL"] }));
                    }
                }
            }
        }
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

    // `effective_limits` folds the `resources.gpu` accelerator sugar into the
    // limits map (extended resources are limits-only; k8s implies the request).
    let resources = ctx.resources.as_ref().map(|r| {
        serde_json::json!({
            "requests": r.requests,
            "limits": r.effective_limits(),
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

    let mut manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": { "app": "module-54-scheduler" },
        },
        "spec": spec,
    });

    // Shape the manifest before it is deserialized, so hardening applies to every
    // task pod this executor creates rather than to whichever call sites
    // remembered. `wants_sa` distinguishes a task that asked for an identity from
    // one that did not: the former keeps its token, the latter should not be
    // handed one it never requested.
    apply_hardening(&mut manifest, &PodHardening::from_env(), ctx.service_account.is_some());

    let pod = serde_json::from_value(manifest)
        .map_err(|e| anyhow::anyhow!("build pod manifest: {e}"))?;
    Ok(pod)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manifest builder produces a valid one-shot Pod with the task's image
    /// and command — verifiable without a cluster.
        /// The default must not change what a working task pod is allowed to do --
    /// except for the token nobody asked for.
    #[test]
    fn hardening_defaults_are_inert_apart_from_the_unrequested_token() {
        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &PodHardening::default(), false);
        let spec = &pod["spec"];
        assert!(spec.get("securityContext").is_none(), "no securityContext by default");
        assert!(spec.get("runtimeClassName").is_none());
        assert!(spec.get("activeDeadlineSeconds").is_none());
        // The one changed default: a task that declared no service_account is
        // not handed a token, because on an IRSA cluster that token is an IAM
        // credential given to arbitrary customer code.
        assert_eq!(spec["automountServiceAccountToken"], serde_json::json!(false));
    }

    /// A task that DID ask for an identity keeps its token.
    #[test]
    fn a_task_requesting_a_service_account_keeps_its_token() {
        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &PodHardening::default(), true);
        assert!(pod["spec"].get("automountServiceAccountToken").is_none());
    }

    #[test]
    fn hardening_shapes_every_field_it_is_given() {
        let h = PodHardening {
            run_as_user: Some(65532),
            read_only_root_fs: true,
            drop_all_capabilities: true,
            seccomp_runtime_default: true,
            active_deadline_secs: Some(3600),
            runtime_class: Some("gvisor".into()),
            node_selector: vec![("dagron.io/untrusted".into(), "true".into())],
            automount_sa_token: false,
        };
        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &h, false);
        let spec = &pod["spec"];
        assert_eq!(spec["securityContext"]["runAsUser"], 65532);
        assert_eq!(spec["securityContext"]["runAsNonRoot"], true);
        assert_eq!(spec["securityContext"]["seccompProfile"]["type"], "RuntimeDefault");
        assert_eq!(spec["activeDeadlineSeconds"], 3600);
        assert_eq!(spec["runtimeClassName"], "gvisor");
        assert_eq!(spec["nodeSelector"]["dagron.io/untrusted"], "true");
        let csc = &spec["containers"][0]["securityContext"];
        assert_eq!(csc["allowPrivilegeEscalation"], false);
        assert_eq!(csc["readOnlyRootFilesystem"], true);
        assert_eq!(csc["capabilities"]["drop"][0], "ALL");
    }

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
            env: vec![EnvVar { name: "S3_BUCKET".into(), value: "dagron-lt".into(), value_from: None }],
            // `gpu` was added to ResourceRequirements and this test was never
            // updated, so the whole `kubernetes` test target stopped compiling --
            // which is why build_pod's missing securityContext went unnoticed for
            // so long: the tests that describe the pod's shape were dead.
            resources: Some(ResourceRequirements { requests, limits, gpu: None }),
            service_account: Some("dagron-etl".to_string()),
            log_sink: None,
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
