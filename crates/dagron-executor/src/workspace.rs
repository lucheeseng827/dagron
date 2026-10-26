//! Task workspaces on each executor: what the operator sets, and the parts an
//! executor builds one from. What a workspace is, a directory a task starts in
//! that is made for each attempt, is [`dagron_core::workspace`].

use std::path::{Path, PathBuf};

use dagron_core::workspace::{ActionSpec, CheckoutSpec, WorkspaceSpec};
pub use dagron_core::workspace::{ACTIONS_DIR, ENV_ACTIONS_DIR, TOOLS_DIR, WORKSPACE_DIR};
use serde_json::{json, Value};

/// An image whose entrypoint is `dagron-ci-exec`. Set, the Kubernetes and
/// Docker executors start each workspace task with an init container from it,
/// which installs the binary into [`TOOLS_DIR`], read-only in the task.
pub const ENV_CI_EXEC_IMAGE: &str = "DAGRON_CI_EXEC_IMAGE";
/// The directory every task process starts in on the local executor, and the
/// workspace of each task that asks for one.
pub const ENV_LOCAL_WORKDIR: &str = "DAGRON_LOCAL_WORKDIR";
/// From this size, a workspace on Kubernetes is a volume of its own (a generic
/// ephemeral volume) rather than an `emptyDir` on the node's disk.
pub const ENV_PVC_FROM: &str = "DAGRON_WORKSPACE_PVC_FROM";
/// The StorageClass of those volumes; the cluster's default when unset.
pub const ENV_STORAGE_CLASS: &str = "DAGRON_WORKSPACE_STORAGE_CLASS";

/// The init container's name in a pod.
pub const INIT_NAME: &str = "install-dagron-ci-exec";
/// The pod volumes a workspace task gets.
const WORKSPACE_VOLUME: &str = "workspace";
const TOOLS_VOLUME: &str = "dagron-tools";
const ACTIONS_VOLUME: &str = "dagron-actions";

/// What the init container runs, after its image's entrypoint: the step
/// runner into [`TOOLS_DIR`], and with it each tool the workspace asks for
/// (`--with node`).
pub fn install_args(ws: &WorkspaceSpec) -> Vec<String> {
    let mut args = vec!["install".to_string(), TOOLS_DIR.to_string()];
    for tool in &ws.tools {
        args.push("--with".to_string());
        args.push(tool.clone());
    }
    args
}

/// `DAGRON_CI_EXEC_IMAGE`: an image reference, or unset (empty).
pub fn parse_image(raw: &str) -> Result<Option<String>, String> {
    match raw.trim() {
        "" => Ok(None),
        image if image.chars().any(char::is_whitespace) => {
            Err(format!("{image:?} is not an image reference"))
        }
        image => Ok(Some(image.to_string())),
    }
}

/// `DAGRON_LOCAL_WORKDIR`: a directory that exists, by its absolute path, or
/// unset (empty). Checked at startup, because a missing one fails every task
/// at spawn with an error that reads as if the task's program were missing.
pub fn parse_local_workdir(raw: &str) -> Result<Option<PathBuf>, String> {
    match raw.trim() {
        "" => Ok(None),
        dir if !Path::new(dir).is_absolute() => Err(format!(
            "the working directory is an absolute path, not {dir:?}"
        )),
        dir if !Path::new(dir).is_dir() => Err(format!("{dir} is not a directory")),
        dir => Ok(Some(PathBuf::from(dir))),
    }
}

/// How workspaces are made on Kubernetes, read once at connect.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KubeWorkspaces {
    /// [`ENV_CI_EXEC_IMAGE`].
    pub ci_exec_image: Option<String>,
    /// [`ENV_PVC_FROM`], in bytes.
    pub pvc_from: Option<u128>,
    /// [`ENV_STORAGE_CLASS`].
    pub storage_class: Option<String>,
    /// [`crate::checkout::ENV_CHECKOUT_IMAGE`]: what a checkout is fetched
    /// with.
    pub checkout_image: Option<String>,
}

/// A workspace's checkout as a pod is given it: what to fetch, and how the
/// attempt's credential, when it has one, reaches the checkout container.
#[derive(Debug, Clone, Copy)]
pub struct PodCheckout<'a> {
    pub checkout: &'a CheckoutSpec,
    pub credential: Option<PodCredential<'a>>,
    /// The link to the patch a patched checkout lays over its commit:
    /// written in, as a link of an hour, minted for this attempt alone.
    pub patch: Option<&'a str>,
}

/// How the credentials of a workspace's actions reach the container that
/// fetches them: each repository's header, in [`crate::checkout::action_headers`]'s
/// order, by reference from the task's Secret of `secret`'s name, or
/// written in where there is none.
#[derive(Debug, Clone, Copy, Default)]
pub struct PodActions<'a> {
    pub headers: &'a [(String, String)],
    pub secret: Option<&'a str>,
}

/// How a checkout's credential reaches its container.
#[derive(Debug, Clone, Copy)]
pub enum PodCredential<'a> {
    /// By reference, from the task's Secret of this name.
    Secret(&'a str),
    /// Written into the pod spec, the header itself: as
    /// `DAGRON_TASK_SECRET_ENV=inline` asks of every secret.
    Inline(&'a str),
}

/// The group a claim workspace is given to every container of its pod: a
/// volume a provisioner makes is root's, and its pod's containers may run as
/// any user.
pub const CLAIM_GROUP: i64 = 65532;

impl KubeWorkspaces {
    pub fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).unwrap_or_default();
        let checkout_image = parse_image(&var(crate::checkout::ENV_CHECKOUT_IMAGE))
            .map_err(|e| format!("{}: {e}", crate::checkout::ENV_CHECKOUT_IMAGE))?;
        Ok(Self {
            checkout_image,
            ..Self::parse(
                &var(ENV_CI_EXEC_IMAGE),
                &var(ENV_PVC_FROM),
                &var(ENV_STORAGE_CLASS),
            )?
        })
    }

    /// The three settings, each empty when unset. A StorageClass without a
    /// size to use it from would make nothing, so it is refused.
    pub fn parse(image: &str, pvc_from: &str, storage_class: &str) -> Result<Self, String> {
        let ci_exec_image = parse_image(image).map_err(|e| format!("{ENV_CI_EXEC_IMAGE}: {e}"))?;
        let pvc_from = match pvc_from.trim() {
            "" => None,
            size => Some(dagron_core::workspace::quantity_bytes(size).ok_or_else(|| {
                format!("{ENV_PVC_FROM}: {size:?} is not a size (20Gi, 500M; 0 for every sized workspace)")
            })?),
        };
        let storage_class = match storage_class.trim() {
            "" => None,
            class if is_dns_subdomain(class) => Some(class.to_string()),
            class => {
                return Err(format!(
                    "{ENV_STORAGE_CLASS}: {class:?} is not a StorageClass name"
                ))
            }
        };
        if storage_class.is_some() && pvc_from.is_none() {
            return Err(format!(
                "{ENV_STORAGE_CLASS} is set without {ENV_PVC_FROM}, so no workspace would use it: \
                 set {ENV_PVC_FROM} to the size from which a workspace is its own volume"
            ));
        }
        Ok(Self {
            ci_exec_image,
            pvc_from,
            storage_class,
            checkout_image: None,
        })
    }

    /// Give a task pod's `spec`, whose one container `task` is already in it,
    /// the workspace `ws`: the volume at [`WORKSPACE_DIR`] as its working
    /// directory; with an exec image the init container that installs
    /// `dagron-ci-exec`, read-only at [`TOOLS_DIR`]; with `checkout`, after
    /// it, the init container that fetches it from
    /// [`KubeWorkspaces::checkout_image`] (which the caller has made sure of);
    /// and with actions, after that, the init container that fetches them,
    /// from the same image, into a volume the task reads at [`ACTIONS_DIR`].
    ///
    /// Init containers ask for the cpu and memory the task asks for. That
    /// costs the pod nothing, since a pod's request is the larger of its init
    /// containers' and the sum of the rest, and it passes the namespace's
    /// quota and limits exactly where the task itself would.
    pub fn shape(&self, spec: &mut Value, ws: &WorkspaceSpec, checkout: Option<&PodCheckout<'_>>) {
        self.shape_with(spec, ws, checkout, PodActions::default());
    }

    /// [`KubeWorkspaces::shape`], with the credentials of the workspace's
    /// actions.
    pub fn shape_with(
        &self,
        spec: &mut Value,
        ws: &WorkspaceSpec,
        checkout: Option<&PodCheckout<'_>>,
        actions: PodActions<'_>,
    ) {
        let workspace = match (&ws.size, ws.size_bytes(), self.pvc_from) {
            (Some(size), Some(bytes), Some(from)) if bytes >= from => {
                let mut claim = json!({
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": size}},
                });
                if let Some(class) = &self.storage_class {
                    claim["storageClassName"] = json!(class);
                }
                // Its root is the provisioner's, root's on most; the pod's
                // group, which every container of it holds, may write it.
                spec["securityContext"]["fsGroup"] = json!(CLAIM_GROUP);
                spec["securityContext"]["fsGroupChangePolicy"] = json!("OnRootMismatch");
                json!({
                    "name": WORKSPACE_VOLUME,
                    "ephemeral": {"volumeClaimTemplate": {"spec": claim}},
                })
            }
            (Some(size), ..) => json!({"name": WORKSPACE_VOLUME, "emptyDir": {"sizeLimit": size}}),
            (None, ..) => json!({"name": WORKSPACE_VOLUME, "emptyDir": {}}),
        };
        let mut volumes = vec![workspace];
        let mut mounts = vec![json!({"name": WORKSPACE_VOLUME, "mountPath": WORKSPACE_DIR})];
        let resources = spec["containers"][0]
            .get("resources")
            .and_then(cpu_and_memory);
        let mut inits = Vec::new();
        if let Some(image) = &self.ci_exec_image {
            volumes.push(json!({"name": TOOLS_VOLUME, "emptyDir": {}}));
            mounts.push(json!({"name": TOOLS_VOLUME, "mountPath": TOOLS_DIR, "readOnly": true}));
            let mut init = json!({
                "name": INIT_NAME,
                "image": image,
                "args": install_args(ws),
                "volumeMounts": [{"name": TOOLS_VOLUME, "mountPath": TOOLS_DIR}],
            });
            if let Some(resources) = &resources {
                init["resources"] = resources.clone();
            }
            inits.push(init);
        }
        if let (Some(fetch), Some(image)) = (checkout, &self.checkout_image) {
            inits.push(checkout_container(fetch, image, resources.as_ref()));
            // git in the task, after a fetch made as another user: the
            // workspace is its own to work in.
            let task_env: Vec<dagron_core::dag::EnvVar> = spec["containers"][0]["env"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|e| e["name"].as_str())
                .map(|name| dagron_core::dag::EnvVar {
                    name: name.to_string(),
                    value: String::new(),
                    value_from: None,
                })
                .collect();
            let safe = crate::checkout::task_env(&task_env);
            if !safe.is_empty() {
                let env = spec["containers"][0]["env"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                spec["containers"][0]["env"] = Value::Array(
                    env.into_iter()
                        .chain(
                            safe.into_iter()
                                .map(|(k, v)| json!({"name": k, "value": v})),
                        )
                        .collect(),
                );
            }
        }
        if let (false, Some(image)) = (ws.actions.is_empty(), &self.checkout_image) {
            volumes.push(json!({"name": ACTIONS_VOLUME, "emptyDir": {}}));
            mounts
                .push(json!({"name": ACTIONS_VOLUME, "mountPath": ACTIONS_DIR, "readOnly": true}));
            inits.push(actions_container(
                &ws.actions,
                actions,
                image,
                resources.as_ref(),
            ));
            let mut env = spec["containers"][0]["env"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            env.push(json!({"name": ENV_ACTIONS_DIR, "value": ACTIONS_DIR}));
            spec["containers"][0]["env"] = Value::Array(env);
        }
        if !inits.is_empty() {
            spec["initContainers"] = Value::Array(inits);
        }
        spec["volumes"] = Value::Array(volumes);
        let task = &mut spec["containers"][0];
        task["workingDir"] = json!(WORKSPACE_DIR);
        task["volumeMounts"] = Value::Array(mounts);
    }
}

/// The init container that fetches `fetch` into the workspace: the script,
/// whatever the image's entrypoint; the fetch's variables, the credential's
/// by reference or written in as `fetch` says; a widened umask, since the
/// task's user may not be this one's.
fn checkout_container(fetch: &PodCheckout<'_>, image: &str, resources: Option<&Value>) -> Value {
    let mut env: Vec<Value> =
        crate::checkout::plain_env(fetch.checkout, fetch.credential.is_some(), Some("0000"))
            .into_iter()
            .map(|(k, v)| json!({"name": k, "value": v}))
            .collect();
    match fetch.credential {
        Some(PodCredential::Secret(secret)) => env.push(json!({
            "name": "GIT_CONFIG_VALUE_0",
            "valueFrom": {"secretKeyRef": {"name": secret, "key": crate::checkout::SECRET_KEY}},
        })),
        Some(PodCredential::Inline(header)) => {
            env.push(json!({"name": "GIT_CONFIG_VALUE_0", "value": header}))
        }
        None => {}
    }
    env.extend(
        crate::checkout::patch_env(fetch.checkout, fetch.patch)
            .into_iter()
            .map(|(k, v)| json!({"name": k, "value": v})),
    );
    let mut container = json!({
        "name": crate::checkout::CONTAINER_NAME,
        "image": image,
        "command": ["sh", "-ec", crate::checkout::SCRIPT],
        "workingDir": WORKSPACE_DIR,
        "env": env,
        "volumeMounts": [{"name": WORKSPACE_VOLUME, "mountPath": WORKSPACE_DIR}],
    });
    if let Some(resources) = resources {
        container["resources"] = resources.clone();
    }
    container
}

/// The init container that fetches `actions` into their volume: the script,
/// whatever the image's entrypoint, and its variables; the credential of
/// each that has one by reference, or written in, as `credentials` says.
fn actions_container(
    actions: &[ActionSpec],
    credentials: PodActions<'_>,
    image: &str,
    resources: Option<&Value>,
) -> Value {
    let repositories: Vec<String> = credentials
        .headers
        .iter()
        .map(|(repository, _)| repository.clone())
        .collect();
    let mut env: Vec<Value> = crate::checkout::actions_env(actions, ACTIONS_DIR)
        .into_iter()
        .chain(crate::checkout::actions_credential_env(&repositories))
        .map(|(k, v)| json!({"name": k, "value": v}))
        .collect();
    for (i, (_, header)) in credentials.headers.iter().enumerate() {
        let name = format!("GIT_CONFIG_VALUE_{}", i + 1);
        env.push(match credentials.secret {
            Some(secret) => json!({
                "name": name,
                "valueFrom": {"secretKeyRef": {
                    "name": secret,
                    "key": crate::checkout::action_secret_key(i + 1),
                }},
            }),
            None => json!({"name": name, "value": header}),
        });
    }
    let mut container = json!({
        "name": crate::checkout::ACTIONS_CONTAINER_NAME,
        "image": image,
        "command": ["sh", "-ec", crate::checkout::ACTIONS_SCRIPT],
        "env": env,
        "volumeMounts": [{"name": ACTIONS_VOLUME, "mountPath": ACTIONS_DIR}],
    });
    if let Some(resources) = resources {
        container["resources"] = resources.clone();
    }
    container
}

/// A container's `resources`, cut to cpu and memory. An accelerator or other
/// extended resource stays with the task alone.
pub(crate) fn cpu_and_memory(resources: &Value) -> Option<Value> {
    let mut out = serde_json::Map::new();
    for part in ["requests", "limits"] {
        let kept: serde_json::Map<String, Value> = resources
            .get(part)
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .filter(|(name, _)| matches!(name.as_str(), "cpu" | "memory"))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        if !kept.is_empty() {
            out.insert(part.to_string(), Value::Object(kept));
        }
    }
    (!out.is_empty()).then_some(Value::Object(out))
}

/// Kubernetes' rule for a StorageClass name, a DNS subdomain.
fn is_dns_subdomain(s: &str) -> bool {
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    s.len() <= 253
        && s.split('.').all(|part| {
            part.starts_with(alnum)
                && part.ends_with(alnum)
                && part.chars().all(|c| alnum(c) || c == '-')
        })
}

/// A directory made for one attempt on the local executor.
///
/// [`Scratch::remove`] removes it once the attempt is over. Dropped without
/// that, as when an attempt is abandoned, it is removed in the background.
pub(crate) struct Scratch(PathBuf);

impl Scratch {
    pub(crate) fn new() -> std::io::Result<Self> {
        let path =
            std::env::temp_dir().join(format!("dagron-ws-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }

    /// Remove it, off the runtime's threads: a workspace can hold gigabytes.
    pub(crate) async fn remove(mut self) {
        let path = std::mem::take(&mut self.0);
        let _ = tokio::task::spawn_blocking(move || remove_logged(&path)).await;
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if self.0.as_os_str().is_empty() {
            return;
        }
        let path = std::mem::take(&mut self.0);
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn_blocking(move || remove_logged(&path));
            }
            Err(_) => remove_logged(&path),
        }
    }
}

fn remove_logged(path: &Path) {
    if let Err(e) = remove_tree(path) {
        tracing::warn!(workspace = %path.display(), error = %e, "a task's workspace could not be removed");
    }
}

/// `remove_dir_all`, and where a directory without write permission refuses
/// it (a Go module cache makes its directories read-only), the same again
/// once each directory in the tree is its owner's to change.
pub(crate) fn remove_tree(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            make_writable(path);
            std::fs::remove_dir_all(path)
        }
        done => done,
    }
}

/// Give the owner full permission on every directory under `root`, never
/// following a link out of it.
#[cfg(unix)]
fn make_writable(root: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(meta) = std::fs::symlink_metadata(&dir) else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let mode = meta.permissions().mode();
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode | 0o700));
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                dirs.push(entry.path());
            }
        }
    }
}

#[cfg(not(unix))]
fn make_writable(_: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_pod() -> Value {
        json!({
            "restartPolicy": "Never",
            "containers": [{
                "name": "task",
                "image": "rust:1",
                "command": ["cargo", "test"],
                "resources": {
                    "requests": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "1Gi"},
                    "limits": {"memory": "8Gi", "nvidia.com/gpu": "1"},
                },
            }],
        })
    }

    #[test]
    fn settings_are_read_once_and_refused_when_they_would_do_nothing() {
        assert_eq!(
            KubeWorkspaces::parse("", "", "").unwrap(),
            KubeWorkspaces::default()
        );
        let all = KubeWorkspaces::parse(" ghcr.io/x/dagron-ci-exec:1 ", "10Gi", "gp3").unwrap();
        assert_eq!(
            all.ci_exec_image.as_deref(),
            Some("ghcr.io/x/dagron-ci-exec:1")
        );
        assert_eq!(all.pvc_from, Some(10 << 30));
        assert_eq!(all.storage_class.as_deref(), Some("gp3"));
        assert_eq!(
            KubeWorkspaces::parse("", "0", "").unwrap().pvc_from,
            Some(0)
        );
        for (image, from, class) in [
            ("a b", "", ""),
            ("", "10GB", ""),
            ("", "10Gi", "GP3"),
            ("", "10Gi", "-gp3"),
            // a class with no size to use it from
            ("", "", "gp3"),
        ] {
            assert!(
                KubeWorkspaces::parse(image, from, class).is_err(),
                "{image:?} {from:?} {class:?}"
            );
        }
    }

    #[test]
    fn a_workspace_is_an_empty_dir_the_task_starts_in() {
        let mut spec = task_pod();
        KubeWorkspaces::default().shape(&mut spec, &WorkspaceSpec::default(), None);
        assert_eq!(
            spec["volumes"],
            json!([{"name": "workspace", "emptyDir": {}}])
        );
        let task = &spec["containers"][0];
        assert_eq!(task["workingDir"], "/workspace");
        assert_eq!(
            task["volumeMounts"],
            json!([{"name": "workspace", "mountPath": "/workspace"}])
        );
        assert!(
            spec.get("initContainers").is_none(),
            "no image, no init container"
        );

        let mut spec = task_pod();
        KubeWorkspaces::default().shape(
            &mut spec,
            &WorkspaceSpec {
                size: Some("20Gi".into()),
                ..WorkspaceSpec::default()
            },
            None,
        );
        assert_eq!(spec["volumes"][0]["emptyDir"]["sizeLimit"], "20Gi");
    }

    #[test]
    fn a_workspace_past_the_operator_s_size_is_a_volume_of_its_own() {
        let ws = KubeWorkspaces {
            pvc_from: Some(10 << 30),
            ..Default::default()
        };
        let mut spec = task_pod();
        ws.shape(
            &mut spec,
            &WorkspaceSpec {
                size: Some("9Gi".into()),
                ..WorkspaceSpec::default()
            },
            None,
        );
        assert_eq!(
            spec["volumes"][0]["emptyDir"]["sizeLimit"], "9Gi",
            "below it, an emptyDir"
        );
        assert!(
            spec.get("securityContext").is_none(),
            "anyone may write an emptyDir"
        );

        let mut spec = task_pod();
        ws.shape(
            &mut spec,
            &WorkspaceSpec {
                size: Some("10Gi".into()),
                ..WorkspaceSpec::default()
            },
            None,
        );
        assert_eq!(
            spec["volumes"][0],
            json!({"name": "workspace", "ephemeral": {"volumeClaimTemplate": {"spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "10Gi"}},
            }}}})
        );
        // Its root is the provisioner's: the pod's group, which every
        // container holds, may write it.
        assert_eq!(
            spec["securityContext"],
            json!({"fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch"})
        );

        let classed = KubeWorkspaces {
            storage_class: Some("gp3".into()),
            ..ws.clone()
        };
        let mut spec = task_pod();
        classed.shape(
            &mut spec,
            &WorkspaceSpec {
                size: Some("1Ti".into()),
                ..WorkspaceSpec::default()
            },
            None,
        );
        assert_eq!(
            spec["volumes"][0]["ephemeral"]["volumeClaimTemplate"]["spec"]["storageClassName"],
            "gp3"
        );

        // No size, no claim: a volume needs a size to ask for.
        let mut spec = task_pod();
        ws.shape(&mut spec, &WorkspaceSpec::default(), None);
        assert_eq!(
            spec["volumes"][0],
            json!({"name": "workspace", "emptyDir": {}})
        );
    }

    #[test]
    fn an_exec_image_installs_dagron_ci_exec_read_only_beside_the_workspace() {
        let ws = KubeWorkspaces {
            ci_exec_image: Some("dagron-ci-exec:1".into()),
            ..Default::default()
        };
        let mut spec = task_pod();
        ws.shape(&mut spec, &WorkspaceSpec::default(), None);
        assert_eq!(
            spec["volumes"][1],
            json!({"name": "dagron-tools", "emptyDir": {}})
        );
        assert_eq!(
            spec["containers"][0]["volumeMounts"][1],
            json!({"name": "dagron-tools", "mountPath": "/dagron/bin", "readOnly": true})
        );
        let init = &spec["initContainers"][0];
        assert_eq!(init["name"], "install-dagron-ci-exec");
        assert_eq!(init["image"], "dagron-ci-exec:1");
        assert!(init.get("command").is_none(), "the image's entrypoint runs");
        assert_eq!(init["args"], json!(["install", "/dagron/bin"]));
        assert_eq!(
            init["volumeMounts"],
            json!([{"name": "dagron-tools", "mountPath": "/dagron/bin"}])
        );
        // The task's cpu and memory, and nothing it alone should hold.
        assert_eq!(
            init["resources"],
            json!({"requests": {"cpu": "2", "memory": "4Gi"}, "limits": {"memory": "8Gi"}})
        );

        // A task that asks for nothing gives its init container nothing.
        let mut spec = task_pod();
        spec["containers"][0]
            .as_object_mut()
            .unwrap()
            .remove("resources");
        ws.shape(&mut spec, &WorkspaceSpec::default(), None);
        assert!(spec["initContainers"][0].get("resources").is_none());
    }

    #[test]
    fn a_checkout_is_fetched_by_an_init_container_after_the_install() {
        let ws = KubeWorkspaces {
            ci_exec_image: Some("dagron-ci-exec:1".into()),
            checkout_image: Some("alpine/git:v2.47.2".into()),
            ..Default::default()
        };
        let checkout = CheckoutSpec {
            repository: "https://github.com/acme/widgets.git".into(),
            commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            depth: None,
            patched: false,
        };
        let mut spec = task_pod();
        spec["containers"][0]["env"] = json!([{"name": "CI_SHA", "value": "x"}]);
        let fetch = PodCheckout {
            checkout: &checkout,
            credential: Some(PodCredential::Secret("sched-1-env")),
            patch: None,
        };
        ws.shape(&mut spec, &WorkspaceSpec::default(), Some(&fetch));

        let inits = spec["initContainers"].as_array().unwrap();
        assert_eq!(inits.len(), 2);
        assert_eq!(
            inits[0]["name"], "install-dagron-ci-exec",
            "the tools first"
        );
        let fetcher = &inits[1];
        assert_eq!(fetcher["name"], "checkout");
        assert_eq!(fetcher["image"], "alpine/git:v2.47.2");
        assert_eq!(
            fetcher["command"],
            json!(["sh", "-ec", crate::checkout::SCRIPT])
        );
        assert_eq!(fetcher["workingDir"], "/workspace");
        assert_eq!(
            fetcher["volumeMounts"],
            json!([{"name": "workspace", "mountPath": "/workspace"}])
        );
        assert_eq!(fetcher["resources"], inits[0]["resources"]);
        let env = fetcher["env"].as_array().unwrap();
        let get = |k: &str| env.iter().find(|e| e["name"] == k).cloned();
        assert_eq!(
            get("DAGRON_CHECKOUT_COMMIT").unwrap()["value"],
            checkout.commit
        );
        assert_eq!(get("DAGRON_CHECKOUT_UMASK").unwrap()["value"], "0000");
        assert_eq!(
            get("GIT_CONFIG_KEY_0").unwrap()["value"],
            "http.https://github.com/acme/widgets.git.extraHeader"
        );
        assert_eq!(get("GIT_CONFIG_COUNT").unwrap()["value"], "2");
        assert_eq!(
            get("GIT_CONFIG_KEY_1").unwrap()["value"],
            "http.followRedirects"
        );
        assert_eq!(get("GIT_CONFIG_VALUE_1").unwrap()["value"], "false");
        assert_eq!(
            get("GIT_CONFIG_VALUE_0").unwrap(),
            json!({"name": "GIT_CONFIG_VALUE_0", "valueFrom": {"secretKeyRef": {
                "name": "sched-1-env", "key": "dagron-checkout-authorization",
            }}}),
            "by reference: the header is in no pod spec"
        );

        // The task holds no credential, and is told the workspace is safe.
        let task_env = spec["containers"][0]["env"].as_array().unwrap();
        assert_eq!(
            task_env
                .iter()
                .map(|e| e["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "CI_SHA",
                "GIT_CONFIG_COUNT",
                "GIT_CONFIG_KEY_0",
                "GIT_CONFIG_VALUE_0"
            ]
        );
        assert_eq!(task_env[2]["value"], "safe.directory");
        assert!(task_env.iter().all(|e| e.get("valueFrom").is_none()));

        // Inline, as the operator asked of every secret: written in.
        let mut spec = task_pod();
        let inline = PodCheckout {
            checkout: &checkout,
            credential: Some(PodCredential::Inline("Authorization: Basic eDp5")),
            patch: None,
        };
        ws.shape(&mut spec, &WorkspaceSpec::default(), Some(&inline));
        let env = spec["initContainers"][1]["env"].as_array().unwrap().clone();
        assert!(env.contains(
            &json!({"name": "GIT_CONFIG_VALUE_0", "value": "Authorization: Basic eDp5"})
        ));

        // No credential: no header and no key naming one.
        let mut spec = task_pod();
        let anonymous = PodCheckout {
            checkout: &checkout,
            credential: None,
            patch: None,
        };
        ws.shape(&mut spec, &WorkspaceSpec::default(), Some(&anonymous));
        let env = spec["initContainers"][1]["env"].as_array().unwrap().clone();
        assert!(
            env.iter()
                .all(|e| !e["name"].as_str().unwrap().starts_with("GIT_CONFIG")),
            "{env:?}"
        );

        // Without the tools, the fetch is the one init container.
        let fetch_only = KubeWorkspaces {
            checkout_image: Some("alpine/git:v2.47.2".into()),
            ..Default::default()
        };
        let mut spec = task_pod();
        fetch_only.shape(&mut spec, &WorkspaceSpec::default(), Some(&anonymous));
        assert_eq!(spec["initContainers"].as_array().unwrap().len(), 1);
        assert_eq!(spec["initContainers"][0]["name"], "checkout");
    }

    /// A task's actions are fetched by an init container of the checkout's
    /// image, after the checkout, into a volume of their own that the task
    /// reads at /dagron/actions, and is told of.
    #[test]
    fn a_task_that_asks_for_node_is_given_it_with_the_step_runner() {
        let ws = KubeWorkspaces {
            ci_exec_image: Some("dagron-ci-exec:1".into()),
            ..Default::default()
        };
        let mut spec = task_pod();
        let with_node = WorkspaceSpec {
            tools: vec!["node".into()],
            ..WorkspaceSpec::default()
        };
        ws.shape(&mut spec, &with_node, None);
        assert_eq!(
            spec["initContainers"][0]["args"],
            json!(["install", "/dagron/bin", "--with", "node"])
        );
        assert_eq!(
            install_args(&WorkspaceSpec::default()),
            ["install", "/dagron/bin"],
            "and none is given it where it asks for none"
        );
    }

    #[test]
    fn a_task_s_actions_are_fetched_by_an_init_container_after_the_checkout() {
        let ws = KubeWorkspaces {
            ci_exec_image: Some("dagron-ci-exec:1".into()),
            checkout_image: Some("alpine/git:v2.47.2".into()),
            ..Default::default()
        };
        let checkout = CheckoutSpec {
            repository: "https://github.com/acme/widgets.git".into(),
            commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            depth: None,
            patched: false,
        };
        let fetch = PodCheckout {
            checkout: &checkout,
            credential: Some(PodCredential::Secret("sched-1-env")),
            patch: None,
        };
        let with_actions = WorkspaceSpec {
            actions: vec![ActionSpec {
                repository: "https://github.com/octo/hello.git".into(),
                git_ref: "v1".into(),
                path: "octo/hello/v1".into(),
                credential: false,
            }],
            ..WorkspaceSpec::default()
        };
        let mut spec = task_pod();
        ws.shape(&mut spec, &with_actions, Some(&fetch));

        let inits = spec["initContainers"].as_array().unwrap();
        let names: Vec<&str> = inits.iter().map(|i| i["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["install-dagron-ci-exec", "checkout", "actions"]);
        let fetcher = &inits[2];
        assert_eq!(fetcher["image"], "alpine/git:v2.47.2");
        assert_eq!(
            fetcher["command"],
            json!(["sh", "-ec", crate::checkout::ACTIONS_SCRIPT])
        );
        assert_eq!(
            fetcher["volumeMounts"],
            json!([{"name": "dagron-actions", "mountPath": "/dagron/actions"}])
        );
        assert_eq!(fetcher["resources"], inits[0]["resources"]);
        let env = fetcher["env"].as_array().unwrap();
        assert!(env.contains(&json!({"name": "DAGRON_ACTION_1_REF", "value": "v1"})));
        assert!(
            env.iter()
                .all(|e| !e["name"].as_str().unwrap().starts_with("GIT_CONFIG")
                    && e.get("valueFrom").is_none()),
            "no credential: {env:?}"
        );
        assert!(spec["volumes"]
            .as_array()
            .unwrap()
            .contains(&json!({"name": "dagron-actions", "emptyDir": {}})));
        let task = &spec["containers"][0];
        assert!(task["volumeMounts"].as_array().unwrap().contains(
            &json!({"name": "dagron-actions", "mountPath": "/dagron/actions", "readOnly": true})
        ));
        assert!(task["env"]
            .as_array()
            .unwrap()
            .contains(&json!({"name": "DAGRON_ACTIONS_DIR", "value": "/dagron/actions"})));

        // None: no volume, no container.
        let mut spec = task_pod();
        ws.shape(&mut spec, &WorkspaceSpec::default(), Some(&fetch));
        assert!(spec["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["name"] != "dagron-actions"));
        assert_eq!(spec["initContainers"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn settings_from_the_environment_are_checked() {
        assert_eq!(parse_image("").unwrap(), None);
        assert!(parse_image("img :1").is_err());
        assert_eq!(parse_local_workdir(" ").unwrap(), None);
        let dir = std::env::temp_dir();
        assert_eq!(
            parse_local_workdir(dir.to_str().unwrap()).unwrap(),
            Some(dir.clone())
        );
        assert!(parse_local_workdir("relative/dir").is_err());
        assert!(parse_local_workdir(dir.join("dagron-no-such-dir").to_str().unwrap()).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_scratch_directory_goes_however_read_only_its_tree_is() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = Scratch::new().unwrap();
        let path = scratch.path().to_path_buf();
        assert!(path.is_dir());
        // What a Go module cache leaves: files in a directory nobody may write.
        let locked = path.join("pkg/mod/example.com@v1");
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::write(locked.join("go.mod"), "module example.com\n").unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
        scratch.remove().await;
        assert!(!path.exists(), "{} is left", path.display());

        // Dropped rather than removed, it still goes.
        let scratch = Scratch::new().unwrap();
        let path = scratch.path().to_path_buf();
        drop(scratch);
        for _ in 0..100 {
            if !path.exists() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("{} is left", path.display());
    }

    /// An action's credential reaches the container that fetches the
    /// actions as the checkout's reaches its own: by reference from the
    /// task's Secret, a key for each, or written in where there is none.
    #[test]
    fn an_action_s_credential_reaches_the_actions_fetch_from_the_task_s_secret() {
        let ws = KubeWorkspaces {
            checkout_image: Some("alpine/git:v2.47.2".into()),
            ..Default::default()
        };
        let with_actions = WorkspaceSpec {
            actions: vec![ActionSpec {
                repository: "https://github.com/acme/tool.git".into(),
                git_ref: "v1".into(),
                path: "acme/tool/v1".into(),
                credential: true,
            }],
            ..WorkspaceSpec::default()
        };
        let headers = vec![(
            "https://github.com/acme/tool.git".to_string(),
            "Authorization: Basic eDp5".to_string(),
        )];
        let mut spec = task_pod();
        ws.shape_with(
            &mut spec,
            &with_actions,
            None,
            PodActions {
                headers: &headers,
                secret: Some("sched-1-env"),
            },
        );
        let env = spec["initContainers"][0]["env"].as_array().unwrap().clone();
        assert!(
            env.contains(&json!({"name": "GIT_CONFIG_COUNT", "value": "2"})),
            "{env:?}"
        );
        assert!(env.contains(&json!({
            "name": "GIT_CONFIG_KEY_1",
            "value": "http.https://github.com/acme/tool.git.extraHeader",
        })));
        assert!(env.contains(&json!({
            "name": "GIT_CONFIG_VALUE_1",
            "valueFrom": {"secretKeyRef": {
                "name": "sched-1-env",
                "key": "dagron-action-authorization-1",
            }},
        })));
        assert!(
            !spec.to_string().contains("eDp5"),
            "the header is not in the pod spec"
        );

        let mut spec = task_pod();
        ws.shape_with(
            &mut spec,
            &with_actions,
            None,
            PodActions {
                headers: &headers,
                secret: None,
            },
        );
        let env = spec["initContainers"][0]["env"].as_array().unwrap().clone();
        assert!(env.contains(&json!({
            "name": "GIT_CONFIG_VALUE_1",
            "value": "Authorization: Basic eDp5",
        })));
    }
}
