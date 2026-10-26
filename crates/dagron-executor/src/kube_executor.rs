//! Kubernetes pod executor (v3) — `EXECUTOR=kubernetes`, `--features kubernetes`.
//!
//! The third `Executor` backend, alongside `LocalExecutor` (subprocess) and
//! `DockerExecutor` (container): each task runs as a one-shot **Pod**. The pod is
//! created with `restartPolicy: Never`, polled to a terminal phase under a hard
//! timeout while its task's log is followed (`kube_log`), and deleted whether
//! it succeeded or timed out — the same create / wait-and-follow / cleanup shape
//! as `DockerExecutor`, just against the Kubernetes API instead of a Docker
//! socket. Swapping to it touches nothing in the reconcile loop (the `Executor`
//! trait is the seam); the `docker_image` field on `ExecContext` doubles as the
//! per-task container image override here.
//!
//! **Services** ([`dagron_core::service`]) are sidecar containers of the
//! task's pod: init containers that restart, which start in order before the
//! task's container, each once the one before it has passed its startup
//! probe, and which the kubelet stops when the task's container has ended.
//! Kubernetes has them from 1.29; a task with services is refused on an
//! older cluster ([`SIDECARS_FROM`]). Such a task has ended when its own
//! container has: its pod has not until each service has stopped, which one
//! that heeds no signal does only when the pod's grace is out, so the pod is
//! not waited for and is deleted with next to no grace.
//!
//! **Cluster-gated.** `kube`/`k8s-openapi` are a pure-Rust client, so this module
//! *compiles* without a cluster; it only *runs* against a live apiserver
//! (in-cluster service account or local kubeconfig, via `Client::try_default`).
//! The whole module is behind the `kubernetes` Cargo feature so a default build
//! carries none of it.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Pod, Secret};
use k8s_openapi::api::networking::v1::{NetworkPolicy, NetworkPolicySpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{Api, DeleteParams, ListParams, LogParams, Patch, PatchParams, PostParams};
use kube::Client;
use tokio::time::{sleep, timeout, Duration};
use uuid::Uuid;

use crate::executor::{
    ExecContext, ExecOutput, Executor, StepContainer, StepOutcome, MAX_STEP_OUTPUT_BYTES,
    STEP_WORKSPACE_DIR,
};
use crate::workspace::KubeWorkspaces;
use dagron_core::service::ServiceSpec;

/// How often to poll a pod's phase while waiting for it to finish.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// How long a task whose pod the watch is telling of waits between looks at
/// whether the watch is still following. No request is made at a look: it
/// is how soon a task goes back to asking for its pod after the watch stops.
const WATCHED_INTERVAL: Duration = Duration::from_secs(5);

/// Whether an executor keeps one watch of its task pods
/// (`on`, the default), or each task asks for its own pod twice a second
/// as before (`off`).
pub const ENV_POD_WATCH: &str = "DAGRON_TASK_POD_WATCH";

/// Parse [`ENV_POD_WATCH`]. A value that is neither is an error at startup.
pub fn parse_pod_watch(raw: &str) -> std::result::Result<bool, String> {
    match raw.trim() {
        "" | "on" => Ok(true),
        "off" => Ok(false),
        other => Err(format!("it is 'on' or 'off', and is '{other}'")),
    }
}

/// One budget for the WHOLE predecessor reap, list and deletes together.
///
/// Per-delete timeouts do not compose: they bound each call and therefore add
/// up, so N stale pods against an unresponsive apiserver delay pod creation by
/// N × 10s — an unbounded dispatch delay that grows with exactly the mess the
/// reap exists to clear. `ctx.timeout_secs` does not cover this; it starts
/// later, at the wait and log collection.
///
/// When the budget expires with a predecessor still there, this dispatch does
/// not start: it is handed back as [`crate::executor::Backpressure`], which
/// costs the task no attempt, and is tried again in a few seconds. It used to
/// start anyway, on the reasoning that refusing would turn a cleanup failure
/// into an outage. That reasoning held while a refusal was a failed attempt;
/// it is a wait now, and starting anyway is how one task ran twice at once.
const REAP_BUDGET_SECS: u64 = 20;

/// How long a dispatch that has just deleted a predecessor's pod looks for it
/// to be gone before it is handed back. A pod whose process heeds its signal
/// goes within a second or two; one that takes its grace is waited on from
/// the queue, not from a worker.
const PREDECESSOR_WAIT_SECS: u64 = 5;

/// How long past its grace a stopped task's pod is given before it is waited
/// on no longer. A job's step runner gives its step the same grace and then
/// ends on its own; the margin is for that ending, and for the kubelet's.
const STOP_MARGIN_SECS: u64 = 5;
/// How long past the pod's own grace [`KubeExecutor::stop_and_remove`] waits
/// to see it gone.
const STOP_WAIT_MARGIN_SECS: u64 = 20;

/// Deadline on one apiserver call made by the fleet sweep (the list, and each
/// delete). The sweep runs on a slow cadence and its work is bounded per call,
/// so a wedged apiserver costs one skipped sweep rather than a stuck loop.
const SWEEP_CALL_TIMEOUT_SECS: u64 = 10;

/// `DAGRON_TASK_NETWORK_POLICY`: `enforced`, or unset. The operator's word
/// that this cluster's CNI enforces NetworkPolicy, and that the task
/// namespace holds [`NO_NETWORK_POLICY`]. Nothing in the API says whether a
/// CNI enforces a policy or stores it and does nothing, so it is theirs to
/// say; a task that asks for no network is refused until they have.
pub const ENV_NETWORK_POLICY: &str = "DAGRON_TASK_NETWORK_POLICY";

/// The NetworkPolicy that takes the network from a task's pod: it selects the
/// pods labelled [`NETWORK_LABEL`]`=none`, and allows them nothing in
/// and nothing out. One for the namespace, made before any task: a policy made
/// with a pod may not be in force when the pod starts, and one that has been
/// there holds a pod from its first instant.
pub const NO_NETWORK_POLICY: &str = "dagron-task-no-network";

/// `DAGRON_TASK_NETWORK_ALLOWLISTS`: the allowlists the operator has made,
/// by name, comma-separated. Each is a NetworkPolicy of theirs in the task
/// namespace ([`allowlist_policy_name`]), made before any task. A task names
/// one (`isolation.network: allow:<name>`) and cannot write one; one that
/// names an allowlist not listed here is refused. Needs
/// [`ENV_NETWORK_POLICY`]: an allowlist is a policy, and holds a pod only
/// where the cluster enforces one.
pub const ENV_NETWORK_ALLOWLISTS: &str = dagron_core::isolation::ENV_NETWORK_ALLOWLISTS;

/// The label on a task's pod that says which network it asked for: `none`,
/// or `allow-<name>` for an allowlist. A task that asked for nothing has no
/// such label.
pub const NETWORK_LABEL: &str = "dagron.dev/network";
const NO_NETWORK: &str = "none";

/// The NetworkPolicy that holds a task's pod to the allowlist `name`: it
/// selects the pods labelled [`NETWORK_LABEL`]`=allow-<name>`, allows them
/// nothing in, and out only what its egress rules say. Those rules are the
/// operator's.
pub fn allowlist_policy_name(name: &str) -> String {
    dagron_core::isolation::allowlist_object_name(name)
}

/// The value of [`NETWORK_LABEL`] on a pod held to the allowlist `name`.
fn allowlist_label(name: &str) -> String {
    format!("allow-{name}")
}

/// What of the network a task asked to be held to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Asked<'a> {
    /// None of it.
    Nothing,
    /// What the allowlist of this name lets it reach.
    Allowlist(&'a str),
}

impl Asked<'_> {
    /// The value of [`NETWORK_LABEL`] that says so.
    fn label(&self) -> String {
        match self {
            Self::Nothing => NO_NETWORK.to_string(),
            Self::Allowlist(name) => allowlist_label(name),
        }
    }

    /// The NetworkPolicy that holds a pod so labelled.
    fn policy(&self) -> String {
        match self {
            Self::Nothing => NO_NETWORK_POLICY.to_string(),
            Self::Allowlist(name) => allowlist_policy_name(name),
        }
    }

    /// The init container that holds such a pod back, which is also the
    /// command of `dagron-ci-exec` it runs.
    fn gate(&self) -> &'static str {
        match self {
            Self::Nothing => GATE_NAME,
            Self::Allowlist(_) => ALLOWLIST_GATE_NAME,
        }
    }

    /// What the task asked, as the opening of a sentence about it.
    fn says(&self) -> String {
        match self {
            Self::Nothing => "this task asked for no network (isolation.network: none)".into(),
            Self::Allowlist(name) => format!(
                "this task asked for the allowlist '{name}' (isolation.network: allow:{name})"
            ),
        }
    }

    /// Whether `policy` is the one that holds a pod so labelled.
    fn held_as_meant(&self, policy: &NetworkPolicy) -> std::result::Result<(), String> {
        match self {
            Self::Nothing => takes_the_network(policy),
            Self::Allowlist(name) => holds_to_an_allowlist(policy, name),
        }
    }

    /// The policy to apply, as text, for a message that says it is missing
    /// or wrong.
    fn manifest(&self) -> String {
        match self {
            Self::Nothing => no_network_policy_manifest(),
            Self::Allowlist(name) => allowlist_policy_manifest(name),
        }
    }
}

/// The init container that holds a task's pod until its network is seen to
/// be gone: the last of its init containers, from the image that installs
/// `dagron-ci-exec`, running that binary's `wait-no-network`.
///
/// The policy is the operator's and its enforcement the CNI's, and neither
/// can be read from the API. A CNI may also take a pod's network a moment
/// after the pod starts, as k3s's own controller was seen to: a labelled pod
/// reached the API server and another pod as it began, and neither two
/// seconds on. So the task's container is not started on their word. It
/// starts after this container has asked for a connection to the cluster's
/// API server and had no answer, several times running; and where it still
/// has one after [`GATE_SECS`], the task fails without having started.
pub const GATE_NAME: &str = "wait-no-network";

/// The same for a task held to an allowlist: `dagron-ci-exec wait-allowlist`.
/// It asks the cluster's API server alone, and not the pod's name servers:
/// an allowlist may well allow those. So an allowlist must not allow the API
/// server. One that did would never be seen to hold its pod, and its tasks
/// would fail here; a task that may reach the API server is not one an
/// allowlist is for.
pub const ALLOWLIST_GATE_NAME: &str = "wait-allowlist";

/// How long the gate gives the cluster to take the pod's network.
const GATE_SECS: u64 = 30;

/// What the gate runs, after its image's entrypoint.
fn gate_args(asked: Asked<'_>) -> Vec<String> {
    vec![asked.gate().to_string(), GATE_SECS.to_string()]
}

/// What the gate says of a pod it gave up on: the opening of its last line.
/// Anything else it ends with is its own failure, and says nothing of the
/// pod's network.
const GATE_STILL_HAS: &str = "still has a network";
/// And of a pod held to an allowlist that still got an answer.
const GATE_STILL_REACHES: &str = "still reaches";

/// Why a task whose gate failed did not start, from what the gate said and
/// the code it ended with.
///
/// Only a gate that says the pod kept its network is evidence about the
/// cluster. One that was killed, could not start, or could not ask says so
/// here in those words: to blame the CNI for it would send an operator
/// looking at a policy that may be holding.
fn gate_failure(said: &str, code: i32, image: Option<&str>, asked: Asked<'_>) -> String {
    let said = said.trim();
    let image = image.unwrap_or("the image");
    let says = asked.says();
    let gate = asked.gate();
    let policy = asked.policy();
    // An image from before the gate takes the word for no command it knows.
    if said.contains("unknown arguments") {
        return format!(
            "{says}, and {image} does not know `{gate}`, which holds such a task's pod \
             until its network is seen to be held: it is older than this engine. The task \
             was not started"
        );
    }
    if asked == Asked::Nothing && said.contains(GATE_STILL_HAS) {
        return format!(
            "{says}, and its pod was not seen to have none within {GATE_SECS} s: {said}. So \
             nothing on this cluster enforced the NetworkPolicy '{policy}' on it, whatever \
             {ENV_NETWORK_POLICY} says. The task was not started"
        );
    }
    if matches!(asked, Asked::Allowlist(_)) && said.contains(GATE_STILL_REACHES) {
        return format!(
            "{says}, and its pod was not seen to be held within {GATE_SECS} s: {said}. A pod \
             is seen to be held by the cluster's API server no longer answering it, so an \
             allowlist must not allow that address. Either the NetworkPolicy '{policy}' \
             does, or nothing on this cluster enforced it, whatever {ENV_NETWORK_POLICY} \
             says. The task was not started"
        );
    }
    let said = if said.is_empty() {
        "it left no word".to_string()
    } else {
        format!("it said: {said}")
    };
    format!(
        "{says}, and the init container that holds its pod until its network is seen to be \
         held ('{gate}') ended with exit code {code} before it could tell; {said}. That is \
         the gate's own failure, and says nothing of what the cluster enforces. The task \
         was not started"
    )
}

/// [`NO_NETWORK_POLICY`], as an object: what an operator applies to the task
/// namespace, and what [`takes_the_network`] accepts.
pub fn no_network_policy() -> NetworkPolicy {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": { "name": NO_NETWORK_POLICY },
        "spec": {
            "podSelector": { "matchLabels": { NETWORK_LABEL: NO_NETWORK } },
            "policyTypes": ["Ingress", "Egress"],
        },
    }))
    .expect("a NetworkPolicy written here is one")
}

/// [`no_network_policy`] as text `kubectl apply -f -` takes.
fn no_network_policy_manifest() -> String {
    serde_json::to_string_pretty(&no_network_policy()).unwrap_or_default()
}

/// A NetworkPolicy for the allowlist `name` with the egress rules given: the
/// shape [`holds_to_an_allowlist`] accepts. What an operator applies to the
/// task namespace, with rules of their own.
pub fn allowlist_policy(name: &str, egress: serde_json::Value) -> NetworkPolicy {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": { "name": allowlist_policy_name(name) },
        "spec": {
            "podSelector": { "matchLabels": { NETWORK_LABEL: allowlist_label(name) } },
            "policyTypes": ["Ingress", "Egress"],
            "egress": egress,
        },
    }))
    .expect("a NetworkPolicy written here is one")
}

/// [`allowlist_policy`] as text `kubectl apply -f -` takes, with one rule in
/// it for the shape of one: an address that is nobody's, to be replaced.
fn allowlist_policy_manifest(name: &str) -> String {
    let example = serde_json::json!([{
        "to": [{ "ipBlock": { "cidr": "192.0.2.10/32" } }],
        "ports": [{ "protocol": "TCP", "port": 443 }],
    }]);
    serde_json::to_string_pretty(&allowlist_policy(name, example)).unwrap_or_default()
}

/// Parse [`ENV_NETWORK_ALLOWLISTS`]: names, comma-separated, each once. A
/// name that could not be one is an error at startup.
pub fn parse_network_allowlists(raw: &str) -> std::result::Result<Vec<String>, String> {
    dagron_core::isolation::parse_allowlists(raw)
}

/// Make a privileged task's own container privileged, and no other of its
/// pod's. A read-only root asked of it stays; the `allowPrivilegeEscalation:
/// false` a read-only root has implied goes, since the API refuses it beside
/// `privileged`, and what asks for it in its own name (`no_new_privileges`,
/// dropped capabilities) is refused before such a task is dispatched.
fn privilege(manifest: &mut serde_json::Value) {
    let Some(task) = manifest["spec"]["containers"]
        .as_array_mut()
        .and_then(|cs| cs.iter_mut().find(|c| c["name"] == TASK_CONTAINER))
    else {
        return;
    };
    let sc = task
        .as_object_mut()
        .and_then(|o| o.entry("securityContext").or_insert_with(|| serde_json::json!({})).as_object_mut());
    if let Some(sc) = sc {
        sc.remove("allowPrivilegeEscalation");
        sc.insert("privileged".to_string(), serde_json::json!(true));
    }
}

/// The proxy an allowlist's policy says it reaches the rest of the world
/// through (its annotation [`dagron_core::isolation::ALLOWLIST_PROXY`]),
/// where it says one. A NetworkPolicy knows addresses and nothing of names:
/// the proxy, the operator's, is what holds a task to a list of hostnames,
/// and the policy lets the task reach it.
fn allowlist_proxy(policy: &NetworkPolicy) -> std::result::Result<Option<String>, String> {
    let key = dagron_core::isolation::ALLOWLIST_PROXY;
    match policy
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
    {
        Some(raw) => dagron_core::isolation::parse_allowlist_proxy(raw)
            .map(Some)
            .map_err(|e| format!("its annotation {key}: {e}")),
        None => Ok(None),
    }
}

/// Parse [`ENV_NETWORK_POLICY`]: `enforced`, or unset (empty, or `off`). A
/// typo is an error at startup, not a cluster taken to enforce nothing.
pub fn parse_network_policy(raw: &str) -> std::result::Result<bool, String> {
    match raw.trim() {
        "" | "off" => Ok(false),
        "enforced" => Ok(true),
        other => Err(format!("expected 'enforced' or nothing, got '{other}'")),
    }
}

/// Whether `policy` is one that holds the pods labelled `label` and no
/// others, in both directions, and lets nothing in to them. Its spec, for
/// what it lets out to be read by the caller.
///
/// A policy under the right name that selected more, or left a direction
/// alone, would have a task run believing it is held. So it is read, and not
/// taken on its name.
fn holds_the_labelled<'a>(
    policy: &'a NetworkPolicy,
    label: &str,
) -> std::result::Result<&'a NetworkPolicySpec, String> {
    let Some(spec) = &policy.spec else {
        return Err("it has no spec".into());
    };
    // No selector, or an empty one, is every pod of the namespace.
    let only_the_label = spec.pod_selector.as_ref().is_some_and(|selector| {
        let labels = selector.match_labels.clone().unwrap_or_default();
        labels.len() == 1
            && labels.get(NETWORK_LABEL).map(String::as_str) == Some(label)
            && selector
                .match_expressions
                .as_ref()
                .is_none_or(Vec::is_empty)
    });
    if !only_the_label {
        return Err(format!(
            "its podSelector is not the label {NETWORK_LABEL}={label} alone"
        ));
    }
    let types = spec.policy_types.clone().unwrap_or_default();
    for direction in ["Ingress", "Egress"] {
        if !types.iter().any(|t| t == direction) {
            return Err(format!("its policyTypes lack {direction}"));
        }
    }
    if spec.ingress.as_ref().is_some_and(|rules| !rules.is_empty()) {
        return Err("it allows some ingress".into());
    }
    Ok(spec)
}

/// Whether `policy` is one that takes the network from the pods labelled for
/// it, and from no others: it selects that label alone, is a policy of both
/// directions, and allows nothing in either.
fn takes_the_network(policy: &NetworkPolicy) -> std::result::Result<(), String> {
    let spec = holds_the_labelled(policy, NO_NETWORK)?;
    if spec.egress.as_ref().is_some_and(|rules| !rules.is_empty()) {
        return Err("it allows some egress".into());
    }
    Ok(())
}

/// Whether `policy` is one that holds the pods labelled for the allowlist
/// `name` to a list: it selects that label alone, is a policy of both
/// directions, allows nothing in, and each of its egress rules names where
/// to. What the rules name is the operator's and is not judged.
///
/// A rule with no `to` allows every address on its ports, which is no list.
/// An operator who means that writes the block that says it.
fn holds_to_an_allowlist(policy: &NetworkPolicy, name: &str) -> std::result::Result<(), String> {
    let spec = holds_the_labelled(policy, &allowlist_label(name))?;
    for (n, rule) in spec.egress.iter().flatten().enumerate() {
        if rule.to.as_ref().is_none_or(Vec::is_empty) {
            return Err(format!(
                "its egress rule {} names no destination (`to`), which is every address: \
                 an allowlist says where",
                n + 1
            ));
        }
    }
    Ok(())
}

/// Whether `selector` selects a pod with `labels`. No selector, or an empty
/// one, is every pod of the namespace. An operator this does not know is
/// taken to select: what is not understood is not taken for harmless.
fn selects(selector: Option<&LabelSelector>, labels: &BTreeMap<String, String>) -> bool {
    let Some(selector) = selector else {
        return true;
    };
    let by_label = selector
        .match_labels
        .iter()
        .flatten()
        .all(|(key, value)| labels.get(key) == Some(value));
    let by_expression = selector.match_expressions.iter().flatten().all(|e| {
        let values = e.values.as_deref().unwrap_or_default();
        match (e.operator.as_str(), labels.get(&e.key)) {
            ("In", Some(value)) => values.contains(value),
            ("In", None) => false,
            ("NotIn", Some(value)) => !values.contains(value),
            ("NotIn", None) => true,
            ("Exists", value) => value.is_some(),
            ("DoesNotExist", value) => value.is_none(),
            _ => true,
        }
    });
    by_label && by_expression
}

/// The policies among `policies`, other than the one named `own`, that
/// select a pod with `labels` and allow it something: each by its name and
/// the directions it has a rule for.
///
/// A NetworkPolicy never takes away what another gives. A pod has what any
/// policy that selects it allows, so the policy that takes a task's network
/// holds it only while no other gives some back: a namespace's "every pod
/// may reach the name server" is enough. The gate does not see this (it asks
/// one address, and such a policy seldom opens that one), so it is read.
///
/// A policy's rules count in a direction it is a policy of, as the API has
/// it: the directions its `policyTypes` names, and where it names none,
/// ingress, and egress if it has egress rules. Rules written under a
/// direction the policy is not of allow nothing.
fn others_that_open(
    policies: &[NetworkPolicy],
    own: &str,
    labels: &BTreeMap<String, String>,
) -> Vec<String> {
    policies
        .iter()
        .filter(|policy| policy.metadata.name.as_deref() != Some(own))
        .filter_map(|policy| {
            let spec = policy.spec.as_ref()?;
            if !selects(spec.pod_selector.as_ref(), labels) {
                return None;
            }
            let has_egress = spec.egress.as_ref().is_some_and(|rules| !rules.is_empty());
            let has_ingress = spec.ingress.as_ref().is_some_and(|rules| !rules.is_empty());
            let is_of = |direction: &str| match &spec.policy_types {
                Some(types) => types.iter().any(|t| t == direction),
                None => direction == "Ingress" || has_egress,
            };
            let mut allows = Vec::new();
            if has_egress && is_of("Egress") {
                allows.push("egress");
            }
            if has_ingress && is_of("Ingress") {
                allows.push("ingress");
            }
            (!allows.is_empty()).then(|| {
                format!(
                    "'{}' ({})",
                    policy.metadata.name.as_deref().unwrap_or_default(),
                    allows.join(" and ")
                )
            })
        })
        .collect()
}

/// What a namespace's policies make of a pod with `labels` whose task asked
/// to be held: the policy that holds it is among them and is the policy it is
/// taken to be, and no other gives it more. `Err` says which is not so.
fn held_by(
    policies: &[NetworkPolicy],
    labels: &BTreeMap<String, String>,
    asked: Asked<'_>,
) -> std::result::Result<(), String> {
    let name = asked.policy();
    let Some(own) = policies
        .iter()
        .find(|policy| policy.metadata.name.as_deref() == Some(name.as_str()))
    else {
        return Err(match asked {
            Asked::Nothing => format!(
                "the NetworkPolicy '{name}' is not there. The policy, as \
                 `kubectl apply -f -` takes it:\n{}",
                asked.manifest()
            ),
            Asked::Allowlist(_) => format!(
                "the NetworkPolicy '{name}' is not there. The chart makes it from \
                 engine.taskNetworkAllowlists. Its shape, with a rule to replace with your \
                 own:\n{}",
                asked.manifest()
            ),
        });
    };
    asked.held_as_meant(own).map_err(|why| match asked {
        Asked::Nothing => format!(
            "the NetworkPolicy '{name}' is not one that takes a pod's network: {why}. It \
             must be:\n{}",
            asked.manifest()
        ),
        Asked::Allowlist(_) => format!(
            "the NetworkPolicy '{name}' is not one that holds a pod to a list: {why}. Its \
             shape, with a rule to replace with your own:\n{}",
            asked.manifest()
        ),
    })?;
    let others = others_that_open(policies, &name, labels);
    if !others.is_empty() {
        let would = match asked {
            Asked::Nothing => "a task labelled for no network would have that",
            Asked::Allowlist(_) => "a task held to this allowlist would have that as well",
        };
        return Err(format!(
            "another NetworkPolicy selects such a task's pod and allows it something: {}. A \
             pod has what any policy that selects it allows, so {would}. Keep each such \
             policy from the pods labelled {NETWORK_LABEL}, by adding \
             `{{key: {NETWORK_LABEL}, operator: DoesNotExist}}` to its podSelector's \
             matchExpressions",
            others.join(", ")
        ));
    }
    Ok(())
}

/// The labels a pod carries when its task asked to be held, but for the
/// values that are a task's own: what the engine has to judge the
/// namespace's policies by before there is a task. A policy that selected by
/// one task's id would be missed here, and is read again when that task is
/// dispatched.
fn labels_of_a_held_pod(asked: Asked<'_>) -> BTreeMap<String, String> {
    let mut labels = crate::executor::TaskIdentity {
        task_id: "a-task".into(),
        run_id: "a-run".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    }
    .labels();
    labels.insert("app".into(), "module-54-scheduler".into());
    labels.insert(NETWORK_LABEL.into(), asked.label());
    labels
}

/// What of the network `ctx`'s task asked to be held to, where it asked.
fn asked_network(ctx: &ExecContext) -> Option<Asked<'_>> {
    use dagron_core::isolation::Network;
    match ctx.isolation.as_ref()?.network.as_ref()? {
        Network::None => Some(Asked::Nothing),
        Network::Allow(name) => Some(Asked::Allowlist(name)),
        Network::Default => None,
    }
}

/// The Kubernetes version a service needs, major and minor: the first in
/// which an init container that restarts (a sidecar) is on unless a cluster
/// turns it off. An older API server drops the field it does not know, and
/// the service would be an init container that never ends, with its task
/// behind it.
pub const SIDECARS_FROM: (u32, u32) = (1, 29);

/// What the task's own container is named in its pod.
const TASK_CONTAINER: &str = "task";

/// What a service's container is named in its task's pod. Under a prefix, so
/// that no service's name is the task's container's or an init container's
/// of this executor's own.
const SERVICE_PREFIX: &str = "svc-";

/// How long one run of a service's `ready` may take, in seconds.
const READY_PROBE_SECS: u64 = 10;

/// How many of a service's last lines go into why it was not ready.
const SERVICE_SAID_LINES: i64 = 20;

/// How much longer than its `ready_timeout_secs` the kubelet gives a
/// service's startup probe. This executor times a service itself and says
/// which was not ready and for how long; the kubelet's own count, which ends
/// in a stopped container and no reason, is the net under that.
const READY_BACKSTOP_SECS: u64 = 10;

/// The grace the pod of a task with services is deleted with once the task
/// has ended: a service is there for its task, and is given none to speak of.
const SERVICE_STOP_GRACE_SECS: u32 = 1;

/// A version as an API server gives it, major and minor: `"1"` and
/// `"31"`, or `"31+"` on a vendor's build. `None` where either is not a
/// number.
fn server_version(major: &str, minor: &str) -> Option<(u32, u32)> {
    let digits = |s: &str| {
        let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        s[..end].parse::<u32>().ok()
    };
    Some((digits(major)?, digits(minor)?))
}

/// Why a task with services is not run on a cluster of this version, where
/// it is older than [`SIDECARS_FROM`]. A cluster that would not say its
/// version is not refused.
fn too_old_for_services(server: Option<(u32, u32)>) -> Option<String> {
    let (major, minor) = server.filter(|version| *version < SIDECARS_FROM)?;
    Some(format!(
        "this task has services, and this cluster is Kubernetes {major}.{minor}: a service is \
         a sidecar container of the task's pod, which Kubernetes has from {}.{}",
        SIDECARS_FROM.0, SIDECARS_FROM.1
    ))
}

/// A service's container in its task's pod: an init container that restarts,
/// which Kubernetes starts before the containers after it and keeps for as
/// long as the pod's own run. Its `ready` is its startup probe, so the next
/// container starts once it has succeeded, and the kubelet stops and starts
/// the service again where it has not within its `ready_timeout_secs`.
fn service_container(
    service: &ServiceSpec,
    pull: Option<dagron_core::dag::ImagePullPolicy>,
    told: &[dagron_core::dag::EnvVar],
) -> serde_json::Value {
    let mut container = serde_json::json!({
        "name": format!("{SERVICE_PREFIX}{}", service.name),
        "image": service.image,
        "restartPolicy": "Always",
    });
    if !service.args.is_empty() {
        container["args"] = serde_json::json!(service.args);
    }
    // What the task's allowlist tells it of a proxy, before the service's
    // own variables and never over one: the pod's policy holds it too.
    let env: Vec<serde_json::Value> = told
        .iter()
        .filter(|t| !service.env.iter().any(|e| e.name == t.name))
        .chain(service.env.iter())
        .map(|e| serde_json::json!({ "name": e.name, "value": e.value }))
        .collect();
    if !env.is_empty() {
        container["env"] = serde_json::Value::Array(env);
    }
    if let Some(policy) = pull {
        container["imagePullPolicy"] = serde_json::json!(policy.as_kubernetes());
    }
    if let Some(ready) = &service.ready {
        // Asked each second. Given up by the kubelet a little after this
        // executor has given it up itself ([`READY_BACKSTOP_SECS`]).
        container["startupProbe"] = serde_json::json!({
            "exec": { "command": ready.argv() },
            "periodSeconds": 1,
            "timeoutSeconds": READY_PROBE_SECS,
            "failureThreshold": service.ready_timeout() + READY_BACKSTOP_SECS,
        });
    }
    if let Some(resources) = &service.resources {
        container["resources"] = serde_json::json!({
            "requests": resources.requests,
            "limits": resources.limits,
        });
    }
    container
}

/// The state of the task's own container, by its name: a pod may come to
/// have another container that is not an init container, and that one's
/// start or end is not the task's.
fn task_state(
    status: &k8s_openapi::api::core::v1::PodStatus,
) -> Option<&k8s_openapi::api::core::v1::ContainerState> {
    status
        .container_statuses
        .iter()
        .flatten()
        .find(|c| c.name == TASK_CONTAINER)?
        .state
        .as_ref()
}

/// Whether the task's own container has started: it runs, or has ended.
fn task_started(status: &k8s_openapi::api::core::v1::PodStatus) -> bool {
    task_state(status).is_some_and(|s| s.running.is_some() || s.terminated.is_some())
}

/// What the task's own container exited with, once it has.
fn task_exit(status: &k8s_openapi::api::core::v1::PodStatus) -> Option<i32> {
    task_state(status)?.terminated.as_ref().map(|t| t.exit_code)
}

/// A service whose container has stopped while its task has yet to start,
/// with its exit code: one that ended by itself, or that the kubelet stopped
/// because its `ready` had not succeeded in time. Either way the kubelet
/// starts it again and the pod waits, for as long as the task's timeout; the
/// attempt is failed with this instead.
///
/// `None` once the task's container has started: a service that stops
/// after that is started again beside it, and the task says for itself what
/// that cost it.
fn failed_service(status: &k8s_openapi::api::core::v1::PodStatus) -> Option<(String, i32)> {
    if task_started(status) {
        return None;
    }
    status
        .init_container_statuses
        .iter()
        .flatten()
        .find_map(|c| {
            let service = c.name.strip_prefix(SERVICE_PREFIX)?;
            let ended = |state: &Option<k8s_openapi::api::core::v1::ContainerState>| {
                state
                    .as_ref()
                    .and_then(|s| s.terminated.as_ref())
                    .map(|t| t.exit_code)
            };
            let code = ended(&c.state).or_else(|| ended(&c.last_state))?;
            Some((service.to_string(), code))
        })
}

/// The services of a pod that run and have yet to pass their startup probe:
/// started, and not ready. Those this executor times.
fn unready_services(status: &k8s_openapi::api::core::v1::PodStatus) -> Vec<&str> {
    status
        .init_container_statuses
        .iter()
        .flatten()
        .filter(|c| c.state.as_ref().is_some_and(|s| s.running.is_some()))
        .filter(|c| c.started != Some(true))
        .filter_map(|c| c.name.strip_prefix(SERVICE_PREFIX))
        .collect()
}

/// Why a service was not ready.
#[derive(Debug, PartialEq, Eq)]
enum NotReady {
    /// Its container stopped, with this exit code.
    Stopped(i32),
    /// Its `ready` had not succeeded in the seconds it had.
    TimedOut(u64),
}

/// How a pod's wait ended.
enum Ended {
    /// It reached a terminal phase, or, for a task with services, its own
    /// container ended: the status then says `Succeeded` or `Failed` by
    /// what that container exited with.
    Pod(k8s_openapi::api::core::v1::PodStatus),
    /// A service of it was not ready, and its task never started.
    Service { name: String, why: NotReady },
}

/// What is left of what a service said, for why it was not ready.
fn said_or_nothing(said: &str) -> String {
    match said.trim() {
        "" => "it said nothing".to_string(),
        said => format!("it said: {said}"),
    }
}

/// Kubernetes executor — each task runs in a freshly created one-shot pod.
pub struct KubeExecutor {
    /// Default image when a task does not specify `docker_image`.
    pub default_image: String,
    /// Namespace the task pods are created in.
    namespace: String,
    client: Client,
    /// `DAGRON_TASK_SECRET_ENV=inline`: put secret-sourced env values in the pod
    /// spec as literals (the pre-Secret behaviour). Default off: they are handed
    /// to the pod by `secretKeyRef` — see [`env_secret_object`].
    inline_secrets: bool,
    /// Which ServiceAccounts a task's `service_account:` may name. Read once at
    /// connect, denies everything by default — see [`ServiceAccountPolicy`].
    sa_policy: ServiceAccountPolicy,
    /// How a task's workspace is made, read once at connect — see
    /// [`KubeWorkspaces`].
    workspaces: KubeWorkspaces,
    /// The operator has said the cluster enforces NetworkPolicy
    /// ([`ENV_NETWORK_POLICY`]), and [`NO_NETWORK_POLICY`] was found in the
    /// namespace and read. A task that asks for no network is refused without.
    network_policy: bool,
    /// The allowlists the operator named ([`ENV_NETWORK_ALLOWLISTS`]), each
    /// one's policy read and found to be one when this executor was made.
    allowlists: Vec<String>,
    /// The proxy each of them says ([`allowlist_proxy`]), by name, as its
    /// policy said when this executor was made.
    allowlist_proxies: BTreeMap<String, String>,
    /// Whether a task may ask for a privileged container here
    /// ([`KubeExecutor::with_privileged`]).
    privileged: bool,
    /// The cluster's version, major and minor, as its API server said when
    /// this executor was made; `None` where it would not say. A task with
    /// services is refused on one older than [`SIDECARS_FROM`].
    server: Option<(u32, u32)>,
    /// The one watch of this executor's task pods, which a task that waits
    /// is told of its pod by. `None` where it is turned off
    /// ([`ENV_POD_WATCH`]): each task then asks for its own.
    watch: Option<crate::kube_watch::PodWatch>,
    /// How many times a waiting task asked the API server for its pod.
    pod_gets: std::sync::atomic::AtomicU64,
    /// What each running task's pod lends its steps' containers, by task id.
    lent: Arc<std::sync::Mutex<std::collections::HashMap<String, KubeLent>>>,
    /// Each registry's mirror, which every image of a task's pod is pulled
    /// through ([`crate::images::ENV_MIRRORS`]). The kubelet
    /// pulls, so the auth file is not read here: credentials are the pull
    /// secrets'.
    registries: crate::images::Registries,
    /// The pull secrets a trusted run's pod names
    /// ([`crate::images::ENV_PULL_SECRETS`]).
    pull_secrets: Vec<String>,
}

/// The pull secrets [`crate::images::ENV_PULL_SECRETS`] names: Secrets of
/// the task namespace, by name, comma separated.
pub fn parse_pull_secrets(v: &str) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for name in v.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let valid = name.len() <= 253
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            && name.starts_with(|c: char| c.is_ascii_alphanumeric())
            && name.ends_with(|c: char| c.is_ascii_alphanumeric());
        if !valid {
            bail!(
                "{}: {name:?} is not a Secret's name (lowercase letters, digits, '-' and '.')",
                crate::images::ENV_PULL_SECRETS
            );
        }
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    Ok(names)
}

/// What a running task's pod lends the containers of its steps
/// ([`Executor::run_step_container`]): each is an ephemeral container added
/// to the pod, with the pod's workspace volume, at the task's own place and
/// at [`STEP_WORKSPACE_DIR`], the task container's security context and
/// image pull policy. The pod's own security context, its network and its
/// NetworkPolicy hold it as they hold the pod.
#[derive(Debug, Clone)]
struct KubeLent {
    pod: String,
    workspace: String,
    security: Option<k8s_openapi::api::core::v1::SecurityContext>,
    pull: Option<String>,
    /// A run that is not trusted: an ephemeral container takes no limits, so
    /// it is given none.
    confined: bool,
    /// What the task's allowlist tells each of its containers, before what
    /// a step sets itself: its proxy, where it has one.
    env: Vec<(String, String)>,
    made: u32,
}

/// What a pod lends, for as long as its attempt runs: taken back however the
/// attempt ends, a stop that drops it included.
struct Lending {
    lent: Arc<std::sync::Mutex<std::collections::HashMap<String, KubeLent>>>,
    task: String,
}

impl Drop for Lending {
    fn drop(&mut self) {
        if let Ok(mut lent) = self.lent.lock() {
            lent.remove(&self.task);
        }
    }
}

/// Why an ephemeral container that waits will not start: the kubelet's
/// reason, where it is one that does not pass.
fn will_not_start(waiting: &k8s_openapi::api::core::v1::ContainerStateWaiting) -> Option<String> {
    let reason = waiting.reason.as_deref()?;
    matches!(
        reason,
        "ImagePullBackOff"
            | "ErrImageNeverPull"
            | "InvalidImageName"
            | "CreateContainerConfigError"
            | "CreateContainerError"
    )
    .then(|| match waiting.message.as_deref() {
        Some(message) if !message.is_empty() => format!("{reason}: {message}"),
        _ => reason.to_string(),
    })
}

/// The ephemeral container of a step, as the pod's spec has one.
fn ephemeral_container(name: &str, step: &StepContainer, lent: &KubeLent) -> serde_json::Value {
    let mut container = serde_json::json!({
        "name": name,
        "image": step.image,
        "env": lent
            .env
            .iter()
            .filter(|(told, _)| !step.env.iter().any(|(own, _)| own == told))
            .chain(step.env.iter())
            .map(|(name, value)| serde_json::json!({"name": name, "value": value}))
            .collect::<Vec<_>>(),
        "workingDir": STEP_WORKSPACE_DIR,
        "volumeMounts": [
            {"name": lent.workspace, "mountPath": crate::workspace::WORKSPACE_DIR},
            {"name": lent.workspace, "mountPath": STEP_WORKSPACE_DIR},
        ],
    });
    if let Some(entrypoint) = &step.entrypoint {
        container["command"] = serde_json::json!([entrypoint]);
    }
    if !step.args.is_empty() {
        container["args"] = serde_json::json!(step.args);
    }
    if let Some(security) = &lent.security {
        container["securityContext"] = serde_json::to_value(security).unwrap_or_default();
    }
    if let Some(pull) = &lent.pull {
        container["imagePullPolicy"] = serde_json::json!(pull);
    }
    container
}

/// Parse `DAGRON_TASK_SECRET_ENV`: `secret` (default) or `inline`. A typo is an
/// error at startup, not a silent choice between two security postures.
fn parse_secret_env_mode(v: Option<&str>) -> Result<bool> {
    match v.map(str::trim).filter(|v| !v.is_empty()) {
        None | Some("secret") => Ok(false),
        Some("inline") => Ok(true),
        Some(other) => bail!("DAGRON_TASK_SECRET_ENV must be 'secret' or 'inline', got '{other}'"),
    }
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

        let inline_secrets =
            parse_secret_env_mode(std::env::var("DAGRON_TASK_SECRET_ENV").ok().as_deref())?;
        let workspaces = KubeWorkspaces::from_env().map_err(|e| anyhow::anyhow!(e))?;
        let network_policy =
            parse_network_policy(&std::env::var(ENV_NETWORK_POLICY).unwrap_or_default())
                .map_err(|e| anyhow::anyhow!("invalid {ENV_NETWORK_POLICY}: {e}"))?;
        let allowlists =
            parse_network_allowlists(&std::env::var(ENV_NETWORK_ALLOWLISTS).unwrap_or_default())
                .map_err(|e| anyhow::anyhow!("invalid {ENV_NETWORK_ALLOWLISTS}: {e}"))?;
        let watched = parse_pod_watch(&std::env::var(ENV_POD_WATCH).unwrap_or_default())
            .map_err(|e| anyhow::anyhow!("invalid {ENV_POD_WATCH}: {e}"))?;
        let registries = crate::images::Registries::parse(
            std::env::var(crate::images::ENV_MIRRORS).ok().as_deref(),
            None,
        )?;
        let pull_secrets = parse_pull_secrets(
            &std::env::var(crate::images::ENV_PULL_SECRETS).unwrap_or_default(),
        )?;
        // The cluster's version, for a task with services. One that will not
        // say is not a reason not to start: most tasks have none.
        let server = match client.apiserver_version().await {
            Ok(info) => server_version(&info.major, &info.minor),
            Err(e) => {
                tracing::warn!(error = %e, "the cluster would not say its version; a task with services is run as if it were {}.{} or later", SIDECARS_FROM.0, SIDECARS_FROM.1);
                None
            }
        };
        Self {
            default_image: default_image.into(),
            namespace,
            client,
            inline_secrets,
            sa_policy: ServiceAccountPolicy::from_env(),
            workspaces,
            network_policy: false,
            allowlists: Vec::new(),
            allowlist_proxies: BTreeMap::new(),
            privileged: false,
            server,
            watch: None,
            pod_gets: std::sync::atomic::AtomicU64::new(0),
            lent: Arc::default(),
            registries,
            pull_secrets,
        }
        .with_pod_watch(watched)
        .with_network_policy(network_policy)
        .await?
        .with_network_allowlists(allowlists)
        .await
    }

    /// Run a task that asks to be privileged (`privileged: true`) with its
    /// container privileged, where `allow`: the operator's word, which the
    /// engine has checked is a dedicated pool's
    /// (`dagron_core::privileged::engine_refusal`). Checked here of what only
    /// the cluster says: that task pods go to nodes of their own
    /// (`DAGRON_TASK_NODE_SELECTOR`), that nothing set for every task pod
    /// contradicts privilege, and that the namespace admits a privileged pod,
    /// asked of the API server without making one (a dry run, which Pod
    /// Security admission judges as it would the pod). Any of them stops the
    /// engine here.
    pub async fn with_privileged(mut self, allow: bool) -> Result<Self> {
        self.privileged = allow;
        if !allow {
            return Ok(self);
        }
        use dagron_core::privileged::ENV;
        let hardening = PodHardening::from_env();
        if hardening.node_selector.is_empty() {
            bail!(
                "{ENV}=allow, and task pods go to any node (DAGRON_TASK_NODE_SELECTOR is unset): a \
                 privileged pod is root on its node, so a dedicated pool's task pods go to nodes \
                 of its own"
            );
        }
        let contradicted = [
            (hardening.drop_all_capabilities, "DAGRON_TASK_DROP_ALL_CAPABILITIES"),
            (hardening.seccomp_runtime_default, "DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT"),
            (hardening.runtime_class.is_some(), "DAGRON_TASK_RUNTIME_CLASS"),
        ];
        if let Some((_, name)) = contradicted.iter().find(|(set, _)| *set) {
            bail!(
                "{ENV}=allow, and {name} holds every task pod to what a privileged container \
                 cannot be held to"
            );
        }
        let probe = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": format!("dagron-privileged-check-{}", &Uuid::new_v4().simple().to_string()[..8]) },
            "spec": {
                "restartPolicy": "Never",
                "automountServiceAccountToken": false,
                "nodeSelector": hardening.node_selector.iter().cloned().collect::<BTreeMap<_, _>>(),
                "containers": [{
                    "name": TASK_CONTAINER,
                    "image": self.registries.mirrored(&self.default_image),
                    "command": ["true"],
                    "securityContext": { "privileged": true },
                }],
            },
        });
        let probe: Pod = serde_json::from_value(probe)
            .map_err(|e| anyhow::anyhow!("the privileged pod asked about: {e}"))?;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let params = PostParams {
            dry_run: true,
            ..Default::default()
        };
        pods.create(&params, &probe).await.map_err(|e| {
            anyhow::anyhow!(
                "{ENV}=allow, and namespace '{}' does not admit a privileged pod: {e}. Label it \
                 pod-security.kubernetes.io/enforce=privileged, and keep it for this pool's tasks",
                self.namespace
            )
        })?;
        tracing::info!(
            namespace = %self.namespace,
            "a task that asks to be privileged is run privileged, on this pool's nodes"
        );
        Ok(self)
    }

    /// Keep one watch of this executor's task pods, or none, in place of
    /// what [`Self::connect`] read from the environment. With none, each
    /// task asks the API server for its own pod twice a second, as every
    /// task did before there was a watch.
    pub fn with_pod_watch(mut self, on: bool) -> Self {
        self.watch = on.then(|| {
            crate::kube_watch::PodWatch::start(Api::namespaced(
                self.client.clone(),
                &self.namespace,
            ))
        });
        self
    }

    /// Whether the watch of task pods is following the namespace now.
    /// `false` where there is none.
    pub fn pod_watch_live(&self) -> bool {
        self.watch.as_ref().is_some_and(|watch| watch.live())
    }

    /// How many times a waiting task has asked the API server for its pod:
    /// each ask is one request the watch did not save.
    pub fn pod_gets(&self) -> u64 {
        self.pod_gets.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Take the operator's word that the cluster enforces NetworkPolicy, in
    /// place of what [`Self::connect`] read from the environment, and check
    /// what can be checked of it: that the namespace holds
    /// [`NO_NETWORK_POLICY`], that it is the policy it is taken to be, and
    /// that no other policy of the namespace gives a labelled pod back what
    /// that one takes ([`others_that_open`]). Any of them stops the engine
    /// here: a task that asked for no network would otherwise run labelled
    /// for a policy that is not there, or beside one that undoes it.
    ///
    /// The namespace is read again before each such task is made
    /// ([`Self::held_alone`]): policies change after an engine has started.
    pub async fn with_network_policy(mut self, enforced: bool) -> Result<Self> {
        if enforced {
            // The gate is from the image that installs dagron-ci-exec, and a
            // task that asks for no network does not start without it.
            if self.workspaces.ci_exec_image.is_none() {
                bail!(
                    "{ENV_NETWORK_POLICY}=enforced needs {}: a task that asks for no network \
                     is held by an init container from that image until its pod's network \
                     is seen to be gone",
                    crate::workspace::ENV_CI_EXEC_IMAGE
                );
            }
            let policies = self.policies().await.map_err(|e| {
                anyhow::anyhow!(
                    "{ENV_NETWORK_POLICY}=enforced, and {e}. The chart grants it with \
                     engine.taskNetworkPolicy=true, and makes the policy '{NO_NETWORK_POLICY}'"
                )
            })?;
            let asked = Asked::Nothing;
            held_by(&policies, &labels_of_a_held_pod(asked), asked).map_err(|why| {
                anyhow::anyhow!(
                    "{ENV_NETWORK_POLICY}=enforced, and in namespace '{}' {why}",
                    self.namespace
                )
            })?;
            tracing::info!(
                namespace = %self.namespace,
                policy = NO_NETWORK_POLICY,
                "a task that asks for no network is labelled for this NetworkPolicy. That the \
                 cluster's CNI enforces it is the operator's word ({ENV_NETWORK_POLICY})"
            );
        }
        self.network_policy = enforced;
        Ok(self)
    }

    /// Take the allowlists the operator named, in place of what
    /// [`Self::connect`] read from the environment, and check what can be
    /// checked of each, as [`Self::with_network_policy`] does and for its
    /// reason: that the namespace holds its policy
    /// ([`allowlist_policy_name`]), that the policy holds the pods labelled
    /// for it, and those alone, to a list, and that no other policy of the
    /// namespace gives such a pod more. Any of them stops the engine here.
    /// Call it after that one: an allowlist needs the operator's word that
    /// the cluster enforces a policy.
    pub async fn with_network_allowlists(mut self, names: Vec<String>) -> Result<Self> {
        if names.is_empty() {
            self.allowlists = names;
            return Ok(self);
        }
        if !self.network_policy {
            bail!(
                "{ENV_NETWORK_ALLOWLISTS} names allowlists ({}), and {ENV_NETWORK_POLICY} is not \
                 'enforced': an allowlist is a NetworkPolicy, and holds a task only where the \
                 cluster enforces one",
                names.join(", ")
            );
        }
        let policies = self.policies().await.map_err(|e| {
            anyhow::anyhow!(
                "{ENV_NETWORK_ALLOWLISTS} names allowlists, and {e}. The chart grants it with \
                 engine.taskNetworkPolicy=true"
            )
        })?;
        let mut proxies = BTreeMap::new();
        for name in &names {
            let asked = Asked::Allowlist(name);
            held_by(&policies, &labels_of_a_held_pod(asked), asked).map_err(|why| {
                anyhow::anyhow!(
                    "{ENV_NETWORK_ALLOWLISTS} names '{name}', and in namespace '{}' {why}",
                    self.namespace
                )
            })?;
            let own = asked.policy();
            if let Some(policy) = policies
                .iter()
                .find(|p| p.metadata.name.as_deref() == Some(own.as_str()))
            {
                if let Some(proxy) = allowlist_proxy(policy).map_err(|why| {
                    anyhow::anyhow!("{ENV_NETWORK_ALLOWLISTS} names '{name}', and {own}'s {why}")
                })? {
                    proxies.insert(name.clone(), proxy);
                }
            }
            tracing::info!(
                namespace = %self.namespace,
                policy = %asked.policy(),
                "a task that names the allowlist '{name}' is labelled for this NetworkPolicy. \
                 What it allows is the operator's"
            );
        }
        self.allowlists = names;
        self.allowlist_proxies = proxies;
        Ok(self)
    }

    /// What a task held to an allowlist with a proxy, and each container of
    /// its pod, is told of it ([`dagron_core::isolation::proxy_env`]): each
    /// container takes out what it sets itself, where its environment is made.
    fn proxy_env(&self, ctx: &ExecContext) -> Vec<dagron_core::dag::EnvVar> {
        let Some(proxy) = asked_network(ctx).and_then(|asked| match asked {
            Asked::Allowlist(name) => self.allowlist_proxies.get(name),
            Asked::Nothing => None,
        }) else {
            return Vec::new();
        };
        dagron_core::isolation::proxy_env(proxy, ctx.services.iter().map(|s| s.name.as_str()))
            .into_iter()
            .map(|(name, value)| dagron_core::dag::EnvVar {
                name,
                value,
                value_from: None,
            })
            .collect()
    }

    /// Every NetworkPolicy of the task namespace.
    async fn policies(&self) -> Result<Vec<NetworkPolicy>> {
        let api: Api<NetworkPolicy> = Api::namespaced(self.client.clone(), &self.namespace);
        let listed = timeout(
            Duration::from_secs(SWEEP_CALL_TIMEOUT_SECS),
            api.list(&ListParams::default()),
        )
        .await
        .map_err(|_| anyhow::anyhow!("no answer in {SWEEP_CALL_TIMEOUT_SECS} s"))
        .and_then(|listed| listed.map_err(anyhow::Error::from))
        .map_err(|e| {
            anyhow::anyhow!(
                "the NetworkPolicies of namespace '{}' could not be read: {e}. The engine \
                 needs get and list on `networkpolicies` there",
                self.namespace
            )
        })?;
        Ok(listed.items)
    }

    /// Whether a pod with `labels`, about to be made for a task that asked to
    /// be held, would be held by the namespace's policies as they are now:
    /// read again here, since they are the operator's and change after an
    /// engine has started. A task is refused where they would not hold it,
    /// and where they cannot be read.
    async fn held_alone(&self, labels: &BTreeMap<String, String>, asked: Asked<'_>) -> Result<()> {
        let refused = |why: String| {
            anyhow::anyhow!(
                "{}, and in namespace '{}' {why}. The task was not started",
                asked.says(),
                self.namespace
            )
        };
        let policies = self.policies().await.map_err(|e| refused(e.to_string()))?;
        held_by(&policies, labels, asked).map_err(refused)
    }

    /// How task workspaces are made, in place of what [`Self::connect`] read
    /// from the environment.
    pub fn with_workspaces(mut self, workspaces: KubeWorkspaces) -> Self {
        self.workspaces = workspaces;
        self
    }

    /// Best-effort pod deletion (cleanup); errors and a stalled request are
    /// ignored so cleanup can never hang `execute()`. With `grace` where it
    /// is given, and the pod's own otherwise.
    async fn cleanup(pods: &Api<Pod>, name: &str, grace: Option<u32>) {
        let params = DeleteParams {
            grace_period_seconds: grace,
            ..DeleteParams::default()
        };
        let _ = timeout(Duration::from_secs(10), pods.delete(name, &params)).await;
    }

    /// [`Self::cleanup`] plus the task's env Secret, if it has one. The Secret is
    /// also owned by the pod (garbage-collected with it), so this is the prompt
    /// path and the ownerReference is the net under a crash.
    ///
    /// The pod of a task with services is deleted with next to no grace
    /// ([`SERVICE_STOP_GRACE_SECS`]): by here its task has ended or never
    /// started, and its services may still run. Left to the pod's own grace,
    /// one that heeds no signal would hold the pod, and what it asks of the
    /// namespace's quota, for half a minute after every task.
    /// Lend what the pod of the task in `ctx` has to its steps' containers,
    /// as the pod is made: where it has a workspace, and the task an identity.
    fn lend(&self, ctx: &ExecContext, pod: &Pod) -> Option<Lending> {
        let task = ctx.identity.as_ref()?.task_id.clone();
        let spec = pod.spec.as_ref()?;
        let own = spec.containers.iter().find(|c| c.name == TASK_CONTAINER)?;
        let workspace = own
            .volume_mounts
            .iter()
            .flatten()
            .find(|m| m.mount_path == crate::workspace::WORKSPACE_DIR)?
            .name
            .clone();
        let lent = KubeLent {
            pod: pod.metadata.name.clone()?,
            workspace,
            // A privileged task's steps are not privileged: the container
            // is the task's own alone.
            security: own.security_context.clone().map(|mut sc| {
                sc.privileged = None;
                sc
            }),
            pull: own.image_pull_policy.clone(),
            confined: ctx.confinement.is_some(),
            env: self
                .proxy_env(ctx)
                .into_iter()
                .map(|e| (e.name, e.value))
                .collect(),
            made: 0,
        };
        self.lent.lock().unwrap().insert(task.clone(), lent);
        Some(Lending {
            lent: Arc::clone(&self.lent),
            task,
        })
    }

    /// A step container's run: an ephemeral container added to the task's
    /// pod, waited on to its time, its output read. It cannot be stopped
    /// alone: one past its time runs on until the pod is deleted, which the
    /// task's end does.
    async fn run_step(&self, task_id: &str, step: StepContainer) -> Result<StepOutcome> {
        let (name, lent) = {
            let mut all = self.lent.lock().unwrap();
            let Some(own) = all.get_mut(task_id) else {
                bail!(
                    "the task is not running on this engine, or its pod has no workspace for a \
                     step's container to share"
                );
            };
            if own.confined {
                bail!(
                    "a run that is not trusted runs no step in an image of its own on Kubernetes: \
                     such a step is an ephemeral container of the job's pod, which Kubernetes \
                     holds to no limit"
                );
            }
            own.made += 1;
            (format!("step-{}", own.made), own.clone())
        };
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        // The step's time runs from its asking: adding its container, and
        // each read of the pod after, are within it.
        let deadline = tokio::time::Instant::now() + step.timeout;
        let ran_out = || StepOutcome {
            exit_code: 124,
            output: String::new(),
            timed_out: true,
        };
        // The step's image through its registry's mirror, as the pod's are.
        let mut container = ephemeral_container(&name, &step, &lent);
        container["image"] = serde_json::json!(self.registries.mirrored(&step.image));
        let patch = serde_json::json!({
            "spec": {"ephemeralContainers": [container]}
        });
        let patched = tokio::time::timeout_at(
            deadline,
            pods.patch_ephemeral_containers(
                &lent.pod,
                &PatchParams::default(),
                &Patch::Strategic(&patch),
            ),
        )
        .await;
        let Ok(patched) = patched else {
            return Ok(ran_out());
        };
        patched.map_err(|e| {
            anyhow::anyhow!(
                "the step's container could not be added to the job's pod: {e}. The engine \
                     needs patch on pods/ephemeralcontainers in namespace '{}' (the chart grants \
                     it)",
                self.namespace
            )
        })?;
        let (exit_code, timed_out) = loop {
            if !self.lent.lock().unwrap().contains_key(task_id) {
                bail!("the task ended before its step's container did");
            }
            let now = match tokio::time::timeout_at(deadline, pods.get(&lent.pod)).await {
                Ok(read) => {
                    read.map_err(|e| anyhow::anyhow!("the job's pod could not be read: {e}"))?
                }
                Err(_) => break (124, true),
            };
            let state = now
                .status
                .as_ref()
                .and_then(|s| s.ephemeral_container_statuses.as_ref())
                .and_then(|all| all.iter().find(|c| c.name == name))
                .and_then(|c| c.state.clone());
            if let Some(state) = state {
                if let Some(ended) = &state.terminated {
                    break (i64::from(ended.exit_code), false);
                }
                if let Some(why) = state.waiting.as_ref().and_then(will_not_start) {
                    bail!("the step's container did not start: {why}");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break (124, true);
            }
            sleep(POLL_INTERVAL).await;
        };
        let params = LogParams {
            container: Some(name.clone()),
            limit_bytes: Some(i64::try_from(MAX_STEP_OUTPUT_BYTES).unwrap_or(i64::MAX)),
            ..LogParams::default()
        };
        let mut output = match timeout(Duration::from_secs(30), pods.logs(&lent.pod, &params)).await
        {
            Ok(Ok(said)) => said,
            Ok(Err(e)) => format!("[the step's container's output could not be read: {e}]\n"),
            Err(_) => "[the step's container's output was not read in time]\n".to_string(),
        };
        if output.len() >= MAX_STEP_OUTPUT_BYTES {
            output.push_str(&format!(
                "\n[the step's container said more than {} MiB: the rest is not kept]\n",
                MAX_STEP_OUTPUT_BYTES >> 20
            ));
        }
        Ok(StepOutcome {
            exit_code,
            output,
            timed_out,
        })
    }

    async fn cleanup_task(
        &self,
        pods: &Api<Pod>,
        ctx: &ExecContext,
        name: &str,
        secret: Option<&str>,
    ) {
        let grace = (!ctx.services.is_empty()).then_some(SERVICE_STOP_GRACE_SECS);
        Self::cleanup(pods, name, grace).await;
        let _ = self.cleanup_secret(secret).await;
    }

    /// Best-effort deletion of the task's env Secret, if it has one, bounded
    /// like [`Self::cleanup`] so it can never hang `execute()`. Returns whether
    /// the Secret is known to be gone: deleted, already absent (`404`), or never
    /// made. `false` is an error or a stalled request, after which it may still
    /// exist. Separate for the one path with no pod to delete: a pod refused at
    /// admission never owned the Secret, so nothing else would collect it.
    async fn cleanup_secret(&self, secret: Option<&str>) -> bool {
        let Some(sn) = secret else { return true };
        let secrets: Api<Secret> = Api::namespaced(self.client.clone(), &self.namespace);
        match timeout(Duration::from_secs(10), secrets.delete(sn, &DeleteParams::default())).await {
            Ok(Ok(_)) => true,
            Ok(Err(kube::Error::Api(s))) if s.code == 404 => true,
            _ => false,
        }
    }

    /// Stop and remove the pod `name` and its env Secret, because the claim
    /// of the attempt that made them is gone: a cancel, a run past its
    /// deadline, a lease another scheduler took.
    ///
    /// **By the names this attempt made**, and nothing else: the task's labels
    /// select a later attempt's pod too.
    ///
    /// The pod is deleted with the task's grace
    /// ([`ExecContext::stop_grace_secs`]), so its process has its signal at
    /// once and a job's step can end as its `CI_STEP_GRACE_SECS` promises. A
    /// delete is a request: the pod is `Terminating` while its grace runs. So
    /// this waits to see it gone, a little past that grace, and says so when
    /// it is not: nothing reads as stopped here while the work may still run.
    ///
    /// `Ok` once the apiserver has said the pod is gone. `Err` when that wait
    /// ran out with the pod still there or the apiserver not saying: the
    /// caller must not say the work was stopped. Whether the Secret went is
    /// not part of it: a Secret runs nothing.
    async fn stop_and_remove(
        &self,
        pods: &Api<Pod>,
        ctx: &ExecContext,
        name: &str,
        secret: Option<&str>,
    ) -> std::result::Result<(), String> {
        let grace = ctx.stop_grace_secs() + STOP_MARGIN_SECS;
        let params = DeleteParams {
            grace_period_seconds: Some(grace as u32),
            ..DeleteParams::default()
        };
        match timeout(Duration::from_secs(10), pods.delete(name, &params)).await {
            Ok(Ok(_)) => tracing::info!(pod = %name, grace, "deleting: the task's claim was lost"),
            // Not made yet, or gone already.
            Ok(Err(kube::Error::Api(s))) if s.code == 404 => {}
            Ok(Err(e)) => {
                tracing::warn!(pod = %name, error = %e, "could not delete a pod whose claim was lost")
            }
            Err(_) => tracing::warn!(pod = %name, "deleting a pod whose claim was lost timed out"),
        }
        // The pod owns the Secret and takes it along; this is for a pod that
        // never came to own it, as on every other path out of `execute`.
        let _ = self.cleanup_secret(secret).await;
        let gone = timeout(Duration::from_secs(grace + STOP_WAIT_MARGIN_SECS), async {
            loop {
                match pods.get_opt(name).await {
                    Ok(None) => break,
                    // There, and not being deleted: the attempt's create was
                    // still in flight when it was dropped, and landed after
                    // the delete above found nothing. Unlike a container made
                    // late, which is never started, a pod runs once it exists.
                    Ok(Some(pod)) if pod.metadata.deletion_timestamp.is_none() => {
                        let _ = timeout(Duration::from_secs(10), pods.delete(name, &params)).await;
                        sleep(POLL_INTERVAL).await;
                    }
                    // Terminating, or not known: look again.
                    Ok(Some(_)) | Err(_) => sleep(POLL_INTERVAL).await,
                }
            }
        })
        .await;
        // And its Secret again, for the same reason: one made after the
        // delete above, by a create that was in flight.
        let _ = self.cleanup_secret(secret).await;
        if gone.is_err() {
            tracing::warn!(
                pod = %name, grace,
                "a pod whose claim was lost is still there past its grace; its node may be \
                 unreachable, and it stays `Terminating` until the kubelet answers"
            );
            return Err(format!(
                "pod {name} was not seen gone within {} s of its delete",
                grace + STOP_WAIT_MARGIN_SECS
            ));
        }
        Ok(())
    }

    /// Delete every pod this project owns for the same task from an **earlier
    /// claim**, and see them gone, before starting this one.
    ///
    /// This is what makes `ARCHITECTURE.md`'s "the lease bounds *concurrent*
    /// execution to one holder" true here. The lease only ever bounded which
    /// scheduler owned the row: when one expired mid-execution, the reclaiming
    /// scheduler created a second pod and both ran the command at once. So did
    /// a rerun of a cancelled run.
    ///
    /// **One attempt, one workload.** A delete is a request: the pod is
    /// `Terminating`, and its process running, until its grace is out. So a
    /// predecessor is deleted with the task's grace, and then looked for. If
    /// one is still there after a moment, or the apiserver would not say, this
    /// dispatch creates nothing: `Err(Backpressure)`, which the engine answers
    /// by putting the task back in its queue with no attempt spent and trying
    /// again shortly. It used to create its pod beside the terminating one.
    ///
    /// Scoped by `managed-by` AND `task-id`, so it can never select a pod this
    /// project did not create, and never one belonging to a different task.
    /// Only what is provably older is touched ([`crate::executor::is_predecessor`]):
    /// a newer claim's pod, and one that carries no proof, are left alone and
    /// do not hold this dispatch up.
    async fn reap_predecessors(
        pods: &Api<Pod>,
        ctx: &ExecContext,
    ) -> std::result::Result<(), crate::executor::Backpressure> {
        let (Some(id), Some(selector)) =
            (&ctx.identity, ctx.identity.as_ref().and_then(|i| i.task_selector()))
        else {
            return Ok(());
        };
        let still_there = |why: String| crate::executor::Backpressure {
            reason: format!("an earlier attempt of this task may still be running: {why}"),
        };
        // The whole reap runs inside one deadline, so an unresponsive apiserver
        // costs a bounded dispatch delay rather than one per leftover pod.
        let budget = tokio::time::Instant::now() + Duration::from_secs(REAP_BUDGET_SECS);

        let found = Self::predecessors(pods, &selector, id, budget).await.map_err(&still_there)?;
        if found.is_empty() {
            return Ok(());
        }
        let params = DeleteParams {
            grace_period_seconds: Some((ctx.stop_grace_secs() + STOP_MARGIN_SECS) as u32),
            ..DeleteParams::default()
        };
        for pod_name in &found {
            tracing::warn!(
                task_id = %id.task_id, pod = %pod_name, current_fence = %id.fence,
                "deleting a pod left by an earlier attempt of this task before starting this one"
            );
            let _ = tokio::time::timeout_at(budget, pods.delete(pod_name, &params)).await;
        }

        // Gone, or this dispatch does not start. Looked for briefly here; a
        // pod that takes its grace is waited on from the queue.
        let look_until = (tokio::time::Instant::now() + Duration::from_secs(PREDECESSOR_WAIT_SECS))
            .min(budget);
        loop {
            let left = Self::predecessors(pods, &selector, id, budget).await.map_err(&still_there)?;
            if left.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= look_until {
                return Err(still_there(format!(
                    "{} of its pods still terminating ({})",
                    left.len(),
                    left.join(", ")
                )));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// The names of the pods `selector` finds that are provably from an
    /// earlier claim than `id`. `Err` says why the apiserver could not be
    /// asked: what cannot be seen cannot be called gone.
    async fn predecessors(
        pods: &Api<Pod>,
        selector: &str,
        id: &crate::executor::TaskIdentity,
        budget: tokio::time::Instant,
    ) -> std::result::Result<Vec<String>, String> {
        let listed = tokio::time::timeout_at(budget, pods.list(&ListParams::default().labels(selector)))
            .await
            .map_err(|_| "listing its pods timed out".to_string())?
            .map_err(|e| format!("its pods could not be listed: {e}"))?;
        Ok(listed.into_iter().filter_map(|pod| predecessor_name(pod, id)).collect())
    }
}

/// `pod`'s name, when its labels prove it is from an earlier claim than `id`.
///
/// Only a strictly LOWER fence is a predecessor. Equal is this dispatch's own
/// pod; higher is a newer claim that is probably running right now, and
/// deleting it would be the fence violation these labels exist to prevent —
/// see `is_predecessor`. By the fence and not the attempt: a rerun sets the
/// attempt back, and a pod a cancelled attempt left would pass for this
/// dispatch's own.
fn predecessor_name(pod: Pod, id: &crate::executor::TaskIdentity) -> Option<String> {
    let labels = pod.metadata.labels.as_ref();
    let label = |key: &str| labels.and_then(|l| l.get(key)).map(String::as_str);
    crate::executor::is_predecessor(
        label(crate::executor::LABEL_FENCE),
        label(crate::executor::LABEL_ATTEMPT),
        id,
    )
    .then_some(pod.metadata.name)
    .flatten()
}

#[async_trait]
impl Executor for KubeExecutor {
    async fn run_step_container(&self, task_id: &str, step: StepContainer) -> Result<StepOutcome> {
        self.run_step(task_id, step).await
    }

    /// Every pod in this namespace carrying this installation's labels and old
    /// enough to judge.
    ///
    /// Selects on installation as well as `managed-by`, so a second dagron
    /// sharing this namespace is invisible here rather than being mistaken for
    /// an orphan. A pod missing `task-id` is skipped: `labels()` refuses to
    /// label partially, so such a pod is not one this project made, and
    /// deleting what we cannot identify is the failure mode the whole label
    /// scheme exists to avoid.
    async fn list_orphan_candidates(
        &self,
        scope: &crate::executor::OrphanScope<'_>,
    ) -> Result<Vec<crate::executor::ManagedWorkload>> {
        use crate::executor::{LABEL_INSTALLATION, LABEL_MANAGED_BY, MANAGED_BY};
        let Some(installation) = crate::executor::label_value(scope.installation) else {
            bail!(
                "DAGRON_INSTALLATION={:?} is not usable as a label value, so the fleet sweep \
                 cannot scope itself and will not run. Use 1-63 alphanumerics, '-', '_' or '.', \
                 starting and ending alphanumeric.",
                scope.installation
            );
        };
        let selector = format!(
            "{LABEL_MANAGED_BY}={MANAGED_BY},{LABEL_INSTALLATION}={installation}"
        );
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let list = timeout(
            Duration::from_secs(SWEEP_CALL_TIMEOUT_SECS),
            pods.list(&ListParams::default().labels(&selector)),
        )
        .await
        .map_err(|_| anyhow::anyhow!("listing pods for the fleet sweep timed out"))??;

        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().saturating_sub(scope.min_age.as_secs()) as i64)
            .unwrap_or(0);

        let mut out = Vec::new();
        for pod in list {
            let Some(name) = pod.metadata.name.clone() else { continue };
            // Young pods are not judged at all — see `OrphanScope::min_age`.
            // A pod whose creation timestamp is missing is treated as young,
            // because the alternative is judging a pod whose age is unknown.
            let born = pod.metadata.creation_timestamp.as_ref().map(|t| t.0.as_second());
            if born.is_none_or(|b| b > cutoff) {
                continue;
            }
            // The apiserver stamps a uid on every object it creates, so a
            // listed pod without one is not something it handed back normally.
            // Without it the delete cannot be bound to this exact pod, and a
            // sweep that cannot address what it judged does not delete.
            let Some(uid) = pod.metadata.uid.clone() else { continue };
            let labels = pod.metadata.labels.unwrap_or_default();
            // Refused here, not filtered: the selector constrains only the
            // labels it names and says nothing about the rest — see
            // `from_labels`.
            let Some(w) =
                crate::executor::ManagedWorkload::from_labels(name, Some(uid), |k| labels.get(k))
            else {
                continue;
            };
            out.push(w);
        }
        Ok(out)
    }

    /// Delete one pod the sweep judged leftover — that pod, not whatever holds
    /// its name by the time the request lands.
    ///
    /// The name comes from the listing rather than a re-derived selector, which
    /// closes the easy half of the problem. The other half is that a name
    /// addresses a slot, not an occupant, and a sweep is spread across time: it
    /// lists, asks the datastore, then deletes. Binding the request to the
    /// listed pod's **uid** makes the delete a compare-and-swap on identity —
    /// the apiserver answers `409` and does nothing if the name now resolves to
    /// some other object, so the guarantee holds without depending on how pod
    /// names happen to be minted.
    ///
    /// Deletion is a request, not an act: the pod enters `Terminating` and its
    /// grace period runs before the container stops. `Ok(())` therefore means
    /// the apiserver accepted the request, not that the workload is gone.
    async fn delete_workload(&self, w: &crate::executor::ManagedWorkload) -> Result<()> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let params = DeleteParams {
            preconditions: w.uid.clone().map(|uid| kube::api::Preconditions {
                uid: Some(uid),
                resource_version: None,
            }),
            ..Default::default()
        };
        let res = timeout(
            Duration::from_secs(SWEEP_CALL_TIMEOUT_SECS),
            pods.delete(&w.handle, &params),
        )
        .await
        .map_err(|_| anyhow::anyhow!("deleting pod '{}' timed out", w.handle))?;
        match res {
            Ok(_) => Ok(()),
            // The precondition doing its job. The name still resolves, but not
            // to the pod the sweep judged, so nothing was deleted — which is
            // the right outcome, reported rather than swallowed because the
            // sweep's contract is that every delete either happened or is
            // explained.
            Err(kube::Error::Api(s)) if s.code == 409 => Err(anyhow::anyhow!(
                "pod '{}' is no longer the pod the sweep judged, so it was left alone",
                w.handle
            )),
            Err(e) => Err(e.into()),
        }
    }

    /// What every dispatch does before it creates anything, and nothing more:
    /// the task's pods from an earlier claim are deleted with its grace and
    /// seen gone ([`Self::reap_predecessors`]). A pod takes its env Secret
    /// with it.
    async fn teardown(&self, ctx: &ExecContext) -> Result<()> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        Self::reap_predecessors(&pods, ctx).await.map_err(anyhow::Error::new)
    }

    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        if ctx.command.is_empty() {
            bail!("empty command");
        }
        // A privileged pod where the operator has not said one may be made:
        // the engine refuses such a task before dispatch, and this is the
        // same answer for any other caller.
        if ctx.privileged {
            if let Some(why) =
                dagron_core::privileged::task_refusal(self.privileged, ctx.isolation.as_ref())
            {
                bail!("{why}");
            }
        }
        let secs = crate::executor::effective_timeout_secs(ctx.timeout_secs);
        let image = ctx.docker_image.as_deref().unwrap_or(&self.default_image);
        // Short unique pod name (DNS-1123 label: lowercase alphanumeric + '-').
        let name = format!("sched-{}", Uuid::new_v4().simple());
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        // The watch is told to keep what it sees of this pod, from before
        // the pod is made: nothing of it is then missed. Forgotten when this
        // returns, by whichever way.
        let mut following = self.watch.as_ref().map(|watch| watch.follow(&name));

        // Before creating: delete anything an earlier attempt of this same
        // task left, and see it gone. Without this the lease bounds the row,
        // not the work. While any of it is still there this dispatch creates
        // nothing, and is handed back to be tried again.
        Self::reap_predecessors(&pods, ctx).await.map_err(anyhow::Error::new)?;

        // Reject a disallowed ServiceAccount before creating anything — but after
        // reaping, so a retry whose ServiceAccount was removed from the allow-list
        // mid-flight still cleans up its predecessor rather than leaving it running
        // under the now-disallowed account. The build below checks it too (so every
        // pod-construction path is covered); doing it here as well keeps a task with
        // secret-sourced env from having its Secret created and then only
        // best-effort cleaned up, which a failed cleanup would leave stored.
        self.sa_policy.check(ctx.service_account.as_deref())?;
        // Likewise a task that asked for no network where nothing would take
        // it away. The engine refuses it before dispatch; this is the same
        // answer for any other caller.
        if let Some(asked) = asked_network(ctx) {
            if !self.network_policy {
                bail!(
                    "{}, and this engine has not been told its cluster enforces NetworkPolicy \
                     ({ENV_NETWORK_POLICY}=enforced)",
                    asked.says()
                );
            }
            // An allowlist the operator did not name is one whose policy
            // nobody read as the engine started.
            if let Asked::Allowlist(name) = asked {
                if !self.allowlists.iter().any(|known| known == name) {
                    bail!(
                        "{}, and this engine was not told of an allowlist of that name: \
                         {ENV_NETWORK_ALLOWLISTS} names {}",
                        asked.says(),
                        if self.allowlists.is_empty() {
                            "none".to_string()
                        } else {
                            self.allowlists.join(", ")
                        }
                    );
                }
            }
            // A NetworkPolicy holds a pod, and a checkout is fetched by an
            // init container of the task's own pod. Under an allowlist it is
            // fetched if the list allows the forge, which is the list's to
            // say; with no network there is nothing to fetch it over.
            if asked == Asked::Nothing && crate::checkout::of(ctx)?.is_some() {
                bail!(
                    "this task asked for no network (isolation.network: none) and has a \
                     checkout. On Kubernetes a checkout is fetched by an init container of \
                     the task's own pod, and the policy that takes the task's network takes \
                     that container's too: there would be nothing to fetch it over. Drop one \
                     of the two, or run the task on the Docker executor, which fetches a \
                     checkout from a container of its own"
                );
            }
            if asked == Asked::Nothing && !crate::checkout::actions_of(ctx)?.is_empty() {
                bail!(
                    "this task asked for no network (isolation.network: none) and uses \
                     actions. On Kubernetes they are fetched by an init container of the \
                     task's own pod, which the policy that takes the task's network holds \
                     too. Drop one of the two, or run the task on the Docker executor"
                );
            }
        }
        // A Docker daemon, which the engine refuses on this executor before
        // it gets here.
        if ctx.services.iter().any(|s| s.daemon) {
            if let Some(why) = dagron_core::docker_daemon::refusal(
                dagron_core::isolation::ExecutorKind::Kubernetes,
                None,
            ) {
                bail!(why);
            }
        }
        // Likewise services on a cluster that has no such container.
        if !ctx.services.is_empty() {
            if let Some(why) = too_old_for_services(self.server) {
                bail!(why);
            }
        }
        // Likewise a checkout this engine has nothing to fetch with, and
        // actions, which are fetched with the same image.
        if crate::checkout::of(ctx)?.is_some() && self.workspaces.checkout_image.is_none() {
            bail!(
                "this task's workspace has a checkout, and this engine has no {} \
                 (an image with git and sh) to fetch it with",
                crate::checkout::ENV_CHECKOUT_IMAGE
            );
        }
        if !crate::checkout::actions_of(ctx)?.is_empty() && self.workspaces.checkout_image.is_none()
        {
            bail!(
                "this task uses actions, and this engine has no {} (an image with git and \
                 sh) to fetch them with",
                crate::checkout::ENV_CHECKOUT_IMAGE
            );
        }

        // Secret-sourced env vars (`value_from`, resolved by the engine just
        // before dispatch) go to the pod by reference, so the plaintext is never
        // in the Pod spec that `get pods` / `describe` / audit logs show. The
        // Secret is created FIRST: a pod whose secretKeyRef does not exist yet
        // waits in CreateContainerConfigError on the kubelet's retry backoff.
        // The checkout's credential goes the same way, to its container alone.
        let secret_name = (!self.inline_secrets
            && (has_secret_env(ctx)
                || checkout_header(ctx).is_some()
                || !crate::checkout::action_headers(ctx).is_empty()))
        .then(|| format!("{name}-env"));
        let attempt = async {
            if let Some(sn) = &secret_name {
                let secrets: Api<Secret> = Api::namespaced(self.client.clone(), &self.namespace);
                secrets.create(&PostParams::default(), &env_secret_object(sn, ctx)?).await.map_err(|e| {
                    anyhow::anyhow!(
                        "create Secret '{sn}' for the task's secret env vars: {e}. The engine needs \
                         create, patch and delete on `secrets` in namespace '{}' (the chart grants them); \
                         or set DAGRON_TASK_SECRET_ENV=inline to keep secret values in the pod spec, \
                         readable by anyone who can get pods",
                        self.namespace
                    )
                })?;
            }

            let mut pod = match build_pod_told(
                &name,
                image,
                &ctx.command,
                ctx,
                secret_name.as_deref(),
                &self.sa_policy,
                &self.workspaces,
                &self.proxy_env(ctx),
            ) {
                Ok(p) => p,
                Err(e) => {
                    self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                        .await;
                    return Err(e);
                }
            };
            through_registries(&mut pod, ctx, &self.registries, &self.pull_secrets);
            // The namespace's policies, as they are now, against the labels
            // this pod will carry: it is not made where they would not hold it.
            if let Some(asked) = asked_network(ctx) {
                let labels = pod.metadata.labels.clone().unwrap_or_default();
                if let Err(e) = self.held_alone(&labels, asked).await {
                    self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                        .await;
                    return Err(e);
                }
            }
            let created = match pods.create(&PostParams::default(), &pod).await {
                Ok(p) => p,
                Err(e) => {
                    // A full namespace quota is the one refusal reported as
                    // back-pressure. Admission stored no Pod, so there is none to
                    // delete and nothing ran: the engine can have the task wait for
                    // room instead of spending an attempt, and decides whether it
                    // does. Every other refusal takes the failure path below: pod
                    // and Secret are deleted and the error is returned.
                    match quota_refusal(&e, &name) {
                        Some(QuotaRefusal::Full(reason)) => {
                            // No pod owns the Secret, so this delete is all that
                            // removes it. It is tried twice, and a Secret that may
                            // have outlived both is said out loud: it holds the
                            // task's secret values and nothing else collects it.
                            if !self.cleanup_secret(secret_name.as_deref()).await
                                && !self.cleanup_secret(secret_name.as_deref()).await
                            {
                                tracing::warn!(
                                    secret = secret_name.as_deref().unwrap_or_default(),
                                    namespace = %self.namespace,
                                    "env Secret of a pod refused by quota could not be deleted and no pod owns it — it may stay in the namespace until deleted by hand"
                                );
                            }
                            return Err(anyhow::Error::new(crate::executor::Backpressure { reason }));
                        }
                        Some(verdict) => {
                            tracing::warn!(pod = %name, verdict = ?verdict, "quota refusal not treated as back-pressure — taking the failure path");
                        }
                        None => {
                            if let Some((reason, message)) = unrecognised_quota_refusal(&e) {
                                tracing::warn!(pod = %name, reason = %reason, message = %message, "quota refusal not treated as back-pressure — not the 403 Forbidden sentence expected for this pod; taking the failure path");
                            }
                        }
                    }
                    self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                        .await;
                    return Err(anyhow::anyhow!("create pod '{name}': {e}"));
                }
            };
            if let (Some(sn), Some(uid)) = (&secret_name, created.metadata.uid.as_deref()) {
                // Own the Secret by the pod so Kubernetes deletes it with the pod even
                // if this process dies before the explicit delete below. A failure is
                // not fatal: the explicit delete still runs on every path out of here.
                let secrets: Api<Secret> = Api::namespaced(self.client.clone(), &self.namespace);
                let patch = serde_json::json!({"metadata": {"ownerReferences": [
                    {"apiVersion": "v1", "kind": "Pod", "name": name, "uid": uid}
                ]}});
                if let Err(e) = secrets.patch(sn, &PatchParams::default(), &Patch::Merge(&patch)).await {
                    tracing::warn!(secret = %sn, error = %e, "could not attach the env Secret to its pod; it will be deleted explicitly");
                }
            }
            // What its steps' containers are lent while it runs.
            let _lending = self.lend(ctx, &created);

            // ── Wait for a terminal phase, bounded by the task timeout ───────────
            // And the task's log followed meanwhile, from when its container
            // has started, as it writes it: what the kubelet
            // rotates away later, or a pod that is evicted takes with it, is
            // in the log already.
            let (started, started_rx) = tokio::sync::watch::channel(false);
            let (over, over_rx) = tokio::sync::watch::channel(false);
            let mut stamped = crate::kube_log::Stamped::default();
            let sink = ctx.log_sink.as_ref();
            let mut kept = crate::executor::Gathered::new(sink);
            let mut log_follow = Box::pin(crate::kube_log::follow(
                &pods,
                &name,
                TASK_CONTAINER,
                started_rx,
                over_rx,
                &mut stamped,
                |whole| kept.push(&whole),
            ));
            let mut waiting = Box::pin(timeout(Duration::from_secs(secs), async {
                // When each service that runs and is not ready yet was first
                // seen so.
                let mut unready: std::collections::HashMap<String, tokio::time::Instant> =
                    std::collections::HashMap::new();
                loop {
                    // The pod as the watch has it, where it has it. Where it
                    // has not (there is no watch, it is not following, it
                    // has seen nothing of this pod yet, or the pod went),
                    // the API server is asked, as it always was.
                    let told = following.as_mut().and_then(|pod| pod.status());
                    let watched = told.is_some();
                    let status = match told {
                        Some(status) => status,
                        None => {
                            self.pod_gets
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            pods.get(&name)
                                .await
                                .map_err(|e| anyhow::anyhow!("poll pod '{name}': {e}"))?
                                .status
                        }
                    };
                    if let Some(mut status) = status {
                        // Before the phase: a task that is over when it is
                        // first seen has a log to read too.
                        if task_started(&status) {
                            started.send_replace(true);
                        }
                        if matches!(status.phase.as_deref(), Some("Succeeded" | "Failed")) {
                            return Ok::<_, anyhow::Error>(Ended::Pod(status));
                        }
                        // A pod with a step's container still running is not
                        // over when its task is: the task has ended when its
                        // own container has, and the pod's delete ends the
                        // rest.
                        if status
                            .ephemeral_container_statuses
                            .as_ref()
                            .is_some_and(|steps| !steps.is_empty())
                        {
                            if let Some(code) = task_exit(&status) {
                                let phase = if code == 0 { "Succeeded" } else { "Failed" };
                                status.phase = Some(phase.to_string());
                                return Ok(Ended::Pod(status));
                            }
                        }
                        if !ctx.services.is_empty() {
                            // The task has ended when its own container has.
                            // Its pod has not, until its services have
                            // stopped, and is not waited for.
                            if let Some(code) = task_exit(&status) {
                                let phase = if code == 0 { "Succeeded" } else { "Failed" };
                                status.phase = Some(phase.to_string());
                                return Ok(Ended::Pod(status));
                            }
                            if let Some((name, code)) = failed_service(&status) {
                                return Ok(Ended::Service {
                                    name,
                                    why: NotReady::Stopped(code),
                                });
                            }
                            let now = tokio::time::Instant::now();
                            let waiting = unready_services(&status);
                            unready.retain(|name, _| waiting.contains(&name.as_str()));
                            for service in waiting {
                                let since = *unready.entry(service.to_string()).or_insert(now);
                                let allowed =
                                    ctx.services.iter().find(|s| s.name == service).map_or(
                                        dagron_core::service::DEFAULT_READY_TIMEOUT_SECS,
                                        |s| s.ready_timeout(),
                                    );
                                if now.duration_since(since) >= Duration::from_secs(allowed) {
                                    return Ok(Ended::Service {
                                        name: service.to_string(),
                                        why: NotReady::TimedOut(allowed),
                                    });
                                }
                            }
                        }
                    }
                    match following.as_mut() {
                        // Told of by the watch: wait for the pod to change.
                        // A task with services looks twice a second all the
                        // same, at what it was last told, for a service that
                        // has been unready too long: no request is made.
                        Some(pod) if watched => {
                            let at_most = if ctx.services.is_empty() {
                                WATCHED_INTERVAL
                            } else {
                                POLL_INTERVAL
                            };
                            pod.changed(at_most).await;
                        }
                        // Not told of yet, or no longer: ask again shortly,
                        // or sooner if the watch has something.
                        Some(pod) => pod.changed(POLL_INTERVAL).await,
                        None => sleep(POLL_INTERVAL).await,
                    }
                }
            }));
            let mut followed = None;
            // The wait first: a log that comes without a pause does not hold
            // the task past its deadline.
            let waited = loop {
                tokio::select! {
                    biased;
                    waited = &mut waiting => break waited,
                    whole = &mut log_follow, if followed.is_none() => followed = Some(whole),
                }
            };
            drop(waiting);
            over.send_replace(true);
            // A pod that is over: its task's log to its end. One that is not
            // (out of time, or no longer seen, or a service of it failed) is
            // not followed further: its log is read once more as far as the
            // kubelet has it, and no further.
            let over_by_itself = matches!(waited, Ok(Ok(Ended::Pod(_))));
            let mut to_end = followed.unwrap_or(false);
            if followed.is_none() && over_by_itself {
                to_end = timeout(crate::executor::FOLLOW_DRAIN, &mut log_follow)
                    .await
                    .unwrap_or(false);
            }
            drop(log_follow);
            if !over_by_itself && *started.borrow() {
                to_end = timeout(
                    crate::kube_log::LAST_READ,
                    crate::kube_log::read_now(
                        &pods,
                        &name,
                        TASK_CONTAINER,
                        &mut stamped,
                        |whole| kept.push(&whole),
                    ),
                )
                .await
                .unwrap_or(false);
            }
            // Read short of its end: what is held of a line stays as read.
            if !to_end {
                stamped.cut();
            }
            // A last line that never got its newline, and the log sealed with
            // what it was sent, and given its end where it is past its cap:
            // whichever way the wait ended.
            if let Some(left) = stamped.rest() {
                kept.push(&left);
            }
            kept.seal();

            let status = match waited {
                Ok(Ok(Ended::Pod(status))) => status,
                // A service that was not ready: the task never started. In
                // the service's own words, the last it wrote before it
                // stopped, which the task's empty log would not say.
                Ok(Ok(Ended::Service { name: service, why })) => {
                    let container = format!("{SERVICE_PREFIX}{service}");
                    let mut said = String::new();
                    // The container that stopped, where the kubelet has
                    // started another since; else the one that is there.
                    for previous in [true, false] {
                        let params = LogParams {
                            container: Some(container.clone()),
                            previous,
                            tail_lines: Some(SERVICE_SAID_LINES),
                            ..LogParams::default()
                        };
                        if let Ok(Ok(logs)) =
                            timeout(Duration::from_secs(10), pods.logs(&name, &params)).await
                        {
                            if !logs.trim().is_empty() {
                                said = logs;
                                break;
                            }
                        }
                    }
                    self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                        .await;
                    match why {
                        NotReady::Stopped(code) => bail!(
                            "the service '{service}' stopped before it was ready (its \
                             container exited {code}): {}",
                            said_or_nothing(&said)
                        ),
                        NotReady::TimedOut(allowed) => bail!(
                            "the service '{service}' was not ready within {allowed} s: its \
                             `ready` had not succeeded; {}",
                            said_or_nothing(&said)
                        ),
                    }
                }
                Ok(Err(e)) => {
                    self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                        .await;
                    return Err(e);
                }
                Err(_) => {
                    self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                        .await;
                    // Typed TimeoutError so the worker can gate retry-on-timeout (#24).
                    return Err(anyhow::Error::new(crate::executor::TimeoutError { secs }));
                }
            };

            // An init container that failed (the one installing dagron-ci-exec)
            // means the task never started: say so in the init container's words,
            // which the task's own empty log would not.
            if let Some((init, code)) = failed_init_container(&status) {
                let params = LogParams { container: Some(init.clone()), ..LogParams::default() };
                let said = match timeout(Duration::from_secs(10), pods.logs(&name, &params)).await {
                    Ok(Ok(logs)) => logs,
                    _ => String::new(),
                };
                self.cleanup_task(&pods, ctx, &name, secret_name.as_deref())
                    .await;
                if let Some(asked) = asked_network(ctx).filter(|asked| init == asked.gate()) {
                    bail!(
                        "{}",
                        gate_failure(&said, code, self.workspaces.ci_exec_image.as_deref(), asked)
                    );
                }
                if init == crate::checkout::CONTAINER_NAME {
                    if let Some(c) = crate::checkout::of(ctx)? {
                        bail!(
                            "the checkout of {} at {} failed (exited {code}): {}",
                            c.repository,
                            c.commit,
                            said.trim()
                        );
                    }
                }
                if init == crate::checkout::ACTIONS_CONTAINER_NAME {
                    bail!(
                        "the fetch of the task's actions failed (exited {code}): {}",
                        said.trim()
                    );
                }
                bail!("the init container {init} exited {code}: {}", said.trim());
            }
            let stopped = stopped_pod(&status);
            let phase = status.phase.unwrap_or_default();

            // ── Delete the pod ──────────────────────────────────────────────────
            // Its output is its log, followed to its end above.
            self.cleanup_task(&pods, ctx, &name, secret_name.as_deref()).await;
            // Why it stopped, where the kubelet or the cluster says: in its log
            // too, which is what is read of a task once it has one. After the
            // seal, which would write over it.
            if let (Some(sink), Some(why)) = (sink, &stopped) {
                sink.note(&format!("{why}\n"));
            }
            let (output, dropped) = kept.output();
            let output = match stopped {
                Some(why) if output.is_empty() || output.ends_with('\n') => format!("{output}{why}\n"),
                Some(why) => format!("{output}\n{why}\n"),
                None => output,
            };

            Ok(ExecOutput {
                success: phase == "Succeeded",
                output,
                dropped,
            })
        };
        tokio::select! {
            // The stop first: one asked for before anything was made makes
            // nothing.
            biased;
            _ = ctx.stop.requested() => {
                match self.stop_and_remove(&pods, ctx, &name, secret_name.as_deref()).await {
                    Ok(()) => Err(anyhow::Error::new(crate::executor::ClaimLost)),
                    Err(why) => Err(crate::executor::ClaimLost::unseen(why)),
                }
            }
            ran = attempt => ran,
        }
    }
}

/// The first init container that exited non-zero, with its code. Not a
/// service's: the kubelet stops each when the task's container has ended,
/// and what one exits with then says nothing of whether the task started.
fn failed_init_container(status: &k8s_openapi::api::core::v1::PodStatus) -> Option<(String, i32)> {
    status
        .init_container_statuses
        .iter()
        .flatten()
        .filter(|c| !c.name.starts_with(SERVICE_PREFIX))
        .find_map(|c| {
            let code = c.state.as_ref()?.terminated.as_ref()?.exit_code;
            (code != 0).then(|| (c.name.clone(), code))
        })
}

/// Why a pod failed that its node stopped, when its node did: evicted (a
/// workspace past its `size:`, a node out of disk or memory), past its
/// deadline, or refused at admission. Only the pod's status says so: an
/// evicted pod's containers are gone, and with them the kubelet's log, which
/// is then its "unable to retrieve container logs"; what was followed of it
/// before is kept. A task that failed by itself leaves no reason on its pod.
fn stopped_pod(status: &k8s_openapi::api::core::v1::PodStatus) -> Option<String> {
    if status.phase.as_deref() != Some("Failed") {
        return None;
    }
    let reason = status
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty())?;
    Some(
        match status
            .message
            .as_deref()
            .map(str::trim)
            .filter(|m| !m.is_empty())
        {
            Some(message) => format!("the task's pod failed ({reason}): {message}"),
            None => format!("the task's pod failed ({reason})"),
        },
    )
}

/// How task pods are hardened, read once from the environment.
///
/// WHY THIS EXISTS: `build_pod` used to emit a Pod carrying only image, command,
/// env, resources and an optional serviceAccountName — no securityContext, no
/// runtimeClassName, no `automountServiceAccountToken`, no
/// `activeDeadlineSeconds`, no node placement. Meanwhile the customer-image
/// contract told customers that "task pods run
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
    /// `allowPrivilegeEscalation: false` on its own. Previously implied by
    /// either of the two fields above; a per-task envelope
    /// (`isolation.no_new_privileges`) can ask for it alone, so it needs its own
    /// bit. No env knob: the process-wide spelling stayed as it was.
    pub no_new_privileges: bool,
    /// `runAsNonRoot: true` without pinning a uid — the check that a task whose
    /// image happens to run as root is refused rather than quietly permitted.
    /// Set from `isolation.run_as_non_root`, or for every task by
    /// `DAGRON_TASK_RUN_AS_NON_ROOT=1` (OpenShift: the SCC gives the uid);
    /// `run_as_user > 0` implies it too.
    pub run_as_non_root: bool,
    /// `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT=1`
    pub seccomp_runtime_default: bool,
    /// `DAGRON_TASK_ACTIVE_DEADLINE_SECS` — a task that hangs otherwise holds a
    /// node slot until the run-level timeout notices.
    pub active_deadline_secs: Option<i64>,
    /// `DAGRON_TASK_RUNTIME_CLASS` — e.g. `gvisor`, for untrusted images.
    pub runtime_class: Option<String>,
    /// `DAGRON_TASK_NODE_SELECTOR` — `k=v,k=v`.
    pub node_selector: Vec<(String, String)>,
    /// `DAGRON_TASK_TOLERATIONS` — `key[=value][:effect]`, comma-separated
    /// ([`parse_tolerations`]). A node selector pins task pods to nodes; a
    /// tainted pool also needs its taint tolerated, or the pods stay Pending.
    pub tolerations: Vec<TaskToleration>,
    /// `DAGRON_TASK_PRIORITY_CLASS` — `priorityClassName` on task pods.
    pub priority_class: Option<String>,
    /// `DAGRON_TASK_FS_GROUP` — the group a pod's volumes are given where the
    /// executor gives one (a claim workspace, [`crate::workspace::CLAIM_GROUP`]).
    pub fs_group: FsGroup,
    /// `DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS` — `k=v,k=v`, on a task pod
    /// that runs as a service account of its own (`service_account:`) and on
    /// no other: what a workload identity webhook asks of the pods it gives a
    /// token (AKS's, `azure.workload.identity/use=true`).
    pub service_account_pod_labels: Vec<(String, String)>,
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
            // Deliberately not env-driven: it exists for the per-task
            // envelope, and the process-wide knobs it would duplicate
            // (`DROP_ALL_CAPABILITIES`, `RUN_AS_USER`) already imply it.
            no_new_privileges: false,
            // Without a uid: where the platform gives one (OpenShift's SCCs),
            // naming one is refused, and `RUN_AS_USER` cannot say this.
            run_as_non_root: flag("DAGRON_TASK_RUN_AS_NON_ROOT"),
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
            // The engine refuses to start on a value that does not parse
            // (dagron-engine's startup checks), so the fallback below is for
            // other embedders of this executor: they get no tolerations and a
            // log line, not a task that fails at dispatch.
            tolerations: match std::env::var("DAGRON_TASK_TOLERATIONS") {
                Ok(raw) => parse_tolerations(&raw).unwrap_or_else(|e| {
                    tracing::error!(error = %e, "ignoring DAGRON_TASK_TOLERATIONS");
                    Vec::new()
                }),
                Err(_) => Vec::new(),
            },
            priority_class: std::env::var("DAGRON_TASK_PRIORITY_CLASS")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty()),
            automount_sa_token: flag("DAGRON_TASK_AUTOMOUNT_SA_TOKEN"),
            // The engine refuses to start on a value that does not parse, as
            // for the tolerations above.
            fs_group: FsGroup::parse(&std::env::var(ENV_TASK_FS_GROUP).unwrap_or_default())
                .unwrap_or_else(|e| {
                    tracing::error!(error = %e, "ignoring {ENV_TASK_FS_GROUP}");
                    FsGroup::Shaped
                }),
            service_account_pod_labels: parse_pod_labels(
                &std::env::var(ENV_TASK_SERVICE_ACCOUNT_POD_LABELS).unwrap_or_default(),
            )
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "ignoring {ENV_TASK_SERVICE_ACCOUNT_POD_LABELS}");
                Vec::new()
            }),
        }
    }
}

/// Labels on a task pod that runs as a service account of its own.
pub const ENV_TASK_SERVICE_ACCOUNT_POD_LABELS: &str = "DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS";

/// `k=v,k=v`, each a label's key and value as Kubernetes takes them; empty for
/// none. The engine refuses to start on one that is not.
pub fn parse_pod_labels(raw: &str) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for pair in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some((k, v)) = pair.split_once('=') else {
            bail!("{ENV_TASK_SERVICE_ACCOUNT_POD_LABELS}: {pair:?} is not key=value");
        };
        let (k, v) = (k.trim(), v.trim());
        if !is_label_key(k) {
            bail!("{ENV_TASK_SERVICE_ACCOUNT_POD_LABELS}: {k:?} is not a label key");
        }
        // The engine's own labels select its task pods (its pod watch, the
        // NetworkPolicies it reads): one set here would move a pod out of them.
        if k == "app" || k.starts_with("dagron.dev/") {
            bail!("{ENV_TASK_SERVICE_ACCOUNT_POD_LABELS}: {k:?} is the engine's own label");
        }
        if !v.is_empty() && !is_label_value(v) {
            bail!("{ENV_TASK_SERVICE_ACCOUNT_POD_LABELS}: {v:?} is not a label value");
        }
        out.push((k.to_string(), v.to_string()));
    }
    Ok(out)
}

/// The group a task pod's volumes are given ([`FsGroup`]).
pub const ENV_TASK_FS_GROUP: &str = "DAGRON_TASK_FS_GROUP";

/// Which `fsGroup` a task pod carries, where the executor gives it one: a
/// claim workspace, whose root is its provisioner's.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsGroup {
    /// The executor's own, [`crate::workspace::CLAIM_GROUP`]: unset.
    #[default]
    Shaped,
    /// This group: a number.
    Group(i64),
    /// None at all: `none`. The platform gives the pod one, as OpenShift's
    /// restricted SCCs do from the namespace's range, and refuse a pod that
    /// names a group outside it.
    Platform,
}

impl FsGroup {
    /// `none`, a group number, or empty for the executor's own.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "" => Ok(Self::Shaped),
            "none" => Ok(Self::Platform),
            n => match n.parse::<i64>() {
                Ok(g) if g >= 0 => Ok(Self::Group(g)),
                _ => anyhow::bail!(
                    "{ENV_TASK_FS_GROUP}={n:?} is neither a group number nor `none`"
                ),
            },
        }
    }
}

/// One toleration a task pod carries, from `DAGRON_TASK_TOLERATIONS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskToleration {
    pub key: String,
    /// `Some` tolerates the taint with this value (`operator: Equal`); `None`
    /// tolerates the key whatever its value (`operator: Exists`).
    pub value: Option<String>,
    /// `None` tolerates every effect.
    pub effect: Option<String>,
}

impl TaskToleration {
    fn manifest(&self) -> serde_json::Value {
        let mut t = serde_json::json!({ "key": self.key });
        match &self.value {
            Some(v) => {
                t["operator"] = serde_json::json!("Equal");
                t["value"] = serde_json::json!(v);
            }
            None => t["operator"] = serde_json::json!("Exists"),
        }
        if let Some(effect) = &self.effect {
            t["effect"] = serde_json::json!(effect);
        }
        t
    }
}

/// Parse `DAGRON_TASK_TOLERATIONS`: comma-separated `key[=value][:effect]`.
///
/// - `key=value` tolerates a taint with that value (`operator: Equal`).
/// - A bare `key` tolerates the key whatever its value (`operator: Exists`).
/// - `:effect` is `NoSchedule`, `PreferNoSchedule` or `NoExecute`. Without it,
///   every effect is tolerated.
///
/// An empty key is refused. With `Exists` it would tolerate every taint on
/// every node, which undoes the taints a tainted pool exists for. An empty
/// value (`key=`) is refused too: a pool's taint carries a value, and a
/// half-typed entry should fail here rather than leave tasks Pending.
///
/// Otherwise a key is what Kubernetes admits as a toleration's key, a label
/// key (`is_label_key`), and a value what it admits with `Equal`, a label
/// value (`is_label_value`). Anything else would pass here only for the
/// apiserver to refuse every task pod that carries it.
pub fn parse_tolerations(raw: &str) -> Result<Vec<TaskToleration>> {
    let mut out = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (kv, effect) = match entry.split_once(':') {
            Some((kv, effect)) => {
                if !matches!(effect, "NoSchedule" | "PreferNoSchedule" | "NoExecute") {
                    bail!("toleration {entry:?}: effect must be NoSchedule, PreferNoSchedule or NoExecute");
                }
                (kv, Some(effect.to_string()))
            }
            None => (entry, None),
        };
        let (key, value) = match kv.split_once('=') {
            Some((k, v)) => (k.trim(), Some(v.trim())),
            None => (kv.trim(), None),
        };
        if !is_label_key(key) {
            bail!("toleration {entry:?}: the key must be a label key: an optional DNS subdomain and '/', then 1 to 63 letters, digits, '-', '_' or '.', a letter or digit at each end");
        }
        if let Some(v) = value {
            if !is_label_value(v) {
                bail!("toleration {entry:?}: the value after '=' must be a label value: 1 to 63 letters, digits, '-', '_' or '.', a letter or digit at each end");
            }
        }
        out.push(TaskToleration {
            key: key.to_string(),
            value: value.map(str::to_string),
            effect,
        });
    }
    Ok(out)
}

impl PodHardening {
    /// Fold an **effective** trust envelope over these process-wide defaults.
    ///
    /// "Effective" means the spec has already been raised to the operator's
    /// floor (`IsolationSpec::apply_floor`) — this function does not enforce a
    /// floor and must never be handed a raw workflow-authored envelope, or a
    /// task would be choosing its own privileges.
    ///
    /// A field the envelope leaves unset keeps the process-wide value, so an
    /// operator's `DAGRON_TASK_*` settings still apply to every task that says
    /// nothing. A field the envelope sets wins outright, including setting it
    /// *weaker* — the floor is what prevents that being an escape, and putting
    /// a second implicit floor here would make the effective envelope differ
    /// from the one that was checked, reported and attested.
    pub fn with_isolation(&self, iso: &dagron_core::isolation::IsolationSpec) -> Self {
        use dagron_core::isolation::Seccomp;
        let mut out = self.clone();
        if let Some(rc) = &iso.runtime_class {
            out.runtime_class = Some(rc.clone());
        }
        if let Some(sc) = iso.seccomp {
            out.seccomp_runtime_default = matches!(sc, Seccomp::RuntimeDefault);
        }
        if let Some(v) = iso.read_only_root_fs {
            out.read_only_root_fs = v;
        }
        if let Some(v) = iso.no_new_privileges {
            out.no_new_privileges = v;
        }
        if let Some(v) = iso.drop_all_capabilities {
            out.drop_all_capabilities = v;
        }
        if let Some(v) = iso.run_as_non_root {
            out.run_as_non_root = v;
        }
        if let Some(u) = iso.run_as_user {
            out.run_as_user = Some(u);
        }
        if let Some(v) = iso.service_account_token {
            out.automount_sa_token = v;
        }
        out
    }
}

/// The label that carries a task pod's priority class
/// (`DAGRON_TASK_PRIORITY_CLASS`), so whatever manages the classes can select
/// the pods one admitted by label, without reading every pod: a hosted
/// deployment's operator stops a subscription tier's leftover work this way when
/// the workspace moves down a tier.
pub const PRIORITY_CLASS_LABEL: &str = "dagron.dev/priority-class";

/// Can `s` be a label value? At most 63 characters, alphanumeric at each end,
/// and `-`, `_` or `.` between. A PriorityClass name may be up to 253, so a
/// long one gets the class and no label.
fn is_label_value(s: &str) -> bool {
    let alnum = |c: char| c.is_ascii_alphanumeric();
    !s.is_empty()
        && s.len() <= 63
        && s.starts_with(alnum)
        && s.ends_with(alnum)
        && s.chars().all(|c| alnum(c) || matches!(c, '-' | '_' | '.'))
}

/// Can `s` be a label key, Kubernetes' "qualified name"? A name that could be
/// a label value, after an optional prefix and `/`: a DNS subdomain of at most
/// 253 characters, dot-separated parts of lowercase letters, digits and `-`,
/// each a letter or digit at both ends (`cloud.dagron.dev/tenant`).
fn is_label_key(s: &str) -> bool {
    let subdomain = |p: &str| {
        let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
        p.len() <= 253
            && p.split('.').all(|part| {
                part.starts_with(alnum)
                    && part.ends_with(alnum)
                    && part.chars().all(|c| alnum(c) || c == '-')
            })
    };
    match s.split_once('/') {
        Some((prefix, name)) => subdomain(prefix) && is_label_value(name),
        None => is_label_value(s),
    }
}

/// Apply hardening to a built Pod manifest.
///
/// Pure and `Value`-shaped, mirroring the enterprise build's confidential
/// path so both shape the same manifest through the same kind of seam rather
/// than two divergent ones.
/// Set `sc`'s fields on the pod's security context, keeping the others it
/// has (a claim workspace's `fsGroup`).
fn merge_security_context(
    spec: &mut serde_json::Map<String, serde_json::Value>,
    sc: serde_json::Value,
) {
    let serde_json::Value::Object(sc) = sc else {
        return;
    };
    match spec
        .get_mut("securityContext")
        .and_then(serde_json::Value::as_object_mut)
    {
        Some(existing) => existing.extend(sc),
        None => {
            spec.insert("securityContext".to_string(), serde_json::Value::Object(sc));
        }
    }
}

pub fn apply_hardening(pod: &mut serde_json::Value, h: &PodHardening, wants_sa: bool) {
    // A pod that runs as a service account of its own is labelled as a
    // workload identity webhook asks (AKS's), and no other pod is.
    if wants_sa && !h.service_account_pod_labels.is_empty() {
        if let Some(labels) = pod
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|m| m.entry("labels").or_insert_with(|| serde_json::json!({})).as_object_mut())
        {
            for (k, v) in &h.service_account_pod_labels {
                labels.insert(k.clone(), serde_json::json!(v));
            }
        }
    }
    if let Some(pc) = h.priority_class.as_deref().filter(|pc| is_label_value(pc)) {
        if let Some(labels) = pod
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|m| {
                m.entry("labels")
                    .or_insert_with(|| serde_json::json!({}))
                    .as_object_mut()
            })
        {
            labels.insert(PRIORITY_CLASS_LABEL.to_string(), serde_json::json!(pc));
        }
    }
    let Some(spec) = pod.get_mut("spec").and_then(serde_json::Value::as_object_mut) else {
        return;
    };

    if let Some(uid) = h.run_as_user {
        let mut sc = serde_json::json!({ "runAsUser": uid });
        if uid > 0 || h.run_as_non_root {
            sc["runAsNonRoot"] = serde_json::json!(true);
        }
        if h.seccomp_runtime_default {
            sc["seccompProfile"] = serde_json::json!({ "type": "RuntimeDefault" });
        }
        merge_security_context(spec, sc);
    } else if h.seccomp_runtime_default || h.run_as_non_root {
        // `runAsNonRoot` without a uid is the useful half on its own: it refuses
        // an image whose user resolves to root instead of picking a uid the
        // image may have no filesystem permissions for.
        let mut sc = serde_json::json!({});
        if h.seccomp_runtime_default {
            sc["seccompProfile"] = serde_json::json!({ "type": "RuntimeDefault" });
        }
        if h.run_as_non_root {
            sc["runAsNonRoot"] = serde_json::json!(true);
        }
        merge_security_context(spec, sc);
    }

    // The group a volume is given, where the executor gave one: another, or
    // none for the platform to give (OpenShift's SCCs assign it).
    if let Some(sc) = spec.get_mut("securityContext").and_then(serde_json::Value::as_object_mut) {
        if sc.contains_key("fsGroup") {
            match h.fs_group {
                FsGroup::Shaped => {}
                FsGroup::Group(g) => {
                    sc.insert("fsGroup".to_string(), serde_json::json!(g));
                }
                FsGroup::Platform => {
                    sc.remove("fsGroup");
                    sc.remove("fsGroupChangePolicy");
                }
            }
        }
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
    if !h.tolerations.is_empty() {
        let t = h.tolerations.iter().map(TaskToleration::manifest).collect();
        spec.insert("tolerations".to_string(), serde_json::Value::Array(t));
    }
    if let Some(pc) = &h.priority_class {
        spec.insert("priorityClassName".to_string(), serde_json::json!(pc));
    }
    // Only withhold the token from tasks that never asked for an identity;
    // a task with `service_account:` wants IRSA and must keep its token.
    if !h.automount_sa_token && !wants_sa {
        spec.insert("automountServiceAccountToken".to_string(), serde_json::json!(false));
    }

    if h.read_only_root_fs || h.drop_all_capabilities || h.no_new_privileges {
        // Init containers too: the one installing dagron-ci-exec runs from an
        // image the task did not choose, and is held to the same envelope.
        for key in ["initContainers", "containers"] {
            let Some(containers) = spec.get_mut(key).and_then(serde_json::Value::as_array_mut) else {
                continue;
            };
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

/// Which ServiceAccounts a task may ask for through `service_account:`.
///
/// A task pod is created in this executor's namespace, beside the engine — whose
/// own ServiceAccount can create pods and Secrets (the chart's `<release>-engine`
/// Role). Nothing stopped a workflow-authored task from naming that ServiceAccount
/// (or any other in the namespace) and running as it: the task's command would
/// then hold the engine's rights, which is a privilege escalation from "run a
/// container" to "create pods and read Secrets in this namespace".
///
/// So a requested ServiceAccount is now allow-listed. The list is an operator
/// setting, and **unset denies every request** — the same default-off posture the
/// rest of this module takes. `service_account:` is the IRSA seam, so an install
/// that uses it lists the task identities it created for that purpose, and never
/// the engine's own ServiceAccount.
#[derive(Debug, Clone, Default)]
pub struct ServiceAccountPolicy {
    allowed: std::collections::BTreeSet<String>,
    /// Tests only (`allow_all`): skip the check entirely. `from_env` never sets
    /// it, so no environment can turn the gate off — only an explicit list opens
    /// specific names.
    allow_any: bool,
}

impl ServiceAccountPolicy {
    /// `DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS` — comma-separated ServiceAccount
    /// names a task may request. Empty or unset denies every request, so a task
    /// that names one is refused rather than run with it. Never list the engine's
    /// own ServiceAccount, or any with pod-create / Secret RBAC: list only the
    /// purpose-built task identities the IRSA seam exists for.
    pub fn from_env() -> Self {
        Self::from_env_value(std::env::var("DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS").ok().as_deref())
    }

    /// [`from_env`](Self::from_env) split off its one variable, so the parse is
    /// pure and testable without racing other tests on the process environment.
    fn from_env_value(value: Option<&str>) -> Self {
        let allowed = value
            .map(|v| {
                v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect()
            })
            .unwrap_or_default();
        Self { allowed, allow_any: false }
    }

    /// A policy that permits any ServiceAccount. Test-only: production always
    /// goes through `from_env`, whose default denies.
    #[cfg(test)]
    fn allow_all() -> Self {
        Self { allowed: std::collections::BTreeSet::new(), allow_any: true }
    }

    /// Refuse a task that asks for a ServiceAccount it may not use. `None` (the
    /// task named none) is always allowed — it runs as the namespace `default`,
    /// whose token `apply_hardening` withholds anyway.
    pub fn check(&self, requested: Option<&str>) -> Result<()> {
        let Some(sa) = requested else { return Ok(()) };
        if self.allow_any || self.allowed.contains(sa) {
            return Ok(());
        }
        bail!(
            "task requests serviceAccount '{sa}', which is not permitted. A task pod runs beside \
             the engine in its namespace, so running as an arbitrary ServiceAccount is a \
             privilege-escalation path (the engine's own ServiceAccount can create pods and read \
             Secrets). List the task identities you created for IRSA in \
             DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS — never the engine's ServiceAccount."
        )
    }
}

/// Build the one-shot task Pod manifest. Split out (and free of any client) so it
/// is unit-testable without a cluster.
///
/// `ctx` supplies the per-task knobs that make load-test pods realistic:
/// * `env` → container env vars (the parameterised ETL image reads these);
/// * `resources` → container `resources.requests/limits` so the k8s scheduler
///   packs/evicts/OOMKills pods like production;
/// * `service_account` → the IRSA seam, so the pod assumes an IAM role for S3,
///   gated by [`ServiceAccountPolicy`].
#[cfg(test)]
fn build_pod(name: &str, image: &str, command: &[String], ctx: &ExecContext) -> Result<Pod> {
    build_pod_with_secrets(
        name,
        image,
        command,
        ctx,
        None,
        &ServiceAccountPolicy::allow_all(),
        &KubeWorkspaces::default(),
    )
}

/// Whether the task carries any secret-sourced env var. `value_from` stays set
/// on a resolved var precisely so the redactor knows its value is a secret
/// (`secrets::resolve`, `environments::resolve_secrets`); it is the same marker.
fn has_secret_env(ctx: &ExecContext) -> bool {
    ctx.env.iter().any(|e| e.value_from.is_some())
}

/// The header the workspace's checkout is fetched with, when the attempt has
/// a credential its repository may be sent.
fn checkout_header(ctx: &ExecContext) -> Option<String> {
    let checkout = crate::checkout::of(ctx).ok().flatten()?;
    crate::checkout::secret_env(checkout, ctx.checkout_credential.as_ref())
        .map(|(_, header)| header)
}

/// The per-task Secret holding the secret-sourced env values, keyed by variable
/// name. Labelled like the pod so an operator can find leftovers.
fn env_secret_object(name: &str, ctx: &ExecContext) -> Result<Secret> {
    let mut data: serde_json::Map<String, serde_json::Value> = ctx
        .env
        .iter()
        .filter(|e| e.value_from.is_some())
        .map(|e| (e.name.clone(), serde_json::Value::String(e.value.clone())))
        .collect();
    if let Some(header) = checkout_header(ctx) {
        data.insert(
            crate::checkout::SECRET_KEY.to_string(),
            serde_json::Value::String(header),
        );
    }
    for (i, (_, header)) in crate::checkout::action_headers(ctx).into_iter().enumerate() {
        data.insert(
            crate::checkout::action_secret_key(i + 1),
            serde_json::Value::String(header),
        );
    }
    let mut labels = serde_json::Map::new();
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        serde_json::json!("dagron"),
    );
    if let Some(id) = &ctx.identity {
        for (k, v) in id.labels() {
            labels.insert(k, serde_json::Value::String(v));
        }
    }
    serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": name, "labels": serde_json::Value::Object(labels)},
        "stringData": data,
    }))
    .map_err(|e| anyhow::anyhow!("build env Secret: {e}"))
}

/// Each of a pod's images through its registry's mirror, and the operator's
/// pull secrets named where its run is trusted. One that is not
/// names none, and its own images (the task's, its services') are pulled
/// `Always`, whatever its policy: the kubelet then asks the registry with the
/// pod's credentials, so a private image a trusted pod left on the node is
/// not started for it from the node's cache. The pod's ServiceAccount must
/// carry no pull secrets of its own, which admission would add.
fn through_registries(
    pod: &mut Pod,
    ctx: &ExecContext,
    registries: &crate::images::Registries,
    pull_secrets: &[String],
) {
    let Some(spec) = pod.spec.as_mut() else {
        return;
    };
    for container in spec
        .containers
        .iter_mut()
        .chain(spec.init_containers.iter_mut().flatten())
    {
        if let Some(image) = container.image.as_mut() {
            *image = registries.mirrored(image);
        }
    }
    if ctx.confinement.is_some() {
        let own = spec.containers.iter_mut().chain(
            spec.init_containers
                .iter_mut()
                .flatten()
                .filter(|c| c.restart_policy.as_deref() == Some("Always")),
        );
        for container in own {
            container.image_pull_policy = Some("Always".to_string());
        }
    }
    if ctx.confinement.is_none() && !pull_secrets.is_empty() {
        let named = spec.image_pull_secrets.get_or_insert_with(Vec::new);
        for name in pull_secrets {
            if !named.iter().any(|r| &r.name == name) {
                named.push(k8s_openapi::api::core::v1::LocalObjectReference { name: name.clone() });
            }
        }
    }
}

/// [`build_pod`] with secret-sourced env vars referenced from Secret
/// `secret_name` instead of inlined. `None` = inline (the legacy shape). A
/// task with a workspace gets it as `workspaces` says.
#[cfg(test)]
fn build_pod_with_secrets(
    name: &str,
    image: &str,
    command: &[String],
    ctx: &ExecContext,
    secret_name: Option<&str>,
    sa_policy: &ServiceAccountPolicy,
    workspaces: &KubeWorkspaces,
) -> Result<Pod> {
    build_pod_told(name, image, command, ctx, secret_name, sa_policy, workspaces, &[])
}

/// [`build_pod_with_secrets`], with `told` set in the task's container
/// before its own environment, and in its checkout's and actions' fetches:
/// what its allowlist tells it of a proxy ([`KubeExecutor::proxy_env`]).
#[allow(clippy::too_many_arguments)]
fn build_pod_told(
    name: &str,
    image: &str,
    command: &[String],
    ctx: &ExecContext,
    secret_name: Option<&str>,
    sa_policy: &ServiceAccountPolicy,
    workspaces: &KubeWorkspaces,
    told: &[dagron_core::dag::EnvVar],
) -> Result<Pod> {
    // Before anything is built: a task may not run as a ServiceAccount it was not
    // granted. Checked here so every path that produces a task pod is covered, and
    // so the refusal is a build error the caller already cleans up after.
    sa_policy.check(ctx.service_account.as_deref())?;

    let env: Vec<serde_json::Value> = told
        .iter()
        .filter(|t| !ctx.env.iter().any(|e| e.name == t.name))
        .chain(ctx.env.iter())
        .map(|e| match secret_name {
            Some(sn) if e.value_from.is_some() => serde_json::json!({
                "name": e.name,
                "valueFrom": { "secretKeyRef": { "name": sn, "key": e.name } },
            }),
            _ => serde_json::json!({ "name": e.name, "value": e.value }),
        })
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
        "name": TASK_CONTAINER,
        "image": image,
        "command": command,
    });
    // Stated only when the task states it: left out, the kubelet's default
    // applies (Always for `:latest` or no tag, IfNotPresent otherwise).
    if let Some(policy) = ctx.image_pull_policy {
        container["imagePullPolicy"] = serde_json::json!(policy.as_kubernetes());
    }
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
    if let Some(ws) = &ctx.workspace {
        let header = checkout_header(ctx);
        let checkout = crate::checkout::of(ctx)?.map(|checkout| crate::workspace::PodCheckout {
            checkout,
            credential: header.as_deref().map(|header| match secret_name {
                Some(sn) => crate::workspace::PodCredential::Secret(sn),
                None => crate::workspace::PodCredential::Inline(header),
            }),
            patch: ctx.checkout_patch.as_deref(),
        });
        let action_headers = crate::checkout::action_headers(ctx);
        let actions = crate::workspace::PodActions {
            headers: &action_headers,
            secret: secret_name,
        };
        workspaces.shape_with(&mut spec, ws, checkout.as_ref(), actions);
        // The pod's policy holds its fetches too: a list by hostname lets
        // them out through its proxy alone, so they are told of it as the
        // task is, each where it set nothing of the name itself.
        if !told.is_empty() {
            let fetches = [
                crate::checkout::CONTAINER_NAME,
                crate::checkout::ACTIONS_CONTAINER_NAME,
            ];
            for init in spec["initContainers"].as_array_mut().into_iter().flatten() {
                if !fetches.iter().any(|f| init["name"] == *f) {
                    continue;
                }
                let own: Vec<serde_json::Value> = init["env"].as_array().cloned().unwrap_or_default();
                let mut env: Vec<serde_json::Value> = told
                    .iter()
                    .filter(|t| !own.iter().any(|e| e["name"] == t.name.as_str()))
                    .map(|t| serde_json::json!({ "name": t.name, "value": t.value }))
                    .collect();
                env.extend(own);
                init["env"] = serde_json::Value::Array(env);
            }
        }
    }

    // A task that asked for no network, or for an allowlist, is not started
    // until its pod is seen to be held: [`GATE_NAME`] or
    // [`ALLOWLIST_GATE_NAME`], after every other init container, so that
    // nothing of the pod runs between it and the task.
    let asked = asked_network(ctx);
    if let Some(asked) = asked {
        let Some(image) = &workspaces.ci_exec_image else {
            bail!(
                "{}. Its pod is held until its network is seen to be held by an init \
                 container from {}, and this engine names none",
                asked.says(),
                crate::workspace::ENV_CI_EXEC_IMAGE
            );
        };
        let mut gate = serde_json::json!({
            "name": asked.gate(),
            "image": image,
            "args": gate_args(asked),
        });
        // What the task asks for, as the other init containers do: it costs
        // the pod nothing, and passes the namespace's limits where the task
        // would.
        if let Some(resources) = spec["containers"][0]
            .get("resources")
            .and_then(crate::workspace::cpu_and_memory)
        {
            gate["resources"] = resources;
        }
        match spec
            .get_mut("initContainers")
            .and_then(serde_json::Value::as_array_mut)
        {
            Some(inits) => inits.push(gate),
            None => spec["initContainers"] = serde_json::json!([gate]),
        }
    }

    // Its services, after every other init container, the gate among them:
    // a service's image and arguments are its author's, and nothing of the
    // author's runs in a pod before it is seen to be held. Each service's
    // name is a name for the pod's loopback address, where its containers
    // find each other.
    if !ctx.services.is_empty() {
        let sidecars: Vec<serde_json::Value> = ctx
            .services
            .iter()
            .map(|service| service_container(service, ctx.image_pull_policy, told))
            .collect();
        match spec
            .get_mut("initContainers")
            .and_then(serde_json::Value::as_array_mut)
        {
            Some(inits) => inits.extend(sidecars),
            None => spec["initContainers"] = serde_json::Value::Array(sidecars),
        }
        let names: Vec<&str> = ctx.services.iter().map(|s| s.name.as_str()).collect();
        spec["hostAliases"] = serde_json::json!([{ "ip": "127.0.0.1", "hostnames": names }]);
    }

    // Identity labels, so this pod can be found by something other than the
    // process that created it — see `TaskIdentity`. An unlabellable identity
    // adds nothing rather than adding a partial set, because a pod carrying
    // `managed-by` and no `task-id` is exactly what a reaper would delete.
    let mut labels = serde_json::Map::new();
    labels.insert("app".to_string(), serde_json::json!("module-54-scheduler"));
    if let Some(id) = &ctx.identity {
        for (k, v) in id.labels() {
            labels.insert(k, serde_json::Value::String(v));
        }
    }
    // A task that asked for no network, or for an allowlist, is labelled for
    // the policy that holds it ([`NO_NETWORK_POLICY`],
    // [`allowlist_policy_name`]). The label is this executor's to set and no
    // workflow's: nothing a task declares becomes a label of its pod but
    // this, and of this a task says only which.
    if let Some(asked) = asked {
        labels.insert(NETWORK_LABEL.to_string(), serde_json::json!(asked.label()));
    }

    let mut manifest = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "labels": serde_json::Value::Object(labels),
        },
        "spec": spec,
    });

    // Shape the manifest before it is deserialized, so hardening applies to every
    // task pod this executor creates rather than to whichever call sites
    // remembered. `wants_sa` distinguishes a task that asked for an identity from
    // one that did not: the former keeps its token, the latter should not be
    // handed one it never requested.
    let hardening = PodHardening::from_env();
    let hardening = match &ctx.isolation {
        Some(iso) => hardening.with_isolation(iso),
        None => hardening,
    };
    apply_hardening(&mut manifest, &hardening, ctx.service_account.is_some());
    if ctx.privileged {
        privilege(&mut manifest);
    }

    let pod = serde_json::from_value(manifest)
        .map_err(|e| anyhow::anyhow!("build pod manifest: {e}"))?;
    Ok(pod)
}

/// What a ResourceQuota refusal of a pod create amounts to.
///
/// Only [`QuotaRefusal::Full`] is back-pressure. The other two are quota
/// refusals that keep the failure path, because waiting either cannot help or
/// cannot be shown to.
#[derive(Debug, PartialEq)]
enum QuotaRefusal {
    /// The namespace is at its quota and the request is within every hard
    /// limit the refusal names, so it fits once something else finishes.
    /// Carries the reason the task shows while it waits.
    Full(String),
    /// The request is over a hard limit on its own — a hard limit of 0
    /// included — so an empty namespace would refuse it too.
    NeverFits(String),
    /// A quota refusal whose lists did not read. Either the apiserver's wording
    /// is not the one this knows, or it is not what it appears to be.
    Unreadable,
}

/// [`classify_quota_refusal`] over the error `pods.create` returned. Only an
/// `Api` error carries a Status to read; the rendered text of any other kind is
/// never searched.
fn quota_refusal(e: &kube::Error, pod: &str) -> Option<QuotaRefusal> {
    match e {
        kube::Error::Api(s) => classify_quota_refusal(s.code, &s.reason, &s.message, pod),
        _ => None,
    }
}

/// The reason and message of a refusal [`quota_refusal`] did not recognise but
/// which reads like one, for the caller to log. Only meaningful after
/// `quota_refusal` returned `None`. The refusal fails like any other pod-create
/// error; this is what keeps that from being silent if the apiserver's sentence
/// is not the one [`classify_quota_refusal`] anchors on.
fn unrecognised_quota_refusal(e: &kube::Error) -> Option<(&str, &str)> {
    match e {
        kube::Error::Api(s) if mentions_quota(s.code, &s.message) => {
            Some((s.reason.as_str(), s.message.as_str()))
        }
        _ => None,
    }
}

/// A `403` whose message says `exceeded quota` anywhere. Deliberately loose
/// where [`classify_quota_refusal`] is strict: this decides only whether a line
/// is logged, never whether a task waits, so a webhook quoting the phrase costs
/// a WARN and nothing else.
fn mentions_quota(code: u16, message: &str) -> bool {
    code == 403 && message.contains("exceeded quota")
}

/// Whether a refused pod create is the namespace's ResourceQuota speaking, and
/// if so which kind. `None` means it is not a quota refusal at all. Free of any
/// client, so the whole decision is unit-testable.
///
/// The sentence is the apiserver's, about the pod this call named:
///
/// ```text
/// pods "sched-…" is forbidden: exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4
/// ```
///
/// It is matched on the Status fields and anchored at the start, never searched
/// for, so an RBAC denial, a LimitRange, PodSecurity or a webhook whose text
/// happens to contain the phrase is not one. Past the anchor the answer errs
/// one way only: a task waits when the three lists read exactly, name the same
/// resources, and no request is over its hard limit. Failing a refusal that
/// would have cleared costs the task one attempt; waiting on one that never
/// clears holds a task until the engine's bound.
fn classify_quota_refusal(code: u16, reason: &str, message: &str, pod: &str) -> Option<QuotaRefusal> {
    if code != 403 || reason != "Forbidden" {
        return None;
    }
    let anchor = format!("pods \"{pod}\" is forbidden: exceeded quota: ");
    let lists = message.strip_prefix(anchor.as_str())?;
    Some(read_quota_lists(lists).unwrap_or(QuotaRefusal::Unreadable))
}

/// The rest of the sentence after `exceeded quota: `, which is
/// `<quota>, requested: <list>, used: <list>, limited: <list>`. `None` when it
/// is anything else.
fn read_quota_lists(rest: &str) -> Option<QuotaRefusal> {
    let (quota, rest) = rest.split_once(", requested: ")?;
    let (requested, rest) = rest.split_once(", used: ")?;
    let (used, limited) = rest.split_once(", limited: ")?;
    if quota.is_empty() {
        return None;
    }
    let (wanted, held, hard) = (quantity_list(requested)?, quantity_list(used)?, quantity_list(limited)?);
    // The apiserver prints the same resources, in the same order, three times.
    // A list that names different ones is not a comparison this can make.
    let same_names =
        |a: &[(&str, i128)], b: &[(&str, i128)]| a.iter().map(|e| e.0).eq(b.iter().map(|e| e.0));
    if !same_names(&wanted, &held) || !same_names(&wanted, &hard) {
        return None;
    }
    for ((name, want), (_, limit)) in wanted.iter().zip(&hard) {
        if want > limit {
            return Some(QuotaRefusal::NeverFits(format!(
                "{name} is over the hard limit of ResourceQuota '{quota}' (requested: {requested}, limited: {limited})"
            )));
        }
    }
    Some(QuotaRefusal::Full(format!(
        "pod refused by namespace ResourceQuota '{quota}' (requested: {requested}, used: {used}, limited: {limited})"
    )))
}

/// One `name=quantity,name=quantity` list of the sentence, in the order
/// written. `None` when it is empty, or an entry is not a resource name and a
/// quantity [`quantity_nanos`] reads.
fn quantity_list(list: &str) -> Option<Vec<(&str, i128)>> {
    list.split(',')
        .map(|entry| {
            let (name, quantity) = entry.split_once('=')?;
            let is_name = !name.is_empty()
                && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'/'));
            if !is_name {
                return None;
            }
            Some((name, quantity_nanos(quantity)?))
        })
        .collect()
}

/// A Kubernetes quantity in nano-units, exactly, or `None`.
///
/// Reads what the apiserver prints: an optional `+`, digits, an optional
/// fraction of at most nine digits, then nothing, a decimal suffix
/// (`n u m k M G T P E`), a binary one (`Ki Mi Gi Ti Pi Ei`) or an exponent
/// (`e3`, `E-2`). A bare `E` is exa; `E` followed by a sign or a digit is an
/// exponent.
///
/// Everything else is refused rather than approximated — a negative value, a
/// stray character, a value that overflows or is finer than a nano-unit. Two
/// of these are compared to decide whether a task waits, and a comparison that
/// is slightly wrong is worse than none.
fn quantity_nanos(q: &str) -> Option<i128> {
    let s = q.strip_prefix('+').unwrap_or(q);
    let digits = |s: &str| s.bytes().take_while(u8::is_ascii_digit).count();
    let (whole, rest) = s.split_at(digits(s));
    let (frac, suffix) = match rest.strip_prefix('.') {
        Some(after) => after.split_at(digits(after)),
        None => ("", rest),
    };
    if whole.is_empty() || frac.len() > 9 || (frac.is_empty() && rest.starts_with('.')) {
        return None;
    }

    // The suffix as a power of ten and a power of two, relative to one unit.
    let (pow10, pow2): (i64, u32) = match suffix {
        "" => (0, 0),
        "n" => (-9, 0),
        "u" => (-6, 0),
        "m" => (-3, 0),
        "k" => (3, 0),
        "M" => (6, 0),
        "G" => (9, 0),
        "T" => (12, 0),
        "P" => (15, 0),
        "E" => (18, 0),
        "Ki" => (0, 10),
        "Mi" => (0, 20),
        "Gi" => (0, 30),
        "Ti" => (0, 40),
        "Pi" => (0, 50),
        "Ei" => (0, 60),
        // `parse` takes one optional sign and at least one digit, which is
        // exactly the exponent's grammar.
        _ => (i64::from(suffix.strip_prefix(['e', 'E'])?.parse::<i32>().ok()?), 0),
    };

    let mantissa = whole
        .bytes()
        .chain(frac.bytes())
        .try_fold(0i128, |acc, b| acc.checked_mul(10)?.checked_add(i128::from(b - b'0')))?;
    let scaled = mantissa.checked_mul(1i128 << pow2)?;
    // Nano-units put nine more decimal places on the unit; the fraction's
    // digits, already folded into the mantissa, take theirs back.
    let pow10 = 9 + pow10 - frac.len() as i64;
    let ten_to = |p: i64| 10i128.checked_pow(u32::try_from(p).ok()?);
    if pow10 >= 0 {
        scaled.checked_mul(ten_to(pow10)?)
    } else {
        let divisor = ten_to(-pow10)?;
        (scaled % divisor == 0).then(|| scaled / divisor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manifest builder produces a valid one-shot Pod with the task's image
    /// and command — verifiable without a cluster.
        /// A claim workspace's group is the executor's unless told otherwise:
    /// another, or none for the platform to give, as OpenShift's restricted
    /// SCCs do. A pod given no group is given none.
    #[test]
    fn a_claim_workspaces_group_is_another_or_the_platforms_when_asked() {
        let claimed = || {
            serde_json::json!({"spec": {
                "securityContext": {"fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch"},
                "containers": [{"name": "task"}],
            }})
        };
        let with = |fs_group| PodHardening { fs_group, ..PodHardening::default() };

        let mut pod = claimed();
        apply_hardening(&mut pod, &PodHardening::default(), false);
        assert_eq!(pod["spec"]["securityContext"]["fsGroup"], 65532, "unset: as today");

        let mut pod = claimed();
        apply_hardening(&mut pod, &with(FsGroup::Group(1000)), false);
        assert_eq!(pod["spec"]["securityContext"]["fsGroup"], 1000);
        assert_eq!(pod["spec"]["securityContext"]["fsGroupChangePolicy"], "OnRootMismatch");

        let mut pod = claimed();
        apply_hardening(&mut pod, &with(FsGroup::Platform), false);
        let sc = &pod["spec"]["securityContext"];
        assert!(sc.get("fsGroup").is_none() && sc.get("fsGroupChangePolicy").is_none(), "{sc}");

        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &with(FsGroup::Group(5)), false);
        assert!(pod["spec"].get("securityContext").is_none(), "no workspace, no group: {pod}");
    }

    /// A pod that runs as a service account of its own carries the labels a
    /// workload identity webhook asks for (AKS's), and no other pod does.
    #[test]
    fn a_pod_with_an_identity_is_labelled_for_its_webhook() {
        let h = PodHardening {
            service_account_pod_labels: vec![("azure.workload.identity/use".into(), "true".into())],
            ..PodHardening::default()
        };
        let mut with_sa = serde_json::json!({"metadata": {"labels": {"a": "b"}}, "spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut with_sa, &h, true);
        assert_eq!(with_sa["metadata"]["labels"]["azure.workload.identity/use"], "true");
        assert_eq!(with_sa["metadata"]["labels"]["a"], "b", "the pod's own labels stay");

        let mut without = serde_json::json!({"metadata": {}, "spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut without, &h, false);
        assert!(without["metadata"].get("labels").is_none(), "{without}");
    }

    #[test]
    fn service_account_pod_labels_parse_or_are_refused() {
        assert_eq!(
            parse_pod_labels(" azure.workload.identity/use=true , team=data ").unwrap(),
            vec![
                ("azure.workload.identity/use".to_string(), "true".to_string()),
                ("team".to_string(), "data".to_string())
            ]
        );
        assert!(parse_pod_labels("").unwrap().is_empty());
        for bad in [
            "novalue",
            "has space=x",
            "k=bad value",
            "/k=v",
            "app=other",
            "dagron.dev/network=none",
        ] {
            assert!(parse_pod_labels(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_task_fs_group_parses_or_is_refused() {
        assert_eq!(FsGroup::parse("").unwrap(), FsGroup::Shaped);
        assert_eq!(FsGroup::parse(" none ").unwrap(), FsGroup::Platform);
        assert_eq!(FsGroup::parse("1000680000").unwrap(), FsGroup::Group(1000680000));
        for bad in ["-1", "root", "65532,0"] {
            assert!(FsGroup::parse(bad).is_err(), "{bad}");
        }
    }

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
        assert!(spec.get("tolerations").is_none());
        assert!(spec.get("priorityClassName").is_none());
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
        // `..Default::default()` rather than an exhaustive literal: the note on
        // the ExecContext below records what happened the last time a field was
        // added to a struct these tests spell out in full, and this test asserts
        // on the fields it names, not on the absence of others.
        let h = PodHardening {
            run_as_user: Some(65532),
            read_only_root_fs: true,
            drop_all_capabilities: true,
            seccomp_runtime_default: true,
            active_deadline_secs: Some(3600),
            runtime_class: Some("gvisor".into()),
            node_selector: vec![("dagron.io/untrusted".into(), "true".into())],
            automount_sa_token: false,
            ..PodHardening::default()
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
    fn tolerations_parse_in_each_form() {
        let t = parse_tolerations(
            " cloud.dagron.dev/tenant=org-9:NoSchedule, dagron.io/untrusted:NoExecute,gpu=true, spot ",
        )
        .unwrap();
        assert_eq!(
            t,
            vec![
                TaskToleration {
                    key: "cloud.dagron.dev/tenant".into(),
                    value: Some("org-9".into()),
                    effect: Some("NoSchedule".into()),
                },
                TaskToleration {
                    key: "dagron.io/untrusted".into(),
                    value: None,
                    effect: Some("NoExecute".into())
                },
                TaskToleration {
                    key: "gpu".into(),
                    value: Some("true".into()),
                    effect: None
                },
                TaskToleration {
                    key: "spot".into(),
                    value: None,
                    effect: None
                },
            ]
        );
        assert!(parse_tolerations("").unwrap().is_empty());
        assert!(parse_tolerations(" , ").unwrap().is_empty());
    }

    /// A toleration that matches every taint would undo every tainted pool,
    /// and a half-typed one would leave tasks Pending: both are refused.
    #[test]
    fn tolerations_refuse_what_would_match_everything_or_nothing() {
        for bad in [
            // an empty key with an effect tolerates every taint with that effect
            ":NoSchedule",
            // an empty key
            "=org-9",
            // an empty value
            "cloud.dagron.dev/tenant=",
            // not an effect
            "k=v:NoSchedul",
            // an empty effect
            "k=v:",
            // a space is not part of a key, nor of a value
            "k y=v",
            "k=v w",
        ] {
            assert!(parse_tolerations(bad).is_err(), "{bad:?} should be refused");
        }
    }

    /// What passes here, the apiserver admits: a key is a label key and a
    /// value a label value, as Kubernetes validates a toleration. Otherwise
    /// the engine would start, and every task pod be refused.
    #[test]
    fn tolerations_are_held_to_kubernetes_rules() {
        let n = |c: &str, len: usize| c.repeat(len);
        // A prefix of 253, and one of 254.
        let prefix = |last: usize| [n("a", 63), n("b", 63), n("c", 63), n("d", last)].join(".");
        for good in [
            "example.com/MyName=v1.0_rc-2".to_string(),
            "a/b".to_string(),
            "node-role.kubernetes.io/control-plane:NoSchedule".to_string(),
            format!("{}={}", n("k", 63), n("v", 63)),
            format!("{}/k", prefix(61)),
        ] {
            assert!(parse_tolerations(&good).is_ok(), "{good:?} should pass");
        }
        for bad in [
            // an empty name after the prefix, or an empty prefix
            "gpu/".to_string(),
            "/gpu".to_string(),
            // a prefix is a DNS subdomain: lowercase, no empty part, no edge dash
            "Example.com/gpu".to_string(),
            "a..b/gpu".to_string(),
            "-a.com/gpu".to_string(),
            "a-.com/gpu".to_string(),
            "a_b.com/gpu".to_string(),
            // one '/' at most
            "a/b/c".to_string(),
            // a name, and a value, is a letter or digit at each end
            "-gpu".to_string(),
            "gpu.".to_string(),
            "k=-reserved".to_string(),
            "k=reserved_".to_string(),
            // a name, or a value, of 64
            n("k", 64),
            format!("k={}", n("v", 64)),
            format!("{}/k", prefix(62)),
        ] {
            assert!(parse_tolerations(&bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn tolerations_and_priority_class_reach_the_pod() {
        let h = PodHardening {
            tolerations: parse_tolerations("cloud.dagron.dev/tenant=org-9:NoSchedule,spot")
                .unwrap(),
            priority_class: Some("dagron-task".into()),
            ..PodHardening::default()
        };
        let mut pod = serde_json::json!({
            "metadata": {"labels": {"app": "module-54-scheduler"}},
            "spec": {"containers": [{"name": "task"}]},
        });
        apply_hardening(&mut pod, &h, false);
        let spec = &pod["spec"];
        assert_eq!(
            spec["tolerations"],
            serde_json::json!([
                {"key": "cloud.dagron.dev/tenant", "operator": "Equal", "value": "org-9", "effect": "NoSchedule"},
                {"key": "spot", "operator": "Exists"},
            ])
        );
        assert_eq!(spec["priorityClassName"], "dagron-task");
        // The class labels the pod too, beside the labels it already had.
        assert_eq!(
            pod["metadata"]["labels"],
            serde_json::json!({"app": "module-54-scheduler", "dagron.dev/priority-class": "dagron-task"})
        );
    }

    /// A class name no label can hold still reaches the pod, unlabelled; with
    /// no class, no label.
    #[test]
    fn the_priority_class_labels_the_pod_only_where_a_label_can_hold_it() {
        let long = "a".repeat(64);
        for (class, labelled) in [
            (Some("dagron-task-pro"), true),
            (Some(long.as_str()), false),
            (Some("dagron.task"), true),
            (None, false),
        ] {
            let h = PodHardening {
                priority_class: class.map(String::from),
                ..PodHardening::default()
            };
            let mut pod = serde_json::json!({"metadata": {"name": "t"}, "spec": {"containers": []}});
            apply_hardening(&mut pod, &h, false);
            assert_eq!(
                pod["metadata"]["labels"].get(PRIORITY_CLASS_LABEL).is_some(),
                labelled,
                "{class:?}"
            );
            assert_eq!(pod["spec"]["priorityClassName"].as_str(), class, "{class:?}");
        }
        assert!(!is_label_value("-x") && !is_label_value("x-") && !is_label_value("a b"));
    }

    // ── Per-task trust envelope (family 3) ────────────────────────────────
    //
    // The process-wide `PodHardening` is one envelope for every task this
    // scheduler dispatches. These cover the seam that lets one task be
    // sandboxed while its neighbour is not.

    fn iso() -> dagron_core::isolation::IsolationSpec {
        dagron_core::isolation::IsolationSpec::default()
    }

    #[test]
    fn a_task_envelope_hardens_one_pod_without_touching_the_process_default() {
        use dagron_core::isolation::Seccomp;

        // The operator set nothing: today every task pod is unhardened.
        let base = PodHardening::default();
        let untrusted = base.with_isolation(&dagron_core::isolation::IsolationSpec {
            runtime_class: Some("gvisor".into()),
            seccomp: Some(Seccomp::RuntimeDefault),
            read_only_root_fs: Some(true),
            drop_all_capabilities: Some(true),
            run_as_non_root: Some(true),
            ..iso()
        });

        let mut sandboxed = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut sandboxed, &untrusted, false);
        assert_eq!(sandboxed["spec"]["runtimeClassName"], "gvisor");
        assert_eq!(sandboxed["spec"]["securityContext"]["runAsNonRoot"], true);
        assert_eq!(
            sandboxed["spec"]["securityContext"]["seccompProfile"]["type"],
            "RuntimeDefault"
        );
        let csc = &sandboxed["spec"]["containers"][0]["securityContext"];
        assert_eq!(csc["readOnlyRootFilesystem"], true);
        assert_eq!(csc["capabilities"]["drop"][0], "ALL");

        // The neighbouring task, same scheduler, same process env: untouched.
        let mut plain = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut plain, &base, false);
        assert!(plain["spec"].get("runtimeClassName").is_none());
        assert!(plain["spec"].get("securityContext").is_none());
    }

    #[test]
    fn an_unset_envelope_field_leaves_the_operators_process_default_alone() {
        let operator = PodHardening {
            read_only_root_fs: true,
            runtime_class: Some("gvisor".into()),
            active_deadline_secs: Some(600),
            ..PodHardening::default()
        };
        // The task asks only about capabilities.
        let merged = operator.with_isolation(&dagron_core::isolation::IsolationSpec {
            drop_all_capabilities: Some(true),
            ..iso()
        });
        assert!(merged.read_only_root_fs, "the operator's setting must survive");
        assert_eq!(merged.runtime_class.as_deref(), Some("gvisor"));
        assert_eq!(merged.active_deadline_secs, Some(600));
        assert!(merged.drop_all_capabilities);
    }

    #[test]
    fn no_new_privileges_can_be_asked_for_on_its_own() {
        // Previously `allowPrivilegeEscalation: false` only appeared as a side
        // effect of a read-only root or dropped capabilities; an envelope that
        // wants just this must get just this.
        let h = PodHardening::default().with_isolation(&dagron_core::isolation::IsolationSpec {
            no_new_privileges: Some(true),
            ..iso()
        });
        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &h, false);
        let csc = &pod["spec"]["containers"][0]["securityContext"];
        assert_eq!(csc["allowPrivilegeEscalation"], false);
        assert!(csc.get("readOnlyRootFilesystem").is_none());
        assert!(csc.get("capabilities").is_none());
    }

    #[test]
    fn run_as_non_root_without_a_uid_is_honoured() {
        // Pinning a uid can break an image that has no filesystem permissions
        // for it; refusing root is the half that always applies.
        let h = PodHardening::default().with_isolation(&dagron_core::isolation::IsolationSpec {
            run_as_non_root: Some(true),
            ..iso()
        });
        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &h, false);
        assert_eq!(pod["spec"]["securityContext"]["runAsNonRoot"], true);
        assert!(pod["spec"]["securityContext"].get("runAsUser").is_none());
    }

    #[test]
    fn an_envelope_may_grant_the_service_account_token_the_process_withholds() {
        // `automount_sa_token` defaults false, so a task with no
        // `service_account:` gets no token. A task that genuinely needs one --
        // and whose envelope was already floored by the operator -- can say so.
        let h = PodHardening::default().with_isolation(&dagron_core::isolation::IsolationSpec {
            service_account_token: Some(true),
            ..iso()
        });
        let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
        apply_hardening(&mut pod, &h, false);
        assert!(
            pod["spec"].get("automountServiceAccountToken").is_none(),
            "the token is not withheld when the envelope asked for it"
        );
    }

    /// The labels must actually reach the manifest — everything the reaper does
    /// is a label selector, so an unlabelled pod is an unreachable pod.
    #[test]
    fn a_pod_carries_the_task_identity_it_was_dispatched_for() {
        let mut ctx = ExecContext::new(vec!["sh".into()], None, Some("alpine:3.19".into()));
        ctx.identity = Some(crate::executor::TaskIdentity {
            task_id: "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e".into(),
            run_id: "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f".into(),
            attempt: 2,
            fence: 2,
            installation: None,
        });
        let pod = build_pod("sched-x", "alpine:3.19", &ctx.command, &ctx).unwrap();
        let labels = pod.metadata.labels.expect("pod is labelled");

        assert_eq!(labels.get(crate::executor::LABEL_TASK_ID).map(String::as_str),
                   Some("0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e"));
        assert_eq!(labels.get(crate::executor::LABEL_ATTEMPT).map(String::as_str), Some("2"));
        assert_eq!(labels.get(crate::executor::LABEL_MANAGED_BY).map(String::as_str),
                   Some(crate::executor::MANAGED_BY));
        // The pre-existing selector stays — operators and any dashboards built
        // on it must not break because identity labels arrived beside it.
        assert_eq!(labels.get("app").map(String::as_str), Some("module-54-scheduler"));
    }

    /// The watch of task pods selects on a label, and tells a task of its
    /// pod only if the pod carries it. Every pod this executor builds does,
    /// with an identity and without one.
    #[test]
    fn every_task_pod_carries_the_label_the_watch_selects_on() {
        let (key, value) = crate::kube_watch::TASK_PODS
            .split_once('=')
            .expect("key=value");
        let known = crate::executor::TaskIdentity {
            task_id: "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e".into(),
            run_id: "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f".into(),
            attempt: 1,
            fence: 1,
            installation: None,
        };
        for identity in [None, Some(known)] {
            let mut ctx = ExecContext::new(vec!["true".into()], None, Some("alpine:3.19".into()));
            ctx.identity = identity;
            let pod = build_pod("sched-x", "alpine:3.19", &ctx.command, &ctx).unwrap();
            let labels = pod.metadata.labels.expect("pod is labelled");
            assert_eq!(labels.get(key).map(String::as_str), Some(value));
        }
    }

    #[test]
    fn the_pod_watch_is_on_unless_it_is_turned_off() {
        assert_eq!(parse_pod_watch(""), Ok(true));
        assert_eq!(parse_pod_watch(" on "), Ok(true));
        assert_eq!(parse_pod_watch("off"), Ok(false));
        assert!(parse_pod_watch("false").unwrap_err().contains("'false'"));
    }

    /// Which of a task's pods a dispatch deletes and waits on before it makes
    /// its own: those its labels prove are from an earlier claim, by the
    /// fence, and by the attempt for a pod made before pods carried one.
    #[test]
    fn a_pod_is_a_predecessor_by_its_fence_and_no_other_is_touched() {
        let mine = crate::executor::TaskIdentity {
            task_id: "0b5d8f2e-3a41-4c7b-9e60-1f2a3b4c5d6e".into(),
            run_id: "9f8e7d6c-5b4a-4938-8271-0a1b2c3d4e5f".into(),
            attempt: 1,
            fence: 9,
            installation: None,
        };
        let pod = |name: &str, labels: &[(&str, &str)]| Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()),
                ..Default::default()
            },
            ..Default::default()
        };
        let (fence, attempt) = (crate::executor::LABEL_FENCE, crate::executor::LABEL_ATTEMPT);
        // What a cancelled attempt left: the attempt a rerun's first dispatch
        // carries too, and a lower fence.
        assert_eq!(
            predecessor_name(pod("cancelled", &[(fence, "4"), (attempt, "1")]), &mine).as_deref(),
            Some("cancelled")
        );
        // This dispatch's own, and a newer claim's whose attempt is the lower.
        assert_eq!(predecessor_name(pod("own", &[(fence, "9"), (attempt, "1")]), &mine), None);
        assert_eq!(predecessor_name(pod("newer", &[(fence, "12"), (attempt, "0")]), &mine), None);
        // From before pods carried a fence: judged by its attempt, as it was.
        assert_eq!(
            predecessor_name(pod("old-style", &[(attempt, "0")]), &mine).as_deref(),
            Some("old-style")
        );
        assert_eq!(predecessor_name(pod("old-same", &[(attempt, "1")]), &mine), None);
        // No proof, no delete.
        assert_eq!(predecessor_name(pod("bare", &[]), &mine), None);
        assert_eq!(predecessor_name(pod("odd", &[(fence, "four")]), &mine), None);
    }

    /// No identity (tests, the no-op fallback) must still build a valid pod —
    /// with the original label and nothing half-written.
    #[test]
    fn a_pod_without_an_identity_keeps_the_original_label_only() {
        let ctx = ExecContext::new(vec!["sh".into()], None, Some("alpine:3.19".into()));
        let pod = build_pod("sched-x", "alpine:3.19", &ctx.command, &ctx).unwrap();
        let labels = pod.metadata.labels.expect("app label survives");
        assert_eq!(labels.get("app").map(String::as_str), Some("module-54-scheduler"));
        assert!(!labels.contains_key(crate::executor::LABEL_MANAGED_BY),
                "an unowned pod must not claim to be managed — the reaper selects on it");
    }

    /// A pod for a task with `network` as given, on an engine whose exec
    /// image is `exec`; with a workspace where `workspace`.
    fn pod_with_network(
        network: Option<dagron_core::isolation::Network>,
        exec: Option<&str>,
        workspace: bool,
    ) -> Result<Pod> {
        let mut ctx = ExecContext::new(vec!["sh".into()], None, None);
        ctx.isolation = network.map(|n| dagron_core::isolation::IsolationSpec {
            network: Some(n),
            drop_all_capabilities: Some(true),
            ..iso()
        });
        if workspace {
            ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec::default());
        }
        let workspaces = KubeWorkspaces {
            ci_exec_image: exec.map(str::to_string),
            ..Default::default()
        };
        build_pod_with_secrets(
            "sched-net",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::default(),
            &workspaces,
        )
    }

    /// A task that asked for no network is labelled for the policy that takes
    /// it, and no other task is: a pod with the label has no network, so one
    /// that did not ask must not carry it.
    #[test]
    fn a_task_that_asked_for_no_network_is_labelled_for_the_policy() {
        use dagron_core::isolation::Network;
        let label_of = |network: Option<Network>| {
            let pod = pod_with_network(network, Some("exec:1"), false).unwrap();
            pod.metadata
                .labels
                .unwrap_or_default()
                .get(NETWORK_LABEL)
                .cloned()
        };
        assert_eq!(label_of(Some(Network::None)).as_deref(), Some("none"));
        assert_eq!(label_of(Some(Network::Default)), None);
        assert_eq!(label_of(None), None);
        // One that named an allowlist is labelled for that allowlist's.
        assert_eq!(
            label_of(Some(Network::Allow("registry".into()))).as_deref(),
            Some("allow-registry")
        );
        // The longest name there can be is a label a pod can carry.
        let longest = "a".repeat(dagron_core::isolation::ALLOWLIST_NAME_MAX);
        let label = label_of(Some(Network::Allow(longest))).unwrap();
        assert_eq!(label.len(), 63);
    }

    /// A task held to an allowlist starts behind a gate of its own, last of
    /// its init containers: the command that asks the API server alone. And
    /// it has no pod without the image the gate is from.
    #[test]
    fn a_task_that_named_an_allowlist_starts_behind_its_gate() {
        use dagron_core::isolation::Network;
        let allow = || Some(Network::Allow("registry".into()));
        let pod = pod_with_network(allow(), Some("exec:1"), true).unwrap();
        let inits = pod.spec.unwrap().init_containers.unwrap_or_default();
        let names: Vec<&str> = inits.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, [crate::workspace::INIT_NAME, ALLOWLIST_GATE_NAME]);
        let gate = inits.last().unwrap();
        assert_eq!(gate.image.as_deref(), Some("exec:1"));
        assert_eq!(
            gate.args.as_deref(),
            Some(&["wait-allowlist".to_string(), "30".to_string()][..])
        );

        let err = pod_with_network(allow(), None, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("isolation.network: allow:registry")
                && err.contains("DAGRON_CI_EXEC_IMAGE"),
            "{err}"
        );
    }

    /// And its pod ends its init containers with the gate: the exec image,
    /// asked to wait until the pod's network is gone, held to the task's
    /// envelope as every init container is. After the one that installs the
    /// task's tools, where it has a workspace, so that nothing of the pod
    /// runs between the gate and the task. No other task has one.
    #[test]
    fn a_task_that_asked_for_no_network_starts_behind_the_gate() {
        use dagron_core::isolation::Network;
        let inits = |network, workspace| {
            pod_with_network(network, Some("exec:1"), workspace)
                .unwrap()
                .spec
                .unwrap()
                .init_containers
                .unwrap_or_default()
        };
        let alone = inits(Some(Network::None), false);
        assert_eq!(alone.len(), 1);
        let gate = &alone[0];
        assert_eq!(gate.name, GATE_NAME);
        assert_eq!(gate.image.as_deref(), Some("exec:1"));
        assert_eq!(
            gate.args.as_deref(),
            Some(&["wait-no-network".to_string(), "30".to_string()][..])
        );
        let held = gate.security_context.as_ref().expect("the task's envelope");
        assert_eq!(
            held.capabilities.as_ref().unwrap().drop.as_ref().unwrap()[0],
            "ALL"
        );

        let names = |network, workspace| -> Vec<String> {
            inits(network, workspace)
                .into_iter()
                .map(|c| c.name)
                .collect()
        };
        assert_eq!(
            names(Some(Network::None), true),
            [crate::workspace::INIT_NAME, GATE_NAME],
            "the gate is last"
        );
        assert_eq!(names(None, true), [crate::workspace::INIT_NAME]);
        assert!(names(Some(Network::Default), false).is_empty());
        assert!(names(None, false).is_empty());
    }

    /// Without an image to run the gate from, such a task has no pod made
    /// for it: one that started ungated would be the task this is for
    /// refusing.
    #[test]
    fn a_task_that_asked_for_no_network_has_no_pod_without_the_gate_s_image() {
        use dagron_core::isolation::Network;
        let err = pod_with_network(Some(Network::None), None, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("isolation.network: none") && err.contains("DAGRON_CI_EXEC_IMAGE"),
            "{err}"
        );
        // One that asked for nothing needs none.
        pod_with_network(None, None, false).unwrap();
    }

    /// A gate that failed says what it is: a pod that kept its network, or
    /// an image from before the gate. Neither reads as the task's failure.
    #[test]
    fn a_gate_that_failed_says_why_the_task_did_not_start() {
        let kept = gate_failure(
            "this pod still has a network: 10.43.0.1:443, the cluster's API server, answered 30 s after the wait began\n",
            125,
            Some("exec:1"),
            Asked::Nothing,
        );
        assert!(
            kept.contains("was not seen to have none within 30 s")
                && kept.contains("10.43.0.1:443")
                && kept.contains(NO_NETWORK_POLICY)
                && kept.contains("The task was not started"),
            "{kept}"
        );
        let old = gate_failure(
            "dagron-ci-exec: unknown arguments\nusage: …",
            125,
            Some("exec:0.11"),
            Asked::Nothing,
        );
        assert!(
            old.contains("exec:0.11 does not know `wait-no-network`")
                && old.contains("older than this engine"),
            "{old}"
        );
    }

    /// Under an allowlist a failed gate says what an allowlist may not allow,
    /// and names that allowlist's policy and its own command, not the other's.
    #[test]
    fn a_failed_gate_says_what_an_allowlist_may_not_allow() {
        let asked = Asked::Allowlist("registry");
        let reached = gate_failure(
            "this pod still reaches 10.43.0.1:443, the cluster's API server, which answered 30 s after the wait began\n",
            125,
            Some("exec:1"),
            asked,
        );
        assert!(
            reached.contains("isolation.network: allow:registry")
                && reached.contains("was not seen to be held within 30 s")
                && reached.contains("an allowlist must not allow that address")
                && reached.contains("'dagron-task-allow-registry'")
                && !reached.contains(NO_NETWORK_POLICY)
                && reached.contains("The task was not started"),
            "{reached}"
        );
        let old = gate_failure(
            "dagron-ci-exec: unknown arguments\nusage: …",
            125,
            Some("exec:0.12"),
            asked,
        );
        assert!(
            old.contains("exec:0.12 does not know `wait-allowlist`")
                && old.contains("older than this engine"),
            "{old}"
        );
        let killed = gate_failure("", 137, Some("exec:1"), asked);
        assert!(
            killed.contains("('wait-allowlist') ended with exit code 137")
                && killed.contains("the gate's own failure")
                && !killed.contains("must not allow"),
            "{killed}"
        );
        // The other gate's words are not this one's evidence.
        let other = gate_failure(
            "this pod still has a network: x",
            125,
            Some("exec:1"),
            asked,
        );
        assert!(other.contains("the gate's own failure"), "{other}");
    }

    /// The allowlists an operator names are read as names, each once, and a
    /// value that could not be a name stops the engine as it starts.
    #[test]
    fn the_allowlists_setting_is_a_list_of_names() {
        assert_eq!(parse_network_allowlists(""), Ok(vec![]));
        assert_eq!(parse_network_allowlists(" , "), Ok(vec![]));
        assert_eq!(
            parse_network_allowlists("registry, forge,registry"),
            Ok(vec!["registry".to_string(), "forge".to_string()])
        );
        for wrong in ["Registry", "a b", "-a", "registry,for.ge", "allow:registry"] {
            assert!(parse_network_allowlists(wrong).is_err(), "{wrong}");
        }
    }

    /// An allowlist's policy may say the proxy the operator's allowlist
    /// reaches names through, in an annotation, and nothing that could carry
    /// the proxy's credentials. What it says is set in the task's container
    /// before the task's own environment, each name the task set left as the
    /// task set it.
    /// A privileged task's own container is privileged, and its pod's other
    /// containers are not. A read-only root asked of it stays, and the
    /// `allowPrivilegeEscalation: false` it implied, which the API refuses
    /// beside `privileged`, goes.
    #[test]
    fn a_privileged_task_s_container_alone_is_privileged() {
        let mut ctx = ExecContext::new(vec!["sh".into()], None, Some("alpine:3.19".into()));
        ctx.privileged = true;
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            read_only_root_fs: Some(true),
            ..iso()
        });
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec::default());
        let workspaces = KubeWorkspaces {
            ci_exec_image: Some("exec:1".into()),
            ..Default::default()
        };
        let pod = build_pod_with_secrets(
            "sched-p",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::default(),
            &workspaces,
        )
        .unwrap();
        let spec = pod.spec.unwrap();
        let task = spec.containers.iter().find(|c| c.name == TASK_CONTAINER).unwrap();
        let sc = task.security_context.clone().unwrap();
        assert_eq!(sc.privileged, Some(true));
        assert_eq!(sc.allow_privilege_escalation, None);
        assert_eq!(sc.read_only_root_filesystem, Some(true));
        for other in spec.init_containers.iter().flatten() {
            let sc = other.security_context.clone().unwrap_or_default();
            assert_ne!(sc.privileged, Some(true), "{} is not the task's", other.name);
        }

        ctx.privileged = false;
        let plain = build_pod("sched-q", "alpine:3.19", &ctx.command, &ctx).unwrap();
        let task = plain.spec.unwrap().containers.into_iter().find(|c| c.name == TASK_CONTAINER).unwrap();
        assert_ne!(task.security_context.unwrap_or_default().privileged, Some(true));
    }

    #[test]
    fn an_allowlist_s_proxy_is_its_policy_s_annotation_and_told_to_the_task() {
        let annotated = |value: &str| {
            let mut policy = allowlist_policy("registry", serde_json::json!([]));
            policy.metadata.annotations = Some(
                [(dagron_core::isolation::ALLOWLIST_PROXY.to_string(), value.to_string())].into(),
            );
            policy
        };
        assert_eq!(
            allowlist_proxy(&annotated("http://egress.dagron.svc:3128")),
            Ok(Some("http://egress.dagron.svc:3128".into()))
        );
        assert_eq!(
            allowlist_proxy(&allowlist_policy("registry", serde_json::json!([]))),
            Ok(None)
        );
        let refused = allowlist_proxy(&annotated("http://u:p@egress:3128")).unwrap_err();
        assert!(refused.contains("dagron.dev/proxy"), "{refused}");

        let mut ctx = ExecContext::new(vec!["true".into()], None, Some("alpine:3.19".into()));
        ctx.env.push(dagron_core::dag::EnvVar {
            name: "NO_PROXY".into(),
            value: "internal.example".into(),
            value_from: None,
        });
        let told = vec![dagron_core::dag::EnvVar {
            name: "HTTPS_PROXY".into(),
            value: "http://egress:3128".into(),
            value_from: None,
        }];
        let pod = build_pod_told(
            "sched-abc",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::allow_all(),
            &KubeWorkspaces::default(),
            &told,
        )
        .unwrap();
        let env: Vec<(String, Option<String>)> = pod.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.value))
            .collect();
        assert_eq!(
            env,
            [
                ("HTTPS_PROXY".to_string(), Some("http://egress:3128".to_string())),
                ("NO_PROXY".to_string(), Some("internal.example".to_string())),
            ]
        );
    }

    /// The pod's policy holds its fetches as it holds the task: what its
    /// allowlist tells the task of a proxy, the checkout's and the actions'
    /// fetches are told too, before their own variables and never over one.
    #[test]
    fn an_allowlist_s_proxy_is_told_to_the_pod_s_fetches() {
        let mut ctx = ExecContext::new(vec!["true".into()], None, Some("alpine:3.19".into()));
        // The task's own setting is the task's alone: its fetches, which are
        // not given its variables, have the operator's.
        ctx.env.push(dagron_core::dag::EnvVar {
            name: "HTTPS_PROXY".into(),
            value: "http://own:8080".into(),
            value_from: None,
        });
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec {
            checkout: Some(dagron_core::workspace::CheckoutSpec {
                repository: "https://github.com/acme/widgets.git".into(),
                commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
                depth: None,
                patched: false,
            }),
            actions: vec![serde_json::from_value(serde_json::json!({
                "repository": "https://github.com/actions/checkout.git",
                "ref": "v4",
                "path": "actions-checkout",
            }))
            .unwrap()],
            ..Default::default()
        });
        let told = vec![
            dagron_core::dag::EnvVar {
                name: "HTTPS_PROXY".into(),
                value: "http://egress:3128".into(),
                value_from: None,
            },
            dagron_core::dag::EnvVar {
                name: "GIT_TERMINAL_PROMPT".into(),
                value: "told".into(),
                value_from: None,
            },
        ];
        let workspaces = KubeWorkspaces {
            checkout_image: Some("alpine/git:v2.47.2".into()),
            ..Default::default()
        };
        let pod = build_pod_told(
            "sched-abc",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::allow_all(),
            &workspaces,
            &told,
        )
        .unwrap();
        let spec = pod.spec.unwrap();
        let task_env = spec.containers[0].env.clone().unwrap_or_default();
        let task_proxies: Vec<_> = task_env
            .iter()
            .filter(|e| e.name == "HTTPS_PROXY")
            .map(|e| e.value.clone().unwrap_or_default())
            .collect();
        assert_eq!(task_proxies, ["http://own:8080"], "the task's own, once");
        let inits = spec.init_containers.unwrap();
        for fetch in [crate::checkout::CONTAINER_NAME, crate::checkout::ACTIONS_CONTAINER_NAME] {
            let init = inits.iter().find(|c| c.name == fetch).unwrap_or_else(|| panic!("{fetch}"));
            let env = init.env.clone().unwrap_or_default();
            let value = |name: &str| {
                let found: Vec<_> = env.iter().filter(|e| e.name == name).collect();
                assert!(found.len() <= 1, "{fetch}: {name} set once");
                found.first().and_then(|e| e.value.clone())
            };
            assert_eq!(value("HTTPS_PROXY").as_deref(), Some("http://egress:3128"), "{fetch}");
            assert_ne!(value("GIT_TERMINAL_PROMPT").as_deref(), Some("told"), "{fetch}: its own kept");
        }
    }

    /// A task's services are in its pod, under its policy: each is told of
    /// the allowlist's proxy as the task is, before its own variables and
    /// never over one.
    #[test]
    fn an_allowlist_s_proxy_is_told_to_the_pod_s_services() {
        let mut ctx = ExecContext::new(vec!["true".into()], None, Some("alpine:3.19".into()));
        ctx.services = vec![serde_json::from_value(serde_json::json!({
            "name": "browser",
            "image": "selenium/standalone-chrome:4",
            "env": [{ "name": "NO_PROXY", "value": "own" }],
        }))
        .unwrap()];
        let told = vec![
            dagron_core::dag::EnvVar {
                name: "HTTPS_PROXY".into(),
                value: "http://egress:3128".into(),
                value_from: None,
            },
            dagron_core::dag::EnvVar {
                name: "NO_PROXY".into(),
                value: "localhost".into(),
                value_from: None,
            },
        ];
        let pod = build_pod_told(
            "sched-abc",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::allow_all(),
            &KubeWorkspaces::default(),
            &told,
        )
        .unwrap();
        let inits = pod.spec.unwrap().init_containers.unwrap();
        let browser = inits.iter().find(|c| c.name.ends_with("browser")).unwrap();
        let env: Vec<(String, Option<String>)> = browser
            .env
            .clone()
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.value))
            .collect();
        assert_eq!(
            env,
            [
                ("HTTPS_PROXY".to_string(), Some("http://egress:3128".to_string())),
                ("NO_PROXY".to_string(), Some("own".to_string())),
            ]
        );
    }

    /// An allowlist's policy is read, and not taken on its name: it holds
    /// the pods labelled for that allowlist and no others, in both
    /// directions, lets nothing in, and each egress rule says where to.
    /// What the rules name is the operator's.
    #[test]
    fn an_allowlist_s_policy_holds_its_own_pods_to_a_list() {
        let name = "registry";
        let to_registry = serde_json::json!([{
            "to": [{"ipBlock": {"cidr": "10.1.2.3/32"}}],
            "ports": [{"protocol": "TCP", "port": 443}],
        }]);
        let policy = |spec: serde_json::Value| -> NetworkPolicy {
            serde_json::from_value(serde_json::json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": allowlist_policy_name(name)},
                "spec": spec,
            }))
            .unwrap()
        };
        let selector = serde_json::json!({"matchLabels": {NETWORK_LABEL: "allow-registry"}});
        let both = serde_json::json!(["Ingress", "Egress"]);

        // The policy this module writes is one it accepts, under the name it
        // asks the namespace for, and its example reads back as one.
        let written = allowlist_policy(name, to_registry.clone());
        assert_eq!(
            written.metadata.name.as_deref(),
            Some("dagron-task-allow-registry")
        );
        assert_eq!(holds_to_an_allowlist(&written, name), Ok(()));
        let example: NetworkPolicy =
            serde_json::from_str(&allowlist_policy_manifest(name)).unwrap();
        assert_eq!(holds_to_an_allowlist(&example, name), Ok(()));
        // Rules of other kinds are the operator's: pods, namespaces, several.
        assert_eq!(
            holds_to_an_allowlist(
                &policy(serde_json::json!({
                    "podSelector": selector, "policyTypes": both,
                    "egress": [
                        {"to": [{"podSelector": {"matchLabels": {"app": "registry"}}}]},
                        {"to": [{"namespaceSelector": {}, "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
                         "ports": [{"protocol": "UDP", "port": 53}]},
                    ],
                })),
                name
            ),
            Ok(())
        );
        // An allowlist of nothing is a list.
        assert_eq!(
            holds_to_an_allowlist(
                &policy(serde_json::json!({"podSelector": selector, "policyTypes": both})),
                name
            ),
            Ok(())
        );

        let refused =
            |spec: serde_json::Value| holds_to_an_allowlist(&policy(spec), name).unwrap_err();
        // Another allowlist's pods, the pods with no network, or every pod.
        for other in [
            serde_json::json!({"matchLabels": {NETWORK_LABEL: "allow-forge"}}),
            serde_json::json!({"matchLabels": {NETWORK_LABEL: "none"}}),
            serde_json::json!({"matchLabels": {NETWORK_LABEL: "allow-registry", "team": "a"}}),
            serde_json::json!({}),
        ] {
            assert!(
                refused(serde_json::json!({
                    "podSelector": other, "policyTypes": both, "egress": to_registry,
                }))
                .contains("podSelector"),
                "{other}"
            );
        }
        // A direction left alone: without Egress among its types the rules
        // are not what holds the pod.
        assert!(refused(serde_json::json!({
            "podSelector": selector, "policyTypes": ["Ingress"], "egress": to_registry,
        }))
        .contains("lack Egress"));
        assert!(refused(serde_json::json!({
            "podSelector": selector, "policyTypes": ["Egress"], "egress": to_registry,
        }))
        .contains("lack Ingress"));
        // Something let in.
        assert!(refused(serde_json::json!({
            "podSelector": selector, "policyTypes": both, "egress": to_registry,
            "ingress": [{}],
        }))
        .contains("allows some ingress"));
        // A rule that says no where: every address, on its ports or on all.
        for nowhere in [
            serde_json::json!({}),
            serde_json::json!({"ports": [{"protocol": "TCP", "port": 443}]}),
            serde_json::json!({"to": []}),
        ] {
            let why = refused(serde_json::json!({
                "podSelector": selector, "policyTypes": both,
                "egress": [{"to": [{"ipBlock": {"cidr": "10.1.2.3/32"}}]}, nowhere],
            }));
            assert!(
                why.contains("egress rule 2 names no destination"),
                "{nowhere}: {why}"
            );
        }
        // And the policy that takes a pod's network is not an allowlist's,
        // nor the other way about.
        assert!(holds_to_an_allowlist(&no_network_policy(), name).is_err());
        assert!(takes_the_network(&written).is_err());
    }

    /// A pod held to an allowlist is judged by the namespace's policies as
    /// one with no network is: its own policy has to be there and be one,
    /// and no other may select the pod and allow something. Another
    /// allowlist's policy, and the policy that takes a pod's network, select
    /// other pods and are no one's concern.
    #[test]
    fn a_namespace_s_policies_hold_a_pod_to_its_allowlist_and_no_more() {
        let asked = Asked::Allowlist("registry");
        let pod = labels_of_a_held_pod(asked);
        assert_eq!(
            pod.get(NETWORK_LABEL).map(String::as_str),
            Some("allow-registry")
        );
        let rule = serde_json::json!([{"to": [{"ipBlock": {"cidr": "10.1.2.3/32"}}]}]);
        let own = allowlist_policy("registry", rule.clone());
        let beside = vec![
            own.clone(),
            allowlist_policy("forge", rule.clone()),
            no_network_policy(),
        ];
        assert_eq!(held_by(&beside, &pod, asked), Ok(()));
        // And each of those is held beside the others, in its turn.
        let forge = Asked::Allowlist("forge");
        assert_eq!(
            held_by(&beside, &labels_of_a_held_pod(forge), forge),
            Ok(())
        );
        let none = Asked::Nothing;
        assert_eq!(held_by(&beside, &labels_of_a_held_pod(none), none), Ok(()));

        // Its policy not there: the others do not stand in for it.
        let why = held_by(&beside[1..], &pod, asked).unwrap_err();
        assert!(
            why.contains("'dagron-task-allow-registry' is not there")
                && why.contains("engine.taskNetworkAllowlists")
                && why.contains("\"dagron.dev/network\": \"allow-registry\""),
            "{why}"
        );
        // Its policy with a rule that says no where.
        let open = allowlist_policy("registry", serde_json::json!([{}]));
        let why = held_by(&[open], &pod, asked).unwrap_err();
        assert!(
            why.contains("is not one that holds a pod to a list")
                && why.contains("names no destination"),
            "{why}"
        );
        // A second policy over every pod widens the list.
        let dns: NetworkPolicy = serde_json::from_value(serde_json::json!({
            "apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
            "metadata": {"name": "allow-dns"},
            "spec": {"podSelector": {}, "policyTypes": ["Egress"],
                     "egress": [{"ports": [{"protocol": "UDP", "port": 53}]}]},
        }))
        .unwrap();
        let why = held_by(&[own, dns], &pod, asked).unwrap_err();
        assert!(
            why.contains("'allow-dns' (egress)")
                && why.contains("a task held to this allowlist would have that as well"),
            "{why}"
        );
    }

    /// A gate that ended any other way did not find a network, and does not
    /// say the cluster enforced nothing: killed for memory, never started,
    /// unable to ask, or gone with no word. Each reads as the gate's own
    /// failure, with its exit code and what it said.
    #[test]
    fn a_gate_that_could_not_tell_does_not_blame_the_cluster() {
        for (said, code) in [
            ("", 137),
            ("", 128),
            ("dagron-ci-exec: KUBERNETES_SERVICE_HOST is \"kubernetes\", which is no address\n", 125),
            ("dagron-ci-exec: this pod's network could not be asked after: Too many open files\n", 125),
        ] {
            let why = gate_failure(said, code, Some("exec:1"), Asked::Nothing);
            assert!(
                why.contains(&format!("exit code {code}"))
                    && why.contains("the gate's own failure")
                    && why.contains("The task was not started"),
                "{said:?}: {why}"
            );
            assert!(
                !why.contains("nothing on this cluster enforced") && !why.contains("within 30 s"),
                "{said:?}: {why}"
            );
            if said.is_empty() {
                assert!(why.contains("it left no word"), "{why}");
            } else {
                assert!(why.contains(said.trim()), "{why}");
            }
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A selector is read as the API reads one: every label and every
    /// expression has to hold, none of either is every pod, and an operator
    /// this does not know is taken to select.
    #[test]
    fn a_selector_is_read_as_the_api_reads_one() {
        let selector =
            |v: serde_json::Value| -> LabelSelector { serde_json::from_value(v).unwrap() };
        let pod = labels(&[("app", "module-54-scheduler"), (NETWORK_LABEL, "none")]);
        let yes = |v: serde_json::Value| selects(Some(&selector(v)), &pod);

        assert!(selects(None, &pod), "no selector is every pod");
        assert!(yes(serde_json::json!({})), "an empty one is every pod");
        assert!(yes(
            serde_json::json!({"matchLabels": {"app": "module-54-scheduler"}})
        ));
        assert!(!yes(serde_json::json!({"matchLabels": {"app": "other"}})));
        assert!(!yes(
            serde_json::json!({"matchLabels": {"app": "module-54-scheduler", "team": "a"}})
        ));
        let by = |key: &str, operator: &str, values: &[&str]| {
            yes(serde_json::json!({"matchExpressions": [
                {"key": key, "operator": operator, "values": values},
            ]}))
        };
        assert!(by("app", "In", &["x", "module-54-scheduler"]));
        assert!(!by("app", "In", &["x"]));
        assert!(!by("team", "In", &["a"]), "a label the pod does not carry");
        assert!(by("app", "NotIn", &["x"]));
        assert!(!by("app", "NotIn", &["module-54-scheduler"]));
        assert!(
            by("team", "NotIn", &["a"]),
            "a label the pod does not carry"
        );
        assert!(by(NETWORK_LABEL, "Exists", &[]));
        assert!(!by("team", "Exists", &[]));
        assert!(by("team", "DoesNotExist", &[]));
        assert!(!by(NETWORK_LABEL, "DoesNotExist", &[]));
        assert!(
            by("app", "Matches", &["x"]),
            "not understood: taken to select"
        );
        // Both halves have to hold.
        assert!(!yes(serde_json::json!({
            "matchLabels": {"app": "module-54-scheduler"},
            "matchExpressions": [{"key": NETWORK_LABEL, "operator": "DoesNotExist"}],
        })));
    }

    /// A pod has what any policy selecting it allows. So the policy that
    /// takes a task's network holds it only while no other of the namespace
    /// selects such a pod and allows something, and the engine says which
    /// does and how to keep it away.
    #[test]
    fn another_policy_that_selects_the_pod_and_allows_something_is_not_let_by() {
        let policy = |name: &str, spec: serde_json::Value| -> NetworkPolicy {
            serde_json::from_value(serde_json::json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": name},
                "spec": spec,
            }))
            .unwrap()
        };
        let none = Asked::Nothing;
        let held_by = |policies: &[NetworkPolicy], labels: &BTreeMap<String, String>| {
            held_by(policies, labels, none)
        };
        let pod = labels_of_a_held_pod(none);
        assert_eq!(pod.get(NETWORK_LABEL).map(String::as_str), Some("none"));
        let dns = serde_json::json!([{"ports": [{"protocol": "UDP", "port": 53}]}]);
        let own = no_network_policy();

        // Alone, and beside policies that select other pods or allow nothing.
        assert_eq!(held_by(std::slice::from_ref(&own), &pod), Ok(()));
        let harmless = vec![
            own.clone(),
            // The chart's: the engine's own pods, by their labels.
            policy(
                "dagron-engine",
                serde_json::json!({
                    "podSelector": {"matchLabels": {"app.kubernetes.io/component": "engine"}},
                    "policyTypes": ["Ingress"], "ingress": [{}],
                }),
            ),
            // A namespace's default deny: every pod, and nothing allowed.
            policy(
                "default-deny",
                serde_json::json!({
                    "podSelector": {}, "policyTypes": ["Ingress", "Egress"],
                }),
            ),
            // One that keeps away from the labelled pods, as the engine asks.
            policy(
                "allow-dns",
                serde_json::json!({
                    "podSelector": {"matchExpressions": [
                        {"key": NETWORK_LABEL, "operator": "DoesNotExist"},
                    ]},
                    "policyTypes": ["Egress"], "egress": dns,
                }),
            ),
        ];
        assert_eq!(held_by(&harmless, &pod), Ok(()));

        // Every pod may reach the name server: the commonest of them.
        let why = held_by(
            &[
                own.clone(),
                policy(
                    "allow-dns",
                    serde_json::json!({
                        "podSelector": {}, "policyTypes": ["Egress"], "egress": dns,
                    }),
                ),
            ],
            &pod,
        )
        .unwrap_err();
        assert!(
            why.contains("'allow-dns' (egress)")
                && why.contains("has what any policy that selects it allows")
                && why.contains(&format!("{{key: {NETWORK_LABEL}, operator: DoesNotExist}}")),
            "{why}"
        );
        // A fence over every pod without a chart's label, as a hosted
        // workspace has: out to the internet, and in from nowhere.
        let why = held_by(
            &[
                policy(
                    "task-fence",
                    serde_json::json!({
                        "podSelector": {"matchExpressions": [
                            {"key": "app.kubernetes.io/name", "operator": "DoesNotExist"},
                        ]},
                        "policyTypes": ["Ingress", "Egress"],
                        "ingress": [],
                        "egress": [{"to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]}],
                    }),
                ),
                own.clone(),
                policy(
                    "probes",
                    serde_json::json!({
                        "podSelector": {"matchLabels": {"app": "module-54-scheduler"}},
                        "ingress": [{"from": [{"namespaceSelector": {}}]}],
                        "egress": [{}],
                    }),
                ),
            ],
            &pod,
        )
        .unwrap_err();
        assert!(
            why.contains("'task-fence' (egress)") && why.contains("'probes' (egress and ingress)"),
            "{why}"
        );

        // Rules under a direction the policy is not of allow nothing: an
        // ingress policy with egress rules written into it, and an egress
        // one with ingress rules. With no types named it is of ingress, and
        // of egress where it has egress rules.
        let stray = |types: serde_json::Value| {
            policy(
                "stray",
                serde_json::json!({
                    "podSelector": {}, "policyTypes": types,
                    "ingress": [{}], "egress": [{}],
                }),
            )
        };
        assert_eq!(
            held_by(
                &[
                    own.clone(),
                    policy(
                        "stray",
                        serde_json::json!({
                            "podSelector": {}, "policyTypes": ["Ingress"], "egress": [{}],
                        })
                    )
                ],
                &pod
            ),
            Ok(()),
            "egress rules in a policy of ingress alone"
        );
        assert_eq!(
            held_by(
                &[
                    own.clone(),
                    policy(
                        "stray",
                        serde_json::json!({
                            "podSelector": {}, "policyTypes": ["Egress"], "ingress": [{}],
                        })
                    )
                ],
                &pod
            ),
            Ok(()),
            "ingress rules in a policy of egress alone"
        );
        let why = held_by(&[own.clone(), stray(serde_json::json!(["Ingress"]))], &pod).unwrap_err();
        assert!(why.contains("'stray' (ingress)"), "{why}");
        let why = held_by(&[own.clone(), stray(serde_json::json!(["Egress"]))], &pod).unwrap_err();
        assert!(why.contains("'stray' (egress)"), "{why}");
        let why = held_by(
            &[
                own.clone(),
                policy(
                    "stray",
                    serde_json::json!({"podSelector": {}, "ingress": [{}]}),
                ),
            ],
            &pod,
        )
        .unwrap_err();
        assert!(why.contains("'stray' (ingress)"), "no types named: {why}");

        // The policy itself gone, or no longer the one it is taken for.
        let why = held_by(&harmless[1..], &pod).unwrap_err();
        assert!(
            why.contains("is not there") && why.contains("kubectl apply"),
            "{why}"
        );
        let mut opened = own.clone();
        opened.spec.as_mut().unwrap().egress = serde_json::from_value(dns.clone()).unwrap();
        let why = held_by(&[opened], &pod).unwrap_err();
        assert!(
            why.contains("is not one that takes a pod's network")
                && why.contains("allows some egress"),
            "{why}"
        );
        // A pod that carries other labels is read by those: one selected by
        // a task's own id is caught when that task is dispatched.
        let mut one = pod.clone();
        one.insert(
            crate::executor::LABEL_TASK_ID.to_string(),
            "t-42".to_string(),
        );
        let by_task = policy(
            "one-task",
            serde_json::json!({
                "podSelector": {"matchLabels": {crate::executor::LABEL_TASK_ID: "t-42"}},
                "egress": [{}],
            }),
        );
        assert_eq!(held_by(&[own.clone(), by_task.clone()], &pod), Ok(()));
        assert!(held_by(&[own, by_task], &one).is_err());
    }

    #[test]
    fn the_network_policy_setting_is_enforced_or_nothing() {
        for unset in ["", "  ", "off"] {
            assert_eq!(parse_network_policy(unset), Ok(false), "{unset:?}");
        }
        assert_eq!(parse_network_policy(" enforced "), Ok(true));
        for wrong in ["true", "1", "enforce", "calico"] {
            assert!(parse_network_policy(wrong).is_err(), "{wrong}");
        }
    }

    /// The policy is read, and not taken on its name: one that allowed
    /// something, or selected other pods, would have a task run believing it
    /// reaches nothing, or take the network from tasks that did not ask.
    #[test]
    fn the_policy_is_one_that_takes_the_network_from_the_labelled_pods_alone() {
        let policy = |spec: serde_json::Value| -> NetworkPolicy {
            serde_json::from_value(serde_json::json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {"name": NO_NETWORK_POLICY},
                "spec": spec,
            }))
            .unwrap()
        };
        let selector = serde_json::json!({"matchLabels": {NETWORK_LABEL: "none"}});
        let both = serde_json::json!(["Ingress", "Egress"]);

        // The policy the engine prints is one it accepts, under the name it
        // asks the namespace for, and what it prints reads back as it.
        assert_eq!(takes_the_network(&no_network_policy()), Ok(()));
        let printed: NetworkPolicy = serde_json::from_str(&no_network_policy_manifest()).unwrap();
        assert_eq!(printed, no_network_policy());
        assert_eq!(printed.metadata.name.as_deref(), Some(NO_NETWORK_POLICY));
        assert_eq!(
            takes_the_network(&policy(serde_json::json!({
                "podSelector": selector, "policyTypes": both, "egress": [], "ingress": [],
            }))),
            Ok(())
        );

        let refused = |spec: serde_json::Value| takes_the_network(&policy(spec)).unwrap_err();
        // Every pod of the namespace, or other pods too.
        assert!(
            refused(serde_json::json!({"podSelector": {}, "policyTypes": both}))
                .contains("podSelector")
        );
        assert!(refused(serde_json::json!({
            "podSelector": {"matchLabels": {NETWORK_LABEL: "none", "team": "a"}},
            "policyTypes": both,
        }))
        .contains("podSelector"));
        assert!(refused(serde_json::json!({
            "podSelector": {"matchLabels": {NETWORK_LABEL: "open"}}, "policyTypes": both,
        }))
        .contains("podSelector"));
        // One direction left alone.
        assert!(
            refused(serde_json::json!({"podSelector": selector, "policyTypes": ["Ingress"]}))
                .contains("lack Egress")
        );
        assert!(refused(serde_json::json!({"podSelector": selector})).contains("lack Ingress"));
        // Something allowed.
        assert!(refused(serde_json::json!({
            "podSelector": selector, "policyTypes": both,
            "egress": [{"to": [{"ipBlock": {"cidr": "10.0.0.0/8"}}]}],
        }))
        .contains("allows some egress"));
        assert!(refused(serde_json::json!({
            "podSelector": selector, "policyTypes": both, "ingress": [{}],
        }))
        .contains("allows some ingress"));
    }

    #[test]
    fn build_pod_applies_the_context_envelope_end_to_end() {
        use dagron_core::isolation::Seccomp;

        let mut ctx = ExecContext::new(vec!["sh".into()], None, Some("alpine:3.19".into()));
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            runtime_class: Some("kata-qemu".into()),
            seccomp: Some(Seccomp::RuntimeDefault),
            drop_all_capabilities: Some(true),
            ..iso()
        });
        let pod = build_pod("sched-iso", "alpine:3.19", &ctx.command, &ctx).unwrap();
        let spec = pod.spec.expect("spec");
        assert_eq!(spec.runtime_class_name.as_deref(), Some("kata-qemu"));
        let csc = spec.containers[0].security_context.as_ref().expect("container securityContext");
        assert_eq!(csc.capabilities.as_ref().unwrap().drop.as_ref().unwrap()[0], "ALL");
    }

    /// The three cases `workshop/04-families/k8s/f3-floor-probe.yaml` submits,
    /// resolved here to the exact pod shapes `verify-k3s.sh` asserts against a
    /// live cluster.
    ///
    /// This test is why that script's expected values are not guesswork: the
    /// floor string below is the one in `k8s/10-engine.yaml`, the declarations
    /// are the ones in the probe workflow, and the assertions are what the
    /// cluster lane looks for. Change any of the three and this fails first,
    /// in CI, rather than as a confusing red lab on someone's EC2 box.
    #[test]
    fn the_floor_probe_resolves_to_the_pod_shapes_the_k3s_lane_asserts() {
        use dagron_core::isolation::{IsolationSpec, Seccomp};

        // Verbatim from workshop/04-families/k8s/10-engine.yaml.
        let floor = IsolationSpec::parse_floor(
            "seccomp=runtime_default,read_only_root_fs=true,no_new_privileges=true,             drop_all_capabilities=true,run_as_non_root=true,run_as_user=65534,             service_account_token=false",
        )
        .expect("the floor in the lab manifest must parse");

        // The engine applies the floor, then the executor shapes the pod.
        let shape = |declared: IsolationSpec| {
            let (effective, tightened) = declared.apply_floor(&floor);
            let h = PodHardening::default().with_isolation(&effective);
            let mut pod = serde_json::json!({"spec": {"containers": [{"name": "task"}]}});
            apply_hardening(&mut pod, &h, false);
            (pod, tightened)
        };

        // 1. silent-task — declares nothing, inherits the whole floor.
        let (silent, tightened) = shape(IsolationSpec::default());
        assert_eq!(silent["spec"]["securityContext"]["runAsUser"], 65534);
        assert_eq!(silent["spec"]["securityContext"]["runAsNonRoot"], true);
        assert_eq!(
            silent["spec"]["securityContext"]["seccompProfile"]["type"],
            "RuntimeDefault"
        );
        assert_eq!(silent["spec"]["automountServiceAccountToken"], false);
        let csc = &silent["spec"]["containers"][0]["securityContext"];
        assert_eq!(csc["readOnlyRootFilesystem"], true);
        assert_eq!(csc["allowPrivilegeEscalation"], false);
        assert_eq!(csc["capabilities"]["drop"][0], "ALL");
        assert!(
            tightened.iter().all(|t| t.requested == "unset"),
            "a silent task disagrees with nothing: {tightened:?}"
        );

        // 2. greedy-task — every field set weak. The floor must win on all of
        // them, and the pod must be indistinguishable from the silent one.
        let (greedy, tightened) = shape(IsolationSpec {
            seccomp: Some(Seccomp::Unconfined),
            read_only_root_fs: Some(false),
            no_new_privileges: Some(false),
            drop_all_capabilities: Some(false),
            run_as_non_root: Some(false),
            service_account_token: Some(true),
            ..Default::default()
        });
        assert_eq!(
            greedy, silent,
            "a workflow that asks for less must get exactly what the floor says"
        );
        // …and every override is reported, so the operator sees the attempt.
        let overridden: Vec<&str> = tightened
            .iter()
            .filter(|t| t.requested != "unset")
            .map(|t| t.field)
            .collect();
        for field in [
            "seccomp",
            "read_only_root_fs",
            "no_new_privileges",
            "drop_all_capabilities",
            "run_as_non_root",
            "service_account_token",
        ] {
            assert!(overridden.contains(&field), "{field} not reported: {tightened:?}");
        }

        // 3. paranoid-task — stricter than the floor. Its own non-root uid
        // survives; a floor that clamped it would be a ceiling.
        let (paranoid, tightened) = shape(IsolationSpec {
            read_only_root_fs: Some(true),
            drop_all_capabilities: Some(true),
            no_new_privileges: Some(true),
            run_as_non_root: Some(true),
            run_as_user: Some(30000),
            ..Default::default()
        });
        assert_eq!(
            paranoid["spec"]["securityContext"]["runAsUser"], 30000,
            "the task's own uid must survive the floor"
        );
        assert_eq!(paranoid["spec"]["automountServiceAccountToken"], false);
        assert_eq!(
            paranoid["spec"]["containers"][0]["securityContext"]["capabilities"]["drop"][0],
            "ALL"
        );
        assert!(
            tightened.iter().all(|t| t.requested == "unset"),
            "hardening beyond the floor is not a disagreement: {tightened:?}"
        );
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
        // No per-task knobs set → no env, no resources, no service account,
        // and no pull policy: the kubelet's default is left to apply.
        assert!(c.env.is_none());
        assert!(c.resources.is_none());
        assert!(spec.service_account_name.is_none());
        assert!(c.image_pull_policy.is_none());
    }

    /// Every image of a pod is pulled through its registry's mirror,
    /// and a trusted run's pod names the operator's pull secrets; one that
    /// is not trusted names none.
    #[test]
    fn a_pod_s_images_go_through_the_mirror_and_a_trusted_run_names_the_pull_secrets() {
        let registries = crate::images::Registries::parse(
            Some("*=mirror.local:5000,docker.io=mirror.local:5000/hub"),
            None,
        )
        .unwrap();
        let secrets = parse_pull_secrets("regcred, other.cred,regcred").unwrap();
        assert_eq!(secrets, ["regcred", "other.cred"]);
        let ctx = ExecContext::new(vec!["true".into()], None, None);
        let ws = KubeWorkspaces::default();
        let mut pod = build_pod_with_secrets(
            "sched-x",
            "alpine:3.20",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::default(),
            &ws,
        )
        .unwrap();
        through_registries(&mut pod, &ctx, &registries, &secrets);
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(
            spec.containers[0].image.as_deref(),
            Some("mirror.local:5000/hub/library/alpine:3.20")
        );
        let named: Vec<&str> = spec
            .image_pull_secrets
            .iter()
            .flatten()
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(named, ["regcred", "other.cred"]);

        let mut untrusted = ExecContext::new(vec!["true".into()], None, None);
        untrusted.confinement = Some(crate::executor::Confinement {
            memory_bytes: 1 << 30,
            pids: 512,
            nano_cpus: 1_000_000_000,
        });
        let mut pod = build_pod_with_secrets(
            "sched-y",
            "ghcr.io/o/i:1",
            &untrusted.command,
            &untrusted,
            None,
            &ServiceAccountPolicy::default(),
            &ws,
        )
        .unwrap();
        through_registries(&mut pod, &untrusted, &registries, &secrets);
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(
            spec.containers[0].image.as_deref(),
            Some("mirror.local:5000/ghcr.io/o/i:1")
        );
        assert!(
            spec.image_pull_secrets.is_none(),
            "a run that is not trusted names no pull secret"
        );
        assert_eq!(
            spec.containers[0].image_pull_policy.as_deref(),
            Some("Always"),
            "and pulls its own image, so the node's cache gives it no private one"
        );

        for bad in ["Reg", "-x", "a b", "x_y"] {
            assert!(parse_pull_secrets(bad).is_err(), "{bad}");
        }
    }

    /// A task's `image_pull_policy` is the pod's, in Kubernetes' spelling.
    #[test]
    fn build_pod_carries_the_task_s_image_pull_policy() {
        use dagron_core::dag::ImagePullPolicy;
        for (policy, written) in [
            (ImagePullPolicy::IfNotPresent, "IfNotPresent"),
            (ImagePullPolicy::Always, "Always"),
            (ImagePullPolicy::Never, "Never"),
        ] {
            let mut ctx = ExecContext::new(vec!["true".into()], None, Some("alpine:3.19".into()));
            ctx.image_pull_policy = Some(policy);
            let pod = build_pod("sched-abc", "alpine:3.19", &ctx.command, &ctx).unwrap();
            let c = &pod.spec.expect("spec").containers[0];
            assert_eq!(c.image_pull_policy.as_deref(), Some(written));
        }
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
            image_pull_policy: None,
            env: vec![EnvVar { name: "S3_BUCKET".into(), value: "dagron-lt".into(), value_from: None }],
            // `gpu` was added to ResourceRequirements and this test was never
            // updated, so the whole `kubernetes` test target stopped compiling --
            // which is why build_pod's missing securityContext went unnoticed for
            // so long: the tests that describe the pod's shape were dead.
            resources: Some(ResourceRequirements { requests, limits, gpu: None }),
            service_account: Some("dagron-etl".to_string()),
            isolation: None,
            workspace: None,
            services: Vec::new(),
            privileged: false,
            log_sink: None,
            identity: None,
            checkout_credential: None,
            checkout_patch: None,
            action_credentials: Vec::new(),
            forge_token: None,
            confinement: None,
            stop: crate::executor::Stop::default(),
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

    // ── A task may run only as a ServiceAccount it was granted ────────────

    /// Unset denies every request: a task that names a ServiceAccount is refused,
    /// so a workflow cannot run as the engine's ServiceAccount (or any other in
    /// the namespace) by naming it. The error names the account and the setting.
    #[test]
    fn a_service_account_is_refused_unless_allow_listed() {
        let policy = ServiceAccountPolicy::from_env_value(None);
        assert!(policy.check(None).is_ok(), "a task that names none runs as default");

        let err = policy.check(Some("dagron-engine")).unwrap_err().to_string();
        assert!(err.contains("dagron-engine"), "{err}");
        assert!(err.contains("DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS"), "{err}");
    }

    /// An operator's allow-list permits exactly the names on it, and nothing else —
    /// so listing the purpose-built IRSA identity does not also admit the engine's.
    #[test]
    fn the_allow_list_permits_only_its_names() {
        let policy = ServiceAccountPolicy::from_env_value(Some(" dagron-etl , dagron-loader "));
        assert!(policy.check(Some("dagron-etl")).is_ok());
        assert!(policy.check(Some("dagron-loader")).is_ok());
        assert!(policy.check(Some("dagron-engine")).is_err(), "the engine's SA is not on the list");
        assert!(policy.check(None).is_ok());
    }

    /// The gate is enforced where the pod is built, so a disallowed ServiceAccount
    /// is a build error and no pod is produced — not a pod that quietly runs as it.
    #[test]
    fn build_refuses_a_disallowed_service_account() {
        let mut ctx = ExecContext::new(vec!["true".into()], None, None);
        ctx.service_account = Some("dagron-engine".to_string());
        let denied = ServiceAccountPolicy::from_env_value(None);
        let ws = KubeWorkspaces::default();
        let err = build_pod_with_secrets("sched-x", "alpine", &ctx.command, &ctx, None, &denied, &ws)
            .unwrap_err()
            .to_string();
        assert!(err.contains("dagron-engine"), "{err}");

        let allowed = ServiceAccountPolicy::from_env_value(Some("dagron-engine"));
        let pod = build_pod_with_secrets("sched-x", "alpine", &ctx.command, &ctx, None, &allowed, &ws)
            .unwrap();
        assert_eq!(pod.spec.unwrap().service_account_name.as_deref(), Some("dagron-engine"));
    }

    /// A workspace task's pod, as the apiserver gets it: the workspace volume
    /// as its working directory, dagron-ci-exec installed by an init container
    /// held to the same hardening as the task, and nothing of either for a
    /// task that asked for no workspace.
    #[test]
    fn a_workspace_task_s_pod_installs_dagron_ci_exec_and_starts_in_its_workspace() {
        let ws = KubeWorkspaces {
            ci_exec_image: Some("dagron-ci-exec:1".into()),
            ..Default::default()
        };
        let mut ctx = ExecContext::new(vec!["dagron-ci-exec".into()], None, None);
        let plain = build_pod_with_secrets(
            "sched-x",
            "rust:1",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::allow_all(),
            &ws,
        )
        .unwrap()
        .spec
        .unwrap();
        assert!(
            plain.init_containers.is_none() && plain.volumes.is_none(),
            "no workspace asked for"
        );
        assert!(plain.containers[0].working_dir.is_none());

        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec {
            size: Some("5Gi".into()),
            ..Default::default()
        });
        let mut manifest = serde_json::to_value(
            build_pod_with_secrets(
                "sched-x",
                "rust:1",
                &ctx.command,
                &ctx,
                None,
                &ServiceAccountPolicy::allow_all(),
                &ws,
            )
            .unwrap(),
        )
        .unwrap();
        let spec = manifest["spec"].take();
        assert_eq!(spec["containers"][0]["workingDir"], "/workspace");
        assert_eq!(spec["volumes"][0]["emptyDir"]["sizeLimit"], "5Gi");
        assert_eq!(spec["initContainers"][0]["name"], "install-dagron-ci-exec");
        assert_eq!(spec["initContainers"][0]["args"], serde_json::json!(["install", "/dagron/bin"]));

        // Hardened like the task: the init container's securityContext is
        // shaped by the same pass. Applied here to the built spec, since the
        // builder reads the operator's settings from the environment.
        let h = PodHardening { read_only_root_fs: true, drop_all_capabilities: true, ..PodHardening::default() };
        let mut pod = serde_json::json!({"spec": spec});
        apply_hardening(&mut pod, &h, false);
        for c in ["initContainers", "containers"] {
            let csc = &pod["spec"][c][0]["securityContext"];
            assert_eq!(csc["readOnlyRootFilesystem"], true, "{c}");
            assert_eq!(csc["capabilities"]["drop"][0], "ALL", "{c}");
            assert_eq!(csc["allowPrivilegeEscalation"], false, "{c}");
        }
    }

    /// What the builder makes for a workspace task, put to a real apiserver
    /// as a dry-run create: it validates every field (the init container,
    /// the volumes, a size limit, a claim template, the read-only mount, the
    /// hardened security contexts) and creates nothing. Runs where
    /// `DAGRON_KUBE_DRY_RUN` names a namespace and the kubeconfig reaches a
    /// cluster; skips, saying so, otherwise.
    #[tokio::test]
    async fn a_real_apiserver_admits_each_workspace_pod_shape() {
        use dagron_core::dag::ResourceRequirements;
        use dagron_core::workspace::WorkspaceSpec;

        let Ok(namespace) = std::env::var("DAGRON_KUBE_DRY_RUN") else {
            return eprintln!("DAGRON_KUBE_DRY_RUN names no namespace - skipping");
        };
        crate::install_crypto_provider();
        let client = Client::try_default().await.expect("a kubeconfig that reaches a cluster");
        let pods: Api<Pod> = Api::namespaced(client, &namespace);
        let image = Some("dagron-ci-exec:1".to_string());
        let hardening = PodHardening {
            run_as_user: Some(65532),
            read_only_root_fs: true,
            drop_all_capabilities: true,
            seccomp_runtime_default: true,
            ..PodHardening::default()
        };
        let cases = [
            (
                "emptydir",
                KubeWorkspaces {
                    ci_exec_image: image.clone(),
                    ..Default::default()
                },
                WorkspaceSpec {
                    size: Some("5Gi".into()),
                    ..Default::default()
                },
            ),
            (
                "claim",
                KubeWorkspaces {
                    ci_exec_image: image.clone(),
                    pvc_from: Some(1 << 30),
                    storage_class: Some("local-path".into()),
                    ..Default::default()
                },
                WorkspaceSpec {
                    size: Some("20Gi".into()),
                    ..Default::default()
                },
            ),
            (
                "no-tools",
                KubeWorkspaces::default(),
                WorkspaceSpec::default(),
            ),
            (
                "checkout",
                KubeWorkspaces {
                    ci_exec_image: image.clone(),
                    checkout_image: Some("alpine/git:v2.47.2".into()),
                    ..Default::default()
                },
                WorkspaceSpec {
                    checkout: Some(dagron_core::workspace::CheckoutSpec {
                        repository: "https://github.com/acme/widgets.git".into(),
                        commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
                        depth: None,
                        patched: false,
                    }),
                    ..Default::default()
                },
            ),
        ];
        for (label, workspaces, ws) in cases {
            let mut ctx = ExecContext::new(vec!["sh".into(), "-c".into(), "true".into()], None, None);
            let fetches = ws.checkout.is_some();
            if fetches {
                ctx.checkout_credential = Some(crate::checkout::Credential {
                    username: "x-access-token".into(),
                    password: "ghs_dry".into(),
                });
            }
            ctx.workspace = Some(ws);
            ctx.resources = Some(ResourceRequirements {
                requests: [("cpu".to_string(), "100m".to_string()), ("memory".to_string(), "64Mi".to_string())]
                    .into_iter()
                    .collect(),
                limits: [("memory".to_string(), "128Mi".to_string())].into_iter().collect(),
                gpu: None,
            });
            let name = format!("dagron-ws-dry-{label}");
            // The credential by reference, as the executor gives it: the
            // Secret need not exist for admission.
            let secret = fetches.then(|| format!("{name}-env"));
            let built = build_pod_with_secrets(
                &name,
                "alpine:3.20",
                &ctx.command,
                &ctx,
                secret.as_deref(),
                &ServiceAccountPolicy::allow_all(),
                &workspaces,
            )
            .unwrap();
            let mut manifest = serde_json::to_value(&built).unwrap();
            apply_hardening(&mut manifest, &hardening, false);
            let pod: Pod = serde_json::from_value(manifest).unwrap();
            let admitted = pods
                .create(&PostParams { dry_run: true, ..PostParams::default() }, &pod)
                .await
                .unwrap_or_else(|e| panic!("{label}: the apiserver refused the pod: {e}"));
            let spec = admitted.spec.unwrap();
            assert_eq!(spec.containers[0].working_dir.as_deref(), Some("/workspace"), "{label}");
            let inits = spec.init_containers.unwrap_or_default();
            assert_eq!(
                inits.len(),
                usize::from(workspaces.ci_exec_image.is_some()) + usize::from(fetches),
                "{label}"
            );
            for init in &inits {
                let sc = init.security_context.as_ref().unwrap();
                assert_eq!(sc.read_only_root_filesystem, Some(true), "{label}");
            }
        }
        // And nothing was made.
        let left = pods.list(&ListParams::default()).await.unwrap();
        assert!(left.items.iter().all(|p| !p.metadata.name.as_deref().unwrap_or("").starts_with("dagron-ws-dry")));
    }

    #[test]
    fn a_failed_init_container_is_found_by_its_exit() {
        use k8s_openapi::api::core::v1::PodStatus;
        let status: PodStatus = serde_json::from_value(serde_json::json!({
            "phase": "Failed",
            "initContainerStatuses": [{
                "name": "install-dagron-ci-exec", "image": "x", "imageID": "", "ready": false,
                "restartCount": 0, "state": {"terminated": {"exitCode": 125}},
            }],
        }))
        .unwrap();
        assert_eq!(failed_init_container(&status), Some(("install-dagron-ci-exec".into(), 125)));
        let fine: PodStatus = serde_json::from_value(serde_json::json!({
            "phase": "Failed",
            "initContainerStatuses": [{
                "name": "install-dagron-ci-exec", "image": "x", "imageID": "", "ready": true,
                "restartCount": 0, "state": {"terminated": {"exitCode": 0}},
            }],
        }))
        .unwrap();
        assert_eq!(failed_init_container(&fine), None, "the task's own failure is not the init's");
        assert_eq!(failed_init_container(&PodStatus::default()), None);
    }

    #[test]
    fn a_pod_its_node_stopped_says_why() {
        use k8s_openapi::api::core::v1::PodStatus;
        let status = |v: serde_json::Value| -> PodStatus { serde_json::from_value(v).unwrap() };
        // As a kubelet leaves a pod whose workspace outgrew its size.
        assert_eq!(
            stopped_pod(&status(serde_json::json!({
                "phase": "Failed",
                "reason": "Evicted",
                "message": "Usage of EmptyDir volume \"workspace\" exceeds the limit \"100Mi\". ",
            }))),
            Some(
                "the task's pod failed (Evicted): Usage of EmptyDir volume \"workspace\" exceeds \
                 the limit \"100Mi\"."
                    .into()
            )
        );
        assert_eq!(
            stopped_pod(&status(
                serde_json::json!({"phase": "Failed", "reason": "DeadlineExceeded"})
            )),
            Some("the task's pod failed (DeadlineExceeded)".into())
        );
        // A task that failed by itself, or did not fail, has nothing to add.
        assert_eq!(
            stopped_pod(&status(serde_json::json!({"phase": "Failed"}))),
            None
        );
        assert_eq!(
            stopped_pod(&status(
                serde_json::json!({"phase": "Failed", "reason": ""})
            )),
            None
        );
        assert_eq!(
            stopped_pod(&status(
                serde_json::json!({"phase": "Succeeded", "reason": "Completed"})
            )),
            None
        );
        assert_eq!(stopped_pod(&PodStatus::default()), None);
    }

    // ── Secret-sourced env vars reach the pod by reference ────────────────
    fn ctx_with_secret() -> ExecContext {
        use dagron_core::dag::{EnvVar, SecretRef};
        let mut ctx = ExecContext::new(vec!["sh".into(), "-c".into(), "true".into()], None, None);
        ctx.env = vec![
            EnvVar { name: "S3_BUCKET".into(), value: "dagron-lt".into(), value_from: None },
            // What `resolve_secrets` leaves behind: the decrypted value, with the
            // `value_from` marker kept so the redactor (and now the executor) know.
            EnvVar {
                name: "API_KEY".into(),
                value: "s3cr3t-plaintext".into(),
                value_from: Some(SecretRef {
                    secret: "API_KEY".into(),
                    optional: false,
                }),
            },
        ];
        ctx
    }

    #[test]
    fn a_secret_env_var_is_a_secret_key_ref_and_its_plaintext_is_not_in_the_pod() {
        let ctx = ctx_with_secret();
        assert!(has_secret_env(&ctx));
        let pod = build_pod_with_secrets(
            "sched-x",
            "alpine",
            &ctx.command,
            &ctx,
            Some("sched-x-env"),
            &ServiceAccountPolicy::allow_all(),
            &KubeWorkspaces::default(),
        )
        .unwrap();

        let wire = serde_json::to_string(&pod).unwrap();
        assert!(!wire.contains("s3cr3t-plaintext"), "plaintext must not be in the Pod spec: {wire}");

        let env = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let key = env.iter().find(|e| e.name == "API_KEY").unwrap();
        assert!(key.value.is_none());
        let r = key.value_from.as_ref().unwrap().secret_key_ref.as_ref().unwrap();
        assert_eq!((r.name.as_str(), r.key.as_str()), ("sched-x-env", "API_KEY"));
        // A plain var is untouched.
        let plain = env.iter().find(|e| e.name == "S3_BUCKET").unwrap();
        assert_eq!(plain.value.as_deref(), Some("dagron-lt"));
        assert!(plain.value_from.is_none());
    }

    #[test]
    fn inline_mode_keeps_the_literal_the_old_way() {
        let ctx = ctx_with_secret();
        let pod = build_pod("sched-x", "alpine", &ctx.command, &ctx).unwrap();
        let env = pod.spec.unwrap().containers[0].env.clone().unwrap();
        let key = env.iter().find(|e| e.name == "API_KEY").unwrap();
        assert_eq!(key.value.as_deref(), Some("s3cr3t-plaintext"));
        assert!(key.value_from.is_none());
    }

    #[test]
    fn the_env_secret_holds_only_the_secret_vars_keyed_by_name() {
        let ctx = ctx_with_secret();
        let sec = env_secret_object("sched-x-env", &ctx).unwrap();
        assert_eq!(sec.metadata.name.as_deref(), Some("sched-x-env"));
        assert_eq!(sec.type_.as_deref(), Some("Opaque"));
        let data = sec.string_data.unwrap();
        assert_eq!(data.len(), 1, "the plain var must not be copied into the Secret");
        assert_eq!(data["API_KEY"], "s3cr3t-plaintext");
        assert_eq!(
            sec.metadata.labels.unwrap().get("app.kubernetes.io/managed-by").map(String::as_str),
            Some("dagron")
        );
    }

    #[test]
    fn a_task_without_secret_vars_needs_no_secret() {
        let mut ctx = ctx_with_secret();
        ctx.env.retain(|e| e.value_from.is_none());
        assert!(!has_secret_env(&ctx));
    }

    #[test]
    fn the_secret_env_mode_defaults_to_reference_and_rejects_a_typo() {
        assert!(!parse_secret_env_mode(None).unwrap());
        assert!(!parse_secret_env_mode(Some("")).unwrap());
        assert!(!parse_secret_env_mode(Some("secret")).unwrap());
        assert!(parse_secret_env_mode(Some(" inline ")).unwrap());
        let e = parse_secret_env_mode(Some("inlin")).unwrap_err().to_string();
        assert!(e.contains("'secret' or 'inline'"), "{e}");
    }

    // ── A full namespace quota waits; every other refusal fails ───────────

    /// The pod the refusals below are about — the name `execute()` minted.
    const POD: &str = "sched-4b313d15d0444782aad133ef1692bbe8";

    /// A full-quota refusal of [`POD`], in the sentence [`CAPTURED`] shows an
    /// apiserver writing. The quantities are ones a busy namespace produces.
    const FULL_REFUSAL: &str = r#"pods "sched-4b313d15d0444782aad133ef1692bbe8" is forbidden: exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4"#;

    /// `Status.message` of two pod creates a Kubernetes 1.36 apiserver refused
    /// with code 403 and reason `Forbidden`, as it sent them: one resource over
    /// the namespace's quota, then two. Each is paired with the pod it names.
    const CAPTURED: [(&str, &str); 2] = [
        (
            "sched-5624d8e39e68430e97fb22577267a546",
            r#"pods "sched-5624d8e39e68430e97fb22577267a546" is forbidden: exceeded quota: tenant-quota, requested: limits.cpu=3600m, used: limits.cpu=500m, limited: limits.cpu=4"#,
        ),
        (
            "sched-31e7fb51ed96400a9f7ee6fe6728036c",
            r#"pods "sched-31e7fb51ed96400a9f7ee6fe6728036c" is forbidden: exceeded quota: tenant-quota, requested: limits.cpu=3600m,limits.memory=8064Mi, used: limits.cpu=500m,limits.memory=256Mi, limited: limits.cpu=4,limits.memory=8Gi"#,
        ),
    ];

    /// A `403 Forbidden` about [`POD`] that goes on to say `detail` — the shape
    /// the apiserver gives everything an admission plugin refuses.
    fn forbidden(detail: &str) -> Option<QuotaRefusal> {
        let message = format!("pods \"{POD}\" is forbidden: {detail}");
        classify_quota_refusal(403, "Forbidden", &message, POD)
    }

    #[test]
    fn a_full_quota_refusal_is_backpressure() {
        assert_eq!(
            classify_quota_refusal(403, "Forbidden", FULL_REFUSAL, POD),
            Some(QuotaRefusal::Full(
                "pod refused by namespace ResourceQuota 'tenant-quota' (requested: limits.cpu=550m, \
                 used: limits.cpu=3750m, limited: limits.cpu=4)"
                    .into()
            ))
        );

        // Several resources over at once: each is within its own hard limit.
        let several = forbidden(
            "exceeded quota: tenant-quota, requested: limits.cpu=550m,limits.memory=512Mi, \
             used: limits.cpu=3750m,limits.memory=7936Mi, limited: limits.cpu=4,limits.memory=8Gi",
        );
        assert!(matches!(&several, Some(QuotaRefusal::Full(r)) if r.contains("'tenant-quota'")), "{several:?}");

        // A request that is exactly the hard limit fits an empty namespace.
        let exact = forbidden(
            "exceeded quota: tenant-quota, requested: limits.cpu=4, used: limits.cpu=500m, limited: limits.cpu=4",
        );
        assert!(matches!(exact, Some(QuotaRefusal::Full(_))), "{exact:?}");

        // An apiserver can report more in use than the hard limit. `used` is not
        // compared with anything: the request still fits the limit, so it waits.
        let over = forbidden(
            "exceeded quota: tenant-quota, requested: limits.cpu=1050m, used: limits.cpu=4800m, limited: limits.cpu=4",
        );
        assert!(matches!(over, Some(QuotaRefusal::Full(_))), "{over:?}");
    }

    /// The sentences an apiserver actually sent read as a full quota, and the
    /// lists come through as written — separators, order and quantity forms
    /// included. A sentence about one pod says nothing about another.
    #[test]
    fn the_refusals_an_apiserver_sent_are_read() {
        let (pod, message) = CAPTURED[0];
        assert_eq!(
            classify_quota_refusal(403, "Forbidden", message, pod),
            Some(QuotaRefusal::Full(
                "pod refused by namespace ResourceQuota 'tenant-quota' (requested: limits.cpu=3600m, \
                 used: limits.cpu=500m, limited: limits.cpu=4)"
                    .into()
            ))
        );

        let (pod, message) = CAPTURED[1];
        assert_eq!(
            classify_quota_refusal(403, "Forbidden", message, pod),
            Some(QuotaRefusal::Full(
                "pod refused by namespace ResourceQuota 'tenant-quota' (requested: \
                 limits.cpu=3600m,limits.memory=8064Mi, used: limits.cpu=500m,limits.memory=256Mi, \
                 limited: limits.cpu=4,limits.memory=8Gi)"
                    .into()
            ))
        );

        assert_eq!(classify_quota_refusal(403, "Forbidden", CAPTURED[0].1, CAPTURED[1].0), None);
    }

    /// The apiserver uses the same sentence for a request no amount of waiting
    /// admits. Only the numbers tell the two apart, so they are compared.
    #[test]
    fn a_request_that_can_never_fit_is_not_backpressure() {
        for detail in [
            // over the hard limit on its own
            "exceeded quota: tenant-quota, requested: limits.cpu=8, used: limits.cpu=0, limited: limits.cpu=4",
            // a hard limit of zero
            "exceeded quota: catch-all, requested: count/pods=1, used: count/pods=0, limited: count/pods=0",
            // one such resource among several that would fit
            "exceeded quota: tenant-quota, requested: limits.cpu=550m,limits.memory=16Gi, \
             used: limits.cpu=3750m,limits.memory=1Gi, limited: limits.cpu=4,limits.memory=8Gi",
            // the comparison is on the value, not the spelling: 4100m > 4
            "exceeded quota: tenant-quota, requested: limits.cpu=4100m, used: limits.cpu=0, limited: limits.cpu=4",
        ] {
            let verdict = forbidden(detail);
            assert!(matches!(verdict, Some(QuotaRefusal::NeverFits(_))), "{detail:?} gave {verdict:?}");
        }
    }

    /// Everything here must keep the failure path untouched, so each is `None`
    /// and not merely "not `Full`": the task records what any other pod-create
    /// error records. Which of them are also logged is pinned by the test below.
    #[test]
    fn other_refusals_are_not_quota() {
        let quota = "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4";
        for detail in [
            // LimitRange
            "maximum cpu usage per Container is 2, but limit is 4",
            // the quota plugin's other sentences
            "failed quota: tenant-quota: must specify limits.cpu",
            "status unknown for quota: tenant-quota, resources: limits.cpu",
            // ServiceAccount admission
            "error looking up service account tenant-a/etl: serviceaccount \"etl\" not found",
            // PodSecurity
            "violates PodSecurity \"restricted:latest\": allowPrivilegeEscalation != false \
             (container \"task\" must set securityContext.allowPrivilegeEscalation=false)",
            // the phrase, but not where the quota plugin puts it
            "denied: exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4",
        ] {
            assert_eq!(forbidden(detail), None, "{detail:?}");
        }

        for message in [
            // RBAC names no pod
            "pods is forbidden: User \"system:serviceaccount:dagron:engine\" cannot create resource \
             \"pods\" in API group \"\" in the namespace \"tenant-a\""
                .to_string(),
            // a webhook's denial is its own text, whatever it says
            format!("admission webhook \"quota.example.com\" denied the request: {quota}"),
            // the right sentence about another pod
            format!("pods \"sched-00000000000000000000000000000000\" is forbidden: {quota}"),
            // the right sentence, not at the start: how the error renders
            format!("ApiError: {FULL_REFUSAL}: Forbidden"),
        ] {
            assert_eq!(classify_quota_refusal(403, "Forbidden", &message, POD), None, "{message:?}");
        }

        // The right sentence under any other status is not the quota plugin's
        // refusal. `Failed to parse error data` is the reason the client reports
        // when the body was not a Status at all.
        for (code, reason) in [
            (409, "Conflict"),
            (429, "TooManyRequests"),
            (500, "InternalError"),
            (409, "Forbidden"),
            (403, "Failed to parse error data"),
            (403, ""),
        ] {
            assert_eq!(
                classify_quota_refusal(code, reason, FULL_REFUSAL, POD),
                None,
                "{code} {reason:?}"
            );
        }
    }

    /// A refusal the classifier turned away is logged when it reads like a
    /// quota refusal all the same: a `403` that says `exceeded quota` anywhere.
    /// That is where an apiserver whose sentence starts differently would show;
    /// without it every such task would fail with nothing to say why none waits.
    #[test]
    fn a_quota_shaped_403_that_misses_the_anchor_is_logged() {
        let api = |code: u16, reason: &str, message: &str| {
            kube::Error::Api(Box::new(kube::core::Status {
                code,
                reason: reason.into(),
                message: message.into(),
                ..Default::default()
            }))
        };
        let reworded = r#"pod "sched-4b313d15d0444782aad133ef1692bbe8" is forbidden: exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4"#;
        let webhook = format!("admission webhook \"quota.example.com\" denied the request: {FULL_REFUSAL}");
        for (reason, message) in [
            // a lead-in that is not the anchored one
            ("Forbidden", reworded),
            // the right sentence under a reason the classifier does not take
            ("Failed to parse error data", FULL_REFUSAL),
            // loose on purpose: a webhook quoting the phrase is logged too
            ("Forbidden", webhook.as_str()),
        ] {
            let e = api(403, reason, message);
            assert_eq!(quota_refusal(&e, POD), None, "{reason:?} {message:?}");
            assert_eq!(unrecognised_quota_refusal(&e), Some((reason, message)));
        }

        // Not a 403, or no mention of quota: an ordinary pod-create error,
        // logged by nothing here.
        for (code, message) in [
            (429, FULL_REFUSAL),
            (500, FULL_REFUSAL),
            (403, "pods \"sched-x\" is forbidden: failed quota: tenant-quota: must specify limits.cpu"),
            (403, "pods is forbidden: User \"system:serviceaccount:dagron:engine\" cannot create resource \"pods\""),
        ] {
            assert_eq!(unrecognised_quota_refusal(&api(code, "Forbidden", message)), None, "{code} {message:?}");
        }
        // Only a Status is read, never the text another kind of error renders to.
        let not_api = kube::Error::ReadEvents(std::io::Error::other(FULL_REFUSAL));
        assert_eq!(unrecognised_quota_refusal(&not_api), None);

        assert!(mentions_quota(403, "exceeded quota"));
        assert!(!mentions_quota(403, "Exceeded Quota"), "the apiserver's own casing, nothing looser");
        assert!(!mentions_quota(401, "exceeded quota"));
    }

    /// Past the anchor it is a quota refusal, but one this cannot vouch for.
    /// It takes the failure path and says so, rather than waiting on a guess.
    #[test]
    fn an_unreadable_quota_message_is_not_a_wait() {
        for detail in [
            // a group missing
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m",
            "exceeded quota: tenant-quota",
            // no quota named
            "exceeded quota: , requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4",
            // the lists name different resources, or a different number of them
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.memory=1Gi, limited: limits.cpu=4",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m,limits.memory=512Mi, \
             used: limits.cpu=3750m, limited: limits.cpu=4",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m,limits.memory=512Mi, \
             used: limits.memory=1Gi,limits.cpu=3750m, limited: limits.cpu=4,limits.memory=8Gi",
            // an empty list, an empty entry, a nameless entry
            "exceeded quota: tenant-quota, requested: , used: limits.cpu=3750m, limited: limits.cpu=4",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m,, used: limits.cpu=3750m,, limited: limits.cpu=4,",
            "exceeded quota: tenant-quota, requested: =550m, used: =3750m, limited: =4",
            // a quantity that does not read
            "exceeded quota: tenant-quota, requested: limits.cpu=lots, used: limits.cpu=3750m, limited: limits.cpu=4",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=-4",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu, limited: limits.cpu=4",
            // a separator the apiserver does not write
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, limits.memory=512Mi, \
             used: limits.cpu=3750m, limits.memory=1Gi, limited: limits.cpu=4, limits.memory=8Gi",
            // trailing text
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4 (retry later)",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4, and more",
            "exceeded quota: tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4\n",
        ] {
            assert_eq!(forbidden(detail), Some(QuotaRefusal::Unreadable), "{detail:?}");
        }
    }

    /// Exact, in nano-units, for every form the apiserver prints.
    #[test]
    fn quantities_are_read_exactly_or_not_at_all() {
        const UNIT: i128 = 1_000_000_000;
        for (text, nanos) in [
            ("550m", 550_000_000),
            ("3750m", 3_750_000_000),
            ("4", 4 * UNIT),
            ("+4", 4 * UNIT),
            ("0", 0),
            ("0.5", UNIT / 2),
            ("1.5m", 1_500_000),
            ("100n", 100),
            ("250u", 250_000),
            ("1k", 1_000 * UNIT),
            ("129M", 129_000_000 * UNIT),
            ("2G", 2_000_000_000 * UNIT),
            ("512Mi", 512 * 1024 * 1024 * UNIT),
            ("8Gi", 8 * 1024 * 1024 * 1024 * UNIT),
            ("0.5Ki", 512 * UNIT),
            ("1e3", 1_000 * UNIT),
            ("1E3", 1_000 * UNIT),
            ("12e+2", 1_200 * UNIT),
            ("5e-3", 5_000_000),
            ("2E", 2_000_000_000_000_000_000 * UNIT),
            ("1Ei", (1i128 << 60) * UNIT),
        ] {
            assert_eq!(quantity_nanos(text), Some(nanos), "{text:?}");
        }

        // The comparisons the classifier makes are numeric, across spellings.
        assert!(quantity_nanos("550m") < quantity_nanos("4"));
        assert!(quantity_nanos("9Gi") > quantity_nanos("8Gi"));
        assert!(quantity_nanos("1G") < quantity_nanos("1Gi"));
        assert_eq!(quantity_nanos("1k"), quantity_nanos("1e3"));
        assert_eq!(quantity_nanos("1000m"), quantity_nanos("1"));

        for bad in [
            "", "+", "abc", "m", "1.2.3", "1.", ".5", "4X", "4mi", "4 ", " 4", "1 Gi", "-1", "+-1",
            "1e", "1e+", "1e3k", "1Ee3", "0x10", "４",
            // finer than a nano-unit: no exact value to compare
            "0.5n", "5e-10", "0.0000000001",
            // past what the arithmetic holds
            "1e40", "340282366920938463463374607431768211456", "200000000000Ei",
        ] {
            assert_eq!(quantity_nanos(bad), None, "{bad:?}");
        }
    }

    /// `execute()` classifies the error `pods.create` hands back while it is
    /// still typed: the Status fields, never the text the error renders to.
    #[test]
    fn a_quota_403_is_classified_from_the_typed_error() {
        let api = |code: u16, reason: &str| {
            kube::Error::Api(Box::new(kube::core::Status {
                code,
                reason: reason.into(),
                message: FULL_REFUSAL.into(),
                ..Default::default()
            }))
        };
        let refused = api(403, "Forbidden");
        assert!(matches!(quota_refusal(&refused, POD), Some(QuotaRefusal::Full(_))));
        assert_eq!(quota_refusal(&refused, "sched-another"), None, "only about the pod this call named");
        assert_eq!(quota_refusal(&api(500, "InternalError"), POD), None);

        // An error with no Status has nothing to classify, even when its text
        // is the whole sentence.
        let not_api = kube::Error::ReadEvents(std::io::Error::other(FULL_REFUSAL));
        assert!(not_api.to_string().contains("exceeded quota: tenant-quota"), "{not_api}");
        assert_eq!(quota_refusal(&not_api, POD), None);
    }

    fn two_services() -> Vec<ServiceSpec> {
        serde_json::from_value(serde_json::json!([
            {
                "name": "pg",
                "image": "postgres:16-alpine",
                "args": ["postgres", "-c", "fsync=off"],
                "env": [{ "name": "POSTGRES_PASSWORD", "value": "pw" }],
                "ready": "pg_isready -h localhost -U postgres",
                "ready_timeout_secs": 90,
                "resources": { "requests": { "cpu": "100m" }, "limits": { "memory": "256Mi" } },
            },
            { "name": "cache", "image": "redis:7", "ready": ["redis-cli", "ping"] },
            { "name": "plain", "image": "busybox:1.36.1" },
        ]))
        .unwrap()
    }

    /// A task's services are sidecars of its pod: init containers that
    /// restart, after every other init container (the gate among them, so
    /// that nothing of the author's runs before the pod is seen to be held),
    /// in the order they are written. `ready` is the startup probe that holds
    /// the next container back, and each service's name is a name for the
    /// pod's loopback address.
    #[test]
    fn a_task_s_services_are_sidecars_after_every_other_init_container() {
        use dagron_core::isolation::Network;
        let mut ctx = ExecContext::new(vec!["sh".into()], None, None);
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec::default());
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            network: Some(Network::None),
            drop_all_capabilities: Some(true),
            ..iso()
        });
        ctx.image_pull_policy = Some(dagron_core::dag::ImagePullPolicy::Always);
        ctx.services = two_services();
        let workspaces = KubeWorkspaces {
            ci_exec_image: Some("exec:1".into()),
            ..Default::default()
        };
        let pod = build_pod_with_secrets(
            "sched-svc",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::default(),
            &workspaces,
        )
        .unwrap();
        let spec = pod.spec.unwrap();
        let inits = spec.init_containers.unwrap();
        let names: Vec<&str> = inits.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                crate::workspace::INIT_NAME,
                GATE_NAME,
                "svc-pg",
                "svc-cache",
                "svc-plain"
            ]
        );
        // The two the executor runs first end; a service does not.
        assert_eq!(inits[0].restart_policy, None);
        assert_eq!(inits[1].restart_policy, None);

        let pg = &inits[2];
        assert_eq!(pg.restart_policy.as_deref(), Some("Always"));
        assert_eq!(pg.image.as_deref(), Some("postgres:16-alpine"));
        assert_eq!(pg.command, None, "the image's own entrypoint");
        assert_eq!(
            pg.args.as_deref(),
            Some(&["postgres".to_string(), "-c".into(), "fsync=off".into()][..])
        );
        let env = pg.env.as_ref().unwrap();
        assert_eq!(
            (env.len(), env[0].name.as_str(), env[0].value.as_deref()),
            (1, "POSTGRES_PASSWORD", Some("pw")),
            "its own variables, and none of the task's"
        );
        assert_eq!(
            pg.image_pull_policy.as_deref(),
            Some("Always"),
            "its task's policy"
        );
        let probe = pg
            .startup_probe
            .as_ref()
            .expect("ready is its startup probe");
        assert_eq!(
            probe.exec.as_ref().unwrap().command.as_deref(),
            Some(
                &[
                    "sh".to_string(),
                    "-c".into(),
                    "pg_isready -h localhost -U postgres".into()
                ][..]
            )
        );
        assert_eq!(
            (
                probe.period_seconds,
                probe.failure_threshold,
                probe.timeout_seconds
            ),
            (Some(1), Some(100), Some(10)),
            "asked each second, and given up by the kubelet a little after the executor has"
        );
        let resources = pg.resources.as_ref().unwrap();
        assert_eq!(resources.requests.as_ref().unwrap()["cpu"].0, "100m");
        assert_eq!(resources.limits.as_ref().unwrap()["memory"].0, "256Mi");
        assert!(
            pg.volume_mounts.is_none(),
            "nothing of the task's workspace"
        );
        // Held to the task's envelope, as every container of the pod is.
        let held = pg.security_context.as_ref().expect("the task's envelope");
        assert_eq!(held.allow_privilege_escalation, Some(false));
        assert_eq!(
            held.capabilities.as_ref().unwrap().drop.as_ref().unwrap()[0],
            "ALL"
        );

        let cache = &inits[3];
        assert_eq!(
            cache
                .startup_probe
                .as_ref()
                .unwrap()
                .exec
                .as_ref()
                .unwrap()
                .command
                .as_deref(),
            Some(&["redis-cli".to_string(), "ping".into()][..]),
            "a program and its arguments, run as written"
        );
        assert_eq!(
            cache.startup_probe.as_ref().unwrap().failure_threshold,
            Some(70)
        );
        assert!(cache.args.is_none() && cache.env.is_none() && cache.resources.is_none());
        assert!(inits[4].startup_probe.is_none(), "ready once started");

        let aliases = spec.host_aliases.unwrap();
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].ip, "127.0.0.1");
        assert_eq!(
            aliases[0].hostnames.as_deref(),
            Some(&["pg".to_string(), "cache".into(), "plain".into()][..])
        );

        // A task with none has neither.
        ctx.services.clear();
        let spec = build_pod_with_secrets(
            "sched-svc",
            "alpine:3.19",
            &ctx.command,
            &ctx,
            None,
            &ServiceAccountPolicy::default(),
            &workspaces,
        )
        .unwrap()
        .spec
        .unwrap();
        assert!(spec.host_aliases.is_none());
        assert_eq!(spec.init_containers.unwrap().len(), 2);
    }

    /// A service whose container stopped while its task has yet to start is
    /// found, with what it exited with: one that ended, and one the kubelet
    /// has started again since. Not once the task's container has started,
    /// and never by the check for a failed init container, which a service's
    /// exit at the end of a pod would otherwise trip.
    #[test]
    fn a_service_that_stopped_before_its_task_started_is_found() {
        use k8s_openapi::api::core::v1::PodStatus;
        let status = |inits: serde_json::Value, task: serde_json::Value| -> PodStatus {
            serde_json::from_value(serde_json::json!({
                "phase": "Pending",
                "initContainerStatuses": inits,
                "containerStatuses": [{
                    "name": "task", "image": "x", "imageID": "", "ready": false,
                    "restartCount": 0, "state": task,
                }],
            }))
            .unwrap()
        };
        let init =
            |name: &str, restarts: i32, state: serde_json::Value, last: serde_json::Value| {
                serde_json::json!({
                    "name": name, "image": "x", "imageID": "", "ready": false,
                    "restartCount": restarts, "state": state, "lastState": last,
                })
            };
        let waiting = serde_json::json!({ "waiting": { "reason": "PodInitializing" } });
        let running = serde_json::json!({ "running": {} });
        let ended = |code: i32| serde_json::json!({ "terminated": { "exitCode": code } });
        let none = serde_json::json!({});

        // Starting, or started and not yet ready: nothing to say.
        let starting = status(
            serde_json::json!([
                init("install-dagron-ci-exec", 0, ended(0), none.clone()),
                init("svc-pg", 0, running.clone(), none.clone()),
            ]),
            waiting.clone(),
        );
        assert_eq!(failed_service(&starting), None);
        assert_eq!(failed_init_container(&starting), None);
        // It runs, and its probe has yet to pass: one this executor times.
        assert_eq!(unready_services(&starting), ["pg"]);
        assert_eq!(
            (task_started(&starting), task_exit(&starting)),
            (false, None)
        );

        // It ended by itself.
        let exited = status(
            serde_json::json!([init("svc-pg", 0, ended(1), none.clone())]),
            waiting.clone(),
        );
        assert_eq!(failed_service(&exited), Some(("pg".into(), 1)));

        // The kubelet stopped it for its probe and has started it again.
        let restarted = status(
            serde_json::json!([
                init("svc-cache", 0, running.clone(), none.clone()),
                init("svc-pg", 1, running.clone(), ended(137)),
            ]),
            waiting.clone(),
        );
        assert_eq!(failed_service(&restarted), Some(("pg".into(), 137)));

        // An init container of the executor's own is not a service.
        let install = status(
            serde_json::json!([init("install-dagron-ci-exec", 0, ended(125), none.clone())]),
            waiting.clone(),
        );
        assert_eq!(failed_service(&install), None);
        assert_eq!(
            failed_init_container(&install),
            Some(("install-dagron-ci-exec".into(), 125))
        );

        // Once the task has started, a service that stops is started again
        // beside it, and at the pod's end each is stopped: neither is why a
        // task did not start.
        let beside = status(
            serde_json::json!([init("svc-pg", 1, running.clone(), ended(1))]),
            running.clone(),
        );
        assert_eq!(failed_service(&beside), None);
        assert_eq!((task_started(&beside), task_exit(&beside)), (true, None));
        let mut over = status(
            serde_json::json!([init("svc-pg", 0, ended(143), none.clone())]),
            ended(0),
        );
        assert_eq!(
            task_exit(&over),
            Some(0),
            "the task has ended, whatever its pod's phase says while its services stop"
        );
        assert!(unready_services(&over).is_empty());
        // By its name: a container beside it that ended is not the task.
        let beside_it: PodStatus = serde_json::from_value(serde_json::json!({
            "phase": "Running",
            "containerStatuses": [
                { "name": "other", "image": "x", "imageID": "", "ready": false,
                  "restartCount": 0, "state": { "terminated": { "exitCode": 9 } } },
                { "name": "task", "image": "x", "imageID": "", "ready": true,
                  "restartCount": 0, "state": { "running": {} } },
            ],
        }))
        .unwrap();
        assert_eq!(
            (task_started(&beside_it), task_exit(&beside_it)),
            (true, None)
        );
        let no_task: PodStatus = serde_json::from_value(serde_json::json!({
            "phase": "Running",
            "containerStatuses": [
                { "name": "other", "image": "x", "imageID": "", "ready": false,
                  "restartCount": 0, "state": { "terminated": { "exitCode": 9 } } },
            ],
        }))
        .unwrap();
        assert_eq!((task_started(&no_task), task_exit(&no_task)), (false, None));
        over.phase = Some("Succeeded".into());
        assert_eq!(failed_service(&over), None);
        assert_eq!(
            failed_init_container(&over),
            None,
            "what a service exits with when its pod ends is not a failed init container"
        );
    }

    /// A service needs a cluster that has sidecar containers: one older is
    /// refused by its version, and one that would not say is not.
    #[test]
    fn a_task_with_services_needs_a_cluster_that_has_sidecars() {
        assert_eq!(server_version("1", "31"), Some((1, 31)));
        assert_eq!(
            server_version("1", "29+"),
            Some((1, 29)),
            "a vendor's build"
        );
        assert_eq!(server_version("1", ""), None);
        assert_eq!(server_version("v1", "31"), None);

        assert_eq!(too_old_for_services(None), None);
        assert_eq!(too_old_for_services(Some((1, 29))), None);
        assert_eq!(too_old_for_services(Some((1, 33))), None);
        assert_eq!(too_old_for_services(Some((2, 0))), None);
        let why = too_old_for_services(Some((1, 28))).unwrap();
        assert!(
            why.contains("this cluster is Kubernetes 1.28") && why.contains("has from 1.29"),
            "{why}"
        );
    }
}

#[cfg(test)]
mod step_container_tests {
    use super::*;
    use k8s_openapi::api::core::v1::{Capabilities, ContainerStateWaiting, SecurityContext};

    fn lent(confined: bool) -> KubeLent {
        KubeLent {
            pod: "sched-1".into(),
            workspace: "workspace".into(),
            security: Some(SecurityContext {
                allow_privilege_escalation: Some(false),
                read_only_root_filesystem: Some(true),
                capabilities: Some(Capabilities {
                    drop: Some(vec!["ALL".into()]),
                    ..Capabilities::default()
                }),
                ..SecurityContext::default()
            }),
            pull: Some("IfNotPresent".into()),
            confined,
            env: Vec::new(),
            made: 0,
        }
    }

    /// A step's ephemeral container has the pod's workspace at both places,
    /// starts in GitHub's, and is held as the task's own container is.
    #[test]
    fn a_steps_container_shares_the_workspace_and_is_held_as_the_task_is() {
        let step = StepContainer {
            image: "alpine:3.20".into(),
            entrypoint: Some("/bin/sh".into()),
            args: vec!["-c".into(), "echo hi".into()],
            env: vec![("INPUT_WHO".into(), "rig".into())],
            timeout: Duration::from_secs(60),
        };
        let c = ephemeral_container("step-1", &step, &lent(false));
        assert_eq!(c["name"], "step-1");
        assert_eq!(c["image"], "alpine:3.20");
        assert_eq!(c["command"], serde_json::json!(["/bin/sh"]));
        assert_eq!(c["args"], serde_json::json!(["-c", "echo hi"]));
        assert_eq!(c["workingDir"], "/github/workspace");
        assert_eq!(
            c["volumeMounts"],
            serde_json::json!([
                {"name": "workspace", "mountPath": "/workspace"},
                {"name": "workspace", "mountPath": "/github/workspace"},
            ])
        );
        assert_eq!(
            c["env"],
            serde_json::json!([{"name": "INPUT_WHO", "value": "rig"}])
        );
        assert_eq!(c["securityContext"]["allowPrivilegeEscalation"], false);
        assert_eq!(
            c["securityContext"]["capabilities"]["drop"],
            serde_json::json!(["ALL"])
        );
        assert_eq!(c["imagePullPolicy"], "IfNotPresent");
        // The image's own entrypoint and command, where the step names none:
        // and no resources, which an ephemeral container may not have.
        let bare = StepContainer {
            entrypoint: None,
            args: Vec::new(),
            ..step
        };
        let c = ephemeral_container("step-2", &bare, &lent(false));
        assert!(c.get("command").is_none() && c.get("args").is_none(), "{c}");
        assert!(c.get("resources").is_none(), "{c}");
    }

    #[test]
    fn a_step_container_that_will_not_start_says_why() {
        let waiting = |reason: &str, message: Option<&str>| ContainerStateWaiting {
            reason: Some(reason.into()),
            message: message.map(str::to_string),
        };
        assert_eq!(
            will_not_start(&waiting(
                "ImagePullBackOff",
                Some("Back-off pulling image \"nope\"")
            )),
            Some("ImagePullBackOff: Back-off pulling image \"nope\"".into())
        );
        assert_eq!(
            will_not_start(&waiting("InvalidImageName", None)),
            Some("InvalidImageName".into())
        );
        // Pulling, or the first failed pull, may yet pass.
        assert_eq!(will_not_start(&waiting("ContainerCreating", None)), None);
        assert_eq!(will_not_start(&waiting("ErrImagePull", None)), None);
    }
}
