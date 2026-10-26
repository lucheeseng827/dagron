//! A task that asked for no network, on a real cluster, through the
//! Kubernetes executor: its pod is labelled for the namespace's standing
//! NetworkPolicy, is held by the gate until its network is seen to be gone,
//! and then reaches nothing from its first instant, where a task that asked
//! for nothing reaches the cluster's API server; an engine told the cluster
//! enforces NetworkPolicy does not start on a namespace without the policy,
//! with one under its name that allows something, or without an image to run
//! the gate from; and such a task is refused before anything is made where
//! the engine was told nothing, or where it has a checkout.
//!
//! "Reaches" is a connection to the API server's own service, which every
//! pod is given the address of: a task answers `reached` when that port
//! answers its plain HTTP with a refusal, and `unreachable` when nothing
//! answers. So the tests need no listener of their own. The task asks the
//! moment it starts, which is the moment the gate is for: on k3s, whose
//! controller takes a labelled pod's network a second or two after the pod
//! starts, a task that was not held reads `reached`.
//!
//! The gate itself is seen too: what it said of a held pod is read from the
//! pod while the task runs, a pod nothing takes the network from fails at
//! it with its container never started, and a namespace whose other
//! policies give a labelled pod something back stops the engine as it
//! starts, and a task when the policy comes later.
//!
//! And an allowlist (`network: allow:<name>`): a task held to one reaches
//! the pod its list names, and neither a pod beside it nor the API server,
//! from the first thing it does. The list is `DAGRON_KUBE_LIVE_ALLOWLIST`
//! where that names one already in the namespace (tests/k3s.sh has the
//! chart make it, and it has to allow the pods labelled
//! `dagron-test/allowed=yes` on 8080), and otherwise one this test applies.
//!
//! What these leave on the cluster: the standing policy in the namespace
//! named (it selects only pods labelled for it), and namespaces of their own
//! named `dagron-test-net-*`, which each deletes as it ends and a test that
//! panicked may not have.
//!
//! Skips, saying so, unless `DAGRON_KUBE_LIVE` names a namespace the
//! kubeconfig reaches and `DAGRON_KUBE_LIVE_EXEC_IMAGE` a dagron-ci-exec
//! image the cluster can run, as `kube_workspace.rs` does. It needs a
//! cluster whose CNI enforces NetworkPolicy: k3s does, with its own
//! controller, and tests/k3s.sh runs this there. On a cluster that enforces
//! none the first test fails at the gate, which is what the gate is for.
#![cfg(feature = "kubernetes")]

use dagron_core::isolation::{IsolationSpec, Network};
use dagron_core::workspace::{CheckoutSpec, WorkspaceSpec};
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity, LABEL_TASK_ID};
use dagron_executor::kube_executor::{
    allowlist_policy, allowlist_policy_name, no_network_policy, KubeExecutor, ALLOWLIST_GATE_NAME,
    GATE_NAME, NETWORK_LABEL, NO_NETWORK_POLICY,
};
use dagron_executor::workspace::KubeWorkspaces;
use k8s_openapi::api::core::v1::{Namespace, Pod};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::{Api, DeleteParams, ListParams, LogParams, PostParams};

const TASK_IMAGE: &str = "alpine:3.20";

/// Says `reached` when the API server's port answers, and `unreachable` when
/// nothing does within five seconds. Plain HTTP to a TLS port is refused with
/// a 400, which is an answer.
const REACH: &str = "wget -T 5 -O /dev/null \
    http://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/ 2>&1 \
    | grep -q '400 Bad Request' && echo reached || echo unreachable";

struct Cluster {
    namespace: String,
    client: kube::Client,
    /// A dagron-ci-exec image the cluster can run: what the gate runs from.
    exec_image: String,
}

async fn cluster() -> Option<Cluster> {
    let Ok(namespace) = std::env::var("DAGRON_KUBE_LIVE") else {
        eprintln!("DAGRON_KUBE_LIVE names no namespace - skipping");
        return None;
    };
    let Ok(exec_image) = std::env::var("DAGRON_KUBE_LIVE_EXEC_IMAGE") else {
        eprintln!("DAGRON_KUBE_LIVE_EXEC_IMAGE names no dagron-ci-exec image - skipping");
        return None;
    };
    dagron_executor::install_crypto_provider();
    let client = kube::Client::try_default()
        .await
        .expect("DAGRON_KUBE_LIVE is set, so a kubeconfig that reaches a cluster");
    Some(Cluster {
        namespace,
        client,
        exec_image,
    })
}

/// An executor on `namespace` with the cluster's exec image, told nothing
/// yet of what the cluster enforces.
async fn executor(c: &Cluster, namespace: &str) -> KubeExecutor {
    KubeExecutor::connect(TASK_IMAGE, namespace)
        .await
        .expect("the executor reaches the namespace")
        .with_workspaces(KubeWorkspaces {
            ci_exec_image: Some(c.exec_image.clone()),
            ..Default::default()
        })
        .with_network_policy(false)
        .await
        .expect("nothing to check")
}

/// The standing policy, in `namespace`, as an operator applies it; there
/// already is as good.
async fn apply_the_policy(c: &Cluster, namespace: &str) {
    let policies: Api<NetworkPolicy> = Api::namespaced(c.client.clone(), namespace);
    match policies
        .create(&PostParams::default(), &no_network_policy())
        .await
    {
        Ok(_) => {}
        Err(kube::Error::Api(status)) if status.code == 409 => {}
        Err(e) => panic!("applying {NO_NETWORK_POLICY}: {e}"),
    }
}

fn task(script: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(120),
        None,
    );
    ctx.identity = Some(TaskIdentity {
        task_id: format!("t-{}", uuid::Uuid::new_v4().simple()),
        run_id: "r-network".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

fn no_network() -> Option<IsolationSpec> {
    Some(IsolationSpec {
        network: Some(Network::None),
        ..Default::default()
    })
}

/// The pods carrying `ctx`'s task id: what its attempt left.
async fn pods_of(c: &Cluster, ctx: &ExecContext) -> Vec<String> {
    let id = &ctx.identity.as_ref().expect("an identity").task_id;
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    pods.list(&ListParams::default().labels(&format!("{LABEL_TASK_ID}={id}")))
        .await
        .expect("list the task's pods")
        .items
        .into_iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .filter_map(|p| p.metadata.name)
        .collect()
}

/// A namespace of a test's own, to put policies in that no other test's pods
/// should meet. The caller deletes it.
async fn a_namespace(c: &Cluster) -> String {
    let name = format!(
        "dagron-test-net-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let namespaces: Api<Namespace> = Api::all(c.client.clone());
    namespaces
        .create(
            &PostParams::default(),
            &serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name},
            }))
            .unwrap(),
        )
        .await
        .expect("a namespace of the test's own");
    name
}

async fn delete_namespace(c: &Cluster, name: &str) {
    let namespaces: Api<Namespace> = Api::all(c.client.clone());
    let _ = namespaces.delete(name, &DeleteParams::default()).await;
}

/// A NetworkPolicy named `name` with `spec`, in `namespace`.
async fn apply(c: &Cluster, namespace: &str, name: &str, spec: serde_json::Value) {
    let policies: Api<NetworkPolicy> = Api::namespaced(c.client.clone(), namespace);
    let policy: NetworkPolicy = serde_json::from_value(serde_json::json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": name},
        "spec": spec,
    }))
    .unwrap();
    policies
        .create(&PostParams::default(), &policy)
        .await
        .unwrap_or_else(|e| panic!("applying {name}: {e}"));
}

/// What the gate of `ctx`'s pod said, read from the pod while its task runs:
/// the executor removes the pod when the task ends, and the gate's word with
/// it. `None` if no pod of the task was seen with a gate that had ended
/// within a minute.
async fn what_the_gate_said(c: &Cluster, ctx: &ExecContext) -> Option<(i32, String)> {
    what_a_gate_said(c, ctx, GATE_NAME).await
}

/// [`what_the_gate_said`], of the gate named `gate`.
async fn what_a_gate_said(c: &Cluster, ctx: &ExecContext, gate: &str) -> Option<(i32, String)> {
    let id = ctx.identity.as_ref().expect("an identity").task_id.clone();
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    for _ in 0..300 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let Ok(found) = pods
            .list(&ListParams::default().labels(&format!("{LABEL_TASK_ID}={id}")))
            .await
        else {
            continue;
        };
        for pod in found.items {
            let ended = pod
                .status
                .as_ref()
                .and_then(|s| s.init_container_statuses.as_ref())
                .and_then(|inits| inits.iter().find(|i| i.name == gate))
                .and_then(|gate| gate.state.as_ref()?.terminated.as_ref())
                .map(|t| t.exit_code);
            let (Some(code), Some(name)) = (ended, pod.metadata.name) else {
                continue;
            };
            let params = LogParams {
                container: Some(gate.to_string()),
                ..LogParams::default()
            };
            if let Ok(said) = pods.logs(&name, &params).await {
                return Some((code, said));
            }
        }
    }
    None
}

/// A task that asked for nothing reaches the API server. One that asked for
/// no network does not, from the first thing it does: its pod carries the
/// label, and its container was not started until the pod had been seen to
/// reach nothing. Whether it has a workspace or not, and so whether the gate
/// is the pod's only init container or its last.
#[tokio::test]
async fn a_task_that_asked_for_no_network_reaches_nothing_from_its_start() {
    let Some(c) = cluster().await else { return };
    // The policy may be minutes old by now or made this moment: the other
    // tests apply it too. Either way it is the gate that holds the task
    // back until it is in force, and what the gate said is read below.
    apply_the_policy(&c, &c.namespace).await;
    let exec = executor(&c, &c.namespace)
        .await
        .with_network_policy(true)
        .await
        .expect("the policy is in the namespace, and is the one it is taken for");

    let open = task(REACH);
    let out = exec.execute(&open).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output.trim(),
        "reached",
        "a task that asked for nothing"
    );

    // It stays a few seconds after it has asked, so that its pod is there to
    // be read: that the task read `unreachable` does not by itself show the
    // gate waited, since any init container delays a task by about as long
    // as this CNI takes.
    let mut alone = task(&format!("{REACH}; sleep 4"));
    alone.isolation = no_network();
    let (out, gate) = tokio::join!(exec.execute(&alone), what_the_gate_said(&c, &alone));
    let out = out.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output.trim(),
        "unreachable",
        "a task that asked for no network got an answer from the API server as it started: \
         it was not held until {NO_NETWORK_POLICY} was in force on its pod \
         ({NETWORK_LABEL}=none)"
    );
    let (code, said) = gate.expect("the task's pod was seen, with a gate that had ended");
    assert_eq!(code, 0, "{said}");
    assert!(
        said.contains("this pod's network is gone: nothing answered at")
            && said.contains("the cluster's API server")
            && said.contains("times running"),
        "the gate says what it asked and what it saw: {said}"
    );
    eprintln!("the gate: {}", said.trim());

    // With a workspace, the gate comes after the container that installs
    // the task's tools, which the task then finds.
    let mut working = task(&format!(
        "test -x /dagron/bin/dagron-ci-exec && echo has its tools; {REACH}"
    ));
    working.isolation = no_network();
    working.workspace = Some(WorkspaceSpec::default());
    let out = exec.execute(&working).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "has its tools\nunreachable\n");

    for ctx in [&open, &alone, &working] {
        assert!(pods_of(&c, ctx).await.is_empty());
    }
}

/// An engine told the cluster enforces NetworkPolicy checks what it can of
/// that as it starts: the policy has to be in its namespace, and it has to
/// have an image to run the gate from. One without either would run such a
/// task labelled for nothing, or unheld.
#[tokio::test]
async fn a_namespace_without_the_policy_or_an_engine_without_the_gate_s_image_does_not_start() {
    let Some(c) = cluster().await else { return };
    // No one applies anything to kube-public.
    let err = executor(&c, "kube-public")
        .await
        .with_network_policy(true)
        .await
        .err()
        .expect("kube-public has no such policy")
        .to_string();
    assert!(
        err.contains("DAGRON_TASK_NETWORK_POLICY=enforced")
            && err.contains("in namespace 'kube-public'")
            && err.contains(&format!("'{NO_NETWORK_POLICY}' is not there"))
            && err.contains("kubectl apply"),
        "{err}"
    );

    apply_the_policy(&c, &c.namespace).await;
    let err = KubeExecutor::connect(TASK_IMAGE, &c.namespace)
        .await
        .expect("the executor reaches the namespace")
        .with_workspaces(KubeWorkspaces::default())
        .with_network_policy(true)
        .await
        .err()
        .expect("there is nothing to run the gate from")
        .to_string();
    assert!(
        err.contains("DAGRON_TASK_NETWORK_POLICY=enforced needs DAGRON_CI_EXEC_IMAGE"),
        "{err}"
    );
}

/// The policy is read, and not taken on its name: one under the name that
/// allows something would have a task run believing it reaches nothing.
#[tokio::test]
async fn a_policy_under_the_name_that_allows_something_stops_the_engine() {
    let Some(c) = cluster().await else { return };
    let name = a_namespace(&c).await;
    let mut wrong = no_network_policy();
    wrong.spec.as_mut().unwrap().egress = serde_json::from_value(serde_json::json!([
        {"to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]}
    ]))
    .unwrap();
    let policies: Api<NetworkPolicy> = Api::namespaced(c.client.clone(), &name);
    policies
        .create(&PostParams::default(), &wrong)
        .await
        .expect("a policy that allows every address");

    let started = executor(&c, &name).await.with_network_policy(true).await;
    delete_namespace(&c, &name).await;
    let err = started.err().expect("the policy allows egress").to_string();
    assert!(
        err.contains("is not one that takes a pod's network") && err.contains("allows some egress"),
        "{err}"
    );
}

/// A task that asked for no network is refused before anything is made where
/// nothing would take it away: the engine was told nothing of the cluster.
/// And where it has a checkout, which a pod with no network could not fetch.
#[tokio::test]
async fn a_task_that_asked_for_no_network_is_refused_where_it_could_not_be_held_or_fetched() {
    let Some(c) = cluster().await else { return };
    apply_the_policy(&c, &c.namespace).await;

    let untold = executor(&c, &c.namespace).await;
    let mut alone = task(REACH);
    alone.isolation = no_network();
    let err = untold
        .execute(&alone)
        .await
        .err()
        .expect("nothing here takes a pod's network")
        .to_string();
    assert!(
        err.contains("isolation.network: none")
            && err.contains("has not been told its cluster enforces NetworkPolicy"),
        "{err}"
    );
    assert!(
        pods_of(&c, &alone).await.is_empty(),
        "nothing was made for it"
    );

    let told = untold
        .with_network_policy(true)
        .await
        .expect("the policy is there");
    let mut fetching = task(REACH);
    fetching.isolation = no_network();
    fetching.workspace = Some(WorkspaceSpec {
        checkout: Some(CheckoutSpec {
            repository: "https://example.com/acme/widgets.git".into(),
            commit: "a".repeat(40),
            depth: None,
            patched: false,
        }),
        ..Default::default()
    });
    let err = told
        .execute(&fetching)
        .await
        .err()
        .expect("its checkout could not be fetched")
        .to_string();
    assert!(
        err.contains("has a checkout") && err.contains("init container of the task's own pod"),
        "{err}"
    );
    assert!(
        pods_of(&c, &fetching).await.is_empty(),
        "nothing was made for it"
    );
}

/// A pod has what any policy that selects it allows. So a namespace where
/// another policy selects a labelled pod and allows it something is one
/// where the standing policy does not hold, and the gate would not see it:
/// such a policy seldom opens the one address the gate asks. The engine
/// reads every policy of the namespace instead. One there as it starts stops
/// it; one that keeps away from the labelled pods does not; and one that
/// comes after it has started stops the next such task, with nothing made.
#[tokio::test]
async fn another_policy_that_gives_a_labelled_pod_something_back_stops_the_engine_and_the_task() {
    let Some(c) = cluster().await else { return };
    let name = a_namespace(&c).await;
    apply_the_policy(&c, &name).await;
    let name_server = serde_json::json!([{"ports": [{"protocol": "UDP", "port": 53}]}]);

    // Every pod of the namespace may reach a name server: the commonest of
    // them, and it selects the labelled pods with the rest.
    apply(
        &c,
        &name,
        "allow-dns",
        serde_json::json!({"podSelector": {}, "policyTypes": ["Egress"], "egress": name_server}),
    )
    .await;
    let refused = executor(&c, &name).await.with_network_policy(true).await;
    let policies: Api<NetworkPolicy> = Api::namespaced(c.client.clone(), &name);
    let _ = policies.delete("allow-dns", &DeleteParams::default()).await;

    // The same, kept from the pods labelled for no network: as the engine
    // tells an operator to write it.
    apply(
        &c,
        &name,
        "allow-dns-to-the-rest",
        serde_json::json!({
            "podSelector": {"matchExpressions": [{"key": NETWORK_LABEL, "operator": "DoesNotExist"}]},
            "policyTypes": ["Egress"],
            "egress": name_server,
        }),
    )
    .await;
    let started = executor(&c, &name).await.with_network_policy(true).await;

    // And one that comes after the engine has started.
    apply(
        &c,
        &name,
        "allow-the-internet",
        serde_json::json!({
            "podSelector": {"matchLabels": {"app": "module-54-scheduler"}},
            "policyTypes": ["Egress"],
            "egress": [{"to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}]}],
        }),
    )
    .await;
    let mut alone = task(REACH);
    alone.isolation = no_network();
    let dispatched = match &started {
        Ok(exec) => Some(exec.execute(&alone).await),
        Err(_) => None,
    };
    let id = &alone.identity.as_ref().unwrap().task_id;
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &name);
    let made = pods
        .list(&ListParams::default().labels(&format!("{LABEL_TASK_ID}={id}")))
        .await
        .map(|found| found.items.len());
    delete_namespace(&c, &name).await;

    let err = refused
        .err()
        .expect("allow-dns selects every pod and allows egress")
        .to_string();
    assert!(
        err.contains("DAGRON_TASK_NETWORK_POLICY=enforced")
            && err.contains("another NetworkPolicy selects such a task's pod")
            && err.contains("'allow-dns' (egress)")
            && err.contains("operator: DoesNotExist"),
        "{err}"
    );
    started.expect("a policy kept from the labelled pods is no one's concern");
    let err = dispatched
        .expect("the engine started")
        .err()
        .expect("a policy that came later gives the pod the internet")
        .to_string();
    assert!(
        err.contains("isolation.network: none")
            && err.contains("'allow-the-internet' (egress)")
            && err.contains("The task was not started"),
        "{err}"
    );
    assert_eq!(
        made.expect("the namespace's pods were listed"),
        0,
        "nothing was made for it"
    );
}

/// The gate's other half: a pod nothing takes the network from does not get
/// past it. It has the gate as its init container and no label, so the
/// standing policy does not select it. The gate gives up after the seconds
/// it was given, says what still answered, and ends with 125; the pod
/// fails, and its own container is never started.
#[tokio::test]
async fn a_pod_that_keeps_its_network_fails_at_the_gate_with_its_container_unstarted() {
    let Some(c) = cluster().await else { return };
    let name = format!(
        "dagron-test-gate-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name},
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 1,
            "initContainers": [{
                "name": GATE_NAME,
                "image": c.exec_image,
                "imagePullPolicy": "IfNotPresent",
                "args": [GATE_NAME, "5"],
            }],
            "containers": [{
                "name": "task",
                "image": TASK_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sh", "-c", "echo started"],
            }],
        },
    }))
    .unwrap();
    pods.create(&PostParams::default(), &pod)
        .await
        .expect("a pod with the gate and no label");

    let mut ended = None;
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let Ok(now) = pods.get(&name).await else {
            continue;
        };
        let status = now.status.unwrap_or_default();
        if matches!(status.phase.as_deref(), Some("Failed" | "Succeeded")) {
            ended = Some(status);
            break;
        }
    }
    let params = LogParams {
        container: Some(GATE_NAME.to_string()),
        ..LogParams::default()
    };
    let said = pods.logs(&name, &params).await.unwrap_or_default();
    let _ = pods.delete(&name, &DeleteParams::default()).await;

    let status = ended.expect("the pod ended within a minute");
    assert_eq!(status.phase.as_deref(), Some("Failed"), "{said}");
    let gate = status
        .init_container_statuses
        .unwrap_or_default()
        .into_iter()
        .find(|i| i.name == GATE_NAME)
        .and_then(|i| i.state?.terminated)
        .expect("the gate ended");
    assert_eq!(gate.exit_code, 125, "{said}");
    assert!(
        said.contains("this pod still has a network") && said.contains("answered"),
        "{said}"
    );
    let task = status.container_statuses.unwrap_or_default();
    assert!(
        task.iter().all(|t| t
            .state
            .as_ref()
            .is_none_or(|s| s.running.is_none() && s.terminated.is_none())),
        "its own container was never started: {task:?}"
    );
}

/// The label an allowlist of these tests allows, on the port it allows.
const ALLOWED_LABEL: &str = "dagron-test/allowed";
const LISTENS_ON: u16 = 8080;

/// A pod that answers plain HTTP on [`LISTENS_ON`], with `labels`. Its
/// address, once it has one and is running.
async fn a_listener(c: &Cluster, name: &str, labels: serde_json::Value) -> String {
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let pod: Pod = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "restartPolicy": "Never",
            "terminationGracePeriodSeconds": 1,
            "containers": [{
                "name": "listens",
                "image": TASK_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sh", "-c", format!(
                    "while true; do printf 'HTTP/1.0 200 OK\\r\\n\\r\\nok\\n' \
                     | nc -l -p {LISTENS_ON} >/dev/null; done"
                )],
            }],
        },
    }))
    .unwrap();
    pods.create(&PostParams::default(), &pod)
        .await
        .unwrap_or_else(|e| panic!("the listener {name}: {e}"));
    for _ in 0..120 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let Ok(now) = pods.get(name).await else {
            continue;
        };
        let status = now.status.unwrap_or_default();
        if status.phase.as_deref() == Some("Running") {
            if let Some(ip) = status.pod_ip {
                return ip;
            }
        }
    }
    panic!("the listener {name} was not running within a minute");
}

/// What a task says of three places, in this order: a pod its allowlist does
/// not name and the API server, each asked once and at once, and then the
/// pod its allowlist names, asked until it answers or ten seconds pass (a
/// CNI that allows by a pod's label learns of the pod a moment after it is
/// there).
fn reaches(other: &str, allowed: &str) -> String {
    format!(
        "once() {{ wget -T 3 -q -O /dev/null \"http://$1/\" 2>/dev/null \
            && echo \"$2: reached\" || echo \"$2: unreachable\"; }}; \
         once {other}:{LISTENS_ON} other; \
         wget -T 3 -O /dev/null http://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/ 2>&1 \
            | grep -q '400 Bad Request' && echo 'api: reached' || echo 'api: unreachable'; \
         i=0; while [ $i -lt 10 ]; do \
            wget -T 2 -q -O /dev/null http://{allowed}:{LISTENS_ON}/ 2>/dev/null \
                && {{ echo 'allowed: reached'; break; }}; \
            i=$((i+1)); sleep 1; done; \
         [ $i -lt 10 ] || echo 'allowed: unreachable'; sleep 4"
    )
}

/// A task held to an allowlist reaches what its list names and nothing else
/// that was asked: not a pod beside the one it may reach, and not the API
/// server, from the first thing it does. A task that asked for nothing
/// reaches all three. The held one starts behind the allowlist's own gate,
/// whose words are read from its pod.
#[tokio::test]
async fn a_task_held_to_an_allowlist_reaches_what_its_list_names_and_nothing_else() {
    let Some(c) = cluster().await else { return };
    apply_the_policy(&c, &c.namespace).await;
    let unique = uuid::Uuid::new_v4().simple().to_string();
    let unique = &unique[..8];
    // The chart's allowlist where the script that brought the cluster up had
    // it made, and one of this test's own where it did not.
    let name = match std::env::var("DAGRON_KUBE_LIVE_ALLOWLIST") {
        Ok(name) if !name.is_empty() => name,
        _ => {
            let name = "live".to_string();
            let policies: Api<NetworkPolicy> = Api::namespaced(c.client.clone(), &c.namespace);
            let policy = allowlist_policy(
                &name,
                serde_json::json!([{
                    "to": [{"podSelector": {"matchLabels": {ALLOWED_LABEL: "yes"}}}],
                    "ports": [{"protocol": "TCP", "port": LISTENS_ON}],
                }]),
            );
            match policies.create(&PostParams::default(), &policy).await {
                Ok(_) => {}
                Err(kube::Error::Api(status)) if status.code == 409 => {}
                Err(e) => panic!("applying {}: {e}", allowlist_policy_name(&name)),
            }
            name
        }
    };
    let (allowed_pod, other_pod) = (format!("allowed-{unique}"), format!("other-{unique}"));
    let allowed = a_listener(&c, &allowed_pod, serde_json::json!({ALLOWED_LABEL: "yes"})).await;
    let other = a_listener(
        &c,
        &other_pod,
        serde_json::json!({"dagron-test/role": "other"}),
    )
    .await;

    let exec = executor(&c, &c.namespace)
        .await
        .with_network_policy(true)
        .await
        .expect("the standing policy is there")
        .with_network_allowlists(vec![name.clone()])
        .await;
    let ran = match &exec {
        Ok(exec) => {
            let open = task(&reaches(&other, &allowed));
            let mut held = task(&reaches(&other, &allowed));
            held.isolation = Some(IsolationSpec {
                network: Some(Network::Allow(name.clone())),
                ..Default::default()
            });
            let open_out = exec.execute(&open).await;
            let (held_out, gate) = tokio::join!(
                exec.execute(&held),
                what_a_gate_said(&c, &held, ALLOWLIST_GATE_NAME)
            );
            Some((open_out, held_out, gate))
        }
        Err(_) => None,
    };
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    for pod in [&allowed_pod, &other_pod] {
        let _ = pods.delete(pod, &DeleteParams::default()).await;
    }

    exec.expect("the allowlist's policy is in the namespace, and is one");
    let (open_out, held_out, gate) = ran.expect("the engine started");
    let open_out = open_out.expect("the task ran");
    assert_eq!(
        open_out.output, "other: reached\napi: reached\nallowed: reached\n",
        "a task that asked for nothing"
    );
    let held_out = held_out.expect("the task ran");
    assert!(held_out.success, "{}", held_out.output);
    assert_eq!(
        held_out.output, "other: unreachable\napi: unreachable\nallowed: reached\n",
        "a task held to the allowlist '{name}' ({NETWORK_LABEL}=allow-{name})"
    );
    let (code, said) = gate.expect("the task's pod was seen, with a gate that had ended");
    assert_eq!(code, 0, "{said}");
    assert!(
        said.contains("this pod is held: nothing answered at")
            && said.contains("the cluster's API server")
            && !said.contains("name server"),
        "the allowlist's gate asks the API server alone: {said}"
    );
    eprintln!("the allowlist's gate: {}", said.trim());
}

/// An allowlist is one the operator named, with a policy that is there. A
/// task that names another is refused before anything is made; an engine
/// told of one whose policy is not in the namespace does not start; and
/// neither does one told of allowlists and not that the cluster enforces a
/// policy.
#[tokio::test]
async fn an_allowlist_nobody_named_or_made_is_refused() {
    let Some(c) = cluster().await else { return };
    apply_the_policy(&c, &c.namespace).await;

    // Told the cluster enforces a policy, and of no allowlist.
    let told = executor(&c, &c.namespace)
        .await
        .with_network_policy(true)
        .await
        .expect("the standing policy is there");
    let mut held = task(REACH);
    held.isolation = Some(IsolationSpec {
        network: Some(Network::Allow("nobody-named-this".into())),
        ..Default::default()
    });
    let err = told
        .execute(&held)
        .await
        .err()
        .expect("the engine was told of no such allowlist")
        .to_string();
    assert!(
        err.contains("isolation.network: allow:nobody-named-this")
            && err.contains("was not told of an allowlist of that name")
            && err.contains("DAGRON_TASK_NETWORK_ALLOWLISTS names none"),
        "{err}"
    );
    assert!(
        pods_of(&c, &held).await.is_empty(),
        "nothing was made for it"
    );

    // Told of one whose policy nobody applied.
    let missing = format!(
        "missing-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let err = told
        .with_network_allowlists(vec![missing.clone()])
        .await
        .err()
        .expect("its policy is not in the namespace")
        .to_string();
    assert!(
        err.contains(&format!("DAGRON_TASK_NETWORK_ALLOWLISTS names '{missing}'"))
            && err.contains(&format!(
                "'{}' is not there",
                allowlist_policy_name(&missing)
            ))
            && err.contains("engine.taskNetworkAllowlists"),
        "{err}"
    );

    // Told of allowlists, and not that anything enforces them.
    let err = executor(&c, &c.namespace)
        .await
        .with_network_allowlists(vec!["live".into()])
        .await
        .err()
        .expect("an allowlist is a policy")
        .to_string();
    assert!(
        err.contains("DAGRON_TASK_NETWORK_POLICY is not 'enforced'"),
        "{err}"
    );
}
