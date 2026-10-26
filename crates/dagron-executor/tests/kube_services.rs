//! A task's services on a real cluster, through the Kubernetes executor:
//! each is a sidecar container of the task's pod, started before the task
//! and seen ready by its startup probe; the task finds each at its name and
//! at the loopback address; the task has ended when its own container has,
//! though a service that heeds no signal holds the pod for its grace; a
//! service that is never ready, or that stops first, fails the attempt in
//! its own words; a task that asked for no network still reaches its
//! services, and nothing else; and nothing is left of any of them.
//!
//! A service here is a small HTTP answer from busybox, [`SERVICE`], which
//! listens only after a pause, and which heeds no signal: it is its
//! container's first process, and was not written to.
//!
//! Skips, saying so, unless `DAGRON_KUBE_LIVE` names a namespace the
//! kubeconfig reaches, as `kube_workspace.rs` does. The cluster has to be
//! Kubernetes 1.29 or later. The test of a task with no network needs
//! `DAGRON_KUBE_LIVE_EXEC_IMAGE` too, and a cluster that enforces
//! NetworkPolicy, as `kube_network.rs` does. tests/k3s.sh runs this.
#![cfg(feature = "kubernetes")]

use std::time::{Duration, Instant};

use dagron_core::isolation::{IsolationSpec, Network};
use dagron_core::service::ServiceSpec;
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity, LABEL_TASK_ID};
use dagron_executor::kube_executor::{no_network_policy, KubeExecutor, NO_NETWORK_POLICY};
use dagron_executor::workspace::KubeWorkspaces;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::{Api, ListParams, PostParams};

const TASK_IMAGE: &str = "alpine:3.20";

/// What a test's service runs: after `$PAUSE` seconds it marks itself up and
/// answers every HTTP request on `$PORT` with its `$NAME`.
const SERVICE: &str = r#"cat > /tmp/answer.sh <<'EOF'
#!/bin/sh
while read -r line; do [ "${#line}" -le 1 ] && break; done
printf 'HTTP/1.0 200 OK\r\n\r\nhello from %s\n' "$NAME"
EOF
chmod +x /tmp/answer.sh
sleep "$PAUSE"
touch /tmp/up
exec nc -lk -p "$PORT" -e /tmp/answer.sh
"#;

struct Cluster {
    namespace: String,
    client: kube::Client,
}

async fn cluster() -> Option<Cluster> {
    let Ok(namespace) = std::env::var("DAGRON_KUBE_LIVE") else {
        eprintln!("DAGRON_KUBE_LIVE names no namespace - skipping");
        return None;
    };
    dagron_executor::install_crypto_provider();
    let client = kube::Client::try_default()
        .await
        .expect("DAGRON_KUBE_LIVE is set, so a kubeconfig that reaches a cluster");
    Some(Cluster { namespace, client })
}

async fn executor(c: &Cluster) -> KubeExecutor {
    KubeExecutor::connect(TASK_IMAGE, &c.namespace)
        .await
        .expect("the executor reaches the namespace")
}

fn task(script: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(180),
        Some(TASK_IMAGE.into()),
    );
    ctx.identity = Some(TaskIdentity {
        task_id: format!("t-{}", uuid::Uuid::new_v4().simple()),
        run_id: "r-services".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

/// A service that answers on `port` once `pause` seconds have passed, and
/// says it is ready once it does, by `ready`.
fn answering(name: &str, port: u16, pause: u32, ready: serde_json::Value) -> ServiceSpec {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "image": TASK_IMAGE,
        "args": ["sh", "-c", SERVICE],
        "env": [
            { "name": "NAME", "value": name },
            { "name": "PORT", "value": port.to_string() },
            { "name": "PAUSE", "value": pause.to_string() },
        ],
        "ready": ready,
    }))
    .unwrap()
}

fn one(service: serde_json::Value) -> ServiceSpec {
    serde_json::from_value(service).unwrap()
}

/// The pods carrying `ctx`'s task id that are not on their way out: what its
/// attempt left. A pod deleted a moment ago is given that moment.
async fn pods_of(c: &Cluster, ctx: &ExecContext) -> Vec<String> {
    let id = &ctx.identity.as_ref().expect("an identity").task_id;
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let mut left = Vec::new();
    for _ in 0..30 {
        left = pods
            .list(&ListParams::default().labels(&format!("{LABEL_TASK_ID}={id}")))
            .await
            .expect("list the task's pods")
            .items
            .into_iter()
            .filter_map(|p| p.metadata.name)
            .collect();
        if left.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    left
}

/// A task's services are up before it starts: each answers the one request
/// the task makes of it, at its name and at the loopback address, though
/// none listens until seconds after it was started. And the task has ended
/// when its own container has: neither service heeds a signal, and each
/// would hold the pod for its grace, which is half a minute.
#[tokio::test]
async fn a_task_s_services_are_ready_before_it_starts_and_do_not_hold_it_up_after() {
    let Some(c) = cluster().await else { return };
    let exec = executor(&c).await;
    let mut ctx = task(
        "wget -qO- http://web:9000/ && wget -qO- http://api:9001/ && \
         wget -qO- http://localhost:9000/ && wget -qO- http://127.0.0.1:9001/",
    );
    ctx.services = vec![
        answering("web", 9000, 2, serde_json::json!("test -f /tmp/up")),
        answering("api", 9001, 1, serde_json::json!(["test", "-f", "/tmp/up"])),
    ];
    let started = Instant::now();
    let out = exec.execute(&ctx).await.unwrap();
    let took = started.elapsed();
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output.lines().collect::<Vec<_>>(),
        [
            "hello from web",
            "hello from api",
            "hello from web",
            "hello from api"
        ],
        "the task's own output, and nothing of its services'"
    );
    assert!(
        took >= Duration::from_secs(3),
        "each was waited for in turn: {took:?}"
    );
    assert!(
        took < Duration::from_secs(25),
        "the task ended with its own container, and did not wait out its services: {took:?}"
    );
    assert!(
        pods_of(&c, &ctx).await.is_empty(),
        "its pod is gone, and soon: its services were given no grace"
    );
}

/// A task that fails beside its services fails, with what it said.
#[tokio::test]
async fn a_task_that_fails_beside_its_services_fails() {
    let Some(c) = cluster().await else { return };
    let exec = executor(&c).await;
    let mut ctx = task("wget -qO- http://web:9000/; echo and then it broke >&2; exit 3");
    ctx.services = vec![answering(
        "web",
        9000,
        0,
        serde_json::json!("test -f /tmp/up"),
    )];
    let out = exec.execute(&ctx).await.unwrap();
    assert!(!out.success, "{}", out.output);
    assert!(
        out.output.contains("hello from web") && out.output.contains("and then it broke"),
        "{}",
        out.output
    );
    assert!(pods_of(&c, &ctx).await.is_empty());
}

/// A service that is not ready within the time it has fails the attempt,
/// which says which and for how long, with the last the service wrote. One
/// that stops before it is ready fails it with what it exited with. Either
/// way the task never starts, and no pod is left.
#[tokio::test]
async fn a_service_that_is_never_ready_fails_the_attempt_in_its_own_words() {
    let Some(c) = cluster().await else { return };
    let exec = executor(&c).await;

    let mut ctx = task("echo the task ran");
    ctx.services = vec![one(serde_json::json!({
        "name": "slow",
        "image": TASK_IMAGE,
        "args": ["sh", "-c", "echo warming up >&2; sleep 300"],
        "ready": "echo not yet; exit 7",
        "ready_timeout_secs": 3,
    }))];
    let started = Instant::now();
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("never ready")
        .to_string();
    assert!(
        err.contains("the service 'slow' was not ready within 3 s")
            && err.contains("it said: warming up"),
        "{err}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "it is not waited for past its own time: {:?}",
        started.elapsed()
    );
    assert!(pods_of(&c, &ctx).await.is_empty(), "nothing is left");

    let mut ctx = task("echo the task ran");
    ctx.services = vec![
        answering("web", 9000, 0, serde_json::json!("test -f /tmp/up")),
        one(serde_json::json!({
            "name": "broken",
            "image": TASK_IMAGE,
            "args": ["sh", "-c", "echo no configuration given >&2; exit 3"],
            "ready": "true",
        })),
    ];
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("it stopped")
        .to_string();
    assert!(
        err.contains("the service 'broken' stopped before it was ready")
            && err.contains("exited 3")
            && err.contains("it said: no configuration given"),
        "{err}"
    );
    assert!(pods_of(&c, &ctx).await.is_empty());
}

/// A task that asked for no network has its services all the same: they are
/// containers of its own pod, at its loopback address, which a policy on the
/// pod's network does not come between. And it reaches nothing else. Its
/// services start after the gate that sees the pod held, as the task does.
#[tokio::test]
async fn a_task_with_no_network_reaches_its_services_and_nothing_else() {
    let Some(c) = cluster().await else { return };
    let Ok(exec_image) = std::env::var("DAGRON_KUBE_LIVE_EXEC_IMAGE") else {
        eprintln!("DAGRON_KUBE_LIVE_EXEC_IMAGE names no dagron-ci-exec image - skipping");
        return;
    };
    let policies: Api<NetworkPolicy> = Api::namespaced(c.client.clone(), &c.namespace);
    match policies
        .create(&PostParams::default(), &no_network_policy())
        .await
    {
        Ok(_) => {}
        Err(kube::Error::Api(status)) if status.code == 409 => {}
        Err(e) => panic!("applying {NO_NETWORK_POLICY}: {e}"),
    }
    let exec = executor(&c)
        .await
        .with_workspaces(KubeWorkspaces {
            ci_exec_image: Some(exec_image),
            ..Default::default()
        })
        .with_network_policy(true)
        .await
        .expect("the namespace holds the policy");
    let mut ctx = task(
        "wget -qO- http://web:9000/ && wget -qO- http://localhost:9000/ && \
         { wget -T 5 -O /dev/null http://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/ 2>&1 \
           | grep -q '400 Bad Request' && echo reached || echo unreachable; }",
    );
    ctx.isolation = Some(IsolationSpec {
        network: Some(Network::None),
        ..Default::default()
    });
    ctx.services = vec![answering(
        "web",
        9000,
        1,
        serde_json::json!("test -f /tmp/up"),
    )];
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output.lines().collect::<Vec<_>>(),
        ["hello from web", "hello from web", "unreachable"],
        "{}",
        out.output
    );
    assert!(pods_of(&c, &ctx).await.is_empty());
}

/// A lost claim takes the services down with the task: the pod is deleted
/// and seen gone, with every container of it.
#[tokio::test]
async fn a_lost_claim_removes_the_task_s_services_with_it() {
    let Some(c) = cluster().await else { return };
    let exec = executor(&c).await;
    let mut ctx = task("trap 'exit 0' TERM; echo up; while :; do sleep 1; done");
    ctx.services = vec![answering(
        "web",
        9000,
        0,
        serde_json::json!("test -f /tmp/up"),
    )];
    ctx.env.push(dagron_core::dag::EnvVar {
        name: dagron_executor::executor::ENV_STEP_GRACE.into(),
        value: "2".into(),
        value_from: None,
    });
    let id = ctx.identity.as_ref().unwrap().task_id.clone();
    let stop = ctx.stop.clone();
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let running = tokio::spawn(async move { exec.execute(&ctx).await });
    let mut up = false;
    for _ in 0..240 {
        let listed = pods
            .list(&ListParams::default().labels(&format!("{LABEL_TASK_ID}={id}")))
            .await
            .unwrap();
        up = listed.items.iter().any(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.container_statuses.as_ref())
                .is_some_and(|cs| {
                    cs.iter()
                        .any(|c| c.state.as_ref().is_some_and(|s| s.running.is_some()))
                })
        });
        if up {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(up, "the task's container never ran");
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.request();
    let err = tokio::time::timeout(Duration::from_secs(120), running)
        .await
        .expect("a stopped execution ends")
        .unwrap()
        .expect_err("a lost claim is no result");
    assert!(
        err.is::<dagron_executor::executor::ClaimLost>(),
        "it says the claim was lost: {err}"
    );
    assert!(
        !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "and that the pod was seen gone: {err}"
    );
    let left = pods
        .list(&ListParams::default().labels(&format!("{LABEL_TASK_ID}={id}")))
        .await
        .unwrap()
        .items;
    assert!(left.is_empty(), "no pod of the attempt is left");
}
