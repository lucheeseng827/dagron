//! The executor's one watch of its task pods, on a real cluster: tasks that
//! run together are each told of their own pod by it, and ask the API server
//! for a pod only until the watch has first seen it; with the watch off they
//! are run as before, by asking twice a second; and a pod that is deleted
//! under its task ends the task's wait, by the watch as it did by asking.
//!
//! Skips, saying so, unless `DAGRON_KUBE_LIVE` names a namespace the
//! kubeconfig reaches, as `kube_workspace.rs` does. tests/k3s.sh runs this.
#![cfg(feature = "kubernetes")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use dagron_executor::executor::{ExecContext, Executor, TaskIdentity, LABEL_TASK_ID};
use dagron_executor::kube_executor::KubeExecutor;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams};

const TASK_IMAGE: &str = "alpine:3.20";
/// How many tasks run together, and for how long each.
const TASKS: usize = 6;
const TASK_SECS: u64 = 6;

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

fn task(script: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(180),
        Some(TASK_IMAGE.into()),
    );
    ctx.identity = Some(TaskIdentity {
        task_id: format!("t-{}", uuid::Uuid::new_v4().simple()),
        run_id: "r-watch".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

/// Run [`TASKS`] tasks together on `exec`, each for [`TASK_SECS`], and say
/// what each wrote.
async fn run_together(exec: Arc<KubeExecutor>) -> Vec<String> {
    let mut going = Vec::new();
    for n in 0..TASKS {
        let exec = Arc::clone(&exec);
        going.push(tokio::spawn(async move {
            let out = exec
                .execute(&task(&format!("sleep {TASK_SECS}; echo done-{n}")))
                .await
                .expect("the task ran");
            assert!(out.success, "task {n}: {}", out.output);
            out.output.trim().to_string()
        }));
    }
    let mut said = Vec::new();
    for task in going {
        said.push(task.await.expect("the task's wait did not panic"));
    }
    said
}

#[tokio::test(flavor = "multi_thread")]
async fn tasks_are_told_of_their_pods_by_one_watch() {
    let Some(c) = cluster().await else { return };
    let expected: Vec<String> = (0..TASKS).map(|n| format!("done-{n}")).collect();

    // With the watch: it follows the namespace soon after the executor is
    // made, and a task asks for its pod only until the watch has seen it.
    let watched = Arc::new(
        KubeExecutor::connect(TASK_IMAGE, &c.namespace)
            .await
            .expect("the executor reaches the namespace"),
    );
    let began = Instant::now();
    while !watched.pod_watch_live() {
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "the watch never followed the namespace"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(run_together(Arc::clone(&watched)).await, expected);
    assert!(
        watched.pod_watch_live(),
        "the watch is still following after its tasks"
    );
    let asked_watched = watched.pod_gets();

    // Without it, as before: every task asks twice a second while it runs.
    let asking = Arc::new(
        KubeExecutor::connect(TASK_IMAGE, &c.namespace)
            .await
            .expect("the executor reaches the namespace")
            .with_pod_watch(false),
    );
    assert!(!asking.pod_watch_live());
    assert_eq!(run_together(Arc::clone(&asking)).await, expected);
    let asked_asking = asking.pod_gets();

    eprintln!(
        "{TASKS} tasks of {TASK_SECS} s: {asked_watched} requests for a pod with the watch, \
         {asked_asking} without"
    );
    // Asking twice a second for TASK_SECS is a dozen requests a task at the
    // least. With the watch a task asks until the watch has seen its pod,
    // which is once or twice.
    assert!(
        asked_asking >= (TASKS as u64) * TASK_SECS,
        "without the watch a task asks for as long as it runs: {asked_asking}"
    );
    assert!(
        asked_watched <= (TASKS as u64) * 4,
        "with the watch a task asks only until its pod is seen: {asked_watched}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pod_deleted_under_its_task_ends_the_task_s_wait() {
    let Some(c) = cluster().await else { return };
    let exec = Arc::new(
        KubeExecutor::connect(TASK_IMAGE, &c.namespace)
            .await
            .expect("the executor reaches the namespace"),
    );
    let began = Instant::now();
    while !exec.pod_watch_live() {
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "the watch never followed the namespace"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A task that would run for minutes.
    let ctx = task("sleep 170; echo never");
    let id = ctx.identity.as_ref().unwrap().task_id.clone();
    let waiting = {
        let exec = Arc::clone(&exec);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };

    // Its pod, once it is running: long enough that the watch has seen it,
    // so that the task is waiting on the watch and not asking.
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let selector = format!("{LABEL_TASK_ID}={id}");
    let began = Instant::now();
    let name = loop {
        let listed = pods
            .list(&ListParams::default().labels(&selector))
            .await
            .expect("list the task's pod");
        let running = listed
            .items
            .into_iter()
            .find(|pod| pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running"));
        if let Some(pod) = running {
            break pod.metadata.name.expect("a pod has a name");
        }
        assert!(
            began.elapsed() < Duration::from_secs(120),
            "the task's pod never ran"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    let asked_before = exec.pod_gets();

    // Deleted at once, by someone else.
    pods.delete(&name, &DeleteParams::default().grace_period(0))
        .await
        .expect("delete the pod");
    let deleted = Instant::now();
    let ended = tokio::time::timeout(Duration::from_secs(60), waiting)
        .await
        .expect("the task's wait ended well before its timeout")
        .expect("the task's wait did not panic");
    eprintln!(
        "the wait ended {:?} after the delete, having asked {} more times: {}",
        deleted.elapsed(),
        exec.pod_gets() - asked_before,
        match &ended {
            Ok(out) => format!("success={} {}", out.success, out.output.trim()),
            Err(e) => format!("{e:#}"),
        }
    );
    // It did not succeed: its pod went before its work was done. Whether it
    // is told as a failed pod or as a pod that is not there is the
    // cluster's timing, as it was when the task asked for itself.
    assert!(
        !matches!(&ended, Ok(out) if out.success),
        "a task whose pod was deleted did not succeed"
    );
}
