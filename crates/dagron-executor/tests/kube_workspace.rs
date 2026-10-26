//! Workspace tasks on a real cluster, through the Kubernetes executor: steps
//! that share files in the workspace, with `dagron-ci-exec` installed
//! read-only beside them by the init container; an install that fails; a
//! workspace that outgrows its size; one that is a volume claim of its own;
//! one that starts with its checkout, fetched by an init container from a git
//! server of the test's own, and one whose checkout cannot be fetched. Each
//! leaves no pod behind. And an attempt whose claim on its task is gone:
//! its pod is deleted and seen gone.
//!
//! Skips, saying so, unless `DAGRON_KUBE_LIVE` names a namespace the
//! kubeconfig reaches. The task image is alpine:3.20, which the cluster pulls.
//! `DAGRON_KUBE_LIVE_EXEC_IMAGE` names a dagron-ci-exec image the cluster can
//! run, `DAGRON_KUBE_LIVE_STORAGE_CLASS` a StorageClass for the claim, and
//! `DAGRON_KUBE_LIVE_CHECKOUT_IMAGE` an image with git and sh with
//! `DAGRON_KUBE_LIVE_HOST` the address the cluster's pods reach this host at;
//! the test needing one skips without it. tests/k3s.sh sets them all. On k3s:
//!
//! ```sh
//! docker build -t dagron-ci-exec:test -f crates/dagron-ci-exec/Dockerfile .
//! docker save dagron-ci-exec:test | sudo k3s ctr images import -
//! DAGRON_KUBE_LIVE=default DAGRON_KUBE_LIVE_EXEC_IMAGE=dagron-ci-exec:test \
//! DAGRON_KUBE_LIVE_STORAGE_CLASS=local-path \
//!   cargo test -p dagron-executor --features kubernetes,dagron-core/sqlite \
//!   --test kube_workspace
//! ```
#![cfg(feature = "kubernetes")]

use std::time::Duration;

use dagron_core::dag::EnvVar;
use dagron_core::workspace::WorkspaceSpec;
use dagron_executor::executor::{ExecContext, Executor, LogSink, TaskIdentity, LABEL_TASK_ID};
use dagron_executor::kube_executor::KubeExecutor;
use dagron_executor::redact::Redactor;
use dagron_executor::workspace::KubeWorkspaces;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kube::api::{Api, ListParams};

mod support;

const TASK_IMAGE: &str = "alpine:3.20";

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

fn exec_image() -> Option<String> {
    let image = std::env::var("DAGRON_KUBE_LIVE_EXEC_IMAGE").ok();
    if image.is_none() {
        eprintln!("DAGRON_KUBE_LIVE_EXEC_IMAGE names no dagron-ci-exec image - skipping");
    }
    image
}

async fn executor(c: &Cluster, workspaces: KubeWorkspaces) -> KubeExecutor {
    KubeExecutor::connect(TASK_IMAGE, &c.namespace)
        .await
        .expect("the executor reaches the namespace")
        .with_workspaces(workspaces)
}

/// A task with a workspace of `size`, its own id for its pod's labels.
fn task(command: &[&str], size: Option<&str>) -> ExecContext {
    let mut ctx = ExecContext::new(
        command.iter().map(|s| s.to_string()).collect(),
        Some(300),
        Some(TASK_IMAGE.into()),
    );
    ctx.workspace = Some(WorkspaceSpec {
        size: size.map(str::to_string),
        ..WorkspaceSpec::default()
    });
    ctx.identity = Some(TaskIdentity {
        task_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

fn env(ctx: &mut ExecContext, name: &str, value: &str) {
    ctx.env.push(EnvVar {
        name: name.into(),
        value: value.into(),
        value_from: None,
    });
}

/// The pods this task left: none, once `execute` has returned and the pod it
/// deleted has finished terminating, which takes the kubelet a moment.
async fn pods_of(c: &Cluster, ctx: &ExecContext) -> Vec<String> {
    let task = &ctx.identity.as_ref().unwrap().task_id;
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let selector = format!("{LABEL_TASK_ID}={task}");
    let mut left = Vec::new();
    for _ in 0..30 {
        left = pods
            .list(&ListParams::default().labels(&selector))
            .await
            .unwrap()
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

#[tokio::test]
async fn steps_share_the_workspace_and_find_dagron_ci_exec_beside_it() {
    let Some(c) = cluster().await else { return };
    let Some(image) = exec_image() else { return };
    let exec = executor(
        &c,
        KubeWorkspaces {
            ci_exec_image: Some(image),
            ..KubeWorkspaces::default()
        },
    )
    .await;
    // As dagron-ci compiles a task with steps: step protocol 1, run by the
    // binary the init container installed.
    let mut ctx = task(&["/dagron/bin/dagron-ci-exec"], Some("64Mi"));
    env(&mut ctx, "CI_STEP_PROTOCOL", "1");
    env(&mut ctx, "CI_STEP_COUNT", "2");
    env(&mut ctx, "CI_STEP_1_NAME", "write");
    env(
        &mut ctx,
        "CI_STEP_1_RUN",
        "pwd; head -c 1048576 /dev/zero > shared; \
         if touch /dagron/bin/x 2>/dev/null; then echo tools-writable; else echo tools-read-only; fi",
    );
    env(&mut ctx, "CI_STEP_2_NAME", "read");
    env(&mut ctx, "CI_STEP_2_RUN", "wc -c < shared");

    let out = exec.execute(&ctx).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    for line in [
        "::dagron-ci::step::1::start::write\n",
        "/workspace\n",
        "tools-read-only\n",
        "::dagron-ci::step::2::start::read\n",
        "1048576\n",
    ] {
        assert!(out.output.contains(line), "{line:?} in {:?}", out.output);
    }
    assert!(
        out.output.contains("::dagron-ci::result::{") && out.output.contains("\"exit_code\":0"),
        "{}",
        out.output
    );
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_failed_install_fails_the_task_in_the_installer_s_words() {
    let Some(c) = cluster().await else { return };
    // alpine as the installer: its `install /dagron/bin` is busybox's, which
    // wants a destination, says so, and exits 1.
    let exec = executor(
        &c,
        KubeWorkspaces {
            ci_exec_image: Some(TASK_IMAGE.into()),
            ..KubeWorkspaces::default()
        },
    )
    .await;
    let ctx = task(&["sh", "-c", "echo the task never ran"], None);
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("no tools, no task")
        .to_string();
    assert!(
        err.contains("the init container install-dagron-ci-exec exited 1"),
        "{err}"
    );
    assert!(err.contains("Usage: install"), "{err}");
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_workspace_past_its_size_is_evicted_and_the_task_says_so() {
    let Some(c) = cluster().await else { return };
    let exec = executor(&c, KubeWorkspaces::default()).await;
    // The kubelet measures a workspace every few seconds and evicts a pod
    // whose workspace is past its limit: the sleep outlasts that.
    let mut ctx = task(
        &[
            "sh",
            "-c",
            "echo writing; head -c 67108864 /dev/zero > big; sleep 240",
        ],
        Some("16Mi"),
    );
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    ctx.log_sink = Some(LogSink::new(tx, "t-evicted".into(), 1, Redactor::default()));
    let out = exec.execute(&ctx).await.expect("a failed pod is a result");
    assert!(!out.success, "{}", out.output);
    assert!(
        out.output.contains("the task's pod failed (Evicted)")
            && out.output.contains("exceeds the limit \"16Mi\""),
        "{}",
        out.output
    );
    // And its log, which is what is read of a task that has one: what it
    // wrote, then why its pod failed.
    let mut log = String::new();
    while let Ok(chunk) = rx.try_recv() {
        log.push_str(&chunk.chunk);
    }
    assert!(
        log.starts_with("writing\nthe task's pod failed (Evicted)") && log.ends_with('\n'),
        "{log}"
    );
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_workspace_from_the_claim_size_up_is_a_volume_of_its_own() {
    let Some(c) = cluster().await else { return };
    let Ok(class) = std::env::var("DAGRON_KUBE_LIVE_STORAGE_CLASS") else {
        return eprintln!("DAGRON_KUBE_LIVE_STORAGE_CLASS names no StorageClass - skipping");
    };
    let claims: Api<PersistentVolumeClaim> = Api::namespaced(c.client.clone(), &c.namespace);
    let names = |list: kube::core::ObjectList<PersistentVolumeClaim>| -> Vec<String> {
        list.items
            .into_iter()
            .filter_map(|p| p.metadata.name)
            .collect()
    };
    let before = names(claims.list(&ListParams::default()).await.unwrap());

    let exec = executor(
        &c,
        KubeWorkspaces {
            pvc_from: Some(1 << 20),
            storage_class: Some(class),
            ..KubeWorkspaces::default()
        },
    )
    .await;
    // Where /workspace is mounted from: a claim's volume, not an emptyDir.
    let ctx = task(
        &[
            "sh",
            "-c",
            "awk '$5 == \"/workspace\" { print $4 }' /proc/self/mountinfo; \
             head -c 1048576 /dev/zero > f && wc -c < f",
        ],
        Some("64Mi"),
    );
    let out = exec.execute(&ctx).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert!(out.output.ends_with("1048576\n"), "{}", out.output);
    assert!(
        !out.output.contains("kubernetes.io~empty-dir"),
        "the workspace is a volume of its own: {}",
        out.output
    );
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());
    // The claim goes with its pod, by the garbage collector.
    for _ in 0..60 {
        if names(claims.list(&ListParams::default()).await.unwrap()) == before {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!(
        "the task's claim outlived its pod: {:?}",
        names(claims.list(&ListParams::default()).await.unwrap())
    );
}

/// What a checkout test needs: the image to fetch with, and where pods reach
/// this host, whose git server they fetch from.
fn checkout_settings() -> Option<(String, String)> {
    match (
        std::env::var("DAGRON_KUBE_LIVE_CHECKOUT_IMAGE"),
        std::env::var("DAGRON_KUBE_LIVE_HOST"),
    ) {
        (Ok(image), Ok(host)) => Some((image, host)),
        _ => {
            eprintln!(
                "DAGRON_KUBE_LIVE_CHECKOUT_IMAGE or DAGRON_KUBE_LIVE_HOST is unset - skipping"
            );
            None
        }
    }
}

#[tokio::test]
async fn a_workspace_starts_with_its_checkout_fetched_by_an_init_container() {
    let Some(c) = cluster().await else { return };
    let Some((image, host)) = checkout_settings() else {
        return;
    };
    let Some(server) = support::git_server::GitServer::start(
        &[
            ("README", "hello from the commit\n"),
            ("src/build.sh", "echo built\n"),
        ],
        "0.0.0.0",
        &host,
        None,
    ) else {
        return;
    };
    let exec = executor(
        &c,
        KubeWorkspaces {
            ci_exec_image: exec_image(),
            checkout_image: Some(image.clone()),
            ..KubeWorkspaces::default()
        },
    )
    .await;
    // The task in the fetch's own image, which has git: its entrypoint is
    // git, and the task's command replaces it.
    let mut ctx = task(
        &[
            "sh",
            "-c",
            "cat README; sh src/build.sh; git rev-parse HEAD; git status --porcelain | wc -l",
        ],
        None,
    );
    ctx.docker_image = Some(image);
    ctx.workspace.as_mut().unwrap().checkout = Some(dagron_core::workspace::CheckoutSpec {
        repository: server.url.clone(),
        commit: server.commit.clone(),
        depth: None,
        patched: false,
    });
    let out = exec.execute(&ctx).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        format!("hello from the commit\nbuilt\n{}\n0\n", server.commit)
    );
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_checkout_that_cannot_be_fetched_fails_the_task_in_git_s_words() {
    let Some(c) = cluster().await else { return };
    let Some((image, host)) = checkout_settings() else {
        return;
    };
    let Some(server) =
        support::git_server::GitServer::start(&[("README", "x\n")], "0.0.0.0", &host, None)
    else {
        return;
    };
    let exec = executor(
        &c,
        KubeWorkspaces {
            checkout_image: Some(image),
            ..KubeWorkspaces::default()
        },
    )
    .await;
    let mut ctx = task(&["sh", "-c", "echo the task never ran"], None);
    let missing = "0123456789abcdef0123456789abcdef01234567";
    ctx.workspace.as_mut().unwrap().checkout = Some(dagron_core::workspace::CheckoutSpec {
        repository: server.url.clone(),
        commit: missing.into(),
        depth: None,
        patched: false,
    });
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("no checkout, no task")
        .to_string();
    assert!(
        err.contains(&format!(
            "the checkout of {} at {missing} failed (exited 128)",
            server.url
        )),
        "{err}"
    );
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());
}

/// A cancel takes a task's claim away. Its worker used to stop waiting, and
/// the pod ran on to the end of its command. Now the pod is deleted with the
/// task's grace, and the executor waits to see it gone before it says so: a
/// delete alone is a request, and the pod is `Terminating` while it is met.
#[tokio::test]
async fn a_lost_claim_deletes_the_pod_and_sees_it_gone() {
    let Some(c) = cluster().await else { return };
    let exec = executor(&c, KubeWorkspaces::default()).await;
    // A job's step runner ends on the signal; so does this.
    let script = "trap 'exit 0' TERM; echo started; while :; do sleep 1; done";
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(300),
        Some(TASK_IMAGE.into()),
    );
    ctx.identity = Some(TaskIdentity {
        task_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    let stop = ctx.stop.clone();
    let pods: Api<Pod> = Api::namespaced(c.client.clone(), &c.namespace);
    let selector = format!("{LABEL_TASK_ID}={}", ctx.identity.as_ref().unwrap().task_id);
    let phases = || async {
        pods.list(&ListParams::default().labels(&selector))
            .await
            .unwrap()
            .items
            .into_iter()
            .map(|p| p.status.and_then(|s| s.phase).unwrap_or_default())
            .collect::<Vec<String>>()
    };

    let running = tokio::spawn(async move { exec.execute(&ctx).await });
    let mut up = false;
    for _ in 0..180 {
        if phases().await.iter().any(|phase| phase == "Running") {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(up, "the task's pod never ran");
    // Up, and into its script: a signal that lands before the shell has set
    // its trap would be the default one.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let asked = std::time::Instant::now();
    stop.request();
    let ended = tokio::time::timeout(Duration::from_secs(120), running)
        .await
        .expect("a stopped execution ends")
        .unwrap();
    let err = ended.expect_err("a lost claim is no result");
    assert!(
        err.is::<dagron_executor::executor::ClaimLost>(),
        "it says the claim was lost: {err}"
    );
    assert!(
        !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "and that what it made was seen gone: {err}"
    );
    assert_eq!(
        phases().await,
        Vec::<String>::new(),
        "the pod is gone by the time the executor says so ({:?} after the stop)",
        asked.elapsed()
    );
}

/// A step of `ctx`'s task in an image of its own, asked for as soon as its
/// pod has lent it what it shares: what came of it.
async fn step_of(
    exec: &KubeExecutor,
    ctx: &ExecContext,
    script: &str,
    secs: u64,
) -> Result<dagron_executor::executor::StepOutcome, String> {
    let task = &ctx.identity.as_ref().unwrap().task_id;
    let step = dagron_executor::executor::StepContainer {
        image: TASK_IMAGE.into(),
        entrypoint: Some("/bin/sh".into()),
        args: vec!["-c".into(), script.into()],
        env: vec![("SAID".into(), "from the step's variables".into())],
        timeout: Duration::from_secs(secs),
    };
    for _ in 0..300 {
        match exec.run_step_container(task, step.clone()).await {
            Ok(outcome) => return Ok(outcome),
            Err(e) if e.to_string().contains("not running on this engine") => {
                tokio::time::sleep(Duration::from_millis(200)).await
            }
            Err(e) => return Err(format!("the step's container: {e:#}")),
        }
    }
    Err("the task's pod never lent its steps anything".into())
}

/// A step of a running task's, in an image of its own (a workflow's Docker
/// action): an ephemeral container of the task's own pod, with its
/// workspace at /workspace and at /github/workspace, where it starts. The
/// task sees what it wrote. One past its time is said so; it cannot be
/// stopped alone, and the task ends all the same, its pod with it. A run
/// that is not trusted runs none.
#[tokio::test]
async fn a_step_runs_in_an_ephemeral_container_of_its_task_s_pod() {
    let Some(c) = cluster().await else { return };
    let Some(image) = exec_image() else { return };
    let exec = std::sync::Arc::new(
        executor(
            &c,
            KubeWorkspaces {
                ci_exec_image: Some(image),
                ..KubeWorkspaces::default()
            },
        )
        .await,
    );
    let script = "echo from-the-task > made; \
                  for i in $(seq 1 600); do [ -f done ] && break; sleep 0.2; done; cat done; sleep 3";
    let ctx = task(&["sh", "-c", script], None);
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        let mut own = task(&["sh", "-c", script], None);
        own.identity = ctx.identity.clone();
        tokio::spawn(async move { exec.execute(&own).await })
    };
    let slow = match step_of(&exec, &ctx, "sleep 20", 3).await {
        Ok(slow) => slow,
        Err(why) => panic!("{why}; and the task: {:?}", running.await),
    };
    assert!(slow.timed_out, "{slow:?}");
    let ran = step_of(
        &exec,
        &ctx,
        "pwd; cat /workspace/made; echo \"$SAID\"; echo from-the-step > /github/workspace/done; exit 3",
        120,
    )
    .await;
    let ran = match ran {
        Ok(ran) => ran,
        Err(why) => panic!("{why}; and the task: {:?}", running.await),
    };
    assert_eq!(
        (ran.exit_code, ran.timed_out, ran.output.as_str()),
        (
            3,
            false,
            "/github/workspace\nfrom-the-task\nfrom the step's variables\n"
        )
    );
    let out = running.await.unwrap().expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "from-the-step\n");
    assert_eq!(pods_of(&c, &ctx).await, Vec::<String>::new());

    // A run that is not trusted: refused, and said why.
    let mut held = task(&["sh", "-c", "sleep 20"], None);
    held.confinement = Some(dagron_executor::executor::Confinement {
        memory_bytes: 256 << 20,
        pids: 128,
        nano_cpus: 1_000_000_000,
    });
    let task_id = held.identity.as_ref().unwrap().task_id.clone();
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        let mut own = task(&["sh", "-c", "sleep 20"], None);
        own.identity = held.identity.clone();
        own.confinement = held.confinement;
        tokio::spawn(async move { exec.execute(&own).await })
    };
    let step = dagron_executor::executor::StepContainer {
        image: TASK_IMAGE.into(),
        entrypoint: None,
        args: Vec::new(),
        env: Vec::new(),
        timeout: Duration::from_secs(10),
    };
    let mut refused = String::new();
    for _ in 0..300 {
        match exec.run_step_container(&task_id, step.clone()).await {
            Err(e) if e.to_string().contains("not running on this engine") => {
                tokio::time::sleep(Duration::from_millis(200)).await
            }
            Err(e) => {
                refused = e.to_string();
                break;
            }
            Ok(o) => panic!("a run that is not trusted ran a step's container: {o:?}"),
        }
    }
    assert!(refused.contains("a run that is not trusted"), "{refused}");
    let _ = running.await;
}
