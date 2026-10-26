//! A privileged task on a real Docker daemon: on an executor whose
//! operator allows one, the task's container has every capability and can do
//! what only root on the host can (mount a filesystem); one that does not ask
//! cannot; and an executor not told it may run one refuses the task before
//! anything is made for it.
//!
//! Needs a daemon and the CLI, and skips, saying so, where either is missing.

use std::process::{Command, Stdio};

use bollard::image::CreateImageOptions;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity};
use futures_util::TryStreamExt;

const BASE: &str = "alpine:3.20";

async fn daemon() -> Option<bollard::Docker> {
    let docker = match bollard::Docker::connect_with_local_defaults() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("no Docker client here ({e}) - skipping");
            return None;
        }
    };
    if let Err(e) = docker.ping().await {
        eprintln!("no Docker daemon answers ({e}) - skipping");
        return None;
    }
    if docker.inspect_image(BASE).await.is_err() {
        docker
            .create_image(
                Some(CreateImageOptions {
                    from_image: BASE,
                    ..Default::default()
                }),
                None,
                None,
            )
            .try_collect::<Vec<_>>()
            .await
            .unwrap_or_else(|e| panic!("pull {BASE}: {e}"));
    }
    Some(docker)
}

fn task(script: &str, privileged: bool) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(60),
        Some(BASE.to_string()),
    );
    ctx.identity = Some(TaskIdentity {
        task_id: format!("t-{}", uuid::Uuid::new_v4().simple()),
        run_id: "r-privileged".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx.privileged = privileged;
    ctx
}

/// Its effective capabilities, and whether it could mount a filesystem.
const PROBE: &str = "grep CapEff /proc/self/status | awk '{print $2}'; \
    mkdir -p /m && mount -t tmpfs none /m 2>/dev/null && echo mounted || echo refused";

/// CAP_SYS_ADMIN, which a container that is not privileged is not given.
const SYS_ADMIN: u32 = 21;

fn read(output: &str) -> (u64, String) {
    let mut lines = output.lines();
    let caps = u64::from_str_radix(lines.next().unwrap().trim(), 16).unwrap();
    (caps, lines.next().unwrap_or_default().to_string())
}

#[tokio::test]
async fn a_privileged_task_has_every_capability_and_one_that_did_not_ask_has_not() {
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE).await.unwrap().with_privileged(true);

    let out = exec.execute(&task(PROBE, true)).await.unwrap();
    assert!(out.success, "{}", out.output);
    let (caps, mount) = read(&out.output);
    assert!(caps >> SYS_ADMIN & 1 == 1, "CapEff {caps:x}");
    assert_eq!(mount, "mounted");

    let out = exec.execute(&task(PROBE, false)).await.unwrap();
    let (plain, mount) = read(&out.output);
    assert!(plain >> SYS_ADMIN & 1 == 0, "CapEff {plain:x}");
    assert_eq!(mount, "refused");
    assert!(caps > plain);
}

#[tokio::test]
async fn an_executor_not_told_it_may_refuses_before_anything_is_made() {
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let ctx = task("true", true);
    let why = exec.execute(&ctx).await.expect_err("refused").to_string();
    assert!(why.contains("DAGRON_TASK_PRIVILEGED=allow"), "{why}");
    let id = &ctx.identity.as_ref().unwrap().task_id;
    let made = Command::new("docker")
        .args(["ps", "-aq", "--filter", &format!("label=dagron.dev/task-id={id}")])
        .stderr(Stdio::null())
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    assert_eq!(made, "", "nothing was made for it");
}
