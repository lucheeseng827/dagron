//! A task's Docker daemon on a real Docker daemon: the task builds an image
//! with it, as a job's `docker build` does, and nothing of it is left once
//! the attempt has ended.
//!
//! The daemon is [`DAEMON`], Docker in Docker, and the task's image is
//! [`CLI`], which has the Docker command line; both are pulled first. The
//! image the task builds is `FROM scratch`, so the daemon pulls nothing.
//!
//! Needs a daemon that runs a privileged container, and the CLI, and skips,
//! saying so, where either is missing. With `DAGRON_TEST_DAEMON_RUNTIME`
//! set (`sysbox-runc`, on a host that has Sysbox), each daemon is run on
//! that runtime instead, unprivileged, and the stopped task's is seen to
//! be.

use std::process::{Command, Stdio};

use bollard::image::CreateImageOptions;
use dagron_core::dag::EnvVar;
use dagron_core::docker_daemon;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity};
use futures_util::TryStreamExt;

const DAEMON: &str = docker_daemon::DEFAULT_IMAGE;
const CLI: &str = "docker.io/library/docker:27-cli";

/// A stand-in for dagron-ci-exec's image: its init container makes the
/// task's workspace, which the task and its daemon are given.
const INSTALLER: &str = "dagron-test-install-exec:2";
const INSTALLER_DOCKERFILE: &str = r##"FROM alpine:3.20
RUN printf '%s\n' '#!/bin/sh' \
      '[ "$1" = install ] || { echo "not install: $*" >&2; exit 2; }' \
      'printf "#!/bin/sh\necho stand-in \"\$@\"\n" > "$2/dagron-ci-exec"' \
      'chmod 755 "$2/dagron-ci-exec"' > /entry \
 && chmod 755 /entry \
 && mkdir -m 0777 /workspace
ENTRYPOINT ["/entry"]
"##;

/// One test at a time: each counts the daemon's volumes.
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    // The CLI, and the daemon at the end of it: one that cannot reach it
    // lists nothing, and every check that nothing is left would pass.
    if !Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
    {
        eprintln!("no docker CLI to look at the daemon with - skipping");
        return None;
    }
    for image in [DAEMON, CLI] {
        if docker.inspect_image(image).await.is_err() {
            docker
                .create_image(
                    Some(CreateImageOptions {
                        from_image: image,
                        ..Default::default()
                    }),
                    None,
                    None,
                )
                .try_collect::<Vec<_>>()
                .await
                .unwrap_or_else(|e| panic!("pull {image}: {e}"));
        }
    }
    let mut build = Command::new("docker")
        .args(["build", "-q", "-t", INSTALLER, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("docker build");
    use std::io::Write;
    build
        .stdin
        .take()
        .unwrap()
        .write_all(INSTALLER_DOCKERFILE.as_bytes())
        .unwrap();
    assert!(build.wait().unwrap().success(), "building {INSTALLER}");
    Some(docker)
}

/// The runtime the tests run a daemon on (`DAGRON_TEST_DAEMON_RUNTIME`):
/// the host daemon's default, privileged, where unset.
fn runtime() -> Option<String> {
    std::env::var("DAGRON_TEST_DAEMON_RUNTIME")
        .ok()
        .filter(|r| !r.trim().is_empty())
}

/// An executor that runs a task's daemon on [`runtime`].
async fn executor() -> DockerExecutor {
    DockerExecutor::connect("alpine:3.20")
        .await
        .expect("connect")
        .with_daemon_runtime(runtime())
        .await
        .expect("the daemon's runtime")
}

/// An executor whose tasks' workspaces an init container makes.
async fn with_workspaces() -> DockerExecutor {
    executor()
        .await
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .expect("the installer image")
}

/// The uid map of the daemon of `task`, running now: the host's whole
/// range (`0 0 4294967295`) where it shares the host's user namespace.
fn daemon_uid_map(task: &str) -> String {
    let label = format!("label=dagron.dev/task-id={task}");
    let out = Command::new("docker")
        .args(["ps", "-q", "--filter", &label, "--filter", "name=-svc-"])
        .output()
        .unwrap();
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let out = Command::new("docker")
        .args(["exec", &id, "cat", "/proc/self/uid_map"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker exec: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// How the daemon of `task`, running now, was made: whether privileged,
/// and on which runtime.
fn daemon_made(task: &str) -> String {
    let label = format!("label=dagron.dev/task-id={task}");
    let out = Command::new("docker")
        .args(["ps", "-q", "--filter", &label, "--filter", "name=-svc-"])
        .output()
        .unwrap();
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(!id.is_empty(), "the task's daemon is not running");
    let out = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.HostConfig.Privileged}} {{.HostConfig.Runtime}}",
            &id,
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Whether a container of `task`'s other than its init and checkout ones
/// runs: the task's own.
fn task_is_running(task: &str) -> bool {
    let label = format!("label=dagron.dev/task-id={task}");
    let out = Command::new("docker")
        .args(["ps", "--format", "{{.Names}}", "--filter", &label])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker ps: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|n| !n.ends_with("-init") && !n.contains("-svc-"))
}

/// The names of the containers labelled with `task`'s id.
fn containers_of(task: &str) -> Vec<String> {
    let label = format!("label=dagron.dev/task-id={task}");
    let out = Command::new("docker")
        .args(["ps", "-a", "--format", "{{.Names}}", "--filter", &label])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker ps: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

/// The daemon's volumes, by name.
fn volumes() -> std::collections::BTreeSet<String> {
    let out = Command::new("docker")
        .args(["volume", "ls", "-q"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker volume ls: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

/// A task of the CLI's image that runs `script` beside its daemon, which
/// it reaches as the engine gives it (`dagron-engine` adds both).
fn task(script: &str, id: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(300),
        Some(CLI.into()),
    );
    ctx.services = vec![docker_daemon::service(DAEMON)];
    ctx.env.push(EnvVar {
        name: docker_daemon::ENV_HOST.into(),
        value: docker_daemon::HOST.into(),
        value_from: None,
    });
    ctx.identity = Some(TaskIdentity {
        task_id: id.into(),
        run_id: "r-docker-daemon".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

#[tokio::test]
async fn a_task_builds_an_image_with_a_daemon_of_its_own_and_none_of_it_is_left() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        // Where a runtime is named, the run is for it: a skip would pass.
        assert!(
            runtime().is_none(),
            "DAGRON_TEST_DAEMON_RUNTIME is set, and no daemon answers"
        );
        return;
    }
    let exec = executor().await;
    let id = format!("t-dind-{}", std::process::id());
    let before = volumes();
    let script = r#"set -e
mkdir -p /tmp/context && cd /tmp/context
echo built-here > f
printf 'FROM scratch\nCOPY f /f\n' > Dockerfile
docker build -q -t dagron-test-built:1 . >/dev/null
docker image inspect --format 'ID {{.Id}}' dagron-test-built:1
echo "host $DOCKER_HOST"
"#;
    let out = exec
        .execute(&task(script, &id))
        .await
        .expect("the attempt runs");
    assert!(out.success, "{}", out.output);
    assert!(out.output.contains("ID sha256:"), "{}", out.output);
    assert!(
        out.output
            .contains(&format!("host {}", docker_daemon::HOST)),
        "{}",
        out.output
    );
    assert!(containers_of(&id).is_empty(), "{:?}", containers_of(&id));
    // Nor the daemon's data root, with what it built.
    let left: Vec<_> = volumes().difference(&before).cloned().collect();
    assert!(left.is_empty(), "volumes left: {left:?}");
    // What it built was the task's daemon's alone.
    let here = Command::new("docker")
        .args(["image", "inspect", "dagron-test-built:1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(
        !here.success(),
        "the image was built on the engine's daemon"
    );
}

/// A container the task runs with its daemon, given the task's directory
/// (`-v "$PWD:/src"`), finds the task's files there: the daemon has the
/// workspace at the task's own path. The image is made of the task's own
/// busybox, so the daemon pulls nothing.
#[tokio::test]
async fn a_container_the_task_runs_finds_its_workspace_at_the_task_s_path() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        // Where a runtime is named, the run is for it: a skip would pass.
        assert!(
            runtime().is_none(),
            "DAGRON_TEST_DAEMON_RUNTIME is set, and no daemon answers"
        );
        return;
    }
    let exec = with_workspaces().await;
    let id = format!("t-dind-ws-{}", std::process::id());
    let before = volumes();
    let script = r#"set -e
cd /workspace
echo the-task-s-file > f
mkdir -p /tmp/bb && cd /tmp/bb
cp /bin/busybox . && cp /lib/ld-musl-*.so.1 .
printf 'FROM scratch\nCOPY busybox /bin/busybox\nCOPY ld-musl-* /lib/\nENTRYPOINT ["/bin/busybox"]\n' > Dockerfile
docker build -q -t dagron-test-bb:1 . >/dev/null
cd /workspace
docker run --rm -v "$PWD:/src" dagron-test-bb:1 cat /src/f
"#;
    let mut ctx = task(script, &id);
    ctx.workspace = Some(Default::default());
    let out = exec.execute(&ctx).await.expect("the attempt runs");
    assert!(out.success, "{}", out.output);
    assert!(out.output.contains("the-task-s-file"), "{}", out.output);
    assert!(containers_of(&id).is_empty(), "{:?}", containers_of(&id));
    let left: Vec<_> = volumes().difference(&before).cloned().collect();
    assert!(left.is_empty(), "volumes left: {left:?}");
}

/// A task with a daemon and a workspace whose claim is taken away while it
/// runs leaves nothing: neither its containers nor the workspace, which
/// the daemon has mounted until it is removed.
#[tokio::test]
async fn a_stopped_task_leaves_neither_its_daemon_nor_its_workspace() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        // Where a runtime is named, the run is for it: a skip would pass.
        assert!(
            runtime().is_none(),
            "DAGRON_TEST_DAEMON_RUNTIME is set, and no daemon answers"
        );
        return;
    }
    let exec = with_workspaces().await;
    let id = format!("t-dind-stop-{}", std::process::id());
    let before = volumes();
    let mut ctx = task("echo kept > /workspace/f; exec sleep 300", &id);
    ctx.workspace = Some(Default::default());
    let stop = ctx.stop.clone();
    let running = tokio::spawn(async move { exec.execute(&ctx).await });
    for _ in 0..600 {
        if task_is_running(&id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(task_is_running(&id), "the task's container never ran");
    let made = daemon_made(&id);
    match runtime() {
        Some(runtime) => {
            assert_eq!(made, format!("false {runtime}"), "unprivileged, on it");
            // And its root is no user of the host's: its user namespace
            // maps uid 0 to some other uid, and not the host's whole range.
            let map = daemon_uid_map(&id);
            assert!(
                !map.split_whitespace().eq(["0", "0", "4294967295"]),
                "the daemon's user namespace is the host's: {map}"
            );
        }
        None => assert!(made.starts_with("true "), "privileged: {made}"),
    }
    stop.request();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(120), running)
        .await
        .expect("a stopped execution ends")
        .unwrap();
    assert!(ended.is_err(), "a lost claim is no result");
    assert!(containers_of(&id).is_empty(), "{:?}", containers_of(&id));
    let left: Vec<_> = volumes().difference(&before).cloned().collect();
    assert!(left.is_empty(), "volumes left: {left:?}");
}

/// A runtime the daemon does not have stops the executor as it is made,
/// naming the variable: every daemon would fail to be made on it.
#[tokio::test]
async fn a_runtime_the_daemon_does_not_have_is_refused_at_start() {
    if daemon().await.is_none() {
        assert!(
            runtime().is_none(),
            "DAGRON_TEST_DAEMON_RUNTIME is set, and no daemon answers"
        );
        return;
    }
    let err = DockerExecutor::connect("alpine:3.20")
        .await
        .expect("connect")
        .with_daemon_runtime(Some("dagron-test-no-such-runtime".into()))
        .await
        .err()
        .expect("refused")
        .to_string();
    assert!(err.contains(docker_daemon::ENV_RUNTIME), "{err}");
    assert!(err.contains("no such runtime"), "{err}");
}

/// A task with a service beside its daemon builds an image with the daemon:
/// the daemon is its first service, as the engine makes it, and the other
/// joins the daemon's network. On Sysbox, a daemon in an ordinary
/// container's network could not make its own.
#[tokio::test]
async fn a_daemon_beside_another_service_starts_and_builds() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        assert!(
            runtime().is_none(),
            "DAGRON_TEST_DAEMON_RUNTIME is set, and no daemon answers"
        );
        return;
    }
    let exec = executor().await;
    let id = format!("t-dind-side-{}", std::process::id());
    let before = volumes();
    let script = r#"set -e
mkdir -p /tmp/context && cd /tmp/context
echo built-here > f
printf 'FROM scratch\nCOPY f /f\n' > Dockerfile
docker build -q -t dagron-test-side:1 . >/dev/null
docker image inspect --format 'ID {{.Id}}' dagron-test-side:1
"#;
    let mut ctx = task(script, &id);
    let side: dagron_core::service::ServiceSpec = serde_json::from_value(serde_json::json!({
        "name": "side",
        "image": "alpine:3.20",
        "args": ["sleep", "300"]
    }))
    .unwrap();
    ctx.services.push(side);
    let out = exec.execute(&ctx).await.expect("the attempt runs");
    assert!(out.success, "{}", out.output);
    assert!(out.output.contains("ID sha256:"), "{}", out.output);
    assert!(containers_of(&id).is_empty(), "{:?}", containers_of(&id));
    let left: Vec<_> = volumes().difference(&before).cloned().collect();
    assert!(left.is_empty(), "volumes left: {left:?}");

    // One whose daemon is not first is refused before anything is made.
    ctx.services.retain(|s| !s.daemon);
    ctx.services.push(docker_daemon::service(DAEMON));
    let err = exec.execute(&ctx).await.unwrap_err().to_string();
    assert!(err.contains("not its first service"), "{err}");
    assert!(containers_of(&id).is_empty(), "{:?}", containers_of(&id));
}
