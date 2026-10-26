//! A task's services on a real Docker daemon: each is started before the
//! task and seen ready, the task finds each at its name and at the loopback
//! address, and nothing of them is left, whether the task passed, a service
//! was never ready, the attempt's claim was lost, or an earlier attempt died
//! with its services running.
//!
//! A service here is a small HTTP answer from busybox, [`SERVICE`], which
//! listens only after a pause: a task that asks it once, and is answered,
//! started after the service was ready. The image is alpine:3.20, which is
//! pulled first.
//!
//! Needs a daemon and the CLI, and skips, saying so, where either is missing.
//! The tests share one lock: some count a task's containers.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use bollard::image::CreateImageOptions;
use dagron_core::service::ServiceSpec;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity};
use futures_util::TryStreamExt;

const BASE: &str = "alpine:3.20";
/// An image no registry has.
const NOWHERE: &str = "dagron-test-no-such-image:never";
/// A task image that runs as a user other than root, built here.
const NON_ROOT: &str = "dagron-test-non-root:1";
const NON_ROOT_DOCKERFILE: &str = r#"FROM alpine:3.20
USER 1000:1000
"#;

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    if Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("no docker CLI to look at the daemon with - skipping");
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
    build(NON_ROOT, NON_ROOT_DOCKERFILE);
    Some(docker)
}

fn build(tag: &str, dockerfile: &str) {
    use std::io::Write;
    let mut child = Command::new("docker")
        .args(["build", "-q", "-t", tag, "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("docker build");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(dockerfile.as_bytes())
        .unwrap();
    assert!(child.wait().unwrap().success(), "building {tag}");
}

/// The names of the containers labelled with `task`'s id, the running ones
/// or all of them.
fn containers_of(task: &str, running_only: bool) -> Vec<String> {
    let mut args = vec!["ps", "--format", "{{.Names}}"];
    if !running_only {
        args.push("-a");
    }
    let label = format!("label=dagron.dev/task-id={task}");
    args.extend(["--filter", &label]);
    let out = Command::new("docker").args(&args).output().unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

/// A task that runs `script`, of the given fence.
fn task(script: &str, id: &str, fence: i64) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(120),
        Some(BASE.into()),
    );
    ctx.identity = Some(TaskIdentity {
        task_id: id.into(),
        run_id: "r-services".into(),
        attempt: 1,
        fence,
        installation: None,
    });
    ctx
}

/// A service that answers on `port` once `pause` seconds have passed, and
/// says it is ready once it does, by `ready`.
fn answering(name: &str, port: u16, pause: u32, ready: serde_json::Value) -> ServiceSpec {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "image": BASE,
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

/// A task's services are up before it starts: each answers the one request
/// the task makes of it, at its name and at the loopback address, though
/// none listens until seconds after it was started. They are started in the
/// order they are written, and none of their containers is left.
#[tokio::test]
async fn a_task_s_services_are_ready_before_it_starts_and_found_at_their_names() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let mut ctx = task(
        "wget -qO- http://web:9000/ && wget -qO- http://api:9001/ && \
         wget -qO- http://localhost:9000/ && wget -qO- http://127.0.0.1:9001/",
        &id,
        1,
    );
    ctx.services = vec![
        answering("web", 9000, 2, serde_json::json!("test -f /tmp/up")),
        answering("api", 9001, 1, serde_json::json!(["test", "-f", "/tmp/up"])),
    ];
    let started = Instant::now();
    let out = exec.execute(&ctx).await.unwrap();
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
        started.elapsed() >= Duration::from_secs(3),
        "each was waited for in turn: {:?}",
        started.elapsed()
    );
    assert!(
        containers_of(&id, false).is_empty(),
        "no container of the attempt is left"
    );
}

/// A task that asked for no network has services with none: they and the
/// task are each other's whole network, at the loopback address and at the
/// services' names. (But on Podman, which gives a container made in
/// another's network that container's names only where it has a network:
/// the loopback address alone is asked there.)
#[tokio::test]
async fn a_task_with_no_network_reaches_its_services_and_has_no_other() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else {
        return;
    };
    let podman = docker.version().await.is_ok_and(|version| {
        version
            .components
            .iter()
            .flatten()
            .any(|component| component.name.contains("Podman"))
    });
    let named = if podman { "localhost" } else { "web" };
    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let mut ctx = task(
        &format!(
            "wget -qO- http://localhost:9000/ && wget -qO- http://{named}:9000/ && \
             echo \"interfaces: $(ls /sys/class/net | tr '\\n' ' ')\""
        ),
        &id,
        1,
    );
    ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
        network: Some(dagron_core::isolation::Network::None),
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
        ["hello from web", "hello from web", "interfaces: lo "],
        "{}",
        out.output
    );
    assert!(containers_of(&id, false).is_empty());
}

/// A service that is not ready within the time it has fails the attempt,
/// with what its `ready` last said and the last the service wrote. One that
/// stops before it is ready fails it with what it exited with. Either way
/// the task never starts, and nothing is left.
#[tokio::test]
async fn a_service_that_is_never_ready_fails_the_attempt_in_its_own_words() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE).await.unwrap();

    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = task("echo the task ran", &id, 1);
    ctx.services = vec![one(serde_json::json!({
        "name": "slow",
        "image": BASE,
        "args": ["sh", "-c", "echo warming up >&2; sleep 300"],
        "ready": "echo not yet; exit 7",
        "ready_timeout_secs": 2,
    }))];
    let started = Instant::now();
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("never ready")
        .to_string();
    assert!(
        err.contains("the service 'slow' was not ready within 2 s")
            && err.contains("its `ready` exited 7 (not yet)")
            && err.contains("it said: warming up"),
        "{err}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "it is not waited for past its own time: {:?}",
        started.elapsed()
    );
    assert!(containers_of(&id, false).is_empty(), "nothing is left");

    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = task("echo the task ran", &id, 1);
    ctx.services = vec![
        answering("web", 9000, 0, serde_json::json!("test -f /tmp/up")),
        one(serde_json::json!({
            "name": "broken",
            "image": BASE,
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
    assert!(
        containers_of(&id, false).is_empty(),
        "nor of the service that was ready"
    );
}

/// A service's image is its author's, as the task's is: one that cannot be
/// had fails the attempt before anything is made, and one that would run as
/// root is refused where the task asked not to.
#[tokio::test]
async fn a_service_s_image_is_fetched_and_held_as_the_task_s_is() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE).await.unwrap();

    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = task("true", &id, 1);
    ctx.services = vec![one(serde_json::json!({ "name": "db", "image": NOWHERE }))];
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("no such image")
        .to_string();
    assert!(
        err.contains(&format!("the image of the service 'db' {NOWHERE}"))
            && err.contains("could not be pulled"),
        "{err}"
    );
    assert!(containers_of(&id, false).is_empty(), "nothing was made");

    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = task("true", &id, 1);
    ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
        run_as_non_root: Some(true),
        ..Default::default()
    });
    // The task's own image is asked first, and states a uid: it passes.
    ctx.docker_image = Some(NON_ROOT.into());
    ctx.services = vec![one(serde_json::json!({ "name": "web", "image": BASE }))];
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("a root image")
        .to_string();
    assert!(
        err.contains(&format!(
            "isolation.run_as_non_root: the image of the service 'web' {BASE} states no user, \
             which is root"
        )),
        "{err}"
    );
    assert!(containers_of(&id, false).is_empty(), "nothing was made");
}

/// Whether `task`'s own container is up: the one whose name ends with none
/// of what an attempt's other containers' names end with.
fn task_is_running(task: &str) -> bool {
    containers_of(task, true)
        .iter()
        .any(|name| !name.contains("-svc-") && !name.ends_with("-init"))
}

/// A lost claim takes the services down with the task: the task's container
/// is told to stop, and its services' are removed after it, with nothing
/// left for the daemon to say it could not remove.
#[tokio::test]
async fn a_lost_claim_removes_the_task_s_services_with_it() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let mut ctx = task("trap 'exit 0' TERM; while :; do sleep 1; done", &id, 1);
    ctx.services = vec![
        answering("web", 9000, 0, serde_json::json!("test -f /tmp/up")),
        answering("api", 9001, 0, serde_json::json!("test -f /tmp/up")),
    ];
    let stop = ctx.stop.clone();
    let running = tokio::spawn(async move { exec.execute(&ctx).await });
    for _ in 0..600 {
        if task_is_running(&id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(task_is_running(&id), "the task's container never ran");
    assert_eq!(containers_of(&id, true).len(), 3, "it and its two services");
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.request();
    let err = tokio::time::timeout(Duration::from_secs(90), running)
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
        "and that what it made was seen gone: {err}"
    );
    assert!(containers_of(&id, false).is_empty(), "nothing is left");
}

/// An attempt whose worker died leaves its task's container and its
/// services' running. The task's next dispatch removes all of them before it
/// starts anything, the one the others' network is made in among them, and
/// then runs beside services of its own.
#[tokio::test]
async fn a_dispatch_removes_an_earlier_attempt_s_services_with_its_task() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let id = format!("t-{}", uuid::Uuid::new_v4().simple());
    let services = || {
        vec![
            answering("web", 9000, 0, serde_json::json!("test -f /tmp/up")),
            answering("api", 9001, 0, serde_json::json!("test -f /tmp/up")),
        ]
    };
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let mut first = task("sleep 300", &id, 4);
    first.services = services();
    // A short grace: a `sleep` heeds no signal.
    first.env.push(dagron_core::dag::EnvVar {
        name: dagron_executor::executor::ENV_STEP_GRACE.into(),
        value: "1".into(),
        value_from: None,
    });
    let dead = tokio::spawn(async move { exec.execute(&first).await });
    for _ in 0..600 {
        if task_is_running(&id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        task_is_running(&id),
        "the first attempt's container never ran"
    );
    // Its worker is gone, with nobody told to stop anything.
    dead.abort();
    let _ = dead.await;
    let left = containers_of(&id, true);
    assert_eq!(left.len(), 3, "what it left: {left:?}");

    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let mut next = task("wget -qO- http://web:9000/", &id, 5);
    next.services = services();
    next.env.push(dagron_core::dag::EnvVar {
        name: dagron_executor::executor::ENV_STEP_GRACE.into(),
        value: "1".into(),
        value_from: None,
    });
    let out = exec.execute(&next).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output.trim(), "hello from web");
    assert!(
        containers_of(&id, false).is_empty(),
        "neither attempt's containers are left: {:?}",
        containers_of(&id, false)
    );
}
