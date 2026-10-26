//! A task's network and its trust envelope on a real Docker daemon: where the
//! operator names a network, the task's container and the one that installs
//! its tools are on it and on no other; a task that asked for no network has
//! a loopback interface and nothing else, while what is run for it first
//! keeps the operator's; and what a task's `isolation:` asks of a container
//! is what its process finds itself under, read from `/proc` by the task.
//!
//! The init container's image is a stand-in, as in `docker_workspace.rs`:
//! its entrypoint does what `dagron-ci-exec install DIR` does, and leaves
//! beside it the address it was installed from, for the task to say.
//!
//! Needs a daemon and the CLI, and skips, saying so, where either is missing.

use std::net::Ipv4Addr;
use std::process::{Command, Stdio};

use bollard::image::CreateImageOptions;
use dagron_core::isolation::{IsolationSpec, Network, Seccomp};
use dagron_core::workspace::WorkspaceSpec;
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity};
use futures_util::TryStreamExt;

const BASE: &str = "alpine:3.20";
/// An installer that also says where it ran: `DIR/installed-on` holds its
/// container's address.
const INSTALLER: &str = "dagron-test-install-exec-says-where:1";
/// A task image that runs as a user other than root.
const NON_ROOT: &str = "dagron-test-non-root:1";

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const INSTALLER_DOCKERFILE: &str = r##"FROM alpine:3.20
RUN printf '%s\n' '#!/bin/sh' \
      '[ "$1" = install ] || { echo "not install: $*" >&2; exit 2; }' \
      'printf "#!/bin/sh\necho stand-in\n" > "$2/dagron-ci-exec"' \
      'chmod 755 "$2/dagron-ci-exec"' \
      'hostname -i > "$2/installed-on"' > /entry \
 && chmod 755 /entry \
 && mkdir -m 0777 /workspace
ENTRYPOINT ["/entry"]
"##;

const NON_ROOT_DOCKERFILE: &str = r#"FROM alpine:3.20
USER 1000:1000
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
        eprintln!("no docker CLI to build the test images with - skipping");
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
    for (tag, dockerfile) in [
        (INSTALLER, INSTALLER_DOCKERFILE),
        (NON_ROOT, NON_ROOT_DOCKERFILE),
    ] {
        build(tag, dockerfile);
    }
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

/// What `docker ARGS` printed, trimmed; a failure is the test's.
fn docker(args: &[&str]) -> String {
    let out = Command::new("docker").args(args).output().expect("docker");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A network of this test's own, removed with it.
struct TestNetwork {
    name: String,
    /// Its subnet, as the daemon gave it: an address and a prefix length.
    subnet: (Ipv4Addr, u32),
}

impl TestNetwork {
    fn make() -> Self {
        let name = format!(
            "dagron-test-net-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        docker(&["network", "create", &name]);
        let subnet = docker(&[
            "network",
            "inspect",
            &name,
            "--format",
            "{{(index .IPAM.Config 0).Subnet}}",
        ]);
        let (address, prefix) = subnet.split_once('/').expect("a subnet is ADDRESS/PREFIX");
        Self {
            name,
            subnet: (address.parse().unwrap(), prefix.parse().unwrap()),
        }
    }

    /// Whether `address` is one of this network's.
    fn holds(&self, address: &str) -> bool {
        let Ok(address) = address.trim().parse::<Ipv4Addr>() else {
            return false;
        };
        let (net, prefix) = self.subnet;
        let mask = u32::MAX.checked_shl(32 - prefix).unwrap_or(0);
        u32::from(address) & mask == u32::from(net) & mask
    }
}

impl Drop for TestNetwork {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["network", "rm", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// The containers labelled with `task`'s id: what this test's task left.
fn containers_of(task: &str) -> String {
    docker(&[
        "ps",
        "-aq",
        "--filter",
        &format!("label=dagron.dev/task-id={task}"),
    ])
}

fn task(script: &str, id: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(120),
        None,
    );
    ctx.identity = Some(TaskIdentity {
        task_id: id.into(),
        run_id: "r-envelope".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

fn workspace_task(script: &str, id: &str) -> ExecContext {
    let mut ctx = task(script, id);
    ctx.workspace = Some(WorkspaceSpec::default());
    ctx
}

fn a_task_id() -> String {
    format!("t-{}", uuid::Uuid::new_v4().simple())
}

/// The interfaces a container has, as its own `/sys` lists them.
const INTERFACES: &str = "ls /sys/class/net | sort | tr '\\n' ' '; echo";
/// What the kernel holds the task's process to, from its own `/proc`.
const UNDER: &str =
    "grep -E '^(CapEff|NoNewPrivs|Seccomp):' /proc/self/status | tr -d '\\t' | tr '\\n' ' '; echo";

/// Where the operator names a network, the task's container and the one
/// that installs its tools are on it, and the task's has no other interface.
/// A container used to be made with no network named, and so on whichever
/// the daemon gives one by default, beside every other container on the host.
#[tokio::test]
async fn a_task_and_its_init_are_on_the_operator_s_network_and_no_other() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let net = TestNetwork::make();
    let id = a_task_id();
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network(Some(net.name.clone()))
        .await
        .expect("the daemon has the network")
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();

    let script = format!("cat /dagron/bin/installed-on; hostname -i; {INTERFACES}");
    let out = exec.execute(&workspace_task(&script, &id)).await.unwrap();
    assert!(out.success, "{}", out.output);
    let lines: Vec<&str> = out.output.lines().collect();
    let [installed_on, ran_on, interfaces] = lines[..] else {
        panic!("three lines: {}", out.output);
    };
    assert!(
        net.holds(installed_on),
        "the init ran on {installed_on}, not on {:?}",
        net.subnet
    );
    assert!(
        net.holds(ran_on),
        "the task ran on {ran_on}, not on {:?}",
        net.subnet
    );
    assert_eq!(interfaces.trim(), "eth0 lo", "one network and loopback");
    assert_eq!(containers_of(&id), "");
}

/// A network the daemon does not have stops the engine as it starts, since
/// every task's container would fail where it is made.
#[tokio::test]
async fn a_network_the_daemon_lacks_is_a_startup_error() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let err = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network(Some("dagron-test-no-such-network".into()))
        .await
        .err()
        .expect("there is no such network")
        .to_string();
    assert!(
        err.contains("DAGRON_DOCKER_NETWORK=dagron-test-no-such-network")
            && err.contains("no such network")
            && err.contains("docker network create"),
        "{err}"
    );
    // `none` and `bridge` are every daemon's, and are not asked after.
    for built_in in ["none", "bridge"] {
        DockerExecutor::connect(BASE)
            .await
            .unwrap()
            .with_network(Some(built_in.into()))
            .await
            .unwrap_or_else(|e| panic!("{built_in}: {e}"));
    }
}

/// A task that asked for no network has a loopback interface and nothing
/// else, whether or not the operator named a network. The container that
/// installs its tools is the operator's, and stays on the operator's.
#[tokio::test]
async fn a_task_that_asked_for_no_network_has_loopback_alone() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let no_network = Some(IsolationSpec {
        network: Some(Network::None),
        ..Default::default()
    });

    // On the daemon's default network.
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let id = a_task_id();
    let mut ctx = task(INTERFACES, &id);
    let with_one = exec.execute(&ctx).await.unwrap();
    assert_eq!(
        with_one.output.trim(),
        "eth0 lo",
        "a task that asked for nothing has one"
    );
    ctx.isolation = no_network.clone();
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output.trim(), "lo");

    // And on the operator's: the init is on it, and the task on none.
    let net = TestNetwork::make();
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network(Some(net.name.clone()))
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();
    let id = a_task_id();
    let mut ctx = workspace_task(&format!("cat /dagron/bin/installed-on; {INTERFACES}"), &id);
    ctx.isolation = no_network;
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.success, "{}", out.output);
    let lines: Vec<&str> = out.output.lines().collect();
    let [installed_on, interfaces] = lines[..] else {
        panic!("two lines: {}", out.output);
    };
    assert!(net.holds(installed_on), "the init ran on {installed_on}");
    assert_eq!(interfaces.trim(), "lo");
    assert_eq!(containers_of(&id), "");
}

/// What a task's envelope asks for is what its process is under: no
/// capability, no gain of privilege, the runtime's seccomp filter, a root
/// filesystem it cannot write, and the uid it named. Its workspace is still
/// its own to write, and its tools were installed under the same envelope.
#[tokio::test]
async fn a_task_runs_under_the_envelope_it_declared() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();

    // Nothing declared: root, every default capability, and it writes its root.
    let id = a_task_id();
    let plain = format!("id -u; touch /x && echo wrote its root; {UNDER}");
    let out = exec.execute(&workspace_task(&plain, &id)).await.unwrap();
    assert!(out.success, "{}", out.output);
    let lines: Vec<&str> = out.output.lines().collect();
    assert_eq!(lines[..2], ["0", "wrote its root"], "{}", out.output);
    assert!(lines[2].contains("NoNewPrivs:0"), "{}", out.output);
    assert!(
        !lines[2].contains("CapEff:0000000000000000"),
        "{}",
        out.output
    );

    // The preset, and a uid.
    let id = a_task_id();
    let script = format!(
        "id -u; touch /x 2>&1 | grep -o 'Read-only file system'; \
         touch /workspace/y && echo wrote its workspace; test -x /dagron/bin/dagron-ci-exec && echo has its tools; {UNDER}"
    );
    let mut ctx = workspace_task(&script, &id);
    ctx.isolation = Some(IsolationSpec {
        run_as_user: Some(1000),
        ..IsolationSpec::hardened()
    });
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        "1000\nRead-only file system\nwrote its workspace\nhas its tools\n\
         CapEff:0000000000000000 NoNewPrivs:1 Seccomp:2 \n"
    );
    assert_eq!(containers_of(&id), "");

    // As root, with one field each: root with no capability, and a root
    // filesystem root cannot write.
    let id = a_task_id();
    let mut ctx = task(&format!("id -u; {UNDER}"), &id);
    ctx.isolation = Some(IsolationSpec {
        drop_all_capabilities: Some(true),
        ..Default::default()
    });
    let out = exec.execute(&ctx).await.unwrap();
    assert!(
        out.output
            .starts_with("0\nCapEff:0000000000000000 NoNewPrivs:1 "),
        "{}",
        out.output
    );

    let mut ctx = task("touch /x 2>&1 | grep -o 'Read-only file system'", &id);
    ctx.isolation = Some(IsolationSpec {
        read_only_root_fs: Some(true),
        ..Default::default()
    });
    let out = exec.execute(&ctx).await.unwrap();
    assert_eq!(out.output.trim(), "Read-only file system");

    // And the one opt-out a container is told of: no seccomp filter.
    let mut ctx = task(UNDER, &id);
    ctx.isolation = Some(IsolationSpec {
        seccomp: Some(Seccomp::Unconfined),
        ..Default::default()
    });
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.output.contains("Seccomp:0"), "{}", out.output);
}

/// A task that asked not to run as root is refused where nothing says it
/// will not: it named no uid, and its image's user is root. Nothing is made
/// for it. An image whose user is a number other than 0 runs.
#[tokio::test]
async fn a_task_that_must_not_be_root_is_refused_an_image_that_is() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let non_root = Some(IsolationSpec {
        run_as_non_root: Some(true),
        ..Default::default()
    });

    let id = a_task_id();
    let mut ctx = task("id -u", &id);
    ctx.isolation = non_root.clone();
    let err = exec
        .execute(&ctx)
        .await
        .err()
        .expect("alpine runs as root")
        .to_string();
    assert!(
        err.contains("isolation.run_as_non_root")
            && err.contains(BASE)
            && err.contains("states no user, which is root")
            && err.contains("isolation.run_as_user"),
        "{err}"
    );
    assert_eq!(containers_of(&id), "", "nothing was made for it");

    // An image that says it is not root, in a number.
    ctx.docker_image = Some(NON_ROOT.into());
    let out = exec.execute(&ctx).await.unwrap();
    assert_eq!(out.output.trim(), "1000");

    // And a uid of the task's own, whatever the image says.
    ctx.docker_image = None;
    ctx.isolation = Some(IsolationSpec {
        run_as_user: Some(1234),
        ..non_root.unwrap()
    });
    let out = exec.execute(&ctx).await.unwrap();
    assert_eq!(out.output.trim(), "1234");
}
