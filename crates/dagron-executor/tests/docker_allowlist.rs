//! An allowlist on a real Docker daemon: a task that names one
//! (`isolation.network: allow:<name>`) is on the operator's internal network
//! for it, `dagron-task-allow-<name>`, where it reaches what the operator put
//! there, and nothing of the host's, and has no way out; a network that is
//! missing, not internal, or a bridge the host has an address on, stops the
//! executor as it starts; one removed since is refused before
//! anything is made; and the proxy a network says is told to the task.
//!
//! Needs a daemon and the CLI, and skips, saying so, where either is missing.

use std::process::{Command, Stdio};

use bollard::image::CreateImageOptions;
use dagron_core::isolation::{IsolationSpec, Network};
use dagron_executor::docker_executor::DockerExecutor;
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity};
use futures_util::TryStreamExt;

const BASE: &str = "alpine:3.20";
/// What stands in for what an operator puts on an allowlist: a web server.
const SERVED: &str = "busybox:1.36.1";
/// How an operator makes an allowlist's network: internal, and a bridge the
/// host has no address on.
const ISOLATED: [&str; 3] = ["--internal", "-o", "com.docker.network.bridge.inhibit_ipv4=true"];

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
    if Command::new("docker")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_err()
    {
        eprintln!("no docker CLI to make the test's networks with - skipping");
        return None;
    }
    for image in [BASE, SERVED] {
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
    Some(docker)
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

/// An allowlist of this test's own: its name, and the network an operator
/// makes for it, with a web server on it. Removed with it.
struct TestAllowlist {
    name: String,
    network: String,
    server: Option<String>,
}

impl TestAllowlist {
    /// `flags` are `docker network create`'s, before the name.
    fn make(flags: &[&str]) -> Self {
        let name = format!("t{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let network = dagron_core::isolation::allowlist_object_name(&name);
        let mut args = vec!["network", "create"];
        args.extend(flags);
        args.push(&network);
        docker(&args);
        Self {
            name,
            network,
            server: None,
        }
    }

    /// Serve `says` at `http://<the server's name>:8080/` on the network.
    fn serve(&mut self, says: &str) -> String {
        let server = format!("{}-web", self.network);
        docker(&[
            "run",
            "-d",
            "--name",
            &server,
            "--network",
            &self.network,
            SERVED,
            "sh",
            "-c",
            &format!("mkdir /www && echo {says} > /www/index.html && httpd -f -p 8080 -h /www"),
        ]);
        self.server = Some(server.clone());
        server
    }
}

impl Drop for TestAllowlist {
    fn drop(&mut self) {
        let quiet = |args: &[&str]| {
            let _ = Command::new("docker")
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        };
        if let Some(server) = &self.server {
            quiet(&["rm", "-f", server]);
        }
        quiet(&["network", "rm", &self.network]);
    }
}

fn task(script: &str, network: Option<Network>) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(120),
        None,
    );
    ctx.identity = Some(TaskIdentity {
        task_id: format!("t-{}", uuid::Uuid::new_v4().simple()),
        run_id: "r-allowlist".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx.isolation = Some(IsolationSpec {
        network,
        ..Default::default()
    });
    ctx
}

/// Whether the container has a default route: the way out of its network.
/// Read from its own routing table, which an internal network leaves
/// without one.
const WAY_OUT: &str =
    "awk 'NR > 1 && $2 == \"00000000\" { found = 1 } END { print found ? \"a way out\" : \"no way out\" }' /proc/net/route";

/// A task that names an allowlist is on the operator's network for it: it
/// reaches what is there, by its name, and has no way out of it. A task
/// that names none is on the daemon's default network, which has one.
#[tokio::test]
async fn a_task_held_to_an_allowlist_reaches_what_is_on_it_and_nothing_else() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let mut allowlist = TestAllowlist::make(&ISOLATED);
    let server = allowlist.serve("reached");
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network_allowlists(vec![allowlist.name.clone()])
        .await
        .unwrap();

    let held = task(
        &format!("wget -q -T 10 -O- http://{server}:8080/; {WAY_OUT}"),
        Some(Network::Allow(allowlist.name.clone())),
    );
    // httpd may take a moment to listen: a task that missed it tries again.
    let mut out = exec.execute(&held).await.unwrap();
    for _ in 0..10 {
        if out.output.contains("reached") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        out = exec.execute(&held).await.unwrap();
    }
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output.trim(), "reached\nno way out", "{}", out.output);

    let free = task(WAY_OUT, None);
    let out = exec.execute(&free).await.unwrap();
    assert_eq!(out.output.trim(), "a way out", "the daemon's default network");
}

/// Nothing of the host's is on an allowlist's network: a service the host
/// serves on every address, which a task on the daemon's default network
/// reaches at its gateway, a task held to a list does not reach.
#[tokio::test]
async fn a_task_held_to_an_allowlist_reaches_nothing_the_host_serves() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        use std::io::Write;
        for conn in listener.incoming().flatten() {
            let _ = (&conn).write_all(b"the host\n");
        }
    });
    let gateway = docker(&["network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}"]);
    let allowlist = TestAllowlist::make(&ISOLATED);
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network_allowlists(vec![allowlist.name.clone()])
        .await
        .unwrap();
    // Where a bridge gives the host an address: the first of the task's own
    // subnet (an internal network has no default route to say it), and the
    // default network's gateway.
    let probe = format!(
        "own=$(ip -4 -o addr show eth0 | awk '{{split($4, a, \".\"); print a[1]\".\"a[2]\".\"a[3]\".1\"}}'); \
         for to in $own {gateway}; do nc -w 3 $to {port} 2>/dev/null; done; echo done"
    );
    let free = exec.execute(&task(&probe, None)).await.unwrap();
    assert!(free.output.contains("the host"), "the probe reaches the host: {}", free.output);
    let held = exec
        .execute(&task(&probe, Some(Network::Allow(allowlist.name.clone()))))
        .await
        .unwrap();
    assert_eq!(held.output.trim(), "done", "{}", held.output);
}

/// The network an allowlist names is the operator's to make internal, and
/// is read as the executor starts: missing, or with a way out, it stops it.
#[tokio::test]
async fn an_allowlist_s_network_must_be_there_and_internal() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let refused = |names: Vec<String>| async move {
        DockerExecutor::connect(BASE)
            .await
            .unwrap()
            .with_network_allowlists(names)
            .await
            .err()
            .expect("refused")
            .to_string()
    };
    let missing = refused(vec!["dagron-test-no-such".into()]).await;
    assert!(
        missing.contains("DAGRON_TASK_NETWORK_ALLOWLISTS names 'dagron-test-no-such'")
            && missing.contains(
                "docker network create --internal -o com.docker.network.bridge.inhibit_ipv4=true \
                 dagron-task-allow-dagron-test-no-such"
            ),
        "{missing}"
    );
    let open = TestAllowlist::make(&[]);
    let not_internal = refused(vec![open.name.clone()]).await;
    assert!(
        not_internal.contains("is not internal") && not_internal.contains("--internal"),
        "{not_internal}"
    );
    // Internal, and the host has an address on it: the host's services are
    // a way to what is not on the list.
    let gated = TestAllowlist::make(&["--internal"]);
    let host_reached = refused(vec![gated.name.clone()]).await;
    assert!(
        host_reached.contains("gives the host an address on it")
            && host_reached.contains("inhibit_ipv4=true"),
        "{host_reached}"
    );
    let proxied = TestAllowlist::make(&[&ISOLATED[..], &["--label", "dagron.dev/proxy=socks5://x:1"]].concat());
    let bad_proxy = refused(vec![proxied.name.clone()]).await;
    assert!(bad_proxy.contains("dagron.dev/proxy"), "{bad_proxy}");
}

/// A task that names an allowlist the executor was not given is refused,
/// and so is one whose network was removed since the executor started:
/// each before anything is made for it.
#[tokio::test]
async fn an_allowlist_not_given_or_gone_is_refused_before_anything_is_made() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let allowlist = TestAllowlist::make(&ISOLATED);
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network_allowlists(vec![allowlist.name.clone()])
        .await
        .unwrap();
    let other = exec
        .execute(&task("true", Some(Network::Allow("forge".into()))))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        other.contains("allowlist 'forge'") && other.contains("DAGRON_TASK_NETWORK_ALLOWLISTS"),
        "{other}"
    );

    docker(&["network", "rm", &allowlist.network]);
    let ctx = task("true", Some(Network::Allow(allowlist.name.clone())));
    let gone = exec.execute(&ctx).await.expect_err("refused").to_string();
    assert!(gone.contains("no network"), "{gone}");
    let id = &ctx.identity.as_ref().unwrap().task_id;
    assert_eq!(
        docker(&["ps", "-aq", "--filter", &format!("label=dagron.dev/task-id={id}")]),
        "",
        "nothing was made for it"
    );
}

/// The proxy an allowlist's network says is told to a task held to it,
/// under each name tools read it by, with what not to send through it.
#[tokio::test]
async fn an_allowlist_s_proxy_is_told_to_the_task() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let allowlist = TestAllowlist::make(
        &[&ISOLATED[..], &["--label", "dagron.dev/proxy=http://egress:3128"]].concat(),
    );
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_network_allowlists(vec![allowlist.name.clone()])
        .await
        .unwrap();
    let out = exec
        .execute(&task(
            "echo \"$HTTPS_PROXY $http_proxy $NO_PROXY\"",
            Some(Network::Allow(allowlist.name.clone())),
        ))
        .await
        .unwrap();
    assert_eq!(
        out.output.trim(),
        "http://egress:3128 http://egress:3128 localhost,127.0.0.1,::1"
    );
}
