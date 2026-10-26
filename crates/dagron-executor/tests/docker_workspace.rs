//! A task's workspace and its tools on a real Docker daemon: a workspace task
//! works in a volume made for it, which a task whose image runs as another
//! user can write too; finds `dagron-ci-exec` installed read-only by an init
//! container; starts with its checkout, fetched by a container of its own
//! from a git server of the test's; reads its actions, fetched from the same
//! server by a container of theirs into a volume it cannot write, with a
//! credential no container it runs beside holds; and leaves no container or
//! volume behind, whether it passed, its install failed, its checkout did or
//! its actions did. And a task that fails keeps what it said.
//!
//! A task's image is the executor's to fetch: one the host lacks is pulled,
//! as the task's `image_pull_policy` says, and a pull that fails fails the
//! attempt in the daemon's words. These pull a small image from Docker Hub,
//! [`PULLED`], after taking it off the host.
//!
//! And what becomes of an attempt whose claim on its task is gone: its
//! containers are stopped and removed, with the grace its job named, at
//! whatever point of the attempt the stop lands, a container still being made
//! included; and a later dispatch of the task removes what a lower fence
//! left, and no other. The container that holds an attempt's actions goes
//! after the rest of it, there and in the fleet sweep, and takes their volume.
//!
//! And what an attempt leaves where the daemon does not do as it is asked,
//! or is slow to, through a proxy of its API that refuses, holds or delays
//! the requests a test names ([`support::docker_proxy`], on unix): no task
//! starts beside its checkout's credential; a task's own container, where it
//! made the workspace (no init container did), goes after its steps' and its
//! checkout's, which mount it by name, on every path; a step's container
//! being made as its task ends is waited for, never started, and removed,
//! and where it is not made in time the task's steps that run are killed
//! all the same; and a stop that lands while a task's steps are taken back
//! takes them back, and asks again for what the daemon is still removing.
//!
//! The init container's image here is a stand-in whose entrypoint does what
//! `dagron-ci-exec install DIR` does, put an executable `DIR/dagron-ci-exec`,
//! and which holds a `/workspace` anyone may write, as the real image does,
//! so that these tests need no static build of the real one. Built from
//! alpine:3.20 with the docker CLI, which is pulled first, as is the image
//! the checkout is fetched with, alpine/git.
//!
//! Needs a daemon and the CLI, and skips, saying so, where either is missing.
//! The tests share one lock: each counts the daemon's volumes before and after.

mod support;

use std::process::{Command, Stdio};

use bollard::image::{CreateImageOptions, RemoveImageOptions};
use bollard::volume::ListVolumesOptions;
use dagron_core::dag::ImagePullPolicy;
use dagron_core::workspace::{ActionSpec, CheckoutSpec, WorkspaceSpec};
use dagron_executor::docker_executor::{DockerExecutor, TaskMount};
use dagron_executor::executor::{ExecContext, Executor, TaskIdentity};
use futures_util::TryStreamExt;
#[cfg(unix)]
use support::docker_proxy::DockerProxy;

const BASE: &str = "alpine:3.20";
const INSTALLER: &str = "dagron-test-install-exec:2";
const BROKEN: &str = "dagron-test-broken-exec:1";
/// A task image that runs as a user other than root.
const NON_ROOT: &str = "dagron-test-non-root:1";
const GIT: &str = "alpine/git:v2.47.2";
/// An image no test here builds or otherwise runs, which the tests of a
/// task's image take off the host before they name it.
const PULLED: &str = "busybox:1.36.1";
/// An image no registry has.
const NOWHERE: &str = "dagron-test-no-such-image:never";

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What a stand-in installer's entrypoint runs: `install DIR` writes a
/// `dagron-ci-exec` that says how it was called.
const INSTALLER_DOCKERFILE: &str = r##"FROM alpine:3.20
RUN printf '%s\n' '#!/bin/sh' \
      '[ "$1" = install ] || { echo "not install: $*" >&2; exit 2; }' \
      'printf "#!/bin/sh\necho stand-in \"\$@\"\n" > "$2/dagron-ci-exec"' \
      'chmod 755 "$2/dagron-ci-exec"' > /entry \
 && chmod 755 /entry \
 && mkdir -m 0777 /workspace
ENTRYPOINT ["/entry"]
"##;

const NON_ROOT_DOCKERFILE: &str = r#"FROM alpine:3.20
USER 1000:1000
"#;

/// A task image with git that runs as a user other than root.
const NON_ROOT_GIT: &str = "dagron-test-non-root-git:1";
const NON_ROOT_GIT_DOCKERFILE: &str = r#"FROM alpine/git:v2.47.2
USER 1000:1000
ENTRYPOINT []
"#;

const BROKEN_DOCKERFILE: &str = r#"FROM alpine:3.20
ENTRYPOINT ["sh", "-c", "echo no space left for the binary >&2; exit 3", "--"]
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
    for image in [BASE, GIT] {
        if docker.inspect_image(image).await.is_ok() {
            continue;
        }
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
    for (tag, dockerfile) in [
        (INSTALLER, INSTALLER_DOCKERFILE),
        (BROKEN, BROKEN_DOCKERFILE),
        (NON_ROOT, NON_ROOT_DOCKERFILE),
        (NON_ROOT_GIT, NON_ROOT_GIT_DOCKERFILE),
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

async fn volumes(docker: &bollard::Docker) -> Vec<String> {
    let mut names: Vec<String> = docker
        .list_volumes(None::<ListVolumesOptions<String>>)
        .await
        .unwrap()
        .volumes
        .unwrap_or_default()
        .into_iter()
        .map(|v| v.name)
        .collect();
    names.sort();
    names
}

/// The containers labelled with `task`'s id: what this test's task left.
fn containers_of(task: &str) -> String {
    let out = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=dagron.dev/task-id={task}"),
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The address a container reaches this host at: the default bridge's
/// gateway.
fn host_from_containers() -> String {
    let out = Command::new("docker")
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ])
        .output()
        .expect("docker network inspect");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn workspace_task(script: &str, task: &str) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), script.into()],
        Some(120),
        None,
    );
    ctx.workspace = Some(WorkspaceSpec::default());
    ctx.identity = Some(TaskIdentity {
        task_id: task.into(),
        run_id: "r-workspace".into(),
        attempt: 1,
        fence: 1,
        installation: None,
    });
    ctx
}

/// `image` taken off the host, and seen gone.
async fn forget(docker: &bollard::Docker, image: &str) {
    let force = RemoveImageOptions {
        force: true,
        ..Default::default()
    };
    let _ = docker.remove_image(image, Some(force), None).await;
    assert!(
        docker.inspect_image(image).await.is_err(),
        "{image} is still on the host: something else here is using it"
    );
}

/// A task that says `ran`, from `image`, under `policy`.
fn from_image(image: &str, policy: Option<ImagePullPolicy>) -> ExecContext {
    let mut ctx = ExecContext::new(
        vec!["sh".into(), "-c".into(), "echo ran".into()],
        Some(60),
        Some(image.into()),
    );
    ctx.image_pull_policy = policy;
    ctx
}

/// The Docker API pulls nothing when a container is made, so a task that
/// named an image the host lacked failed with "No such image". The executor
/// fetches it now, as the task's policy says.
#[tokio::test]
async fn a_task_s_image_is_fetched_as_its_policy_says() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else {
        return;
    };
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    forget(&docker, PULLED).await;

    // never: nothing is pulled, and the attempt says what the host lacks.
    let err = exec
        .execute(&from_image(PULLED, Some(ImagePullPolicy::Never)))
        .await
        .err()
        .expect("there is nothing to run it from")
        .to_string();
    assert!(
        err.contains(&format!("does not hold the task's image {PULLED}"))
            && err.contains("image_pull_policy is never"),
        "{err}"
    );
    assert!(docker.inspect_image(PULLED).await.is_err(), "not pulled");

    // No policy: the host lacks it, so it is pulled, and the task runs.
    let out = exec
        .execute(&from_image(PULLED, None))
        .await
        .expect("pulled, then run");
    assert!(
        out.success && out.output.contains("ran\n"),
        "{:?}",
        out.output
    );
    assert!(
        docker.inspect_image(PULLED).await.is_ok(),
        "the host holds it now"
    );

    // On a host that holds it, every policy runs it.
    for policy in [
        ImagePullPolicy::Never,
        ImagePullPolicy::IfNotPresent,
        ImagePullPolicy::Always,
    ] {
        let out = exec
            .execute(&from_image(PULLED, Some(policy)))
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", policy.as_str()));
        assert!(out.success, "{}: {:?}", policy.as_str(), out.output);
    }

    // always, of an image the host holds and no registry has: the pull fails,
    // and the attempt with it, rather than run on what happens to be there.
    let err = exec
        .execute(&from_image(NON_ROOT, Some(ImagePullPolicy::Always)))
        .await
        .err()
        .expect("always means pulled")
        .to_string();
    assert!(
        err.contains(&format!("the task's image {NON_ROOT}"))
            && err.contains("could not be pulled"),
        "{err}"
    );
    // The same image, left to the default, runs: the host holds it.
    let out = exec.execute(&from_image(NON_ROOT, None)).await.unwrap();
    assert!(out.success, "{:?}", out.output);
    forget(&docker, PULLED).await;
}

#[tokio::test]
async fn a_pull_that_fails_fails_the_attempt_in_the_daemon_s_words_and_makes_nothing() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else {
        return;
    };
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let before = volumes(&docker).await;
    let mut ctx = workspace_task("true", "no-image");
    ctx.docker_image = Some(NOWHERE.into());
    let err = exec
        .execute(&ctx)
        .await
        .err()
        .expect("no image, no task")
        .to_string();
    assert!(
        err.contains(&format!("the task's image {NOWHERE}")) && err.contains("could not be pulled"),
        "{err}"
    );
    // What the registry or the daemon said is in it, whatever that was here.
    assert!(
        err.len() > format!("the task's image {NOWHERE}: the image could not be pulled: ").len(),
        "{err}"
    );
    assert_eq!(containers_of("no-image"), "", "nothing was made for it");
    assert_eq!(volumes(&docker).await, before);
}

#[tokio::test]
async fn a_failing_task_keeps_what_it_said() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let out = exec
        .execute(&ExecContext::new(
            vec![
                "sh".into(),
                "-c".into(),
                "echo said before failing; echo and on stderr >&2; exit 3".into(),
            ],
            Some(60),
            None,
        ))
        .await
        .expect("a task's own failure is its result, not the backend's error");
    assert!(!out.success);
    assert!(
        out.output.contains("said before failing\n"),
        "{:?}",
        out.output
    );
    assert!(out.output.contains("and on stderr\n"), "{:?}", out.output);
}

#[tokio::test]
async fn a_workspace_task_works_in_a_volume_with_dagron_ci_exec_beside_it() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());

    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();
    // Steps share files through the workspace: 64 MiB written, then read back.
    let script = "pwd; head -c 67108864 /dev/zero > big; wc -c < big; \
                  /dagron/bin/dagron-ci-exec --speaks 1; \
                  if touch /dagron/bin/x 2>/dev/null; then echo tools-writable; else echo tools-read-only; fi";
    let out = exec.execute(&workspace_task(script, &task)).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        "/workspace\n67108864\nstand-in --speaks 1\ntools-read-only\n"
    );

    assert_eq!(
        containers_of(&task),
        "",
        "the task's and the init's containers are gone"
    );
    assert_eq!(volumes(&docker).await, before, "and so are their volumes");
}

#[tokio::test]
async fn a_failed_install_fails_the_task_in_the_installer_s_words() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());

    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(BROKEN.into()))
        .await
        .unwrap();
    let err = exec
        .execute(&workspace_task("echo the task never ran", &task))
        .await
        .expect_err("no tools, no task")
        .to_string();
    assert!(err.contains("exited 3"), "{err}");
    assert!(err.contains("no space left for the binary"), "{err}");

    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);
}

#[tokio::test]
async fn an_exec_image_that_cannot_be_had_is_a_startup_error() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let err = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some("dagron-test-no-such-image:never".into()))
        .await
        .err()
        .expect("refused at startup")
        .to_string();
    assert!(
        err.contains("DAGRON_CI_EXEC_IMAGE=dagron-test-no-such-image:never"),
        "{err}"
    );
}

#[tokio::test]
async fn a_task_whose_image_runs_as_another_user_writes_its_workspace() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());

    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();
    let mut ctx = workspace_task("id -u; touch /workspace/x && echo wrote", &task);
    ctx.docker_image = Some(NON_ROOT.into());
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "1000\nwrote\n");

    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);
}

#[tokio::test]
async fn a_workspace_starts_with_its_checkout_fetched_by_a_container_of_its_own() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
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
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());

    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap()
        .with_checkout_image(Some(GIT.into()))
        .await
        .unwrap();
    // A task as another user than the fetch's: it reads the commit, git works
    // in it, and it can change what the fetch wrote.
    let mut ctx = workspace_task(
        "cat README; sh src/build.sh; git rev-parse HEAD; git status --porcelain | wc -l; \
         echo more >> README && mkdir src/new && echo edited",
        &task,
    );
    ctx.docker_image = Some(NON_ROOT_GIT.into());
    ctx.workspace.as_mut().unwrap().checkout = Some(CheckoutSpec {
        repository: server.url.clone(),
        commit: server.commit.clone(),
        depth: None,
        patched: false,
    });
    let out = exec.execute(&ctx).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        format!(
            "hello from the commit\nbuilt\n{}\n0\nedited\n",
            server.commit
        )
    );

    assert_eq!(
        containers_of(&task),
        "",
        "the task's, the init's and the checkout's are gone"
    );
    assert_eq!(volumes(&docker).await, before, "and so are their volumes");
}

#[tokio::test]
async fn a_checkout_that_cannot_be_fetched_fails_the_task_in_git_s_words() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) =
        support::git_server::GitServer::start(&[("README", "x\n")], "0.0.0.0", &host, None)
    else {
        return;
    };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());

    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_checkout_image(Some(GIT.into()))
        .await
        .unwrap();
    let mut ctx = workspace_task("echo the task never ran", &task);
    let missing = "0123456789abcdef0123456789abcdef01234567";
    ctx.workspace.as_mut().unwrap().checkout = Some(CheckoutSpec {
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
            "the checkout of {} at {missing} failed",
            server.url
        )),
        "{err}"
    );
    assert!(err.contains("exited 128"), "{err}");

    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);
}

#[tokio::test]
async fn a_checkout_with_no_image_to_fetch_it_with_is_refused_before_anything_is_made() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let mut ctx = workspace_task("true", &task);
    ctx.workspace.as_mut().unwrap().checkout = Some(CheckoutSpec {
        repository: "https://github.com/acme/widgets.git".into(),
        commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
        depth: None,
        patched: false,
    });
    let err = exec.execute(&ctx).await.expect_err("refused").to_string();
    assert!(err.contains("DAGRON_CHECKOUT_IMAGE"), "{err}");
    assert_eq!(containers_of(&task), "");
}

/// An action of `server`'s repository at `git_ref`, under a path of its own.
fn action_of(server: &support::git_server::GitServer, git_ref: &str) -> ActionSpec {
    ActionSpec {
        repository: server.url.clone(),
        git_ref: git_ref.into(),
        path: format!("acme/hello/{git_ref}"),
        credential: false,
    }
}

/// An executor that installs the stand-in and fetches with alpine/git, and
/// gives every task `mounts`.
async fn fetching(mounts: Vec<TaskMount>) -> DockerExecutor {
    DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_task_mounts(mounts, None)
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap()
        .with_checkout_image(Some(GIT.into()))
        .await
        .unwrap()
}

/// A task's actions are fetched before its container is made, by a container
/// of their own, into a volume another container holds, which the task
/// mounts by name and reads alone. They were fetched into an anonymous
/// volume of the task's own container, made read-only, which Docker before
/// 29 refuses ("must not set ReadOnly mode when using anonymous volumes"):
/// every job that used an action failed as its container was made.
#[tokio::test]
async fn a_task_reads_its_actions_and_cannot_change_them() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) = support::git_server::GitServer::start(
        &[
            ("action.yml", "name: hello\n"),
            ("say.sh", "echo hello from the action\n"),
        ],
        "0.0.0.0",
        &host,
        None,
    ) else {
        return;
    };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());

    // As root, whom only a read-only mount keeps out.
    let mut ctx = workspace_task(
        "cd \"$DAGRON_ACTIONS_DIR/acme/hello/main\" && pwd; cat action.yml; sh say.sh; ls -A; \
         if touch x 2>/dev/null; then echo writable; else echo read-only; fi",
        &task,
    );
    ctx.workspace.as_mut().unwrap().actions = vec![action_of(&server, "main")];
    let out = fetching(Vec::new())
        .await
        .execute(&ctx)
        .await
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output,
        "/dagron/actions/acme/hello/main\nname: hello\nhello from the action\n\
         action.yml\nsay.sh\nread-only\n",
        "the action's files and none of git's, which it cannot change"
    );

    assert_eq!(
        containers_of(&task),
        "",
        "the task's, the init's and the actions' containers are gone"
    );
    assert_eq!(volumes(&docker).await, before, "and so are their volumes");
}

/// An action that cannot be fetched fails its task in git's words, and the
/// task's container is never made. Nothing of the attempt is left: not when
/// the fetch fails, nor when the fetch is done and the task's container
/// cannot be made (here, for a mount of the operator's whose path is not
/// there).
#[tokio::test]
async fn an_attempt_whose_actions_never_reach_its_task_leaves_nothing() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) = support::git_server::GitServer::start(
        &[("action.yml", "name: hello\n")],
        "0.0.0.0",
        &host,
        None,
    ) else {
        return;
    };
    let before = volumes(&docker).await;

    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = workspace_task("echo the task never ran", &task);
    ctx.workspace.as_mut().unwrap().actions = vec![action_of(&server, "no-such-ref")];
    let err = fetching(Vec::new())
        .await
        .execute(&ctx)
        .await
        .expect_err("no actions, no task")
        .to_string();
    assert!(
        err.contains("the fetch of the task's actions failed"),
        "{err}"
    );
    assert!(
        err.contains(&format!(
            "the action {} has no no-such-ref to fetch",
            server.url
        )),
        "{err}"
    );
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);

    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let missing = std::env::temp_dir()
        .join(format!("dagron-missing-{}", uuid::Uuid::new_v4().simple()))
        .to_str()
        .unwrap()
        .to_string();
    let exec = fetching(vec![TaskMount {
        source: missing.clone(),
        target: "/srv/missing".into(),
        read_only: true,
    }])
    .await;
    let mut ctx = workspace_task("echo the task never ran", &task);
    ctx.workspace.as_mut().unwrap().actions = vec![action_of(&server, "main")];
    let err = exec
        .execute(&ctx)
        .await
        .expect_err("no container, no task")
        .to_string();
    assert!(err.contains(&missing), "in the daemon's words: {err}");
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);
}

/// Whether `task`'s own container is running now: not its init container,
/// its checkout's, its actions' or their fetch's, which carry the task's
/// labels too and run before it.
fn task_is_running(task: &str) -> bool {
    let out = Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("label=dagron.dev/task-id={task}"),
            "--filter",
            "status=running",
            "--format",
            "{{.Names}}",
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).lines().any(|name| {
        !name.ends_with("-init")
            && !name.ends_with("-checkout")
            && !name.ends_with("-actions")
            && !name.ends_with("-actions-fetch")
    })
}

/// Run `ctx` on `exec` until its task's container is up, take its claim away,
/// and say how the execution ended and how long that took.
async fn lose_the_claim(
    exec: DockerExecutor,
    ctx: ExecContext,
    task: &str,
) -> (anyhow::Error, std::time::Duration) {
    let stop = ctx.stop.clone();
    let running = tokio::spawn(async move { exec.execute(&ctx).await });
    for _ in 0..600 {
        if task_is_running(task) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(task_is_running(task), "the task's container never ran");
    // Up, and into its script: a signal that lands before the shell has set
    // its trap would be the default one.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let asked = std::time::Instant::now();
    stop.request();
    let ended = tokio::time::timeout(std::time::Duration::from_secs(90), running)
        .await
        .expect("a stopped execution ends")
        .unwrap();
    (ended.expect_err("a lost claim is no result"), asked.elapsed())
}

/// A stop that lands while the attempt's container is being made leaves
/// nothing. The attempt is dropped where it stands, and the daemon carried
/// out a create it had already been asked for: after the stop had looked for
/// the container and found none. What was left was a container made and
/// never started, with its volume, though the stop had said the work was
/// gone. A stop a few milliseconds after a dispatch is how a run cancelled as
/// it starts looks.
#[tokio::test]
async fn a_stop_while_a_container_is_being_made_leaves_nothing() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else {
        return;
    };
    let before = volumes(&docker).await;
    // Across the window: before anything is asked for, while the container
    // is being made, and once it is up.
    for (turn, wait_ms) in [0_u64, 1, 2, 4, 8, 12, 16, 24, 32, 48, 64, 96, 128, 200, 400]
        .into_iter()
        .cycle()
        .take(45)
        .enumerate()
    {
        let task = format!("stopped-early-{turn}");
        let exec = DockerExecutor::connect(BASE).await.unwrap();
        let ctx = workspace_task("sleep 60", &task);
        let stop = ctx.stop.clone();
        let running = tokio::spawn(async move { exec.execute(&ctx).await });
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
        stop.request();
        let ended = tokio::time::timeout(std::time::Duration::from_secs(60), running)
            .await
            .expect("a stopped execution ends")
            .unwrap();
        assert!(ended.is_err(), "a lost claim is no result");
        assert_eq!(
            containers_of(&task),
            "",
            "stopped {wait_ms} ms in (turn {turn}): a container of the attempt is left"
        );
    }
    assert_eq!(
        volumes(&docker).await,
        before,
        "nor a volume of any of them"
    );
}

/// A cancel takes a task's claim away. Its worker used to stop waiting, and
/// the container went on to the end of its command with its volumes held. Now
/// the executor stops and removes what the attempt made: the task's container
/// is told to stop, and a task that listens ends at once.
#[tokio::test]
async fn a_lost_claim_stops_the_task_and_leaves_nothing_of_the_attempt() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();
    // A job's step runner ends on the signal; so does this.
    let script = "trap 'exit 0' TERM; echo started > proof; while :; do sleep 1; done";
    let (err, took) = lose_the_claim(exec, workspace_task(script, &task), &task).await;

    assert!(
        err.is::<dagron_executor::executor::ClaimLost>(),
        "it says the claim was lost: {err}"
    );
    assert!(
        !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "and that what it made was seen gone: {err}"
    );
    assert!(
        took < std::time::Duration::from_secs(10),
        "a task that heeds its signal is not waited on for its grace: {took:?}"
    );
    assert_eq!(containers_of(&task), "", "no container of the attempt is left");
    assert_eq!(volumes(&docker).await, before, "and no volume");
}

/// A task that does not heed its signal is given its grace and then killed:
/// the grace its job named, which is what a step is promised.
#[tokio::test]
async fn a_task_that_ignores_its_signal_is_killed_once_its_grace_is_out() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();
    let mut ctx = workspace_task("trap '' TERM; while :; do sleep 1; done", &task);
    ctx.env.push(dagron_core::dag::EnvVar {
        name: dagron_executor::executor::ENV_STEP_GRACE.into(),
        value: "2".into(),
        value_from: None,
    });
    let (err, took) = lose_the_claim(exec, ctx, &task).await;

    assert!(err.is::<dagron_executor::executor::ClaimLost>(), "{err}");
    assert!(
        !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "and that what it made was seen gone: {err}"
    );
    assert!(
        took >= std::time::Duration::from_secs(2),
        "it was given its grace: {took:?}"
    );
    assert!(
        took < std::time::Duration::from_secs(40),
        "and no more than a little past it: {took:?}"
    );
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);
}

/// A stop at any point of an attempt with actions leaves nothing: before
/// their fetch, during it, as the task's container is made, and once the
/// task runs. The actions' container is removed after the task's: its
/// volume, which the task mounts by name, goes with it only then.
#[tokio::test]
async fn a_stop_at_any_point_of_an_attempt_with_actions_leaves_nothing() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) = support::git_server::GitServer::start(
        &[("action.yml", "name: hello\n")],
        "0.0.0.0",
        &host,
        None,
    ) else {
        return;
    };
    let before = volumes(&docker).await;
    let script = "trap 'exit 0' TERM; while :; do sleep 1; done";
    let with_actions = |task: &str| {
        let mut ctx = workspace_task(script, task);
        ctx.workspace.as_mut().unwrap().actions = vec![action_of(&server, "main")];
        ctx
    };
    for wait_ms in [0_u64, 10, 50, 100, 200, 400, 800, 1600] {
        let task = format!("t-{}", uuid::Uuid::new_v4().simple());
        let exec = fetching(Vec::new()).await;
        let ctx = with_actions(&task);
        let stop = ctx.stop.clone();
        let running = tokio::spawn(async move { exec.execute(&ctx).await });
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
        stop.request();
        let err = tokio::time::timeout(std::time::Duration::from_secs(60), running)
            .await
            .expect("a stopped execution ends")
            .unwrap()
            .expect_err("a lost claim is no result");
        assert!(
            err.is::<dagron_executor::executor::ClaimLost>()
                && !err.is::<dagron_executor::executor::TeardownUnseen>(),
            "stopped {wait_ms} ms in: {err}"
        );
        assert_eq!(containers_of(&task), "", "stopped {wait_ms} ms in");
        assert_eq!(volumes(&docker).await, before, "stopped {wait_ms} ms in");
    }

    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let (err, _) = lose_the_claim(fetching(Vec::new()).await, with_actions(&task), &task).await;
    assert!(
        err.is::<dagron_executor::executor::ClaimLost>()
            && !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "{err}"
    );
    assert_eq!(
        containers_of(&task),
        "",
        "no container of the attempt is left"
    );
    assert_eq!(volumes(&docker).await, before, "and no volume");
}

/// A checkout image that sends git's fetch of [`PRIVATE`] to the test's
/// server, by a `url.insteadOf` of its system config.
const REWRITING_GIT: &str = "dagron-test-rewriting-git:1";
/// A private action's repository: https, the only kind a credential is sent
/// to beyond this host.
const PRIVATE: &str = "https://actions.dagron.invalid/acme/private.git";

/// A private action's credential reaches its fetch and no container the task
/// runs beside. The container that fetched the actions was kept, stopped, as
/// long as the task ran, to hold their volume, and with each action's
/// credential in its settings: `docker inspect` read it, from any task given
/// the host's daemon. Their volume is held by a container of its own now,
/// made with no variables and never started, and the fetch is removed before
/// the task's container is made.
#[tokio::test]
async fn a_private_action_s_credential_is_in_no_container_while_its_task_runs() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) = support::git_server::GitServer::start(
        &[("action.yml", "name: private\n")],
        "0.0.0.0",
        &host,
        None,
    ) else {
        return;
    };
    build(
        REWRITING_GIT,
        &format!(
            "FROM {GIT}\nRUN git config --system url.{}.insteadOf {PRIVATE}\n",
            server.url
        ),
    );
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let credential = dagron_executor::checkout::Credential {
        username: "x-access-token".into(),
        password: format!("ghs_{}", uuid::Uuid::new_v4().simple()),
    };
    let authorization = credential.authorization();
    let secret = authorization.trim_start_matches("Basic ").to_string();

    let mut ctx = workspace_task(
        "sleep 4; cat \"$DAGRON_ACTIONS_DIR/acme/private/main/action.yml\"",
        &task,
    );
    ctx.workspace.as_mut().unwrap().actions = vec![ActionSpec {
        repository: PRIVATE.into(),
        git_ref: "main".into(),
        path: "acme/private/main".into(),
        credential: true,
    }];
    ctx.action_credentials = vec![(PRIVATE.into(), credential)];
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap()
        .with_checkout_image(Some(REWRITING_GIT.into()))
        .await
        .unwrap();
    let running = tokio::spawn(async move { exec.execute(&ctx).await });
    for _ in 0..600 {
        if task_is_running(&task) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(task_is_running(&task), "the task's container never ran");

    let made = containers_of(&task);
    for id in made.lines() {
        let env = docker_cli(&["inspect", "-f", "{{.Name}} {{json .Config.Env}}", id]);
        assert!(
            !env.contains(&secret),
            "a container of the attempt holds the credential while the task runs: {env}"
        );
    }

    let out = tokio::time::timeout(std::time::Duration::from_secs(120), running)
        .await
        .expect("the task ends")
        .unwrap()
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(
        out.output, "name: private\n",
        "fetched, with the credential"
    );
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before);
    forget(&docker, REWRITING_GIT).await;
}

/// A leftover container of the task `task`, labelled as dagron labels its own.
fn leftover(task: &str, attempt: &str, fence: Option<&str>) -> String {
    let mut args: Vec<String> = ["run", "-d"].map(String::from).to_vec();
    let mut label = |key: &str, value: &str| {
        args.push("--label".into());
        args.push(format!("{key}={value}"));
    };
    label("dagron.dev/managed-by", "dagron");
    label("dagron.dev/task-id", task);
    label("dagron.dev/attempt", attempt);
    if let Some(fence) = fence {
        label("dagron.dev/fence", fence);
    }
    args.extend([BASE, "sleep", "600"].map(String::from));
    let out = Command::new("docker").args(&args).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The dispatch of `task` under fence 9, attempt 1, whose task names `grace`
/// seconds for a stopped step.
fn rerun_of(task: &str, grace: &str) -> ExecContext {
    let mut ctx = ExecContext::new(vec!["true".into()], Some(60), None);
    ctx.identity = Some(TaskIdentity {
        task_id: task.into(),
        run_id: "r-rerun".into(),
        attempt: 1,
        fence: 9,
        installation: None,
    });
    ctx.env.push(dagron_core::dag::EnvVar {
        name: dagron_executor::executor::ENV_STEP_GRACE.into(),
        value: grace.into(),
        value_from: None,
    });
    ctx
}

/// A rerun sets a task's attempt back to 0, so its first dispatch carries the
/// attempt a cancelled one did, and the two used to run side by side. The
/// fence only rises: a dispatch removes the containers of a lower one before
/// it starts, and leaves a higher one's alone, whatever their attempts say.
#[tokio::test]
async fn a_dispatch_removes_a_lower_fence_s_container_and_no_other() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    // What a cancelled first attempt left; a newer claim's, though its attempt
    // is the lower; and one from before containers carried a fence.
    let cancelled = leftover(&task, "1", Some("4"));
    let newer = leftover(&task, "0", Some("12"));
    let old_style = leftover(&task, "0", None);

    let exec = DockerExecutor::connect(BASE).await.unwrap();
    // A short grace: the leftovers are `sleep`s, which heed no signal.
    let ctx = rerun_of(&task, "1");
    let out = exec.execute(&ctx).await.unwrap();
    assert!(out.success, "{}", out.output);

    let left = containers_of(&task);
    let left: Vec<&str> = left.lines().collect();
    let has = |id: &str| left.iter().any(|short| id.starts_with(short));
    assert!(!has(&cancelled), "the cancelled attempt's is gone: {left:?}");
    assert!(has(&newer), "a newer claim's is left alone: {left:?}");
    assert!(!has(&old_style), "one with no fence is judged by its attempt, as it was: {left:?}");
    assert_eq!(left.len(), 1, "and this dispatch's own is removed after it: {left:?}");
    let _ = Command::new("docker").args(["rm", "-f", &newer]).output();
}

/// One attempt, one workload. A dispatch whose predecessor is still within
/// its grace starts nothing beside it: it is handed back, which costs the
/// task no attempt, and the predecessor is not killed early to make room.
/// Once the predecessor is gone the next try runs.
#[tokio::test]
async fn a_dispatch_does_not_start_beside_an_earlier_attempt_still_in_its_grace() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    // A `sleep`, which heeds no signal: it takes all of its grace to stop.
    let earlier = leftover(&task, "1", Some("4"));
    let exec = DockerExecutor::connect(BASE).await.unwrap();

    // A grace well past the reap's budget.
    let waited = std::time::Instant::now();
    let err = exec
        .execute(&rerun_of(&task, "120"))
        .await
        .expect_err("nothing starts beside a predecessor that is still stopping");
    let refused = err
        .downcast_ref::<dagron_executor::executor::Backpressure>()
        .unwrap_or_else(|| panic!("it is handed back, not failed: {err}"));
    assert!(
        refused.reason.contains("an earlier attempt of this task may still be running"),
        "{}",
        refused.reason
    );
    assert!(
        waited.elapsed() < std::time::Duration::from_secs(60),
        "within the reap's budget, not the grace: {:?}",
        waited.elapsed()
    );
    let left = containers_of(&task);
    let left: Vec<&str> = left.lines().collect();
    assert_eq!(left.len(), 1, "nothing was made beside it: {left:?}");
    assert!(earlier.starts_with(left[0]), "and it is the earlier attempt's: {left:?}");
    assert!(task_is_running(&task), "which was not killed before its grace was out");

    // Tried again, as the engine does: with the predecessor gone, it runs.
    let _ = Command::new("docker").args(["rm", "-f", &earlier]).output();
    let out = exec.execute(&rerun_of(&task, "1")).await.unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(containers_of(&task), "");
}

/// A teardown alone, for a task that was asked to stop and whose work nobody
/// is watching: what its earlier claims left is stopped with its grace and
/// removed, and nothing is made. While any of it is still there the teardown
/// says so, and is asked again; a later claim's container is not its to touch.
#[tokio::test]
async fn a_teardown_removes_what_earlier_claims_left_and_makes_nothing() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let earlier = leftover(&task, "1", Some("4"));
    let newer = leftover(&task, "0", Some("12"));
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    let count = || containers_of(&task).lines().count();

    // Within a two-minute grace it is still there: said, and not killed early.
    let err = exec
        .teardown(&rerun_of(&task, "120"))
        .await
        .expect_err("what is still stopping is not gone");
    let still = err
        .downcast_ref::<dagron_executor::executor::Backpressure>()
        .unwrap_or_else(|| panic!("it says what is left: {err}"));
    assert!(
        still.reason.contains("an earlier attempt of this task may still be running"),
        "{}",
        still.reason
    );
    assert_eq!(count(), 2, "and nothing was made: {}", containers_of(&task));

    // Asked again, with a grace that runs out: gone, and the later claim's is
    // left where it was.
    let _ = Command::new("docker").args(["rm", "-f", &earlier]).output();
    let _again = leftover(&task, "1", Some("5"));
    exec.teardown(&rerun_of(&task, "1")).await.expect("nothing of the earlier claims is left");
    let left = containers_of(&task);
    let left: Vec<&str> = left.lines().collect();
    assert_eq!(left.len(), 1, "{left:?}");
    assert!(newer.starts_with(left[0]), "the later claim's container: {left:?}");
    let _ = Command::new("docker").args(["rm", "-f", &newer]).output();
}

/// `docker` run with `args`: what it wrote.
fn docker_cli(args: &[&str]) -> String {
    let out = Command::new("docker").args(args).output().unwrap();
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The `--label` arguments of a container of `task`'s attempt under fence 4,
/// and of `installation` where one is given.
fn labels_of(task: &str, installation: Option<&str>) -> Vec<String> {
    let mut labels = vec![
        "dagron.dev/managed-by=dagron".to_string(),
        format!("dagron.dev/task-id={task}"),
        "dagron.dev/run-id=r-leftover".to_string(),
        "dagron.dev/attempt=1".to_string(),
        "dagron.dev/fence=4".to_string(),
    ];
    if let Some(installation) = installation {
        labels.push(format!("dagron.dev/installation={installation}"));
    }
    labels
        .into_iter()
        .flat_map(|label| ["--label".to_string(), label])
        .collect()
}

/// What an earlier attempt of a task with actions left, as an attempt names
/// its containers: `name-actions`, which holds the actions' volume, made and
/// never started; then `name`, the task's, running, which mounts that volume
/// by name. Made with `labels`. The volume's name.
fn leftover_with_actions(name: &str, labels: &[String]) -> String {
    let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
    let holder = format!("{name}-actions");
    let mut create = vec!["create", "--name", &holder, "-v", "/dagron/actions"];
    create.extend(&labels);
    create.extend([BASE, "true"]);
    docker_cli(&create);
    let volume = docker_cli(&[
        "inspect",
        "-f",
        "{{range .Mounts}}{{.Name}}{{end}}",
        &holder,
    ]);
    // Made a few seconds after: a fetch comes between them.
    std::thread::sleep(std::time::Duration::from_secs(3));
    let mount = format!("type=volume,src={volume},dst=/dagron/actions,readonly,volume-nocopy");
    let mut run = vec!["run", "-d", "--name", name, "--mount", &mount];
    run.extend(&labels);
    run.extend([BASE, "sleep", "600"]);
    docker_cli(&run);
    volume
}

/// What an earlier attempt of a task with actions left: the container that
/// holds the actions' volume, and the task's, still running, which mounts
/// that volume by name. A dispatch's teardown removes the holder only once
/// the task's is gone, and the volume goes with the holder. Removed while
/// the task's container was still there, the holder would go and leave its
/// volume, in use, which the task's container, having mounted it by name,
/// would not take with it either. So while the task's container is still in
/// its grace, both are left.
#[tokio::test]
async fn a_teardown_removes_an_earlier_attempt_s_actions_and_their_volume() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let name = format!("sched-{}", uuid::Uuid::new_v4().simple());
    let volume = leftover_with_actions(&name, &labels_of(&task, None));
    let exec = DockerExecutor::connect(BASE).await.unwrap();

    // Within a two-minute grace the task's container is still there, and so
    // is the holder of what it mounts.
    let err = exec
        .teardown(&rerun_of(&task, "120"))
        .await
        .expect_err("what is still stopping is not gone");
    assert!(err.is::<dagron_executor::executor::Backpressure>(), "{err}");
    assert_eq!(
        containers_of(&task).lines().count(),
        2,
        "the holder of a volume a container still there mounts is left"
    );
    assert!(volumes(&docker).await.contains(&volume));

    // Asked again, with a grace that runs out: both gone, and the volume.
    exec.teardown(&rerun_of(&task, "1"))
        .await
        .expect("nothing of the earlier claim is left");
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before, "nor the actions' volume");
}

/// The fleet sweep judges each container by its age, and the container that
/// holds an attempt's actions (or its tools) is made before the task's: it
/// was a candidate while the task's was still too young to be one, and was
/// removed first, and its volume, in use, was left for good. It is no
/// candidate while another container of its attempt is there, however old it
/// is. Once they are gone it is one, and its volume goes with it.
#[tokio::test]
async fn the_fleet_sweep_takes_a_holder_of_volumes_only_after_the_rest_of_its_attempt() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let installation = format!("sweep-{}", uuid::Uuid::new_v4().simple());
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let name = format!("sched-{}", uuid::Uuid::new_v4().simple());
    leftover_with_actions(&name, &labels_of(&task, Some(&installation)));
    let holder = docker_cli(&["inspect", "-f", "{{.Id}}", &format!("{name}-actions")]);
    let exec = DockerExecutor::connect(BASE).await.unwrap();
    // Old enough: the holder; the task's container is not.
    let scope = dagron_executor::executor::OrphanScope {
        installation: &installation,
        min_age: std::time::Duration::from_secs(2),
    };

    let found = exec.list_orphan_candidates(&scope).await.unwrap();
    assert!(
        found.is_empty(),
        "the task's container is too young to judge, and the holder waits for it: {found:?}"
    );

    // Its task's container gone (a later pass of the sweep's), the holder is
    // a candidate, and removed with its volume.
    docker_cli(&["rm", "-f", &name]);
    let found = exec.list_orphan_candidates(&scope).await.unwrap();
    assert_eq!(
        found.iter().map(|w| w.handle.as_str()).collect::<Vec<_>>(),
        [holder.as_str()]
    );
    exec.delete_workload(&found[0]).await.unwrap();
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before, "nor the actions' volume");
}

/// A step in an image of its own, [`BASE`], that runs `script` for `secs`
/// at most.
fn step_container(script: &str, secs: u64) -> dagron_executor::executor::StepContainer {
    dagron_executor::executor::StepContainer {
        image: BASE.into(),
        entrypoint: Some("/bin/sh".into()),
        args: vec!["-c".into(), script.into()],
        env: vec![("SAID".into(), "from the step's variables".into())],
        timeout: std::time::Duration::from_secs(secs),
    }
}

/// A step of `task`'s in an image of its own, asked for as soon as the task
/// has lent it what it shares: what came of it.
async fn step_of(
    exec: &impl Executor,
    task: &str,
    script: &str,
    secs: u64,
) -> dagron_executor::executor::StepOutcome {
    let step = step_container(script, secs);
    for _ in 0..150 {
        match exec.run_step_container(task, step.clone()).await {
            Ok(outcome) => return outcome,
            Err(e) if e.to_string().contains("not running on this engine") => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await
            }
            Err(e) => panic!("the step's container: {e:#}"),
        }
    }
    panic!("the task never lent its steps anything");
}

/// A step of a running task's, in an image of its own (a workflow's Docker
/// action): its container is made beside the task's, with the task's
/// workspace at /workspace and at /github/workspace, where it starts, and is
/// gone when it has ended. One past its time is stopped. The task sees what
/// it wrote; and nothing of either is left.
#[tokio::test]
async fn a_step_runs_in_a_container_of_its_own_beside_its_task_in_its_workspace() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let exec = DockerExecutor::connect(BASE)
        .await
        .unwrap()
        .with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap();
    steps_beside_their_task(&docker, exec).await;
}

/// The same where no init container makes the workspace: it is a volume of
/// the task's own container, which its steps' mount by name, and it goes
/// with the task's, after theirs.
#[tokio::test]
async fn a_step_runs_beside_its_task_in_the_workspace_its_task_s_container_made() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    steps_beside_their_task(&docker, DockerExecutor::connect(BASE).await.unwrap()).await;
}

/// Two steps of a workspace task's run on `exec`, one past its time, and
/// nothing of the task left after them.
async fn steps_beside_their_task(docker: &bollard::Docker, exec: DockerExecutor) {
    let before = volumes(docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(exec);
    // The task writes a file, then waits for the step's answer in another;
    // and then a moment, as a step runner waits for the step to end.
    let script = "echo from-the-task > made; \
                  for i in $(seq 1 300); do [ -f done ] && break; sleep 0.2; done; cat done; sleep 3";
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        let ctx = workspace_task(script, &task);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    let slow = step_of(&*exec, &task, "sleep 30", 2).await;
    assert!(slow.timed_out, "{slow:?}");
    assert_eq!(slow.exit_code, 124);
    let ran = step_of(
        &*exec,
        &task,
        "pwd; cat /workspace/made; echo \"$SAID\"; echo from-the-step > /github/workspace/done; exit 3",
        60,
    )
    .await;
    assert_eq!(
        (ran.exit_code, ran.timed_out, ran.output.as_str()),
        (
            3,
            false,
            "/github/workspace\nfrom-the-task\nfrom the step's variables\n"
        )
    );
    let out = running.await.unwrap().unwrap();
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "from-the-step\n");
    assert_eq!(
        containers_of(&task),
        "",
        "the task's, its init's and its steps' containers are gone"
    );
    assert_eq!(volumes(docker).await, before, "and so are their volumes");
}

/// A tar of a Dockerfile's directory, as a step runner sends it: one that
/// copies a script into an image of [`BASE`].
fn build_context(says: &str) -> Vec<u8> {
    let dir = std::env::temp_dir().join(format!("dagron-build-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("Dockerfile"),
        "FROM alpine:3.20\nCOPY say.sh /say.sh\n",
    )
    .unwrap();
    std::fs::write(dir.join("say.sh"), format!("echo {says}\n")).unwrap();
    let out = Command::new("tar")
        .arg("-cf")
        .arg("-")
        .arg("-C")
        .arg(&dir)
        .arg(".")
        .output()
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// A step's image built from a Dockerfile, for a running task, where the
/// operator lets the executor build one: the step then runs in it, and one
/// it built and holds as it built it is not built again. An image another
/// put under the name is built over, never reused; a name it built nothing
/// under is never pulled. Where the operator has not, none is built.
#[tokio::test]
async fn a_step_s_image_is_built_from_its_dockerfile_where_the_operator_lets_it() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let image = format!("dagron-ci-action:test-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(
        DockerExecutor::connect(BASE)
            .await
            .unwrap()
            .with_build_actions(true)
            .with_ci_exec_image(Some(INSTALLER.into()))
            .await
            .unwrap(),
    );
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        let ctx = workspace_task(
            "for i in $(seq 1 300); do [ -f done ] && break; sleep 0.2; done; sleep 3",
            &task,
        );
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    // Another's image under the name, before anything is built.
    let (repo, tag) = image.split_once(':').unwrap();
    docker
        .tag_image(BASE, Some(bollard::image::TagImageOptions { repo, tag }))
        .await
        .unwrap();
    let build = dagron_executor::executor::StepBuild {
        image: image.clone(),
        context: build_context("from-the-built-image"),
        dockerfile: "Dockerfile".into(),
        timeout: std::time::Duration::from_secs(300),
    };
    let mut built = None;
    for _ in 0..150 {
        match exec.build_step_image(&task, build.clone()).await {
            Ok(b) => {
                built = Some(b);
                break;
            }
            Err(e) if e.to_string().contains("not running on this engine") => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await
            }
            Err(e) => panic!("the step's image: {e:#}"),
        }
    }
    let built = built.expect("the task never lent its steps anything");
    assert_eq!(built.image, image);
    assert!(
        !built.output.contains("built here before"),
        "another's image under the name is built over: {}",
        built.output
    );
    assert!(
        docker.inspect_image(&image).await.is_ok(),
        "{}",
        built.output
    );
    let again = exec.build_step_image(&task, build.clone()).await.unwrap();
    assert!(
        again.output.contains("built here before"),
        "{}",
        again.output
    );
    // A built name it built nothing under is not pulled.
    let err = exec
        .run_step_container(
            &task,
            dagron_executor::executor::StepContainer {
                image: "dagron-ci-action:never-built-here".into(),
                entrypoint: None,
                args: Vec::new(),
                env: Vec::new(),
                timeout: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no image this engine built"), "{err}");
    let ran = exec
        .run_step_container(
            &task,
            dagron_executor::executor::StepContainer {
                image: image.clone(),
                entrypoint: Some("/bin/sh".into()),
                args: vec![
                    "-c".into(),
                    "sh /say.sh; echo done > /github/workspace/done".into(),
                ],
                env: Vec::new(),
                timeout: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (ran.exit_code, ran.output.as_str()),
        (0, "from-the-built-image\n")
    );
    let out = running.await.unwrap().unwrap();
    assert!(out.success, "{}", out.output);

    // Where the operator has not let it, no image is built.
    let plain = DockerExecutor::connect(BASE).await.unwrap();
    let err = plain
        .build_step_image(&task, build)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("DAGRON_DOCKER_BUILD_ACTIONS"), "{err}");
    forget(&docker, &image).await;
}

/// An executor that reaches the daemon through `proxy`, and installs the
/// stand-in where `installs`.
#[cfg(unix)]
async fn through(proxy: &DockerProxy, installs: bool) -> DockerExecutor {
    let exec = DockerExecutor::connect_with(proxy.client(), BASE)
        .await
        .unwrap();
    if !installs {
        return exec;
    }
    exec.with_ci_exec_image(Some(INSTALLER.into()))
        .await
        .unwrap()
}

/// The places in `seen`, a proxy's, of the requests by `method` whose path,
/// with no query, ends with `tail`.
#[cfg(unix)]
fn asked(seen: &[String], method: &str, tail: &str) -> Vec<usize> {
    seen.iter()
        .enumerate()
        .filter(|(_, request)| {
            request.split_once(' ').is_some_and(|(m, target)| {
                m == method && target.split('?').next().unwrap_or_default().ends_with(tail)
            })
        })
        .map(|(i, _)| i)
        .collect()
}

/// Whether `seen`, a proxy's, has a request to make a container named with
/// `suffix`.
#[cfg(unix)]
fn made(seen: &[String], suffix: &str) -> bool {
    seen.iter().any(|request| {
        request
            .strip_prefix("POST /containers/create?")
            .is_some_and(|query| {
                query
                    .split('&')
                    .any(|pair| pair.starts_with("name=") && pair.ends_with(suffix))
            })
    })
}

/// Each container of `task`'s, by its name, with its state, in the order of
/// their names.
#[cfg(unix)]
fn states_of(task: &str) -> Vec<(String, String)> {
    let listed = docker_cli(&[
        "ps",
        "-a",
        "--filter",
        &format!("label=dagron.dev/task-id={task}"),
        "--format",
        "{{.Names}} {{.State}}",
    ]);
    let mut states: Vec<(String, String)> = listed
        .lines()
        .filter_map(|line| line.split_once(' '))
        .map(|(name, state)| (name.to_string(), state.to_string()))
        .collect();
    states.sort();
    states
}

/// The task container's name of the attempt whose checkout's container
/// `seen`, a proxy's, has the first request to remove.
#[cfg(unix)]
fn checkout_s_task(seen: &[String]) -> String {
    asked(seen, "DELETE", "-checkout")
        .first()
        .and_then(|&i| seen[i].split(['?', ' ']).nth(1))
        .and_then(|path| path.strip_prefix("/containers/"))
        .and_then(|checkout| checkout.strip_suffix("-checkout"))
        .unwrap_or_else(|| panic!("the checkout's container was asked to go: {seen:?}"))
        .to_string()
}

/// What is left of `task`'s removed as a later claim's dispatch removes it,
/// through `exec`: then nothing of it, and no volume but `before`.
#[cfg(unix)]
async fn torn_down(exec: &DockerExecutor, docker: &bollard::Docker, task: &str, before: &[String]) {
    exec.teardown(&rerun_of(task, "1"))
        .await
        .expect("nothing of the earlier claim is left");
    assert_eq!(containers_of(task), "", "no container of the task is left");
    assert_eq!(volumes(docker).await, before, "and no volume");
}

/// The proxy the tests below reach the daemon through sees every request,
/// each on a connection of its own (bollard keeps none idle), and what it
/// refuses reaches the client as the daemon's own refusal would.
#[cfg(unix)]
#[tokio::test]
async fn the_proxy_sees_each_request_and_refuses_as_the_daemon_does() {
    let _one = LOCK.lock().await;
    if daemon().await.is_none() {
        return;
    }
    let proxy = DockerProxy::start();
    let client = proxy.client();
    for _ in 0..3 {
        client.ping().await.expect("a ping goes through");
    }
    let probe = format!("dagron-proxy-probe-{}", uuid::Uuid::new_v4().simple());
    proxy.refuse("GET", &format!("/containers/{probe}/json"), Some(1), 500);
    let refused = client
        .inspect_container(&probe, None)
        .await
        .expect_err("refused");
    assert!(
        matches!(
            refused,
            bollard::errors::Error::DockerResponseServerError {
                status_code: 500,
                ..
            }
        ),
        "{refused:?}"
    );
    // Only the first: the second reaches the daemon, which has no such
    // container.
    let missing = client
        .inspect_container(&probe, None)
        .await
        .expect_err("no such container");
    assert!(
        matches!(
            missing,
            bollard::errors::Error::DockerResponseServerError {
                status_code: 404,
                ..
            }
        ),
        "{missing:?}"
    );
    let seen = proxy.seen();
    assert_eq!(seen.len(), 5, "{seen:?}");
    assert_eq!(asked(&seen, "GET", "/_ping"), [0, 1, 2]);
    assert_eq!(
        asked(&seen, "GET", &format!("/containers/{probe}/json")),
        [3, 4]
    );
    proxy.clear();
}

/// The container that fetches a task's checkout holds the repository's
/// credential, in the settings `docker inspect` reads. It was removed as soon
/// as it had run, and not seen removed: one the daemon refused to remove was
/// left, and the task started beside it. A task's container is started now
/// only once its checkout's is seen removed, and an attempt whose checkout's
/// is not fails before its task starts, whether or not an init container
/// made the workspace. A later dispatch removes what it left.
#[cfg(unix)]
#[tokio::test]
async fn a_task_is_never_started_beside_its_checkout_s_credential() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) =
        support::git_server::GitServer::start(&[("README", "x\n")], "0.0.0.0", &host, None)
    else {
        return;
    };
    // A credential is sent to https alone: the private repository's fetch
    // goes to the test's server.
    build(
        REWRITING_GIT,
        &format!(
            "FROM {GIT}\nRUN git config --system url.{}.insteadOf {PRIVATE}\n",
            server.url
        ),
    );
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    for installs in [false, true] {
        let task = format!("t-{}", uuid::Uuid::new_v4().simple());
        let credential = dagron_executor::checkout::Credential {
            username: "x-access-token".into(),
            password: format!("ghs_{}", uuid::Uuid::new_v4().simple()),
        };
        let secret = credential
            .authorization()
            .trim_start_matches("Basic ")
            .to_string();
        let mut ctx = workspace_task("echo the task ran", &task);
        ctx.workspace.as_mut().unwrap().checkout = Some(CheckoutSpec {
            repository: PRIVATE.into(),
            commit: server.commit.clone(),
            depth: None,
            patched: false,
        });
        ctx.checkout_credential = Some(credential);
        let exec = through(&proxy, installs)
            .await
            .with_checkout_image(Some(REWRITING_GIT.into()))
            .await
            .unwrap();
        proxy.refuse("DELETE", "-checkout", None, 500);
        let from = proxy.seen().len();

        let err = exec
            .execute(&ctx)
            .await
            .expect_err("not started beside the credential")
            .to_string();
        assert!(
            err.contains("was not seen removed"),
            "installs {installs}: {err}"
        );
        let seen = proxy.seen()[from..].to_vec();
        let name = checkout_s_task(&seen);
        assert!(
            asked(&seen, "POST", &format!("/containers/{name}/start")).is_empty(),
            "installs {installs}: the task's container was started: {seen:?}"
        );
        // Of what is left, the checkout's container alone holds it.
        let left = containers_of(&task);
        assert!(!left.is_empty(), "the checkout's container is left");
        for id in left.lines() {
            let env = docker_cli(&["inspect", "-f", "{{.Name}} {{json .Config.Env}}", id]);
            assert_eq!(
                env.contains(&secret),
                env.starts_with(&format!("/{name}-checkout ")),
                "installs {installs}: {env}"
            );
        }

        proxy.clear();
        torn_down(&exec, &docker, &task, &before).await;
    }
    forget(&docker, REWRITING_GIT).await;
}

/// A checkout's container the daemon is still removing when first asked
/// (it answers a conflict) is asked again, and the task starts once it is
/// seen gone.
#[cfg(unix)]
#[tokio::test]
async fn a_checkout_s_container_still_being_removed_is_waited_for() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let host = host_from_containers();
    let Some(server) =
        support::git_server::GitServer::start(&[("README", "x\n")], "0.0.0.0", &host, None)
    else {
        return;
    };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let mut ctx = workspace_task("cat README", &task);
    ctx.workspace.as_mut().unwrap().checkout = Some(CheckoutSpec {
        repository: server.url.clone(),
        commit: server.commit.clone(),
        depth: None,
        patched: false,
    });
    let exec = through(&proxy, false)
        .await
        .with_checkout_image(Some(GIT.into()))
        .await
        .unwrap();
    proxy.refuse("DELETE", "-checkout", Some(1), 409);

    let out = exec.execute(&ctx).await.expect("the task ran");
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "x\n");
    let seen = proxy.seen();
    let name = checkout_s_task(&seen);
    let removals = asked(&seen, "DELETE", "-checkout");
    let started = asked(&seen, "POST", &format!("/containers/{name}/start"));
    assert!(
        removals.len() >= 2 && started.len() == 1 && removals[1] < started[0],
        "asked again, and seen gone before the task started: {seen:?}"
    );

    proxy.clear();
    torn_down(&exec, &docker, &task, &before).await;
}

/// What a task that waits for its step's `done` runs.
#[cfg(unix)]
const AWAITS_ITS_STEP: &str =
    "for i in $(seq 1 300); do [ -f done ] && break; sleep 0.2; done; cat done";

/// `ctx`, whose task waits for its step, run on `exec` beside a step that
/// writes `done`: what came of the task.
#[cfg(unix)]
async fn ran_beside_a_step(
    exec: &std::sync::Arc<DockerExecutor>,
    ctx: ExecContext,
    task: &str,
) -> dagron_executor::executor::ExecOutput {
    let running = {
        let exec = std::sync::Arc::clone(exec);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    let ran = step_of(
        &**exec,
        task,
        "echo from-the-step > /github/workspace/done",
        60,
    )
    .await;
    assert_eq!(ran.exit_code, 0, "{ran:?}");
    tokio::time::timeout(std::time::Duration::from_secs(120), running)
        .await
        .expect("the task ends")
        .unwrap()
        .expect("the task ran")
}

/// What `task` left where its step's container was not seen removed: the
/// task's container and the step's, both exited, and nothing else. Their
/// names.
#[cfg(unix)]
fn left_beside_a_step(task: &str) -> (String, String) {
    let left = states_of(task);
    let step = left
        .iter()
        .map(|(name, _)| name)
        .find(|name| name.ends_with("-step-1"))
        .unwrap_or_else(|| panic!("the step's container is left: {left:?}"))
        .clone();
    let name = step.strip_suffix("-step-1").unwrap().to_string();
    assert_eq!(
        left,
        [
            (name.clone(), "exited".to_string()),
            (step.clone(), "exited".to_string())
        ],
        "the task's container and the step's, and nothing else"
    );
    (name, step)
}

/// Where no init container made a task's workspace, it is a volume of the
/// task's own container, and its steps' containers mount it by name. The
/// task's was removed as the task ended, while the container of a step the
/// daemon had not removed still mounted the volume: Docker kept it, in use,
/// and nothing removed it ever after. The task's goes only once its steps'
/// are seen gone now, and is otherwise left, stopped, with its volume, and a
/// later dispatch removes the step's first and then the task's, and the
/// volume with it.
#[cfg(unix)]
#[tokio::test]
async fn a_task_s_own_container_waits_for_its_step_s_and_takes_their_workspace() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(through(&proxy, false).await);
    proxy.refuse("DELETE", "-step-1", None, 500);

    let out = ran_beside_a_step(&exec, workspace_task(AWAITS_ITS_STEP, &task), &task).await;
    assert!(out.success, "{}", out.output);
    assert_eq!(out.output, "from-the-step\n");
    let (name, step) = left_beside_a_step(&task);
    let volume = docker_cli(&["inspect", "-f", "{{range .Mounts}}{{.Name}}{{end}}", &name]);
    assert!(
        !before.contains(&volume) && volumes(&docker).await.contains(&volume),
        "the task's workspace is left with it"
    );

    proxy.clear();
    let ids = [&step, &name].map(|n| docker_cli(&["inspect", "-f", "{{.Id}}", n]));
    let from = proxy.seen().len();
    torn_down(&exec, &docker, &task, &before).await;
    let seen = proxy.seen()[from..].to_vec();
    let [step_removed, task_removed] =
        ids.map(|id| asked(&seen, "DELETE", &format!("/containers/{id}")));
    assert!(
        !step_removed.is_empty()
            && !task_removed.is_empty()
            && step_removed.last() < task_removed.first(),
        "the step's first, then the task's: {seen:?}"
    );
}

/// The fleet sweep likewise: the task's own container is no candidate while
/// its step's is there, however old it is, and is one once the step's is
/// gone, and goes with the workspace.
#[cfg(unix)]
#[tokio::test]
async fn the_fleet_sweep_takes_a_task_s_own_container_after_its_step_s() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let installation = format!("sweep-{}", uuid::Uuid::new_v4().simple());
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(through(&proxy, false).await);
    proxy.refuse("DELETE", "-step-1", None, 500);
    let mut ctx = workspace_task(AWAITS_ITS_STEP, &task);
    ctx.identity.as_mut().unwrap().installation = Some(installation.clone());

    let out = ran_beside_a_step(&exec, ctx, &task).await;
    assert!(out.success, "{}", out.output);
    let (name, step) = left_beside_a_step(&task);
    let [step_id, task_id] = [&step, &name].map(|n| docker_cli(&["inspect", "-f", "{{.Id}}", n]));

    proxy.clear();
    let scope = dagron_executor::executor::OrphanScope {
        installation: &installation,
        min_age: std::time::Duration::ZERO,
    };
    for (judged, id) in [("the step's", &step_id), ("the task's", &task_id)] {
        let found = exec.list_orphan_candidates(&scope).await.unwrap();
        assert_eq!(
            found.iter().map(|w| w.handle.as_str()).collect::<Vec<_>>(),
            [id.as_str()],
            "{judged} alone"
        );
        exec.delete_workload(&found[0]).await.unwrap();
    }
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before, "nor the task's workspace");
    torn_down(&exec, &docker, &task, &before).await;
}

/// A stop while the container of a step the daemon would not remove is
/// there: the task's own, whose volume the step's mounts, is killed and
/// left, and the stop says so, and says which step's it did not see
/// removed. A later dispatch removes both, and the volume.
#[cfg(unix)]
#[tokio::test]
async fn a_stop_leaves_a_task_s_own_container_while_a_step_s_mounts_its_workspace() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(through(&proxy, false).await);
    proxy.refuse("DELETE", "-step-1", None, 500);
    let ctx = workspace_task(
        "trap 'exit 0' TERM; for i in $(seq 1 300); do [ -f done ] && break; sleep 0.2; done; \
         while :; do sleep 1; done",
        &task,
    );
    let stop = ctx.stop.clone();
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    let ran = step_of(
        &*exec,
        &task,
        "echo from-the-step > /github/workspace/done",
        60,
    )
    .await;
    assert_eq!(ran.exit_code, 0, "{ran:?}");
    assert!(
        !asked(&proxy.seen(), "DELETE", "-step-1").is_empty(),
        "the step's container was asked to go"
    );

    stop.request();
    let err = tokio::time::timeout(std::time::Duration::from_secs(90), running)
        .await
        .expect("a stopped execution ends")
        .unwrap()
        .expect_err("a lost claim is no result");
    assert!(err.is::<dagron_executor::executor::ClaimLost>(), "{err:#}");
    let unseen = err
        .downcast_ref::<dagron_executor::executor::TeardownUnseen>()
        .unwrap_or_else(|| panic!("it says what it did not see removed: {err:#}"));
    assert!(
        unseen.0.contains("-step-1 was not removed")
            && unseen
                .0
                .contains(" was left: its workspace is a volume one of those may still mount"),
        "{}",
        unseen.0
    );
    assert!(!task_is_running(&task), "the task's own was killed");
    left_beside_a_step(&task);

    proxy.clear();
    torn_down(&exec, &docker, &task, &before).await;
}

/// What an earlier attempt with no init container left, as an attempt names
/// its containers: `name`, the task's, whose volume is its workspace, and
/// `name-step-1`, a step's, which mounts that volume by name; both running,
/// and made with `labels`. Their ids, the task's first.
#[cfg(unix)]
fn leftover_with_a_step(name: &str, labels: &[String]) -> (String, String) {
    let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
    let mut run = vec!["run", "-d", "--name", name, "-v", "/workspace"];
    run.extend(&labels);
    run.extend([BASE, "sleep", "600"]);
    let task = docker_cli(&run);
    let volume = docker_cli(&["inspect", "-f", "{{range .Mounts}}{{.Name}}{{end}}", name]);
    let step = format!("{name}-step-1");
    let mount = format!("type=volume,src={volume},dst=/github/workspace,volume-nocopy");
    let mut run = vec!["run", "-d", "--name", &step, "--mount", &mount];
    run.extend(&labels);
    run.extend([BASE, "sleep", "600"]);
    (task, docker_cli(&run))
}

/// A teardown removed what an earlier attempt left all at once: where the
/// daemon did not remove a step's container at the first asking, the task's
/// own went first, with the volume the step's still mounted, which Docker
/// kept, in use, and nothing removed ever after. The step's goes first now,
/// asked again, and the task's only once it is gone, and its volume with it.
#[cfg(unix)]
#[tokio::test]
async fn a_teardown_removes_an_earlier_attempt_s_steps_before_the_task_s_own_container() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let name = format!("sched-{}", uuid::Uuid::new_v4().simple());
    let (task_id, step_id) = leftover_with_a_step(&name, &labels_of(&task, None));
    let proxy = DockerProxy::start();
    let exec = through(&proxy, false).await;
    proxy.refuse("DELETE", &format!("/containers/{step_id}"), Some(1), 500);

    exec.teardown(&rerun_of(&task, "1"))
        .await
        .expect("nothing of the earlier claim is left");
    assert_eq!(containers_of(&task), "");
    assert_eq!(volumes(&docker).await, before, "nor the task's workspace");
    let seen = proxy.seen();
    let step_removed = asked(&seen, "DELETE", &format!("/containers/{step_id}"));
    let task_removed = asked(&seen, "DELETE", &format!("/containers/{task_id}"));
    assert!(
        step_removed.len() >= 2 && !task_removed.is_empty() && step_removed[1] < task_removed[0],
        "the step's asked for again, and gone before the task's: {seen:?}"
    );

    proxy.clear();
    torn_down(&exec, &docker, &task, &before).await;
}

/// A step of `task`'s asked for on `exec`, on a task of its own, as soon as
/// the task has lent it what it shares: what came of it, whatever it was.
#[cfg(unix)]
fn step_beside(
    exec: &std::sync::Arc<DockerExecutor>,
    task: &str,
    script: &str,
    secs: u64,
) -> tokio::task::JoinHandle<anyhow::Result<dagron_executor::executor::StepOutcome>> {
    let (exec, task, step) = (
        std::sync::Arc::clone(exec),
        task.to_string(),
        step_container(script, secs),
    );
    tokio::spawn(async move {
        for _ in 0..150 {
            match exec.run_step_container(&task, step.clone()).await {
                Err(e) if e.to_string().contains("not running on this engine") => {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await
                }
                ran => return ran,
            }
        }
        anyhow::bail!("the task never lent its steps anything")
    })
}

/// A task's steps are taken back as it ends: each step's container still
/// there is killed and removed. A stop that landed while that was under way
/// dropped it, and found nothing to take back, since the task had given its
/// loan up as it ended: the step's container ran on, with the workspace
/// mounted, past the attempt. The loan is kept, ended, to the attempt's end
/// now, and the stop takes the steps back again. And a step asked for once
/// the task has ended is refused, and nothing is made for it.
#[cfg(unix)]
#[tokio::test]
async fn a_stop_while_a_task_s_steps_are_taken_back_takes_them_back() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    for installs in [true, false] {
        let task = format!("t-{}", uuid::Uuid::new_v4().simple());
        let exec = std::sync::Arc::new(through(&proxy, installs).await);
        // Its step's own time is long: only its being taken back kills it.
        proxy.hold("POST", "-step-1/kill", Some(1));
        let from = proxy.seen().len();
        let ctx = workspace_task(
            "for i in $(seq 1 300); do [ -f started ] && break; sleep 0.2; done",
            &task,
        );
        let stop = ctx.stop.clone();
        let running = {
            let exec = std::sync::Arc::clone(&exec);
            tokio::spawn(async move { exec.execute(&ctx).await })
        };
        let first = step_beside(
            &exec,
            &task,
            "touch /github/workspace/started; sleep 600",
            600,
        );
        proxy.wait_held(std::time::Duration::from_secs(60)).await;

        let err = exec
            .run_step_container(&task, step_container("true", 60))
            .await
            .expect_err("the task has ended")
            .to_string();
        assert!(
            err.contains("not running on this engine"),
            "installs {installs}: {err}"
        );
        let seen = proxy.seen()[from..].to_vec();
        assert!(
            made(&seen, "-step-1") && !made(&seen, "-step-2"),
            "installs {installs}: nothing is made for a step of a task that has ended: {seen:?}"
        );

        stop.request();
        proxy.release();
        let err = tokio::time::timeout(std::time::Duration::from_secs(90), running)
            .await
            .expect("a stopped execution ends")
            .unwrap()
            .expect_err("a lost claim is no result");
        assert!(
            err.is::<dagron_executor::executor::ClaimLost>()
                && !err.is::<dagron_executor::executor::TeardownUnseen>(),
            "installs {installs}: {err:#}"
        );
        assert_eq!(
            containers_of(&task),
            "",
            "installs {installs}: the step's container is gone"
        );
        assert_eq!(
            volumes(&docker).await,
            before,
            "installs {installs}: and the workspace"
        );
        tokio::time::timeout(std::time::Duration::from_secs(60), first)
            .await
            .expect("the step's run ends with it")
            .unwrap()
            .ok();

        proxy.clear();
        torn_down(&exec, &docker, &task, &before).await;
    }
}

/// And a stop that lands while the attempt's end asks again for a step's
/// container the daemon did not remove as the task ended: the loan is still
/// there, and the stop takes the step back.
#[cfg(unix)]
#[tokio::test]
async fn a_stop_while_a_step_s_container_is_asked_for_again_takes_it_back() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(through(&proxy, false).await);
    // Killed as the task ends, not seen removed, and asked for again.
    proxy.refuse("DELETE", "-step-1", None, 500);
    proxy.hold("POST", "-step-1/kill", Some(2));
    let ctx = workspace_task(
        "for i in $(seq 1 300); do [ -f started ] && break; sleep 0.2; done",
        &task,
    );
    let stop = ctx.stop.clone();
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    let first = step_beside(
        &exec,
        &task,
        "touch /github/workspace/started; sleep 600",
        600,
    );
    proxy.wait_held(std::time::Duration::from_secs(60)).await;

    // The daemon removes it from now.
    proxy.clear();
    stop.request();
    proxy.release();
    let err = tokio::time::timeout(std::time::Duration::from_secs(90), running)
        .await
        .expect("a stopped execution ends")
        .unwrap()
        .expect_err("a lost claim is no result");
    assert!(
        err.is::<dagron_executor::executor::ClaimLost>()
            && !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "{err:#}"
    );
    assert_eq!(containers_of(&task), "", "the step's container is gone");
    assert_eq!(volumes(&docker).await, before, "and the workspace");
    tokio::time::timeout(std::time::Duration::from_secs(60), first)
        .await
        .expect("the step's run ends with it")
        .unwrap()
        .ok();

    torn_down(&exec, &docker, &task, &before).await;
}

/// What a task whose end the test lets come runs: it waits for `go` in its
/// workspace ([`let_go`]).
#[cfg(unix)]
const AWAITS_GO: &str = "for i in $(seq 1 600); do [ -f go ] && break; sleep 0.2; done";

/// `go` put in the workspace of the task's container `name`, which waits
/// for it ([`AWAITS_GO`]): the task ends.
#[cfg(unix)]
fn let_go(name: &str) {
    docker_cli(&["exec", name, "touch", "go"]);
}

/// The names of the containers `seen`, a proxy's, has requests to make, in
/// the order they were asked for.
#[cfg(unix)]
fn asked_to_make(seen: &[String]) -> Vec<String> {
    seen.iter()
        .filter_map(|request| request.strip_prefix("POST /containers/create?"))
        .filter_map(|query| query.split('&').find_map(|pair| pair.strip_prefix("name=")))
        .map(str::to_string)
        .collect()
}

/// Once what `task`'s attempt on `exec` lent its steps has ended, as the
/// task's end takes them back. Asked by a step whose image this host does
/// not hold, under the task's policy, which is never to pull: while the
/// loan is open, it is refused before anything is made for it.
#[cfg(unix)]
async fn loan_ended(exec: &DockerExecutor, task: &str) {
    let probe = dagron_executor::executor::StepContainer {
        image: NOWHERE.into(),
        ..step_container("true", 60)
    };
    for _ in 0..300 {
        let err = exec
            .run_step_container(task, probe.clone())
            .await
            .expect_err("nothing is made for a step whose image is not here")
            .to_string();
        if err.contains("not running on this engine") {
            return;
        }
        assert!(err.contains("image_pull_policy is never"), "{err}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the task's loan never ended");
}

/// A step's container whose making is under way as its task ends: the
/// task's end waits for the request, then takes the container back, which
/// is never started, before the task's own goes with the workspace it
/// mounts. The end did not wait: the container was made once the task's own
/// had gone with its volume, which Docker made again, by name, for the
/// step's, and which nothing removed.
#[cfg(unix)]
#[tokio::test]
async fn a_step_s_container_being_made_as_its_task_ends_is_never_started_and_is_removed() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(through(&proxy, false).await);
    // The task's container is the first made, its step's the second.
    proxy.delay("POST", "/containers/create", Some(2));
    let mut ctx = workspace_task(AWAITS_GO, &task);
    ctx.image_pull_policy = Some(ImagePullPolicy::Never);
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    let step = step_beside(&exec, &task, "true", 60);
    proxy.wait_held(std::time::Duration::from_secs(60)).await;
    let making = asked_to_make(&proxy.seen());
    let name = making[0].clone();
    assert_eq!(
        making,
        [name.clone(), format!("{name}-step-1")],
        "the step's is the one delayed"
    );

    let_go(&name);
    loan_ended(&exec, &task).await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(
        !running.is_finished(),
        "the task's end waits for its step's container being made"
    );
    let from = proxy.seen().len();
    proxy.release();
    let out = tokio::time::timeout(std::time::Duration::from_secs(90), running)
        .await
        .expect("the task's end is over once its step's container is made")
        .unwrap()
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    let err = tokio::time::timeout(std::time::Duration::from_secs(60), step)
        .await
        .expect("the step's run ends")
        .unwrap()
        .expect_err("the step's container is never started")
        .to_string();
    assert!(
        err.contains("the task ended before its step's container started"),
        "{err}"
    );
    let seen = proxy.seen();
    let made_for_the_step = format!("/containers/{name}-step-1");
    assert!(
        asked(&seen, "POST", &format!("{made_for_the_step}/start")).is_empty(),
        "the step's container was started: {seen:?}"
    );
    assert!(
        asked(&seen, "DELETE", &made_for_the_step)
            .iter()
            .any(|&i| i >= from),
        "the step's container was asked to go once made: {seen:?}"
    );
    assert_eq!(containers_of(&task), "", "nothing of the task is left");
    assert_eq!(volumes(&docker).await, before, "nor its workspace");

    proxy.clear();
    torn_down(&exec, &docker, &task, &before).await;
}

/// A task's end waits ten seconds at most for a step's container being
/// made, and the attempt's end as long again. Past that it killed none of
/// its steps' containers: one that ran (a job runs two steps at once) ran on
/// past the attempt, with the workspace mounted, to its own limit. Each that
/// runs is killed now all the same, and the one being made, made once the
/// attempt is over, is never started.
#[cfg(unix)]
#[tokio::test]
async fn a_task_s_end_that_cannot_wait_for_a_step_s_container_kills_its_steps_that_run() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = std::sync::Arc::new(through(&proxy, false).await);
    // The task's container is the first made, its first step's the second,
    // and its second step's the third: made only once the attempt is over.
    proxy.delay("POST", "/containers/create", Some(3));
    let ctx = workspace_task(AWAITS_GO, &task);
    let running = {
        let exec = std::sync::Arc::clone(&exec);
        tokio::spawn(async move { exec.execute(&ctx).await })
    };
    // Its own time is long: only the task's end kills it.
    let first = step_beside(&exec, &task, "sleep 600", 600);
    let runs = |task: &str| {
        states_of(task)
            .iter()
            .any(|(name, state)| name.ends_with("-step-1") && state == "running")
    };
    for _ in 0..600 {
        if runs(&task) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(runs(&task), "the first step's container runs");
    let second = step_beside(&exec, &task, "true", 60);
    proxy.wait_held(std::time::Duration::from_secs(60)).await;
    let making = asked_to_make(&proxy.seen());
    let name = making[0].clone();
    assert_eq!(
        making,
        [
            name.clone(),
            format!("{name}-step-1"),
            format!("{name}-step-2")
        ],
        "the second step's is the one delayed"
    );

    let_go(&name);
    let out = tokio::time::timeout(std::time::Duration::from_secs(90), running)
        .await
        .expect("the attempt ends")
        .unwrap()
        .expect("the task ran");
    assert!(out.success, "{}", out.output);
    let step = format!("{name}-step-1");
    assert!(
        !states_of(&task).contains(&(step.clone(), "running".to_string())),
        "a step's container ran on past its task's end: {:?}",
        states_of(&task)
    );
    let seen = proxy.seen();
    assert!(
        !asked(&seen, "POST", &format!("/containers/{step}/kill")).is_empty(),
        "the step's container was killed: {seen:?}"
    );

    proxy.release();
    let err = tokio::time::timeout(std::time::Duration::from_secs(60), second)
        .await
        .expect("the second step's run ends")
        .unwrap()
        .expect_err("the second step's container is never started")
        .to_string();
    assert!(
        err.contains("the task ended before its step's container started"),
        "{err}"
    );
    tokio::time::timeout(std::time::Duration::from_secs(60), first)
        .await
        .expect("the first step's run ends with it")
        .unwrap()
        .ok();
    let seen = proxy.seen();
    assert!(
        asked(&seen, "POST", &format!("/containers/{name}-step-2/start")).is_empty(),
        "the second step's container was started: {seen:?}"
    );

    proxy.clear();
    torn_down(&exec, &docker, &task, &before).await;
}

/// A stop asks again for a container the daemon is still removing, which
/// it answers with a conflict until it is done: a removal the dropped
/// attempt had asked for goes on in the daemon. The stop asked once, said
/// the attempt's work was not seen gone, and where the task made its
/// workspace, killed and left the task's container.
#[cfg(unix)]
#[tokio::test]
async fn a_stop_asks_again_for_what_the_daemon_is_still_removing() {
    let _one = LOCK.lock().await;
    let Some(docker) = daemon().await else { return };
    let before = volumes(&docker).await;
    let proxy = DockerProxy::start();
    let task = format!("t-{}", uuid::Uuid::new_v4().simple());
    let exec = through(&proxy, false).await;
    // A task with no checkout has no checkout's container, which the stop
    // alone asks for: answered as the daemon answers while a removal of it
    // is under way.
    proxy.refuse("DELETE", "-checkout", Some(1), 409);

    let script = "trap 'exit 0' TERM; while :; do sleep 1; done";
    let (err, _) = lose_the_claim(exec, workspace_task(script, &task), &task).await;
    assert!(
        err.is::<dagron_executor::executor::ClaimLost>()
            && !err.is::<dagron_executor::executor::TeardownUnseen>(),
        "{err:#}"
    );
    let seen = proxy.seen();
    assert!(
        asked(&seen, "DELETE", "-checkout").len() >= 2,
        "asked again: {seen:?}"
    );
    assert_eq!(containers_of(&task), "", "nothing of the task is left");
    assert_eq!(volumes(&docker).await, before, "nor its workspace");

    proxy.clear();
    torn_down(&through(&proxy, false).await, &docker, &task, &before).await;
}
