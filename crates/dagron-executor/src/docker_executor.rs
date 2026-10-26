use anyhow::Result;
use async_trait::async_trait;
use bollard::{
    container::{
        AttachContainerOptions, AttachContainerResults, Config, CreateContainerOptions,
        KillContainerOptions, ListContainersOptions, LogOutput, LogsOptions,
        RemoveContainerOptions, StopContainerOptions, WaitContainerOptions,
    },
    exec::{CreateExecOptions, StartExecResults},
    image::{BuildImageOptions, CreateImageOptions},
    models::{HostConfig, HostConfigLogConfig, Mount, MountTypeEnum, MountVolumeOptions},
    Docker,
};
use dagron_core::dag::ImagePullPolicy;
use dagron_core::service::ServiceSpec;
use futures_util::{StreamExt, TryStreamExt};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// How long a pull may go without a word from the daemon before it is given
/// up. Not a bound on the pull: a large image takes as long as it takes, and
/// the daemon reports its progress all the while. A registry that stops
/// answering mid-pull would otherwise hold the attempt, and its worker, for
/// good.
const IMAGE_PULL_STALL: Duration = Duration::from_secs(300);

/// Held while one of an attempt's containers is being made, by the request
/// that makes it and not by the attempt, and taken by the stop before it
/// removes anything ([`DockerExecutor::create_under`]). A step's container
/// is not made under it: the stop holds it while it takes the steps back.
type Making = std::sync::Arc<tokio::sync::Mutex<()>>;

/// How long best-effort container cleanup may take before it is abandoned.
/// Matches `KubeExecutor::cleanup`, which bounds pod deletion the same way.
const CLEANUP_TIMEOUT_SECS: u64 = 10;

/// One budget for the WHOLE predecessor reap, list and removals together.
///
/// Per-removal timeouts do not compose: they bound each call and therefore add
/// up, so N stale containers against a wedged dockerd delay `create_container`
/// by N × [`CLEANUP_TIMEOUT_SECS`] — an unbounded dispatch delay that grows
/// with exactly the mess the reap exists to clear. `ctx.timeout_secs` does not
/// cover this; it starts later, at the wait and log collection.
///
/// When the budget expires with a predecessor still there, this dispatch does
/// not start: it is handed back as [`crate::executor::Backpressure`], which
/// costs the task no attempt, and is tried again in a few seconds. It used to
/// start anyway, on the reasoning that refusing would turn a cleanup failure
/// into an outage. That reasoning held while a refusal was a failed attempt;
/// it is a wait now, and starting anyway is how one task ran twice at once.
const REAP_BUDGET_SECS: u64 = 20;

/// How long past its grace a stopped task's container is left before it is
/// killed. A job's step runner gives its step the same grace and then ends on
/// its own; the margin is for that ending.
const STOP_MARGIN_SECS: u64 = 5;

/// How long a service that is not ready yet is left before it is asked again.
const READY_POLL: Duration = Duration::from_millis(500);
/// How long one run of a service's `ready` may take before the service is
/// counted as not ready yet.
const READY_PROBE_SECS: u64 = 10;
/// How much of what a `ready` wrote is kept for why a service was not ready.
const READY_SAID_BYTES: usize = 2048;
/// How many of a service's last lines go into why it was not ready.
const SERVICE_SAID_LINES: &str = "20";
use uuid::Uuid;

use crate::checkout::ENV_CHECKOUT_IMAGE;
use crate::executor::{
    ExecContext, ExecOutput, Executor, StepBuild, StepBuilt, StepContainer, StepOutcome,
    BUILT_STEP_IMAGE, MAX_STEP_OUTPUT_BYTES, STEP_WORKSPACE_DIR,
};
use crate::lines::WholeLines;
use crate::workspace::{install_args, ENV_CI_EXEC_IMAGE, TOOLS_DIR, WORKSPACE_DIR};
use dagron_core::workspace::{ActionSpec, CheckoutSpec, ACTIONS_DIR, ENV_ACTIONS_DIR};

/// The mounts every task container gets, `SOURCE:TARGET[:ro|rw]`, comma
/// separated. See [`parse_mounts`].
pub const ENV_MOUNTS: &str = "DAGRON_DOCKER_MOUNTS";
/// The directory every task container starts in. See [`parse_workdir`].
pub const ENV_WORKDIR: &str = "DAGRON_DOCKER_WORKDIR";
/// The log driver every container of this executor's is made with, where the
/// operator names one. See [`parse_log_driver`].
pub const ENV_LOG_DRIVER: &str = "DAGRON_DOCKER_LOG_DRIVER";
/// The network every container this executor makes is on: a task's, the one
/// that installs its tools and the one that fetches its checkout. Unset, each
/// is on the daemon's default network, as it always was.
pub const ENV_NETWORK: &str = "DAGRON_DOCKER_NETWORK";

/// The bridge driver's option that gives the host no IPv4 address on a
/// network: what an allowlist's network sets, so that a task held to it
/// reaches nothing of the host's.
pub const INHIBIT_IPV4: &str = "com.docker.network.bridge.inhibit_ipv4";
/// Whether a trusted run's step may have its image built from a Dockerfile
/// on this executor's daemon (a GitHub Docker action whose `runs.image` is
/// one). See [`parse_build_actions`].
pub const ENV_BUILD_ACTIONS: &str = "DAGRON_DOCKER_BUILD_ACTIONS";

/// `DAGRON_DOCKER_BUILD_ACTIONS`: `allow`, or unset (empty) for none.
///
/// A build runs what its Dockerfile says on the daemon, with the daemon's
/// network and none of a task's confinement, and what it builds is kept on
/// the host; so it is the operator's to allow, and the engine asks it only
/// for a run that is trusted.
pub fn parse_build_actions(raw: &str) -> std::result::Result<bool, String> {
    match raw.trim() {
        "" => Ok(false),
        "allow" => Ok(true),
        other => Err(format!("{other:?} is not allow, or unset for no build")),
    }
}

/// The network mode a daemon gives a container that is to have no network.
const NO_NETWORK: &str = "none";
/// Modes every daemon takes, and that need not be among the networks it
/// lists (Podman lists neither).
const BUILT_IN_NETWORKS: [&str; 2] = [NO_NETWORK, "bridge"];

/// `DAGRON_DOCKER_NETWORK`: a network's name, `none`, or unset (empty) for
/// the daemon's default.
///
/// `host` is refused. It is the host's own network, which reaches what the
/// engine's host reaches, the daemon and the engine's own ports among it, and
/// keeping a job off that is what naming a network is for. So is
/// `container:NAME`, another container's network, which is no network of
/// the operator's making.
pub fn parse_network(raw: &str) -> std::result::Result<Option<String>, String> {
    let name = raw.trim();
    if name.is_empty() {
        return Ok(None);
    }
    if name == "host" {
        return Err(
            "host is the host's own network: a job on it reaches what the engine's host does"
                .into(),
        );
    }
    let mut chars = name.chars();
    let starts = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
    if !starts || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')) {
        return Err(format!(
            "{name:?} is not a network's name: letters, digits, '_', '.' and '-', beginning \
             with a letter or a digit"
        ));
    }
    Ok(Some(name.to_string()))
}

/// Whether an image's `USER` is one that can be told not to be root from
/// its text: a number other than 0, with or without a group. The rule a
/// kubelet holds a pod's `runAsNonRoot` to, for the same reason: a name is
/// whatever the image's own `/etc/passwd` says it is.
fn stated_non_root(user: &str) -> std::result::Result<(), String> {
    let name = user.split(':').next().unwrap_or_default().trim();
    if name.is_empty() {
        return Err("states no user, which is root".into());
    }
    match name.parse::<u64>() {
        Ok(0) => Err("runs as user 0, which is root".into()),
        Ok(_) => Ok(()),
        Err(_) if name == "root" => Err("runs as root".into()),
        Err(_) => Err(format!(
            "runs as the user {name:?}, and a name cannot be told from root without running \
             the image"
        )),
    }
}

/// What a task's trust envelope asks of every container made for it, in the
/// daemon's terms. The Kubernetes executor's `PodHardening` gives the same
/// three to a pod's init containers as to its task, and so does this.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Hardening {
    read_only_root_fs: bool,
    drop_all_capabilities: bool,
    security_opt: Vec<String>,
}

impl Hardening {
    fn of(ctx: &ExecContext) -> Self {
        let Some(iso) = &ctx.isolation else {
            return Self::default();
        };
        let read_only_root_fs = iso.read_only_root_fs == Some(true);
        let drop_all_capabilities = iso.drop_all_capabilities == Some(true);
        let mut security_opt = Vec::new();
        // Asking for any of the three forbids gaining privilege, as a pod's
        // `allowPrivilegeEscalation: false` goes with any of them.
        if read_only_root_fs || drop_all_capabilities || iso.no_new_privileges == Some(true) {
            security_opt.push("no-new-privileges".to_string());
        }
        // `runtime_default` is what a container gets when nothing is said:
        // the daemon's own profile. Only the opt-out is said.
        if iso.seccomp == Some(dagron_core::isolation::Seccomp::Unconfined) {
            security_opt.push("seccomp=unconfined".to_string());
        }
        Self {
            read_only_root_fs,
            drop_all_capabilities,
            security_opt,
        }
    }
}

/// Which of an attempt's containers a setting is being asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Made {
    /// The task's own: the image and the command its workflow names.
    Task,
    /// One the executor runs for it first, from an image the operator set:
    /// the one that installs its tools, and the one that fetches its checkout.
    ForIt,
}
/// The driver that keeps a container's whole log for the daemon to read back:
/// a file on the daemon's host, which nothing rotates unless the daemon's own
/// options for it do. Podman takes the name for its own file driver
/// (`k8s-file`).
pub const FILE_LOG_DRIVER: &str = "json-file";

/// Which log driver this executor's containers are made with.
///
/// A task's own output is read from its container's streams as it writes
/// them, and does not depend on the driver. What the daemon gives
/// back of a container's output once the container has exited does, and is
/// whatever the container's log driver kept: a Docker action's step
/// container's output; the last words of a container that failed before its
/// task started (the one that installs its tools, its checkout's, a
/// service's); and a task's own, where its container could not be attached to
/// or the follow of it was cut. A container that names no driver gets the
/// daemon's own, which is the host's choice and not always one that keeps
/// everything: Podman's is the journal, which drops lines past a rate limit
/// that all of a user's containers share. A job that wrote 30,000 lines in a
/// second had fewer than 10,000 of them in its log when its log was read
/// back, and nothing in the log to say so.
///
/// Naming one is the operator's to do, and not this executor's by default.
/// The driver that keeps everything is a file that nothing bounds, and a host
/// whose own driver is bounded (the journal, Docker's `local`) may be one
/// where that bound is what keeps a task that writes without end from filling
/// the disk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LogDriver {
    /// Whichever the daemon gives a container that names none.
    #[default]
    Daemons,
    /// This one, named to the daemon for each container.
    Named(String),
}

/// `DAGRON_DOCKER_LOG_DRIVER`: a driver's name, or unset (empty, or
/// `daemon`) for the daemon's own default.
pub fn parse_log_driver(raw: &str) -> std::result::Result<LogDriver, String> {
    let named = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':');
    match raw.trim() {
        "" | "daemon" => Ok(LogDriver::Daemons),
        name if name.chars().all(named) => Ok(LogDriver::Named(name.to_string())),
        other => Err(format!(
            "{other:?} is not a log driver's name, nor `daemon` for the daemon's own"
        )),
    }
}

/// What does and what does not depend on a container's log driver, said
/// with each warning of one that may not keep it all.
const READ_BACK: &str = "A task's own log is read from its container's streams as it runs, \
    and does not depend on it; what is read back does: a Docker action's step output, what a \
    container that failed before its task started last said, and a task's log where its \
    container could not be followed.";

/// What a log driver keeps of a container's output for the daemon to give
/// back: all of it, some of it, or none.
#[derive(Debug, PartialEq, Eq)]
enum Kept {
    Whole,
    /// Not all of it, or not always: why.
    Part(&'static str),
    /// Nothing the daemon can read back: why.
    Nothing(&'static str),
}

/// What `driver` keeps, with the options it has where none are set. A driver
/// this does not know is taken to keep some: a daemon that sends its
/// containers' logs elsewhere may keep a copy to read back, and may not.
fn kept_by(driver: &str) -> Kept {
    match driver {
        // A file on the daemon's host, not rotated unless the daemon's
        // options say so. `k8s-file` is Podman's.
        "json-file" | "k8s-file" => Kept::Whole,
        "local" => Kept::Part(
            "Docker's `local` driver keeps 100 MB of a container's log by default, in five \
             files of 20 MB, so a container that writes more loses the beginning of what is \
             read back of it, and nothing says so",
        ),
        "journald" => Kept::Part(
            "the journal drops lines past its rate limit (10,000 in 30 seconds by default, one \
             limit for all of a user's containers), so a container that writes faster loses \
             lines of what is read back of it, and nothing says so",
        ),
        "none" | "passthrough" | "passthrough-tty" => Kept::Nothing(
            "it keeps no log for the daemon to read back, so every Docker action's step output \
             would be empty, and so would what a container that failed before its task started \
             said, and a task's log and output where its container could not be followed",
        ),
        _ => Kept::Part(
            "what is read back of a container comes from the daemon, which has it only where \
             it keeps a copy of what it sends elsewhere",
        ),
    }
}

/// One of a log driver's options, where it is set to something.
fn option_of<'a>(
    options: &'a std::collections::HashMap<String, String>,
    key: &str,
) -> Option<&'a str> {
    options.get(key).map(|v| v.trim()).filter(|v| !v.is_empty())
}

/// The bound a log driver's options put on what it keeps of one container's
/// output, as `max-size=10m, max-file=3`; `None` where they put none.
///
/// A daemon's own options for a driver are given to every container of that
/// driver that sets none, and show among the container's settings. A file
/// that is rotated keeps the end of a long log and loses its beginning.
fn rotation_of(options: &std::collections::HashMap<String, String>) -> Option<String> {
    let size = option_of(options, "max-size").filter(|v| !matches!(*v, "0" | "-1"))?;
    Some(match option_of(options, "max-file") {
        Some(files) => format!("max-size={size}, max-file={files}"),
        None => format!("max-size={size}"),
    })
}

/// Where a log driver's options have the daemon not wait for the driver, as
/// `mode=non-blocking, max-buffer-size=1m`; `None` where it waits.
///
/// A daemon in this mode holds a container's output in a buffer for the
/// driver to take, and drops what arrives while the buffer is full. So a
/// task that writes faster than the driver takes it loses lines from
/// anywhere in its log, whatever the driver then keeps of the rest.
fn non_blocking_of(options: &std::collections::HashMap<String, String>) -> Option<String> {
    option_of(options, "mode").filter(|mode| *mode == "non-blocking")?;
    Some(match option_of(options, "max-buffer-size") {
        Some(size) => format!("mode=non-blocking, max-buffer-size={size}"),
        None => "mode=non-blocking".to_string(),
    })
}

/// A mount every task container on this engine gets.
///
/// The operator's, never a workflow's: a workflow that could name a host path
/// could read the host. That is why these come from the engine's environment
/// and why no task key reaches them. The local `dagron-ci run` sets them to
/// put the working tree into every task, at the path it has on the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMount {
    /// An absolute host path (a bind mount) or a volume's name.
    pub source: String,
    /// The absolute path in the container.
    pub target: String,
    pub read_only: bool,
}

impl TaskMount {
    fn to_docker(&self) -> Mount {
        let typ = if self.source.starts_with('/') { MountTypeEnum::BIND } else { MountTypeEnum::VOLUME };
        Mount {
            target: Some(self.target.clone()),
            source: Some(self.source.clone()),
            typ: Some(typ),
            read_only: Some(self.read_only),
            ..Default::default()
        }
    }
}

/// `DAGRON_DOCKER_MOUNTS`: `SOURCE:TARGET[:ro|rw]` entries, comma separated,
/// as `docker run -v` writes one. SOURCE is an absolute host path, bound as
/// it is (a missing one is Docker's error, not a directory it creates), or a
/// volume's name, which Docker creates on first use. TARGET is an absolute
/// path, each at most once.
///
/// Read once, at startup, and a bad entry is a startup error: every task
/// gets these, so one found wrong at dispatch would fail every task.
pub fn parse_mounts(raw: &str) -> std::result::Result<Vec<TaskMount>, String> {
    let mut mounts: Vec<TaskMount> = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let parts: Vec<&str> = entry.split(':').collect();
        let (source, target, read_only) = match parts.as_slice() {
            [source, target] => (*source, *target, false),
            [source, target, "ro"] => (*source, *target, true),
            [source, target, "rw"] => (*source, *target, false),
            [_, _, mode] => return Err(format!("{entry:?}: the mode is ro or rw, not {mode:?}")),
            _ => return Err(format!("{entry:?} is not SOURCE:TARGET or SOURCE:TARGET:ro")),
        };
        if !source.starts_with('/') && !is_volume_name(source) {
            return Err(format!(
                "{entry:?}: the source is an absolute host path or a volume's name \
                 (letters, digits, '_', '.', '-'), not {source:?}"
            ));
        }
        if !target.starts_with('/') || target == "/" {
            return Err(format!("{entry:?}: the target is an absolute path below /, not {target:?}"));
        }
        if mounts.iter().any(|m| m.target == target) {
            return Err(format!("{entry:?}: {target} is mounted twice"));
        }
        mounts.push(TaskMount { source: source.into(), target: target.into(), read_only });
    }
    Ok(mounts)
}

/// `DAGRON_DOCKER_WORKDIR`: an absolute path, or unset (empty) for the
/// image's own working directory.
pub fn parse_workdir(raw: &str) -> std::result::Result<Option<String>, String> {
    match raw.trim() {
        "" => Ok(None),
        dir if dir.starts_with('/') => Ok(Some(dir.to_string())),
        dir => Err(format!("the working directory is an absolute path, not {dir:?}")),
    }
}

/// Docker's rule for a volume name: `[a-zA-Z0-9][a-zA-Z0-9_.-]+`.
fn is_volume_name(s: &str) -> bool {
    let mut bytes = s.bytes();
    s.len() >= 2
        && bytes.next().is_some_and(|b| b.is_ascii_alphanumeric())
        && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// Docker executor — each task runs in a freshly created container.
///
/// The container is attached to and started, and waited on (with a hard
/// timeout) while what it writes is read into the task's log; then its output
/// is read to its end, and the container is force-removed whether the task
/// succeeds or times out: after the containers that mount its workspace, and,
/// where it made that workspace, only once they are seen gone (below). This
/// is the "spawnable worker in Docker container form" backend.
///
/// **A workspace task** gets an anonymous volume at [`WORKSPACE_DIR`] as its
/// working directory, unless the operator chose where tasks work (a working
/// directory, or a mount at [`WORKSPACE_DIR`]). With an exec image, an init
/// container from it first installs `dagron-ci-exec` into an anonymous volume
/// of its own, and makes the workspace's too: a new volume takes the mode of
/// the image's directory it is mounted over, and the image's `/workspace` is
/// writable by anyone, so a task whose image runs as another user can write
/// its workspace (a volume Docker makes for the task's own image is root's).
/// The task mounts both by name, the tools read-only. Anonymous, so that
/// removing the container that made them with its volumes (`docker rm -v`, as
/// every removal here does) leaves nothing. But only once nothing mounts
/// them: Docker skips a volume still in use, and never removes one with a
/// container that mounted it by name. So a container that made a volume
/// others of its attempt mount goes only once they are seen gone, on every
/// path (the end of a task, a timeout, a stop, the predecessor reap or the
/// fleet sweep), and is otherwise left for the next of them: the init
/// container after the rest of its attempt, and the task's own, where it
/// made the workspace (there is no init container), after its checkout's
/// and its steps', which mount the workspace by name.
///
/// **A checkout** is fetched into the workspace between the task container's
/// creation and its start, by a container of its own from the
/// [`ENV_CHECKOUT_IMAGE`] that mounts the workspace and alone carries the
/// repository's credential: it is seen removed before the task starts, and
/// one that is not fails the attempt; see [`crate::checkout`].
///
/// **Its actions** are fetched before the task's container is made, into an
/// anonymous volume at [`ACTIONS_DIR`] of a container of the same image that
/// holds it, made with no variables and never started. The task mounts it by
/// name and read-only, as it does its tools, and the holder goes after the
/// task's container, as the init container does. The fetch is a container of
/// its own that mounts the volume by name, and alone carries the actions'
/// credentials: it is removed before the task's container is made.
///
/// **Services** are containers made for the attempt before the task's and
/// started after its checkout, each seen ready before the next and all of
/// them before the task. They and the task share one network, the first
/// service's (`service_config` says how). Each carries the task's
/// labels, so the predecessor reap and the fleet sweep find it as they find
/// the task's container, and each is removed when the task ends, however it
/// ends.
pub struct DockerExecutor {
    /// Default image when a task does not specify `docker_image`.
    pub default_image: String,
    /// Mounted into every task container ([`ENV_MOUNTS`]).
    pub mounts: Vec<TaskMount>,
    /// Where every task container starts ([`ENV_WORKDIR`]); the image's own
    /// when `None`.
    pub workdir: Option<String>,
    /// The image whose entrypoint is `dagron-ci-exec`, installed into each
    /// workspace task ([`ENV_CI_EXEC_IMAGE`]); none when `None`.
    pub ci_exec_image: Option<String>,
    /// The image a workspace's checkout is fetched with
    /// ([`ENV_CHECKOUT_IMAGE`]); a task with a checkout fails without one.
    pub checkout_image: Option<String>,
    /// The log driver each container is made with ([`ENV_LOG_DRIVER`]).
    pub log_driver: LogDriver,
    /// The network each container is made on ([`ENV_NETWORK`]); the
    /// daemon's default when `None`.
    pub network: Option<String>,
    /// The allowlists the operator has made, by name
    /// (`dagron_core::isolation::ENV_NETWORK_ALLOWLISTS`), each an internal
    /// network of theirs ([`DockerExecutor::with_network_allowlists`]).
    allowlists: BTreeMap<String, Allowlist>,
    /// The runtime a task's Docker daemon is run on, unprivileged
    /// (`dagron_core::docker_daemon::ENV_RUNTIME`); privileged on the
    /// daemon's default when `None`.
    pub daemon_runtime: Option<String>,
    /// Whether the driver in use has been judged ([`kept_by`]): as the
    /// engine started, where the driver was known then, and otherwise at the
    /// first container made.
    log_driver_judged: Arc<AtomicBool>,
    /// Whether the first container made has been read for what the driver
    /// alone does not tell: the driver, where the daemon did not say, and
    /// the rotation the daemon gives it ([`rotation_of`]).
    first_container_judged: Arc<AtomicBool>,
    /// Whether a task's container could not be attached to, and that was
    /// said ([`DockerExecutor::attach`]).
    attach_refused: Arc<AtomicBool>,
    /// Whether a trusted run's step may have its image built from a
    /// Dockerfile here ([`ENV_BUILD_ACTIONS`]).
    pub build_actions: bool,
    /// Whether the daemon is Podman, as it says of itself. It differs from
    /// Docker in one thing a service meets
    /// ([`DockerExecutor::say_what_podman_leaves_out`]).
    podman: bool,
    /// Whether that difference has been said in the engine's log.
    podman_said: Arc<AtomicBool>,
    /// What each running task lends its steps' containers, by its task
    /// container's name: from the task's start to its attempt's end
    /// ([`Loan`]).
    lent: Loans,
    /// The id of each step's image this executor built, by its name: what
    /// one held under that name must still be, to be reused or run. Anyone
    /// with the daemon, or a pull, could put another image under the name.
    built: Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    /// Where images are pulled from, and with what
    /// ([`crate::images::ENV_MIRRORS`], [`crate::images::ENV_AUTH_FILE`]).
    registries: crate::images::Registries,
    /// One pull of an image at a time on this host: a lock per image, as it
    /// is pulled, that a second task wanting it waits on and then finds it
    /// held.
    /// Held weakly: a lock no pull holds is let go, so the map is only as
    /// large as the pulls going on.
    #[allow(clippy::type_complexity)]
    pulling: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>,
        >,
    >,
    /// Each run's tags, resolved once to the digest they were first pulled
    /// or found at: the run's later tasks are given that image, whatever the
    /// tag has moved to since.
    pins: Arc<std::sync::Mutex<Pins>>,
    /// Pulls and cache hits are counted here where the engine gave one.
    metrics: Option<Arc<dagron_core::metrics::Metrics>>,
    /// Whether a task may ask for a privileged container here
    /// (`dagron_core::privileged::ENV`).
    privileged: bool,
    docker: Docker,
}

/// How many runs' pins are kept at most, a backstop for runs this engine
/// does not see end (another replica ended them): past it the run least
/// recently given an image goes first. A run's pins are dropped as this
/// engine ends it ([`Executor::forget_run`]).
const PINNED_RUNS_KEPT: usize = 16_384;

/// Each run's images, pinned ([`DockerExecutor::pins`]): by run, each with
/// the tick it was last given one at.
#[derive(Debug, Default)]
struct Pins {
    runs: std::collections::HashMap<String, (u64, std::collections::HashMap<String, String>)>,
    tick: u64,
}

impl Pins {
    fn get(&mut self, run: &str, image: &str) -> Option<String> {
        self.tick += 1;
        let tick = self.tick;
        let (used, pins) = self.runs.get_mut(run)?;
        *used = tick;
        pins.get(image).cloned()
    }

    fn put(&mut self, run: &str, image: &str, pin: String) {
        self.tick += 1;
        let tick = self.tick;
        let entry = self.runs.entry(run.to_string()).or_default();
        entry.0 = tick;
        entry.1.insert(image.to_string(), pin);
        if self.runs.len() > PINNED_RUNS_KEPT {
            let oldest = self
                .runs
                .iter()
                .min_by_key(|(_, (used, _))| *used)
                .map(|(run, _)| run.clone());
            if let Some(oldest) = oldest {
                self.runs.remove(&oldest);
            }
        }
    }

    fn forget(&mut self, run: &str) {
        self.runs.remove(run);
    }
}

/// The run a task is of, where it says.
fn run_of(ctx: &ExecContext) -> Option<&str> {
    ctx.identity.as_ref().map(|i| i.run_id.as_str())
}

/// What a running task lends the containers of its steps
/// ([`Executor::run_step_container`]), from its start to its end: the task's
/// workspace, at its own place and at [`STEP_WORKSPACE_DIR`], its network,
/// and what it is held to.
///
/// Kept under its task container's name until the attempt is over
/// ([`Loan`]), and past the task's end, which ends it (`ended`): a step's
/// container not seen removed is asked for again from it, by the attempt's
/// end or by the stop, which finds it whatever became of the attempt.
#[derive(Debug, Clone)]
struct Lent {
    /// The task's id, which its steps ask by.
    task_id: String,
    /// The fence of the claim whose attempt lent it: of a task's loans here,
    /// the latest claim's is the one its steps are lent.
    fence: i64,
    host: HostConfig,
    user: Option<String>,
    labels: Option<std::collections::HashMap<String, String>>,
    isolation: Option<dagron_core::isolation::IsolationSpec>,
    pull: Option<ImagePullPolicy>,
    /// The task's run, whose images a step's are pinned with.
    run: Option<String>,
    /// Whether the task's run is trusted: only then is the engine's
    /// credential sent with a pull of a step's image.
    trusted: bool,
    /// What the task's allowlist tells each of its containers, before what
    /// a step sets itself: its proxy, where it has one.
    env: Vec<String>,
    /// How many step containers it has made, for their names.
    made: u32,
    /// Those not yet seen removed.
    running: Vec<String>,
    /// Whether the loan has ended, as the task did or as a later attempt of
    /// it lent its own: no step's container is made for it from then on,
    /// and its steps ask in vain.
    ended: bool,
    /// Held shared by each request that makes a step's container, to the
    /// request's end and no further, and taken whole by
    /// [`DockerExecutor::take_back`] before it looks for the steps'
    /// containers: one that was being made is there for it to remove.
    steps: Arc<tokio::sync::RwLock<()>>,
}

/// The loans of the tasks running here ([`Lent`]), by their task containers'
/// names.
type Loans = Arc<std::sync::Mutex<std::collections::HashMap<String, Lent>>>;

/// An attempt's hold on its task's loan: what is lent under its task
/// container's name, `.1`, is let go when the attempt is over, however it
/// ends (an execution dropped from outside included), and not before, so
/// that the stop still finds it. Only that name's: a later attempt's loan
/// is its own.
struct Loan(Loans, String);

impl Drop for Loan {
    fn drop(&mut self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.1);
    }
}

/// An allowlist of the operator's, as this executor holds a task to it: an
/// internal network of theirs, which reaches nothing but what they have put
/// on it, and the proxy they say it reaches the rest through, where they say
/// one ([`dagron_core::isolation::ALLOWLIST_PROXY`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Allowlist {
    network: String,
    proxy: Option<String>,
}

/// The volumes an init container made, by name, for the task to mount: its
/// tools, and the workspace when the task has one of its making.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InitVolumes {
    tools: String,
    workspace: Option<String>,
}

impl DockerExecutor {
    /// Connect to the local Docker daemon and verify reachability.
    pub async fn connect(default_image: impl Into<String>) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| anyhow::anyhow!("Docker connect: {e}"))?;
        Self::connect_with(docker, default_image).await
    }

    /// [`Self::connect`], through a client of the caller's: the daemon it
    /// reaches is asked whether it answers, and whether it is Podman.
    pub async fn connect_with(docker: Docker, default_image: impl Into<String>) -> Result<Self> {
        docker
            .ping()
            .await
            .map_err(|e| anyhow::anyhow!("Docker daemon unreachable: {e}"))?;
        // A daemon that will not say what it is is taken for Docker.
        let podman = docker.version().await.is_ok_and(|version| {
            version
                .components
                .iter()
                .flatten()
                .any(|component| component.name.contains("Podman"))
        });
        Ok(Self {
            default_image: default_image.into(),
            mounts: Vec::new(),
            workdir: None,
            ci_exec_image: None,
            checkout_image: None,
            log_driver: LogDriver::default(),
            network: None,
            allowlists: BTreeMap::new(),
            daemon_runtime: None,
            build_actions: false,
            log_driver_judged: Arc::default(),
            first_container_judged: Arc::default(),
            attach_refused: Arc::default(),
            podman,
            podman_said: Arc::default(),
            lent: Arc::default(),
            built: Arc::default(),
            registries: crate::images::Registries::default(),
            pulling: Arc::default(),
            pins: Arc::default(),
            metrics: None,
            privileged: false,
            docker,
        })
    }

    /// Pull through these mirrors, with these credentials. Call
    /// it before [`DockerExecutor::with_ci_exec_image`] and
    /// [`DockerExecutor::with_checkout_image`], whose images it applies to.
    pub fn with_registries(mut self, registries: crate::images::Registries) -> Self {
        self.registries = registries;
        self
    }

    /// Count pulls and cache hits in the engine's metrics.
    pub fn with_metrics(mut self, metrics: Arc<dagron_core::metrics::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Run a task's Docker daemon on `runtime`, unprivileged
    /// (`dagron_core::docker_daemon::ENV_RUNTIME`), asked of the daemon
    /// now: a runtime it does not have would fail every such task where its
    /// daemon is made, so it stops the engine here instead.
    pub async fn with_daemon_runtime(mut self, runtime: Option<String>) -> Result<Self> {
        if let Some(name) = &runtime {
            let info = self
                .docker
                .info()
                .await
                .map_err(|e| anyhow::anyhow!("Docker info: {e}"))?;
            let has = info.runtimes.as_ref().is_some_and(|r| r.contains_key(name));
            if !has {
                let known: Vec<String> = info
                    .runtimes
                    .map(|r| r.into_keys().collect())
                    .unwrap_or_default();
                anyhow::bail!(
                    "{}={name}: this daemon has no such runtime (it has: {}). Install it on \
                     the engine's host and register it with the daemon, or leave the variable \
                     unset to run a task's daemon privileged",
                    dagron_core::docker_daemon::ENV_RUNTIME,
                    if known.is_empty() {
                        "none it names".to_string()
                    } else {
                        known.join(", ")
                    }
                );
            }
        }
        self.daemon_runtime = runtime;
        Ok(self)
    }

    /// Make every container on `network` ([`ENV_NETWORK`]), asked of the
    /// daemon now: a network it does not have would fail every task where its
    /// container is made, so it stops the engine here instead.
    pub async fn with_network(mut self, network: Option<String>) -> Result<Self> {
        if let Some(name) = &network {
            if !BUILT_IN_NETWORKS.contains(&name.as_str()) {
                self.docker
                    .inspect_network::<String>(name, None)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "{ENV_NETWORK}={name}: this daemon has no such network ({e}). \
                             Make it first (`docker network create {name}`), or name one it has"
                        )
                    })?;
            }
            if name == NO_NETWORK {
                tracing::warn!(
                    "{ENV_NETWORK}={NO_NETWORK}: no container this executor makes has a \
                     network, the one that fetches a checkout among them, so a task with a \
                     checkout fails where it is fetched"
                );
            }
        }
        self.network = network;
        Ok(self)
    }

    /// Hold a task that names one of `names` (`isolation.network:
    /// allow:<name>`) to the operator's network for it, each read of the
    /// daemon now: a task cannot write what an allowlist allows, so it is a
    /// network of theirs, `dagron-task-allow-<name>`
    /// ([`dagron_core::isolation::allowlist_object_name`]), made
    /// `--internal`. An internal network has no way out of the host: a
    /// container on it reaches the others on it and nothing else, which is
    /// what the operator puts there (a registry, a mirror, a proxy). One that
    /// is missing, or is not internal, stops the engine here: a task on a
    /// network with a way out would reach whatever the host does, believing
    /// it held to a list.
    ///
    /// A network that says a proxy of the operator's
    /// ([`dagron_core::isolation::ALLOWLIST_PROXY`]) has each task held to
    /// it told of the proxy, which is what holds it to a list of hostnames.
    pub async fn with_network_allowlists(mut self, names: Vec<String>) -> Result<Self> {
        use dagron_core::isolation::{allowlist_object_name, ENV_NETWORK_ALLOWLISTS};
        let mut allowlists = BTreeMap::new();
        for name in names {
            let network = allowlist_object_name(&name);
            let proxy = self.read_allowlist(&network).await.map_err(|why| {
                anyhow::anyhow!("{ENV_NETWORK_ALLOWLISTS} names '{name}', and {why}")
            })?;
            tracing::info!(
                %network,
                proxy = proxy.as_deref().unwrap_or("-"),
                "a task that names the allowlist '{name}' is on this internal network. What \
                 it reaches is the operator's"
            );
            allowlists.insert(name, Allowlist { network, proxy });
        }
        self.allowlists = allowlists;
        Ok(self)
    }

    /// Read an allowlist's network as it is now: that it is there, internal,
    /// and a bridge the host has no address on ([`INHIBIT_IPV4`]), and the
    /// proxy it says, where it says one.
    async fn read_allowlist(&self, network: &str) -> std::result::Result<Option<String>, String> {
        let made = format!(
            "Make it with `docker network create --internal -o {INHIBIT_IPV4}=true {network}`, \
             and put on it what the allowlist allows"
        );
        let found = self
            .docker
            .inspect_network::<String>(network, None)
            .await
            .map_err(|e| format!("this daemon has no network {network} ({e}). {made}"))?;
        if found.internal != Some(true) {
            return Err(format!(
                "the network {network} is not internal: a container on it reaches what the \
                 host does, and is held to no list. {made}"
            ));
        }
        // Internal is no route off the host; the host itself is another
        // matter. A bridge with an address of the host's on it is a way to
        // whatever the host serves on that address (its daemon's ports, the
        // engine's own), so the bridge has none.
        let driver = found.driver.as_deref().unwrap_or("bridge");
        let inhibited = found
            .options
            .as_ref()
            .and_then(|options| options.get(INHIBIT_IPV4))
            .is_some_and(|v| v == "true");
        let why = if driver != "bridge" {
            Some(format!("is of the {driver} driver, of which nothing here says what reaches the host"))
        } else if !inhibited {
            Some(format!("gives the host an address on it ({INHIBIT_IPV4} is not true)"))
        } else if found.enable_ipv6 == Some(true) {
            Some("gives the host an IPv6 address on it".to_string())
        } else {
            None
        };
        if let Some(why) = why {
            return Err(format!(
                "the network {network} {why}: a container on it reaches what the host serves \
                 there, and is held to no list. {made}"
            ));
        }
        match found
            .labels
            .as_ref()
            .and_then(|labels| labels.get(dagron_core::isolation::ALLOWLIST_PROXY))
        {
            Some(proxy) => dagron_core::isolation::parse_allowlist_proxy(proxy)
                .map(Some)
                .map_err(|e| {
                    format!(
                        "the network {network}'s label {}: {e}",
                        dagron_core::isolation::ALLOWLIST_PROXY
                    )
                }),
            None => Ok(None),
        }
    }

    /// The allowlist a task asked for, read of the daemon again before
    /// anything is made for it: a network removed, or made again without
    /// `--internal`, since the engine started is no allowlist.
    async fn held_to(&self, name: &str) -> Result<()> {
        let Some(allowlist) = self.allowlists.get(name) else {
            anyhow::bail!(
                "this task asked for the allowlist '{name}' (isolation.network: allow:{name}), \
                 and this executor's operator has made no such allowlist: they name the ones \
                 they make in {}",
                dagron_core::isolation::ENV_NETWORK_ALLOWLISTS
            );
        };
        self.read_allowlist(&allowlist.network)
            .await
            .map(|_| ())
            .map_err(|why| {
                anyhow::anyhow!(
                    "this task asked for the allowlist '{name}', and {why}, so it is not run"
                )
            })
    }

    /// The allowlist `ctx`'s task asked for, where this executor has it.
    fn allowlist_of(&self, ctx: &ExecContext) -> Option<&Allowlist> {
        let name = ctx.isolation.as_ref()?.network.as_ref()?.allowlist()?;
        self.allowlists.get(name)
    }

    /// What a task held to an allowlist with a proxy, and each container of
    /// its, is told of it ([`dagron_core::isolation::proxy_env`]): each
    /// container takes out what it sets itself, where its environment is made.
    fn proxy_env(&self, ctx: &ExecContext) -> Vec<(String, String)> {
        let Some(proxy) = self.allowlist_of(ctx).and_then(|a| a.proxy.as_deref()) else {
            return Vec::new();
        };
        dagron_core::isolation::proxy_env(proxy, ctx.services.iter().map(|s| s.name.as_str()))
    }

    /// Make every container with `driver` ([`ENV_LOG_DRIVER`]), judged now,
    /// whether it is one the operator named or the daemon's own, which the
    /// daemon is asked for. A driver that keeps no log for the daemon to read
    /// back stops the engine here, since every Docker action's step output,
    /// what a container that failed before its task started said, and a
    /// task's log and output where its container could not be followed, would
    /// be empty; one that may not keep it all is said once, with what it costs
    /// and what would keep it; and a file that keeps it all is said too where
    /// the operator chose it, since nothing bounds it.
    pub async fn with_log_driver(mut self, driver: LogDriver) -> Result<Self> {
        let named = matches!(driver, LogDriver::Named(_));
        let used = match &driver {
            LogDriver::Named(name) => Some(name.clone()),
            LogDriver::Daemons => self
                .docker
                .info()
                .await
                .map_err(|e| anyhow::anyhow!("Docker info: {e}"))?
                .logging_driver
                .filter(|name| !name.is_empty()),
        };
        match used.as_deref().map(|name| (name, kept_by(name))) {
            Some((name, Kept::Whole)) if named => tracing::info!(
                log_driver = %name,
                "each container keeps its log in a file on the daemon's host until the \
                 container is removed: nothing bounds it but the daemon's own options for this \
                 driver"
            ),
            Some((_, Kept::Whole)) => {}
            Some((name, Kept::Part(why))) => tracing::warn!(
                log_driver = %name,
                "what is read back of a container may not be whole with this log driver: \
                 {why}. {READ_BACK} {ENV_LOG_DRIVER}={FILE_LOG_DRIVER} has each container keep \
                 its log in a file, which the daemon reads back whole and nothing bounds"
            ),
            Some((name, Kept::Nothing(why))) => anyhow::bail!(
                "the log driver {name:?} cannot be used for tasks: {why}. \
                 {ENV_LOG_DRIVER}={FILE_LOG_DRIVER} has each container keep its log in a file"
            ),
            // A daemon need not say (Podman's does not, through this API).
            // A container does: the first one made is asked.
            None => tracing::debug!(
                "the daemon does not say which log driver its containers get: the first \
                 container made will"
            ),
        }
        self.log_driver_judged
            .store(used.is_some(), Ordering::SeqCst);
        self.log_driver = driver;
        Ok(self)
    }

    /// Read the first container made, `name`, for what could not be known as
    /// the engine started.
    ///
    /// Which driver it has, where the daemon was left to choose and did not
    /// say what it chooses. Too late to stop the engine by then, so a driver
    /// that keeps nothing is said as loudly as a log can. And the options
    /// its driver was given, which a daemon does not say: a file that would
    /// keep a whole log keeps only the end of a long one where the daemon
    /// rotates it, and no driver is given what the daemon dropped because it
    /// does not wait for one.
    async fn judge_first_container(&self, name: &str) {
        if self.first_container_judged.swap(true, Ordering::SeqCst) {
            return;
        }
        let log_config = match self.docker.inspect_container(name, None).await {
            Ok(made) => made.host_config.and_then(|h| h.log_config),
            Err(e) => {
                // Not learnt this time: the next container made is asked.
                self.first_container_judged.store(false, Ordering::SeqCst);
                tracing::debug!(container = %name, error = %e, "could not read the container's log settings");
                return;
            }
        };
        let (driver, options) = match log_config {
            Some(l) => (
                l.typ.filter(|driver| !driver.is_empty()),
                l.config.unwrap_or_default(),
            ),
            None => (None, Default::default()),
        };
        let kept = driver.as_deref().map(kept_by);
        if !self.log_driver_judged.swap(true, Ordering::SeqCst) {
            match (driver.as_deref(), &kept) {
                (Some(_), Some(Kept::Whole)) => {}
                (Some(driver), Some(Kept::Part(why))) => tracing::warn!(
                    log_driver = %driver,
                    "what is read back of a container may not be whole with this log driver: \
                     {why}. {READ_BACK} {ENV_LOG_DRIVER}={FILE_LOG_DRIVER} has each container \
                     keep its log in a file, which the daemon reads back whole and nothing \
                     bounds"
                ),
                (Some(driver), Some(Kept::Nothing(why))) => tracing::error!(
                    log_driver = %driver,
                    "the daemon's log driver cannot be used for tasks: {why}. \
                     {ENV_LOG_DRIVER}={FILE_LOG_DRIVER} has each container keep its log in a \
                     file"
                ),
                _ => tracing::warn!(
                    "neither the daemon nor its containers say which log driver they have: what \
                     is read back of a container is whatever that driver keeps. {READ_BACK}"
                ),
            }
        }
        if let (Some(driver), Some(Kept::Whole), Some(rotation)) =
            (driver.as_deref(), &kept, rotation_of(&options))
        {
            tracing::warn!(
                log_driver = %driver,
                "the daemon rotates each container's log ({rotation}), so a container that \
                 writes more than that keeps the end of what is read back of it and loses the \
                 beginning, and nothing says so. {READ_BACK} The bound is the daemon's own \
                 option for this driver, and is raised there"
            );
        }
        // Whatever the driver keeps, it is not given what the daemon dropped.
        // A driver that keeps nothing has been said already, and louder.
        let read_back = !matches!(kept, Some(Kept::Nothing(_)));
        if let Some(delivery) = non_blocking_of(&options).filter(|_| read_back) {
            tracing::warn!(
                log_driver = %driver.as_deref().unwrap_or("unnamed"),
                "the daemon does not wait for its log driver ({delivery}): what a container \
                 writes while the daemon's buffer is full is dropped, so what is read back of \
                 one that writes fast loses lines from anywhere in it, and nothing says so. \
                 {READ_BACK} The mode is the daemon's own option for this driver, and \
                 `mode=blocking` there ends this loss, though not what the driver itself may \
                 lose"
            );
        }
    }

    /// How each container's log is kept: by the driver this executor names,
    /// or, where it names none, by the daemon's own.
    fn log_config(&self) -> Option<HostConfigLogConfig> {
        match &self.log_driver {
            LogDriver::Named(name) => Some(HostConfigLogConfig {
                typ: Some(name.clone()),
                config: None,
            }),
            LogDriver::Daemons => None,
        }
    }

    /// What every container this executor makes is made with, but for its
    /// mounts: kept until its log is read, the operator's log driver and
    /// network, and what the task's envelope asks of a container.
    ///
    /// A task that asked for no network has none in its own container, and
    /// one held to an allowlist is on the operator's network for it. What is
    /// run for it first is the operator's, on the operator's network: a
    /// checkout could not be fetched otherwise.
    ///
    /// A task of a run that is not trusted is held to limits
    /// ([`crate::executor::Confinement`]), in each container made for it:
    /// what is run for it first unpacks what its author wrote.
    fn host_config(&self, ctx: &ExecContext, made: Made, mounts: Vec<Mount>) -> HostConfig {
        use dagron_core::isolation::Network;
        let hardening = Hardening::of(ctx);
        let asked = match made {
            Made::Task => ctx.isolation.as_ref().and_then(|iso| iso.network.as_ref()),
            Made::ForIt => None,
        };
        // A task held to an allowlist is on the operator's network for it.
        // One this executor does not have is refused before anything is
        // made; were it not, the task would have no network at all.
        let network_mode = match asked {
            Some(Network::None) => Some(NO_NETWORK.to_string()),
            Some(Network::Allow(name)) => Some(
                self.allowlists
                    .get(name)
                    .map_or_else(|| NO_NETWORK.to_string(), |a| a.network.clone()),
            ),
            Some(Network::Default) | None => self.network.clone(),
        };
        HostConfig {
            auto_remove: Some(false), // we remove manually to capture logs first
            mounts: if mounts.is_empty() {
                None
            } else {
                Some(mounts)
            },
            log_config: self.log_config(),
            network_mode,
            readonly_rootfs: hardening.read_only_root_fs.then_some(true),
            cap_drop: hardening
                .drop_all_capabilities
                .then(|| vec!["ALL".to_string()]),
            security_opt: if hardening.security_opt.is_empty() {
                None
            } else {
                Some(hardening.security_opt)
            },
            memory: ctx.confinement.map(|held| held.memory_bytes),
            // The same again as the whole of memory and swap: no swap.
            memory_swap: ctx.confinement.map(|held| held.memory_bytes),
            pids_limit: ctx.confinement.map(|held| held.pids),
            nano_cpus: ctx.confinement.map(|held| held.nano_cpus),
            ..Default::default()
        }
    }

    /// Refuse a task that asked not to run as root where nothing says it
    /// will not: it named no uid, and its image's `USER` is root, or is a
    /// name. A daemon has no setting for this, so it is read from the image,
    /// which is on the host by now. `what` says whose image it is: the
    /// task's, or one of its services', which are held as the task is.
    /// `named` is the image as the task wrote it, which a refusal names;
    /// `held`, the one it is given (through its mirror, at its run's
    /// digest), which is read.
    async fn refuse_a_root_image(
        &self,
        ctx: &ExecContext,
        what: &str,
        named: &str,
        held: &str,
    ) -> Result<()> {
        self.refuse_root(ctx.isolation.as_ref(), what, named, held)
            .await
    }

    /// [`Self::refuse_a_root_image`], for what a task is held to.
    async fn refuse_root(
        &self,
        isolation: Option<&dagron_core::isolation::IsolationSpec>,
        what: &str,
        image: &str,
        held: &str,
    ) -> Result<()> {
        let Some(iso) = isolation else {
            return Ok(());
        };
        if iso.run_as_non_root != Some(true) || iso.run_as_user.is_some() {
            return Ok(());
        }
        let user = self
            .docker
            .inspect_image(held)
            .await
            .map_err(|e| anyhow::anyhow!("{what} {image} could not be read: {e}"))?
            .config
            .and_then(|c| c.user)
            .unwrap_or_default();
        stated_non_root(&user).map_err(|why| {
            anyhow::anyhow!(
                "isolation.run_as_non_root: {what} {image} {why}. Give the task a uid \
                 (isolation.run_as_user), or an image whose USER is a number other than 0"
            )
        })
    }

    /// Let a trusted run's step have its image built from a Dockerfile here
    /// ([`ENV_BUILD_ACTIONS`]).
    pub fn with_build_actions(mut self, allow: bool) -> Self {
        self.build_actions = allow;
        self
    }

    /// The mounts and working directory every task container gets.
    pub fn with_task_mounts(mut self, mounts: Vec<TaskMount>, workdir: Option<String>) -> Self {
        self.mounts = mounts;
        self.workdir = workdir;
        self
    }

    /// Install `dagron-ci-exec` into every workspace task from `image`
    /// ([`ENV_CI_EXEC_IMAGE`]), pulled now if this daemon lacks it, so that an
    /// image that cannot be had is a startup error rather than every workspace
    /// task's. Call it after [`DockerExecutor::with_task_mounts`]: an operator
    /// mount over [`TOOLS_DIR`] is refused.
    pub async fn with_ci_exec_image(mut self, image: Option<String>) -> Result<Self> {
        let image = image.map(|i| self.registries.mirrored(&i));
        if let Some(image) = &image {
            let tools = format!("{TOOLS_DIR}/");
            if let Some(m) = self.mounts.iter().find(|m| m.target == TOOLS_DIR || m.target.starts_with(&tools)) {
                anyhow::bail!(
                    "{ENV_MOUNTS} mounts {} where {ENV_CI_EXEC_IMAGE} installs dagron-ci-exec \
                     ({TOOLS_DIR}); set one of them",
                    m.target
                );
            }
            self.pull_if_missing(image)
                .await
                .map_err(|e| anyhow::anyhow!("{ENV_CI_EXEC_IMAGE}={image}: {e}"))?;
        }
        self.ci_exec_image = image;
        Ok(self)
    }

    /// Fetch each workspace's checkout with `image` ([`ENV_CHECKOUT_IMAGE`]),
    /// pulled now if this daemon lacks it, for the reason
    /// [`DockerExecutor::with_ci_exec_image`] pulls its image now.
    pub async fn with_checkout_image(mut self, image: Option<String>) -> Result<Self> {
        let image = image.map(|i| self.registries.mirrored(&i));
        if let Some(image) = &image {
            self.pull_if_missing(image)
                .await
                .map_err(|e| anyhow::anyhow!("{ENV_CHECKOUT_IMAGE}={image}: {e}"))?;
        }
        self.checkout_image = image;
        Ok(self)
    }

    /// One of the engine's own images (the tools', the checkout's), pulled
    /// with its credential where the host lacks it.
    async fn pull_if_missing(&self, image: &str) -> Result<()> {
        if self.holds(image).await {
            return Ok(());
        }
        self.timed_pull(image, true).await
    }

    /// Run a task that asks to be privileged (`privileged: true`) in a
    /// privileged container, where `allow`: the operator's word, which the
    /// engine has checked is a dedicated pool's
    /// (`dagron_core::privileged::engine_refusal`). Without it such a task
    /// is refused before anything is made.
    pub fn with_privileged(mut self, allow: bool) -> Self {
        self.privileged = allow;
        self
    }

    /// [`Self::pull`], timed and counted, with the engine's credential for
    /// the image's registry where `trusted`.
    async fn timed_pull(&self, image: &str, trusted: bool) -> Result<()> {
        let credentials = trusted
            .then(|| self.registries.credentials(image))
            .flatten();
        let started = std::time::Instant::now();
        let pulled = self.pull(image, credentials).await;
        if let Some(metrics) = &self.metrics {
            metrics.observe_image_pull(started.elapsed().as_secs_f64(), pulled.is_ok());
        }
        pulled
    }

    fn hit(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.inc_image_cache_hits();
        }
    }

    /// The lock that one pull of `image` at a time on this host is held to.
    fn gate(&self, image: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut pulling = self.pulling.lock().unwrap();
        pulling.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = pulling.get(image).and_then(std::sync::Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        pulling.insert(image.to_string(), Arc::downgrade(&gate));
        gate
    }

    /// The image a task of `run` is given for `image`: through its mirror,
    /// and at its run's digest where one was resolved. Asks nothing of the
    /// daemon: [`Self::fetch_image`] resolved it, as the attempt began.
    fn resolved(&self, run: Option<&str>, image: &str) -> String {
        let wanted = self.registries.mirrored(image);
        run.and_then(|run| self.pins.lock().unwrap().get(run, &wanted))
            .unwrap_or(wanted)
    }

    /// Whether the daemon holds `image`.
    async fn holds(&self, image: &str) -> bool {
        self.docker.inspect_image(image).await.is_ok()
    }

    /// Pull `image`, to the end of the daemon's stream: an error in it is the
    /// registry's or the daemon's own words. A task's `timeout_secs` does not
    /// bound it, since that is for the task's work; [`IMAGE_PULL_STALL`] does,
    /// between one word from the daemon and the next.
    async fn pull(
        &self,
        image: &str,
        credentials: Option<bollard::auth::DockerCredentials>,
    ) -> Result<()> {
        let mut pull = self.docker.create_image(
            Some(CreateImageOptions { from_image: image, ..Default::default() }),
            None,
            credentials,
        );
        loop {
            match timeout(IMAGE_PULL_STALL, pull.next()).await {
                Ok(Some(step)) => {
                    step.map_err(|e| anyhow::anyhow!("the image could not be pulled: {e}"))?;
                }
                Ok(None) => return Ok(()),
                Err(_) => anyhow::bail!(
                    "the image could not be pulled: the daemon said nothing of it for {} s",
                    IMAGE_PULL_STALL.as_secs()
                ),
            }
        }
    }

    /// A run's image at the digest its run resolved it to: had already, or
    /// pulled by that digest where the host has lost it since.
    async fn fetch_pinned(
        &self,
        what: &str,
        pin: String,
        policy: ImagePullPolicy,
        trusted: bool,
    ) -> Result<String> {
        let gate = self.gate(&pin);
        let _one = gate.lock().await;
        if self.holds(&pin).await {
            self.hit();
        } else if policy == ImagePullPolicy::Never {
            anyhow::bail!(
                "this Docker host does not hold {what} {pin}, and its image_pull_policy is never"
            );
        } else {
            self.timed_pull(&pin, trusted)
                .await
                .map_err(|e| anyhow::anyhow!("{what} {pin}: {e}"))?;
        }
        Ok(pin)
    }

    /// Have a task's `image` on the host before its container is made, as its
    /// `image_pull_policy` says. A task that states none gets
    /// [`ImagePullPolicy::IfNotPresent`]: an image the host lacks is pulled,
    /// which is what the engine's own two images always got.
    ///
    /// The Docker API pulls nothing when a container is made, as `docker run`
    /// does, so a task that named an image the host lacked failed with the
    /// daemon's "No such image" unless someone had pulled it by hand.
    ///
    /// An image is pulled through its registry's mirror, one pull of it at a
    /// time on this host, with the engine's credential for its registry
    /// where the run is `trusted`: a run that is not trusted
    /// pulls with none, and pulls whatever its policy says, so that it cannot
    /// name a private image the engine may read, neither from the registry
    /// nor from what this host holds. The first of a run's tasks to want a tag resolves it to the
    /// digest the host then holds; the run's later tasks are given that
    /// digest, and pull nothing for it, whatever their policy.
    ///
    /// A service's image is fetched the same, under its task's policy.
    /// `what` says whose image it is. What comes back is the image to make
    /// the container with ([`Self::resolved`] says the same).
    async fn fetch_image(
        &self,
        what: &str,
        image: &str,
        policy: Option<ImagePullPolicy>,
        run: Option<&str>,
        trusted: bool,
    ) -> Result<String> {
        let wanted = self.registries.mirrored(image);
        // A run that is not trusted has its images pulled, as no one,
        // whatever its policy says: one the host holds may be a private image
        // a trusted run pulled with the engine's credential, which the
        // registry is to refuse it, not the host's cache to give it. Its run
        // pulls each once, then has it at the digest it was given.
        let policy = match trusted {
            true => policy.unwrap_or(ImagePullPolicy::IfNotPresent),
            false => ImagePullPolicy::Always,
        };
        let never = |held: &str| {
            anyhow::anyhow!(
                "this Docker host does not hold {what} {held}, and its image_pull_policy is never"
            )
        };
        let pin_of = || run.and_then(|run| self.pins.lock().unwrap().get(run, &wanted));
        if let Some(pin) = pin_of() {
            return self.fetch_pinned(what, pin, policy, trusted).await;
        }
        {
            let gate = self.gate(&wanted);
            let one = gate.lock().await;
            // Another of the run's tasks may have resolved it while this one
            // waited its turn: the run's image is that one's, pulled once.
            if let Some(pin) = pin_of() {
                drop(one);
                return self.fetch_pinned(what, pin, policy, trusted).await;
            }
            let pulled = match policy {
                ImagePullPolicy::IfNotPresent if self.holds(&wanted).await => {
                    self.hit();
                    Ok(())
                }
                ImagePullPolicy::IfNotPresent | ImagePullPolicy::Always => {
                    self.timed_pull(&wanted, trusted).await
                }
                ImagePullPolicy::Never if self.holds(&wanted).await => {
                    self.hit();
                    Ok(())
                }
                ImagePullPolicy::Never => return Err(never(&wanted)),
            };
            pulled.map_err(|e| anyhow::anyhow!("{what} {wanted}: {e}"))?;
            let Some(run) = run else {
                return Ok(wanted);
            };
            // Pinned before the lock is let go: a task of the run that waited
            // on it finds the pin, and pulls nothing.
            let digests = match self.docker.inspect_image(&wanted).await {
                Ok(info) => info.repo_digests.unwrap_or_default(),
                Err(_) => Vec::new(),
            };
            let pin = crate::images::pinned(&wanted, &digests);
            if let Some(pin) = &pin {
                self.pins.lock().unwrap().put(run, &wanted, pin.clone());
            }
            drop(one);
            Ok(pin.unwrap_or(wanted))
        }
    }

    /// Whether a workspace task needs a volume made for it: not where the
    /// operator chose where tasks work, with a working directory or a mount
    /// at [`WORKSPACE_DIR`].
    fn makes_workspace(&self, ctx: &ExecContext) -> bool {
        ctx.workspace.is_some()
            && self.workdir_for(ctx).is_none()
            && !self
                .mounts_for(ctx)
                .iter()
                .any(|m| m.target == WORKSPACE_DIR)
    }

    /// The operator's mounts, for the tasks they are given to: every task
    /// but one of a run that is not trusted. A mount is the host's, or a
    /// volume other tasks share, and may be the daemon's own socket; such a
    /// task's code is a stranger's.
    fn mounts_for(&self, ctx: &ExecContext) -> &[TaskMount] {
        match ctx.confinement {
            Some(_) => &[],
            None => &self.mounts,
        }
    }

    /// The operator's working directory, likewise: it names a place in what
    /// they mounted.
    fn workdir_for(&self, ctx: &ExecContext) -> Option<&String> {
        match ctx.confinement {
            Some(_) => None,
            None => self.workdir.as_ref(),
        }
    }

    /// The container a task runs in: its image, command and environment (and
    /// `extra_env` after it), this engine's mounts and working directory, its
    /// workspace, the volumes of its init container (the tools read-only),
    /// the volume its actions were fetched into (`actions`, read-only), and
    /// its identity labels.
    fn container_config(
        &self,
        ctx: &ExecContext,
        image: &str,
        init: Option<&InitVolumes>,
        actions: Option<&str>,
        extra_env: &[(String, String)],
    ) -> Config<String> {
        let env: Vec<String> = self
            .proxy_env(ctx)
            .iter()
            .filter(|(k, _)| !ctx.env.iter().any(|e| &e.name == k))
            .map(|(k, v)| format!("{k}={v}"))
            .chain(ctx.env.iter().map(|e| format!("{}={}", e.name, e.value)))
            .chain(extra_env.iter().map(|(k, v)| format!("{k}={v}")))
            .collect();
        let mut mounts: Vec<Mount> = self
            .mounts_for(ctx)
            .iter()
            .map(TaskMount::to_docker)
            .collect();
        if self.makes_workspace(ctx) {
            mounts.push(match init.and_then(|i| i.workspace.as_deref()) {
                Some(volume) => named_volume(volume, WORKSPACE_DIR, false),
                None => anonymous_volume(WORKSPACE_DIR),
            });
        }
        if let Some(init) = init {
            mounts.push(named_volume(&init.tools, TOOLS_DIR, true));
        }
        // Its actions, in a volume of their own that a container of the
        // checkout's image made and filled before this one was made, and that
        // it reads alone. By name, as the tools are: Docker before 29 refuses
        // a volume made for this container that it may only read.
        if let Some(volume) = actions {
            mounts.push(named_volume(volume, ACTIONS_DIR, true));
        }
        let working_dir = self
            .workdir_for(ctx)
            .cloned()
            .or_else(|| ctx.workspace.as_ref().map(|_| WORKSPACE_DIR.to_string()));
        Config {
            image: Some(image.to_string()),
            cmd: Some(ctx.command.clone()),
            env: if env.is_empty() { None } else { Some(env) },
            labels: identity_labels(ctx),
            working_dir,
            // The task's uid is the task's alone. What is run for it first
            // runs as its own image says: the volumes it makes take their
            // owner from that, and a fetch is not the task's code.
            user: ctx
                .isolation
                .as_ref()
                .and_then(|iso| iso.run_as_user)
                .map(|uid| uid.to_string()),
            // A privileged task's own container alone is privileged: what
            // is run for it first, its services and its steps are not.
            host_config: Some(HostConfig {
                privileged: ctx.privileged.then_some(true),
                ..self.host_config(ctx, Made::Task, mounts)
            }),
            ..Default::default()
        }
    }

    /// The init container that installs `dagron-ci-exec` from `image` into
    /// an anonymous volume at [`TOOLS_DIR`], with another at
    /// [`WORKSPACE_DIR`] when the task's workspace is of this executor's
    /// making: both take their mode from the image's directories, and the
    /// task mounts them by name. Labelled as the task is, so the reap and the
    /// sweep find it as they find the task's container.
    fn init_config(&self, ctx: &ExecContext, image: &str) -> Config<String> {
        let mut mounts = vec![anonymous_volume(TOOLS_DIR)];
        if self.makes_workspace(ctx) {
            mounts.push(anonymous_volume(WORKSPACE_DIR));
        }
        Config {
            image: Some(image.to_string()),
            cmd: Some(install_args(
                ctx.workspace.as_ref().unwrap_or(&Default::default()),
            )),
            labels: identity_labels(ctx),
            host_config: Some(self.host_config(ctx, Made::ForIt, mounts)),
            ..Default::default()
        }
    }

    /// Run the init container `name`: `Ok` with its volumes once
    /// `dagron-ci-exec` is in them. A failure is the backend's, not the
    /// task's, and says why in the init container's own words.
    async fn install_tools(
        &self,
        ctx: &ExecContext,
        image: &str,
        name: &str,
        secs: u64,
        making: &Making,
    ) -> Result<InitVolumes> {
        let what = "the init container that installs dagron-ci-exec";
        let (code, said) = self
            .run_once(name, self.init_config(ctx, image), secs, what, making)
            .await?;
        if code != 0 {
            anyhow::bail!("{what} ({image}) exited {code}: {}", said.trim());
        }
        let mut volumes = self.volumes_of(name).await?;
        Ok(InitVolumes {
            tools: volumes
                .remove(TOOLS_DIR)
                .ok_or_else(|| anyhow::anyhow!("{what} has no volume at {TOOLS_DIR}"))?,
            workspace: volumes.remove(WORKSPACE_DIR),
        })
    }

    /// The container that fetches `checkout` into the workspace volume
    /// `volume`, with the checkout's image, as [`crate::checkout::SCRIPT`]
    /// and its variables say. With a widened umask: the task's user may not
    /// be this container's.
    fn checkout_config(
        &self,
        ctx: &ExecContext,
        checkout: &CheckoutSpec,
        image: &str,
        volume: &str,
    ) -> Config<String> {
        let mut env =
            crate::checkout::fetch_env(checkout, ctx.checkout_credential.as_ref(), Some("0000"));
        env.extend(crate::checkout::patch_env(
            checkout,
            ctx.checkout_patch.as_deref(),
        ));
        let env = env.into_iter().map(|(k, v)| format!("{k}={v}")).collect();
        Config {
            image: Some(image.to_string()),
            // The script, whatever the image's own entrypoint (alpine/git's is
            // git): a set entrypoint also drops the image's command.
            entrypoint: Some(vec![
                "sh".into(),
                "-ec".into(),
                crate::checkout::SCRIPT.into(),
            ]),
            env: Some(env),
            working_dir: Some(WORKSPACE_DIR.to_string()),
            labels: identity_labels(ctx),
            host_config: Some(self.host_config(
                ctx,
                Made::ForIt,
                vec![named_volume(volume, WORKSPACE_DIR, false)],
            )),
            ..Default::default()
        }
    }

    /// The container that holds the volume a task's actions are fetched
    /// into, an anonymous one at [`ACTIONS_DIR`], with the checkout's image:
    /// made and never started, and with none of the fetch's variables, so
    /// that what is kept as long as the task runs holds none of their
    /// credentials. Labelled as the task is, so the reap and the sweep find
    /// it as they find the task's container.
    fn actions_holder_config(&self, ctx: &ExecContext, image: &str) -> Config<String> {
        Config {
            image: Some(image.to_string()),
            // Never run: a command only because Docker makes no container
            // without one, and the image need not have one.
            entrypoint: Some(vec!["sh".into()]),
            labels: identity_labels(ctx),
            host_config: Some(self.host_config(
                ctx,
                Made::ForIt,
                vec![anonymous_volume(ACTIONS_DIR)],
            )),
            ..Default::default()
        }
    }

    /// The container that fetches `actions` into the volume `volume`, with
    /// the checkout's image, as [`crate::checkout::ACTIONS_SCRIPT`] and its
    /// variables say: with the credential of each that has one
    /// ([`crate::checkout::action_headers`]), and none for the rest.
    fn actions_config(
        &self,
        ctx: &ExecContext,
        actions: &[ActionSpec],
        image: &str,
        volume: &str,
    ) -> Config<String> {
        let env = crate::checkout::actions_fetch_env(ctx, actions, ACTIONS_DIR)
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        Config {
            image: Some(image.to_string()),
            entrypoint: Some(vec![
                "sh".into(),
                "-ec".into(),
                crate::checkout::ACTIONS_SCRIPT.into(),
            ]),
            env: Some(env),
            labels: identity_labels(ctx),
            host_config: Some(self.host_config(
                ctx,
                Made::ForIt,
                vec![named_volume(volume, ACTIONS_DIR, false)],
            )),
            ..Default::default()
        }
    }

    /// Fetch `actions` for the attempt whose task container is `name`: `Ok`
    /// with the name of the volume they are in once they are there. The
    /// volume is made by the container that holds it, [`actions_name`],
    /// which the caller removes after the task's on every path: it goes with
    /// its volume then, and not while the task's container still mounts it.
    /// The fetch runs in a container of its own, [`actions_fetch_name`], the
    /// only one with the actions' credentials, removed as soon as it has
    /// run, on every path: one not seen removed fails the attempt, so that
    /// no task's container is made while it is there. A failure says why in
    /// git's words.
    async fn fetch_actions(
        &self,
        ctx: &ExecContext,
        actions: &[ActionSpec],
        name: &str,
        secs: u64,
        making: &Making,
    ) -> Result<String> {
        let image = self
            .checkout_image
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no {ENV_CHECKOUT_IMAGE} to fetch the actions with"))?;
        let holder = actions_name(name);
        self.create(making, &holder, self.actions_holder_config(ctx, image))
            .await
            .map_err(|e| anyhow::anyhow!("the actions' volume: {e}"))?;
        let volume = self
            .volumes_of(&holder)
            .await?
            .remove(ACTIONS_DIR)
            .ok_or_else(|| anyhow::anyhow!("{holder} has no volume at {ACTIONS_DIR}"))?;
        let fetch = actions_fetch_name(name);
        let config = self.actions_config(ctx, actions, image, &volume);
        let ran = self
            .run_once(&fetch, config, secs, "the actions' fetch", making)
            .await;
        let removed = self.removed_within(&fetch).await.is_ok();
        let (code, said) = ran?;
        if code != 0 {
            anyhow::bail!(
                "the fetch of the task's actions failed ({image} exited {code}): {}",
                said.trim()
            );
        }
        if !removed {
            anyhow::bail!(
                "the container that fetched the task's actions ({fetch}) was not seen removed: \
                 the task is not made beside their credentials"
            );
        }
        tracing::info!(count = actions.len(), "fetched the task's actions");
        Ok(volume)
    }

    /// Fetch `checkout` into the workspace volume `volume` in the container
    /// `name`, the only one with the repository's credential, removed as soon
    /// as it has run, on every path: one not seen removed fails the attempt,
    /// so that no task's container is started while it is there. The task's
    /// is made by then, and never started: the caller removes it once the
    /// checkout's is seen gone, and otherwise, where the task's own volume is
    /// the workspace, leaves it for the task's next dispatch or the fleet
    /// sweep. A failure says why in git's words.
    async fn fetch_checkout(
        &self,
        ctx: &ExecContext,
        checkout: &CheckoutSpec,
        volume: &str,
        name: &str,
        secs: u64,
        making: &Making,
    ) -> Result<()> {
        let image = self
            .checkout_image
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no {ENV_CHECKOUT_IMAGE} to fetch the checkout with"))?;
        let config = self.checkout_config(ctx, checkout, image, volume);
        let ran = self
            .run_once(name, config, secs, "the checkout", making)
            .await;
        let removed = self.removed_within(name).await.is_ok();
        let (code, said) = ran?;
        if code != 0 {
            anyhow::bail!(
                "the checkout of {} at {} failed ({image} exited {code}): {}",
                checkout.repository,
                checkout.commit,
                said.trim()
            );
        }
        if !removed {
            anyhow::bail!(
                "the container that fetched the task's checkout ({name}) was not seen removed: \
                 the task is not started beside the repository's credential"
            );
        }
        tracing::info!(repository = %checkout.repository, commit = %checkout.commit, "checked out");
        Ok(())
    }

    /// Make one of the attempt's containers, `name`, under its [`Making`]
    /// ([`Self::create_under`]).
    async fn create(&self, making: &Making, name: &str, config: Config<String>) -> Result<()> {
        // No await between the guard and the spawn: a caller dropped here has
        // either asked for nothing or handed the request over whole.
        let guard = making.clone().lock_owned().await;
        self.create_under(guard, name, config).await
    }

    /// Make container `name`, to the end of the request, whatever becomes of
    /// the caller, holding `guard` until then.
    ///
    /// A stop drops the attempt where it stands. A create the daemon had been
    /// asked for was still carried out, after the stop had looked for the
    /// container, found none, and said the work was gone: a container made
    /// and never started, with its volumes, left until the task's next
    /// dispatch or the sweep. So the request runs on a task of its own and
    /// holds `guard` to its end: the attempt's [`Making`], which the stop
    /// takes before it removes anything, or a step's share of its task's
    /// loan ([`Lent::steps`]), which [`Self::take_back`] takes whole. What
    /// was being made is there to remove.
    async fn create_under<G: Send + 'static>(
        &self,
        guard: G,
        name: &str,
        config: Config<String>,
    ) -> Result<()> {
        let docker = self.docker.clone();
        let made_name = name.to_string();
        let name = name.to_string();
        let made = tokio::spawn(async move {
            let _held = guard;
            let options = CreateContainerOptions {
                name,
                platform: None,
            };
            docker.create_container(Some(options), config).await
        });
        let made = made.await.map_err(|e| {
            anyhow::anyhow!("the request that makes the container ended early: {e}")
        })?;
        made?;
        self.judge_first_container(&made_name).await;
        Ok(())
    }

    /// Create, start and wait on the one-shot container `name`: its exit
    /// code, and what it said when that is not 0. `what` names it in errors.
    /// The caller removes it.
    async fn run_once(
        &self,
        name: &str,
        config: Config<String>,
        secs: u64,
        what: &str,
        making: &Making,
    ) -> Result<(i64, String)> {
        self.create(making, name, config)
            .await
            .map_err(|e| anyhow::anyhow!("{what}: {e}"))?;
        self.docker.start_container::<String>(name, None).await?;
        let waited = timeout(Duration::from_secs(secs), async {
            self.docker
                .wait_container::<String>(name, None::<WaitContainerOptions<String>>)
                .try_next()
                .await
        })
        .await;
        let code = match waited {
            Ok(Ok(Some(response))) => response.status_code,
            Ok(Ok(None)) => anyhow::bail!("{what}: its wait stream ended unexpectedly"),
            // bollard reports a non-zero exit as an error carrying the code.
            Ok(Err(bollard::errors::Error::DockerContainerWaitError { code, .. })) => code,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {
                return Err(anyhow::Error::new(crate::executor::TimeoutError { secs })
                    .context(format!("{what} did not finish")))
            }
        };
        let mut said = String::new();
        if code != 0 {
            let mut logs = self.docker.logs::<String>(
                name,
                Some(LogsOptions {
                    stdout: true,
                    stderr: true,
                    ..Default::default()
                }),
            );
            let _ = timeout(Duration::from_secs(CLEANUP_TIMEOUT_SECS), async {
                while let Some(Ok(chunk)) = logs.next().await {
                    said.push_str(&String::from_utf8_lossy(&chunk.into_bytes()));
                }
            })
            .await;
        }
        Ok((code, said))
    }

    /// The name of each volume container `name` mounts, by where it is.
    async fn volumes_of(&self, name: &str) -> Result<std::collections::HashMap<String, String>> {
        let inspected = self
            .docker
            .inspect_container(name, None)
            .await
            .map_err(|e| anyhow::anyhow!("inspecting {name}'s volumes: {e}"))?;
        Ok(inspected
            .mounts
            .unwrap_or_default()
            .into_iter()
            .filter_map(|m| Some((m.destination?, m.name?)))
            .collect())
    }
}

/// A volume made for one container, at `target`: removed with the container.
fn anonymous_volume(target: &str) -> Mount {
    Mount {
        target: Some(target.to_string()),
        typ: Some(MountTypeEnum::VOLUME),
        ..Default::default()
    }
}

/// Another container's volume `volume`, by name, at `target`. Never filled
/// from this container's image: while the volume is empty Docker would copy
/// the image's directory at `target` into it, mode and all, and with the
/// working directory there that is a directory Docker made, root's, 0755,
/// which would undo the world-writable workspace the init container made.
fn named_volume(volume: &str, target: &str, read_only: bool) -> Mount {
    Mount {
        target: Some(target.to_string()),
        source: Some(volume.to_string()),
        typ: Some(MountTypeEnum::VOLUME),
        read_only: read_only.then_some(true),
        volume_options: Some(MountVolumeOptions {
            no_copy: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Identity labels, so a container can be found by something other than the
/// process that created it — see `TaskIdentity`. An identity that will not
/// express as labels contributes none, rather than a partial set a reaper
/// could match on and misread.
fn identity_labels(ctx: &ExecContext) -> Option<std::collections::HashMap<String, String>> {
    let labels = ctx.identity.as_ref().map(|i| i.labels()).filter(|l| !l.is_empty());
    labels.map(|l| l.into_iter().collect())
}

#[async_trait]
impl Executor for DockerExecutor {
    fn prefetches(&self) -> bool {
        true
    }

    fn forget_run(&self, run_id: &str) {
        self.pins.lock().unwrap().forget(run_id);
    }

    /// Each of a run's images pulled now, side by side, one pull of each at
    /// a time on this host, and pinned for the run as its tasks would pin
    /// them: the first of its tasks then waits on none, or on one pull of
    /// its image already going. An image a policy of `never` names, or one
    /// a step builds here, is left to its task.
    async fn prefetch(&self, run_id: &str, images: Vec<crate::executor::RunImage>) {
        let mut seen = std::collections::HashSet::new();
        let wanted: Vec<(String, Option<ImagePullPolicy>)> = images
            .into_iter()
            .filter(|i| i.policy != Some(ImagePullPolicy::Never))
            .map(|i| {
                (
                    i.image.unwrap_or_else(|| self.default_image.clone()),
                    i.policy,
                )
            })
            .filter(|(image, _)| {
                // One built here for a step, as its run tells one: no
                // registry has it.
                let built = image
                    .strip_prefix(BUILT_STEP_IMAGE)
                    .is_some_and(|tag| tag.starts_with(':'));
                !built && seen.insert(image.clone())
            })
            .collect();
        let pulls = wanted.iter().map(|(image, policy)| async move {
            if let Err(e) = self
                .fetch_image("a run's image", image, *policy, Some(run_id), true)
                .await
            {
                tracing::debug!(run_id, image = %image, error = %format!("{e:#}"), "a run's image was not fetched ahead of its task, which will say why");
            }
        });
        futures_util::future::join_all(pulls).await;
    }

    async fn run_step_container(&self, task_id: &str, step: StepContainer) -> Result<StepOutcome> {
        self.run_step(task_id, step).await
    }

    async fn build_step_image(&self, task_id: &str, build: StepBuild) -> Result<StepBuilt> {
        self.build_step(task_id, build).await
    }

    /// Every container on this daemon carrying this installation's labels and
    /// old enough to judge.
    ///
    /// `all: true` on purpose — an exited container still holds its writable
    /// layer, so a leftover that stopped on its own is still disk this sweep
    /// should reclaim. Scoped on installation as well as `managed-by`, so a
    /// second dagron on the same daemon is invisible here rather than being
    /// mistaken for an orphan.
    async fn list_orphan_candidates(
        &self,
        scope: &crate::executor::OrphanScope<'_>,
    ) -> Result<Vec<crate::executor::ManagedWorkload>> {
        use crate::executor::{LABEL_INSTALLATION, LABEL_MANAGED_BY, MANAGED_BY};
        let Some(installation) = crate::executor::label_value(scope.installation) else {
            anyhow::bail!(
                "DAGRON_INSTALLATION={:?} is not usable as a label value, so the fleet sweep \
                 cannot scope itself and will not run. Use 1-63 alphanumerics, '-', '_' or '.', \
                 starting and ending alphanumeric.",
                scope.installation
            );
        };
        let mut filters = std::collections::HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![
                format!("{LABEL_MANAGED_BY}={MANAGED_BY}"),
                format!("{LABEL_INSTALLATION}={installation}"),
            ],
        );
        let list = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            })),
        )
        .await
        .map_err(|_| anyhow::anyhow!("listing containers for the fleet sweep timed out"))??;

        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().saturating_sub(scope.min_age.as_secs()) as i64)
            .unwrap_or(0);
        // Every name listed, young or old: what a container that made a
        // volume waits on.
        let names: Vec<String> = list
            .iter()
            .flat_map(|c| c.names.iter().flatten().cloned())
            .collect();

        let mut out = Vec::new();
        for c in list {
            let Some(id) = c.id.clone() else { continue };
            // Young containers are not judged — see `OrphanScope::min_age`. A
            // missing creation time counts as young, because the alternative is
            // judging a container whose age is unknown.
            if c.created.is_none_or(|born| born > cutoff) {
                continue;
            }
            // Nor is one that made a volume others of its attempt mount by
            // name, while any of them is there, however old it is: a holder
            // of volumes waits for the rest of its attempt, and a task's own
            // container for its checkout's and its steps', which mount the
            // workspace it made where no init container did (`held_back`).
            // Made before them, it is old first, and removed before them its
            // volume would be left for good. A later pass judges it.
            if c.names.iter().flatten().any(|n| held_back(n, &names)) {
                continue;
            }
            let labels = c.labels.unwrap_or_default();
            // Refused here, not filtered: the selector constrains only the
            // labels it names and says nothing about the rest — see
            // `from_labels`. No uid: a container id is already an identity
            // rather than a name for one, so deletion stays id-based.
            let Some(w) = crate::executor::ManagedWorkload::from_labels(id, None, |k| labels.get(k))
            else {
                continue;
            };
            out.push(w);
        }
        Ok(out)
    }

    /// Remove one container the sweep judged leftover.
    ///
    /// By **id**, taken from the listing, so the container removed is the one
    /// judged rather than whatever a name might resolve to now.
    ///
    /// `force: true` because a leftover is as likely to be still running as
    /// exited — the scheduler that would have stopped it is the one that died
    /// — and because removal is what reclaims the writable layer, which is the
    /// disk this sweep exists to give back.
    async fn delete_workload(&self, w: &crate::executor::ManagedWorkload) -> Result<()> {
        timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.remove_container(
                &w.handle,
                Some(RemoveContainerOptions { force: true, v: true, ..Default::default() }),
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("removing container '{}' timed out", w.handle))??;
        Ok(())
    }

    /// What every dispatch does before it creates anything, and nothing more:
    /// the task's containers from an earlier claim are stopped with its grace
    /// and removed, and seen gone ([`Self::reap_predecessors`]).
    async fn teardown(&self, ctx: &ExecContext) -> Result<()> {
        self.reap_predecessors(ctx).await.map_err(anyhow::Error::new)
    }

    async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
        if ctx.command.is_empty() {
            anyhow::bail!("empty command");
        }
        // A privileged container where the operator has not said one may be
        // run: the engine refuses such a task before dispatch, and this is
        // the same answer for any other caller.
        if ctx.privileged {
            if let Some(why) = dagron_core::privileged::task_refusal(self.privileged, ctx.isolation.as_ref()) {
                anyhow::bail!("{why}");
            }
        }
        // An allowlist is an internal network of the operator's, read again
        // before anything is made: the engine refuses one it was not told of
        // before dispatch, and this is the same answer for any other caller,
        // since the alternative is a task with a way out that believes it is
        // held to a list.
        if let Some(name) = ctx
            .isolation
            .as_ref()
            .and_then(|iso| iso.network.as_ref())
            .and_then(|network| network.allowlist())
        {
            self.held_to(name).await?;
        }
        // A task's Docker daemon is its first service, as the engine makes
        // it: the others, and the task, join its network. On a runtime of its
        // own (Sysbox) it could not make its network's devices in another's.
        if ctx.services.iter().skip(1).any(|s| s.daemon) {
            anyhow::bail!(
                "this task's Docker daemon is not its first service: the daemon's network is \
                 the one the task and its other services share"
            );
        }
        // A task's Docker daemon has its workspace where an init container
        // made it, a volume the daemon is made with. Without one the task's
        // workspace is its own container's, made after the daemon, and a
        // `docker run -v "$PWD:…"` would find an empty directory there, and
        // say nothing.
        if ctx.services.iter().any(|s| s.daemon)
            && self.makes_workspace(ctx)
            && self.ci_exec_image.is_none()
        {
            anyhow::bail!(
                "this task has a Docker daemon (docker_daemon: true) and a workspace, and \
                 this engine has no {ENV_CI_EXEC_IMAGE}, whose init container makes the \
                 workspace the daemon is given: set it, or name the tasks' working directory \
                 ({ENV_WORKDIR})"
            );
        }
        let secs = crate::executor::effective_timeout_secs(ctx.timeout_secs);
        // Short unique name — stays well within Docker's 64-char limit.
        let name = format!("sched-{}", Uuid::new_v4().simple());
        // Its checkout, settled before anything is made: into a workspace of
        // this executor's making only (the operator's directory holds the code
        // already, and a fetch would overwrite it), and refused here when this
        // engine has nothing to fetch it with.
        let checkout = match crate::checkout::of(ctx)? {
            Some(checkout) if self.makes_workspace(ctx) => Some(checkout),
            Some(_) => {
                tracing::info!(
                    "the workspace is the operator's directory: nothing is checked out into it"
                );
                None
            }
            None => None,
        };
        if checkout.is_some() && self.checkout_image.is_none() {
            anyhow::bail!(
                "this task's workspace has a checkout, and this engine has no {ENV_CHECKOUT_IMAGE} \
                 (an image with git and sh) to fetch it with"
            );
        }
        // Its actions likewise: fetched by the same image, into a volume at
        // a place of their own, which no mount of the operator's may hide.
        let actions = crate::checkout::actions_of(ctx)?;
        if !actions.is_empty() {
            if self.checkout_image.is_none() {
                anyhow::bail!(
                    "this task uses actions, and this engine has no {ENV_CHECKOUT_IMAGE} (an \
                     image with git and sh) to fetch them with"
                );
            }
            let under = format!("{ACTIONS_DIR}/");
            if let Some(m) = self
                .mounts_for(ctx)
                .iter()
                .find(|m| m.target == ACTIONS_DIR || m.target.starts_with(&under))
            {
                anyhow::bail!(
                    "{ENV_MOUNTS} mounts {} where a task's actions are fetched to \
                     ({ACTIONS_DIR}), and this task uses actions",
                    m.target
                );
            }
        }
        // Before creating: stop and remove anything an earlier attempt of this
        // same task left, and see it gone. Without this the lease bounds the
        // row, not the work. While any of it is still there this dispatch
        // starts nothing, and is handed back to be tried again.
        self.reap_predecessors(ctx).await.map_err(anyhow::Error::new)?;

        let making = Making::default();
        // What the task lends its steps, kept past its end until this
        // attempt is over, however it ends: the stop takes them back too.
        let _loan = Loan(Arc::clone(&self.lent), name.clone());
        let attempt = async {
            // The task's image before anything is made for the attempt: one
            // that cannot be had makes nothing, and says why.
            let named = ctx.docker_image.as_deref().unwrap_or(&self.default_image);
            let (run, trusted) = (run_of(ctx), ctx.confinement.is_none());
            let image = self
                .fetch_image(
                    "the task's image",
                    named,
                    ctx.image_pull_policy,
                    run,
                    trusted,
                )
                .await?;
            self.refuse_a_root_image(ctx, "the task's image", named, &image)
                .await?;
            // And each of its services', likewise, and held to the same. All
            // but its Docker daemon, which runs as root, as the operator who
            // offered it accepted.
            // The daemon's image is the operator's, fetched once and kept,
            // whatever the task's policy says of its own: `Never` would fail
            // every daemon on an engine whose image is not yet here, and
            // `Always` would pull it from its registry at every attempt.
            for service in &ctx.services {
                let what = service_image(service);
                let policy = match service.daemon {
                    true => Some(ImagePullPolicy::IfNotPresent),
                    false => ctx.image_pull_policy,
                };
                let image = self
                    .fetch_image(&what, &service.image, policy, run, trusted)
                    .await?;
                if !service.daemon {
                    self.refuse_a_root_image(ctx, &what, &service.image, &image)
                        .await?;
                }
            }
            // A workspace task's tools next, in a container whose volumes the
            // task's own then mounts; removed after it on every path.
            let (init, volumes) = match (&self.ci_exec_image, &ctx.workspace) {
                (Some(image), Some(_)) => {
                    let init = init_name(&name);
                    match self.install_tools(ctx, image, &init, secs, &making).await {
                        Ok(volumes) => (Some(init), Some(volumes)),
                        Err(e) => {
                            self.force_remove(&init).await;
                            return Err(e);
                        }
                    }
                }
                _ => (None, None),
            };
            let ran = self
                .run_container(
                    ctx,
                    &name,
                    volumes.as_ref(),
                    checkout,
                    actions,
                    secs,
                    &making,
                )
                .await;
            // What mounts the task's workspace by name first: its steps'
            // containers, taken back again (they hold its network too), so
            // that those not seen removed are asked for again; and its
            // checkout's. Then its actions' fetch, which mounts only the
            // actions' volume. Those two are gone by now unless the daemon
            // did not say so: each is asked for again, and waited for while
            // a removal of it is under way.
            let mut mounting: Vec<String> = self
                .take_back(&name)
                .await
                .into_iter()
                .map(|(step, _)| step)
                .collect();
            if checkout.is_some() && self.removed_within(&checkout_name(&name)).await.is_err() {
                mounting.push(checkout_name(&name));
            }
            let workspace_free = mounting.is_empty();
            let mut gone = workspace_free;
            if !actions.is_empty() {
                gone &= self
                    .removed_within(&actions_fetch_name(&name))
                    .await
                    .is_ok();
            }
            // Then the task's own. Where it made the workspace (no init
            // container did), the volume goes with it only once nothing
            // mounts it, so it is removed only once its steps' and its
            // checkout's are seen gone, and is otherwise killed and left for
            // the task's next dispatch or the fleet sweep, which take it
            // after them: with the task's variables in its settings until
            // then.
            let owns = self.makes_workspace(ctx)
                && volumes
                    .as_ref()
                    .and_then(|v| v.workspace.as_ref())
                    .is_none();
            if workspace_free || !owns {
                gone &= self.removed_within(&name).await.is_ok();
            } else {
                self.kill(&name).await;
                tracing::warn!(
                    container = %name, ?mounting,
                    "containers that mount the task's workspace were not seen removed, so the \
                     task's own, whose volume the workspace is, was killed and left, with the \
                     task's variables, its secrets among them, in its settings. The task's next \
                     dispatch, a stop's teardown, or the fleet sweep (DAGRON_INSTALLATION) \
                     removes it after them; a task that has ended is not dispatched again, so \
                     where the sweep does not run, remove those containers and then this one \
                     by hand, each with its volumes"
                );
            }
            // Then its services', which outlive it only to here: the task's
            // was made in the first one's network.
            gone &= self.remove_services(ctx, &name).await;
            // Then its actions' holder and its init container, whose volumes
            // those mounted by name: each goes with its volumes only once
            // nothing mounts them, so only once the rest is seen gone, and is
            // otherwise left for the task's next dispatch or the fleet sweep,
            // which take it after the rest.
            let holders: Vec<String> = [(!actions.is_empty()).then(|| actions_name(&name)), init]
                .into_iter()
                .flatten()
                .collect();
            if gone {
                for holder in &holders {
                    self.force_remove(holder).await;
                }
            } else if !holders.is_empty() {
                tracing::warn!(
                    container = %name, ?holders,
                    "a container of the attempt was not seen removed: those that hold its volumes \
                     are left for the task's next dispatch or the fleet sweep"
                );
            }
            ran
        };
        tokio::select! {
            // The stop first: one asked for before anything was made makes
            // nothing.
            biased;
            _ = ctx.stop.requested() => {
                // The attempt is dropped by now. A container it had asked the
                // daemon for is still being made: wait for that, so that it
                // is there to remove and not made after the remove.
                let _made = making.lock().await;
                match self.stop_and_remove(ctx, &name).await {
                    Ok(()) => Err(anyhow::Error::new(crate::executor::ClaimLost)),
                    Err(why) => Err(crate::executor::ClaimLost::unseen(why)),
                }
            }
            ran = attempt => ran,
        }
    }
}


/// The names an attempt's containers are made under, from its task
/// container's: what [`DockerExecutor::stop_and_remove`] removes.
fn init_name(name: &str) -> String {
    format!("{name}-init")
}
fn checkout_name(name: &str) -> String {
    format!("{name}-checkout")
}
fn actions_name(name: &str) -> String {
    format!("{name}-{}", crate::checkout::ACTIONS_CONTAINER_NAME)
}
/// The container that fetches an attempt's actions into the volume
/// [`actions_name`] holds: a name no holder's ends with.
fn actions_fetch_name(name: &str) -> String {
    format!("{}-fetch", actions_name(name))
}

/// The task container's name of the attempt the container `name` (as a
/// listing gives it, or not) holds volumes for, which the attempt's others
/// mount by name: where it is its init container or its actions' holder.
fn holder_of(name: &str) -> Option<&str> {
    let name = name.trim_start_matches('/');
    name.strip_suffix(&init_name(""))
        .or_else(|| name.strip_suffix(&actions_name("")))
}

/// Whether the container `name` made a volume that another of `names` mounts
/// by name, and is to be left until that one is gone. Removed before it, its
/// volume would be in use, which Docker skips, and then never removed: not
/// with a container that mounted it by name. A holder of volumes
/// ([`holder_of`]) waits for every other container of its attempt; any other
/// container, for those that mount its workspace ([`mounts_workspace_of`]),
/// which is the task's own where no init container made it. By name alone,
/// so that what any version of this executor left is judged the same.
fn held_back(name: &str, names: &[String]) -> bool {
    let Some(task) = holder_of(name) else {
        let name = name.trim_start_matches('/');
        return names
            .iter()
            .any(|other| mounts_workspace_of(other.trim_start_matches('/'), name));
    };
    names.iter().any(|other| {
        let other = other.trim_start_matches('/');
        holder_of(other).is_none()
            && other
                .strip_prefix(task)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('-'))
    })
}

/// Whether the container `other` mounts, by name, the workspace of the
/// attempt whose task container is `task`: where it is its checkout's
/// ([`checkout_name`]), or one of its steps' ([`step_name`]).
fn mounts_workspace_of(other: &str, task: &str) -> bool {
    other.strip_prefix(task).is_some_and(|rest| {
        rest == checkout_name("")
            || rest
                .strip_prefix("-step-")
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
    })
}

/// The name an attempt's `n`th step's container is made under, from its
/// task container's.
fn step_name(task: &str, n: u32) -> String {
    format!("{task}-step-{n}")
}

/// Whether a container of these names holds volumes for its attempt
/// ([`holder_of`]).
fn holds(names: &[String]) -> bool {
    names.iter().any(|n| holder_of(n).is_some())
}

/// Those of `listed`, each a container's id and its names, that are not
/// [`held_back`] by another of them.
fn not_held_back(listed: &[(String, Vec<String>)]) -> Vec<(String, Vec<String>)> {
    let names: Vec<String> = listed.iter().flat_map(|(_, n)| n.iter().cloned()).collect();
    listed
        .iter()
        .filter(|(_, n)| !n.iter().any(|n| held_back(n, &names)))
        .cloned()
        .collect()
}

/// The name an attempt's `i`th service's container is made under, from its
/// task container's.
fn service_name(name: &str, i: usize) -> String {
    format!("{name}-svc-{i}")
}

/// What a service's image is called where one could not be had or run.
fn service_image(service: &ServiceSpec) -> String {
    format!("the image of the service '{}'", service.name)
}

/// The network mode of a container made in the network of container `first`.
fn network_of(first: &str) -> String {
    format!("container:{first}")
}

/// What is left of what a service said, for why it was not ready: its last
/// lines, or that it said nothing.
fn said_or_nothing(said: &str) -> String {
    match said.trim() {
        "" => "it said nothing".to_string(),
        said => format!("it said: {said}"),
    }
}

impl DockerExecutor {
    /// The container a service runs in.
    ///
    /// **One network for the attempt.** The first service's container is the
    /// one whose network the other services' and the task's are made in
    /// (`first` is `None` for it, and its name for the rest). It is on the
    /// network the task would have been on, or on none where the task asked
    /// for none, and every service's name is a name for its loopback address.
    /// So the task finds each service at its name, as a pod's containers find
    /// each other, and no two attempts' services answer to one name on the
    /// operator's network: a name given on that network (an alias) would be
    /// every attempt's at once.
    ///
    /// What this does not do is hide a service. Its container is on the
    /// operator's network as the task's own would have been, and whatever
    /// else is on that network reaches its ports at its address.
    ///
    /// **Held as the task is**: its envelope, its uid, and the limits of a
    /// run that is not trusted. A service's image and arguments are its
    /// author's, as the task's are. It gets none of the operator's mounts and
    /// nothing of the task's workspace.
    ///
    /// **The task's Docker daemon** ([`dagron_core::docker_daemon`]) is the
    /// one service that is not: its container is privileged and runs as
    /// root, which is what a daemon in a container needs, or runs on the
    /// operator's runtime, which gives it that in a user namespace of its
    /// own and no privileges ([`Self::with_daemon_runtime`]), and has what the
    /// task has at the task's own paths: the operator's mounts, and the
    /// workspace volume where an init container made one. So a path the
    /// task names to it is a path it has. Its network and limits are a
    /// service's.
    fn service_config(
        &self,
        ctx: &ExecContext,
        service: &ServiceSpec,
        first: Option<&str>,
        init: Option<&InitVolumes>,
    ) -> Config<String> {
        let mut mounts = Vec::new();
        if service.daemon {
            mounts.extend(
                self.mounts_for(ctx)
                    .iter()
                    .filter(|m| !dagron_core::docker_daemon::owns(&m.target))
                    .map(TaskMount::to_docker),
            );
            if let Some(volume) = init.and_then(|i| i.workspace.as_deref()) {
                mounts.push(named_volume(volume, WORKSPACE_DIR, false));
            }
        }
        let mut host = self.host_config(ctx, Made::Task, mounts);
        if service.daemon {
            match &self.daemon_runtime {
                Some(runtime) => host.runtime = Some(runtime.clone()),
                None => host.privileged = Some(true),
            }
            host.readonly_rootfs = None;
            host.cap_drop = None;
            host.security_opt = None;
        }
        match first {
            Some(first) => host.network_mode = Some(network_of(first)),
            None => {
                host.extra_hosts = Some(
                    ctx.services
                        .iter()
                        .map(|s| format!("{}:127.0.0.1", s.name))
                        .collect(),
                );
            }
        }
        // A service is on the task's network, held to its allowlist: it is
        // told of the list's proxy as the task is (the daemon pulls through
        // it), before its own variables and never over one.
        let proxy = self.proxy_env(ctx);
        let env: Vec<String> = proxy
            .iter()
            .filter(|(k, _)| !service.env.iter().any(|e| &e.name == k))
            .map(|(k, v)| format!("{k}={v}"))
            .chain(service.env.iter().map(|e| format!("{}={}", e.name, e.value)))
            .collect();
        Config {
            image: Some(self.resolved(run_of(ctx), &service.image)),
            cmd: (!service.args.is_empty()).then(|| service.args.clone()),
            env: (!env.is_empty()).then_some(env),
            labels: identity_labels(ctx),
            user: ctx
                .isolation
                .as_ref()
                .and_then(|iso| iso.run_as_user)
                .filter(|_| !service.daemon)
                .map(|uid| uid.to_string()),
            host_config: Some(host),
            ..Default::default()
        }
    }

    /// Make the attempt's services' containers, the first of them first, and
    /// start none. The name of the first, whose network the task's container
    /// is then made in; `None` for a task with no services.
    async fn make_services(
        &self,
        ctx: &ExecContext,
        name: &str,
        init: Option<&InitVolumes>,
        making: &Making,
    ) -> Result<Option<String>> {
        if ctx.services.is_empty() {
            return Ok(None);
        }
        let first = service_name(name, 0);
        for (i, service) in ctx.services.iter().enumerate() {
            let config = self.service_config(ctx, service, (i > 0).then_some(first.as_str()), init);
            self.create(making, &service_name(name, i), config)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("the service '{}' could not be made: {e}", service.name)
                })?;
        }
        self.say_what_podman_leaves_out(ctx);
        Ok(Some(first))
    }

    /// Podman gives a container made in another's network that container's
    /// host names only where the other has a network. A task that asked for
    /// none finds its services at `localhost` there, and not at their names.
    /// Said once, in the engine's log: nothing here can give them.
    fn say_what_podman_leaves_out(&self, ctx: &ExecContext) {
        let no_network = ctx.isolation.as_ref().and_then(|iso| iso.network.as_ref())
            == Some(&dagron_core::isolation::Network::None);
        if self.podman && no_network && !self.podman_said.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "this daemon is Podman, and a task that asked for no network \
                 (isolation.network: none) has services: it reaches them at localhost, and \
                 not at their names, which Podman gives only to a container with a network"
            );
        }
    }

    /// Start the attempt's services in the order they are written, each seen
    /// ready before the next is started.
    async fn start_services(&self, ctx: &ExecContext, name: &str, secs: u64) -> Result<()> {
        for (i, service) in ctx.services.iter().enumerate() {
            let container = service_name(name, i);
            self.docker
                .start_container::<String>(&container, None)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("the service '{}' could not be started: {e}", service.name)
                })?;
            self.see_ready(&container, service, secs).await?;
            tracing::debug!(service = %service.name, container = %container, "service ready");
        }
        Ok(())
    }

    /// Wait until `service`, running in `container`, is ready: until its
    /// `ready` exits 0 there. No longer than its `ready_timeout_secs`, nor
    /// than the task's own timeout. One with no `ready` is asked once
    /// whether it still runs.
    ///
    /// A service that stops first, or is not ready in time, is an error that
    /// says what it last said: the attempt fails with it, and its task never
    /// starts.
    async fn see_ready(&self, container: &str, service: &ServiceSpec, secs: u64) -> Result<()> {
        let stopped = |code: i64, said: String| {
            anyhow::anyhow!(
                "the service '{}' stopped before it was ready (its container exited {code}): {}",
                service.name,
                said_or_nothing(&said)
            )
        };
        let Some(ready) = &service.ready else {
            return match self.exit_of(container).await? {
                Some(code) => Err(stopped(code, self.said(container).await)),
                None => Ok(()),
            };
        };
        let argv = ready.argv();
        let allowed = service.ready_timeout().min(secs);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(allowed);
        loop {
            if let Some(code) = self.exit_of(container).await? {
                return Err(stopped(code, self.said(container).await));
            }
            let probed = timeout(
                Duration::from_secs(READY_PROBE_SECS),
                self.run_in(container, &argv),
            )
            .await;
            let last = match probed {
                Ok(Ok((0, _))) => return Ok(()),
                Ok(Ok((code, said))) if said.trim().is_empty() => {
                    format!("its `ready` exited {code}")
                }
                Ok(Ok((code, said))) => format!("its `ready` exited {code} ({})", said.trim()),
                Ok(Err(e)) => format!("its `ready` could not be run: {e}"),
                Err(_) => format!("its `ready` did not end within {READY_PROBE_SECS} s"),
            };
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "the service '{}' was not ready within {allowed} s: {last}; {}",
                    service.name,
                    said_or_nothing(&self.said(container).await)
                );
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    /// The exit code of `container` if it has stopped; `None` while it runs.
    async fn exit_of(&self, container: &str) -> Result<Option<i64>> {
        let state = self
            .docker
            .inspect_container(container, None)
            .await
            .map_err(|e| anyhow::anyhow!("inspecting the service's container {container}: {e}"))?
            .state
            .unwrap_or_default();
        if state.running == Some(true) {
            return Ok(None);
        }
        Ok(Some(state.exit_code.unwrap_or(-1)))
    }

    /// Run `argv` in the running `container`: its exit code, and the start of
    /// what it wrote.
    async fn run_in(&self, container: &str, argv: &[String]) -> Result<(i64, String)> {
        let exec = self
            .docker
            .create_exec(
                container,
                CreateExecOptions {
                    cmd: Some(argv.to_vec()),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await?;
        let mut said = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.docker.start_exec(&exec.id, None).await?
        {
            while let Some(Ok(chunk)) = output.next().await {
                if said.len() < READY_SAID_BYTES {
                    said.push_str(&String::from_utf8_lossy(&chunk.into_bytes()));
                }
            }
        }
        let code = self
            .docker
            .inspect_exec(&exec.id)
            .await?
            .exit_code
            .unwrap_or(-1);
        Ok((code, said))
    }

    /// The last lines `container` wrote, for an error. Best effort: nothing
    /// where the daemon does not answer.
    async fn said(&self, container: &str) -> String {
        let mut logs = self.docker.logs::<String>(
            container,
            Some(LogsOptions {
                stdout: true,
                stderr: true,
                tail: SERVICE_SAID_LINES.to_string(),
                ..Default::default()
            }),
        );
        let mut said = String::new();
        let _ = timeout(Duration::from_secs(CLEANUP_TIMEOUT_SECS), async {
            while let Some(Ok(chunk)) = logs.next().await {
                said.push_str(&String::from_utf8_lossy(&chunk.into_bytes()));
            }
        })
        .await;
        said
    }

    /// Remove the attempt's services' containers, the first of them last: a
    /// daemon may refuse to remove a container whose network another is
    /// still made in (Podman does). Each is killed first
    /// ([`Self::kill`]). Whether each was seen gone.
    async fn remove_services(&self, ctx: &ExecContext, name: &str) -> bool {
        let mut gone = true;
        for i in (0..ctx.services.len()).rev() {
            let service = service_name(name, i);
            self.kill(&service).await;
            gone &= self.remove_seen(&service).await.is_ok();
        }
        gone
    }

    /// Kill the named container, if it runs. A service is given no grace:
    /// it is there for its task, which has ended. And a forced removal is
    /// not a kill on every daemon. Podman's asks the container to stop and
    /// waits ten seconds for one that does not heed it, which a server that
    /// is its container's first process does not unless it was written to,
    /// and ten seconds is all a removal here is given.
    async fn kill(&self, name: &str) {
        self.kill_running(name).await;
    }

    /// [`Self::kill`], saying whether the container was running: not where
    /// the daemon says it was not (409), or that it is not there (404).
    async fn kill_running(&self, name: &str) -> bool {
        let killed = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker
                .kill_container(name, Some(KillContainerOptions { signal: "SIGKILL" })),
        )
        .await;
        !matches!(
            killed,
            Ok(Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404 | 409,
                ..
            }))
        )
    }

    /// Attach to the task container `name`'s two streams before it starts,
    /// so that what it writes is read from its first byte as it writes it.
    /// Where the daemon would not, why: its output is then read
    /// back once it has stopped, as it was before it could be followed.
    async fn attach(&self, name: &str) -> std::result::Result<AttachContainerResults, String> {
        let options = AttachContainerOptions::<String> {
            stdout: Some(true),
            stderr: Some(true),
            stream: Some(true),
            logs: Some(false),
            ..Default::default()
        };
        match timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.attach_container(name, Some(options)),
        )
        .await
        {
            Ok(Ok(attached)) => Ok(attached),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("no answer within {CLEANUP_TIMEOUT_SECS} s")),
        }
    }

    /// The task container `name` started, and could not be attached to
    /// first, for `why`: said once, and after at debug. Said only of one
    /// that started, since a daemon that makes a container ready as it is
    /// attached to (Podman's) says why one cannot start there first, and
    /// that is the task's failure, not this.
    fn say_not_attached(&self, name: &str, why: &str) {
        if self.attach_refused.swap(true, Ordering::SeqCst) {
            tracing::debug!(container = %name, error = %why, "could not attach to the task's container");
        } else {
            tracing::warn!(
                container = %name,
                error = %why,
                "could not attach to a task's container, so its output is read back from \
                 the daemon once it has stopped: its log is not followed as it runs, and is \
                 what the container's log driver kept. Said once"
            );
        }
    }

    /// Read the stopped container `name`'s log back from the daemon, for what
    /// was not read of its streams as it ran (`taken`): all of it where they
    /// were not attached; else what comes after what was read of each, where
    /// the daemon's copy begins with what was (it may have dropped or
    /// rotated some away). Within `secs`.
    async fn read_back(&self, name: &str, taken: &mut Taken<'_>, secs: u64) {
        let mut logs = self.docker.logs::<String>(
            name,
            Some(LogsOptions {
                stdout: true,
                stderr: true,
                ..Default::default()
            }),
        );
        use std::hash::Hasher;
        let mut skip = Skip {
            out: Placing::new(taken.out.0, taken.out.1.finish()),
            err: Placing::new(taken.err.0, taken.err.1.finish()),
        };
        let read = timeout(Duration::from_secs(secs), async {
            while let Some(item) = logs.next().await {
                match item {
                    Ok(frame) => {
                        if let Some(frame) = skip.past(frame) {
                            taken.frame(frame);
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
        .await;
        match &read {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(container = %name, error = %e, "the container's log could not be read back; output may be truncated")
            }
            Err(_) => {
                tracing::warn!(container = %name, secs, "log collection timed out; output may be truncated")
            }
        }
        taken.unplaced = !matches!(read, Ok(Ok(()))) || !skip.out.placed() || !skip.err.placed();
    }
}

/// How long before its container is seen to have stopped an attachment may
/// end and be the container's end: the daemon closes a container's streams,
/// and with them its attachments, before it says the container has stopped,
/// and Docker waits up to ten seconds between for a log driver that is slow
/// to take the last of them. One that ended earlier was cut.
const ATTACH_END_SLACK: Duration = Duration::from_secs(30);

/// `work`, while what the attached container writes meanwhile is read.
async fn reading<T>(
    work: impl std::future::Future<Output = T>,
    follow: &mut Option<AttachContainerResults>,
    taken: &mut Taken<'_>,
) -> T {
    tokio::pin!(work);
    loop {
        // The work first: a container that writes without a pause is not
        // read past its deadline.
        tokio::select! {
            biased;
            done = &mut work => return done,
            frame = next_frame(follow) => taken.followed(frame, follow),
        }
    }
}

/// The next of what an attached container wrote; never, where none is
/// attached.
async fn next_frame(
    follow: &mut Option<AttachContainerResults>,
) -> Option<std::result::Result<LogOutput, bollard::errors::Error>> {
    match follow {
        Some(attached) => attached.output.next().await,
        None => std::future::pending().await,
    }
}

/// What has been read of a task container's two streams: the task's output
/// so far, each line sent to its log as it is read.
struct Taken<'a> {
    sink: Option<&'a crate::executor::LogSink>,
    name: &'a str,
    /// A line of stderr is noted in the engine's own log only at `debug`,
    /// masked as the local executor masks it: the mask is made only where
    /// the line is to be written.
    redactor: Option<crate::redact::Redactor>,
    lines: WholeLines,
    /// Every whole line, in the order each ended: the task's output, as much
    /// of it as is kept, each sent to its log as it is.
    kept: crate::executor::Gathered<'a>,
    /// How many bytes of each stream have been read, and what they were.
    out: (usize, std::hash::DefaultHasher),
    err: (usize, std::hash::DefaultHasher),
    /// Whether its streams were attached.
    attached: bool,
    /// When the attachment came to its end, where it did.
    ended_at: Option<std::time::Instant>,
    /// Whether the streams were read to their end as the task wrote them.
    whole: bool,
    /// Whether what was read back of a follow that was cut could not be
    /// placed after it, or did not come in time.
    unplaced: bool,
}

impl<'a> Taken<'a> {
    fn new(ctx: &'a ExecContext, name: &'a str, attached: bool) -> Self {
        // A task's stderr is part of its output, and nothing wrong with the
        // engine: it is kept in the task's log, and noted in the engine's own
        // only at `debug`. At `warn`, a job that succeeded drew a warning for
        // every line, and dagron-ci's step markers are stderr lines.
        let redactor = tracing::enabled!(tracing::Level::DEBUG)
            .then(|| crate::redact::Redactor::from_task_env(&ctx.env));
        Self {
            sink: ctx.log_sink.as_ref(),
            name,
            redactor,
            lines: WholeLines::default(),
            kept: crate::executor::Gathered::new(ctx.log_sink.as_ref()),
            out: Default::default(),
            err: Default::default(),
            attached,
            ended_at: None,
            whole: false,
            unplaced: false,
        }
    }

    /// A frame of the container's output. The two streams come as frames
    /// that need not end with a line: each stream's unfinished line waits for
    /// its end (`WholeLines`).
    fn frame(&mut self, frame: LogOutput) {
        use std::hash::Hasher;
        let whole = match frame {
            LogOutput::StdOut { message } => {
                self.out.0 += message.len();
                self.out.1.write(&message);
                self.lines.stdout(&message)
            }
            LogOutput::StdErr { message } => {
                self.err.0 += message.len();
                self.err.1.write(&message);
                let whole = self.lines.stderr(&message);
                if let (Some(redactor), Some(whole)) = (&self.redactor, &whole) {
                    tracing::debug!(
                        container = %self.name,
                        stderr = %redactor.redact(whole.trim()),
                        "container stderr"
                    );
                }
                whole
            }
            _ => None,
        };
        if let Some(whole) = whole {
            self.take(whole);
        }
    }

    fn take(&mut self, whole: String) {
        self.kept.push(&whole);
    }

    /// What an attached container's streams gave while it was waited on:
    /// a frame, their end, or a cut.
    fn followed(
        &mut self,
        item: Option<std::result::Result<LogOutput, bollard::errors::Error>>,
        follow: &mut Option<AttachContainerResults>,
    ) {
        match item {
            Some(Ok(frame)) => self.frame(frame),
            // Their end: the container's, or the attachment's alone, which
            // [`Self::settle`] tells apart.
            None => {
                self.ended_at = Some(std::time::Instant::now());
                *follow = None;
            }
            Some(Err(e)) => {
                tracing::warn!(container = %self.name, error = %e, "the task's output stopped being followed; the rest is read back once it has stopped");
                *follow = None;
            }
        }
    }

    /// Read an attached container's streams to their end, once it has
    /// stopped: within [`FOLLOW_DRAIN`](crate::executor::FOLLOW_DRAIN).
    async fn drain(&mut self, mut attached: AttachContainerResults) {
        let drained = timeout(crate::executor::FOLLOW_DRAIN, async {
            while let Some(item) = attached.output.next().await {
                match item {
                    Ok(frame) => self.frame(frame),
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
        .await;
        match drained {
            Ok(Ok(())) => self.whole = true,
            Ok(Err(e)) => {
                tracing::warn!(container = %self.name, error = %e, "the end of the task's output could not be read as it was written; it is read back")
            }
            Err(_) => {
                tracing::warn!(container = %self.name, "the end of the task's output did not come; it is read back")
            }
        }
    }

    /// Whether the attachment's end, where it came, was the container's
    /// (`waited_at` is when the wait ended): one that came longer than
    /// `slack` before was cut, by something between this and the daemon.
    fn settle(&mut self, waited_at: std::time::Instant, slack: Duration) {
        if let Some(at) = self.ended_at {
            if at + slack >= waited_at {
                self.whole = true;
            } else {
                tracing::warn!(container = %self.name, "the task's output stopped being followed well before it ended; the rest is read back");
            }
        }
    }

    /// The task's output, its log sealed with it: a last line that never got
    /// its newline, and what waited when the reading ran out of time,
    /// included, and a log past its cap given its end. And, where the follow
    /// was cut and what was read back after it could not be placed or did
    /// not come, a line of the log says so, after that end.
    fn end(mut self) -> (String, usize) {
        if let Some(left) = self.lines.rest() {
            self.take(left);
        }
        self.kept.seal();
        if let Some(sink) = self.sink {
            if self.attached && !self.whole && self.unplaced {
                sink.note(
                    "[dagron] this task's output stopped being followed before it ended, and \
                     what the container runtime kept of it could not be read on from there: \
                     what it wrote after that may not be in this log\n",
                );
            }
        }
        self.kept.output()
    }
}

/// How much of each stream of a container's log, read back, was read
/// already as it was written.
struct Skip {
    out: Placing,
    err: Placing,
}

/// One stream of a container's log read back, after what was followed of
/// it: so many bytes, which hashed so. Where the daemon's copy does not begin
/// with them (it dropped some, or rotated them away), what comes after
/// cannot be placed, and none of it is taken.
struct Placing {
    left: usize,
    want: u64,
    seen: std::hash::DefaultHasher,
    lost: bool,
}

impl Placing {
    fn new(followed: usize, want: u64) -> Self {
        Self {
            left: followed,
            want,
            seen: Default::default(),
            lost: false,
        }
    }

    /// What of `message` comes after what was followed.
    fn past(&mut self, message: bytes::Bytes) -> Option<bytes::Bytes> {
        use std::hash::Hasher;
        if self.lost {
            return None;
        }
        if self.left == 0 {
            return Some(message);
        }
        let passed = self.left.min(message.len());
        self.seen.write(&message[..passed]);
        self.left -= passed;
        if self.left == 0 && self.seen.finish() != self.want {
            self.lost = true;
            return None;
        }
        (passed < message.len()).then(|| message.slice(passed..))
    }

    /// Whether what was followed was found, and what comes after is placed.
    fn placed(&self) -> bool {
        self.left == 0 && !self.lost
    }
}

impl Skip {
    /// What of `frame` comes after what was read already.
    fn past(&mut self, frame: LogOutput) -> Option<LogOutput> {
        match frame {
            LogOutput::StdOut { message } => self
                .out
                .past(message)
                .map(|message| LogOutput::StdOut { message }),
            LogOutput::StdErr { message } => self
                .err
                .past(message)
                .map(|message| LogOutput::StdErr { message }),
            other => Some(other),
        }
    }
}

impl DockerExecutor {
    /// Fetch the task's actions into a volume of their own, create the task's
    /// container `name` with it, fetch its checkout into its workspace, attach
    /// to it, start and wait on it while what it writes is read into its log,
    /// and read what is left of that once it has stopped. The task's container
    /// is killed where it has not ended, never removed here: the caller
    /// removes it after the containers that mount its workspace.
    ///
    /// A task with services has their containers made before its own, which
    /// is made in the first one's network, and has them started and seen
    /// ready after its checkout and before it starts: the order a pod's are
    /// in. The caller removes them after the task's, and the container that
    /// holds its actions' volume after them. Its steps are taken back as it
    /// ends ([`Self::take_back`]); the caller takes them back again, for
    /// those not seen removed.
    #[allow(clippy::too_many_arguments)]
    async fn run_container(
        &self,
        ctx: &ExecContext,
        name: &str,
        init: Option<&InitVolumes>,
        checkout: Option<&CheckoutSpec>,
        actions: &[ActionSpec],
        secs: u64,
        making: &Making,
    ) -> Result<ExecOutput> {
        let image = self.resolved(
            run_of(ctx),
            ctx.docker_image.as_deref().unwrap_or(&self.default_image),
        );
        let image = image.as_str();

        // ── Actions, first, into a volume of their own ──────────────────────
        // Made by a container that holds it and never runs, so that the
        // task's own is made with that volume by name: the caller removes
        // the holder after the task's. The fetch, which carries the actions'
        // credentials, is gone before the task's container is made.
        let actions_volume = if actions.is_empty() {
            None
        } else {
            Some(self.fetch_actions(ctx, actions, name, secs, making).await?)
        };

        // ── Create container ────────────────────────────────────────────────
        // After a checkout another container made, git in the task is told the
        // workspace is safe: its files may be another user's.
        let mut extra_env = if checkout.is_some() {
            crate::checkout::task_env(&ctx.env)
        } else {
            Vec::new()
        };
        // Where its actions are.
        if !actions.is_empty() {
            extra_env.push((ENV_ACTIONS_DIR.to_string(), ACTIONS_DIR.to_string()));
        }
        let first = self.make_services(ctx, name, init, making).await?;
        let mut config =
            self.container_config(ctx, image, init, actions_volume.as_deref(), &extra_env);
        if let (Some(first), Some(host)) = (&first, config.host_config.as_mut()) {
            host.network_mode = Some(network_of(first));
        }
        self.create(making, name, config).await?;

        // ── Checkout, into the workspace the task was just given ────────────
        if let Some(checkout) = checkout {
            let fetched = match init.and_then(|i| i.workspace.clone()) {
                Some(volume) => Ok(volume),
                None => self.volumes_of(name).await.and_then(|mut v| {
                    v.remove(WORKSPACE_DIR).ok_or_else(|| {
                        anyhow::anyhow!("the task's container has no volume at {WORKSPACE_DIR}")
                    })
                }),
            };
            let fetched = match fetched {
                Ok(volume) => {
                    self.fetch_checkout(ctx, checkout, &volume, &checkout_name(name), secs, making)
                        .await
                }
                Err(e) => Err(e),
            };
            // Never started: the caller removes it after the checkout's, or
            // leaves it where that is not seen gone and its volume is the
            // workspace.
            fetched?;
        }

        // ── Services, each ready before the task has started ────────────────
        self.start_services(ctx, name, secs).await?;

        // What its steps' containers are lent while it runs: before it
        // starts, since its first step may ask for one at once.
        self.lend(ctx, name, init, first.as_deref()).await;

        // ── Attach, before it starts ────────────────────────────────────────
        // What it writes is read from its own streams as it writes them, from
        // its first byte, and is its log while it runs: not what
        // its log driver kept of them, read back once it is over, which may
        // not be all of it.
        let attached = self.attach(name).await;

        // ── Start ───────────────────────────────────────────────────────────
        if let Err(e) = self.docker.start_container::<String>(name, None).await {
            if let Err(why) = &attached {
                tracing::debug!(container = %name, error = %why, "could not attach to the task's container before it failed to start");
            }
            let _ = self.take_back(name).await;
            return Err(e.into());
        }
        let mut follow = match attached {
            Ok(attached) => Some(attached),
            Err(why) => {
                self.say_not_attached(name, &why);
                None
            }
        };

        // ── Wait (with hard timeout), reading what it writes meanwhile ──────
        let mut taken = Taken::new(ctx, name, follow.is_some());
        let waiting = timeout(Duration::from_secs(secs), async {
            self.docker
                .wait_container::<String>(name, None::<WaitContainerOptions<String>>)
                .try_next()
                .await
        });
        let wait_result = reading(waiting, &mut follow, &mut taken).await;
        let waited_at = std::time::Instant::now();

        let ended = match wait_result {
            Ok(Ok(Some(response))) => Ok(response.status_code),
            // bollard reports a non-zero exit as an error carrying the code. It
            // is the task's failure, and its output is what says why: read it
            // to its end like a success's, rather than removing the container
            // first and keeping only "Docker container wait error".
            Ok(Err(bollard::errors::Error::DockerContainerWaitError { code, .. })) => Ok(code),
            Ok(Ok(None)) => Err(anyhow::anyhow!(
                "container '{name}' wait stream ended unexpectedly"
            )),
            Ok(Err(e)) => Err(e.into()),
            // The typed TimeoutError, so that the worker can gate
            // retry-on-timeout (#24).
            Err(_) => Err(anyhow::Error::new(crate::executor::TimeoutError { secs })),
        };
        // One that did not end on its own, or whose end was not seen, is
        // ended: what it wrote last is then read to its end, and it is not
        // left running while that is. Read meanwhile, as everything here is:
        // a daemon waits for a reader that is slow, and gives what it holds
        // for one only moments once the container has stopped.
        let ran_on = match ended {
            Ok(_) => false,
            Err(_) => reading(self.kill_running(name), &mut follow, &mut taken).await,
        };
        // The task is over, however it went: so is every step of it, once it
        // has been ended above.
        let _ = reading(self.take_back(name), &mut follow, &mut taken).await;

        // ── The rest of what it wrote ───────────────────────────────────────
        // Bounded, like `KubeExecutor`'s follow once its pod is over, for the
        // same reason: a stall here costs no task compute, it costs the
        // worker, which would sit here for good and never hand the stopped
        // container back to be removed. What was read before the deadline is
        // kept.
        if let Some(attached) = follow.take() {
            taken.drain(attached).await;
        }
        // Where the container had stopped (the wait answered, or the kill
        // found it stopped: it ended a moment before the deadline), an end
        // of the attachment a moment before was its end. Where it ran on, an
        // end before the wait gave up was a cut.
        let slack = if ran_on {
            Duration::ZERO
        } else {
            ATTACH_END_SLACK
        };
        taken.settle(waited_at, slack);
        // What was not read as it was written, once it has stopped: all of it
        // where its streams were not attached, as before they could be; and
        // after what was, where the follow was cut.
        if !taken.whole {
            let secs = if ended.is_ok() {
                secs
            } else {
                crate::executor::FOLLOW_DRAIN.as_secs()
            };
            self.read_back(name, &mut taken, secs).await;
        }
        let (output, dropped) = taken.end();

        Ok(ExecOutput {
            success: ended? == 0,
            output,
            dropped,
        })
    }

    /// Stop and remove every container this project owns for the same task
    /// from an **earlier claim**, and see them gone, before starting this one.
    ///
    /// The counterpart to `KubeExecutor::reap_predecessors`, and it exists for
    /// the same reason: a lease bounds which scheduler owns the task *row*, and
    /// bounded nothing about the container. When a lease expired mid-run the
    /// reclaiming scheduler started a second container while the first was
    /// still executing the same command; so did a rerun of a cancelled run.
    ///
    /// **One attempt, one workload.** A predecessor is told to stop and given
    /// the task's grace, as its own worker would have given it, then removed.
    /// Then the task's containers are listed again. If a predecessor is still
    /// there, or the daemon would not say, this dispatch starts nothing:
    /// `Err(Backpressure)`, which the engine answers by putting the task back
    /// in its queue with no attempt spent and trying again shortly. A task
    /// whose grace is longer than [`REAP_BUDGET_SECS`] is handed back that way
    /// until its predecessor has had its grace.
    ///
    /// Only what is provably older is touched ([`crate::executor::is_predecessor`]):
    /// a newer claim's container, and one that carries no proof, are left
    /// alone and do not hold this dispatch up.
    async fn reap_predecessors(
        &self,
        ctx: &ExecContext,
    ) -> std::result::Result<(), crate::executor::Backpressure> {
        let Some(id) = &ctx.identity else { return Ok(()) };
        let Some(task) = crate::executor::label_value(&id.task_id) else { return Ok(()) };
        let still_there = |why: String| crate::executor::Backpressure {
            reason: format!("an earlier attempt of this task may still be running: {why}"),
        };
        // The whole reap runs inside one deadline, so a wedged daemon costs a
        // bounded dispatch delay rather than one per leftover.
        let budget = tokio::time::Instant::now() + Duration::from_secs(REAP_BUDGET_SECS);

        self.end_earlier_loans(id, budget).await;
        let found = self.predecessors(&task, id, budget).await.map_err(&still_there)?;
        if found.is_empty() {
            return Ok(());
        }
        let grace = ctx.stop_grace_secs() + STOP_MARGIN_SECS;
        // Together: the grace is each container's own, not theirs in turn.
        // A container that made a volume others of its attempt mount by name
        // goes only once they are gone, as listed (`held_back`): its init
        // container and its actions' holder after the rest (what they made,
        // the workspace, the tools and the actions, is mounted by the task,
        // the workspace by its Docker daemon too), and the task's own, which
        // made the workspace where no init container did, after its steps'
        // and its checkout's. A volume still mounted is not removed with the
        // container that made it, nor with one that mounted it by name. So
        // the first wave stops all but the holders, and removes those that
        // nothing holds back; the second, listed again, removes what that
        // freed, and what the first did not see removed. One whose attempt
        // still has another is left for the next try.
        let (holders, rest): (Vec<_>, Vec<_>) =
            found.iter().cloned().partition(|(_, names)| holds(names));
        let free: Vec<String> = not_held_back(&found)
            .into_iter()
            .map(|(cid, _)| cid)
            .collect();
        self.reap_wave(&rest, id, grace, budget, |cid| {
            free.iter().any(|f| f == cid)
        })
        .await;
        let listed = if rest.is_empty() {
            holders
        } else {
            self.predecessors(&task, id, budget)
                .await
                .map_err(&still_there)?
        };
        self.reap_wave(&not_held_back(&listed), id, grace, budget, |_| true)
            .await;

        let mut left = self
            .predecessors(&task, id, budget)
            .await
            .map_err(&still_there)?;
        // Once more for what is left, the holders last and as above. A
        // service's container whose network another was made in cannot be
        // removed on Podman while that other is there, and all of them were
        // removed at once: by now it can.
        if !left.is_empty() {
            let mut now = not_held_back(&left);
            now.sort_by_key(|(_, names)| holds(names));
            for (cid, _) in &now {
                let _ = tokio::time::timeout_at(budget, self.force_remove(cid)).await;
            }
            left = self
                .predecessors(&task, id, budget)
                .await
                .map_err(&still_there)?;
        }
        if left.is_empty() {
            Ok(())
        } else {
            Err(still_there(format!(
                "{} of its containers still stopping ({})",
                left.len(),
                left.iter()
                    .map(|(cid, _)| &cid[..cid.len().min(12)])
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
        }
    }

    /// Stop each of `wave`, given `grace`, together, and remove each that
    /// `removes` names once it has stopped.
    async fn reap_wave(
        &self,
        wave: &[(String, Vec<String>)],
        id: &crate::executor::TaskIdentity,
        grace: u64,
        budget: tokio::time::Instant,
        removes: impl Fn(&str) -> bool + Sync,
    ) {
        let removes = &removes;
        futures_util::future::join_all(wave.iter().map(|(cid, _)| async move {
            tracing::warn!(
                task_id = %id.task_id, container = %cid, current_fence = %id.fence,
                "stopping a container left by an earlier attempt of this task before starting this one"
            );
            let stopped = tokio::time::timeout_at(
                budget,
                self.docker.stop_container(cid, Some(StopContainerOptions { t: grace as i64 })),
            )
            .await;
            // Said once the daemon has said it stopped, which is before this
            // attempt's own container is made: what shows the order. A
            // container that had exited by then (304) is stopped all the same.
            let already = matches!(
                stopped,
                Ok(Err(bollard::errors::Error::DockerResponseServerError { status_code: 304, .. }))
            );
            if already || matches!(stopped, Ok(Ok(()))) {
                tracing::warn!(
                    task_id = %id.task_id, container = %cid, current_fence = %id.fence, already,
                    "stopped a container left by an earlier attempt of this task before starting this one"
                );
            }
            // Out of time while it still had its grace: it is not killed for
            // this dispatch's convenience. The next try finds it stopped.
            if stopped.is_ok() && removes(cid) {
                let _ = tokio::time::timeout_at(budget, self.force_remove(cid)).await;
            }
        }))
        .await;
    }

    /// The ids of the containers this project owns for `task` that are
    /// provably from an earlier claim than `id`, each with its names, which
    /// say whether it holds volumes its attempt's others mount by name
    /// ([`holds`]). `Err` says why the daemon could not be asked: what cannot
    /// be seen cannot be called gone.
    async fn predecessors(
        &self,
        task: &str,
        id: &crate::executor::TaskIdentity,
        budget: tokio::time::Instant,
    ) -> std::result::Result<Vec<(String, Vec<String>)>, String> {
        let mut filters = std::collections::HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![
                format!("{}={}", crate::executor::LABEL_MANAGED_BY, crate::executor::MANAGED_BY),
                format!("{}={}", crate::executor::LABEL_TASK_ID, task),
            ],
        );
        let listed = tokio::time::timeout_at(
            budget,
            self.docker.list_containers(Some(ListContainersOptions {
                all: true,
                filters,
                ..Default::default()
            })),
        )
        .await
        .map_err(|_| "listing its containers timed out".to_string())?
        .map_err(|e| format!("its containers could not be listed: {e}"))?;
        Ok(listed
            .into_iter()
            .filter(|c| {
                // A strictly lower fence only — see `is_predecessor`. A higher
                // one is a newer claim that is very likely alive, and this one
                // may be a stalled predecessor that only just woke up. By the
                // fence and not the attempt: a rerun sets the attempt back, and
                // what a cancelled attempt left would pass for this dispatch's.
                let label = |key: &str| c.labels.as_ref().and_then(|l| l.get(key)).map(String::as_str);
                crate::executor::is_predecessor(
                    label(crate::executor::LABEL_FENCE),
                    label(crate::executor::LABEL_ATTEMPT),
                    id,
                )
            })
            .filter_map(|c| c.id.map(|cid| (cid, c.names.unwrap_or_default())))
            .collect())
    }

    /// Stop and remove what the attempt whose task container is `name` made,
    /// because its claim on the task is gone: a cancel, a run past its
    /// deadline, a lease another scheduler took.
    ///
    /// **By the names this attempt made**, and nothing else. The task's labels
    /// would select a later attempt's containers too, and a stalled worker
    /// that wakes up to find its claim gone must not take its successor's work
    /// down with its own.
    ///
    /// The task container is told to stop and given the task's grace
    /// ([`ExecContext::stop_grace_secs`]) before it is killed, so that a job's
    /// step can end as its `CI_STEP_GRACE_SECS` promises. Then what the
    /// attempt made is removed with its volumes, each whether or not it
    /// exists: which of them this attempt had reached is not known here, and
    /// a remove of nothing is nothing. What mounts the task's workspace by
    /// name goes first, its steps' containers and its checkout's, and its
    /// actions' fetch with them, which mounts only the actions' volume. Then
    /// the task's own: where it made its workspace (no init container did),
    /// only once its steps' and its checkout's are seen gone, and otherwise
    /// killed and left. Then its services', the first of them last
    /// of them ([`Self::remove_services`]), given no grace: a service is there
    /// for the task, which has ended. The actions' holder and the init's go
    /// last of all, since the task mounts their volumes by name, and only
    /// once the rest are seen gone.
    ///
    /// `Ok` once the daemon has said each is gone: removed, or not there.
    /// `Err` names those it did not say that of, and why: a removal it
    /// refused or did not answer leaves a container that may still be running,
    /// and the caller must not say the work was stopped. It names a holder,
    /// or the task's own container, left for that reason too.
    ///
    /// The caller holds the attempt's [`Making`] by now, so a container the
    /// dropped attempt had asked the daemon for has been made, and is one of
    /// those removed here. A start it had in flight can still land: on a
    /// container that is gone, which the daemon refuses.
    async fn stop_and_remove(
        &self,
        ctx: &ExecContext,
        name: &str,
    ) -> std::result::Result<(), String> {
        let grace = ctx.stop_grace_secs() + STOP_MARGIN_SECS;
        let stopped = timeout(
            Duration::from_secs(grace + CLEANUP_TIMEOUT_SECS),
            self.docker.stop_container(name, Some(StopContainerOptions { t: grace as i64 })),
        )
        .await;
        match stopped {
            Ok(Ok(())) => tracing::info!(container = %name, "stopped: the task's claim was lost"),
            // Not made yet, or gone already: nothing to stop.
            Ok(Err(bollard::errors::Error::DockerResponseServerError { status_code: 304 | 404, .. })) => {}
            Ok(Err(e)) => {
                tracing::warn!(container = %name, error = %e, "could not stop a container whose claim was lost; removing it")
            }
            Err(_) => {
                tracing::warn!(container = %name, "stopping a container whose claim was lost timed out; removing it")
            }
        }
        let mut unseen = Vec::new();
        let lost = |made: &str, why: String| {
            tracing::warn!(
                container = %made, %why,
                "a container whose claim was lost was not seen removed; it may still be running"
            );
            format!("container {made} {why}")
        };
        // What mounts its workspace by name first: its steps' containers,
        // taken back (they hold its network too), and its checkout's. Then
        // its actions' fetch, which does not mount the workspace.
        for (step, why) in self.take_back(name).await {
            unseen.push(format!("container {step} {why}"));
        }
        // Each asked for again while a removal the dropped attempt had asked
        // for goes on, which the daemon answers with a conflict.
        if let Err(why) = self.removed_within(&checkout_name(name)).await {
            unseen.push(lost(&checkout_name(name), why));
        }
        let workspace_free = unseen.is_empty();
        if let Err(why) = self.removed_within(&actions_fetch_name(name)).await {
            unseen.push(lost(&actions_fetch_name(name), why));
        }
        // Then the task's own. Where it made its workspace, a volume its
        // steps' and its checkout's mount by name, it goes only once those
        // are seen gone, and is otherwise killed and left, and said: the next
        // teardown removes it after them.
        let owns = self.makes_workspace(ctx) && self.ci_exec_image.is_none();
        if workspace_free || !owns {
            if let Err(why) = self.removed_within(name).await {
                unseen.push(lost(name, why));
            }
        } else {
            self.kill(name).await;
            if !self.absent(name).await {
                unseen.push(format!(
                    "container {name} was left: its workspace is a volume one of those may still \
                     mount"
                ));
            }
        }
        // Then its services', the first of them last, each killed first.
        for i in (0..ctx.services.len()).rev() {
            let service = service_name(name, i);
            self.kill(&service).await;
            if let Err(why) = self.removed_within(&service).await {
                unseen.push(lost(&service, why));
            }
        }
        // Its actions' holder and its init container last, and only once
        // all of those are seen gone: the volumes they made are mounted by
        // name by the task (the workspace by its Docker daemon too, a
        // service, and by its steps), and each goes with its container only
        // once nothing mounts it. Otherwise they are left, and said: the next
        // teardown removes them after the rest.
        let rest_gone = unseen.is_empty();
        for holder in [actions_name(name), init_name(name)] {
            if rest_gone {
                if let Err(why) = self.removed_within(&holder).await {
                    unseen.push(lost(&holder, why));
                }
            } else if !self.absent(&holder).await {
                unseen.push(format!(
                    "container {holder} was left: it holds a volume one of those may still mount"
                ));
            }
        }
        if unseen.is_empty() {
            Ok(())
        } else {
            Err(unseen.join("; "))
        }
    }

    /// Lend what the task in `name` has to the containers of its steps, as it
    /// starts: where it has a workspace of this executor's making, and an
    /// identity to be found by. Under `name`, and as the task's only loan
    /// that has not ended: one an earlier claim's attempt of the task on this
    /// engine still holds ends here. Unless a later claim's loan is here
    /// already: then this attempt's claim was lost before it got this far,
    /// and its loan is ended as it is made, so that it never ends the live
    /// one's.
    async fn lend(
        &self,
        ctx: &ExecContext,
        name: &str,
        init: Option<&InitVolumes>,
        first: Option<&str>,
    ) {
        let Some((task_id, fence)) = ctx.identity.as_ref().map(|i| (i.task_id.clone(), i.fence))
        else {
            return;
        };
        if !self.makes_workspace(ctx) {
            return;
        }
        let workspace = match init.and_then(|i| i.workspace.clone()) {
            Some(volume) => volume,
            None => match self
                .volumes_of(name)
                .await
                .ok()
                .and_then(|mut v| v.remove(WORKSPACE_DIR))
            {
                Some(volume) => volume,
                None => return,
            },
        };
        let mut host = self.host_config(
            ctx,
            Made::Task,
            vec![
                named_volume(&workspace, WORKSPACE_DIR, false),
                named_volume(&workspace, STEP_WORKSPACE_DIR, false),
            ],
        );
        // Its network is the task's: the first service's, which the task
        // joined, else the task's own.
        host.network_mode = Some(network_of(first.unwrap_or(name)));
        let mut lent = Lent {
            task_id: task_id.clone(),
            fence,
            host,
            user: ctx
                .isolation
                .as_ref()
                .and_then(|iso| iso.run_as_user)
                .map(|uid| uid.to_string()),
            labels: identity_labels(ctx),
            isolation: ctx.isolation.clone(),
            pull: ctx.image_pull_policy,
            run: run_of(ctx).map(str::to_string),
            trusted: ctx.confinement.is_none(),
            env: self
                .proxy_env(ctx)
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect(),
            made: 0,
            running: Vec::new(),
            ended: false,
            steps: Arc::default(),
        };
        let mut all = self.lent.lock().unwrap();
        let later = all
            .values()
            .any(|l| l.task_id == task_id && l.fence > fence);
        if later {
            lent.ended = true;
        } else {
            for earlier in all.values_mut().filter(|l| l.task_id == task_id) {
                earlier.ended = true;
            }
        }
        all.insert(name.to_string(), lent);
    }

    /// End the loans of the task's earlier claims here, before the reap
    /// looks for what they made, and wait, until `budget` at most, for each
    /// request making a step's container on one to be over: what it made is
    /// then there for the reap to find, and nothing is made on them after.
    ///
    /// An attempt of a lost claim can still be under way on this engine,
    /// stalled, with its loan not yet ended: the next claim's [`Self::lend`]
    /// comes only after this reap. A step's container made on that loan once
    /// the reap had removed the task's workspace would mount the volume by
    /// name, which makes it again, empty, and no one would remove it.
    async fn end_earlier_loans(
        &self,
        id: &crate::executor::TaskIdentity,
        budget: tokio::time::Instant,
    ) {
        let making: Vec<_> = {
            let mut all = self.lent.lock().unwrap();
            all.values_mut()
                .filter(|l| l.task_id == id.task_id && l.fence < id.fence)
                .map(|l| {
                    l.ended = true;
                    Arc::clone(&l.steps)
                })
                .collect()
        };
        for steps in making {
            let _ = tokio::time::timeout_at(budget, steps.write_owned()).await;
        }
    }

    /// The task in `task` is over: what it lent is taken back, and each of
    /// its steps' containers still there is stopped and removed. Those not
    /// seen removed, each with why: they mount the task's workspace by name.
    ///
    /// The loan is ended first, so that no step's container is made for the
    /// task from then on, and kept ([`Loan`]), so that one not seen removed
    /// is asked for again by the next call: the attempt's end, or the stop,
    /// which finds it whatever became of the attempt. A step's container
    /// that was being made as it ended is waited for, [`CLEANUP_TIMEOUT_SECS`]
    /// at most, and is among those removed: each request that makes one holds
    /// its share of the loan's `steps` to its end ([`Self::create_under`]),
    /// and this takes them whole first. One still being made after that is
    /// not waited for further: each step's container there is then killed,
    /// that one's a kill of nothing, which is never started on an ended loan,
    /// and none is seen removed.
    ///
    /// Neither this nor a step's create takes the attempt's [`Making`]: the
    /// stop holds that while it takes the steps back.
    async fn take_back(&self, task: &str) -> Vec<(String, String)> {
        let steps = {
            let mut all = self.lent.lock().unwrap();
            let Some(lent) = all.get_mut(task) else {
                return Vec::new();
            };
            lent.ended = true;
            Arc::clone(&lent.steps)
        };
        let made = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            steps.write_owned(),
        )
        .await;
        let Ok(made) = made else {
            let running = self.running_of(task);
            for step in &running {
                self.kill(step).await;
            }
            return running
                .into_iter()
                .map(|step| {
                    let why = format!(
                        "was killed and not seen removed: a step's container was still being \
                         made after {CLEANUP_TIMEOUT_SECS} s"
                    );
                    (step, why)
                })
                .collect();
        };
        drop(made);
        let mut left = Vec::new();
        for step in self.running_of(task) {
            self.kill(&step).await;
            match self.step_removed_within(&step).await {
                Ok(()) => self.settle(task, &step),
                Err(why) => {
                    tracing::warn!(container = %step, %why, "a step's container was not seen removed");
                    left.push((step, why));
                }
            }
        }
        left
    }

    /// What the task `task_id` lends its steps, with its task container's
    /// name: the loan that has not ended, of which there is one at most.
    fn loan_of(&self, task_id: &str) -> Option<(String, Lent)> {
        self.lent
            .lock()
            .unwrap()
            .iter()
            .find(|(_, l)| l.task_id == task_id && !l.ended)
            .map(|(task, l)| (task.clone(), l.clone()))
    }

    /// The containers of the steps of the task in `task` not yet seen
    /// removed.
    fn running_of(&self, task: &str) -> Vec<String> {
        self.lent
            .lock()
            .unwrap()
            .get(task)
            .map(|l| l.running.clone())
            .unwrap_or_default()
    }

    /// The step's container `step`, of the task in `task`, is seen removed.
    fn settle(&self, task: &str, step: &str) {
        if let Some(lent) = self.lent.lock().unwrap().get_mut(task) {
            lent.running.retain(|s| s != step);
        }
    }

    /// [`Self::remove_step`], asked again while it errs, for
    /// [`CLEANUP_TIMEOUT_SECS`] at most: a removal a dropped call asked for
    /// goes on in the daemon, which answers another with a conflict until it
    /// is done. Why it was not seen removed, where it was not.
    async fn step_removed_within(&self, name: &str) -> std::result::Result<(), String> {
        let mut why = None;
        let removed = timeout(Duration::from_secs(CLEANUP_TIMEOUT_SECS), async {
            loop {
                match self.remove_step(name).await {
                    Ok(()) => return,
                    Err(e) => why = Some(e),
                }
                tokio::time::sleep(READY_POLL).await;
            }
        })
        .await;
        removed.map_err(|_| {
            why.unwrap_or_else(|| format!("was not removed within {CLEANUP_TIMEOUT_SECS} s"))
        })
    }

    /// Remove a step's container, and none of its volumes: they are the task's.
    async fn remove_step(&self, name: &str) -> std::result::Result<(), String> {
        let removed = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.remove_container(
                name,
                Some(RemoveContainerOptions {
                    force: true,
                    v: false,
                    ..Default::default()
                }),
            ),
        )
        .await;
        match removed {
            Ok(Ok(()))
            | Ok(Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            })) => Ok(()),
            Ok(Err(e)) => Err(format!("was not removed: {e}")),
            Err(_) => Err("was not removed in time".to_string()),
        }
    }

    /// A step container's run: made, started, waited on to its time, its
    /// output read, and removed. Its image is had first, outside the task's
    /// loan, so that a pull never holds up the task's end; then its name is
    /// taken under the loan, which has not ended, and its container is made
    /// holding a share of it ([`Self::run_step_in`]).
    async fn run_step(&self, task_id: &str, step: StepContainer) -> Result<StepOutcome> {
        let (task, lent) = self.loan_of(task_id).ok_or_else(|| {
            anyhow::anyhow!(
                "the task is not running on this engine, or has no workspace of the Docker \
                 executor's making for a step's container to share"
            )
        })?;
        // One built here for a step is had already, whatever the task's
        // policy: no registry has it.
        let built = step
            .image
            .strip_prefix(BUILT_STEP_IMAGE)
            .is_some_and(|tag| tag.starts_with(':'));
        // One built here is run by its id, as it was built; never pulled,
        // since a registry's image of the name is none of ours.
        let image = if built {
            match self.built_id(&step.image).await {
                Some(id) => id,
                None => anyhow::bail!(
                    "{} is no image this engine built and still holds as it built it: the \
                     step is to be run again, and its image built again",
                    step.image
                ),
            }
        } else {
            self.fetch_image(
                "the step's image",
                &step.image,
                lent.pull,
                lent.run.as_deref(),
                lent.trusted,
            )
            .await?
        };
        self.refuse_root(
            lent.isolation.as_ref(),
            "the step's image",
            &step.image,
            &image,
        )
        .await?;
        // A share of the loan, waited for while its task's end takes it whole.
        let share = Arc::clone(&lent.steps).read_owned().await;
        let name = {
            let mut all = self.lent.lock().unwrap();
            let Some(own) = all.get_mut(&task).filter(|l| !l.ended) else {
                anyhow::bail!("the task ended before its step's container was made");
            };
            own.made += 1;
            let name = step_name(&task, own.made);
            own.running.push(name.clone());
            name
        };
        let env: Vec<String> = lent
            .env
            .iter()
            .filter(|set| {
                let name = set.split_once('=').map_or(set.as_str(), |(n, _)| n);
                !step.env.iter().any(|(own, _)| own == name)
            })
            .cloned()
            .chain(step.env.iter().map(|(k, v)| format!("{k}={v}")))
            .collect();
        let config = Config {
            image: Some(image),
            entrypoint: step.entrypoint.clone().map(|e| vec![e]),
            cmd: (!step.args.is_empty()).then(|| step.args.clone()),
            env: (!env.is_empty()).then_some(env),
            labels: lent.labels.clone(),
            working_dir: Some(STEP_WORKSPACE_DIR.to_string()),
            user: lent.user.clone(),
            host_config: Some(lent.host.clone()),
            ..Default::default()
        };
        let ran = self
            .run_step_in(&task, &name, config, step.timeout, share)
            .await;
        // One not seen removed stays among the task's running steps, for
        // take_back to ask for again: it mounts the task's workspace by name.
        match self.remove_step(&name).await {
            Ok(()) => self.settle(&task, &name),
            Err(why) => {
                tracing::warn!(container = %name, %why, "a step's container was not seen removed")
            }
        }
        ran
    }

    /// A step's image, built from its Dockerfile where this daemon does not
    /// hold it already: with the limits of memory the task is held to, and
    /// in no longer than the step's time.
    async fn build_step(&self, task_id: &str, build: StepBuild) -> Result<StepBuilt> {
        if !self.build_actions {
            anyhow::bail!(
                "this engine's operator has not let it build an image from a Dockerfile for a \
                 step ({ENV_BUILD_ACTIONS}=allow)"
            );
        }
        let (_, lent) = self.loan_of(task_id).ok_or_else(|| {
            anyhow::anyhow!(
                "the task is not running on this engine, or has no workspace of the Docker \
                 executor's making for a step's container to share"
            )
        })?;
        if self.built_id(&build.image).await.is_some() {
            return Ok(StepBuilt {
                output: format!(
                    "{} was built here before, from the same files\n",
                    build.image
                ),
                image: build.image,
            });
        }
        // Podman reads an empty platform, which bollard always sends, as a
        // wrong one: the daemon's own is named.
        let platform = match self.docker.version().await {
            Ok(v) => match (v.os, v.arch) {
                (Some(os), Some(arch)) => format!("{os}/{arch}"),
                _ => String::new(),
            },
            Err(_) => String::new(),
        };
        let memory = lent
            .host
            .memory
            .and_then(|m| u64::try_from(m).ok())
            .filter(|m| *m > 0);
        let options = BuildImageOptions {
            dockerfile: build.dockerfile.clone(),
            t: build.image.clone(),
            rm: true,
            forcerm: true,
            memory,
            memswap: memory.and_then(|m| i64::try_from(m).ok()),
            labels: std::collections::HashMap::from([(
                "dagron.ci.step-image".to_string(),
                "built".to_string(),
            )]),
            platform,
            ..Default::default()
        };
        let mut output = String::new();
        let mut said = |line: &str| {
            let room = MAX_STEP_OUTPUT_BYTES.saturating_sub(output.len());
            if room > 0 {
                let mut end = line.len().min(room);
                while !line.is_char_boundary(end) {
                    end -= 1;
                }
                output.push_str(&line[..end]);
            }
        };
        let mut stream =
            self.docker
                .build_image(options, None, Some(bytes::Bytes::from(build.context)));
        let drained = timeout(build.timeout, async {
            while let Some(item) = stream.next().await {
                let info = item.map_err(|e| format!("the daemon would not build it: {e}"))?;
                if let Some(text) = info.stream.as_deref() {
                    said(text);
                }
                if let Some(error) = info.error {
                    return Err(error);
                }
            }
            Ok(())
        })
        .await;
        let tail = |output: &str| {
            let mut start = output.len().saturating_sub(4096);
            while !output.is_char_boundary(start) {
                start += 1;
            }
            output[start..].to_string()
        };
        match drained {
            Err(_) => anyhow::bail!(
                "its image was not built in the step's time ({}s); what the build said last:\n{}",
                build.timeout.as_secs(),
                tail(&output)
            ),
            Ok(Err(why)) => anyhow::bail!(
                "its image was not built: {}; what the build said last:\n{}",
                why.trim(),
                tail(&output)
            ),
            Ok(Ok(())) => {}
        }
        let id = match self
            .docker
            .inspect_image(&build.image)
            .await
            .ok()
            .and_then(|i| i.id)
        {
            Some(id) => id,
            None => anyhow::bail!(
                "the daemon said its image was built, and holds no {}; what the build said \
                 last:\n{}",
                build.image,
                tail(&output)
            ),
        };
        self.built.lock().unwrap().insert(build.image.clone(), id);
        Ok(StepBuilt {
            image: build.image,
            output,
        })
    }

    /// The id of the step's image this executor built under `name`, where
    /// the daemon holds that image under that name still.
    async fn built_id(&self, name: &str) -> Option<String> {
        let recorded = self.built.lock().unwrap().get(name).cloned()?;
        let held = self.docker.inspect_image(name).await.ok()?.id?;
        (held == recorded).then_some(recorded)
    }

    /// Make the container `name` of a step of the task in `task`, start it,
    /// wait on it to `limit`, and read what it said.
    ///
    /// The request that makes it holds `share`, the step's share of the
    /// task's loan ([`Lent::steps`]), to its end and no further: the step's
    /// run does not hold up the task's end, and [`Self::take_back`], which
    /// takes the loan whole first, finds a container that was being made as
    /// the task ended, and removes it. Neither takes the attempt's
    /// [`Making`], which the stop holds while it takes the steps back.
    async fn run_step_in(
        &self,
        task: &str,
        name: &str,
        config: Config<String>,
        limit: Duration,
        share: tokio::sync::OwnedRwLockReadGuard<()>,
    ) -> Result<StepOutcome> {
        self.create_under(share, name, config)
            .await
            .map_err(|e| anyhow::anyhow!("the step's container could not be made: {e}"))?;
        // The task may have ended while it was made: its end takes this back,
        // and it is not started.
        let open = self
            .lent
            .lock()
            .unwrap()
            .get(task)
            .is_some_and(|l| !l.ended);
        if !open {
            anyhow::bail!("the task ended before its step's container started");
        }
        self.docker
            .start_container::<String>(name, None)
            .await
            .map_err(|e| anyhow::anyhow!("the step's container could not be started: {e}"))?;
        let waited = timeout(limit, async {
            self.docker
                .wait_container::<String>(name, None::<WaitContainerOptions<String>>)
                .try_next()
                .await
        })
        .await;
        let (exit_code, timed_out) = match waited {
            Ok(Ok(Some(response))) => (response.status_code, false),
            Ok(Err(bollard::errors::Error::DockerContainerWaitError { code, .. })) => (code, false),
            Ok(Ok(None)) => anyhow::bail!("the step's container's wait ended unexpectedly"),
            Ok(Err(e)) => anyhow::bail!("the step's container could not be waited on: {e}"),
            Err(_) => {
                self.kill(name).await;
                (124, true)
            }
        };
        Ok(StepOutcome {
            exit_code,
            output: self.output_of(name).await,
            timed_out,
        })
    }

    /// What a stopped container said, both streams in their order, to
    /// [`MAX_STEP_OUTPUT_BYTES`].
    async fn output_of(&self, name: &str) -> String {
        let mut said: Vec<u8> = Vec::new();
        let mut cut = false;
        let mut logs = self.docker.logs::<String>(
            name,
            Some(LogsOptions {
                stdout: true,
                stderr: true,
                ..Default::default()
            }),
        );
        let _ = timeout(Duration::from_secs(CLEANUP_TIMEOUT_SECS * 6), async {
            while let Some(item) = logs.next().await {
                let bytes = match item {
                    Ok(LogOutput::StdOut { message }) | Ok(LogOutput::StdErr { message }) => {
                        message
                    }
                    _ => continue,
                };
                let room = MAX_STEP_OUTPUT_BYTES.saturating_sub(said.len());
                if bytes.len() > room {
                    said.extend_from_slice(&bytes[..room]);
                    cut = true;
                    break;
                }
                said.extend_from_slice(&bytes);
            }
        })
        .await;
        let mut text = String::from_utf8_lossy(&said).into_owned();
        if cut {
            text.push_str(&format!(
                "\n[the step's container said more than {} MiB: the rest is not kept]\n",
                MAX_STEP_OUTPUT_BYTES >> 20
            ));
        }
        text
    }

    /// Force-remove the named container, ignoring errors (best-effort cleanup).
    ///
    /// With its anonymous volumes (`v`): a workspace, or an init container's
    /// tools. A named volume an operator mounted is never one of them.
    ///
    /// Bounded, for the reason `KubeExecutor::cleanup` is bounded: most of the
    /// call sites run *after* `execute()` has already given up on the task,
    /// the timeout path among them. An unresponsive daemon here would hold the
    /// worker past the very ceiling that just expired — so the wall clock a task
    /// is promised would be `secs` plus however long the daemon takes to answer.
    ///
    /// A container left behind by a stalled remove is the smaller problem, and
    /// not a new one: this is best-effort either way, and the discarded `Result`
    /// already meant a failed remove leaks the container.
    async fn force_remove(&self, name: &str) {
        let _ = self.remove_seen(name).await;
    }

    /// [`Self::remove_seen`], asked again while it is not seen gone, for
    /// [`CLEANUP_TIMEOUT_SECS`] at most: a removal the caller gave up waiting
    /// for, or one an attempt dropped by a stop had asked for, goes on in the
    /// daemon, which answers another with a conflict until it is done. `Err`
    /// says what the daemon last said, where it never said the container
    /// was gone.
    async fn removed_within(&self, name: &str) -> std::result::Result<(), String> {
        let mut why = None;
        let removed = timeout(Duration::from_secs(CLEANUP_TIMEOUT_SECS), async {
            loop {
                match self.remove_seen(name).await {
                    Ok(()) => return,
                    Err(e) => why = Some(e),
                }
                tokio::time::sleep(READY_POLL).await;
            }
        })
        .await;
        removed.map_err(|_| {
            why.unwrap_or_else(|| format!("was not removed within {CLEANUP_TIMEOUT_SECS} s"))
        })
    }

    /// [`Self::force_remove`], saying whether the container is gone: removed,
    /// or not there to remove. `Err` says why the daemon did not say so.
    async fn remove_seen(&self, name: &str) -> std::result::Result<(), String> {
        let removed = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.remove_container(
                name,
                Some(RemoveContainerOptions { force: true, v: true, ..Default::default() }),
            ),
        )
        .await;
        match removed {
            Ok(Ok(())) => Ok(()),
            Ok(Err(bollard::errors::Error::DockerResponseServerError { status_code: 404, .. })) => Ok(()),
            Ok(Err(e)) => Err(format!("could not be removed: {e}")),
            Err(_) => Err(format!("was not removed within {CLEANUP_TIMEOUT_SECS} s")),
        }
    }

    /// Whether the daemon says there is no container `name`. One it does
    /// not answer for in [`CLEANUP_TIMEOUT_SECS`] may be there.
    async fn absent(&self, name: &str) -> bool {
        let there = timeout(
            Duration::from_secs(CLEANUP_TIMEOUT_SECS),
            self.docker.inspect_container(name, None),
        )
        .await;
        matches!(
            there,
            Ok(Err(bollard::errors::Error::DockerResponseServerError { status_code: 404, .. }))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stream of a log read back, of which `followed` was read as it was
    /// written: what of `frames` comes after it.
    fn placed_after(followed: &[u8], frames: &[&[u8]]) -> (Vec<u8>, bool) {
        use std::hash::Hasher;
        let mut hash = std::hash::DefaultHasher::default();
        hash.write(followed);
        let mut placing = Placing::new(followed.len(), hash.finish());
        let mut after = Vec::new();
        for frame in frames {
            if let Some(rest) = placing.past(bytes::Bytes::copy_from_slice(frame)) {
                after.extend_from_slice(&rest);
            }
        }
        (after, placing.placed())
    }

    /// What is read back of a follow that was cut goes on after what was
    /// followed, however the daemon's frames cut it.
    #[test]
    fn a_log_read_back_goes_on_after_what_was_followed() {
        let log = b"line 1
line 2
line 3
";
        for at in 0..=log.len() {
            for cut in 0..=log.len() {
                let (after, placed) = placed_after(&log[..at], &[&log[..cut], &log[cut..]]);
                assert!(placed, "followed {at}, framed at {cut}");
                assert_eq!(after, &log[at..], "followed {at}, framed at {cut}");
            }
        }
    }

    /// A daemon's copy that does not begin with what was followed (it
    /// dropped or rotated lines away), or that is shorter, cannot be placed:
    /// none of it is taken, and that is known.
    #[test]
    fn a_log_read_back_that_does_not_begin_with_what_was_followed_is_not_placed() {
        let (after, placed) = placed_after(
            b"line 1
line 2
",
            &[
                b"line 2
line 3
",
                b"line 4
",
            ],
        );
        assert_eq!((after.as_slice(), placed), (&b""[..], false));
        let (after, placed) = placed_after(
            b"line 1
line 2
",
            &[b"line 1
"],
        );
        assert_eq!((after.as_slice(), placed), (&b""[..], false));
    }

    /// An attachment that ended moments before its container was seen to
    /// stop ended with it; one that ended long before was cut; and, where
    /// the wait gave up, one that ended before was cut.
    #[test]
    fn an_attachment_that_ended_long_before_its_container_was_cut() {
        let ctx = ExecContext::new(vec!["true".into()], None, None);
        let now = std::time::Instant::now();
        let ended = |at: std::time::Instant, waited: std::time::Instant, slack: Duration| {
            let mut taken = Taken::new(&ctx, "c", true);
            taken.ended_at = Some(at);
            taken.settle(waited, slack);
            taken.whole
        };
        let later = |secs: u64| now + Duration::from_secs(secs);
        assert!(ended(now, later(2), ATTACH_END_SLACK));
        assert!(!ended(
            now,
            now + ATTACH_END_SLACK + Duration::from_secs(1),
            ATTACH_END_SLACK
        ));
        // A wait that gave up: the container ran on, and only an end once it
        // was being stopped is its end.
        assert!(!ended(now, later(2), Duration::ZERO));
        assert!(ended(later(2), now, Duration::ZERO));
        let mut taken = Taken::new(&ctx, "c", true);
        taken.settle(now, ATTACH_END_SLACK);
        assert!(!taken.whole, "an attachment that is still open is not over");
    }

    fn mount(source: &str, target: &str, read_only: bool) -> TaskMount {
        TaskMount { source: source.into(), target: target.into(), read_only }
    }

    /// A run's pins are kept while it is given images, dropped when it ends,
    /// and past the bound the run least recently given one goes first.
    #[test]
    fn a_run_s_pins_are_kept_while_it_runs() {
        let mut pins = Pins::default();
        pins.put("r-1", "alpine:3", "alpine@sha256:11".into());
        pins.put("r-2", "alpine:3", "alpine@sha256:22".into());
        assert_eq!(
            pins.get("r-1", "alpine:3").as_deref(),
            Some("alpine@sha256:11")
        );
        assert_eq!(
            pins.get("r-2", "alpine:3").as_deref(),
            Some("alpine@sha256:22")
        );
        pins.forget("r-1");
        assert_eq!(pins.get("r-1", "alpine:3"), None);
        for n in 0..PINNED_RUNS_KEPT {
            // r-2 is given its image now and then, as a live run is.
            if n % 1000 == 0 {
                assert!(
                    pins.get("r-2", "alpine:3").is_some(),
                    "a live run kept its pin"
                );
            }
            pins.put(&format!("other-{n}"), "busybox", "busybox@sha256:00".into());
        }
        assert_eq!(pins.runs.len(), PINNED_RUNS_KEPT);
        assert_eq!(
            pins.get("r-2", "alpine:3").as_deref(),
            Some("alpine@sha256:22")
        );
        assert_eq!(
            pins.get("other-0", "busybox"),
            None,
            "the run least recently given one went"
        );
    }

    fn bare_executor(docker: Docker) -> DockerExecutor {
        DockerExecutor {
            default_image: "alpine:latest".into(),
            mounts: Vec::new(),
            workdir: None,
            ci_exec_image: None,
            checkout_image: None,
            log_driver: LogDriver::default(),
            network: None,
            allowlists: BTreeMap::new(),
            daemon_runtime: None,
            build_actions: false,
            log_driver_judged: Arc::default(),
            first_container_judged: Arc::default(),
            attach_refused: Arc::default(),
            podman: false,
            podman_said: Arc::default(),
            lent: Arc::default(),
            built: Arc::default(),
            registries: crate::images::Registries::default(),
            pulling: Arc::default(),
            pins: Arc::default(),
            metrics: None,
            privileged: false,
            docker,
        }
    }

    /// The three containers of one attempt, as this executor would make them.
    fn made_for(exec: &DockerExecutor, ctx: &ExecContext) -> [Config<String>; 3] {
        let checkout = CheckoutSpec {
            repository: "https://example.com/acme/widgets.git".into(),
            commit: "a".repeat(40),
            depth: None,
            patched: false,
        };
        [
            exec.container_config(ctx, "rust:1", None, None, &[]),
            exec.init_config(ctx, "tools:1"),
            exec.checkout_config(ctx, &checkout, "git:1", "v-ws"),
        ]
    }

    /// Where the operator names a network, the task's container, the one
    /// that installs its tools and the one that fetches its checkout are all
    /// made on it, and on no other. Where they name none, none is named, and
    /// each gets the daemon's default, as it always did.
    #[test]
    fn every_container_is_on_the_operator_s_network_and_none_is_named_otherwise() {
        let ctx = workspace_ctx();
        let networks = |exec: &DockerExecutor| {
            made_for(exec, &ctx).map(|c| c.host_config.and_then(|h| h.network_mode))
        };
        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        assert_eq!(networks(&exec), [None, None, None]);

        exec.network = Some("jobs".into());
        let jobs = Some("jobs".to_string());
        assert_eq!(networks(&exec), [jobs.clone(), jobs.clone(), jobs]);
    }

    fn on_allowlist(name: &str) -> ExecContext {
        let mut ctx = workspace_ctx();
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            network: Some(dagron_core::isolation::Network::Allow(name.into())),
            ..Default::default()
        });
        ctx
    }

    fn allowing(proxy: Option<&str>) -> DockerExecutor {
        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        exec.network = Some("jobs".into());
        exec.allowlists.insert(
            "registry".into(),
            Allowlist {
                network: "dagron-task-allow-registry".into(),
                proxy: proxy.map(str::to_string),
            },
        );
        exec
    }

    /// A task held to an allowlist is on the operator's network for it, and
    /// the two run for it first on the operator's jobs network: a checkout
    /// is the operator's to fetch. An allowlist this executor was not told
    /// of leaves the task with no network at all, were it ever asked for:
    /// it is refused before anything is made.
    #[test]
    fn a_task_held_to_an_allowlist_is_on_its_network_alone() {
        let exec = allowing(None);
        let networks = |ctx: &ExecContext| {
            made_for(&exec, ctx).map(|c| c.host_config.and_then(|h| h.network_mode))
        };
        let jobs = Some("jobs".to_string());
        assert_eq!(
            networks(&on_allowlist("registry")),
            [
                Some("dagron-task-allow-registry".into()),
                jobs.clone(),
                jobs.clone()
            ]
        );
        assert_eq!(
            networks(&on_allowlist("forge")),
            [Some("none".into()), jobs.clone(), jobs]
        );
        let env = exec
            .container_config(&on_allowlist("registry"), "rust:1", None, None, &[])
            .env
            .unwrap_or_default();
        assert!(
            !env.iter().any(|e| e.to_ascii_uppercase().contains("PROXY")),
            "no proxy is told of where the allowlist says none: {env:?}"
        );
    }

    /// An allowlist that says a proxy has the task told of it, under each
    /// name tools read, its services' names spared, and its own setting
    /// kept; its Docker daemon and its other services too, and its steps,
    /// each under its own.
    #[test]
    fn an_allowlist_s_proxy_is_told_to_the_task_and_what_it_runs() {
        let exec = allowing(Some("http://egress:3128"));
        let mut ctx = on_allowlist("registry");
        ctx.services = two_services();
        ctx.env.push(dagron_core::dag::EnvVar {
            name: "https_proxy".into(),
            value: "http://own:8080".into(),
            value_from: None,
        });
        let env = exec
            .container_config(&ctx, "rust:1", None, None, &[])
            .env
            .unwrap();
        for told in [
            "HTTP_PROXY=http://egress:3128",
            "HTTPS_PROXY=http://egress:3128",
            "http_proxy=http://egress:3128",
            "NO_PROXY=localhost,127.0.0.1,::1,pg,cache",
            "no_proxy=localhost,127.0.0.1,::1,pg,cache",
        ] {
            assert!(env.contains(&told.to_string()), "{told} in {env:?}");
        }
        assert_eq!(
            env.iter().filter(|e| e.starts_with("https_proxy=")).collect::<Vec<_>>(),
            ["https_proxy=http://own:8080"],
            "the task's own setting, and only it"
        );
        // Its services are told too, before their own, which win.
        ctx.services[0].env.push(dagron_core::dag::EnvVar {
            name: "NO_PROXY".into(),
            value: "pg-own".into(),
            value_from: None,
        });
        let pg = exec.service_config(&ctx, &ctx.services[0], None, None).env.unwrap();
        assert!(pg.contains(&"HTTPS_PROXY=http://egress:3128".to_string()), "{pg:?}");
        assert_eq!(pg.last().map(String::as_str), Some("NO_PROXY=pg-own"), "{pg:?}");
        assert_eq!(pg.iter().filter(|e| e.starts_with("NO_PROXY=")).count(), 1, "{pg:?}");
        assert!(pg.contains(&"POSTGRES_PASSWORD=pw".to_string()), "{pg:?}");
        // What the task sets itself is the task's: its service, which is not
        // given the task's variables, has the operator's.
        assert!(pg.contains(&"https_proxy=http://egress:3128".to_string()), "{pg:?}");
        ctx.services = vec![dagron_core::docker_daemon::service("docker:27-dind")];
        let daemon = exec.service_config(&ctx, &ctx.services[0], None, None);
        assert!(
            daemon
                .env
                .unwrap_or_default()
                .contains(&"HTTPS_PROXY=http://egress:3128".to_string()),
            "the daemon pulls through it"
        );
        assert_eq!(
            daemon.host_config.unwrap().network_mode.as_deref(),
            Some("dagron-task-allow-registry"),
            "and is on the allowlist's network, as the task"
        );
    }

    /// A privileged task's own container is privileged; what is run for it
    /// first, its services and its steps are not.
    #[test]
    fn a_privileged_task_s_container_alone_is_privileged() {
        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        exec.privileged = true;
        let mut ctx = workspace_ctx();
        ctx.privileged = true;
        ctx.services = two_services();
        let privileged = made_for(&exec, &ctx).map(|c| c.host_config.and_then(|h| h.privileged));
        assert_eq!(privileged, [Some(true), None, None]);
        let pg = exec.service_config(&ctx, &ctx.services[0], None, None);
        assert_eq!(pg.host_config.and_then(|h| h.privileged), None);

        ctx.privileged = false;
        let plain = made_for(&exec, &ctx).map(|c| c.host_config.and_then(|h| h.privileged));
        assert_eq!(plain, [None, None, None]);
    }

    /// A task that asked for no network has none in its own container. The
    /// two run for it first keep the operator's: a checkout is fetched over
    /// it. And with no network named by the operator they keep the daemon's.
    #[test]
    fn a_task_with_no_network_has_none_and_its_checkout_is_still_fetched() {
        let mut ctx = workspace_ctx();
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            network: Some(dagron_core::isolation::Network::None),
            ..Default::default()
        });
        let networks = |exec: &DockerExecutor| {
            made_for(exec, &ctx).map(|c| c.host_config.and_then(|h| h.network_mode))
        };
        let none = Some("none".to_string());
        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        assert_eq!(networks(&exec), [none.clone(), None, None]);

        exec.network = Some("jobs".into());
        let jobs = Some("jobs".to_string());
        assert_eq!(networks(&exec), [none, jobs.clone(), jobs]);

        // Asking for the network by name is asking for what every task has.
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            network: Some(dagron_core::isolation::Network::Default),
            ..Default::default()
        });
        let task = exec.container_config(&ctx, "rust:1", None, None, &[]);
        assert_eq!(
            task.host_config.unwrap().network_mode.as_deref(),
            Some("jobs")
        );
    }

    /// What a task's envelope asks of a container is asked of all three, and
    /// a task that declared nothing is made as it always was. The uid is the
    /// task's alone.
    #[test]
    fn the_envelope_is_asked_of_every_container_and_the_uid_of_the_task_s() {
        use dagron_core::isolation::{IsolationSpec, Seccomp};
        let shape = |c: Config<String>| {
            let h = c.host_config.unwrap();
            (c.user, h.readonly_rootfs, h.cap_drop, h.security_opt)
        };
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());

        let plain = workspace_ctx();
        for made in made_for(&exec, &plain) {
            assert_eq!(shape(made), (None, None, None, None));
        }

        let mut hardened = workspace_ctx();
        hardened.isolation = Some(IsolationSpec {
            run_as_user: Some(1000),
            ..IsolationSpec::hardened()
        });
        let all = Some(vec!["ALL".to_string()]);
        let no_new = Some(vec!["no-new-privileges".to_string()]);
        let [task, init, checkout] = made_for(&exec, &hardened).map(shape);
        assert_eq!(
            task,
            (Some("1000".into()), Some(true), all.clone(), no_new.clone())
        );
        assert_eq!(init, (None, Some(true), all.clone(), no_new.clone()));
        assert_eq!(checkout, (None, Some(true), all, no_new.clone()));

        // One field alone asks for itself, and for no gain of privilege, as a
        // pod's does.
        let mut read_only = workspace_ctx();
        read_only.isolation = Some(IsolationSpec {
            read_only_root_fs: Some(true),
            ..Default::default()
        });
        let task = shape(exec.container_config(&read_only, "rust:1", None, None, &[]));
        assert_eq!(task, (None, Some(true), None, no_new));

        // The runtime's own seccomp profile is what a container has when
        // nothing is said, so only the opt-out is said.
        for (seccomp, said) in [
            (Seccomp::RuntimeDefault, None),
            (
                Seccomp::Unconfined,
                Some(vec!["seccomp=unconfined".to_string()]),
            ),
        ] {
            let mut ctx = workspace_ctx();
            ctx.isolation = Some(IsolationSpec {
                seccomp: Some(seccomp),
                ..Default::default()
            });
            let (_, _, _, security_opt) =
                shape(exec.container_config(&ctx, "rust:1", None, None, &[]));
            assert_eq!(security_opt, said, "{seccomp:?}");
        }
    }

    #[test]
    fn the_network_setting_is_a_network_s_name_and_never_the_host_s() {
        for unset in ["", "  "] {
            assert_eq!(parse_network(unset), Ok(None), "{unset:?}");
        }
        for name in ["jobs", " ci_jobs-1.net ", "none", "bridge", "0net"] {
            assert_eq!(
                parse_network(name),
                Ok(Some(name.trim().to_string())),
                "{name:?}"
            );
        }
        assert!(parse_network("host")
            .unwrap_err()
            .contains("the host's own network"));
        for wrong in ["container:engine", "my net", "-jobs", "jobs,other", "a/b"] {
            let err = parse_network(wrong).unwrap_err();
            assert!(err.contains("not a network's name"), "{wrong}: {err}");
        }
    }

    #[test]
    fn a_step_s_image_is_built_only_where_the_operator_says_allow() {
        for unset in ["", "  "] {
            assert_eq!(parse_build_actions(unset), Ok(false), "{unset:?}");
        }
        assert_eq!(parse_build_actions(" allow "), Ok(true));
        for wrong in ["yes", "true", "Allow", "1"] {
            assert!(parse_build_actions(wrong).is_err(), "{wrong}");
        }
    }

    /// An image is taken not to run as root only where its `USER` says so in
    /// a number: what a kubelet takes for `runAsNonRoot`.
    #[test]
    fn an_image_s_user_is_not_root_only_where_a_number_says_so() {
        for ok in ["1000", "1000:1000", "65534:0", " 12 "] {
            assert_eq!(stated_non_root(ok), Ok(()), "{ok:?}");
        }
        for (user, why) in [
            ("", "states no user"),
            ("0", "user 0"),
            ("0:1000", "user 0"),
            ("root", "runs as root"),
            ("root:root", "runs as root"),
            ("node", "a name cannot be told from root"),
            (":1000", "states no user"),
        ] {
            let err = stated_non_root(user).unwrap_err();
            assert!(err.contains(why), "{user:?}: {err}");
        }
    }

    /// Where the operator names a log driver, every container this executor
    /// makes is made with it: the task's, the one that installs its tools, and
    /// the one that fetches its checkout. Where they name none, none is named,
    /// and each gets the daemon's own, as it always did.
    #[test]
    fn every_container_names_the_driver_the_operator_chose_and_none_otherwise() {
        let ctx = workspace_ctx();
        let checkout = CheckoutSpec {
            repository: "https://example.com/acme/widgets.git".into(),
            commit: "a".repeat(40),
            depth: None,
            patched: false,
        };
        let driver_of = |config: Config<String>| {
            config
                .host_config
                .and_then(|h| h.log_config)
                .map(|l| (l.typ, l.config))
        };
        let drivers = |exec: &DockerExecutor| {
            [
                driver_of(exec.container_config(&ctx, "rust:1", None, None, &[])),
                driver_of(exec.init_config(&ctx, "tools:1")),
                driver_of(exec.checkout_config(&ctx, &checkout, "git:1", "v-ws")),
            ]
        };

        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        assert_eq!(exec.log_driver, LogDriver::Daemons);
        assert_eq!(drivers(&exec), [None, None, None]);

        // Its own options are the daemon's to give: a rotation an operator
        // set there for the sake of the host's disk is not undone here.
        exec.log_driver = LogDriver::Named("json-file".into());
        let file = Some((Some("json-file".to_string()), None));
        assert_eq!(drivers(&exec), [file.clone(), file.clone(), file]);
    }

    #[test]
    fn the_log_driver_setting_is_a_name_or_the_daemon_s_own() {
        for unset in ["", "  ", "daemon", " daemon "] {
            assert_eq!(parse_log_driver(unset), Ok(LogDriver::Daemons), "{unset:?}");
        }
        assert_eq!(
            parse_log_driver(" json-file "),
            Ok(LogDriver::Named("json-file".into()))
        );
        assert_eq!(
            parse_log_driver("grafana/loki-docker-driver:3.0"),
            Ok(LogDriver::Named("grafana/loki-docker-driver:3.0".into()))
        );
        for wrong in ["json file", "json-file,local", "a=b", "x;y"] {
            let err = parse_log_driver(wrong).unwrap_err();
            assert!(err.contains("not a log driver's name"), "{wrong}: {err}");
        }
    }

    /// A log driver's options, as a container's settings carry them.
    fn options(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// A daemon that does not wait for its log driver shows so among a
    /// container's settings, with the buffer it drops past where one is set.
    /// Unset, or set to wait, it drops nothing.
    #[test]
    fn a_container_s_settings_show_a_daemon_that_does_not_wait_for_its_driver() {
        assert_eq!(
            non_blocking_of(&options(&[
                ("mode", "non-blocking"),
                ("max-buffer-size", "4m")
            ]))
            .as_deref(),
            Some("mode=non-blocking, max-buffer-size=4m")
        );
        assert_eq!(
            non_blocking_of(&options(&[("mode", " non-blocking ")])).as_deref(),
            Some("mode=non-blocking")
        );
        for none in [
            options(&[]),
            options(&[("mode", "blocking")]),
            options(&[("mode", "")]),
            options(&[("max-buffer-size", "4m")]),
            options(&[("max-size", "10m"), ("max-file", "3")]),
        ] {
            assert_eq!(non_blocking_of(&none), None, "{none:?}");
        }
    }

    /// A daemon's rotation of a driver's logs shows among a container's
    /// settings, and is what bounds a file that would otherwise keep a whole
    /// log. Unset, or set to no bound, there is none.
    #[test]
    fn a_container_s_settings_show_the_rotation_it_was_given() {
        assert_eq!(
            rotation_of(&options(&[("max-size", "10m"), ("max-file", "3")])).as_deref(),
            Some("max-size=10m, max-file=3")
        );
        assert_eq!(
            rotation_of(&options(&[("max-size", " 1g ")])).as_deref(),
            Some("max-size=1g")
        );
        for none in [
            options(&[]),
            options(&[("max-file", "3")]),
            options(&[("max-size", "-1")]),
            options(&[("max-size", "0")]),
            options(&[("max-size", "")]),
            options(&[("compress", "true")]),
        ] {
            assert_eq!(rotation_of(&none), None, "{none:?}");
        }
    }

    /// What each driver keeps for the daemon to read back. A file that
    /// nothing rotates keeps it all. The journal, Docker's `local` and a
    /// driver this does not know keep some, and each says what is lost. And
    /// one that keeps nothing says that.
    #[test]
    fn a_driver_is_judged_by_what_it_keeps_to_read_back() {
        for whole in ["json-file", "k8s-file"] {
            assert_eq!(kept_by(whole), Kept::Whole, "{whole}");
        }
        let part = |driver: &str, says: &[&str]| match kept_by(driver) {
            Kept::Part(why) => {
                for word in says {
                    assert!(why.contains(word), "{driver}: {why}");
                }
            }
            other => panic!("{driver}: {other:?}"),
        };
        part("journald", &["rate limit", "10,000"]);
        // Bounded by default, at what its own documents give.
        part("local", &["100 MB", "beginning"]);
        for unknown in [
            "syslog",
            "fluentd",
            "awslogs",
            "grafana/loki-docker-driver:3.0",
        ] {
            part(unknown, &["keeps a copy"]);
        }
        for nothing in ["none", "passthrough", "passthrough-tty"] {
            match kept_by(nothing) {
                Kept::Nothing(why) => assert!(why.contains("empty"), "{why}"),
                other => panic!("{nothing}: {other:?}"),
            }
        }
    }

    fn workspace_ctx() -> ExecContext {
        let mut ctx = ExecContext::new(vec!["make".into()], None, None);
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec::default());
        ctx
    }

    /// A workspace task gets a volume made for its container, removed with
    /// it, as its working directory; with an init container, the init's two
    /// volumes instead, by name: the workspace it made, and its tools
    /// read-only.
    #[test]
    fn a_workspace_task_starts_in_a_volume_of_its_own() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let config = exec.container_config(&workspace_ctx(), "rust:1", None, None, &[]);
        assert_eq!(config.working_dir.as_deref(), Some("/workspace"));
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].target.as_deref(), Some("/workspace"));
        assert_eq!(mounts[0].typ, Some(MountTypeEnum::VOLUME));
        assert_eq!(mounts[0].source, None, "anonymous, so `rm -v` removes it");

        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let config = exec.container_config(&workspace_ctx(), "rust:1", Some(&init), None, &[]);
        let host = config.host_config.unwrap();
        assert_eq!(host.volumes_from, None);
        let mounts = host.mounts.unwrap();
        let at = |target: &str| {
            mounts
                .iter()
                .find(|m| m.target.as_deref() == Some(target))
                .unwrap()
        };
        assert_eq!(mounts.len(), 2);
        assert_eq!(at("/workspace").source.as_deref(), Some("v-ws"));
        assert_eq!(at("/workspace").read_only, None);
        assert_eq!(at("/dagron/bin").source.as_deref(), Some("v-tools"));
        assert_eq!(at("/dagron/bin").read_only, Some(true));
        for m in &mounts {
            let no_copy = m.volume_options.as_ref().and_then(|o| o.no_copy);
            assert_eq!(
                no_copy,
                Some(true),
                "never filled from the task's image: {m:?}"
            );
        }

        // No workspace asked for: none made, and no tools.
        let config = exec.container_config(
            &ExecContext::new(vec!["make".into()], None, None),
            "rust:1",
            None,
            None,
            &[],
        );
        assert_eq!(config.working_dir, None);
        let host = config.host_config.unwrap();
        assert_eq!(host.mounts, None);
        assert_eq!(host.volumes_from, None);
    }

    /// The checkout's container: the script whatever the image's entrypoint,
    /// the fetch's variables with the attempt's credential and a widened
    /// umask, the task's workspace volume by name, and the task's labels. And
    /// the task, after it, told git that the workspace is safe.
    #[test]
    fn a_checkout_is_fetched_by_a_container_of_its_own_into_the_task_s_volume() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let mut ctx = workspace_ctx();
        ctx.identity = Some(crate::executor::TaskIdentity {
            task_id: "t-1".into(),
            run_id: "r-1".into(),
            attempt: 1,
            fence: 1,
            installation: None,
        });
        ctx.checkout_credential = Some(crate::checkout::Credential {
            username: "x-access-token".into(),
            password: "ghs_1".into(),
        });
        let checkout = CheckoutSpec {
            repository: "https://github.com/acme/widgets.git".into(),
            commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            depth: None,
            patched: false,
        };
        let config = exec.checkout_config(&ctx, &checkout, "alpine/git:v2.47.2", "v-ws");
        assert_eq!(
            config.entrypoint,
            Some(vec![
                "sh".to_string(),
                "-ec".to_string(),
                crate::checkout::SCRIPT.to_string()
            ])
        );
        assert_eq!(config.cmd, None);
        assert_eq!(config.working_dir.as_deref(), Some("/workspace"));
        let env = config.env.unwrap();
        for want in [
            "DAGRON_CHECKOUT_REPOSITORY=https://github.com/acme/widgets.git",
            "DAGRON_CHECKOUT_COMMIT=4b4708e725f29891d22b4c96af9f0a403ff51c7c",
            "DAGRON_CHECKOUT_UMASK=0000",
            "GIT_TERMINAL_PROMPT=0",
            "GIT_CONFIG_COUNT=2",
            "GIT_CONFIG_KEY_0=http.https://github.com/acme/widgets.git.extraHeader",
            "GIT_CONFIG_KEY_1=http.followRedirects",
            "GIT_CONFIG_VALUE_1=false",
        ] {
            assert!(env.iter().any(|e| e == want), "{want} in {env:?}");
        }
        assert!(env
            .iter()
            .any(|e| e.starts_with("GIT_CONFIG_VALUE_0=Authorization: Basic ")));
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].source.as_deref(), Some("v-ws"));
        assert_eq!(mounts[0].target.as_deref(), Some("/workspace"));
        let labels = config.labels.unwrap();
        assert_eq!(
            labels
                .get(crate::executor::LABEL_TASK_ID)
                .map(String::as_str),
            Some("t-1")
        );

        // The task's own container holds none of it, but the safe directory.
        let task = exec.container_config(
            &ctx,
            "rust:1",
            None,
            None,
            &crate::checkout::task_env(&ctx.env),
        );
        let env = task.env.unwrap();
        assert!(
            env.iter()
                .all(|e| !e.contains("ghs_1") && !e.contains("Authorization")),
            "{env:?}"
        );
        assert!(
            env.iter().any(|e| e == "GIT_CONFIG_KEY_0=safe.directory"),
            "{env:?}"
        );
    }

    /// A task's actions are fetched by a container of the checkout's image
    /// into an anonymous volume another container holds, made with none of
    /// the fetch's variables; the task's container is then made with it by
    /// name, and reads it alone. Nothing of the attempt's credential goes
    /// with them, and an action's own reaches the fetch alone.
    #[test]
    fn a_task_s_actions_are_fetched_into_a_volume_it_reads_alone() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let mut ctx = workspace_ctx();
        ctx.checkout_credential = Some(crate::checkout::Credential {
            username: "x-access-token".into(),
            password: "ghs_1".into(),
        });
        let actions = vec![ActionSpec {
            repository: "https://github.com/octo/hello.git".into(),
            git_ref: "v1".into(),
            path: "octo/hello/v1".into(),
            credential: false,
        }];
        ctx.workspace.as_mut().unwrap().actions = actions.clone();

        let task = exec.container_config(&ctx, "rust:1", None, Some("v-actions"), &[]);
        let mounts = task.host_config.unwrap().mounts.unwrap();
        let held = mounts
            .iter()
            .find(|m| m.target.as_deref() == Some("/dagron/actions"))
            .expect("a volume for the actions");
        assert_eq!(held.typ, Some(MountTypeEnum::VOLUME));
        assert_eq!(
            held.source.as_deref(),
            Some("v-actions"),
            "the holder's, by name"
        );
        assert_eq!(held.read_only, Some(true));
        assert_eq!(
            held.volume_options.as_ref().and_then(|o| o.no_copy),
            Some(true),
            "never filled from the task's image"
        );

        let holder = exec.actions_holder_config(&ctx, "alpine/git:v2.47.2");
        assert_eq!(holder.image.as_deref(), Some("alpine/git:v2.47.2"));
        assert_eq!(holder.env, None, "none of the fetch's variables");
        let mounts = holder.host_config.unwrap().mounts.unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].source, None, "anonymous, so `rm -v` removes it");
        assert_eq!(mounts[0].target.as_deref(), Some("/dagron/actions"));

        let config = exec.actions_config(&ctx, &actions, "alpine/git:v2.47.2", "v-actions");
        assert_eq!(
            config.entrypoint,
            Some(vec![
                "sh".to_string(),
                "-ec".to_string(),
                crate::checkout::ACTIONS_SCRIPT.to_string()
            ])
        );
        let env = config.env.unwrap();
        for want in [
            "DAGRON_ACTIONS_DIR=/dagron/actions",
            "DAGRON_ACTION_COUNT=1",
            "DAGRON_ACTION_1_REPOSITORY=https://github.com/octo/hello.git",
            "DAGRON_ACTION_1_REF=v1",
            "DAGRON_ACTION_1_PATH=octo/hello/v1",
        ] {
            assert!(env.iter().any(|e| e == want), "{want} in {env:?}");
        }
        assert!(
            env.iter()
                .all(|e| !e.contains("ghs_1") && !e.starts_with("GIT_CONFIG")),
            "{env:?}"
        );
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(
            mounts[0].source.as_deref(),
            Some("v-actions"),
            "the holder's"
        );
        assert_eq!(mounts[0].target.as_deref(), Some("/dagron/actions"));
        assert_eq!(mounts[0].read_only, None, "the fetch writes it");

        // An action's credential is the fetch's, and not the holder's, which
        // is kept as long as the task runs.
        let private = ActionSpec {
            repository: "https://github.com/octo/private.git".into(),
            git_ref: "v1".into(),
            path: "octo/private/v1".into(),
            credential: true,
        };
        let mut with_one = workspace_ctx();
        with_one.workspace.as_mut().unwrap().actions = vec![private.clone()];
        with_one.action_credentials = vec![(
            private.repository.clone(),
            crate::checkout::Credential {
                username: "x-access-token".into(),
                password: "ghs_2".into(),
            },
        )];
        let header = format!(
            "GIT_CONFIG_VALUE_1=Authorization: {}",
            with_one.action_credentials[0].1.authorization()
        );
        let fetch = exec.actions_config(&with_one, &[private], "git:1", "v-actions");
        assert!(fetch.env.unwrap().contains(&header));
        assert_eq!(exec.actions_holder_config(&with_one, "git:1").env, None);

        // A task with none is given no such volume.
        let none = exec.container_config(&workspace_ctx(), "rust:1", None, None, &[]);
        assert!(none
            .host_config
            .unwrap()
            .mounts
            .unwrap()
            .iter()
            .all(|m| m.target.as_deref() != Some("/dagron/actions")));

        // No container of the attempt has a volume that is both anonymous
        // and read-only: Docker before 29 refuses to make one.
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        for config in [
            exec.container_config(&ctx, "rust:1", Some(&init), Some("v-actions"), &[]),
            exec.container_config(&ctx, "rust:1", None, Some("v-actions"), &[]),
            exec.init_config(&ctx, "tools:1"),
            exec.actions_holder_config(&ctx, "git:1"),
            exec.actions_config(&ctx, &actions, "git:1", "v-actions"),
        ] {
            for m in config.host_config.unwrap().mounts.unwrap_or_default() {
                assert!(
                    m.source.is_some() || m.read_only != Some(true),
                    "anonymous and read-only: {m:?}"
                );
            }
        }
    }

    /// A container that holds volumes for its attempt (its init container,
    /// its actions' holder) waits while any other container of that attempt
    /// is listed, as a listing names them: removed before them, its volume,
    /// still in use, would be left for good. Not on another attempt's, nor on
    /// another holder, nor on the actions' fetch once that is gone. The
    /// task's own container, which made the workspace where no init
    /// container did, waits likewise for its checkout's and its steps'.
    #[test]
    fn a_holder_of_volumes_waits_for_the_rest_of_its_attempt() {
        let a = "sched-0a";
        assert_eq!(holder_of(&format!("/{}", init_name(a))), Some(a));
        assert_eq!(holder_of(&actions_name(a)), Some(a));
        for other in [
            a.to_string(),
            actions_fetch_name(a),
            checkout_name(a),
            service_name(a, 0),
        ] {
            assert_eq!(holder_of(&other), None, "{other}");
        }

        let listed = |names: &[String]| -> Vec<(String, Vec<String>)> {
            names
                .iter()
                .map(|n| (format!("id-{n}"), vec![format!("/{n}")]))
                .collect()
        };
        let ids = |kept: Vec<(String, Vec<String>)>| -> Vec<String> {
            kept.into_iter().map(|(id, _)| id).collect()
        };
        for sibling in [
            a.to_string(),
            actions_fetch_name(a),
            service_name(a, 1),
            step_name(a, 3),
        ] {
            let all = listed(&[actions_name(a), init_name(a), sibling.clone()]);
            assert_eq!(
                ids(not_held_back(&all)),
                [format!("id-{sibling}")],
                "both wait on {sibling}"
            );
        }
        // Another attempt's, one whose name begins as this one's does
        // included, holds nothing up; nor does one holder another.
        let all = listed(&[
            actions_name(a),
            init_name(a),
            "sched-0ab".into(),
            init_name("sched-0b"),
        ]);
        assert_eq!(not_held_back(&all).len(), 4);
        assert!(!holds(&all[2].1) && holds(&all[3].1));

        // The task's own container waits for those that mount its workspace
        // by name, its checkout's and its steps', and for no other; its
        // holders wait for it.
        let named = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let (step, checkout) = (step_name(a, 1), checkout_name(a));
        for (names, left) in [
            (
                named(&[a, &step, &checkout, &init_name(a)]),
                named(&[&step, &checkout]),
            ),
            (named(&[a, &init_name(a)]), named(&[a])),
            (
                named(&[a, &service_name(a, 0), &actions_fetch_name(a)]),
                named(&[a, &service_name(a, 0), &actions_fetch_name(a)]),
            ),
            (
                named(&[a, &step_name("sched-0ab", 1)]),
                named(&[a, &step_name("sched-0ab", 1)]),
            ),
            (
                named(&[a, &format!("{a}-step-x"), &format!("{a}-step-")]),
                named(&[a, &format!("{a}-step-x"), &format!("{a}-step-")]),
            ),
        ] {
            let want: Vec<String> = left.iter().map(|n| format!("id-{n}")).collect();
            assert_eq!(ids(not_held_back(&listed(&names))), want, "of {names:?}");
        }
        assert!(mounts_workspace_of(&step_name(a, 12), a));
        assert!(!mounts_workspace_of(&step_name(a, 1), &step_name(a, 1)));
    }

    /// A task's loan is kept under its task container's name, and its steps
    /// are lent the latest attempt's: lent again by another attempt, the
    /// earlier one's has ended. Taken back, a loan ends and is kept, for the
    /// stop to find, and an attempt's hold on it lets go of its own and of
    /// no other's.
    #[tokio::test]
    async fn a_task_s_steps_are_lent_its_latest_attempt_s_and_its_loan_outlives_its_end() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let mut ctx = workspace_ctx();
        ctx.identity = Some(crate::executor::TaskIdentity {
            task_id: "t-1".into(),
            run_id: "r-1".into(),
            attempt: 1,
            fence: 1,
            installation: None,
        });
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let (first, second) = ("sched-1", "sched-2");
        exec.lend(&ctx, first, Some(&init), None).await;
        exec.lend(&ctx, second, Some(&init), None).await;
        let lent_to = |exec: &DockerExecutor| exec.loan_of("t-1").map(|(task, _)| task);
        assert_eq!(lent_to(&exec).as_deref(), Some(second));
        let ended = |exec: &DockerExecutor, task: &str| {
            exec.lent.lock().unwrap().get(task).map(|l| l.ended)
        };
        assert_eq!(
            ended(&exec, first),
            Some(true),
            "lent again, the first ended"
        );

        // The first's end, with no step running, takes back nothing, and
        // ends nothing of the second's.
        assert!(exec.take_back(first).await.is_empty());
        assert_eq!(lent_to(&exec).as_deref(), Some(second));
        assert!(exec.take_back("sched-none").await.is_empty());
        // Taken back, the second is ended and kept: no step is lent to the
        // task, and the stop still finds what it lent.
        assert!(exec.take_back(second).await.is_empty());
        assert_eq!(lent_to(&exec), None);
        assert_eq!(ended(&exec, second), Some(true));

        drop(Loan(Arc::clone(&exec.lent), first.into()));
        let mut left: Vec<String> = exec.lent.lock().unwrap().keys().cloned().collect();
        left.sort();
        assert_eq!(
            left,
            [second],
            "an attempt's hold lets go of its own loan alone"
        );
        drop(Loan(Arc::clone(&exec.lent), second.into()));
        assert!(exec.lent.lock().unwrap().is_empty());
    }

    /// The identity of claim `fence` of task `t-1`.
    fn claim(fence: i64) -> crate::executor::TaskIdentity {
        crate::executor::TaskIdentity {
            task_id: "t-1".into(),
            run_id: "r-1".into(),
            attempt: fence,
            fence,
            installation: None,
        }
    }

    /// Of a task's loans, the latest claim's is lent: an attempt of an
    /// earlier claim that lends only after a later one did, having stalled,
    /// ends nothing of the later one's, and its own loan is ended as it is
    /// made.
    #[tokio::test]
    async fn a_lost_claim_s_loan_lent_late_ends_nothing_of_the_live_one_s() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let mut ctx = workspace_ctx();
        ctx.identity = Some(claim(2));
        exec.lend(&ctx, "sched-2", Some(&init), None).await;
        ctx.identity = Some(claim(1));
        exec.lend(&ctx, "sched-1", Some(&init), None).await;

        assert_eq!(
            exec.loan_of("t-1").map(|(task, _)| task).as_deref(),
            Some("sched-2"),
            "the live claim's steps are still lent its workspace"
        );
        let ended = |task: &str| exec.lent.lock().unwrap().get(task).map(|l| l.ended);
        assert_eq!(
            ended("sched-1"),
            Some(true),
            "kept for its own end to take back"
        );
        assert_eq!(ended("sched-2"), Some(false));
    }

    /// A reap ends the loans of the task's earlier claims here before it
    /// looks for their containers, and waits for a step's container being
    /// made on one: it is then there to find, and none is made after. A
    /// loan of its own claim, or of a later one, it leaves.
    #[tokio::test]
    async fn a_reap_ends_an_earlier_claim_s_loan_and_waits_for_its_step_being_made() {
        let exec = Arc::new(bare_executor(
            Docker::connect_with_local_defaults().unwrap(),
        ));
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let mut ctx = workspace_ctx();
        ctx.identity = Some(claim(1));
        exec.lend(&ctx, "sched-1", Some(&init), None).await;
        let steps = Arc::clone(&exec.lent.lock().unwrap()["sched-1"].steps);
        let share = steps.read_owned().await;

        let ending = {
            let exec = Arc::clone(&exec);
            tokio::spawn(async move {
                let budget = tokio::time::Instant::now() + Duration::from_secs(5);
                exec.end_earlier_loans(&claim(2), budget).await
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            exec.loan_of("t-1").is_none(),
            "nothing is lent on it from now on"
        );
        assert!(
            !ending.is_finished(),
            "the reap waits for the create under way"
        );
        drop(share);
        timeout(Duration::from_secs(5), ending)
            .await
            .expect("and goes on once it is over")
            .unwrap();

        // A loan of the reaping claim's own, or of a later claim's, is not
        // its to end.
        ctx.identity = Some(claim(3));
        exec.lend(&ctx, "sched-3", Some(&init), None).await;
        for reaping in [2, 3] {
            let budget = tokio::time::Instant::now() + Duration::from_secs(5);
            exec.end_earlier_loans(&claim(reaping), budget).await;
            assert_eq!(
                exec.loan_of("t-1").map(|(task, _)| task).as_deref(),
                Some("sched-3"),
                "reaped by claim {reaping}"
            );
        }
    }

    /// The reap a dispatch starts with ends the earlier claims' loans before
    /// anything else, whether or not the daemon then answers it.
    #[tokio::test]
    async fn a_dispatch_s_reap_ends_an_earlier_claim_s_loan_first() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let mut ctx = workspace_ctx();
        ctx.identity = Some(claim(1));
        exec.lend(&ctx, "sched-1", Some(&init), None).await;
        ctx.identity = Some(claim(2));
        let _ = exec.reap_predecessors(&ctx).await;
        assert!(exec.loan_of("t-1").is_none());
    }

    /// A task's end ends its loan at once, so that no step's container is
    /// made for it from then on, and takes its steps back only once each
    /// request that makes one has let go of its share: a container being
    /// made as the task ended is there to remove.
    #[tokio::test]
    async fn a_task_s_end_waits_for_a_step_s_container_being_made() {
        let exec = Arc::new(bare_executor(
            Docker::connect_with_local_defaults().unwrap(),
        ));
        let mut ctx = workspace_ctx();
        ctx.identity = Some(crate::executor::TaskIdentity {
            task_id: "t-1".into(),
            run_id: "r-1".into(),
            attempt: 1,
            fence: 1,
            installation: None,
        });
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        exec.lend(&ctx, "sched-1", Some(&init), None).await;
        // The share a step's create holds while its request is under way.
        let steps = Arc::clone(&exec.lent.lock().unwrap()["sched-1"].steps);
        let share = steps.read_owned().await;

        let taking = {
            let exec = Arc::clone(&exec);
            tokio::spawn(async move { exec.take_back("sched-1").await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!taking.is_finished(), "the end waits for the create");
        assert!(exec.loan_of("t-1").is_none(), "and has ended the loan");

        drop(share);
        let left = timeout(Duration::from_secs(5), taking)
            .await
            .expect("the end takes the steps back once the create is over")
            .unwrap();
        assert!(left.is_empty(), "{left:?}");
    }

    /// Where the operator chose where tasks work, a workspace task works
    /// there and no volume is made: `dagron-ci run` keeps its tree this way.
    #[test]
    fn the_operator_s_directory_is_the_workspace_where_it_set_one() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let exec = bare_executor(docker.clone())
            .with_task_mounts(vec![mount("/src/tree", "/src/tree", false)], Some("/src/tree".into()));
        let config = exec.container_config(&workspace_ctx(), "rust:1", None, None, &[]);
        assert_eq!(config.working_dir.as_deref(), Some("/src/tree"));
        assert_eq!(config.host_config.unwrap().mounts.unwrap().len(), 1, "the tree, and no volume");

        let exec = bare_executor(docker).with_task_mounts(vec![mount("/srv/ci", "/workspace", false)], None);
        let config = exec.container_config(&workspace_ctx(), "rust:1", None, None, &[]);
        assert_eq!(config.working_dir.as_deref(), Some("/workspace"));
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].source.as_deref(), Some("/srv/ci"));
    }

    /// The init container: the image's entrypoint asked to install into an
    /// anonymous volume at /dagron/bin, and the workspace's volume made beside
    /// it, from the image's own world-writable /workspace; labelled as the
    /// task is so that the reap and the sweep find it.
    #[test]
    fn the_init_container_installs_into_a_volume_and_makes_the_workspace_s() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap())
            .with_task_mounts(vec![mount("/src/tree", "/src/tree", false)], None);
        let mut ctx = workspace_ctx();
        ctx.identity = Some(crate::executor::TaskIdentity {
            task_id: "t-1".into(),
            run_id: "r-1".into(),
            attempt: 2,
            fence: 2,
            installation: None,
        });
        let config = exec.init_config(&ctx, "dagron-ci-exec:1");
        assert_eq!(config.image.as_deref(), Some("dagron-ci-exec:1"));
        assert_eq!(
            config.cmd,
            Some(vec!["install".to_string(), "/dagron/bin".to_string()])
        );
        assert_eq!(
            config.entrypoint, None,
            "the image's own entrypoint is dagron-ci-exec"
        );
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert_eq!(
            mounts.len(),
            2,
            "the operator's mounts are the task's, not the init's"
        );
        assert_eq!(mounts[0].target.as_deref(), Some("/dagron/bin"));
        assert_eq!(mounts[1].target.as_deref(), Some("/workspace"));
        assert!(
            mounts.iter().all(|m| m.source.is_none()),
            "anonymous: removed with it"
        );
        // Where the operator's directory is the workspace, the init makes none.
        let theirs = bare_executor(Docker::connect_with_local_defaults().unwrap())
            .with_task_mounts(vec![mount("/srv/ci", "/workspace", false)], None);
        let mounts = theirs
            .init_config(&ctx, "dagron-ci-exec:1")
            .host_config
            .unwrap()
            .mounts
            .unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].target.as_deref(), Some("/dagron/bin"));
        let labels = config.labels.unwrap();
        assert_eq!(labels.get(crate::executor::LABEL_TASK_ID).map(String::as_str), Some("t-1"));
        assert_eq!(labels.get(crate::executor::LABEL_ATTEMPT).map(String::as_str), Some("2"));
    }

    #[test]
    fn mounts_are_written_as_docker_run_v_writes_them() {
        assert_eq!(parse_mounts("").unwrap(), vec![]);
        assert_eq!(
            parse_mounts(" /src/tree:/src/tree , /bin/x:/dagron/bin/x:ro,cache-1:/root/.cache:rw,")
                .unwrap(),
            vec![
                mount("/src/tree", "/src/tree", false),
                mount("/bin/x", "/dagron/bin/x", true),
                mount("cache-1", "/root/.cache", false),
            ]
        );
        // A bare name is a volume, however much it looks like a path.
        assert_eq!(parse_mounts("src:/b").unwrap(), vec![mount("src", "/b", false)]);
        for bad in [
            "/src",        // no target
            "/a:/b:ro:z",  // more than a mode
            "/a:/b:z",     // a mode Docker has, and this does not take
            "./src:/b",    // a relative path
            "~/src:/b",    // a shell's spelling of one
            "-v:/b",       // not a volume's name
            "x:/b",        // too short for one
            "/a:b",        // a relative target
            "/a:/",        // over the root
            "/a:/b,/c:/b", // one target twice
        ] {
            assert!(parse_mounts(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_working_directory_is_absolute() {
        assert_eq!(parse_workdir("").unwrap(), None);
        assert_eq!(parse_workdir(" /src/tree ").unwrap(), Some("/src/tree".into()));
        assert!(parse_workdir("src/tree").is_err());
    }

    /// What a container is created with: the engine's mounts, as binds for a
    /// path and volumes for a name, and its working directory. Nothing here
    /// talks to a daemon: a client is only a socket path until it is used.
    #[test]
    fn every_task_container_gets_the_engine_s_mounts_and_directory() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let bare = bare_executor(docker);
        let ctx = ExecContext::new(vec!["true".into()], None, None);
        let config = bare.container_config(&ctx, "alpine:latest", None, None, &[]);
        assert_eq!(config.working_dir, None);
        assert_eq!(config.host_config.unwrap().mounts, None, "no mounts unless the operator sets them");

        let exec = bare.with_task_mounts(
            vec![mount("/src/tree", "/src/tree", false), mount("cargo", "/usr/local/cargo/registry", true)],
            Some("/src/tree".into()),
        );
        let config = exec.container_config(&ctx, "rust:1", None, None, &[]);
        assert_eq!(config.image.as_deref(), Some("rust:1"));
        assert_eq!(config.working_dir.as_deref(), Some("/src/tree"));
        let mounts = config.host_config.unwrap().mounts.unwrap();
        assert_eq!(mounts[0].typ, Some(MountTypeEnum::BIND));
        assert_eq!(mounts[0].source.as_deref(), Some("/src/tree"));
        assert_eq!(mounts[0].read_only, Some(false));
        assert_eq!(mounts[1].typ, Some(MountTypeEnum::VOLUME));
        assert_eq!(mounts[1].target.as_deref(), Some("/usr/local/cargo/registry"));
        assert_eq!(mounts[1].read_only, Some(true));
    }

    /// A task of a run that is not trusted gets none of that: no mount of the
    /// operator's, which may be the host's, another task's, or the daemon's
    /// own socket, and not their working directory, which is in one. It
    /// works in a workspace of this executor's making, and each container
    /// made for it has limits.
    #[test]
    fn a_confined_task_gets_nothing_of_the_operator_s_and_is_held_to_limits() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let exec = bare_executor(docker).with_task_mounts(
            vec![
                mount("/var/run/docker.sock", "/var/run/docker.sock", false),
                mount("shared", WORKSPACE_DIR, false),
            ],
            Some("/src/tree".into()),
        );
        let limits = |host: &HostConfig| {
            (
                host.memory,
                host.memory_swap,
                host.pids_limit,
                host.nano_cpus,
            )
        };
        let mut ctx = workspace_ctx();

        // As every other task has them.
        assert!(!exec.makes_workspace(&ctx));
        let own = exec.container_config(&ctx, "rust:1", None, None, &[]);
        assert_eq!(own.working_dir.as_deref(), Some("/src/tree"));
        let host = own.host_config.unwrap();
        assert_eq!(limits(&host), (None, None, None, None));
        assert_eq!(host.mounts.unwrap().len(), 2);

        let held_to = crate::executor::Confinement {
            memory_bytes: 4 << 30,
            pids: 2048,
            nano_cpus: 2_000_000_000,
        };
        ctx.confinement = Some(held_to);
        assert!(
            exec.makes_workspace(&ctx),
            "its workspace is not the operator's mount"
        );
        let confined = exec.container_config(&ctx, "rust:1", None, None, &[]);
        assert_eq!(confined.working_dir.as_deref(), Some(WORKSPACE_DIR));
        let host = confined.host_config.unwrap();
        let held = (
            Some(4 << 30),
            Some(4 << 30),
            Some(2048),
            Some(2_000_000_000),
        );
        assert_eq!(limits(&host), held);
        let mounts = host.mounts.unwrap();
        assert_eq!(mounts.len(), 1, "{mounts:?}");
        assert_eq!(mounts[0].target.as_deref(), Some(WORKSPACE_DIR));
        assert_eq!(
            (mounts[0].typ, mounts[0].source.as_deref()),
            (Some(MountTypeEnum::VOLUME), None),
            "a volume made for it, and nobody else's"
        );

        // What is run for it first is held as it is.
        let init = exec.init_config(&ctx, "tools:1").host_config.unwrap();
        assert_eq!(limits(&init), held);

        // A confined task with no workspace has no mount at all.
        let mut bare = ExecContext::new(vec!["true".into()], None, None);
        bare.confinement = Some(crate::executor::Confinement {
            pids: 64,
            ..held_to
        });
        let config = exec.container_config(&bare, "alpine:latest", None, None, &[]);
        assert_eq!(config.working_dir, None);
        let host = config.host_config.unwrap();
        assert_eq!(host.mounts, None);
        assert_eq!(host.pids_limit, Some(64));
    }

    fn two_services() -> Vec<ServiceSpec> {
        serde_json::from_value(serde_json::json!([
            {
                "name": "pg",
                "image": "postgres:16-alpine",
                "args": ["postgres", "-c", "fsync=off"],
                "env": [{ "name": "POSTGRES_PASSWORD", "value": "pw" }],
                "ready": "pg_isready -h localhost -U postgres",
            },
            { "name": "cache", "image": "redis:7" },
        ]))
        .unwrap()
    }

    /// A task's services and the task share one network: the first service's
    /// container is on the network the task would have been on, with every
    /// service's name as a name for its loopback address, and the rest are
    /// made in its network, the task's container among them. So a task finds
    /// a service at its name, and no other task's service answers to it.
    #[test]
    fn a_task_s_services_share_the_first_one_s_network_under_their_names() {
        assert_eq!(service_name("sched-a1", 0), "sched-a1-svc-0");
        assert_eq!(service_name("sched-a1", 2), "sched-a1-svc-2");
        assert_eq!(network_of("sched-a1-svc-0"), "container:sched-a1-svc-0");

        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap())
            .with_task_mounts(vec![mount("shared", "/shared", true)], None);
        let mut ctx = workspace_ctx();
        ctx.services = two_services();
        let names = Some(vec![
            "pg:127.0.0.1".to_string(),
            "cache:127.0.0.1".to_string(),
        ]);

        let first = exec.service_config(&ctx, &ctx.services[0], None, None);
        assert_eq!(first.image.as_deref(), Some("postgres:16-alpine"));
        assert_eq!(
            first.cmd.as_deref(),
            Some(&["postgres".to_string(), "-c".into(), "fsync=off".into()][..]),
            "its arguments, in place of the image's own command"
        );
        assert_eq!(
            first.env.as_deref(),
            Some(&["POSTGRES_PASSWORD=pw".to_string()][..]),
            "its own variables, and none of the task's"
        );
        assert_eq!(first.working_dir, None);
        let host = first.host_config.unwrap();
        assert_eq!(
            host.network_mode, None,
            "the daemon's default, as the task's"
        );
        assert_eq!(host.extra_hosts, names);
        assert_eq!(
            host.mounts, None,
            "none of the operator's mounts, nor the workspace"
        );

        let second = exec.service_config(&ctx, &ctx.services[1], Some("sched-a1-svc-0"), None);
        assert_eq!(second.cmd, None, "the image's own command");
        assert_eq!(second.env, None);
        let host = second.host_config.unwrap();
        assert_eq!(
            host.network_mode.as_deref(),
            Some("container:sched-a1-svc-0")
        );
        assert_eq!(
            host.extra_hosts, None,
            "a daemon refuses names for a container made in another's network"
        );

        // On the operator's network where they named one.
        exec.network = Some("jobs".into());
        let host = exec
            .service_config(&ctx, &ctx.services[0], None, None)
            .host_config
            .unwrap();
        assert_eq!(host.network_mode.as_deref(), Some("jobs"));

        // A task that asked for no network has services with none: they and
        // it are each other's whole network.
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            network: Some(dagron_core::isolation::Network::None),
            ..Default::default()
        });
        let host = exec
            .service_config(&ctx, &ctx.services[0], None, None)
            .host_config
            .unwrap();
        assert_eq!(host.network_mode.as_deref(), Some(NO_NETWORK));
        assert_eq!(host.extra_hosts, names);
    }

    /// A service's image and arguments are its author's, as its task's are:
    /// its container is held to the task's envelope and uid, and to the
    /// limits of a run that is not trusted.
    #[test]
    fn a_service_is_held_as_its_task_is() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let mut ctx = ExecContext::new(vec!["true".into()], None, None);
        ctx.services = two_services();
        let free = exec.service_config(&ctx, &ctx.services[0], None, None);
        assert_eq!(free.user, None);
        let host = free.host_config.unwrap();
        assert_eq!(
            (
                host.readonly_rootfs,
                host.cap_drop,
                host.security_opt,
                host.memory
            ),
            (None, None, None, None)
        );

        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            run_as_user: Some(1000),
            read_only_root_fs: Some(true),
            drop_all_capabilities: Some(true),
            ..Default::default()
        });
        ctx.confinement = Some(crate::executor::Confinement {
            memory_bytes: 256 << 20,
            pids: 128,
            nano_cpus: 1_000_000_000,
        });
        for (i, first) in [(0, None), (1, Some("sched-a1-svc-0"))] {
            let held = exec.service_config(&ctx, &ctx.services[i], first, None);
            assert_eq!(held.user.as_deref(), Some("1000"), "the task's uid");
            let host = held.host_config.unwrap();
            assert_eq!(host.readonly_rootfs, Some(true));
            assert_eq!(host.cap_drop, Some(vec!["ALL".to_string()]));
            assert_eq!(
                host.security_opt,
                Some(vec!["no-new-privileges".to_string()])
            );
            assert_eq!(
                (
                    host.memory,
                    host.memory_swap,
                    host.pids_limit,
                    host.nano_cpus
                ),
                (
                    Some(256 << 20),
                    Some(256 << 20),
                    Some(128),
                    Some(1_000_000_000)
                )
            );
        }
    }

    /// The task's Docker daemon is privileged and root whatever the task's
    /// envelope says, keeps the task's limits and network, and has the
    /// workspace an init container made at the task's own path.
    #[test]
    fn the_task_s_docker_daemon_is_privileged_and_has_its_workspace() {
        // An operator who gives every task the tree, a cache, and the
        // host's own daemon's socket.
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap()).with_task_mounts(
            vec![
                mount("/src/tree", "/src/tree", false),
                mount("cargo", "/usr/local/cargo/registry", true),
                mount("/var/run/docker.sock", "/var/run/docker.sock", false),
                mount("images", "/var/lib/docker", false),
            ],
            None,
        );
        let mut ctx = ExecContext::new(vec!["true".into()], None, None);
        ctx.services = vec![dagron_core::docker_daemon::service("docker:27-dind")];
        ctx.isolation = Some(dagron_core::isolation::IsolationSpec {
            run_as_user: Some(1000),
            read_only_root_fs: Some(true),
            drop_all_capabilities: Some(true),
            ..Default::default()
        });
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let daemon = exec.service_config(&ctx, &ctx.services[0], None, Some(&init));
        assert_eq!(daemon.user, None, "root, whatever the task's uid");
        let host = daemon.host_config.unwrap();
        assert_eq!(host.privileged, Some(true));
        assert_eq!(
            (host.readonly_rootfs, host.cap_drop, host.security_opt),
            (None, None, None)
        );
        let at: Vec<(Option<String>, Option<String>)> = host
            .mounts
            .unwrap()
            .into_iter()
            .map(|m| (m.source, m.target))
            .collect();
        let pair = |s: &str, t: &str| (Some(s.to_string()), Some(t.to_string()));
        assert_eq!(
            at,
            [
                pair("/src/tree", "/src/tree"),
                pair("cargo", "/usr/local/cargo/registry"),
                pair("v-ws", WORKSPACE_DIR),
            ],
            "the operator's mounts and the workspace, at their paths; nothing where a \
             daemon keeps its own (the host's socket, a shared data root), nor the tools"
        );

        // With no workspace of an init container's, the operator's alone.
        let daemon = exec.service_config(&ctx, &ctx.services[0], None, None);
        assert_eq!(daemon.host_config.unwrap().mounts.unwrap().len(), 2);
        // And any other service has nothing of them.
        ctx.services = two_services();
        let pg = exec.service_config(&ctx, &ctx.services[0], None, Some(&init));
        let host = pg.host_config.unwrap();
        assert_eq!((host.privileged, host.mounts), (None, None));
    }

    /// On the operator's runtime (Sysbox's), the daemon is not privileged:
    /// the runtime gives it what it needs, in a user namespace of its own.
    /// It is the same container otherwise.
    #[test]
    fn on_the_operator_s_runtime_the_daemon_is_not_privileged() {
        let mut exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        exec.daemon_runtime = Some(dagron_core::docker_daemon::RUNTIME_SYSBOX.into());
        let mut ctx = ExecContext::new(vec!["true".into()], None, None);
        ctx.services = vec![dagron_core::docker_daemon::service("docker:27-dind")];
        let init = InitVolumes {
            tools: "v-tools".into(),
            workspace: Some("v-ws".into()),
        };
        let daemon = exec.service_config(&ctx, &ctx.services[0], None, Some(&init));
        let host = daemon.host_config.unwrap();
        assert_eq!(host.privileged, None);
        assert_eq!(host.runtime.as_deref(), Some("sysbox-runc"));
        assert_eq!(host.mounts.unwrap().len(), 1, "its workspace still");
        // And no other service is run on it.
        ctx.services = two_services();
        let pg = exec.service_config(&ctx, &ctx.services[0], None, None);
        assert_eq!(pg.host_config.unwrap().runtime, None);
    }

    /// A task with a Docker daemon whose workspace this engine would make
    /// in the task's own container, made after the daemon's, is refused
    /// before anything is made: the daemon would have an empty directory
    /// where the task has its files.
    #[tokio::test]
    async fn a_daemon_with_a_workspace_no_init_container_makes_is_refused() {
        let exec = bare_executor(Docker::connect_with_local_defaults().unwrap());
        let mut ctx = ExecContext::new(vec!["true".into()], Some(10), None);
        ctx.services = vec![dagron_core::docker_daemon::service("docker:27-dind")];
        ctx.workspace = Some(Default::default());
        let err = exec.execute(&ctx).await.unwrap_err().to_string();
        assert!(err.contains(ENV_CI_EXEC_IMAGE), "{err}");
        assert!(err.contains(ENV_WORKDIR), "{err}");
    }

    #[test]
    fn why_a_service_was_not_ready_says_what_it_said_or_that_it_said_nothing() {
        assert_eq!(said_or_nothing(" \n"), "it said nothing");
        assert_eq!(
            said_or_nothing("FATAL: no password\n"),
            "it said: FATAL: no password"
        );
        let pg = &two_services()[0];
        assert_eq!(service_image(pg), "the image of the service 'pg'");
    }
}
