//! **A Docker daemon beside a task**: `docker_daemon: true`.
//!
//! A job that runs `docker build` or `docker run` wants a daemon, and a
//! task's container has none: the engine's own is not the task's to call,
//! since whatever can call it can do on the engine's host what that daemon
//! can. A task that names `docker_daemon: true` is given a daemon of its
//! own instead, Docker in Docker: a service ([`crate::service`]) named
//! [`NAME`], made for the attempt and removed with it, which the task
//! reaches as `DOCKER_HOST` ([`HOST`]). `docker:cli` and the other images
//! that have the Docker CLI find it there. It listens on the loopback
//! address that the task and its services share, and on no address
//! anything else reaches.
//!
//! **The operator's to offer.** A daemon in a container runs privileged:
//! it makes containers of its own, and that is what makes them. An engine
//! starts one only where its operator has said it may ([`ENV`] set to
//! `dind`), and from the image they name ([`ENV_IMAGE`]). A task that asks
//! for one anywhere else is failed with the reason, and is not tried again.
//! A run that is not trusted is refused the key ([`crate::confine`]).
//!
//! | Executor   | A daemon |
//! |------------|----------|
//! | Docker     | a service's container, privileged (or unprivileged on [`ENV_RUNTIME`]'s runtime), with the task's workspace and the operator's mounts at their own paths (but where a daemon keeps its own: [`OWN_PATHS`]) |
//! | Kubernetes | refused: a privileged sidecar is the cluster's policy to allow |
//! | Local      | refused: a task there has no container to be beside |
//!
//! **The workspace.** Where the task's workspace is a volume made for it
//! (a CI job's), the daemon has the same volume at the same path, so a
//! `docker run -v "$PWD:/src"` from the task finds the task's files. A
//! `docker build .` sends its context to the daemon, and needs no such
//! thing.
//!
//! **Unprivileged, on a runtime that makes it so.** Where the operator
//! names a container runtime for it ([`ENV_RUNTIME`], Sysbox's
//! `sysbox-runc`), the daemon's container is run on that runtime and is not
//! privileged: the runtime gives it a user namespace of its own, in which
//! its root is no user of the host's, and what a daemon needs inside it.
//! The engine's host has to have the runtime, which is asked of its daemon
//! as the engine starts.
//!
//! **What the daemon is not held to**: the task's isolation. Where no
//! runtime is named its container is privileged whatever the task's
//! envelope says, as the operator who offered it has accepted, and so the
//! task can start one that is too. What it holds, its images and
//! containers, goes with it when the attempt ends. An engine that offers
//! one runs no work that is not trusted, on a runtime or not
//! ([`refusal_with_untrusted`]).
//!
//! **The daemon is the task's first service**: the task and its other
//! services join its network. A daemon on Sysbox can make its network's
//! devices only in a network of its own user namespace, and not in one an
//! ordinary container made.

use anyhow::{bail, Result};

use crate::isolation::ExecutorKind;
use crate::service::{Ready, ServiceSpec};

/// Whether this engine starts a daemon beside a task that asks for one:
/// `dind`, or unset for none.
pub const ENV: &str = "DAGRON_DOCKER_DAEMON";

/// The image the daemon is run from ([`DEFAULT_IMAGE`] when unset).
pub const ENV_IMAGE: &str = "DAGRON_DOCKER_DAEMON_IMAGE";

/// The container runtime the daemon is run on, which makes it unprivileged
/// ([`RUNTIME_SYSBOX`]); unset: the host daemon's default, privileged.
pub const ENV_RUNTIME: &str = "DAGRON_DOCKER_DAEMON_RUNTIME";

/// Sysbox's runtime, which runs a Docker daemon in a container without
/// privileges.
pub const RUNTIME_SYSBOX: &str = "sysbox-runc";

/// The runtime a daemon is run on, from [`ENV_RUNTIME`] as it is set, and
/// whether [`ENV`] offers one at all: `None` for the default.
pub fn runtime(value: Option<&str>, offered: bool) -> Result<Option<String>> {
    let name = value.map(str::trim).unwrap_or_default();
    if name.is_empty() {
        return Ok(None);
    }
    if !offered {
        bail!("{ENV_RUNTIME}={name}: this engine offers no daemon to run on it ({ENV}=dind)");
    }
    let plain = name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !plain {
        bail!("{ENV_RUNTIME}={name:?} is not a runtime's name, as a Docker daemon lists them");
    }
    // A runtime that gives a container no user namespace of its own (runc,
    // crun, the host's default) cannot run a daemon unprivileged: every
    // daemon would fail to start, where the check at start said all was well.
    if !name.starts_with(SYSBOX_PREFIX) {
        bail!(
            "{ENV_RUNTIME}={name}: a daemon runs unprivileged only on Sysbox's runtime \
             ({RUNTIME_SYSBOX}); unset it to run the daemon privileged"
        );
    }
    Ok(Some(name.to_string()))
}

/// What the name of a Sysbox runtime begins with, as its installer
/// registers it with Docker (`sysbox-runc`).
const SYSBOX_PREFIX: &str = "sysbox";

/// The image the daemon is run from when [`ENV_IMAGE`] names none.
pub const DEFAULT_IMAGE: &str = "docker.io/library/docker:27-dind";

/// The daemon's name among the task's services, and its host name.
pub const NAME: &str = "docker";

/// Where the daemon listens, without TLS, and what the task is given as
/// [`ENV_HOST`]: the loopback address of the network the task and its
/// services share, and no other. What else is on the operator's network
/// does not reach it, as it would a service's port. An address and not
/// [`NAME`]: a task with no network on Podman has no names for its
/// services, and has this address all the same.
pub const HOST: &str = "tcp://127.0.0.1:2375";

/// Where in its container a daemon keeps its own: its socket and run files,
/// and its data root. An operator's mount at one of them, or under it, is
/// not given to a task's daemon: the engine host's own daemon's socket
/// mounted there would keep this one from making its own, and every task's
/// daemon would share one data root.
pub const OWN_PATHS: &[&str] = &["/var/run", "/run", "/var/lib/docker"];

/// The variable the Docker CLI reads the daemon's address from.
pub const ENV_HOST: &str = "DOCKER_HOST";

/// How long the daemon has to answer once started, in seconds.
pub const READY_TIMEOUT_SECS: u64 = 120;

/// Why an engine that offers a daemon does not start where it also runs
/// work that is not trusted: a daemon's container is root on the engine's
/// host (privileged), or root in a user namespace of its own (on Sysbox),
/// and either way a daemon a task drives, which such work is not given
/// beside.
pub fn refusal_with_untrusted() -> String {
    format!(
        "{ENV}=dind runs a Docker daemon beside a task that asks, privileged and root on \
         this engine's host unless {ENV_RUNTIME} names Sysbox's runtime, and this engine \
         also runs work that is not trusted (DAGRON_UNTRUSTED_ISOLATION_FLOOR): run the two \
         on engines of their own"
    )
}

/// Whether `target`, a mount's place in a container, is a daemon's own
/// ([`OWN_PATHS`]) or under one.
pub fn owns(target: &str) -> bool {
    let target = target.trim_end_matches('/');
    OWN_PATHS
        .iter()
        .any(|own| target == *own || target.starts_with(&format!("{own}/")))
}

/// The image an engine starts a daemon from, from [`ENV`] and
/// [`ENV_IMAGE`] as they are set: `None` where it starts none.
pub fn offered(gate: Option<&str>, image: Option<&str>) -> Result<Option<String>> {
    match gate.map(str::trim).unwrap_or_default() {
        "" => Ok(None),
        "dind" => {
            let image = image.map(str::trim).filter(|i| !i.is_empty());
            let image = image.unwrap_or(DEFAULT_IMAGE);
            if let Err(why) = crate::service::check_image(image) {
                bail!("{ENV_IMAGE}: {why}");
            }
            Ok(Some(image.to_string()))
        }
        other => bail!("{ENV} is {other:?}: dind, or unset for no daemon"),
    }
}

/// Why an engine with `executor` starts no daemon beside a task, given
/// what its operator offered; `None` where it does.
pub fn refusal(executor: ExecutorKind, offered: Option<&str>) -> Option<String> {
    match executor {
        ExecutorKind::Docker if offered.is_some() => None,
        ExecutorKind::Docker => Some(format!(
            "the task asks for a Docker daemon (docker_daemon: true), and this engine starts \
             none: its operator offers one with {ENV}=dind"
        )),
        ExecutorKind::Kubernetes => Some(
            "the task asks for a Docker daemon (docker_daemon: true), and this engine's tasks \
             are pods: a daemon is run beside a task by an engine on Docker"
                .to_string(),
        ),
        ExecutorKind::Local => Some(
            "the task asks for a Docker daemon (docker_daemon: true), and this engine's tasks \
             are its own processes: a daemon is run beside a task by an engine on Docker"
                .to_string(),
        ),
    }
}

/// The daemon, as the service the task is given, from `image`.
pub fn service(image: &str) -> ServiceSpec {
    ServiceSpec {
        name: NAME.to_string(),
        image: image.to_string(),
        // `dockerd` first, so the image's entrypoint adds no listener of
        // its own (a `dind` image's listens on every address), and the
        // daemon starts at once: one that listens on an address without
        // TLS, and is not told so, waits before it starts.
        args: [
            "dockerd",
            "--host=unix:///var/run/docker.sock",
            &format!("--host={HOST}"),
            "--tls=false",
        ]
        .map(str::to_string)
        .to_vec(),
        env: Vec::new(),
        ready: Some(Ready::Exec(vec!["docker".to_string(), "info".to_string()])),
        ready_timeout_secs: Some(READY_TIMEOUT_SECS),
        resources: None,
        daemon: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_engine_offers_a_daemon_when_its_operator_says_dind() {
        assert_eq!(offered(None, None).unwrap(), None);
        assert_eq!(offered(Some(" "), Some("x:1")).unwrap(), None);
        assert_eq!(
            offered(Some("dind"), None).unwrap().as_deref(),
            Some(DEFAULT_IMAGE)
        );
        assert_eq!(
            offered(Some("dind"), Some("mirror.example/docker:27-dind"))
                .unwrap()
                .as_deref(),
            Some("mirror.example/docker:27-dind")
        );
        let err = offered(Some("yes"), None).unwrap_err().to_string();
        assert!(err.contains("dind, or unset"), "{err}");
        let err = offered(Some("dind"), Some("a b")).unwrap_err().to_string();
        assert!(err.contains(ENV_IMAGE), "{err}");
    }

    #[test]
    fn a_daemon_s_runtime_is_named_only_where_one_is_offered() {
        assert_eq!(runtime(None, true).unwrap(), None);
        assert_eq!(runtime(Some(" "), false).unwrap(), None);
        assert_eq!(
            runtime(Some("sysbox-runc"), true).unwrap().as_deref(),
            Some(RUNTIME_SYSBOX)
        );
        let err = runtime(Some("sysbox-runc"), false).unwrap_err().to_string();
        assert!(err.contains("offers no daemon"), "{err}");
        let err = runtime(Some("a b"), true).unwrap_err().to_string();
        assert!(err.contains("not a runtime's name"), "{err}");
        // One that gives a container no user namespace of its own would let
        // every daemon fail, unprivileged.
        for plain in ["runc", "crun", "io.containerd.runc.v2"] {
            let err = runtime(Some(plain), true).unwrap_err().to_string();
            assert!(err.contains("only on Sysbox's runtime"), "{plain}: {err}");
        }
    }

    #[test]
    fn a_daemon_is_given_no_mount_where_it_keeps_its_own() {
        for own in [
            "/var/run",
            "/var/run/docker.sock",
            "/run/",
            "/run/containerd",
            "/var/lib/docker",
            "/var/lib/docker/volumes",
        ] {
            assert!(owns(own), "{own}");
        }
        for not in [
            "/workspace",
            "/var/lib/dockerd",
            "/runner",
            "/var",
            "/cache",
        ] {
            assert!(!owns(not), "{not}");
        }
    }

    #[test]
    fn only_an_engine_on_docker_that_offers_one_runs_a_daemon() {
        assert_eq!(refusal(ExecutorKind::Docker, Some(DEFAULT_IMAGE)), None);
        let off = refusal(ExecutorKind::Docker, None).unwrap();
        assert!(off.contains("DAGRON_DOCKER_DAEMON=dind"), "{off}");
        assert!(refusal(ExecutorKind::Kubernetes, Some(DEFAULT_IMAGE)).is_some());
        assert!(refusal(ExecutorKind::Local, Some(DEFAULT_IMAGE)).is_some());
    }

    #[test]
    fn the_daemon_is_a_service_the_task_reaches_as_docker_host() {
        let daemon = service(DEFAULT_IMAGE);
        assert!(daemon.daemon);
        assert_eq!(daemon.name, NAME);
        assert!(HOST.starts_with("tcp://127.0.0.1:"), "loopback alone");
        assert!(daemon.args.contains(&format!("--host={HOST}")));
        assert!(daemon.args.iter().all(|a| !a.contains("0.0.0.0")));
        daemon
            .validate()
            .expect("the daemon is a service as any other is");
    }
}
