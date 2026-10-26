//! **A privileged task**: `privileged: true`.
//!
//! Some jobs need what only a privileged container has: a Docker daemon
//! inside the job itself, a cluster to test against (kind, k3s), a loop
//! device, a filesystem to mount. A privileged container is root on its host,
//! with every device and every capability: whatever it runs can do on that
//! host what the host's root can. So it is the operator's to allow, and only
//! where nothing else is run:
//!
//! - **The engine allows it** ([`ENV`] set to `allow`). A task that asks for
//!   it anywhere else is failed with the reason, and is not tried again.
//! - **On a dedicated pool.** An engine that allows it claims only the runner
//!   classes it names (`RUNNER_CLASSES`), and not [`DEFAULT_RUNNER_CLASS`],
//!   which is every task's that names none: a privileged task reaches it by
//!   its `runner_class`, and no task that did not ask is run beside it. On
//!   Kubernetes its task pods are also placed on nodes of their own
//!   (`DAGRON_TASK_NODE_SELECTOR`), since a pod's privilege is its node's.
//! - **For trusted runs.** Such an engine runs no work that is not trusted,
//!   and a run that is not trusted is refused the key ([`crate::confine`]).
//! - **Where the namespace admits it** (Kubernetes): the engine asks the API
//!   server as it starts to admit a privileged pod, without making one, so a
//!   namespace whose Pod Security admission refuses one
//!   (`pod-security.kubernetes.io/enforce` other than `privileged`) stops it.
//! - **Held to none of what contradicts it.** A privileged container has
//!   every capability, may gain privilege, and runs on the host's own
//!   runtime: a task held to dropped capabilities, no new privileges, a
//!   sandbox runtime or seccomp's default profile, by its own envelope or by
//!   the floor, is refused rather than run as neither.
//!
//! | Executor   | A privileged task |
//! |------------|-------------------|
//! | Docker     | its container is privileged; what is run for it first, its services and its steps are not |
//! | Kubernetes | its container's security context is privileged; the pod's others, and its steps', are not |
//! | Local      | refused: a task there is the engine's own process |

use crate::dag::DEFAULT_RUNNER_CLASS;
use crate::isolation::{ExecutorKind, IsolationSpec, Seccomp};

/// Whether this engine runs a task that asks to be privileged: `allow`, or
/// unset (or `off`) for none.
pub const ENV: &str = "DAGRON_TASK_PRIVILEGED";

/// Read [`ENV`].
pub fn allowed(raw: Option<&str>) -> Result<bool, String> {
    match raw.map(str::trim).unwrap_or_default() {
        "" | "off" => Ok(false),
        "allow" => Ok(true),
        other => Err(format!("{ENV} is {other:?}: allow, or unset for none")),
    }
}

/// Why an engine that allows privileged tasks does not start, where it does
/// not: it runs tasks as its own processes, it is no dedicated pool (it
/// claims every class, or the default one), it also runs work that is not
/// trusted, or its floor holds every task to what a privileged one cannot
/// be held to.
pub fn engine_refusal(
    executor: ExecutorKind,
    classes: &[String],
    runs_untrusted: bool,
    floor: Option<&IsolationSpec>,
) -> Option<String> {
    if executor == ExecutorKind::Local {
        return Some(
            "it runs tasks as its own processes (EXECUTOR=local), and a privileged task is a \
             container's"
                .to_string(),
        );
    }
    if classes.is_empty() {
        return Some(format!(
            "it claims every runner class (RUNNER_CLASSES is unset): a privileged task is run \
             on a dedicated pool, an engine that claims the classes named for such work and no \
             other, and not {DEFAULT_RUNNER_CLASS}"
        ));
    }
    if classes.iter().any(|c| c == DEFAULT_RUNNER_CLASS) {
        return Some(format!(
            "it claims the {DEFAULT_RUNNER_CLASS} class (RUNNER_CLASSES), every task's that names \
             no runner_class: a privileged task is run on a dedicated pool, beside no task that \
             did not ask to be"
        ));
    }
    if runs_untrusted {
        return Some(
            "it also runs work that is not trusted (DAGRON_UNTRUSTED_ISOLATION_FLOOR): run the \
             two on engines of their own"
                .to_string(),
        );
    }
    if let Some(what) = floor.and_then(contradicted) {
        return Some(format!(
            "its floor (DAGRON_TASK_ISOLATION_FLOOR) holds every task to {what}, which a \
             privileged task cannot be held to"
        ));
    }
    None
}

/// Why a task that asks to be privileged is not run here, given whether
/// this engine allows one and the task's effective envelope; `None` where it
/// is run.
pub fn task_refusal(allowed: bool, isolation: Option<&IsolationSpec>) -> Option<String> {
    if !allowed {
        return Some(format!(
            "the task asks to be privileged (privileged: true), and this engine runs no \
             privileged task: its operator allows one on a dedicated pool ({ENV}=allow), which \
             the task reaches by its runner_class"
        ));
    }
    isolation.and_then(contradicted).map(|what| {
        format!(
            "the task asks to be privileged (privileged: true), and is held to {what} \
             (isolation), which a privileged container cannot be held to"
        )
    })
}

/// The first of what `iso` asks for that a privileged container does not
/// have, as a task writes it.
fn contradicted(iso: &IsolationSpec) -> Option<&'static str> {
    if iso.drop_all_capabilities == Some(true) {
        return Some("drop_all_capabilities");
    }
    if iso.no_new_privileges == Some(true) {
        return Some("no_new_privileges");
    }
    if iso.runtime_class.is_some() {
        return Some("a runtime_class");
    }
    if iso.seccomp == Some(Seccomp::RuntimeDefault) {
        return Some("seccomp: runtime_default");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classes(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn the_operator_allows_it_or_it_is_off() {
        assert_eq!(allowed(None), Ok(false));
        assert_eq!(allowed(Some("")), Ok(false));
        assert_eq!(allowed(Some("off")), Ok(false));
        assert_eq!(allowed(Some(" allow ")), Ok(true));
        assert!(allowed(Some("yes")).unwrap_err().contains(ENV));
    }

    #[test]
    fn an_engine_that_allows_it_is_a_dedicated_pool_of_trusted_work() {
        let docker = ExecutorKind::Docker;
        let dind = classes(&["dind"]);
        assert_eq!(engine_refusal(docker, &dind, false, None), None);
        assert_eq!(engine_refusal(ExecutorKind::Kubernetes, &dind, false, None), None);

        let local = engine_refusal(ExecutorKind::Local, &dind, false, None).unwrap();
        assert!(local.contains("EXECUTOR=local"), "{local}");
        let every = engine_refusal(docker, &[], false, None).unwrap();
        assert!(every.contains("RUNNER_CLASSES is unset"), "{every}");
        let shared = engine_refusal(docker, &classes(&["dind", "default"]), false, None).unwrap();
        assert!(shared.contains("the default class"), "{shared}");
        let untrusted = engine_refusal(docker, &dind, true, None).unwrap();
        assert!(untrusted.contains("DAGRON_UNTRUSTED_ISOLATION_FLOOR"), "{untrusted}");

        let hardened = IsolationSpec::parse_floor("hardened").unwrap();
        let floor = engine_refusal(docker, &dind, false, Some(&hardened)).unwrap();
        assert!(floor.contains("DAGRON_TASK_ISOLATION_FLOOR"), "{floor}");
        let read_only = IsolationSpec::parse_floor("read_only_root_fs=true").unwrap();
        assert_eq!(engine_refusal(docker, &dind, false, Some(&read_only)), None);
    }

    #[test]
    fn a_task_is_refused_where_it_is_not_allowed_or_is_held_to_what_contradicts_it() {
        let off = task_refusal(false, None).unwrap();
        assert!(off.contains(ENV) && off.contains("runner_class"), "{off}");
        assert_eq!(task_refusal(true, None), None);
        for (floor, what) in [
            ("drop_all_capabilities=true", "drop_all_capabilities"),
            ("no_new_privileges=true", "no_new_privileges"),
            ("runtime_class=gvisor", "runtime_class"),
            ("seccomp=runtime_default", "seccomp: runtime_default"),
        ] {
            let iso = IsolationSpec::parse_floor(floor).unwrap();
            let why = task_refusal(true, Some(&iso)).unwrap();
            assert!(why.contains(what), "{floor}: {why}");
        }
        for fine in ["read_only_root_fs=true", "run_as_user=1000", "network=none"] {
            let iso = IsolationSpec::parse_floor(fine).unwrap();
            assert_eq!(task_refusal(true, Some(&iso)), None, "{fine}");
        }
    }
}
