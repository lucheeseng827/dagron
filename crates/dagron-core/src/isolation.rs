//! **Per-task trust envelope** — what one task is allowed to do, declared on
//! the task rather than on the process, and floored by the operator.
//!
//! The gap this closes is stated in `docs/FAMILIES.md` (family 3). dagron could
//! already harden task pods — `DAGRON_TASK_RUNTIME_CLASS`,
//! `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT`, `DAGRON_TASK_READ_ONLY_ROOT_FS` and
//! the rest of [`PodHardening`](../../dagron_executor/kube_executor/struct.PodHardening.html)
//! — but every one of those is read from the **engine's** environment at boot,
//! so the envelope is a property of the scheduler process and identical for
//! every task it dispatches. That is fine for one team running its own DAGs and
//! wrong for the workload family this exists for: running *other people's* code.
//! A tenant's untrusted step and the platform's own trusted step share a
//! scheduler, and today they share a trust envelope too.
//!
//! Two things are needed, and neither is the other:
//!
//! 1. **Declaration** — a task states the envelope it wants ([`IsolationSpec`]),
//!    so a workflow can ask for gVisor on the step that runs a customer
//!    container without forcing it on the step that writes to the warehouse.
//! 2. **A floor** — the operator states the envelope no task may go below
//!    ([`IsolationSpec::apply_floor`]). This is the half that makes declaration
//!    safe: if a workflow author could only *set* the envelope, then in a
//!    multi-tenant engine the author of the untrusted workload would be the one
//!    choosing how untrusted it is.
//!
//! The floor is applied by taking the stronger of each field, never the weaker,
//! and the tightenings are returned so the engine can say what it did instead of
//! silently disagreeing with the spec. What actually took effect is what gets
//! attested ([`IsolationSpec::canonical`] feeds `isolation_digest` in
//! `dagron_crypto::attest`), so "this ran under gVisor with no capabilities" is
//! a signed claim rather than a configuration someone believes was in force.
//!
//! ## What this is not
//!
//! It is a *declaration and enforcement seam*, not a sandbox dagron implements.
//! The isolation is delivered by the runtime the pod names — gVisor, Kata, a
//! confidential runtime class — or by what a container runtime does when it is
//! asked. Not every executor can ask for every field: a subprocess runs as the
//! engine does, a container takes no Kubernetes runtime class, and a pod's
//! network is taken away only by a NetworkPolicy, on a cluster whose CNI
//! enforces one. [`IsolationSpec::enforceable_by`]
//! says so per executor, and a task that asked for an envelope the executor
//! cannot deliver must be refused rather than run unprotected: the failure mode
//! this whole module exists to prevent is a task that *believes* it is sandboxed.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// The seccomp posture for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Seccomp {
    /// No profile — the container runtime's syscall surface, unfiltered.
    /// Ordered first because it is the *weakest*: `Ord` is what
    /// [`IsolationSpec::apply_floor`] uses to take the stronger of two values,
    /// so the declaration order of this enum is load-bearing.
    Unconfined,
    /// The container runtime's default profile (`RuntimeDefault` in Kubernetes).
    RuntimeDefault,
}

/// What of the network a task has: `default`, `allow:<name>` or `none`,
/// each stronger than the one before it.
///
/// It is not `Ord`. Two allowlists are not one stronger than the other, so
/// what a floor does with this field is written out where it is done
/// ([`IsolationSpec::apply_floor`]) and not left to a derived order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Network {
    /// The network its executor gives every task. The weakest.
    Default,
    /// What an allowlist of the operator's lets the task reach, and nothing
    /// else. The name is all a task says. What the name allows is the
    /// operator's: on Kubernetes a NetworkPolicy of theirs, made before any
    /// task and read by the engine as it starts. A task picks one of those
    /// and cannot write one.
    Allow(String),
    /// None. The task has its own loopback interface and nothing else: no
    /// name resolves, and no address but its own answers.
    None,
}

/// The longest name an allowlist may have. A pod is labelled
/// `allow-<name>`, and a label's value is 63 characters at most.
pub const ALLOWLIST_NAME_MAX: usize = 57;

/// Whether `name` can name an allowlist: a DNS label, since it becomes part
/// of an object's name and a label's value.
pub fn valid_allowlist_name(name: &str) -> std::result::Result<(), String> {
    let shaped = !name.is_empty()
        && name.len() <= ALLOWLIST_NAME_MAX
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if shaped {
        Ok(())
    } else {
        Err(format!(
            "an allowlist's name is 1 to {ALLOWLIST_NAME_MAX} characters of [a-z0-9-], \
             neither starting nor ending with '-', and '{name}' is not"
        ))
    }
}

/// `DAGRON_TASK_NETWORK_ALLOWLISTS`: the allowlists the operator has made,
/// by name, comma-separated. A task names one (`isolation.network:
/// allow:<name>`) and cannot write one; one that names an allowlist not
/// listed here is refused. What a name allows is the operator's: on
/// Kubernetes a NetworkPolicy of theirs, on Docker an internal network of
/// theirs, each named `dagron-task-allow-<name>`.
pub const ENV_NETWORK_ALLOWLISTS: &str = "DAGRON_TASK_NETWORK_ALLOWLISTS";

/// Parse [`ENV_NETWORK_ALLOWLISTS`]: names, comma-separated, each once. A
/// name that could not be one is an error at startup.
pub fn parse_allowlists(raw: &str) -> std::result::Result<Vec<String>, String> {
    let mut names: Vec<String> = Vec::new();
    for name in raw.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        valid_allowlist_name(name)?;
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    Ok(names)
}

/// The name of what holds a task to the allowlist `name`: the operator's
/// NetworkPolicy on Kubernetes, their network on Docker.
pub fn allowlist_object_name(name: &str) -> String {
    format!("dagron-task-allow-{name}")
}

/// Where an operator says an allowlist reaches the rest of the world through
/// a proxy of theirs: a label of its network on Docker, an annotation of its
/// NetworkPolicy on Kubernetes. A task held to that allowlist is told of the
/// proxy (`HTTP_PROXY` and the rest), so that what it fetches by name goes
/// through it, and the proxy is what holds it to a list of hostnames: a
/// network, or a NetworkPolicy, knows addresses and nothing of names.
pub const ALLOWLIST_PROXY: &str = "dagron.dev/proxy";

/// Read an allowlist's proxy ([`ALLOWLIST_PROXY`]): `http://` or `https://`,
/// a host and a port, and nothing more. No user or password: every process
/// of a task reads its environment, and a proxy's credentials would be the
/// task's to read.
pub fn parse_allowlist_proxy(raw: &str) -> std::result::Result<String, String> {
    let raw = raw.trim();
    let rest = raw
        .strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
        .ok_or_else(|| format!("{raw:?} is not an http:// or https:// URL"))?;
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.contains('@') {
        return Err(format!(
            "{raw:?} carries a user: a task reads its proxy from its environment, and the \
             proxy's credentials with it"
        ));
    }
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| format!("{raw:?} names no port"))?;
    // A bracketed host is an IPv6 address, and nothing else: a `/`, a space
    // or a `,` there would reach every task's `HTTP_PROXY` as written.
    let host_ok = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        Some(v6) => !v6.is_empty() && v6.chars().all(|c| c.is_ascii_hexdigit() || matches!(c, ':' | '.')),
        None => {
            !host.is_empty()
                && host
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
        }
    };
    if !host_ok || port.parse::<u16>().map_or(true, |p| p == 0) {
        return Err(format!(
            "{raw:?} is not a proxy's URL: a scheme, a host and a port, and no path"
        ));
    }
    Ok(raw.trim_end_matches('/').to_string())
}

/// What a task held to an allowlist with a proxy is told: the proxy, under
/// each name tools read it by, and what not to send through it, which is
/// the task's own loopback and the names of its services (each of them on
/// that loopback, which the proxy cannot reach).
pub fn proxy_env<'a>(
    proxy: &str,
    local: impl IntoIterator<Item = &'a str>,
) -> Vec<(String, String)> {
    let mut no_proxy = vec!["localhost", "127.0.0.1", "::1"];
    no_proxy.extend(local);
    let no_proxy = no_proxy.join(",");
    let mut env = Vec::new();
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
        env.push((name.to_string(), proxy.to_string()));
    }
    for name in ["NO_PROXY", "no_proxy"] {
        env.push((name.to_string(), no_proxy.clone()));
    }
    env
}

impl Network {
    const ALLOW: &'static str = "allow:";

    /// The spelling a spec, a floor and the attested envelope use.
    pub fn label(&self) -> String {
        match self {
            Self::Default => "default".into(),
            Self::Allow(name) => format!("{}{name}", Self::ALLOW),
            Self::None => "none".into(),
        }
    }

    /// Read that spelling. The error says what is expected, for the caller
    /// to say of what.
    pub fn parse(raw: &str) -> std::result::Result<Self, String> {
        match raw {
            "default" => Ok(Self::Default),
            "none" => Ok(Self::None),
            other => match other.strip_prefix(Self::ALLOW) {
                Some(name) => {
                    valid_allowlist_name(name)?;
                    Ok(Self::Allow(name.to_string()))
                }
                None => Err(format!("expects none|default|allow:<name>, got '{other}'")),
            },
        }
    }

    /// The allowlist this names, where it names one.
    pub fn allowlist(&self) -> Option<&str> {
        match self {
            Self::Allow(name) => Some(name),
            _ => None,
        }
    }
}

impl Serialize for Network {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.label())
    }
}

impl<'de> Deserialize<'de> for Network {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(&raw).map_err(|why| serde::de::Error::custom(format!("network {why}")))
    }
}

/// Executors an envelope can be asked to enforce. Named here rather than
/// imported so `dagron-core` keeps no dependency on the executor crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorKind {
    Local,
    Docker,
    Kubernetes,
}

/// An executor, and what its operator has said of what is under it. An
/// envelope is judged against both, since one field's enforcement is not the
/// executor's alone: whether a NetworkPolicy does anything is the cluster's
/// CNI's to decide, and only the operator knows which CNI that is.
///
/// An [`ExecutorKind`] alone is one that has been told nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enforcer {
    pub kind: ExecutorKind,
    /// Kubernetes only: the operator has said that the cluster's CNI enforces
    /// NetworkPolicy, and the task namespace holds the policy that takes the
    /// network from a pod labelled for it.
    pub network_policy: bool,
    /// The allowlists the operator has made, by name: a task may name one of
    /// these ([`Network::Allow`]) and no other.
    pub allowlists: Vec<String>,
}

impl From<ExecutorKind> for Enforcer {
    fn from(kind: ExecutorKind) -> Self {
        Self {
            kind,
            network_policy: false,
            allowlists: Vec::new(),
        }
    }
}

impl From<&Enforcer> for Enforcer {
    fn from(enforcer: &Enforcer) -> Self {
        enforcer.clone()
    }
}

impl ExecutorKind {
    /// Parse the `EXECUTOR` env value. Unknown values are `None` — the caller
    /// decides whether that is a startup error or a reason to skip the check.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "local" => Some(Self::Local),
            "docker" => Some(Self::Docker),
            "k8s" | "kube" | "kubernetes" => Some(Self::Kubernetes),
            _ => None,
        }
    }

    /// The spelling used in refusal messages — matches the `EXECUTOR` value an
    /// operator would have set, so the error names something they can act on.
    fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Docker => "docker",
            Self::Kubernetes => "k8s",
        }
    }
}

/// `isolation:` — the trust envelope for one task.
///
/// Every field is an `Option` so that "unset" is distinguishable from "set to
/// the weak value". The difference matters for the floor: an unset field takes
/// the floor's value, whereas a field explicitly set weaker than the floor is a
/// request that gets refused and reported.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
// A misspelled key must not deserialize into an empty envelope. `parse_floor`
// already refuses an unknown key on the operator's side, for the reason stated
// there — a floor with a typo in it is a floor that is not in force — and the
// author's side has exactly the same failure: `drop_all_capabilties: true`
// would silently yield no hardening at all. The floor still holds the line, so
// nothing drops below it, but hardening someone asked for must never go missing
// without a word.
#[serde(deny_unknown_fields)]
pub struct IsolationSpec {
    /// Kubernetes `runtimeClassName` — `gvisor`, `kata-qemu`, `kata-qemu-tdx`.
    ///
    /// Not orderable, so the floor does not "strengthen" it: a floor that names
    /// a runtime class **pins** it, and a task naming a different one is
    /// refused. Which sandbox runtime exists on a node is an operator fact, and
    /// a workflow author guessing at it is either wrong or attempting an escape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    /// Seccomp posture. See [`Seccomp`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seccomp: Option<Seccomp>,
    /// Mount the container's root filesystem read-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_root_fs: Option<bool>,
    /// Forbid privilege escalation (`allowPrivilegeEscalation: false`, the
    /// `no_new_privs` bit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_new_privileges: Option<bool>,
    /// Drop every Linux capability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drop_all_capabilities: Option<bool>,
    /// Refuse to start if the image's user resolves to uid 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_as_non_root: Option<bool>,
    /// Numeric uid to run as. `0` is rejected at validation: a spec asking to
    /// run as root should say so by leaving this unset and
    /// `run_as_non_root: false`, not by writing a uid that reads as a value
    /// when it is an opt-out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_as_user: Option<i64>,
    /// Mount a ServiceAccount token into the task pod. Its floor direction is
    /// *false is stronger*: a token is an ambient credential, and on an IRSA
    /// cluster it is an IAM role handed to whatever the task runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_token: Option<bool>,
    /// What of the network the task has. See [`Network`]. `none` is for a
    /// step that should reach nothing: one that runs code nobody here wrote,
    /// on inputs already in its workspace. `allow:<name>` is for one that
    /// should reach a registry, a forge or a database and nothing else: the
    /// name is an allowlist the operator made.
    ///
    /// It is the task's container that has no network. What an executor runs
    /// for the task before it (the container that installs its tools, the one
    /// that fetches its checkout) is the operator's, and is on the operator's
    /// network: a checkout could not be fetched otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<Network>,
}

/// One field the floor overrode, for reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tightened {
    pub field: &'static str,
    /// What the spec asked for (`"unset"` when it asked for nothing).
    pub requested: String,
    /// What it will actually run under.
    pub applied: String,
}

impl std::fmt::Display for Tightened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} -> {}", self.field, self.requested, self.applied)
    }
}

impl IsolationSpec {
    /// A conservative envelope: unprivileged, unwritable, uncapable, filtered.
    ///
    /// Provided as the *recommended floor* for an engine that runs untrusted
    /// work, not as a default — defaults that silently change what a working
    /// task may do do not belong in a minor release, the same reasoning that
    /// left `PodHardening` opt-in.
    pub fn hardened() -> Self {
        Self {
            runtime_class: None,
            seccomp: Some(Seccomp::RuntimeDefault),
            read_only_root_fs: Some(true),
            no_new_privileges: Some(true),
            drop_all_capabilities: Some(true),
            run_as_non_root: Some(true),
            run_as_user: None,
            service_account_token: Some(false),
            // Not part of it: a task with no network cannot fetch what most
            // tasks fetch, and a floor that breaks them is one nobody sets.
            network: None,
        }
    }

    /// True when nothing is declared.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Reject a spec that cannot mean what it says.
    pub fn validate(&self) -> Result<()> {
        if let Some(rc) = &self.runtime_class {
            // A RuntimeClass name is a Kubernetes object name (RFC 1123
            // subdomain). Validated here so a typo fails when the workflow is
            // submitted rather than as an unschedulable pod much later.
            if rc.is_empty() || rc.len() > 253 {
                bail!("isolation.runtime_class must be 1-253 characters, got {}", rc.len());
            }
            // A RuntimeClass name is a DNS-1123 *subdomain*: dot-separated
            // labels, each non-empty and each bounded by an alphanumeric.
            // Checking only the whole string's ends let `a..b` (empty label)
            // and `a-.b` (label ending in '-') through, and Kubernetes would
            // then refuse the pod at submission — the late failure this
            // validation exists to prevent.
            // 63 is the DNS-1123 label cap; the 253 above is the cap on the
            // whole subdomain. Both are the apiserver's, and a name that only
            // fails the per-label one still gets the pod rejected at
            // submission — the late failure this check exists to prevent.
            let valid_label = |label: &str| {
                !label.is_empty()
                    && label.len() <= 63
                    && label
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                    && !label.starts_with('-')
                    && !label.ends_with('-')
            };
            if !rc.split('.').all(valid_label) {
                bail!(
                    "isolation.runtime_class '{rc}' must be a lowercase RFC 1123 subdomain: \
                     dot-separated labels of [a-z0-9-], each non-empty and neither starting \
                     nor ending with '-'"
                );
            }
        }
        match self.run_as_user {
            Some(0) => bail!(
                "isolation.run_as_user must not be 0 — to run as root, leave run_as_user unset \
                 and set run_as_non_root: false, so the spec says what it means"
            ),
            Some(u) if u < 0 => bail!("isolation.run_as_user must not be negative, got {u}"),
            _ => {}
        }
        if self.run_as_non_root == Some(true) && self.run_as_user == Some(0) {
            // Unreachable given the check above; kept as a guard in case the
            // uid rule is ever loosened.
            bail!("isolation.run_as_non_root: true contradicts run_as_user: 0");
        }
        // Read from text, a name is checked as it is read. One built in code
        // is checked here, before it becomes a label and an object's name.
        if let Some(Network::Allow(name)) = &self.network {
            valid_allowlist_name(name).map_err(|why| anyhow::anyhow!("isolation.network: {why}"))?;
        }
        Ok(())
    }

    /// Return this envelope raised to `floor`, plus every field the floor
    /// overrode.
    ///
    /// "Raised" means the stronger of the two per field, never the weaker — so
    /// a task may harden itself beyond the floor freely, and may not go under
    /// it at all. `runtime_class` is the exception and is *pinned* rather than
    /// compared: see the field's documentation.
    pub fn apply_floor(&self, floor: &IsolationSpec) -> (IsolationSpec, Vec<Tightened>) {
        let mut out = self.clone();
        let mut notes = Vec::new();

        /// Take the stronger of a requested and a floored boolean.
        ///
        /// `strong` says which value is the hardening one, because the fields
        /// do not agree: `true` hardens `read_only_root_fs`, while `false` is
        /// what hardens `service_account_token`. A floor set to the weak value
        /// constrains nothing.
        fn stronger_bool(
            field: &'static str,
            requested: Option<bool>,
            floor: Option<bool>,
            // Which value is the strong one: `true` for read-only-root-fs,
            // `false` for mounting a service-account token.
            strong: bool,
            notes: &mut Vec<Tightened>,
        ) -> Option<bool> {
            let Some(f) = floor else { return requested };
            if f != strong {
                // The floor does not constrain this field in the strong
                // direction, so it imposes nothing.
                return requested;
            }
            if requested == Some(strong) {
                return requested;
            }
            notes.push(Tightened {
                field,
                requested: requested.map(|b| b.to_string()).unwrap_or_else(|| "unset".into()),
                applied: strong.to_string(),
            });
            Some(strong)
        }

        if let Some(f) = floor.seccomp {
            let requested = self.seccomp;
            // A floor of `Unconfined` is the weak value: it constrains nothing,
            // exactly as `stronger_bool` returns early when the floor is not the
            // hardening value. Without this guard an unset task would be pinned
            // to `Unconfined`, which the canonical digest would record and
            // `PodHardening::with_isolation` would turn into
            // `seccomp_runtime_default = false` — overriding the operator's own
            // `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT` in the name of a floor.
            if f == Seccomp::Unconfined {
                // fall through: nothing to raise
            } else if requested.is_none() || requested < Some(f) {
                notes.push(Tightened {
                    field: "seccomp",
                    requested: requested
                        .map(|s| format!("{s:?}"))
                        .unwrap_or_else(|| "unset".into()),
                    applied: format!("{f:?}"),
                });
                out.seccomp = Some(f);
            }
        }

        out.read_only_root_fs = stronger_bool(
            "read_only_root_fs",
            self.read_only_root_fs,
            floor.read_only_root_fs,
            true,
            &mut notes,
        );
        out.no_new_privileges = stronger_bool(
            "no_new_privileges",
            self.no_new_privileges,
            floor.no_new_privileges,
            true,
            &mut notes,
        );
        out.drop_all_capabilities = stronger_bool(
            "drop_all_capabilities",
            self.drop_all_capabilities,
            floor.drop_all_capabilities,
            true,
            &mut notes,
        );
        out.run_as_non_root = stronger_bool(
            "run_as_non_root",
            self.run_as_non_root,
            floor.run_as_non_root,
            true,
            &mut notes,
        );
        out.service_account_token = stronger_bool(
            "service_account_token",
            self.service_account_token,
            floor.service_account_token,
            false,
            &mut notes,
        );

        // The stronger of the two: `none` over an allowlist over the
        // executor's own network. A floor of `default` is the weak value and
        // constrains nothing, as for seccomp. Two allowlists are not one
        // stronger than the other, so a floor that names one pins it, as a
        // floor's runtime class is pinned: a task that named another gets the
        // floor's, and is told.
        let raised = match (&floor.network, &self.network) {
            (Some(Network::None), asked) if *asked != Some(Network::None) => Some(Network::None),
            (Some(Network::Allow(pinned)), asked) => match asked {
                Some(Network::None) => None,
                Some(Network::Allow(named)) if named == pinned => None,
                _ => Some(Network::Allow(pinned.clone())),
            },
            _ => None,
        };
        if let Some(applied) = raised {
            notes.push(Tightened {
                field: "network",
                requested: self
                    .network
                    .as_ref()
                    .map(Network::label)
                    .unwrap_or_else(|| "unset".into()),
                applied: applied.label(),
            });
            out.network = Some(applied);
        }

        // A floor uid applies unless the task picked its own non-root uid. A
        // task that wants uid 12000 instead of the floor's 65534 is not asking
        // for more privilege, and refusing it would make the floor unusable for
        // images that ship a fixed user.
        if let Some(f) = floor.run_as_user {
            if self.run_as_user.is_none() {
                notes.push(Tightened {
                    field: "run_as_user",
                    requested: "unset".into(),
                    applied: f.to_string(),
                });
                out.run_as_user = Some(f);
            }
        }

        // A pinned runtime class is not negotiable in either direction: the
        // operator knows which sandbox runtimes the nodes actually have.
        if let Some(f) = &floor.runtime_class {
            if self.runtime_class.as_deref() != Some(f.as_str()) {
                notes.push(Tightened {
                    field: "runtime_class",
                    requested: self.runtime_class.clone().unwrap_or_else(|| "unset".into()),
                    applied: f.clone(),
                });
                out.runtime_class = Some(f.clone());
            }
        }

        (out, notes)
    }

    /// Which fields of this envelope `executor` has no way to deliver.
    ///
    /// Empty means the envelope is enforceable as written. A non-empty result
    /// on a task that asked for isolation is a refusal, not a warning: a step
    /// that believes it is sandboxed and is not is worse than one that never
    /// claimed to be, because the belief is what a tenant's threat model was
    /// built on.
    pub fn enforceable_by(&self, executor: impl Into<Enforcer>) -> Vec<&'static str> {
        let executor = executor.into();
        let mut unmet = Vec::new();
        match executor.kind {
            // A pod has no switch for its network. A NetworkPolicy is what
            // takes it away, and only where the cluster's CNI enforces one,
            // which the API does not say. So the field is deliverable where
            // the operator has said so, and nowhere else: a pod labelled for
            // a policy nothing enforces reaches what any other does. An
            // allowlist is a policy too, and one of the operator's making:
            // deliverable where they have said the cluster enforces one and
            // have named this one.
            ExecutorKind::Kubernetes => {
                let held = match &self.network {
                    Some(Network::None) => executor.network_policy,
                    Some(Network::Allow(name)) => {
                        executor.network_policy && executor.allowlists.contains(name)
                    }
                    Some(Network::Default) | None => true,
                };
                if !held {
                    unmet.push("network");
                }
            }
            // A container is made with what the daemon is asked for, and it
            // can be asked for all of this but a Kubernetes runtime class,
            // which names a handler on a cluster's nodes and nothing a daemon
            // has. The rest is `DockerExecutor`'s to ask for, field by field.
            // `service_account_token: false` is met by there being none.
            // A daemon has nothing that gives a container part of a network,
            // so an allowlist is a whole network of the operator's: an
            // internal one, which reaches nothing but what they put on it.
            // Deliverable where they have named it, and the executor reads
            // each one it is given as it starts.
            ExecutorKind::Docker => {
                if self.runtime_class.is_some() {
                    unmet.push("runtime_class");
                }
                if let Some(Network::Allow(name)) = &self.network {
                    if !executor.allowlists.contains(name) {
                        unmet.push("network");
                    }
                }
            }
            // The Local executor runs the task as a subprocess, in the
            // engine's own trust domain: no field has an enforcement path.
            // Listed one by one, so that a refusal names what was asked for.
            ExecutorKind::Local => {
                if self.runtime_class.is_some() {
                    unmet.push("runtime_class");
                }
                if matches!(self.seccomp, Some(Seccomp::RuntimeDefault)) {
                    unmet.push("seccomp");
                }
                if self.read_only_root_fs == Some(true) {
                    unmet.push("read_only_root_fs");
                }
                if self.no_new_privileges == Some(true) {
                    unmet.push("no_new_privileges");
                }
                if self.drop_all_capabilities == Some(true) {
                    unmet.push("drop_all_capabilities");
                }
                if self.run_as_non_root == Some(true) {
                    unmet.push("run_as_non_root");
                }
                if self.run_as_user.is_some() {
                    unmet.push("run_as_user");
                }
                if self.service_account_token == Some(false) {
                    unmet.push("service_account_token");
                }
                if matches!(self.network, Some(Network::None | Network::Allow(_))) {
                    unmet.push("network");
                }
            }
        }
        unmet
    }

    /// Refuse an envelope `executor` cannot deliver, naming every unmet field.
    pub fn require_enforceable_by(&self, executor: impl Into<Enforcer>) -> Result<()> {
        let executor = executor.into();
        let unmet = self.enforceable_by(&executor);
        if !unmet.is_empty() {
            bail!(
                "the {} executor cannot enforce isolation.{} — a task must not run believing it \
                 is sandboxed when it is not; drop the field, or dispatch this task to a \
                 runner_class whose executor can: the Kubernetes executor enforces every field, \
                 network only where its operator has said the cluster enforces NetworkPolicy \
                 (DAGRON_TASK_NETWORK_POLICY=enforced) and an allowlist only where they have \
                 named it (DAGRON_TASK_NETWORK_ALLOWLISTS), and the Docker executor every field \
                 but runtime_class, an allowlist only where its operator has named it",
                executor.kind.label(),
                unmet.join(", isolation."),
            );
        }
        Ok(())
    }

    /// A stable, human-readable rendering of the *effective* envelope, used for
    /// the attestation digest and for logging what a task ran under.
    ///
    /// Sorted keys, `key=value` per line. Unset fields are omitted rather than
    /// rendered as a default, so a digest distinguishes "the operator required
    /// a read-only root" from "nobody said anything about the root filesystem".
    pub fn canonical(&self) -> String {
        let mut fields: BTreeMap<&'static str, String> = BTreeMap::new();
        if let Some(v) = &self.runtime_class {
            fields.insert("runtime_class", v.clone());
        }
        if let Some(v) = self.seccomp {
            fields.insert(
                "seccomp",
                match v {
                    Seccomp::Unconfined => "unconfined",
                    Seccomp::RuntimeDefault => "runtime_default",
                }
                .to_string(),
            );
        }
        if let Some(v) = self.read_only_root_fs {
            fields.insert("read_only_root_fs", v.to_string());
        }
        if let Some(v) = self.no_new_privileges {
            fields.insert("no_new_privileges", v.to_string());
        }
        if let Some(v) = self.drop_all_capabilities {
            fields.insert("drop_all_capabilities", v.to_string());
        }
        if let Some(v) = self.run_as_non_root {
            fields.insert("run_as_non_root", v.to_string());
        }
        if let Some(v) = self.run_as_user {
            fields.insert("run_as_user", v.to_string());
        }
        if let Some(v) = self.service_account_token {
            fields.insert("service_account_token", v.to_string());
        }
        if let Some(v) = &self.network {
            fields.insert("network", v.label());
        }
        fields
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Parse an operator floor from its env spelling: the same `key=value`
    /// form [`IsolationSpec::canonical`] emits, comma- or newline-separated.
    ///
    /// ```text
    /// DAGRON_TASK_ISOLATION_FLOOR="seccomp=runtime_default,drop_all_capabilities=true,\
    ///                              read_only_root_fs=true,run_as_non_root=true,\
    ///                              service_account_token=false"
    /// DAGRON_TASK_ISOLATION_FLOOR=hardened     # the IsolationSpec::hardened() preset
    /// ```
    ///
    /// An unknown key is an error rather than an ignored line: a floor with a
    /// typo in it is a floor that is not in force, and discovering that from a
    /// breach report is the whole failure this module exists to avoid.
    pub fn parse_floor(raw: &str) -> Result<IsolationSpec> {
        let raw = raw.trim();
        if raw.eq_ignore_ascii_case("hardened") {
            return Ok(Self::hardened());
        }
        let mut out = IsolationSpec::default();
        for entry in raw.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()) {
            let (key, value) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("isolation floor entry '{entry}' is not key=value"))?;
            let (key, value) = (key.trim(), value.trim());
            let as_bool = || -> Result<bool> {
                match value {
                    "true" | "1" | "yes" | "on" => Ok(true),
                    "false" | "0" | "no" | "off" => Ok(false),
                    other => bail!("isolation floor '{key}' expects a boolean, got '{other}'"),
                }
            };
            match key {
                "runtime_class" => out.runtime_class = Some(value.to_string()),
                "seccomp" => {
                    out.seccomp = Some(match value {
                        "runtime_default" => Seccomp::RuntimeDefault,
                        "unconfined" => Seccomp::Unconfined,
                        other => bail!(
                            "isolation floor 'seccomp' expects runtime_default|unconfined, \
                             got '{other}'"
                        ),
                    })
                }
                "read_only_root_fs" => out.read_only_root_fs = Some(as_bool()?),
                "no_new_privileges" => out.no_new_privileges = Some(as_bool()?),
                "drop_all_capabilities" => out.drop_all_capabilities = Some(as_bool()?),
                "run_as_non_root" => out.run_as_non_root = Some(as_bool()?),
                "run_as_user" => {
                    out.run_as_user = Some(
                        value
                            .parse()
                            .map_err(|_| anyhow::anyhow!("isolation floor 'run_as_user' expects an integer, got '{value}'"))?,
                    )
                }
                "service_account_token" => out.service_account_token = Some(as_bool()?),
                "network" => {
                    out.network = Some(
                        Network::parse(value)
                            .map_err(|why| anyhow::anyhow!("isolation floor 'network' {why}"))?,
                    )
                }
                other => bail!(
                    "unknown isolation floor key '{other}' — a floor with a typo in it is a \
                     floor that is not in force"
                ),
            }
        }
        out.validate()?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_task_may_harden_beyond_the_floor_but_not_under_it() {
        let floor = IsolationSpec {
            drop_all_capabilities: Some(true),
            ..Default::default()
        };

        // Under: an explicit `false` is overridden and reported.
        let weak = IsolationSpec {
            drop_all_capabilities: Some(false),
            ..Default::default()
        };
        let (effective, notes) = weak.apply_floor(&floor);
        assert_eq!(effective.drop_all_capabilities, Some(true));
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].field, "drop_all_capabilities");
        assert_eq!(notes[0].requested, "false");

        // Beyond: extra hardening the floor never asked for survives untouched.
        let strong = IsolationSpec {
            drop_all_capabilities: Some(true),
            read_only_root_fs: Some(true),
            ..Default::default()
        };
        let (effective, notes) = strong.apply_floor(&floor);
        assert_eq!(effective.read_only_root_fs, Some(true));
        assert!(notes.is_empty(), "{notes:?}");
    }

    #[test]
    fn an_unset_field_takes_the_floors_value() {
        let floor = IsolationSpec::hardened();
        let (effective, notes) = IsolationSpec::default().apply_floor(&floor);
        assert_eq!(effective, floor);
        assert!(
            notes.iter().all(|n| n.requested == "unset"),
            "a silent spec asked for nothing, so every note is a default: {notes:?}"
        );
    }

    #[test]
    fn the_service_account_token_floor_runs_the_other_way() {
        // `false` is the strong value here, so a floor of `false` must override
        // a task asking for `true` — the direction the generic bool rule would
        // get backwards.
        let floor = IsolationSpec {
            service_account_token: Some(false),
            ..Default::default()
        };
        let wants_token = IsolationSpec {
            service_account_token: Some(true),
            ..Default::default()
        };
        let (effective, notes) = wants_token.apply_floor(&floor);
        assert_eq!(effective.service_account_token, Some(false));
        assert_eq!(notes.len(), 1, "{notes:?}");
    }

    #[test]
    fn seccomp_strength_ordering_is_respected_in_both_directions() {
        let floor = IsolationSpec {
            seccomp: Some(Seccomp::RuntimeDefault),
            ..Default::default()
        };
        let unconfined = IsolationSpec {
            seccomp: Some(Seccomp::Unconfined),
            ..Default::default()
        };
        let (effective, notes) = unconfined.apply_floor(&floor);
        assert_eq!(effective.seccomp, Some(Seccomp::RuntimeDefault));
        assert_eq!(notes.len(), 1, "{notes:?}");

        // A floor of `unconfined` constrains nothing — it is the weak value.
        let weak_floor = IsolationSpec {
            seccomp: Some(Seccomp::Unconfined),
            ..Default::default()
        };
        let hardened = IsolationSpec {
            seccomp: Some(Seccomp::RuntimeDefault),
            ..Default::default()
        };
        let (effective, notes) = hardened.apply_floor(&weak_floor);
        assert_eq!(effective.seccomp, Some(Seccomp::RuntimeDefault));
        assert!(notes.is_empty(), "{notes:?}");
    }

    #[test]
    fn a_misspelled_isolation_key_is_refused_rather_than_silently_dropped() {
        // The failure this guards: `drop_all_capabilties` (transposed) parsing
        // into an empty envelope, so the author's hardening vanishes and the
        // task runs on the floor alone with nothing said about it.
        let err = serde_yaml::from_str::<IsolationSpec>("drop_all_capabilties: true")
            .expect_err("a misspelled key must not deserialize");
        assert!(
            err.to_string().contains("unknown field"),
            "unexpected error: {err}"
        );

        // The correct spelling still parses, of course.
        let ok: IsolationSpec = serde_yaml::from_str("drop_all_capabilities: true").unwrap();
        assert_eq!(ok.drop_all_capabilities, Some(true));
    }

    #[test]
    fn an_unconfined_floor_constrains_nothing_even_for_an_unset_task() {
        // `Unconfined` is the weak value, so a floor set to it imposes nothing —
        // the same rule the boolean fields follow. Pinning an unset task to it
        // would be recorded in the canonical digest and would switch off the
        // operator's own DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT.
        let floor = IsolationSpec {
            seccomp: Some(Seccomp::Unconfined),
            ..Default::default()
        };
        let (effective, notes) = IsolationSpec::default().apply_floor(&floor);
        assert_eq!(
            effective.seccomp, None,
            "an unset task must keep the process default, not be pinned to unconfined"
        );
        assert!(notes.is_empty(), "nothing was tightened: {notes:?}");
        assert_eq!(effective.canonical(), "", "and nothing is attested about seccomp");
    }

    #[test]
    fn a_pinned_runtime_class_cannot_be_swapped_by_a_workflow_author() {
        let floor = IsolationSpec {
            runtime_class: Some("gvisor".into()),
            ..Default::default()
        };
        // The escape attempt: name a runtime class that is not a sandbox.
        let escape = IsolationSpec {
            runtime_class: Some("runc".into()),
            ..Default::default()
        };
        let (effective, notes) = escape.apply_floor(&floor);
        assert_eq!(effective.runtime_class.as_deref(), Some("gvisor"));
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].requested, "runc");
    }

    #[test]
    fn a_task_keeps_its_own_non_root_uid_but_inherits_the_floors_when_silent() {
        let floor = IsolationSpec {
            run_as_user: Some(65534),
            ..Default::default()
        };

        let own = IsolationSpec {
            run_as_user: Some(12000),
            ..Default::default()
        };
        let (effective, notes) = own.apply_floor(&floor);
        assert_eq!(
            effective.run_as_user,
            Some(12000),
            "an image with a fixed non-root user must still work under a uid floor"
        );
        assert!(notes.is_empty(), "{notes:?}");

        let (effective, notes) = IsolationSpec::default().apply_floor(&floor);
        assert_eq!(effective.run_as_user, Some(65534));
        assert_eq!(notes.len(), 1);
    }

    #[test]
    fn applying_a_floor_is_idempotent() {
        let floor = IsolationSpec::hardened();
        let (once, _) = IsolationSpec::default().apply_floor(&floor);
        let (twice, notes) = once.apply_floor(&floor);
        assert_eq!(once, twice);
        assert!(
            notes.is_empty(),
            "re-applying a floor to an already-floored spec must report nothing: {notes:?}"
        );
    }

    #[test]
    fn root_is_declared_not_smuggled_through_a_uid() {
        let err = IsolationSpec {
            run_as_user: Some(0),
            ..Default::default()
        }
        .validate()
        .unwrap_err();
        assert!(err.to_string().contains("must not be 0"), "{err}");

        assert!(IsolationSpec {
            run_as_user: Some(-1),
            ..Default::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn a_bad_runtime_class_fails_at_submit_not_at_schedule() {
        for bad in [
            "", "GVisor", "-gvisor", "gvisor-", "gvi sor", "gvisor_x",
            // Per-label failures: an empty label, and labels bounded by '-'.
            // Both passed a whole-string check and both are rejected by the
            // apiserver, which is far too late to find out.
            "a..b", "a-.b", "a.-b", ".gvisor", "gvisor.",
        ] {
            assert!(
                IsolationSpec { runtime_class: Some(bad.into()), ..Default::default() }
                    .validate()
                    .is_err(),
                "'{bad}' should be refused"
            );
        }
        // A 64-character label is one over the DNS-1123 cap, and the whole
        // string is well under the 253-character subdomain cap — so only the
        // per-label check can catch it.
        assert!(
            IsolationSpec { runtime_class: Some("a".repeat(64)), ..Default::default() }
                .validate()
                .is_err(),
            "a 64-character label must be refused"
        );
        assert!(
            IsolationSpec { runtime_class: Some("a".repeat(63)), ..Default::default() }
                .validate()
                .is_ok(),
            "63 characters is the cap, not one under it"
        );
        for ok in ["gvisor", "kata-qemu-tdx", "kata.cc", "runsc2"] {
            assert!(
                IsolationSpec {
                    runtime_class: Some(ok.into()),
                    ..Default::default()
                }
                .validate()
                .is_ok(),
                "'{ok}' should be accepted"
            );
        }
    }

    #[test]
    fn an_envelope_the_executor_cannot_deliver_is_refused_by_name() {
        let spec = IsolationSpec {
            runtime_class: Some("gvisor".into()),
            drop_all_capabilities: Some(true),
            ..Default::default()
        };
        assert!(spec.require_enforceable_by(ExecutorKind::Kubernetes).is_ok());

        let err = spec.require_enforceable_by(ExecutorKind::Local).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("isolation.runtime_class"), "{msg}");
        assert!(msg.contains("isolation.drop_all_capabilities"), "{msg}");

        // Fields set to their *weak* value ask for no enforcement, so they are
        // deliverable anywhere — otherwise `seccomp: unconfined` would make a
        // spec unrunnable on the executor that confines nothing.
        let weak = IsolationSpec {
            seccomp: Some(Seccomp::Unconfined),
            read_only_root_fs: Some(false),
            service_account_token: Some(true),
            ..Default::default()
        };
        assert!(weak.require_enforceable_by(ExecutorKind::Local).is_ok());
        assert!(IsolationSpec::default().require_enforceable_by(ExecutorKind::Local).is_ok());
    }

    /// Each executor is refused what it has no way to deliver, and nothing
    /// else. A container takes all of the envelope but a Kubernetes runtime
    /// class; a pod takes all of it but `network: none`, which no field of a
    /// pod gives; a subprocess takes none of it.
    #[test]
    fn each_executor_is_refused_only_what_it_cannot_deliver() {
        let unmet = |spec: &IsolationSpec, kind: ExecutorKind| spec.enforceable_by(kind);
        let unmet_by = |spec: &IsolationSpec, on: Enforcer| spec.enforceable_by(&on);

        // The preset an operator is pointed at for untrusted work holds on
        // Docker and on Kubernetes, and on neither with a subprocess.
        let hardened = IsolationSpec::hardened();
        assert!(unmet(&hardened, ExecutorKind::Docker).is_empty());
        assert!(unmet(&hardened, ExecutorKind::Kubernetes).is_empty());
        assert_eq!(
            unmet(&hardened, ExecutorKind::Local),
            [
                "seccomp",
                "read_only_root_fs",
                "no_new_privileges",
                "drop_all_capabilities",
                "run_as_non_root",
                "service_account_token"
            ]
        );

        let no_network = IsolationSpec {
            network: Some(Network::None),
            run_as_user: Some(1000),
            ..Default::default()
        };
        assert!(unmet(&no_network, ExecutorKind::Docker).is_empty());
        assert_eq!(unmet(&no_network, ExecutorKind::Kubernetes), ["network"]);
        assert_eq!(
            unmet(&no_network, ExecutorKind::Local),
            ["run_as_user", "network"]
        );
        let msg = no_network
            .require_enforceable_by(ExecutorKind::Kubernetes)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("the k8s executor cannot enforce isolation.network"),
            "{msg}"
        );
        assert!(
            msg.contains("the Docker executor every field but runtime_class"),
            "{msg}"
        );
        assert!(msg.contains("DAGRON_TASK_NETWORK_POLICY=enforced"), "{msg}");
        // A cluster whose operator has said it enforces NetworkPolicy takes
        // the field. What they said of a cluster is nothing to a subprocess.
        let told = |kind| Enforcer {
            kind,
            network_policy: true,
            allowlists: Vec::new(),
        };
        assert!(unmet_by(&no_network, told(ExecutorKind::Kubernetes)).is_empty());
        assert_eq!(
            unmet_by(&no_network, told(ExecutorKind::Local)),
            ["run_as_user", "network"]
        );

        let sandboxed = IsolationSpec {
            runtime_class: Some("gvisor".into()),
            ..Default::default()
        };
        assert_eq!(unmet(&sandboxed, ExecutorKind::Docker), ["runtime_class"]);
        assert!(unmet(&sandboxed, ExecutorKind::Kubernetes).is_empty());

        // The weak value asks for nothing, so it is deliverable anywhere.
        let open = IsolationSpec {
            network: Some(Network::Default),
            ..Default::default()
        };
        for kind in [
            ExecutorKind::Local,
            ExecutorKind::Docker,
            ExecutorKind::Kubernetes,
        ] {
            assert!(unmet(&open, kind).is_empty(), "{kind:?}");
        }
    }

    /// `network` is read from a spec and from a floor, raised by a floor of
    /// `none` and by no other, and attested under its own key.
    #[test]
    fn network_is_declared_floored_and_attested() {
        let spec: IsolationSpec = serde_yaml::from_str("network: none").unwrap();
        assert_eq!(spec.network, Some(Network::None));
        assert_eq!(spec.canonical(), "network=none");
        assert!(serde_yaml::from_str::<IsolationSpec>("network: host").is_err());

        let floor = IsolationSpec::parse_floor("network=none,read_only_root_fs=true").unwrap();
        assert_eq!(floor.network, Some(Network::None));
        assert!(IsolationSpec::parse_floor("network=host")
            .unwrap_err()
            .to_string()
            .contains("expects none|default|allow:<name>"));

        // A task that said nothing, and one that asked for the network, are
        // both given none, and told so.
        for asked in [None, Some(Network::Default)] {
            let (out, notes) = IsolationSpec {
                network: asked.clone(),
                ..Default::default()
            }
            .apply_floor(&floor);
            assert_eq!(out.network, Some(Network::None));
            let note = notes
                .iter()
                .find(|n| n.field == "network")
                .expect("the floor says what it did");
            assert_eq!(note.applied, "none");
            assert_eq!(
                note.requested,
                if asked.is_some() { "default" } else { "unset" }
            );
        }
        // One that asked for none already is left as it is, with nothing to say.
        let (out, notes) = IsolationSpec {
            network: Some(Network::None),
            ..Default::default()
        }
        .apply_floor(&floor);
        assert_eq!(out.network, Some(Network::None));
        assert!(notes.iter().all(|n| n.field != "network"));

        // A floor of the weak value constrains nothing.
        let open = IsolationSpec::parse_floor("network=default").unwrap();
        let (out, notes) = IsolationSpec::default().apply_floor(&open);
        assert_eq!(out.network, None);
        assert!(notes.is_empty());
        assert_eq!(
            IsolationSpec::hardened().network,
            None,
            "the preset leaves the network alone"
        );
    }

    /// An allowlist is named in a spec and in a floor with one spelling, and
    /// attested with it. A name is a DNS label and nothing longer than a
    /// pod's label can hold.
    #[test]
    fn the_operator_s_allowlists_are_names_each_once() {
        assert_eq!(parse_allowlists("").unwrap(), Vec::<String>::new());
        assert_eq!(
            parse_allowlists(" registry, forge ,registry,").unwrap(),
            ["registry", "forge"]
        );
        assert!(parse_allowlists("registry,Forge").is_err());
        assert_eq!(allowlist_object_name("registry"), "dagron-task-allow-registry");
    }

    #[test]
    fn an_allowlist_s_proxy_is_a_scheme_a_host_and_a_port() {
        for ok in [
            "http://egress:3128",
            "https://proxy.corp.example:8443/",
            "http://10.0.0.7:3128",
            "http://[fd00::7]:3128",
        ] {
            assert!(parse_allowlist_proxy(ok).is_ok(), "{ok}");
        }
        assert_eq!(
            parse_allowlist_proxy(" https://proxy:8443/ ").unwrap(),
            "https://proxy:8443"
        );
        for bad in [
            "egress:3128",
            "socks5://egress:1080",
            "http://egress",
            "http://egress:0",
            "http://egress:3128/path",
            "http://egress:port",
            "http://:3128",
            "http://[]:3128",
            "http://[a/b]:3128",
            "http://[x y]:3128",
            "http://[fd00::7,b]:3128",
        ] {
            assert!(parse_allowlist_proxy(bad).is_err(), "{bad}");
        }
        let user = parse_allowlist_proxy("http://me:secret@egress:3128").unwrap_err();
        assert!(user.contains("carries a user"), "{user}");
    }

    #[test]
    fn a_proxy_is_told_under_every_name_and_spares_the_task_s_own() {
        let env: BTreeMap<String, String> =
            proxy_env("http://egress:3128", ["db", "cache"]).into_iter().collect();
        for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
            assert_eq!(env[name], "http://egress:3128", "{name}");
        }
        for name in ["NO_PROXY", "no_proxy"] {
            assert_eq!(env[name], "localhost,127.0.0.1,::1,db,cache", "{name}");
        }
    }

    #[test]
    fn an_allowlist_is_named_with_one_spelling() {
        let spec: IsolationSpec = serde_yaml::from_str("network: allow:registry").unwrap();
        assert_eq!(spec.network, Some(Network::Allow("registry".into())));
        assert_eq!(spec.canonical(), "network=allow:registry");
        assert!(spec.validate().is_ok());
        // What is written back is what was read, in YAML and in JSON.
        let yaml = serde_yaml::to_string(&spec).unwrap();
        assert_eq!(serde_yaml::from_str::<IsolationSpec>(&yaml).unwrap(), spec, "{yaml}");
        let json = serde_json::to_string(&spec).unwrap();
        assert_eq!(json, r#"{"network":"allow:registry"}"#);
        assert_eq!(serde_json::from_str::<IsolationSpec>(&json).unwrap(), spec);
        let floor = IsolationSpec::parse_floor("network=allow:registry").unwrap();
        assert_eq!(floor.network, spec.network);

        let longest = "a".repeat(ALLOWLIST_NAME_MAX);
        assert!(Network::parse(&format!("allow:{longest}")).is_ok());
        for wrong in [
            "allow:",
            "allow:Registry",
            "allow:-a",
            "allow:a-",
            "allow:a b",
            "allow:a.b",
            "allow:a:b",
            "allowlist",
            "Allow:registry",
        ] {
            let why = Network::parse(wrong).unwrap_err();
            assert!(why.contains("allow"), "{wrong}: {why}");
            assert!(
                serde_yaml::from_str::<IsolationSpec>(&format!("network: \"{wrong}\"")).is_err(),
                "{wrong}"
            );
            assert!(IsolationSpec::parse_floor(&format!("network={wrong}")).is_err(), "{wrong}");
        }
        assert!(Network::parse(&format!("allow:{longest}a")).is_err(), "one too long");
        // One built in code is caught where a spec is checked.
        let built = IsolationSpec {
            network: Some(Network::Allow("Not A Name".into())),
            ..Default::default()
        };
        assert!(built.validate().unwrap_err().to_string().contains("isolation.network"));
    }

    /// A floor of `none` is over an allowlist, an allowlist is over the
    /// executor's own network, and a floor that names an allowlist pins it:
    /// a task that named another gets the floor's, and is told.
    #[test]
    fn a_floor_that_names_an_allowlist_pins_it() {
        let allow = |name: &str| Some(Network::Allow(name.into()));
        let with = |network: Option<Network>| IsolationSpec {
            network,
            ..Default::default()
        };
        let floor = with(allow("registry"));
        // (what the task asked, what it runs under, what it is told it asked)
        for (asked, runs, told) in [
            (None, allow("registry"), Some("unset")),
            (Some(Network::Default), allow("registry"), Some("default")),
            (allow("forge"), allow("registry"), Some("allow:forge")),
            (allow("registry"), allow("registry"), None),
            (Some(Network::None), Some(Network::None), None),
        ] {
            let (out, notes) = with(asked.clone()).apply_floor(&floor);
            assert_eq!(out.network, runs, "{asked:?}");
            let note = notes.iter().find(|n| n.field == "network");
            assert_eq!(note.map(|n| n.requested.as_str()), told, "{asked:?}");
            if let Some(note) = note {
                assert_eq!(note.applied, "allow:registry");
            }
        }
        // A floor of none is over any allowlist a task names.
        let (out, notes) = with(allow("registry")).apply_floor(&with(Some(Network::None)));
        assert_eq!(out.network, Some(Network::None));
        assert_eq!(notes[0].requested, "allow:registry");
    }

    /// An allowlist is deliverable on Kubernetes where the operator has said
    /// the cluster enforces a policy and has named that allowlist, and
    /// nowhere else: not one they did not name, not on their word about
    /// another, and not by a daemon or a subprocess.
    #[test]
    fn an_allowlist_is_held_only_where_the_operator_named_it() {
        let asks = IsolationSpec {
            network: Some(Network::Allow("registry".into())),
            ..Default::default()
        };
        let on = |kind, network_policy, names: &[&str]| Enforcer {
            kind,
            network_policy,
            allowlists: names.iter().map(|n| n.to_string()).collect(),
        };
        let k8s = ExecutorKind::Kubernetes;
        assert!(asks.enforceable_by(&on(k8s, true, &["forge", "registry"])).is_empty());
        assert_eq!(asks.enforceable_by(&on(k8s, true, &["forge"])), ["network"]);
        assert_eq!(asks.enforceable_by(&on(k8s, true, &[])), ["network"]);
        assert_eq!(
            asks.enforceable_by(&on(k8s, false, &["registry"])),
            ["network"],
            "a name with no word that anything enforces it"
        );
        // On Docker an allowlist is a network of the operator's, held where
        // they have named it, and with no word of a CNI: a daemon keeps a
        // container to its network itself.
        let docker = ExecutorKind::Docker;
        assert!(asks.enforceable_by(&on(docker, false, &["registry"])).is_empty());
        assert_eq!(asks.enforceable_by(&on(docker, false, &["forge"])), ["network"]);
        assert_eq!(asks.enforceable_by(docker), ["network"]);
        assert_eq!(
            asks.enforceable_by(&on(ExecutorKind::Local, true, &["registry"])),
            ["network"]
        );
        let msg = asks.require_enforceable_by(k8s).unwrap_err().to_string();
        assert!(msg.contains("DAGRON_TASK_NETWORK_ALLOWLISTS"), "{msg}");
        // What the operator named changes nothing for a task that asked for none.
        let none = IsolationSpec {
            network: Some(Network::None),
            ..Default::default()
        };
        assert!(none.enforceable_by(&on(k8s, true, &[])).is_empty());
    }

    #[test]
    fn canonical_distinguishes_unset_from_the_weak_value() {
        let unset = IsolationSpec::default().canonical();
        let weak = IsolationSpec {
            read_only_root_fs: Some(false),
            ..Default::default()
        }
        .canonical();
        assert_ne!(
            unset, weak,
            "'nobody said' and 'explicitly not required' are different claims to attest"
        );
        assert_eq!(unset, "");
        assert_eq!(weak, "read_only_root_fs=false");
    }

    #[test]
    // The out-of-order field assignment below is the point of the test, so
    // clippy's "build it in one initializer" suggestion would delete what is
    // being checked.
    #[allow(clippy::field_reassign_with_default)]
    fn canonical_is_field_order_independent() {
        let a = IsolationSpec {
            seccomp: Some(Seccomp::RuntimeDefault),
            read_only_root_fs: Some(true),
            run_as_user: Some(65534),
            ..Default::default()
        };
        let mut b = IsolationSpec::default();
        b.run_as_user = Some(65534);
        b.read_only_root_fs = Some(true);
        b.seccomp = Some(Seccomp::RuntimeDefault);
        assert_eq!(a.canonical(), b.canonical());
    }

    #[test]
    fn a_floor_parses_from_its_env_spelling_and_round_trips() {
        let floor = IsolationSpec::parse_floor(
            "seccomp=runtime_default,drop_all_capabilities=true,read_only_root_fs=true,\
             no_new_privileges=true,run_as_non_root=true,service_account_token=false",
        )
        .unwrap();
        assert_eq!(floor, IsolationSpec::hardened());

        assert_eq!(IsolationSpec::parse_floor("hardened").unwrap(), IsolationSpec::hardened());
        assert_eq!(IsolationSpec::parse_floor("  ").unwrap(), IsolationSpec::default());

        // The canonical rendering is itself a valid floor spelling.
        let reparsed = IsolationSpec::parse_floor(&floor.canonical()).unwrap();
        assert_eq!(reparsed, floor);
    }

    #[test]
    fn a_typo_in_the_floor_fails_loudly_rather_than_being_ignored() {
        for bad in [
            "drop_all_capabilties=true", // transposed
            "read_only_root_fs=yes-please",
            "seccomp=strict",
            "run_as_user=nobody",
            "drop_all_capabilities",
            "run_as_user=0",
        ] {
            assert!(
                IsolationSpec::parse_floor(bad).is_err(),
                "'{bad}' must not be silently ignored"
            );
        }
    }

    #[test]
    fn executor_kind_parses_the_spellings_the_engine_accepts() {
        assert_eq!(ExecutorKind::parse("k8s"), Some(ExecutorKind::Kubernetes));
        assert_eq!(ExecutorKind::parse("Kubernetes"), Some(ExecutorKind::Kubernetes));
        assert_eq!(ExecutorKind::parse(" local "), Some(ExecutorKind::Local));
        assert_eq!(ExecutorKind::parse("docker"), Some(ExecutorKind::Docker));
        assert_eq!(ExecutorKind::parse("nomad"), None);
    }
}
