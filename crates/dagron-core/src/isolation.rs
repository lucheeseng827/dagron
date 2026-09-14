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
//! confidential runtime class — and on the Local and Docker executors most of
//! these fields have no enforcement path at all. [`IsolationSpec::enforceable_by`]
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

/// Executors an envelope can be asked to enforce. Named here rather than
/// imported so `dagron-core` keeps no dependency on the executor crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorKind {
    Local,
    Docker,
    Kubernetes,
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
    pub fn enforceable_by(&self, executor: ExecutorKind) -> Vec<&'static str> {
        let mut unmet = Vec::new();
        match executor {
            ExecutorKind::Kubernetes => {}
            // Both the Local (subprocess) and Docker executors run the task in
            // the engine's own trust domain today; neither shapes a security
            // context. Listing the fields individually — rather than refusing
            // any `isolation:` block outright — keeps the door open for a
            // Docker path that maps the subset it *can* honour
            // (`--read-only`, `--cap-drop=ALL`, `--security-opt`) without this
            // function having to change shape.
            ExecutorKind::Local | ExecutorKind::Docker => {
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
            }
        }
        unmet
    }

    /// Refuse an envelope `executor` cannot deliver, naming every unmet field.
    pub fn require_enforceable_by(&self, executor: ExecutorKind) -> Result<()> {
        let unmet = self.enforceable_by(executor);
        if !unmet.is_empty() {
            bail!(
                "the {} executor cannot enforce isolation.{} — a task must not run believing it \
                 is sandboxed when it is not; drop the field, or dispatch this task to a \
                 runner_class backed by the Kubernetes executor",
                executor.label(),
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
