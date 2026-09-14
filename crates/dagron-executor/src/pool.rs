//! What an `EXECUTOR=local` pool permits a task to do.
//!
//! A local pool runs its tasks as **subprocesses of the engine**. That is the
//! whole reason it exists — a `pulse` status check must not pay a pod cold
//! start, and a `build` task needs the daemon socket the pool already holds —
//! and it is also the whole problem: `Command` inherits the parent environment,
//! and `command:` comes from the workflow. So on a local pool a spec author can
//! run any program the pool's user can run, and read every variable the pool
//! process holds.
//!
//! That was measured rather than reasoned about. Against the pool's own
//! `run_command`, with the environment `ee/compose.image-build.yaml` gives the
//! build pool:
//!
//! ```text
//! command: ["sh", "-c", "echo $DAGRON_BUILD_ATTEST_KEY $DAGRON_REGISTRY_PASSWORD"]
//!   → 1111…1111 the-push-token          (the image-signing key and the push token)
//! command: ["sh", "-c", "echo $DAGRON_BUILD_BACKEND"], env: [DAGRON_BUILD_BACKEND=kubernetes]
//!   → kubernetes                        (a task overrides a deployment knob)
//! command: ["id"]
//!   → uid=0(root)                       (as the pool's user, which holds the socket)
//! ```
//!
//! and `DAGRON_REDACT_ENV` does not help: it masks values in *stored output*, so
//! `printf %s "$TOKEN" | base64` walks straight past it. Redaction is about
//! accidents in logs, not about a task that wants the value.
//!
//! Two knobs close that, and they are **opt-in**: unset, this module changes
//! nothing at all, and every deployment behaves exactly as it did.
//!
//! * `DAGRON_LOCAL_COMMAND_ALLOWLIST` — the programs a task of this pool may
//!   run. A pool is an engine process with `RUNNER_CLASSES` set, so a
//!   process-wide setting *is* per-pool.
//! * `DAGRON_LOCAL_ENV_PASSTHROUGH` — which of the pool's own variables reach a
//!   task. Everything else is cleared.
//!
//! ## Why the allowlist matches resolved paths, not `command[0]`
//!
//! A string comparison against `command[0]` is defeated three ways, each
//! measured against this crate:
//!
//! ```text
//! command: ["dagron-build"],           env: [PATH=/tmp/mine:…]  → /tmp/mine/dagron-build
//! command: ["/tmp/mine/dagron-build"]                           → the same file
//! command: ["/tmp/mine/./dagron-build"]                         → the same file
//! ```
//!
//! So the check resolves `command[0]` exactly as the OS would — a `/` in it
//! makes it a path, otherwise it is searched on `PATH` — canonicalises the
//! result, and compares canonical paths. Then it **spawns the canonical path**
//! rather than the original word, which removes the second resolution
//! altogether: there is no window between the check and the exec for a `PATH`
//! to change under it, because the exec does no lookup.
//!
//! What an allowlist does **not** buy, and must not be sold as: it bounds
//! *which program* runs, not what that program does. `dagron-build` builds an
//! image from a recipe, and a recipe installs packages, which runs those
//! packages' code. The allowlist keeps a task from reaching for `sh`; the
//! builder's own refusals (`ee/dagron-build`'s recipe validation, the base
//! allow-list) are what bound the build.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{bail, Result};
use dagron_core::dag::EnvVar;

/// The programs a task of this pool may run: comma-separated, each either an
/// absolute path or a bare name resolved on the pool's `PATH`. Unset = no
/// allowlist, which is what every deployment before this had.
pub const ENV_COMMAND_ALLOWLIST: &str = "DAGRON_LOCAL_COMMAND_ALLOWLIST";

/// Pool environment variables a task may see, beyond [`BASELINE_ENV`]:
/// comma-separated names. Unset = no isolation, and the task inherits the
/// pool's whole environment as it always has.
pub const ENV_PASSTHROUGH: &str = "DAGRON_LOCAL_ENV_PASSTHROUGH";

/// Passed through whenever isolation is on, without the operator listing them.
///
/// Not a convenience: `env_clear()` without these produces a task that fails
/// with "No such file or directory" (no `PATH`), or one that cannot verify a
/// TLS certificate, and an operator debugging that will turn isolation off. The
/// set is deliberately small and contains nothing a deployment would put a
/// credential in.
pub const BASELINE_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TZ",
    "TERM",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    // TLS trust and proxying: a build reaches a registry, and an air-gapped
    // site reaches it through a proxy with a private CA.
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
];

/// Does `name` steer the dynamic loader?
///
/// A **prefix** rule rather than a list of names. `LD_` is glibc's namespace and
/// `DYLD_` is dyld's, and both hold more entries than the famous two —
/// `LD_AUDIT` loads a library the same way `LD_PRELOAD` does, `LD_ORIGIN_PATH`
/// and `LD_LIBRARY_PATH` move where one is found. Enumerating them dates the
/// moment libc adds another; refusing the namespace does not.
///
/// Nothing in [`BASELINE_ENV`] starts with either, so this cannot collide with
/// what isolation deliberately passes through.
fn is_loader_env(name: &str) -> bool {
    name.starts_with("LD_") || name.starts_with("DYLD_")
}

/// What this pool permits. Cheap to clone; built once from the environment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolPolicy {
    /// Canonical absolute paths of the permitted programs. `None` = no
    /// allowlist. `Some(empty)` = an allowlist was configured and nothing in it
    /// resolved, which refuses every task — the safe direction for a
    /// misconfiguration, and loud rather than silent.
    allow: Option<Vec<PathBuf>>,
    /// Pool variables the operator named. `None` = isolation off.
    passthrough: Option<BTreeSet<String>>,
}

/// The process-wide policy, read once.
pub fn policy() -> &'static PoolPolicy {
    static P: OnceLock<PoolPolicy> = OnceLock::new();
    P.get_or_init(|| {
        let p = PoolPolicy::parse(
            std::env::var(ENV_COMMAND_ALLOWLIST).ok(),
            std::env::var(ENV_PASSTHROUGH).ok(),
            |name| resolve_on_path(name, std::env::var("PATH").ok().as_deref()),
        );
        if let Some(allow) = &p.allow {
            if allow.is_empty() {
                tracing::error!(
                    "{ENV_COMMAND_ALLOWLIST} is set but nothing in it resolved to an \
                     executable; every task on this pool will be refused"
                );
            } else {
                tracing::info!(
                    programs = allow.len(),
                    "local pool command allowlist in force"
                );
            }
        }
        if let Some(names) = &p.passthrough {
            tracing::info!(
                named = names.len(),
                baseline = BASELINE_ENV.len(),
                "local pool environment isolation in force"
            );
        }
        p
    })
}

impl PoolPolicy {
    /// Build from the two raw settings. `resolve` turns a bare program name
    /// into a path (injected so the whole policy is testable without a `PATH`).
    pub fn parse(
        allowlist: Option<String>,
        passthrough: Option<String>,
        resolve: impl Fn(&str) -> Option<PathBuf>,
    ) -> Self {
        let allow = allowlist
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .filter_map(|entry| {
                        let candidate = if entry.contains('/') {
                            Some(PathBuf::from(entry))
                        } else {
                            resolve(entry)
                        };
                        match candidate.as_deref().map(std::fs::canonicalize) {
                            Some(Ok(p)) => Some(p),
                            _ => {
                                tracing::warn!(
                                    entry,
                                    "{ENV_COMMAND_ALLOWLIST} names a program that does not \
                                     resolve to a file here; it permits nothing"
                                );
                                None
                            }
                        }
                    })
                    .collect()
            });

        let passthrough = passthrough
            .as_deref()
            .map(str::trim)
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            });

        Self { allow, passthrough }
    }

    /// Whether an allowlist is in force.
    pub fn restricts_commands(&self) -> bool {
        self.allow.is_some()
    }

    /// Whether environment isolation is in force.
    pub fn isolates_env(&self) -> bool {
        self.passthrough.is_some()
    }

    /// The program to spawn for `argv0`, or a refusal.
    ///
    /// With no allowlist this is `argv0` verbatim — byte-identical to what the
    /// executor did before. With one, it is the **canonical path**, so the
    /// spawn does no lookup of its own and cannot land somewhere else than the
    /// path that was checked.
    pub fn program(&self, argv0: &str, resolve: impl Fn(&str) -> Option<PathBuf>) -> Result<String> {
        let Some(allow) = &self.allow else {
            return Ok(argv0.to_string());
        };
        let candidate = if argv0.contains('/') {
            PathBuf::from(argv0)
        } else {
            resolve(argv0).unwrap_or_else(|| PathBuf::from(argv0))
        };
        let canonical = std::fs::canonicalize(&candidate).unwrap_or(candidate);
        if allow.iter().any(|a| *a == canonical) {
            return Ok(canonical.to_string_lossy().into_owned());
        }
        bail!(
            "this pool permits only {}; '{argv0}' resolves to {} and is refused \
             ({ENV_COMMAND_ALLOWLIST})",
            allow
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(", "),
            canonical.display()
        )
    }

    /// The environment the child gets, or `None` to inherit the pool's (the
    /// unisolated default). `read` resolves a pool variable by name.
    ///
    /// A task variable whose name the operator listed is a **refusal**, not an
    /// override that quietly loses: a deployment knob a task can set is not a
    /// deployment knob, and running the task under a configuration its author
    /// did not write is worse than not running it. `PATH` is protected the same
    /// way — the operator decided what this pool runs, and a task rewriting
    /// where things are found undoes that.
    pub fn child_env(
        &self,
        task_env: &[EnvVar],
        read: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Vec<(String, String)>>> {
        let Some(named) = &self.passthrough else {
            return Ok(None);
        };

        for e in task_env {
            if named.contains(&e.name) || e.name == "PATH" {
                bail!(
                    "this task sets '{}', which this pool owns and a task may not \
                     replace ({ENV_PASSTHROUGH})",
                    e.name
                );
            }
            // The allowlist decides WHICH program runs; a loader variable
            // decides what runs INSIDE it. `LD_PRELOAD=/workspace/x.so` gets
            // that library's constructors executed by ld.so before the
            // allowlisted program reaches `main`, in its process, holding
            // whatever it holds -- the pool's socket, a build credential. So an
            // allowlist that does not refuse these is not an allowlist; it is a
            // suggestion about argv[0].
            //
            // Refused rather than dropped: a task that asked for a preload and
            // silently did not get one is a debugging session, and refusing is
            // a `FaultClass::Config` the engine never retries.
            if is_loader_env(&e.name) {
                bail!(
                    "this task sets '{}', a dynamic-loader variable. It would run code of the \
                     task's choosing inside the allowlisted program, which is the boundary \
                     {ENV_COMMAND_ALLOWLIST} exists to draw. Bake what you need into the \
                     image, or name the program itself in the allowlist.",
                    e.name
                );
            }
        }

        // Deterministic order, and one value per name: the operator's list is
        // layered over the baseline, then the task's own env last.
        let mut out: BTreeMap<String, String> = BTreeMap::new();
        for name in BASELINE_ENV.iter().map(|s| s.to_string()).chain(named.iter().cloned()) {
            if let Some(v) = read(&name) {
                out.insert(name, v);
            }
        }
        for e in task_env {
            out.insert(e.name.clone(), e.value.clone());
        }
        Ok(Some(out.into_iter().collect()))
    }
}

/// Find `name` on `path`, the way `execvp` would: the first entry that holds a
/// file with an execute bit. `None` when it is not there.
pub fn resolve_on_path(name: &str, path: Option<&str>) -> Option<PathBuf> {
    let path = path?;
    path.split(':')
        .filter(|d| !d.is_empty())
        .map(|d| Path::new(d).join(name))
        .find(|c| is_executable(c))
}

#[cfg(unix)]
fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(p: &Path) -> bool {
    p.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(name: &str, value: &str) -> EnvVar {
        EnvVar { name: name.into(), value: value.into(), value_from: None }
    }

    /// A directory holding one real executable, plus a decoy of the same name
    /// somewhere else — the shape every allowlist bypass takes.
    struct Fixture {
        real: PathBuf,
        decoy: PathBuf,
        _dir: PathBuf,
    }

    fn fixture(tag: &str) -> Fixture {
        let root = std::env::temp_dir().join(format!("dagron-pool-{tag}-{}", std::process::id()));
        let bin = root.join("bin");
        let evil = root.join("evil");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&evil).unwrap();
        let real = bin.join("dagron-build");
        let decoy = evil.join("dagron-build");
        for (p, body) in [(&real, "#!/bin/sh\necho real\n"), (&decoy, "#!/bin/sh\necho decoy\n")] {
            std::fs::write(p, body).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
        Fixture { real, decoy, _dir: root }
    }

    /// Unset is the whole compatibility story: a policy built from nothing must
    /// change nothing, for either knob.
    #[test]
    fn unset_permits_everything_and_isolates_nothing() {
        for empty in [None, Some(String::new()), Some("   ".to_string())] {
            let p = PoolPolicy::parse(empty.clone(), None, |_| None);
            assert!(!p.restricts_commands(), "{empty:?} must not restrict commands");
            assert_eq!(p.program("sh", |_| None).unwrap(), "sh", "spawned verbatim");
        }
        let p = PoolPolicy::parse(None, None, |_| None);
        assert!(!p.isolates_env());
        assert_eq!(
            p.child_env(&[ev("DAGRON_BUILD_BACKEND", "kubernetes")], |_| None).unwrap(),
            None,
            "no isolation means inherit, as it always did"
        );
    }

    /// The three spellings that defeat a string comparison against `command[0]`.
    /// Each was measured reaching a planted file before this existed.
    #[test]
    fn the_allowlist_matches_the_file_not_the_word() {
        let f = fixture("spellings");
        let path = format!("{}", f.decoy.parent().unwrap().display());
        // The allowlist names the real binary by absolute path.
        let p = PoolPolicy::parse(
            Some(f.real.display().to_string()),
            None,
            |_| None,
        );

        // The permitted file, however it is spelled.
        for spelling in [
            f.real.display().to_string(),
            format!("{}/./dagron-build", f.real.parent().unwrap().display()),
            format!("{}/../bin/dagron-build", f.real.parent().unwrap().display()),
        ] {
            let got = p.program(&spelling, |_| None).unwrap_or_else(|e| panic!("{spelling}: {e}"));
            assert_eq!(
                std::fs::canonicalize(&got).unwrap(),
                std::fs::canonicalize(&f.real).unwrap(),
                "{spelling} names the permitted file"
            );
        }

        // A decoy of the same NAME, reached three ways. Each of these got
        // through a comparison on the word "dagron-build".
        let err = p.program("dagron-build", |n| resolve_on_path(n, Some(&path))).unwrap_err();
        assert!(err.to_string().contains("refused"), "PATH plant: {err}");
        let err = p.program(&f.decoy.display().to_string(), |_| None).unwrap_err();
        assert!(err.to_string().contains("refused"), "absolute decoy: {err}");
        let err = p
            .program(&format!("{}/./dagron-build", f.decoy.parent().unwrap().display()), |_| None)
            .unwrap_err();
        assert!(err.to_string().contains("refused"), "dot-path decoy: {err}");
    }

    /// What is spawned is the canonical path, not the word the task wrote. That
    /// is what removes the second resolution: there is no window between the
    /// check and the exec for a PATH to change under it.
    #[test]
    fn a_permitted_command_is_spawned_by_its_canonical_path() {
        let f = fixture("canonical");
        let bin = f.real.parent().unwrap().display().to_string();
        // The allowlist entry is a bare name, resolved on the pool's PATH.
        let p = PoolPolicy::parse(
            Some("dagron-build".into()),
            None,
            |n| resolve_on_path(n, Some(&bin)),
        );
        let program = p.program("dagron-build", |n| resolve_on_path(n, Some(&bin))).unwrap();
        assert!(
            program.starts_with('/'),
            "an allowlisted spawn is by absolute path, got {program:?}"
        );
        assert_eq!(
            std::fs::canonicalize(&program).unwrap(),
            std::fs::canonicalize(&f.real).unwrap()
        );
    }

    /// A symlink is the same file. An allowlist that missed this would be
    /// defeated by `ln -s`, which is not a sophisticated attack.
    #[test]
    fn a_symlink_to_a_permitted_program_is_the_same_program() {
        let f = fixture("symlink");
        let link = f.real.parent().unwrap().join("build-link");
        let _ = std::fs::remove_file(&link);
        #[cfg(unix)]
        std::os::unix::fs::symlink(&f.real, &link).unwrap();
        let p = PoolPolicy::parse(Some(f.real.display().to_string()), None, |_| None);
        #[cfg(unix)]
        assert!(
            p.program(&link.display().to_string(), |_| None).is_ok(),
            "a symlink to the permitted file resolves to it"
        );

        // And the reverse: naming the LINK in the allowlist permits the target,
        // because that is the file that runs. Worth pinning so nobody "fixes"
        // canonicalisation into a string compare later.
        let p = PoolPolicy::parse(Some(link.display().to_string()), None, |_| None);
        #[cfg(unix)]
        assert!(p.program(&f.real.display().to_string(), |_| None).is_ok());
    }

    /// An allowlist naming nothing that exists refuses everything. Fail closed:
    /// a typo in the operator's list must not silently mean "no allowlist".
    #[test]
    fn an_allowlist_that_resolves_to_nothing_refuses_everything() {
        let p = PoolPolicy::parse(Some("/nope/not-here,also-not-here".into()), None, |_| None);
        assert!(p.restricts_commands());
        let err = p.program("sh", |_| None).unwrap_err();
        assert!(err.to_string().contains("refused"), "{err}");
    }

    /// Isolation: the pool's own secrets stop at the boundary, and what the
    /// operator named comes through.
    #[test]
    fn isolation_passes_what_was_named_and_clears_the_rest() {
        let pool: BTreeMap<&str, &str> = [
            ("PATH", "/usr/bin"),
            ("HOME", "/root"),
            // The build pool's environment, from ee/compose.image-build.yaml.
            ("DATABASE_URL", "postgres://dagron:dagron@postgres:5432/workflow"),
            ("DAGRON_ENV_SECRET_KEY", "dev-insecure-env-secret-key"),
            ("DAGRON_BUILD_ATTEST_KEY", "1111111111111111111111111111111111111111111111111111111111111111"),
            ("DOCKER_HOST", "unix:///run/podman/podman.sock"),
            ("DAGRON_BUILD_TIMEOUT_SECS", "840"),
        ]
        .into_iter()
        .collect();
        let read = |n: &str| pool.get(n).map(|s| s.to_string());

        let p = PoolPolicy::parse(
            None,
            Some("DOCKER_HOST,DAGRON_BUILD_ATTEST_KEY,DAGRON_BUILD_TIMEOUT_SECS".into()),
            |_| None,
        );
        assert!(p.isolates_env());
        let got: BTreeMap<String, String> =
            p.child_env(&[ev("DAGRON_BUILD_RECIPE", "name: x")], read).unwrap().unwrap().into_iter().collect();

        // The named deployment knobs, and the baseline.
        assert_eq!(got.get("DOCKER_HOST").map(String::as_str), Some("unix:///run/podman/podman.sock"));
        assert_eq!(got.get("DAGRON_BUILD_TIMEOUT_SECS").map(String::as_str), Some("840"));
        assert_eq!(got.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert_eq!(got.get("HOME").map(String::as_str), Some("/root"));
        // The task's own env.
        assert_eq!(got.get("DAGRON_BUILD_RECIPE").map(String::as_str), Some("name: x"));
        // Everything else is gone. These are the two that matter: the datastore
        // credential is a write to any run's state, and the env secret key
        // decrypts every stored task secret.
        assert!(!got.contains_key("DATABASE_URL"), "the datastore credential must not cross");
        assert!(!got.contains_key("DAGRON_ENV_SECRET_KEY"), "the env secret key must not cross");
    }

    /// A named variable that the pool does not actually hold is simply absent —
    /// not an empty string, which a program would read as "configured, blank".
    #[test]
    fn a_named_variable_the_pool_does_not_have_is_absent_not_empty() {
        let p = PoolPolicy::parse(None, Some("NOT_SET_ANYWHERE".into()), |_| None);
        let got = p.child_env(&[], |_| None).unwrap().unwrap();
        assert!(got.iter().all(|(n, _)| n != "NOT_SET_ANYWHERE"), "{got:?}");
    }

    /// The override the isolation exists to stop, refused rather than silently
    /// losing — a task that ran under a configuration its author did not write
    /// is worse than a task that did not run.
    #[test]
    fn a_task_may_not_replace_a_variable_the_pool_owns() {
        let p = PoolPolicy::parse(None, Some("DAGRON_BUILD_ATTEST_KEY,DAGRON_REGISTRY_PASSWORD".into()), |_| None);
        for name in ["DAGRON_BUILD_ATTEST_KEY", "DAGRON_REGISTRY_PASSWORD", "PATH"] {
            let err = p
                .child_env(&[ev(name, "mine")], |_| Some("theirs".into()))
                .unwrap_err();
            assert!(
                err.to_string().contains(name),
                "setting {name} must be refused by name: {err}"
            );
        }
        // A name the operator did not claim is the task's to set.
        assert!(p.child_env(&[ev("DAGRON_BUILD_RECIPE", "name: x")], |_| None).is_ok());
        // And with isolation off, nothing is refused — the compatibility path.
        let off = PoolPolicy::parse(None, None, |_| None);
        assert!(off.child_env(&[ev("PATH", "/tmp")], |_| None).is_ok());
    }

    /// The allowlist says which program runs. A loader variable says what runs
    /// inside it, so a task that can set one has walked around the allowlist
    /// rather than through it.
    #[test]
    fn a_task_may_not_steer_the_dynamic_loader() {
        let p = PoolPolicy::parse(None, Some("DAGRON_BUILD_ATTEST_KEY".into()), |_| None);
        for name in [
            "LD_PRELOAD",
            "LD_LIBRARY_PATH",
            "LD_AUDIT",
            // Not in anyone's famous-five list, which is the point of matching
            // the namespace rather than enumerating it.
            "LD_ORIGIN_PATH",
            "DYLD_INSERT_LIBRARIES",
            "DYLD_LIBRARY_PATH",
        ] {
            let err = p
                .child_env(&[ev(name, "/workspace/evil.so")], |_| None)
                .unwrap_err();
            assert!(
                err.to_string().contains(name),
                "{name} must be refused: {err}"
            );
        }

        // Nothing the baseline passes through is caught by the prefix rule, and
        // an ordinary task variable that merely starts with an L is not either.
        for name in BASELINE_ENV {
            assert!(!is_loader_env(name), "{name} is in BASELINE_ENV");
        }
        assert!(p.child_env(&[ev("LANG", "C.UTF-8")], |_| None).is_ok());
        assert!(p.child_env(&[ev("LDAP_URL", "ldap://x")], |_| None).is_ok());

        // With isolation off this is the pre-existing compatibility path: the
        // task's env is layered onto the inherited one and nothing is refused,
        // which is also why the allowlist alone is not a boundary.
        let off = PoolPolicy::parse(None, None, |_| None);
        assert!(off.child_env(&[ev("LD_PRELOAD", "/x.so")], |_| None).is_ok());
    }

    /// PATH resolution is the OS's rule, not a guess: first match wins, a
    /// non-executable file is not a match, and an empty entry is skipped.
    #[test]
    fn path_resolution_follows_execvp() {
        let f = fixture("execvp");
        let bin = f.real.parent().unwrap().display().to_string();
        let evil = f.decoy.parent().unwrap().display().to_string();

        // First match wins, both directions.
        assert_eq!(
            resolve_on_path("dagron-build", Some(&format!("{bin}:{evil}"))),
            Some(f.real.clone())
        );
        assert_eq!(
            resolve_on_path("dagron-build", Some(&format!("{evil}:{bin}"))),
            Some(f.decoy.clone())
        );
        // Not on the path at all.
        assert_eq!(resolve_on_path("dagron-build", Some("/nowhere")), None);
        assert_eq!(resolve_on_path("dagron-build", None), None);
        // Empty entries are skipped rather than meaning "the current directory",
        // which is how a `PATH=":$PATH"` turns into a local-file hijack.
        assert_eq!(resolve_on_path("dagron-build", Some("::")), None);

        // A file without an execute bit is not a program.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let plain = f.real.parent().unwrap().join("not-executable");
            std::fs::write(&plain, "x").unwrap();
            std::fs::set_permissions(&plain, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert_eq!(resolve_on_path("not-executable", Some(&bin)), None);
        }
    }

    /// Whitespace and empty entries in either list are an operator's YAML, not
    /// an attack — tolerated rather than turned into a program named "".
    #[test]
    fn the_lists_tolerate_operator_whitespace() {
        let f = fixture("whitespace");
        let p = PoolPolicy::parse(
            Some(format!("  {} , , ", f.real.display())),
            Some(" DOCKER_HOST , ,DAGRON_BUILD_PUSH ".into()),
            |_| None,
        );
        assert!(p.program(&f.real.display().to_string(), |_| None).is_ok());
        let got: BTreeMap<String, String> = p
            .child_env(&[], |n| (n == "DOCKER_HOST" || n == "DAGRON_BUILD_PUSH").then(|| "v".into()))
            .unwrap()
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(got.get("DOCKER_HOST").map(String::as_str), Some("v"));
        assert_eq!(got.get("DAGRON_BUILD_PUSH").map(String::as_str), Some("v"));
        assert!(got.iter().all(|(n, _)| !n.is_empty()));
    }
}
