//! **Task workspace**: a directory a task starts in, empty at the start of
//! each attempt (or holding its checkout) and thrown away after it.
//!
//! A CI job is one container that runs several steps in order, and the steps
//! share files: one builds `target/`, the next tests it. The container's own
//! filesystem would hold them, but it is the image's copy-on-write layer: slow
//! to write gigabytes into, and on Kubernetes charged to the node's root disk
//! with nothing to bound it. A workspace is a volume made for the one attempt
//! instead, and the task's working directory:
//!
//! | Executor   | The workspace |
//! |------------|---------------|
//! | Kubernetes | an `emptyDir` at [`WORKSPACE_DIR`], with `size` as its `sizeLimit`; a generic ephemeral volume of that size once `size` reaches `DAGRON_WORKSPACE_PVC_FROM` |
//! | Docker     | a volume at [`WORKSPACE_DIR`], made for the attempt (by its init container, below, where it has one) and removed after it; `DAGRON_DOCKER_WORKDIR` instead, where the operator set one |
//! | Local      | a new temporary directory, removed after the attempt; `DAGRON_LOCAL_WORKDIR` instead, where the operator set one |
//!
//! `size` binds on Kubernetes only: a Docker volume and a local directory have
//! no limit to set.
//!
//! **dagron's tools.** Where the operator names an image holding
//! `dagron-ci-exec` (`DAGRON_CI_EXEC_IMAGE`), the Kubernetes and Docker
//! executors also start each workspace task with an init container from that
//! image, which installs the binary into a volume of its own, mounted
//! read-only at [`TOOLS_DIR`]. A step runner then exists in every image,
//! whatever the image is: the entrypoint pattern Tekton and Argo use. It is a
//! volume beside the workspace rather than a directory in it, so that a step
//! can neither see it in a checkout nor change it.
//!
//! **A checkout.** `checkout:` names a repository and a commit; the workspace
//! then starts with that commit checked out in it, fetched alone (depth 1)
//! before the task's command runs. On Kubernetes and Docker a container from
//! the operator's `DAGRON_CHECKOUT_IMAGE` (any image with `git` and `sh`)
//! fetches it; locally, `git` itself. A credential, when the operator's token
//! service gives one, is minted for that attempt alone, reaches the fetch and
//! nothing else (never the task), and is sent only over https.
//!
//! **Tools.** `tools: [node]` installs, with the step runner, the Node.js
//! a JavaScript action runs on: one build that runs in an image of any
//! libc, beside `dagron-ci-exec` at `/dagron/bin/node`. It is over 100 MB,
//! so a task that names it alone is given it.
//!
//! **Actions.** `actions:` names repositories a task's steps use as actions
//! (a GitHub workflow's `uses: owner/repo@v4`): each is fetched before the
//! task's command runs, as the checkout is and by the same image, into a
//! directory of its own under [`ACTIONS_DIR`], a volume beside the
//! workspace that the task reads and cannot change. A ref is fetched as it
//! stands when the attempt starts, as GitHub's runner fetches one. No
//! credential is sent with it, but for one marked `credential: true` (an
//! action of the repository's own owner's, in a trusted run): that one is
//! fetched with a credential the token service mints for its repository,
//! for that attempt alone, as the checkout's is, where it gives one.
//!
//! **Caches.** `cache:` names directories a task keeps between runs: a
//! package manager's downloads, a compiler's registry. Each is kept as one
//! compressed archive in the operator's cache store (`DAGRON_CACHE_URL`), under
//! a key the task computes from `key_files` in its workspace. The engine gives
//! the task signed URLs: to read the newest entries of its repository and ref
//! (and of `restore_from`, a pull request's base branch), and, where the run is
//! trusted, to write one more. `dagron-ci-cache`, installed with the step
//! runner, restores before the task's steps and saves after they succeed. A
//! run that is not trusted is given no URL to write with: it reads, and never
//! writes what a later run would read.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// Where the workspace is, in a task's container or pod.
pub const WORKSPACE_DIR: &str = "/workspace";

/// Where dagron's tools are, read-only, in a workspace task's container or pod
/// when the operator set `DAGRON_CI_EXEC_IMAGE`.
pub const TOOLS_DIR: &str = "/dagron/bin";

/// Where a task's actions are, read-only, in its container or pod: each in
/// the directory its [`ActionSpec::path`] names under this one. The
/// variable of the same meaning, which every executor sets for such a task.
pub const ACTIONS_DIR: &str = "/dagron/actions";
pub const ENV_ACTIONS_DIR: &str = "DAGRON_ACTIONS_DIR";

/// At most this many actions on a task.
pub const MAX_ACTIONS: usize = 32;

/// The tools a task may ask to be installed with the step runner: Node.js,
/// for a step that runs a JavaScript action.
pub const TOOLS: &[&str] = &["node"];

/// `workspace:` on a task. `{}` asks for a workspace of no particular size.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceSpec {
    /// How much the workspace may hold, as a Kubernetes quantity (`20Gi`,
    /// `500M`). Unset: as much as the node gives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// The commit the workspace starts with. Unset: it starts empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkout: Option<CheckoutSpec>,
    /// Directories kept between runs. Unset: the task starts cold.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cache: Vec<PathCacheSpec>,
    /// The actions its steps use, each fetched before it starts. Unset:
    /// none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionSpec>,
    /// Tools installed with the step runner, of [`TOOLS`]. Unset: the step
    /// runner alone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<String>,
}

/// `workspace.actions[]`: a repository of an action, and what of it, fetched
/// into [`ACTIONS_DIR`] before the task starts.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActionSpec {
    /// The repository, as a checkout's is written ([`check_repository`]).
    pub repository: String,
    /// A tag, a branch, or a commit's full id: fetched as it stands when
    /// the attempt starts.
    #[serde(rename = "ref")]
    pub git_ref: String,
    /// Where under [`ACTIONS_DIR`]: names of letters, digits, `.`, `_`, `-`
    /// and `%`, between `/`s.
    pub path: String,
    /// Fetched with a credential the token service mints for this
    /// repository, where it gives one; with none otherwise. The service
    /// says whether it does: dagron-ci's gives one for an action of the
    /// run's repository's owner's, in a trusted run.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub credential: bool,
}

impl ActionSpec {
    pub fn validate(&self) -> Result<()> {
        if let Err(e) = check_repository(&self.repository) {
            bail!("workspace.actions: the repository {e}");
        }
        if let Err(e) = check_ref(&self.git_ref) {
            bail!("workspace.actions {:?}: {e}", self.repository);
        }
        let path = &self.path;
        let named = |part: &str| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '%'))
        };
        if path.is_empty() || path.len() > 512 || !path.split('/').all(named) {
            bail!(
                "workspace.actions {:?}: the path {path:?} is not one under the actions' \
                 directory: names of letters, digits, '.', '_', '-' and '%', between '/'s",
                self.repository
            );
        }
        Ok(())
    }
}

/// A ref an action is fetched at: a tag's or a branch's name as git takes
/// one, of letters, digits, `.`, `_`, `-` and `/`, or a commit's full id.
pub fn check_ref(name: &str) -> std::result::Result<(), String> {
    let plain = !name.is_empty()
        && name.len() <= 255
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'));
    if !plain
        || name.starts_with(['-', '/', '.'])
        || name.ends_with(['/', '.'])
        || name.ends_with(".lock")
        || name.contains("..")
        || name.contains("//")
        || name.contains("/.")
    {
        return Err(format!(
            "the ref {name:?} is not a tag's, a branch's or a commit's: letters, digits, '.', \
             '_', '-' and '/', as git names one"
        ));
    }
    Ok(())
}

/// At most this many caches on a task.
pub const MAX_CACHES: usize = 8;
/// At most this many paths in a cache.
pub const MAX_CACHE_PATHS: usize = 16;
/// At most this many globs in a cache's key.
pub const MAX_CACHE_KEY_FILES: usize = 32;

/// `workspace.cache[]`: directories kept between runs, as one entry of the
/// cache store, under a key the task computes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PathCacheSpec {
    /// The cache's name, and the start of every key of it: `cargo`, `npm`.
    /// Letters, digits, `.`, `_` and `-`.
    pub prefix: String,
    /// Files of the workspace whose contents make the key, as globs (`*`,
    /// `**`, `?`) relative to it: `Cargo.lock`, `**/package-lock.json`. None:
    /// the key is the prefix alone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_files: Vec<String>,
    /// The directories kept: absolute, `~/…` in the task's home, or relative
    /// to the workspace.
    pub paths: Vec<String>,
    /// Another ref of the run's repository to read from when its own ref has
    /// nothing: a pull request's base branch. Never written to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_from: Option<String>,
}

impl PathCacheSpec {
    pub fn validate(&self) -> Result<()> {
        let p = &self.prefix;
        if p.is_empty()
            || p.len() > 64
            || !p
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            bail!("workspace.cache prefix {p:?} is not a name: 1 to 64 letters, digits, '.', '_' or '-'");
        }
        if self.paths.is_empty() {
            bail!("workspace.cache {p:?} names no paths: name the directories it keeps");
        }
        if self.paths.len() > MAX_CACHE_PATHS {
            bail!(
                "workspace.cache {p:?} names {} paths; at most {MAX_CACHE_PATHS}",
                self.paths.len()
            );
        }
        if self.key_files.len() > MAX_CACHE_KEY_FILES {
            bail!(
                "workspace.cache {p:?} names {} key files; at most {MAX_CACHE_KEY_FILES}",
                self.key_files.len()
            );
        }
        let plain = |what: &str, v: &str| -> Result<()> {
            if v.is_empty() || v.len() > 1024 {
                bail!("workspace.cache {p:?}: a {what} is empty or longer than 1024");
            }
            if v.contains("{{") {
                bail!("workspace.cache {p:?}: the {what} {v:?} names a parameter the run does not have");
            }
            if v.chars().any(|c| c.is_control()) {
                bail!("workspace.cache {p:?}: the {what} {v:?} holds a control character");
            }
            Ok(())
        };
        for path in &self.paths {
            plain("path", path)?;
            if path.split('/').any(|c| c == "..") {
                bail!("workspace.cache {p:?}: the path {path:?} climbs out with '..'");
            }
        }
        for file in &self.key_files {
            plain("key file", file)?;
            if file.starts_with('/') || file.split('/').any(|c| c == "..") {
                bail!("workspace.cache {p:?}: the key file {file:?} is not in the workspace: name it relative to it");
            }
        }
        if let Some(from) = &self.restore_from {
            if from.len() > 255
                || from.contains("{{")
                || from.chars().any(|c| c.is_control() || c.is_whitespace())
            {
                bail!("workspace.cache {p:?}: restore_from {from:?} is not a ref");
            }
        }
        Ok(())
    }
}

/// `workspace.checkout`: a repository and the commit of it to check out. Both
/// may name the run's parameters (`{{ ci_sha }}`), filled in when the run is
/// made, and are checked as they are then.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckoutSpec {
    /// An `https://` URL; `http://` for a repository fetched with no
    /// credential, since none is ever sent over it (but to this host's own
    /// loopback address).
    pub repository: String,
    /// The commit's full id: 40 hex digits, or 64 for a SHA-256 repository.
    pub commit: String,
    /// How much of the repository's history comes with the commit: how many
    /// commits deep. Unset, or 1: the commit alone, which is what a build
    /// reads. 0: all of it, every branch's and the tags, for a task that
    /// asks git what changed (`git merge-base`, `git describe`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    /// Whether a patch is laid over the commit once it is checked out: a run
    /// on a patch bundle, whose files dagron-ci keeps and hands each attempt's
    /// checkout a short-lived link to through the token service
    /// (`DAGRON_CHECKOUT_TOKEN_URL`). A checkout that is to be patched and is
    /// given no patch fails: it never runs the commit alone.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub patched: bool,
}

impl CheckoutSpec {
    pub fn validate(&self) -> Result<()> {
        if let Err(e) = check_repository(&self.repository) {
            bail!("workspace.checkout.repository: {e}");
        }
        if let Err(e) = check_commit(&self.commit) {
            bail!("workspace.checkout.commit: {e}");
        }
        Ok(())
    }
}

/// A repository URL a checkout may fetch: `https://` or `http://`, a host
/// (and port) with no user or password in it, and a path, with nothing a
/// shell or git would read as more than a URL.
pub fn check_repository(url: &str) -> std::result::Result<(), String> {
    if url.contains("{{") {
        return Err(format!("{url:?} names a parameter the run does not have"));
    }
    let Some(rest) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
    else {
        return Err(format!("{url:?} is not an https:// URL"));
    };
    if url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(format!("{url:?} holds a space or a control character"));
    }
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.contains('@') {
        return Err(format!(
            "{url:?} carries a user in it: a credential comes from the operator's token service, never the workflow"
        ));
    }
    let host_ok = match authority.strip_prefix('[') {
        // An IPv6 address, bracketed, then an optional port.
        Some(v6) => v6.split_once(']').is_some_and(|(addr, after)| {
            !addr.is_empty()
                && addr
                    .chars()
                    .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
                && (after.is_empty() || after.strip_prefix(':').is_some_and(is_port))
        }),
        None => {
            let (host, port) = match authority.split_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            };
            !host.is_empty()
                && host
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
                && port.is_none_or(is_port)
        }
    };
    if !host_ok {
        return Err(format!("{url:?} names no host"));
    }
    if path.is_empty() || path.contains(['?', '#', '\\']) || path.split('/').any(|seg| seg == "..")
    {
        return Err(format!(
            "{url:?} names no repository path, or one with a query, a fragment or a `..`"
        ));
    }
    Ok(())
}

/// A port number, 1 to 65535.
fn is_port(p: &str) -> bool {
    !p.is_empty()
        && p.len() <= 5
        && p.bytes().all(|b| b.is_ascii_digit())
        && p.parse::<u32>().is_ok_and(|n| (1..=65535).contains(&n))
}

/// A commit id: 40 lowercase hex digits (SHA-1) or 64 (SHA-256). A branch, a
/// tag or an abbreviated id moves or can be ambiguous; a full id is the one
/// commit the run is for.
pub fn check_commit(id: &str) -> std::result::Result<(), String> {
    if id.contains("{{") {
        return Err(format!("{id:?} names a parameter the run does not have"));
    }
    if (id.len() == 40 || id.len() == 64)
        && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(format!(
            "{id:?} is not a commit's full id (40 or 64 lowercase hex digits)"
        ))
    }
}

impl WorkspaceSpec {
    /// `size`, when set, is a quantity of more than nothing; a checkout names
    /// a repository and a commit as [`check_repository`] and [`check_commit`]
    /// take them.
    pub fn validate(&self) -> Result<()> {
        if let Some(checkout) = &self.checkout {
            checkout.validate()?;
        }
        if self.cache.len() > MAX_CACHES {
            bail!(
                "workspace.cache names {} caches; at most {MAX_CACHES}",
                self.cache.len()
            );
        }
        let mut seen = std::collections::HashSet::new();
        for cache in &self.cache {
            cache.validate()?;
            if !seen.insert(cache.prefix.as_str()) {
                bail!("workspace.cache names {:?} twice", cache.prefix);
            }
        }
        for (i, tool) in self.tools.iter().enumerate() {
            if !TOOLS.contains(&tool.as_str()) {
                bail!(
                    "workspace.tools names {tool:?}, and the tools are {}",
                    TOOLS.join(", ")
                );
            }
            if self.tools[..i].contains(tool) {
                bail!("workspace.tools names {tool:?} twice");
            }
        }
        if self.actions.len() > MAX_ACTIONS {
            bail!(
                "workspace.actions names {} actions; at most {MAX_ACTIONS}",
                self.actions.len()
            );
        }
        for (i, action) in self.actions.iter().enumerate() {
            action.validate()?;
            // Each is a directory of its own: one fetched into another's
            // would be in it, or over it.
            let within = |a: &str, b: &str| a == b || a.starts_with(&format!("{b}/"));
            if let Some(other) = self.actions[..i]
                .iter()
                .find(|o| within(&action.path, &o.path) || within(&o.path, &action.path))
            {
                bail!(
                    "workspace.actions: {:?} and {:?} would be fetched one into the other",
                    other.path,
                    action.path
                );
            }
        }
        if let Some(size) = &self.size {
            match quantity_bytes(size) {
                Some(0) => bail!("workspace.size {size:?} is zero"),
                Some(_) => {}
                None => bail!(
                    "workspace.size {size:?} is not a size: write a number and a unit, \
                     Ki Mi Gi Ti Pi Ei or k M G T P E (20Gi, 500M)"
                ),
            }
        }
        Ok(())
    }

    /// `size` in bytes; `None` when unset (or not a size, which
    /// [`WorkspaceSpec::validate`] refuses).
    pub fn size_bytes(&self) -> Option<u128> {
        self.size.as_deref().and_then(quantity_bytes)
    }
}

/// A Kubernetes quantity as a number of bytes: a decimal number (`1`, `1.5`)
/// and an optional binary (`Ki` … `Ei`) or decimal (`k` … `E`) suffix. The
/// rest of Kubernetes' grammar (signs, exponents, milli) has no use in a size
/// and is refused. A fraction of a byte rounds up.
pub fn quantity_bytes(raw: &str) -> Option<u128> {
    let digits_end = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(raw.len());
    let (number, suffix) = raw.split_at(digits_end);
    let multiplier: u128 = match suffix {
        "" => 1,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "Ei" => 1 << 60,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        "E" => 1_000_000_000_000_000_000,
        _ => return None,
    };
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    // "1." and ".5" are Kubernetes' too; "." alone is not a number.
    if whole.is_empty() && fraction.is_empty() || fraction.contains('.') {
        return None;
    }
    // A fraction finer than a byte at the largest unit is noise; refusing it
    // keeps the arithmetic below inside u128.
    if fraction.len() > 18 {
        return None;
    }
    let parse = |s: &str| {
        if s.is_empty() {
            Some(0)
        } else {
            s.parse::<u128>().ok()
        }
    };
    let scale = 10u128.pow(fraction.len() as u32);
    let scaled = parse(whole)?
        .checked_mul(scale)?
        .checked_add(parse(fraction)?)?;
    let bytes = scaled.checked_mul(multiplier)?;
    Some(bytes.div_ceil(scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_are_read_as_kubernetes_writes_them() {
        assert_eq!(quantity_bytes("0"), Some(0));
        assert_eq!(quantity_bytes("512"), Some(512));
        assert_eq!(quantity_bytes("20Gi"), Some(20 << 30));
        assert_eq!(quantity_bytes("1.5Gi"), Some(3 << 29));
        assert_eq!(quantity_bytes("500M"), Some(500_000_000));
        assert_eq!(quantity_bytes("1k"), Some(1_000));
        assert_eq!(quantity_bytes("2Ei"), Some(2 << 60));
        assert_eq!(quantity_bytes(".5Ki"), Some(512));
        assert_eq!(quantity_bytes("1."), Some(1));
        // A fraction of a byte rounds up rather than to nothing.
        assert_eq!(quantity_bytes("0.1"), Some(1));
        for bad in [
            "",
            ".",
            "Gi",
            "-1Gi",
            "+1Gi",
            "1e3",
            "1E3",
            "100m",
            "1.2.3",
            "1 Gi",
            "1gi",
            "1GB",
            "1KiB",
            "0.0000000000000000001",
        ] {
            assert_eq!(quantity_bytes(bad), None, "{bad:?}");
        }
        // Past what u128 holds is not a size either, rather than a wrapped one.
        assert_eq!(quantity_bytes("999999999999999999999999Ei"), None);
    }

    #[test]
    fn a_workspace_size_is_more_than_nothing() {
        assert!(WorkspaceSpec::default().validate().is_ok());
        let sized = |s: &str| WorkspaceSpec {
            size: Some(s.into()),
            ..WorkspaceSpec::default()
        };
        assert!(sized("20Gi").validate().is_ok());
        assert_eq!(sized("20Gi").size_bytes(), Some(20 << 30));
        assert!(sized("0")
            .validate()
            .unwrap_err()
            .to_string()
            .contains("zero"));
        assert!(sized("20GB")
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not a size"));
    }

    #[test]
    fn a_workspace_takes_only_the_keys_it_knows() {
        let ws: WorkspaceSpec = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(ws, WorkspaceSpec::default());
        let err = serde_json::from_value::<WorkspaceSpec>(serde_json::json!({"caches": []}))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown field `caches`"), "{err}");
        let err = serde_json::from_value::<WorkspaceSpec>(
            serde_json::json!({"cache": [{"prefix": "cargo", "paths": ["/x"], "keys": []}]}),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `keys`"), "{err}");
    }

    #[test]
    fn a_cache_names_itself_its_paths_and_files_of_the_workspace() {
        let ws = |v: serde_json::Value| -> WorkspaceSpec { serde_json::from_value(v).unwrap() };
        let ok = ws(serde_json::json!({"cache": [
            {"prefix": "cargo", "key_files": ["Cargo.lock"], "paths": ["/usr/local/cargo/registry"]},
            {"prefix": "npm", "key_files": ["**/package-lock.json"], "paths": ["~/.npm"], "restore_from": "main"},
        ]}));
        assert!(ok.validate().is_ok());
        assert_eq!(ok.cache[1].restore_from.as_deref(), Some("main"));
        for (v, why) in [
            (
                serde_json::json!({"prefix": "a b", "paths": ["/x"]}),
                "not a name",
            ),
            (
                serde_json::json!({"prefix": "", "paths": ["/x"]}),
                "not a name",
            ),
            (
                serde_json::json!({"prefix": "x", "paths": []}),
                "names no paths",
            ),
            (
                serde_json::json!({"prefix": "x", "paths": ["../up"]}),
                "climbs out",
            ),
            (
                serde_json::json!({"prefix": "x", "paths": ["/x"], "key_files": ["/etc/passwd"]}),
                "not in the workspace",
            ),
            (
                serde_json::json!({"prefix": "x", "paths": ["/x"], "key_files": ["a/../../b"]}),
                "not in the workspace",
            ),
            (
                serde_json::json!({"prefix": "x", "paths": ["{{ nope }}"]}),
                "names a parameter",
            ),
            (
                serde_json::json!({"prefix": "x", "paths": ["/x"], "restore_from": "a b"}),
                "not a ref",
            ),
        ] {
            let err = ws(serde_json::json!({"cache": [v]}))
                .validate()
                .unwrap_err()
                .to_string();
            assert!(err.contains(why), "{err}");
        }
        let twice = ws(serde_json::json!({"cache": [
            {"prefix": "x", "paths": ["/a"]}, {"prefix": "x", "paths": ["/b"]}
        ]}));
        assert!(twice.validate().unwrap_err().to_string().contains("twice"));
    }

    #[test]
    fn a_checkout_names_an_https_repository_and_a_full_commit_id() {
        let sha1 = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
        for url in [
            "https://github.com/acme/widgets.git",
            "https://ghe.example.com:8443/acme/widgets",
            "http://127.0.0.1:3000/repo.git",
            "https://[::1]:8443/repo.git",
            "https://gitlab.com/group/sub/repo.git",
        ] {
            assert_eq!(check_repository(url), Ok(()), "{url}");
        }
        for (url, why) in [
            ("github.com/acme/widgets", "not an https:// URL"),
            ("ssh://git@github.com/acme/widgets", "not an https:// URL"),
            ("file:///etc", "not an https:// URL"),
            ("ext::sh -c touch% /tmp/x", "not an https:// URL"),
            (
                "https://x-access-token:secret@github.com/a/b",
                "carries a user",
            ),
            ("https://github.com", "names no repository path"),
            ("https://github.com/", "names no repository path"),
            ("https://github.com/a/b?x=1", "a query"),
            ("https://github.com/a/../b", "`..`"),
            ("https://github.com/a/b c", "a space"),
            (
                "https://git\nhub.com/a/b".replace("\\n", "\n").as_str(),
                "a control character",
            ),
            ("https:///a/b", "names no host"),
            ("https://github.com:0/a/b", "names no host"),
            ("https://github.com:99999/a/b", "names no host"),
            ("https://git_hub.com/a/b", "names no host"),
            ("https://{{ ci_forge_host }}/a/b.git", "names a parameter"),
        ] {
            let err = check_repository(url).unwrap_err();
            assert!(err.contains(why), "{url:?}: {err}");
        }
        assert_eq!(check_commit(sha1), Ok(()));
        assert_eq!(check_commit(&"ab".repeat(32)), Ok(()), "SHA-256");
        for bad in [
            "",
            "main",
            "4b4708e",
            &sha1.to_uppercase(),
            &format!("{sha1}0"),
            "{{ ci_sha }}",
        ] {
            assert!(check_commit(bad).is_err(), "{bad:?}");
        }
        let ws = |repository: &str, commit: &str| WorkspaceSpec {
            checkout: Some(CheckoutSpec {
                repository: repository.into(),
                commit: commit.into(),
                depth: None,
                patched: false,
            }),
            ..WorkspaceSpec::default()
        };
        assert!(ws("https://github.com/acme/widgets.git", sha1)
            .validate()
            .is_ok());
        let err = ws("https://github.com/acme/widgets.git", "")
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("workspace.checkout.commit: "), "{err}");
        let err = ws("git@github.com:acme/widgets.git", sha1)
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("workspace.checkout.repository: "), "{err}");
        // Its keys, and no others. How deep its history goes is a number,
        // and is not written where it is not said.
        let deep: WorkspaceSpec = serde_json::from_value(serde_json::json!({
            "checkout": {"repository": "https://h/r", "commit": sha1, "depth": 0}
        }))
        .unwrap();
        assert_eq!(deep.checkout.as_ref().unwrap().depth, Some(0));
        let plain = serde_json::to_value(ws("https://h/r", sha1)).unwrap();
        assert!(plain["checkout"].get("depth").is_none(), "{plain}");
        for bad in [serde_json::json!("all"), serde_json::json!(-1)] {
            assert!(serde_json::from_value::<WorkspaceSpec>(serde_json::json!({
                "checkout": {"repository": "https://h/r", "commit": sha1, "depth": bad}
            }))
            .is_err());
        }
        let err = serde_json::from_value::<WorkspaceSpec>(serde_json::json!({
            "checkout": {"repository": "https://h/r", "commit": sha1, "ref": "main"}
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `ref`"), "{err}");
    }

    #[test]
    fn a_task_names_the_tools_it_needs_installed_of_those_there_are() {
        let ws = |tools: serde_json::Value| -> WorkspaceSpec {
            serde_json::from_value(serde_json::json!({ "tools": tools })).unwrap()
        };
        assert!(ws(serde_json::json!(["node"])).validate().is_ok());
        assert!(ws(serde_json::json!([])).validate().is_ok());
        for (tools, why) in [
            (serde_json::json!(["python"]), "the tools are node"),
            (serde_json::json!(["node", "node"]), "twice"),
        ] {
            let err = ws(tools).validate().unwrap_err().to_string();
            assert!(err.contains(why), "{err}");
        }
        let none = serde_json::to_value(WorkspaceSpec::default()).unwrap();
        assert!(none.get("tools").is_none(), "{none}");
    }

    #[test]
    fn an_action_names_a_repository_a_ref_and_its_own_directory() {
        let ws = |actions: serde_json::Value| -> WorkspaceSpec {
            serde_json::from_value(serde_json::json!({ "actions": actions })).unwrap()
        };
        let ok = ws(serde_json::json!([
            {"repository": "https://github.com/actions/setup-node.git", "ref": "v4", "path": "actions/setup-node/v4"},
            {"repository": "https://github.com/octo/hello.git", "ref": "releases/v1", "path": "octo/hello/releases%2Fv1"},
            {"repository": "https://github.com/octo/hello.git", "ref": "4b4708e725f29891d22b4c96af9f0a403ff51c7c", "path": "octo/hello/4b4708e725f29891d22b4c96af9f0a403ff51c7c"},
        ]));
        assert!(ok.validate().is_ok(), "{:?}", ok.validate());
        assert_eq!(ok.actions[1].git_ref, "releases/v1");
        assert_eq!(
            serde_json::to_value(&ok.actions[0]).unwrap()["ref"],
            "v4",
            "written as `ref`"
        );
        // Unset, it is not written.
        let none = serde_json::to_value(WorkspaceSpec::default()).unwrap();
        assert!(none.get("actions").is_none(), "{none}");

        for (action, why) in [
            (
                serde_json::json!({"repository": "git@github.com:a/b", "ref": "v1", "path": "a/b/v1"}),
                "not an https:// URL",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "v1 ; rm", "path": "a/b/v1"}),
                "is not a tag's",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "-v1", "path": "a/b/v1"}),
                "is not a tag's",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "a..b", "path": "a/b/v1"}),
                "is not a tag's",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "v1.lock", "path": "a/b/v1"}),
                "is not a tag's",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "", "path": "a/b/v1"}),
                "is not a tag's",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "v1", "path": "a/../../etc"}),
                "is not one under the actions' directory",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "v1", "path": "/etc"}),
                "is not one under the actions' directory",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "v1", "path": "a//b"}),
                "is not one under the actions' directory",
            ),
            (
                serde_json::json!({"repository": "https://h/a/b", "ref": "v1", "path": "a/b c"}),
                "is not one under the actions' directory",
            ),
        ] {
            let err = ws(serde_json::json!([action]))
                .validate()
                .unwrap_err()
                .to_string();
            assert!(err.contains(why), "{action}: {err}");
        }
        let overlapping = ws(serde_json::json!([
            {"repository": "https://h/a/b", "ref": "v1", "path": "a/b/v1"},
            {"repository": "https://h/a/c", "ref": "v1", "path": "a/b/v1/c"},
        ]));
        let err = overlapping.validate().unwrap_err().to_string();
        assert!(err.contains("one into the other"), "{err}");
        let same = ws(serde_json::json!([
            {"repository": "https://h/a/b", "ref": "v1", "path": "a/b/v1"},
            {"repository": "https://h/a/b", "ref": "v1", "path": "a/b/v1"},
        ]));
        assert!(same.validate().is_err());
        let many: Vec<serde_json::Value> = (0..=MAX_ACTIONS)
            .map(|i| serde_json::json!({"repository": "https://h/a/b", "ref": "v1", "path": format!("a/{i}")}))
            .collect();
        let err = ws(serde_json::Value::Array(many))
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("at most 32"), "{err}");
        let err = serde_json::from_value::<WorkspaceSpec>(serde_json::json!({
            "actions": [{"repository": "https://h/a/b", "ref": "v1", "path": "a", "token": "x"}]
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `token`"), "{err}");
    }
}
