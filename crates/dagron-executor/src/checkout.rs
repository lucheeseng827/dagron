//! A workspace's checkout: how each executor fetches the commit a task's
//! `workspace.checkout` names into its workspace before the task runs, and
//! with what credential. What a checkout is, and how its repository and commit
//! are checked, is [`dagron_core::workspace::CheckoutSpec`].
//!
//! The fetch is one script, [`SCRIPT`], the same everywhere: `git` itself on
//! the local executor; on Docker and Kubernetes, a container from the
//! operator's [`ENV_CHECKOUT_IMAGE`] (any image with `git` and `sh`) that
//! mounts the workspace and is gone before the task starts. It fetches the
//! one commit, depth 1, and checks it out detached.
//!
//! **The credential** is minted for each attempt by the operator's token
//! service ([`CheckoutTokens`], called by the worker), handed to that fetch
//! alone and revoked when the attempt ends. Git is given it as an
//! `http.<repository>.extraHeader` in its environment (`GIT_CONFIG_*`), so it
//! is never written into the checkout, is sent only to the repository's own
//! URL, and only over https or to this host's loopback address
//! ([`may_carry_credential`]). A fetch that carries one follows no redirect:
//! git would send the header on to wherever a redirect of its first request
//! moved the fetch. The task never sees it: on Kubernetes it
//! reaches the checkout container from the task's Secret, which the task's
//! container does not name.

use async_trait::async_trait;
use dagron_core::workspace::{ActionSpec, CheckoutSpec};

/// An image with `git` and `sh`, which the Docker and Kubernetes executors
/// fetch a workspace's checkout with. Unset, a task with a checkout fails
/// there, saying so.
pub const ENV_CHECKOUT_IMAGE: &str = "DAGRON_CHECKOUT_IMAGE";

/// The checkout container's name, in a pod and in a Docker container's name.
pub const CONTAINER_NAME: &str = "checkout";

/// The key of the credential in a task's Kubernetes Secret.
pub const SECRET_KEY: &str = "dagron-checkout-authorization";

/// The container that fetches a task's actions, in a pod and in a Docker
/// container's name.
pub const ACTIONS_CONTAINER_NAME: &str = "actions";

/// The fetch of a task's actions, run as `sh -ec ACTIONS_SCRIPT` with the
/// checkout's image. Each comes into a directory of its own under
/// `DAGRON_ACTIONS_DIR`, alone (depth 1), at its ref as that ref stands now,
/// with no credential but its own where it asks for one
/// ([`actions_credential_env`]); its `.git` is removed after, so that the directory
/// holds the action's files and nothing of git's. Its inputs are variables,
/// never text in it: [`actions_env`]. What it says is which commit each ref
/// was.
pub const ACTIONS_SCRIPT: &str = r#"i=1
while [ "$i" -le "$DAGRON_ACTION_COUNT" ]; do
  eval "repository=\$DAGRON_ACTION_${i}_REPOSITORY; ref=\$DAGRON_ACTION_${i}_REF; dir=\$DAGRON_ACTIONS_DIR/\$DAGRON_ACTION_${i}_PATH"
  mkdir -p "$dir"
  git -C "$dir" -c init.defaultBranch=action init -q
  git -C "$dir" remote add origin "$repository"
  git -C "$dir" fetch -q --depth=1 --no-tags origin "$ref" || { echo "the action $repository has no $ref to fetch, or could not be reached" >&2; exit 1; }
  git -C "$dir" -c advice.detachedHead=false checkout -q --detach FETCH_HEAD
  echo "fetched the action $repository at $ref: $(git -C "$dir" rev-parse HEAD)"
  rm -rf "$dir/.git"
  i=$((i + 1))
done
"#;

/// The fetch, run as `sh -ec SCRIPT` in the workspace. Its inputs are
/// variables, never text in it: [`plain_env`] and [`secret_env`]. The
/// commit comes alone, or as many commits deep as the checkout says
/// (`DAGRON_CHECKOUT_DEPTH`, always set). At 0 it comes with every
/// branch's history and the tags, and is then fetched by its own id as
/// well: a pull request's commit is on no branch. With
/// `DAGRON_CHECKOUT_UMASK` (a container's fetch), what it writes, and the
/// workspace's top where it may, are open to every user: the task's user may
/// not be the fetch's, and a workspace is the one task's alone.
pub const SCRIPT: &str = r#"if [ -n "${DAGRON_CHECKOUT_UMASK:-}" ]; then umask "$DAGRON_CHECKOUT_UMASK"; chmod a+rwx . 2>/dev/null || true; fi
git -c init.defaultBranch=checkout init -q .
git remote add origin "$DAGRON_CHECKOUT_REPOSITORY"
if [ -n "${DAGRON_CHECKOUT_PATCHED:-}" ]; then
  if [ -z "${DAGRON_CHECKOUT_PATCH_URL:-}" ]; then
    echo "this checkout is of a patch, and no link to the patch was given" >&2; exit 1
  fi
  if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    echo "this checkout is of a patch, and its image has neither curl nor wget to fetch it" >&2; exit 1
  fi
  fetched=
  for wait in 0 2 4 8 16; do
    sleep "$wait"
    if command -v curl >/dev/null 2>&1; then curl -fsS -o .git/dagron-patch.tar "$DAGRON_CHECKOUT_PATCH_URL" && { fetched=1; break; }
    else wget -q -O .git/dagron-patch.tar "$DAGRON_CHECKOUT_PATCH_URL" && { fetched=1; break; }
    fi
  done
  [ -n "$fetched" ] || { echo "the patch could not be fetched" >&2; exit 1; }
fi
case "${DAGRON_CHECKOUT_DEPTH:-1}" in
  0) git fetch -q --tags origin "+refs/heads/*:refs/remotes/origin/*"
     git fetch -q --no-tags origin "$DAGRON_CHECKOUT_COMMIT" ;;
  *) git fetch -q --depth="$DAGRON_CHECKOUT_DEPTH" --no-tags origin "$DAGRON_CHECKOUT_COMMIT" ;;
esac
git -c advice.detachedHead=false checkout -q --detach FETCH_HEAD
echo "checked out $DAGRON_CHECKOUT_COMMIT from $DAGRON_CHECKOUT_REPOSITORY"
if [ -n "${DAGRON_CHECKOUT_PATCHED:-}" ]; then
  # Nothing is laid or deleted under a symbolic link of the commit's: tar and
  # rm would follow it out of the workspace. A directory's own entry is
  # checked as well as those above it.
  under_link() {
    p=${1#./}
    case "$p" in */) p=${p%/}; [ ! -L "./$p" ] || { echo "the patch's $1 is a symbolic link of the commit's: the patch is not laid over it" >&2; return 1; } ;; esac
    while case "$p" in */*) true ;; *) false ;; esac; do
      p=${p%/*}
      if [ -L "./$p" ]; then echo "the patch's $1 is under $p, a symbolic link of the commit's: the patch is not laid over it" >&2; return 1; fi
    done
  }
  tar -tf .git/dagron-patch.tar > .git/dagron-patch.paths
  while IFS= read -r path || [ -n "$path" ]; do [ -z "$path" ] || under_link "$path" || exit 1; done < .git/dagron-patch.paths
  rm -f .git/dagron-patch.paths
  # The patch's .dagron-deleted alone says what it deletes: a file of that
  # name the commit has is set aside, and put back after.
  if [ -e .dagron-deleted ] || [ -L .dagron-deleted ]; then mv -- .dagron-deleted .git/dagron-commit-deleted; fi
  tar -xf .git/dagron-patch.tar
  rm -f .git/dagron-patch.tar
  if [ -f .dagron-deleted ]; then
    while IFS= read -r gone || [ -n "$gone" ]; do
      [ -n "$gone" ] || continue
      under_link "$gone" || exit 1
      rm -rf -- "./$gone"
    done < .dagron-deleted
    rm -f .dagron-deleted
  fi
  if [ -e .git/dagron-commit-deleted ] || [ -L .git/dagron-commit-deleted ]; then mv -- .git/dagron-commit-deleted .dagron-deleted; fi
  echo "laid the patch over $DAGRON_CHECKOUT_COMMIT"
fi
"#;

/// A credential for one checkout: HTTP basic, as the token service gives it
/// (GitHub's is `x-access-token` and an installation token).
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

impl Credential {
    /// The `Authorization` header value: `Basic` and the pair, base64.
    pub fn authorization(&self) -> String {
        format!(
            "Basic {}",
            base64(format!("{}:{}", self.username, self.password).as_bytes())
        )
    }
}

/// What the worker asks the token service for: a credential for this
/// repository and commit, for one attempt of one task of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenRequest {
    pub repository: String,
    pub commit: String,
    pub run_id: String,
    pub task_id: String,
    pub attempt: i64,
    /// For a forge token the task holds, the permissions it asks for
    /// ([`dagron_core::forge_token`]); `None` for the checkout's own, which
    /// reaches the fetch alone.
    pub permissions: Option<std::collections::BTreeMap<String, String>>,
    /// For an action's fetch ([`ActionSpec::credential`]): `repository` is
    /// the action's, and `commit` the ref it is fetched at.
    pub action: bool,
}

/// The operator's token service. The engine implements it over HTTP
/// (`DAGRON_CHECKOUT_TOKEN_URL`); a trait here keeps this crate free of an
/// HTTP client, the same seam as [`crate::worker::LeaseKeeper`].
#[async_trait]
pub trait CheckoutTokens: Send + Sync + 'static {
    /// A credential for `request`, or `None` when the repository is fetched
    /// without one (a public repository, a forge with no app). An error fails
    /// the attempt: a checkout that needed a credential would fail anyway,
    /// later and less clearly.
    async fn mint(&self, request: &TokenRequest) -> anyhow::Result<Option<Credential>>;
    /// Revoke a credential [`CheckoutTokens::mint`] gave, now that its attempt
    /// is over. Best effort: an error is logged, and the credential expires
    /// on its own.
    async fn revoke(&self, credential: &Credential) -> anyhow::Result<()>;
    /// The link to the patch a patched checkout lays over its commit
    /// ([`CheckoutSpec::patched`]), for `request`'s attempt. A service that
    /// keeps no patches has none to give: the attempt then fails.
    async fn patch(&self, request: &TokenRequest) -> anyhow::Result<String> {
        let _ = request;
        anyhow::bail!("this engine's token service gives no patch links")
    }
}

/// Whether a credential may be sent to `repository`: over https, or over
/// plain http to this host's own loopback address, where it never crosses a
/// network.
pub fn may_carry_credential(repository: &str) -> bool {
    if repository.starts_with("https://") {
        return true;
    }
    let Some(rest) = repository.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default(),
        None => authority.split(':').next().unwrap_or_default(),
    };
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The variables the fetch runs with, but the credential: the repository and
/// commit, no prompt for a password, and, with a credential, which header git
/// sends and to which URL, and that it follows no redirect. `umask` widens
/// what the checkout writes, for a task in a container whose user may not be
/// the checkout's.
pub fn plain_env(
    checkout: &CheckoutSpec,
    with_credential: bool,
    umask: Option<&str>,
) -> Vec<(String, String)> {
    let mut env = vec![
        (
            "DAGRON_CHECKOUT_REPOSITORY".to_string(),
            checkout.repository.clone(),
        ),
        (
            "DAGRON_CHECKOUT_COMMIT".to_string(),
            checkout.commit.clone(),
        ),
        (
            "DAGRON_CHECKOUT_DEPTH".to_string(),
            checkout.depth.unwrap_or(1).to_string(),
        ),
        ("GIT_TERMINAL_PROMPT".to_string(), "0".to_string()),
    ];
    if let Some(umask) = umask {
        env.push(("DAGRON_CHECKOUT_UMASK".to_string(), umask.to_string()));
    }
    if with_credential {
        env.push(("GIT_CONFIG_COUNT".to_string(), "2".to_string()));
        env.push((
            "GIT_CONFIG_KEY_0".to_string(),
            format!("http.{}.extraHeader", checkout.repository),
        ));
        // Git sends the header to wherever a redirect of its first request
        // moves the fetch, and the credential is for this URL alone.
        env.push((
            "GIT_CONFIG_KEY_1".to_string(),
            "http.followRedirects".to_string(),
        ));
        env.push(("GIT_CONFIG_VALUE_1".to_string(), "false".to_string()));
    }
    env
}

/// The one variable that carries the credential: the header git sends, as
/// [`plain_env`]'s `GIT_CONFIG_KEY_0` names it. `None` when there is no
/// credential, or the repository may not be sent one.
pub fn secret_env(
    checkout: &CheckoutSpec,
    credential: Option<&Credential>,
) -> Option<(String, String)> {
    let credential = credential.filter(|_| may_carry_credential(&checkout.repository))?;
    Some((
        "GIT_CONFIG_VALUE_0".to_string(),
        format!("Authorization: {}", credential.authorization()),
    ))
}

/// The variables of a patched checkout ([`CheckoutSpec::patched`]): that
/// it is one, and the link to its patch, which the token service gave the
/// attempt (`None` where it gave none, and the fetch then fails). The link
/// is a bearer's: it is the fetch's alone, as the credential is.
pub fn patch_env(checkout: &CheckoutSpec, link: Option<&str>) -> Vec<(String, String)> {
    if !checkout.patched {
        return Vec::new();
    }
    let mut env = vec![("DAGRON_CHECKOUT_PATCHED".to_string(), "1".to_string())];
    if let Some(link) = link {
        env.push(("DAGRON_CHECKOUT_PATCH_URL".to_string(), link.to_string()));
    }
    env
}

/// Every variable of the fetch, the credential's included: for an executor
/// that hands them to the fetch's own process or container.
pub fn fetch_env(
    checkout: &CheckoutSpec,
    credential: Option<&Credential>,
    umask: Option<&str>,
) -> Vec<(String, String)> {
    let secret = secret_env(checkout, credential);
    let mut env = plain_env(checkout, secret.is_some(), umask);
    env.extend(secret);
    env
}

/// The variables [`ACTIONS_SCRIPT`] runs with: where the actions go, how
/// many, and each one's repository, ref and directory under `dir`.
pub fn actions_env(actions: &[ActionSpec], dir: &str) -> Vec<(String, String)> {
    let mut env = vec![
        (
            dagron_core::workspace::ENV_ACTIONS_DIR.to_string(),
            dir.to_string(),
        ),
        ("DAGRON_ACTION_COUNT".to_string(), actions.len().to_string()),
        ("GIT_TERMINAL_PROMPT".to_string(), "0".to_string()),
    ];
    for (i, action) in actions.iter().enumerate() {
        let k = i + 1;
        env.push((
            format!("DAGRON_ACTION_{k}_REPOSITORY"),
            action.repository.clone(),
        ));
        env.push((format!("DAGRON_ACTION_{k}_REF"), action.git_ref.clone()));
        env.push((format!("DAGRON_ACTION_{k}_PATH"), action.path.clone()));
    }
    env
}

/// The key of the task's Secret the credential of the `n`th action with
/// one is held under (from 1), as [`action_headers`] numbers them.
pub fn action_secret_key(n: usize) -> String {
    format!("dagron-action-authorization-{n}")
}

/// The header each action's fetch is sent with, by its repository, each
/// once and in order: for each action that asks for a credential and was
/// minted one, where its repository may be sent one.
pub fn action_headers(ctx: &crate::executor::ExecContext) -> Vec<(String, String)> {
    let Some(ws) = ctx.workspace.as_ref() else {
        return Vec::new();
    };
    let mut out: Vec<(String, String)> = Vec::new();
    for action in ws.actions.iter().filter(|a| a.credential) {
        if out
            .iter()
            .any(|(repository, _)| *repository == action.repository)
            || !may_carry_credential(&action.repository)
        {
            continue;
        }
        let minted = ctx
            .action_credentials
            .iter()
            .find(|(repository, _)| *repository == action.repository);
        if let Some((_, credential)) = minted {
            out.push((
                action.repository.clone(),
                format!("Authorization: {}", credential.authorization()),
            ));
        }
    }
    out
}

/// The variables that send each of `repositories` its header, but the
/// headers: which header git sends to which URL, and that it follows no
/// redirect. The `n`th repository's header is `GIT_CONFIG_VALUE_<n>`, from
/// 1. None where there are none.
pub fn actions_credential_env(repositories: &[String]) -> Vec<(String, String)> {
    if repositories.is_empty() {
        return Vec::new();
    }
    let mut env = vec![
        (
            "GIT_CONFIG_COUNT".to_string(),
            (repositories.len() + 1).to_string(),
        ),
        // Git sends a header to wherever a redirect of its first request
        // moves the fetch, and each credential is for its URL alone.
        (
            "GIT_CONFIG_KEY_0".to_string(),
            "http.followRedirects".to_string(),
        ),
        ("GIT_CONFIG_VALUE_0".to_string(), "false".to_string()),
    ];
    for (i, repository) in repositories.iter().enumerate() {
        env.push((
            format!("GIT_CONFIG_KEY_{}", i + 1),
            format!("http.{repository}.extraHeader"),
        ));
    }
    env
}

/// Every variable of the actions' fetch, their credentials' included: for
/// an executor that hands them to the fetch's own process or container.
pub fn actions_fetch_env(
    ctx: &crate::executor::ExecContext,
    actions: &[ActionSpec],
    dir: &str,
) -> Vec<(String, String)> {
    let headers = action_headers(ctx);
    let repositories: Vec<String> = headers.iter().map(|(r, _)| r.clone()).collect();
    let mut env = actions_env(actions, dir);
    env.extend(actions_credential_env(&repositories));
    env.extend(
        headers
            .into_iter()
            .enumerate()
            .map(|(i, (_, header))| (format!("GIT_CONFIG_VALUE_{}", i + 1), header)),
    );
    env
}

/// The actions a task's workspace names, each checked again at the last
/// moment before a fetch is handed it, as [`of`] checks a checkout.
pub fn actions_of(ctx: &crate::executor::ExecContext) -> anyhow::Result<&[ActionSpec]> {
    let actions = ctx
        .workspace
        .as_ref()
        .map(|w| w.actions.as_slice())
        .unwrap_or_default();
    for action in actions {
        action.validate()?;
    }
    Ok(actions)
}

/// What a task in a container is given after a checkout another container
/// made: git's `safe.directory` for the workspace, since its files may belong
/// to another user and git refuses a repository it does not own. As
/// `GIT_CONFIG_*`, which git reads as its command line's, and only where the
/// task sets none of its own.
pub fn task_env(task_env: &[dagron_core::dag::EnvVar]) -> Vec<(String, String)> {
    if task_env.iter().any(|e| e.name == "GIT_CONFIG_COUNT") {
        return Vec::new();
    }
    vec![
        ("GIT_CONFIG_COUNT".to_string(), "1".to_string()),
        ("GIT_CONFIG_KEY_0".to_string(), "safe.directory".to_string()),
        (
            "GIT_CONFIG_VALUE_0".to_string(),
            dagron_core::workspace::WORKSPACE_DIR.to_string(),
        ),
    ]
}

/// The checkout a task's workspace asks for, checked again at the last moment
/// before a fetch runs: it was checked when the run was made, but a fetch is
/// handed it, and so the check is not left to one place.
pub fn of(ctx: &crate::executor::ExecContext) -> anyhow::Result<Option<&CheckoutSpec>> {
    match ctx.workspace.as_ref().and_then(|w| w.checkout.as_ref()) {
        None => Ok(None),
        Some(checkout) => {
            checkout.validate()?;
            Ok(Some(checkout))
        }
    }
}

/// Fetch `checkout` into `dir` with this host's `git`, as the local executor
/// does: a process of its own, not the task's command (so not subject to a
/// pool's command allow-list), with this engine's environment and the fetch's
/// variables, bounded by `timeout_secs`.
pub(crate) async fn fetch_locally(
    dir: &std::path::Path,
    checkout: &CheckoutSpec,
    credential: Option<&Credential>,
    patch: Option<&str>,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let mut env = fetch_env(checkout, credential, None);
    env.extend(patch_env(checkout, patch));
    let out = run_locally(SCRIPT, dir, env, timeout_secs, "the checkout").await?;
    if out.status.success() {
        tracing::info!(repository = %checkout.repository, commit = %checkout.commit, "checked out");
        return Ok(());
    }
    anyhow::bail!(
        "the checkout of {} at {} failed ({}): {}{}",
        checkout.repository,
        checkout.commit,
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr).trim_end()
    )
}

/// Fetch `actions` into `dir` with this host's `git`, as the local executor
/// does: as [`fetch_locally`] fetches a checkout.
pub(crate) async fn fetch_actions_locally(
    dir: &std::path::Path,
    ctx: &crate::executor::ExecContext,
    actions: &[ActionSpec],
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let env = actions_fetch_env(ctx, actions, &dir.to_string_lossy());
    let out = run_locally(ACTIONS_SCRIPT, dir, env, timeout_secs, "the actions' fetch").await?;
    if out.status.success() {
        tracing::info!(count = actions.len(), "fetched the task's actions");
        return Ok(());
    }
    anyhow::bail!(
        "the fetch of the task's actions failed ({}): {}{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr).trim_end()
    )
}

/// Run `script` in `dir` as `sh -ec`, a process of its own: with this
/// engine's environment, but for git's variables that would point it at
/// another repository, and `env` after it, bounded by `timeout_secs`.
async fn run_locally(
    script: &str,
    dir: &std::path::Path,
    env: Vec<(String, String)>,
    timeout_secs: u64,
    what: &str,
) -> anyhow::Result<std::process::Output> {
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-ec")
        .arg(script)
        .current_dir(dir)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    // A git variable this process happens to carry would point the fetch at
    // another repository.
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_COMMON_DIR",
    ] {
        cmd.env_remove(name);
    }
    // After any git configuration this engine's environment carries (a
    // proxy's, say), rather than in its place.
    let inherited = std::env::var("GIT_CONFIG_COUNT")
        .ok()
        .and_then(|n| n.trim().parse::<usize>().ok())
        .unwrap_or(0);
    cmd.envs(appended(env, inherited));
    match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), cmd.output()).await {
        Ok(out) => out.map_err(|e| anyhow::anyhow!("{what} could not start sh: {e}")),
        Err(_) => Err(
            anyhow::Error::new(crate::executor::TimeoutError { secs: timeout_secs })
                .context(format!("{what} did not finish")),
        ),
    }
}

/// `env` with its git configuration entries numbered after `inherited`
/// others, and the count to match.
fn appended(env: Vec<(String, String)>, inherited: usize) -> Vec<(String, String)> {
    let renumbered = |name: &str, prefix: &str| {
        let i: usize = name.strip_prefix(prefix)?.parse().ok()?;
        Some(format!("{prefix}{}", inherited + i))
    };
    env.into_iter()
        .map(|(name, value)| {
            if name == "GIT_CONFIG_COUNT" {
                let own: usize = value.parse().unwrap_or(0);
                return (name, (inherited + own).to_string());
            }
            match renumbered(&name, "GIT_CONFIG_KEY_")
                .or_else(|| renumbered(&name, "GIT_CONFIG_VALUE_"))
            {
                Some(renamed) => (renamed, value),
                None => (name, value),
            }
        })
        .collect()
}

/// Standard base64, padded.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

    fn checkout(repository: &str) -> CheckoutSpec {
        CheckoutSpec {
            repository: repository.into(),
            commit: SHA.into(),
            depth: None,
            patched: false,
        }
    }

    fn credential() -> Credential {
        Credential {
            username: "x-access-token".into(),
            password: "ghs_job_lived".into(),
        }
    }

    #[test]
    fn base64_is_the_standard_padded_one() {
        for (plain, encoded) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            (
                "x-access-token:ghs_job_lived",
                "eC1hY2Nlc3MtdG9rZW46Z2hzX2pvYl9saXZlZA==",
            ),
        ] {
            assert_eq!(base64(plain.as_bytes()), encoded, "{plain:?}");
        }
    }

    #[test]
    fn a_credential_goes_over_https_or_to_loopback_only() {
        for url in [
            "https://github.com/acme/widgets.git",
            "http://127.0.0.1:3000/repo.git",
            "http://localhost/repo.git",
            "http://[::1]:8080/repo.git",
        ] {
            assert!(may_carry_credential(url), "{url}");
        }
        for url in [
            "http://github.com/acme/widgets.git",
            "http://172.17.0.1:3000/repo.git",
            "http://127.0.0.1.example.com/repo.git",
            "http://localhost.example.com/repo.git",
            "ssh://github.com/a/b",
        ] {
            assert!(!may_carry_credential(url), "{url}");
        }
    }

    #[test]
    fn the_fetch_is_given_the_header_for_its_repository_alone() {
        let c = checkout("https://github.com/acme/widgets.git");
        let env = fetch_env(&c, Some(&credential()), None);
        let get = |k: &str| env.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(
            get("DAGRON_CHECKOUT_REPOSITORY"),
            Some(c.repository.as_str())
        );
        assert_eq!(get("DAGRON_CHECKOUT_COMMIT"), Some(SHA));
        assert_eq!(get("GIT_TERMINAL_PROMPT"), Some("0"));
        assert_eq!(get("GIT_CONFIG_COUNT"), Some("2"));
        assert_eq!(
            get("GIT_CONFIG_KEY_0"),
            Some("http.https://github.com/acme/widgets.git.extraHeader")
        );
        assert_eq!(
            get("GIT_CONFIG_VALUE_0"),
            Some("Authorization: Basic eC1hY2Nlc3MtdG9rZW46Z2hzX2pvYl9saXZlZA==")
        );
        assert_eq!(get("GIT_CONFIG_KEY_1"), Some("http.followRedirects"));
        assert_eq!(get("GIT_CONFIG_VALUE_1"), Some("false"));
        assert_eq!(get("DAGRON_CHECKOUT_UMASK"), None);
        assert_eq!(
            fetch_env(&c, None, Some("0000"))
                .iter()
                .find(|(n, _)| n == "DAGRON_CHECKOUT_UMASK")
                .map(|(_, v)| v.as_str()),
            Some("0000")
        );

        // No credential, or one the URL may not carry: no header, and no key
        // naming one.
        for env in [
            fetch_env(&c, None, None),
            fetch_env(
                &checkout("http://example.com/a/b.git"),
                Some(&credential()),
                None,
            ),
        ] {
            assert!(
                env.iter().all(|(n, _)| !n.starts_with("GIT_CONFIG_")),
                "{env:?}"
            );
        }
        // The plain part never holds the password, whatever is passed.
        assert!(plain_env(&c, true, None)
            .iter()
            .all(|(_, v)| !v.contains("ghs_job_lived") && !v.contains("Z2hz")));
    }

    #[test]
    fn a_local_fetch_s_header_follows_the_engine_s_own_git_configuration() {
        let c = checkout("https://github.com/acme/widgets.git");
        let env = appended(fetch_env(&c, Some(&credential()), None), 3);
        let get = |k: &str| env.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("GIT_CONFIG_COUNT"), Some("5"));
        assert_eq!(
            get("GIT_CONFIG_KEY_3"),
            Some("http.https://github.com/acme/widgets.git.extraHeader")
        );
        assert!(get("GIT_CONFIG_VALUE_3").is_some_and(|v| v.starts_with("Authorization: Basic ")));
        assert_eq!(get("GIT_CONFIG_KEY_4"), Some("http.followRedirects"));
        assert_eq!(get("GIT_CONFIG_VALUE_4"), Some("false"));
        assert_eq!(
            get("GIT_CONFIG_KEY_0"),
            None,
            "the engine's own entries stand"
        );
        // Without a credential, nothing of git's configuration is touched.
        assert!(appended(fetch_env(&c, None, None), 3)
            .iter()
            .all(|(n, _)| !n.starts_with("GIT_CONFIG_")));
    }

    #[test]
    fn a_credential_does_not_show_its_password() {
        let shown = format!("{:?}", credential());
        assert!(!shown.contains("ghs_job_lived"), "{shown}");
        assert!(shown.contains("x-access-token"), "{shown}");
    }

    #[test]
    fn a_task_is_told_the_workspace_is_safe_unless_it_configures_git_itself() {
        let env = task_env(&[]);
        assert_eq!(
            env,
            vec![
                ("GIT_CONFIG_COUNT".to_string(), "1".to_string()),
                ("GIT_CONFIG_KEY_0".to_string(), "safe.directory".to_string()),
                ("GIT_CONFIG_VALUE_0".to_string(), "/workspace".to_string()),
            ]
        );
        let own = [dagron_core::dag::EnvVar {
            name: "GIT_CONFIG_COUNT".into(),
            value: "2".into(),
            value_from: None,
        }];
        assert!(task_env(&own).is_empty());
    }

    /// An action that asks for a credential is fetched with the one minted
    /// for its repository, sent to that repository's URL alone and to no
    /// redirect of it; each repository's once. One that asks for none, one
    /// that was minted none, and one over plain http to another host are
    /// fetched with none.
    #[test]
    fn an_action_s_credential_reaches_its_own_repository_s_fetch_alone() {
        let action = |repository: &str, git_ref: &str, credential: bool| ActionSpec {
            repository: repository.into(),
            git_ref: git_ref.into(),
            path: format!("p/{git_ref}"),
            credential,
        };
        let mut ctx = crate::executor::ExecContext::new(vec!["true".into()], None, None);
        ctx.workspace = Some(dagron_core::workspace::WorkspaceSpec {
            actions: vec![
                action("https://github.com/acme/tool.git", "v1", true),
                action("https://github.com/acme/tool.git", "v2", true),
                action("https://github.com/octo/hello.git", "v1", false),
                action("https://github.com/acme/unminted.git", "v1", true),
                action("http://mirror.example/acme/plain.git", "v1", true),
            ],
            ..Default::default()
        });
        ctx.action_credentials = vec![
            ("https://github.com/acme/tool.git".into(), credential()),
            ("http://mirror.example/acme/plain.git".into(), credential()),
            ("https://github.com/octo/hello.git".into(), credential()),
        ];
        let header = format!("Authorization: {}", credential().authorization());
        assert_eq!(
            action_headers(&ctx),
            [(
                "https://github.com/acme/tool.git".to_string(),
                header.clone()
            )]
        );
        let actions = ctx.workspace.as_ref().unwrap().actions.clone();
        let env = actions_fetch_env(&ctx, &actions, "/dagron/actions");
        let get = |key: &str| env.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        assert_eq!(get("GIT_CONFIG_COUNT"), Some("2"));
        assert_eq!(get("GIT_CONFIG_KEY_0"), Some("http.followRedirects"));
        assert_eq!(get("GIT_CONFIG_VALUE_0"), Some("false"));
        assert_eq!(
            get("GIT_CONFIG_KEY_1"),
            Some("http.https://github.com/acme/tool.git.extraHeader")
        );
        assert_eq!(get("GIT_CONFIG_VALUE_1"), Some(header.as_str()));
        assert_eq!(get("GIT_CONFIG_KEY_2"), None);
        assert_eq!(get("DAGRON_ACTION_COUNT"), Some("5"));

        // None asks: no git configuration at all.
        ctx.action_credentials.clear();
        let env = actions_fetch_env(&ctx, &actions, "/dagron/actions");
        assert!(
            env.iter().all(|(k, _)| !k.starts_with("GIT_CONFIG")),
            "{env:?}"
        );
        assert_eq!(action_secret_key(1), "dagron-action-authorization-1");
    }
}
