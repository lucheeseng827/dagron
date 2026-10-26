//! What a runner is set with: environment variables alone, read once as it
//! starts. A setting that cannot be read stops it there, and says which.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dagron_core::isolation::ExecutorKind;
use dagron_core::runner::Secrets;

/// Where dagron-api is.
pub const ENV_URL: &str = "DAGRON_RUNNER_URL";
/// A file holding the one-time join token, for the first start.
pub const ENV_JOIN_TOKEN_FILE: &str = "DAGRON_RUNNER_JOIN_TOKEN_FILE";
/// A file holding the runner's credential: written by the first start, or
/// given (a Kubernetes Secret, say).
pub const ENV_TOKEN_FILE: &str = "DAGRON_RUNNER_TOKEN_FILE";
/// Where the runner keeps its credential when no file is named for it.
pub const ENV_STATE_DIR: &str = "DAGRON_RUNNER_STATE_DIR";
pub const ENV_NAME: &str = "DAGRON_RUNNER_NAME";
pub const ENV_LABELS: &str = "DAGRON_RUNNER_LABELS";
pub const ENV_CAPACITY: &str = "DAGRON_RUNNER_CAPACITY";
pub const ENV_EXECUTOR: &str = "DAGRON_RUNNER_EXECUTOR";
pub const ENV_SECRETS: &str = "DAGRON_RUNNER_SECRETS";
/// `allow`: talk to dagron-api over plain `http://` on a host that is not
/// this one. The credential, and under `server` every secret, would cross the
/// network in the clear.
pub const ENV_INSECURE: &str = "DAGRON_RUNNER_INSECURE";
/// How long a draining runner waits for its tasks before it stops them.
pub const ENV_DRAIN_TIMEOUT: &str = "DAGRON_RUNNER_DRAIN_TIMEOUT_SECS";
/// Where a task that asks for an identity asks dagron-api for tokens,
/// as the task reaches it: `{DAGRON_RUNNER_URL}/oidc/token` unless set, for
/// a task whose network reaches dagron-api by another name.
pub const ENV_OIDC_REQUEST_URL: &str = "DAGRON_RUNNER_OIDC_REQUEST_URL";
/// Where a task that asks for its run's artifacts reaches dagron-api's
/// `/task/v1/artifacts`: `{DAGRON_RUNNER_URL}/task/v1/artifacts` unless set,
/// for a task whose network reaches dagron-api by another name.
pub const ENV_ARTIFACTS_URL: &str = "DAGRON_RUNNER_ARTIFACTS_URL";

/// The most tasks a runner runs at once, as dagron-api takes it.
pub const MAX_CAPACITY: u32 = 256;

#[derive(Debug, Clone)]
pub struct Config {
    pub url: String,
    pub name: Option<String>,
    /// Empty: every label the runner was registered with.
    pub labels: Vec<String>,
    pub capacity: u32,
    pub executor: ExecutorKind,
    pub secrets: Secrets,
    pub token_file: PathBuf,
    pub join_token_file: Option<PathBuf>,
    /// `None`: a draining runner waits for its tasks as long as they run.
    pub drain_timeout: Option<Duration>,
    /// dagron-api's `/oidc/token`, as a task reaches it.
    pub oidc_request_url: String,
    pub artifact_api_url: String,
}

impl Config {
    pub fn from_env() -> Result<Config> {
        Config::from_lookup(|name| std::env::var(name).ok())
    }

    /// Read from `get`, which stands in for the environment.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Config> {
        let get = |name: &str| {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let url =
            get(ENV_URL).with_context(|| format!("{ENV_URL} is not set: where dagron-api is"))?;
        let insecure = get(ENV_INSECURE).as_deref() == Some("allow");
        let url = check_url(&url, insecure)?;
        let oidc_request_url = match get(ENV_OIDC_REQUEST_URL) {
            Some(raw) => check_named(ENV_OIDC_REQUEST_URL, &raw, insecure)?,
            None => format!("{url}/oidc/token"),
        };
        let artifact_api_url = match get(ENV_ARTIFACTS_URL) {
            Some(raw) => check_named(ENV_ARTIFACTS_URL, &raw, insecure)?
                .trim_end_matches('/')
                .to_string(),
            None => format!("{url}{}", dagron_core::artifact_api::PATH),
        };
        let labels: Vec<String> = get(ENV_LABELS)
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect();
        for label in &labels {
            dagron_core::dag::validate_runner_class(label)
                .map_err(|e| anyhow::anyhow!("{ENV_LABELS}: {e}"))?;
        }
        let capacity = match get(ENV_CAPACITY) {
            None => 1,
            Some(raw) => match raw.parse::<u32>() {
                Ok(n) if (1..=MAX_CAPACITY).contains(&n) => n,
                _ => bail!("{ENV_CAPACITY} is 1 to {MAX_CAPACITY}, not '{raw}'"),
            },
        };
        let executor = match get(ENV_EXECUTOR) {
            None => ExecutorKind::Docker,
            Some(raw) => ExecutorKind::parse(&raw)
                .with_context(|| format!("{ENV_EXECUTOR} is local, docker or k8s, not '{raw}'"))?,
        };
        let secrets = match get(ENV_SECRETS) {
            None => Secrets::Server,
            Some(raw) => Secrets::parse(&raw)
                .with_context(|| format!("{ENV_SECRETS} is server or home, not '{raw}'"))?,
        };
        let state_dir = get(ENV_STATE_DIR)
            .map(PathBuf::from)
            .unwrap_or_else(default_state_dir);
        let token_file = get(ENV_TOKEN_FILE)
            .map(PathBuf::from)
            .unwrap_or_else(|| state_dir.join("credential"));
        let drain_timeout = match get(ENV_DRAIN_TIMEOUT) {
            None => None,
            Some(raw) => Some(Duration::from_secs(raw.parse::<u64>().with_context(
                || format!("{ENV_DRAIN_TIMEOUT} is a number of seconds, not '{raw}'"),
            )?)),
        };
        Ok(Config {
            url,
            name: get(ENV_NAME),
            labels,
            capacity,
            executor,
            secrets,
            token_file,
            join_token_file: get(ENV_JOIN_TOKEN_FILE).map(PathBuf::from),
            drain_timeout,
            oidc_request_url,
            artifact_api_url,
        })
    }
}

/// `~/.dagron-runner`, or `.dagron-runner` where there is no home.
fn default_state_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(|home| PathBuf::from(home).join(".dagron-runner"))
        .unwrap_or_else(|| PathBuf::from(".dagron-runner"))
}

/// dagron-api's base URL, without a trailing slash: `https://`, or `http://`
/// to this machine. Plain `http://` elsewhere only where it is allowed.
pub fn check_url(raw: &str, insecure_allowed: bool) -> Result<String> {
    check_named(ENV_URL, raw, insecure_allowed)
}

/// [`check_url`], of the setting `name`.
fn check_named(name: &str, raw: &str, insecure_allowed: bool) -> Result<String> {
    let url = reqwest::Url::parse(raw).with_context(|| format!("{name} is not a URL: '{raw}'"))?;
    match url.scheme() {
        "https" => {}
        "http" => {
            let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
            if !local && !insecure_allowed {
                bail!(
                    "{name} is http:// to another host, which would carry the runner's \
                     credential, and its tasks' secrets, in the clear: use https://, or set \
                     {ENV_INSECURE}=allow"
                );
            }
        }
        other => bail!("{name} is https:// (or http:// to this machine), not {other}://"),
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("{name} has no query or fragment: '{raw}'");
    }
    Ok(raw.trim_end_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(vars: &[(&str, &str)]) -> Result<Config> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|name| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone()))
    }

    #[test]
    fn a_runner_needs_where_dagron_api_is_and_defaults_the_rest() {
        assert!(read(&[]).unwrap_err().to_string().contains(ENV_URL));
        let c = read(&[
            (ENV_URL, "https://dagron.example.com/"),
            (ENV_STATE_DIR, "/var/lib/r"),
        ])
        .unwrap();
        assert_eq!(c.url, "https://dagron.example.com");
        assert_eq!(
            (c.capacity, c.executor, c.secrets),
            (1, ExecutorKind::Docker, Secrets::Server)
        );
        assert!(c.labels.is_empty());
        assert_eq!(c.token_file, PathBuf::from("/var/lib/r/credential"));
        assert!(c.drain_timeout.is_none());
        assert_eq!(c.oidc_request_url, "https://dagron.example.com/oidc/token");
        let c = read(&[
            (ENV_URL, "https://dagron.example.com/"),
            (ENV_OIDC_REQUEST_URL, "https://dagron.internal/oidc/token"),
        ])
        .unwrap();
        assert_eq!(c.oidc_request_url, "https://dagron.internal/oidc/token");
        assert_eq!(
            c.artifact_api_url,
            "https://dagron.example.com/task/v1/artifacts"
        );
        let c = read(&[
            (ENV_URL, "https://dagron.example.com/"),
            (
                ENV_ARTIFACTS_URL,
                "https://dagron.internal/task/v1/artifacts/",
            ),
        ])
        .unwrap();
        assert_eq!(
            c.artifact_api_url,
            "https://dagron.internal/task/v1/artifacts"
        );
        let e = read(&[
            (ENV_URL, "https://dagron.example.com/"),
            (ENV_OIDC_REQUEST_URL, "http://dagron.internal/oidc/token"),
        ])
        .unwrap_err()
        .to_string();
        assert!(e.contains(ENV_OIDC_REQUEST_URL), "{e}");
    }

    #[test]
    fn each_setting_is_read_or_refused_by_name() {
        let c = read(&[
            (ENV_URL, "https://d.example"),
            (ENV_LABELS, "gpu, mac-arm64"),
            (ENV_CAPACITY, "4"),
            (ENV_EXECUTOR, "local"),
            (ENV_SECRETS, "home"),
            (ENV_TOKEN_FILE, "/run/secrets/dagron"),
            (ENV_DRAIN_TIMEOUT, "600"),
        ])
        .unwrap();
        assert_eq!(c.labels, ["gpu", "mac-arm64"]);
        assert_eq!(
            (c.capacity, c.executor, c.secrets),
            (4, ExecutorKind::Local, Secrets::Home)
        );
        assert_eq!(c.token_file, PathBuf::from("/run/secrets/dagron"));
        assert_eq!(c.drain_timeout, Some(Duration::from_secs(600)));
        for (name, bad) in [
            (ENV_CAPACITY, "0"),
            (ENV_CAPACITY, "999"),
            (ENV_EXECUTOR, "podman"),
            (ENV_SECRETS, "cloud"),
            (ENV_LABELS, "Has Space"),
        ] {
            let e = read(&[(ENV_URL, "https://d.example"), (name, bad)])
                .unwrap_err()
                .to_string();
            assert!(e.contains(name), "{name}={bad}: {e}");
        }
    }

    #[test]
    fn plain_http_is_for_this_machine_unless_allowed() {
        assert!(check_url("http://127.0.0.1:8080", false).is_ok());
        assert!(check_url("http://localhost:8080/", false).is_ok());
        assert!(check_url("http://[::1]:8080", false).is_ok());
        let e = check_url("http://dagron.internal:8080", false)
            .unwrap_err()
            .to_string();
        assert!(e.contains(ENV_INSECURE), "{e}");
        assert!(check_url("http://dagron.internal:8080", true).is_ok());
        assert!(check_url("ftp://dagron.internal", true).is_err());
        assert!(check_url("https://d.example/?x=1", false).is_err());
        // A path prefix is kept: an ingress may serve dagron-api under one.
        assert_eq!(
            check_url("https://d.example/dagron/", false).unwrap(),
            "https://d.example/dagron"
        );
    }
}
