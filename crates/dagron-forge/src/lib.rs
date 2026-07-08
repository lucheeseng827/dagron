//! Forge feedback — post a commit status / PR check to GitHub or GitLab.
//!
//! When a workflow declares a `notify.git` block, the engine calls
//! [`ForgeClient::post_status`] on run finalization so the run's result surfaces
//! as a green/red check on the commit that triggered it. Best-effort, mirroring
//! the OpenLineage emitter: a forge being unreachable never affects run
//! execution.
//!
//! Config from env (a client is returned only if at least one token is set):
//!   `GITHUB_TOKEN` (+ optional `GITHUB_API_BASE`, default `https://api.github.com`)
//!   `GITLAB_TOKEN` (+ optional `GITLAB_API_BASE`, default `https://gitlab.com/api/v4`)

use anyhow::{Context, Result};
use serde_json::{json, Value};

const GITHUB_DEFAULT: &str = "https://api.github.com";
const GITLAB_DEFAULT: &str = "https://gitlab.com/api/v4";

/// The check outcome to publish. `Pending` is available for a future
/// run-started hook; the engine posts `Success`/`Failure` on finalize today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitState {
    Pending,
    Success,
    Failure,
}

impl CommitState {
    /// From a dagron run status string (`succeeded` → success, else failure).
    pub fn from_run_status(status: &str) -> Self {
        match status {
            "succeeded" => CommitState::Success,
            "running" | "pending" => CommitState::Pending,
            _ => CommitState::Failure, // failed, cancelled, …
        }
    }

    fn github(self) -> &'static str {
        match self {
            CommitState::Pending => "pending",
            CommitState::Success => "success",
            CommitState::Failure => "failure",
        }
    }

    fn gitlab(self) -> &'static str {
        match self {
            CommitState::Pending => "pending",
            CommitState::Success => "success",
            CommitState::Failure => "failed",
        }
    }

    fn description(self) -> &'static str {
        match self {
            CommitState::Pending => "dagron run in progress",
            CommitState::Success => "dagron run succeeded",
            CommitState::Failure => "dagron run failed",
        }
    }
}

/// A resolved `notify.git` target (templates already substituted by the caller).
#[derive(Debug, Clone)]
pub struct GitTarget {
    /// `github` or `gitlab`.
    pub provider: String,
    /// GitHub `owner/repo`, or GitLab project path/id.
    pub repo: String,
    /// Commit SHA to attach the status to.
    pub sha: String,
    /// Status context/name (GitHub `context`, GitLab `name`).
    pub context: String,
    /// Optional link back to the run in the dagron UI.
    pub target_url: Option<String>,
}

/// Posts commit statuses to whichever forge(s) have a token configured.
pub struct ForgeClient {
    http: reqwest::Client,
    github_token: Option<String>,
    github_base: String,
    gitlab_token: Option<String>,
    gitlab_base: String,
}

impl ForgeClient {
    /// Build from env. Returns `None` when no forge token is configured (feature
    /// off), so the engine skips the call entirely.
    pub fn from_env() -> Option<Self> {
        let github_token = env_nonempty("GITHUB_TOKEN");
        let gitlab_token = env_nonempty("GITLAB_TOKEN");
        if github_token.is_none() && gitlab_token.is_none() {
            return None;
        }
        Some(Self {
            // Bounded timeout so a slow/hung forge can't keep this best-effort
            // finalization call pending indefinitely.
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
            github_token,
            github_base: env_nonempty("GITHUB_API_BASE")
                .unwrap_or_else(|| GITHUB_DEFAULT.to_string()),
            gitlab_token,
            gitlab_base: env_nonempty("GITLAB_API_BASE")
                .unwrap_or_else(|| GITLAB_DEFAULT.to_string()),
        })
    }

    /// Post a commit status for `target` in `state`. Errors (unknown provider,
    /// missing token, non-2xx) are returned for the caller to log best-effort.
    pub async fn post_status(&self, target: &GitTarget, state: CommitState) -> Result<()> {
        match target.provider.to_ascii_lowercase().as_str() {
            "github" => {
                let token = self
                    .github_token
                    .as_deref()
                    .context("notify.git provider=github but GITHUB_TOKEN is unset")?;
                let (url, body) = github_request(&self.github_base, target, state);
                self.send(&url, token, "token", &body, "GitHub").await
            }
            "gitlab" => {
                let token = self
                    .gitlab_token
                    .as_deref()
                    .context("notify.git provider=gitlab but GITLAB_TOKEN is unset")?;
                let (url, body) = gitlab_request(&self.gitlab_base, target, state);
                self.send(&url, token, "gitlab", &body, "GitLab").await
            }
            other => anyhow::bail!("unknown notify.git provider '{other}' (use github or gitlab)"),
        }
    }

    async fn send(
        &self,
        url: &str,
        token: &str,
        scheme: &str,
        body: &Value,
        forge: &str,
    ) -> Result<()> {
        let req = self
            .http
            .post(url)
            .json(body)
            .header("User-Agent", "dagron");
        // GitHub uses `Authorization: token <t>`; GitLab uses a PRIVATE-TOKEN header.
        let req = if scheme == "gitlab" {
            req.header("PRIVATE-TOKEN", token)
        } else {
            req.header("Authorization", format!("token {token}"))
        };
        let resp = req
            .send()
            .await
            .with_context(|| format!("posting {forge} commit status"))?;
        if !resp.status().is_success() {
            anyhow::bail!("{forge} returned {} posting commit status", resp.status());
        }
        Ok(())
    }
}

/// GitHub Statuses API: `POST {base}/repos/{owner/repo}/statuses/{sha}`.
pub fn github_request(base: &str, t: &GitTarget, state: CommitState) -> (String, Value) {
    let url = format!(
        "{}/repos/{}/statuses/{}",
        base.trim_end_matches('/'),
        t.repo,
        t.sha
    );
    let mut body = json!({
        "state": state.github(),
        "context": t.context,
        "description": state.description(),
    });
    if let Some(u) = &t.target_url {
        body["target_url"] = json!(u);
    }
    (url, body)
}

/// GitLab Commit Status API: `POST {base}/projects/{url-encoded}/statuses/{sha}`.
pub fn gitlab_request(base: &str, t: &GitTarget, state: CommitState) -> (String, Value) {
    let url = format!(
        "{}/projects/{}/statuses/{}",
        base.trim_end_matches('/'),
        urlencode(&t.repo),
        t.sha
    );
    let mut body = json!({
        "state": state.gitlab(),
        "name": t.context,
        "description": state.description(),
    });
    if let Some(u) = &t.target_url {
        body["target_url"] = json!(u);
    }
    (url, body)
}

/// Minimal percent-encoding for a GitLab project path (`group/proj` → `group%2Fproj`).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(provider: &str) -> GitTarget {
        GitTarget {
            provider: provider.to_string(),
            repo: "acme/etl".to_string(),
            sha: "deadbeef".to_string(),
            context: "dagron/nightly".to_string(),
            target_url: Some("https://dagron.example/runs/1".to_string()),
        }
    }

    #[test]
    fn run_status_maps_to_state() {
        assert_eq!(
            CommitState::from_run_status("succeeded"),
            CommitState::Success
        );
        assert_eq!(CommitState::from_run_status("failed"), CommitState::Failure);
        assert_eq!(
            CommitState::from_run_status("cancelled"),
            CommitState::Failure
        );
        assert_eq!(
            CommitState::from_run_status("running"),
            CommitState::Pending
        );
    }

    #[test]
    fn github_request_shape() {
        let (url, body) = github_request(GITHUB_DEFAULT, &target("github"), CommitState::Success);
        assert_eq!(
            url,
            "https://api.github.com/repos/acme/etl/statuses/deadbeef"
        );
        assert_eq!(body["state"], "success");
        assert_eq!(body["context"], "dagron/nightly");
        assert_eq!(body["target_url"], "https://dagron.example/runs/1");
    }

    #[test]
    fn gitlab_request_encodes_project_and_maps_failed() {
        let (url, body) = gitlab_request(GITLAB_DEFAULT, &target("gitlab"), CommitState::Failure);
        assert_eq!(
            url,
            "https://gitlab.com/api/v4/projects/acme%2Fetl/statuses/deadbeef"
        );
        assert_eq!(body["state"], "failed"); // GitLab spells failure "failed"
        assert_eq!(body["name"], "dagron/nightly");
    }

    #[test]
    fn ghe_base_override_is_honored() {
        let (url, _) = github_request(
            "https://ghe.acme.com/api/v3",
            &target("github"),
            CommitState::Pending,
        );
        assert!(
            url.starts_with("https://ghe.acme.com/api/v3/repos/"),
            "got {url}"
        );
    }
}
