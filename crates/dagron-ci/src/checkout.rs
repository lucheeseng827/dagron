//! Checkout credentials, minted for one attempt of one job: what an engine
//! asks for before a task whose workspace has a checkout runs
//! (`POST /ci/v1/checkout-tokens`, its DAGRON_CHECKOUT_TOKEN_URL), and tells
//! dagron-ci to revoke once the attempt is over (`DELETE`, the same path).
//!
//! **Bound to the run.** The engine presents the secret both were given
//! (DAGRON_CHECKOUT_TOKEN_SECRET_FILE there) and names a run, a repository
//! and a commit. dagron-ci mints only for a run it submitted and that is not
//! over, and only for that run's own repository (`https://<forge
//! host>/<owner>/<name>.git`, as the compiler writes it) and commit: the
//! secret cannot be turned into a credential for anything else the App may
//! read.
//!
//! **An action's fetch** (`"action": true`) is the one other: for a trusted
//! run alone, a repository of the run's repository's own owner's on its
//! forge (`https://<host>/<owner>/<name>.git`), to read it and nothing else,
//! as GitHub shares a private action within its owner; `commit` is the ref
//! it is fetched at, and is what its row records.
//!
//! **A run it has only just submitted** may be asked for before it is on
//! record: dagron-api creates the run, and its engine can start the run's
//! first task, before dagron-ci has read the answer that names it. A run that
//! is not on record is therefore looked for again, for [`RECORD_WAIT`], before
//! the engine is told it is not dagron-ci's.
//!
//! **A run that is not dagron-ci's.** An engine that names dagron-ci as its
//! token service asks it about every checkout, a workflow's that has nothing
//! to do with CI included. Told "not one dagron-ci submitted" (404), the
//! engine failed that task, so naming dagron-ci made every checkout on the
//! engine CI's. What it is told now turns on whose repository it is:
//! - **one dagron-ci does not serve:** `204`, no credential. The run is none
//!   of dagron-ci's business, and its commit is fetched as an engine with no
//!   token service fetches it, which is all a public repository needs;
//! - **one dagron-ci serves:** refused as before (404). The runs of a
//!   connected repository are dagron-ci's to submit: one that is not on its
//!   record has no checks and no count, and is not waved through.
//!
//! Either answer is given once the run has been looked for, as above. Neither
//! hands anything out: a `204` carries no credential.
//!
//! **A run that runs again.** A rerun, a retry or a clear starts a job of a
//! run dagron-ci has on record as over. Before it answers that the run is
//! over, dagron-ci asks the run's cell itself whether it is running
//! (`rerun.rs`). If it is, the record re-opens and the credential is minted
//! as for any live run; if it is not, or the cell cannot be asked, there is
//! none. The request is the occasion to look, never the evidence.
//!
//! **Bound to the cell.** A secret is one cell's (a file in
//! CI_CHECKOUT_TOKEN_SECRETS_DIR, named as the cell), or every cell's
//! (CI_CHECKOUT_TOKEN_SECRET_FILE). An engine holding one cell's secret is
//! answered for the runs dagron-ci submitted to that cell alone, so where
//! cells are tenants, one tenant's engine cannot mint for another's run.
//!
//! **On record.** Each credential has a row (`ci_checkout_credentials`) that
//! names the run, the task and the attempt it was minted for, its repository
//! and commit, the cell that asked, and when the forge says it lapses. The
//! row is written before the credential is handed over: no row, no
//! credential, and one that could not be recorded is revoked instead. A
//! revoke marks the row with when. The row holds the credential's SHA-256,
//! which is how the revoke finds it, and never the credential.
//!
//! **Minted by a GitHub App** (CI_GITHUB_APP_ID, CI_GITHUB_APP_KEY_FILE): an
//! installation token for the one repository with `contents: read`, which
//! GitHub expires within the hour and dagron-ci revokes when told the attempt
//! is over. Without an App, for a repository not on github.com, or one the App
//! is not installed on, the answer is `204`: the commit is fetched without a
//! credential, which is all a public repository needs.
//!
//! **Minted on GitLab** with the token CI_GITLAB_TOKENS_DIR keeps for the
//! project's host: a project access token of the one project, its role
//! Reporter and its scope `read_repository`, which GitLab expires by the
//! day (dagron-ci asks for two days on) and dagron-ci revokes by its id
//! when told the attempt is over, with the host's token; the row keeps the
//! id. The host's token itself goes to no job. Without one for the host, or
//! where it may not mint (it must be a personal access token, a user's or a
//! service account's, that is the project's Maintainer: GitLab lets no
//! group or project access token mint another), the answer is
//! `204`. An action's credential and a job's forge token are a GitHub App's
//! alone.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::db::{CheckoutCredential, RunCommit};
use crate::metrics::{CheckoutOutcome, Reopened};
use crate::problem::problem;
use crate::server::AppState;

pub const PATH: &str = "/ci/v1/checkout-tokens";

/// The largest request: a repository, a commit and a few ids.
pub const MAX_BODY: usize = 8 * 1024;

/// How long an answer may take: each read of the run, the looks for one that
/// is not on record yet, and both of a mint's calls to GitHub, together. A credential is on
/// the path of a job's start, and the engine waits 10 s for one. Less than
/// that, so that a slow GitHub is reported as such rather than taken for
/// dagron-ci not answering.
const MINT_BUDGET: Duration = Duration::from_secs(8);

/// How long a run that is not on record is looked for before the engine is
/// told it is not one dagron-ci submitted. The run exists in the engine from
/// the moment dagron-api creates it, and dagron-ci records it only once it
/// has that answer: an engine quick to start the run's first task asks in
/// between, and a refusal there fails a job nothing was wrong with. Out of
/// [`MINT_BUDGET`], and well past the write it waits on.
const RECORD_WAIT: Duration = Duration::from_secs(2);

/// How long the write of a credential's row may take, and the mark on it
/// when it is revoked. The first comes after the mint, which may have used
/// the whole of [`MINT_BUDGET`], and the two are still inside the 10 s an
/// engine waits. The second is bookkeeping, after the forge has revoked the
/// credential, and holds the engine's request no longer than this.
const RECORD_WRITE: Duration = Duration::from_secs(1);

/// The pause before the second look for such a run; each later one is twice
/// the last.
const RECORD_POLL: Duration = Duration::from_millis(25);

/// How long one call to GitHub may take.
const GITHUB_TIMEOUT: Duration = Duration::from_secs(8);

/// How many times a mint's call is sent where GitHub failed it on its side,
/// and the pause before the second send; each later one is twice the last.
/// github.com answers an installation token's POST with an empty 500 now and
/// then, and the next one is minted: two in eleven on a quiet afternoon,
/// each a job failed at its checkout. Out of [`MINT_BUDGET`], which still
/// cuts the whole short.
pub(crate) const MINT_SENDS: u32 = 3;
const MINT_RESEND: Duration = Duration::from_millis(250);

const GITHUB_API_VERSION: &str = "2022-11-28";

/// The user a GitHub installation token is presented as, over HTTP basic.
pub const GITHUB_TOKEN_USER: &str = "x-access-token";

/// A self-registered GitHub App, as CI_GITHUB_APP_ID names it, with its
/// private key (CI_GITHUB_APP_KEY_FILE) or with the service that holds the key
/// for it (CI_GITHUB_APP_TOKEN_URL).
#[derive(Clone)]
pub struct GitHubApp {
    id: u64,
    keeper: Keeper,
}

/// Who holds an App's private key, and so who asks GitHub for its
/// installation tokens.
#[derive(Clone)]
enum Keeper {
    /// dagron-ci: it signs the App's token and asks GitHub itself.
    Key(EncodingKey),
    /// A token service: dagron-ci asks it, and never holds the key. One key
    /// mints for every installation of its App, so where an App serves many
    /// owners its key is kept in one place, which says whose installations
    /// each caller may have tokens for.
    Service(TokenService),
}

/// The service an App's installation tokens are asked of
/// (CI_GITHUB_APP_TOKEN_URL), and the secret dagron-ci asks it under.
#[derive(Clone)]
struct TokenService {
    url: String,
    secret: String,
    /// A client for `url`: by no proxy when it is plain http, as
    /// [`crate::config::Clients`] has it.
    http: reqwest::Client,
}

/// Who an installation token is asked of, in what an error says.
const SERVICE: &str = "the App's token service";

impl std::fmt::Debug for GitHubApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubApp")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl GitHubApp {
    /// The App `id`, whose private key is `pem`: refused when it is not an
    /// RSA key, so that a bad one stops dagron-ci at start.
    pub fn new(id: u64, pem: &[u8]) -> anyhow::Result<GitHubApp> {
        let key = EncodingKey::from_rsa_pem(pem)
            .map_err(|e| anyhow::anyhow!("not an RSA private key in PEM: {e}"))?;
        Ok(GitHubApp {
            id,
            keeper: Keeper::Key(key),
        })
    }

    /// The App `id`, whose key a token service holds: its installation
    /// tokens are asked of `url`, under `secret`.
    pub fn with_token_service(id: u64, url: String, secret: String) -> GitHubApp {
        let http = crate::config::Clients::new(|| {
            reqwest::Client::builder()
                .timeout(GITHUB_TIMEOUT)
                .redirect(reqwest::redirect::Policy::none())
        })
        .to(&url)
        .clone();
        GitHubApp {
            id,
            keeper: Keeper::Service(TokenService { url, secret, http }),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Who holds the App's key, as the configuration says it: `here`, or the
    /// token service's URL. Never the key, nor the secret.
    pub fn keeper(&self) -> &str {
        match &self.keeper {
            Keeper::Key(_) => "here",
            Keeper::Service(service) => &service.url,
        }
    }

    /// The App's own token, which it asks GitHub for installation tokens
    /// with: a JWT it signs, dated a minute back for a clock ahead of
    /// GitHub's, and good for nine minutes more (GitHub takes ten at most).
    pub fn jwt(&self, now: u64) -> anyhow::Result<String> {
        #[derive(Serialize)]
        struct Claims {
            iat: u64,
            exp: u64,
            iss: String,
        }
        let key = match &self.keeper {
            Keeper::Key(key) => key,
            Keeper::Service(_) => anyhow::bail!("the App's key is held by its token service"),
        };
        let claims = Claims {
            iat: now.saturating_sub(60),
            exp: now + 540,
            iss: self.id.to_string(),
        };
        jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, key)
            .map_err(|e| anyhow::anyhow!("signing the App's token: {e}"))
    }
}

impl TokenService {
    /// An installation token for `full_name`, asked of the service: the
    /// request GitHub's own would be, by repository. A 404 is an App that is
    /// not installed on it, or not for this caller; the service's other
    /// answers are read as GitHub's are ([`is_refusal`]).
    async fn mint(
        &self,
        full_name: &str,
        permissions: serde_json::Value,
        one_repository: bool,
    ) -> Result<Minted, MintError> {
        let res = self
            .http
            .post(&self.url)
            .bearer_auth(&self.secret)
            .json(&serde_json::json!({
                "repository": full_name,
                "permissions": permissions,
                "one_repository": one_repository,
            }))
            .send()
            .await
            .map_err(|e| MintError::Failed(format!("{SERVICE} could not be reached: {e}")))?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(MintError::NotInstalled);
        }
        answer(res, SERVICE, "an installation token").await
    }
}

/// What the endpoint holds: the engines' secrets, the App, and where
/// github.com's API is.
pub struct Checkouts {
    /// Each secret an engine may present, as its digest, with the one cell
    /// it answers for: `None` for every cell.
    secrets: Vec<(Option<String>, [u8; 32])>,
    app: Option<GitHubApp>,
    /// github.com's API when not at its own address (CI_GITHUB_API_URL).
    github_api: Option<String>,
    /// CI_GITLAB_TOKENS_DIR's tokens, by host: what a project access token
    /// is minted and revoked with.
    gitlab_tokens: BTreeMap<String, String>,
    /// Where every GitLab host's API answers instead: a test's stub.
    gitlab_origin: Option<String>,
    http: crate::config::Clients,
    /// [`MINT_BUDGET`], or a test's.
    budget: Duration,
    /// [`RECORD_WAIT`], or a test's.
    record_wait: Duration,
}

/// Whose secret an engine presented.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Engine {
    /// The one every cell's engines may present.
    AnyCell,
    /// One cell's own: it is answered for that cell's runs alone.
    Cell(String),
}

impl Checkouts {
    /// The endpoint, answering engines that present `secret` for a run on
    /// any cell; [`Checkouts::with_cell_secrets`] adds each cell's own.
    pub fn new(
        secret: Option<String>,
        app: Option<GitHubApp>,
        github_api: Option<String>,
    ) -> Checkouts {
        Checkouts {
            secrets: secret.iter().map(|secret| (None, digest(secret))).collect(),
            app,
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(GITHUB_TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            github_api,
            gitlab_tokens: BTreeMap::new(),
            gitlab_origin: None,
            budget: MINT_BUDGET,
            record_wait: RECORD_WAIT,
        }
    }

    /// Answer an engine presenting a cell's secret for that cell's runs.
    pub fn with_cell_secrets(mut self, secrets: &BTreeMap<String, String>) -> Checkouts {
        self.secrets.extend(
            secrets
                .iter()
                .map(|(cell, secret)| (Some(cell.clone()), digest(secret))),
        );
        self
    }

    /// Mint a GitLab project's checkouts with the token `tokens` keeps for
    /// its host (CI_GITLAB_TOKENS_DIR).
    pub fn with_gitlab_tokens(mut self, tokens: &BTreeMap<String, String>) -> Checkouts {
        self.gitlab_tokens = tokens
            .iter()
            .map(|(host, token)| (host.to_ascii_lowercase(), token.clone()))
            .collect();
        self
    }

    #[cfg(test)]
    pub(crate) fn with_gitlab_origin(mut self, origin: &str) -> Checkouts {
        self.gitlab_origin = Some(origin.to_string());
        self
    }

    #[cfg(test)]
    fn with_budget(mut self, budget: Duration) -> Checkouts {
        self.budget = budget;
        self
    }

    #[cfg(test)]
    fn with_record_wait(mut self, record_wait: Duration) -> Checkouts {
        self.record_wait = record_wait;
        self
    }

    /// Whose secret `headers` carry, if any. Compared as digests, and with
    /// every secret, so how long it takes says nothing of any of them.
    fn engine(&self, headers: &HeaderMap) -> Option<Engine> {
        let given = digest(crate::api::bearer(headers)?);
        let mut found = None;
        for (cell, secret) in &self.secrets {
            if *secret == given && found.is_none() {
                found = Some(match cell {
                    Some(cell) => Engine::Cell(cell.clone()),
                    None => Engine::AnyCell,
                });
            }
        }
        found
    }

    fn api(&self) -> &str {
        self.github_api
            .as_deref()
            .unwrap_or("https://api.github.com")
    }

    /// An installation token for `full_name` alone: `contents: read` for a
    /// checkout's, or the permissions a job's forge token was given.
    async fn installation_token(
        &self,
        app: &GitHubApp,
        full_name: &str,
        permissions: Option<&BTreeMap<String, String>>,
    ) -> Result<Minted, MintError> {
        let permissions = match permissions {
            Some(given) => serde_json::json!(given),
            None => serde_json::json!({"contents": "read"}),
        };
        installation_token(
            self.http.to(self.api()),
            self.api(),
            app,
            full_name,
            permissions,
        )
        .await
    }

    async fn revoke(&self, token: &str) -> Result<(), String> {
        let res = self
            .github(reqwest::Method::DELETE, "/installation/token", token)
            .send()
            .await
            .map_err(|e| format!("GitHub could not be reached: {e}"))?;
        // Gone already (expired, or revoked twice) is as good as revoked.
        match res.status().as_u16() {
            200..=299 | 401 | 404 => Ok(()),
            status => Err(format!(
                "GitHub answered {status} to the revoke: {}",
                said(res).await
            )),
        }
    }

    /// The token the operator keeps for the GitLab host `host`.
    fn gitlab_token(&self, host: &str) -> Option<&str> {
        self.gitlab_tokens
            .get(&host.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// `{api}/projects/{project}/access_tokens[/{id}]` on `host`.
    fn access_tokens_url(
        &self,
        host: &str,
        project: &str,
        id: Option<&str>,
    ) -> Result<String, String> {
        let api = match &self.gitlab_origin {
            Some(origin) => origin.clone(),
            None => format!("https://{host}{}", crate::forge::gitlab::API_PATH),
        };
        let mut url = reqwest::Url::parse(&api).map_err(|e| format!("{api}: {e}"))?;
        url.path_segments_mut()
            .map_err(|()| format!("{api} takes no path"))?
            .pop_if_empty()
            .push("projects")
            .push(project)
            .push("access_tokens")
            .extend(id);
        Ok(url.to_string())
    }

    /// A project access token of `repo` alone, to read it: Reporter, with
    /// `read_repository`, lapsing two days on (GitLab dates it by the day),
    /// and revoked once the attempt is over.
    async fn project_token(
        &self,
        repo: &crate::db::Repo,
        run_id: &str,
    ) -> Result<Minted, MintError> {
        let Some(token) = self.gitlab_token(&repo.forge_host) else {
            return Err(MintError::NotInstalled);
        };
        if !crate::forge::is_url_safe(repo) {
            return Err(MintError::Refused(format!(
                "{}/{} cannot be put in a URL as it is",
                repo.forge_host, repo.full_name
            )));
        }
        let url = self
            .access_tokens_url(&repo.forge_host, &repo.full_name, None)
            .map_err(MintError::Failed)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let (y, m, d) = crate::cron::civil((now / 86_400) as i64 + 2);
        let name: String = format!("dagron-ci checkout {run_id}")
            .chars()
            .take(255)
            .collect();
        let res = self
            .http
            .to(&url)
            .post(&url)
            .bearer_auth(token)
            .timeout(GITHUB_TIMEOUT)
            .json(&serde_json::json!({
                "name": name,
                "scopes": ["read_repository"],
                "access_level": 20,
                "expires_at": format!("{y:04}-{m:02}-{d:02}"),
            }))
            .send()
            .await
            .map_err(|e| MintError::Failed(format!("GitLab could not be reached: {e}")))?;
        let status = res.status().as_u16();
        match status {
            200 | 201 => {}
            // The host's token may not mint one here: it is a group's or a
            // project's access token, which mints none, or is not the
            // project's Maintainer, or cannot see the project.
            401 | 403 | 404 => return Err(MintError::NotInstalled),
            400..=428 | 430..=499 => {
                return Err(MintError::Refused(format!(
                    "GitLab answered {status} to the mint: {}",
                    said(res).await
                )))
            }
            _ => {
                return Err(MintError::Failed(format!(
                    "GitLab answered {status} to the mint: {}",
                    said(res).await
                )))
            }
        }
        #[derive(Deserialize)]
        struct ProjectToken {
            id: u64,
            token: String,
            expires_at: Option<String>,
        }
        let body = res
            .bytes()
            .await
            .map_err(|e| MintError::Failed(format!("GitLab's answer could not be read: {e}")))?;
        let minted: ProjectToken = serde_json::from_slice(&body)
            .map_err(|e| MintError::Failed(format!("GitLab's answer is not a token: {e}")))?;
        Ok(Minted {
            token: minted.token,
            expires_at: minted.expires_at,
            forge_token_id: Some(minted.id.to_string()),
        })
    }

    /// Revoke the project access token `id` of the project `repository`
    /// names, with the token the operator keeps for its host. One gone
    /// already is as good as revoked.
    async fn revoke_project_token(&self, repository: &str, id: &str) -> Result<(), String> {
        let Some((host, project)) = repository_name(repository) else {
            return Err(format!("{repository} names no GitLab project"));
        };
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("{id:?} is not a GitLab token's id"));
        }
        let Some(token) = self.gitlab_token(&host) else {
            return Err(format!(
                "CI_GITLAB_TOKENS_DIR keeps no token for {host} to revoke it with"
            ));
        };
        let url = self.access_tokens_url(&host, &project, Some(id))?;
        let res = self
            .http
            .to(&url)
            .delete(&url)
            .bearer_auth(token)
            .timeout(GITHUB_TIMEOUT)
            .send()
            .await
            .map_err(|e| format!("GitLab could not be reached: {e}"))?;
        match res.status().as_u16() {
            200..=299 | 404 => Ok(()),
            status => Err(format!(
                "GitLab answered {status} to the revoke: {}",
                said(res).await
            )),
        }
    }

    /// Revoke a credential that was minted and not handed over: by its id
    /// on GitLab, as itself on GitHub.
    async fn revoke_minted(
        &self,
        repository: &str,
        token: &str,
        id: Option<&str>,
    ) -> Result<(), String> {
        match id {
            Some(id) => self.revoke_project_token(repository, id).await,
            None => self.revoke(token).await,
        }
    }

    fn github(&self, method: reqwest::Method, path: &str, bearer: &str) -> reqwest::RequestBuilder {
        github(self.http.to(self.api()), self.api(), method, path, bearer)
    }
}

/// A request to GitHub's API at `api`, as `bearer`.
fn github(
    http: &reqwest::Client,
    api: &str,
    method: reqwest::Method,
    path: &str,
    bearer: &str,
) -> reqwest::RequestBuilder {
    http.request(method, format!("{api}{path}"))
        .bearer_auth(bearer)
        .header("accept", "application/vnd.github+json")
        .header("x-github-api-version", GITHUB_API_VERSION)
}

/// An installation token of `app` for the repository `full_name` alone, with
/// `permissions` and no other: GitHub refuses it for any other repository,
/// and expires it within the hour. [`MintError::NotInstalled`] where the App
/// has no installation that covers the repository.
///
/// The checkout's credential is one (`contents: read`), and so is the token
/// dagron-ci makes its own calls for a repository with
/// ([`crate::forge::Forge`]).
pub(crate) async fn installation_token(
    http: &reqwest::Client,
    api: &str,
    app: &GitHubApp,
    full_name: &str,
    permissions: serde_json::Value,
) -> Result<Minted, MintError> {
    installation_token_of(http, api, app, full_name, permissions, true).await
}

/// [`installation_token`], for the repository alone where `one_repository`
/// says, and otherwise for the installation that covers it: a token with an
/// organization's permission and none of a repository's, such as the one an
/// organization's runners are registered with.
pub(crate) async fn installation_token_of(
    http: &reqwest::Client,
    api: &str,
    app: &GitHubApp,
    full_name: &str,
    permissions: serde_json::Value,
    one_repository: bool,
) -> Result<Minted, MintError> {
    let (owner, name) = full_name
        .split_once('/')
        .ok_or_else(|| MintError::Refused(format!("{full_name:?} is not owner/name")))?;
    if let Keeper::Service(service) = &app.keeper {
        return service.mint(full_name, permissions, one_repository).await;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let jwt = app
        .jwt(now)
        .map_err(|e| MintError::Refused(e.to_string()))?;

    #[derive(Deserialize)]
    struct Installation {
        id: u64,
    }
    let res = resent(|| {
        github(
            http,
            api,
            reqwest::Method::GET,
            &format!("/repos/{owner}/{name}/installation"),
            &jwt,
        )
    })
    .await
    .map_err(|e| MintError::Failed(format!("GitHub could not be reached: {e}")))?;
    if res.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(MintError::NotInstalled);
    }
    let installation: Installation = read(res, "the App's installation").await?;

    let body = match one_repository {
        true => serde_json::json!({
            "repositories": [name],
            "permissions": permissions,
        }),
        false => serde_json::json!({ "permissions": permissions }),
    };
    // A 500 may still have minted a token, which no one is handed: it is the
    // installation's, for this repository and these permissions alone, and
    // expires within the hour.
    let res = resent(|| {
        github(
            http,
            api,
            reqwest::Method::POST,
            &format!("/app/installations/{}/access_tokens", installation.id),
            &jwt,
        )
        .json(&body)
    })
    .await
    .map_err(|e| MintError::Failed(format!("GitHub could not be reached: {e}")))?;
    read(res, "an installation token").await
}

/// `request`'s answer, sent again where GitHub failed it on its side: a 5xx,
/// or a connection it did not take ([`MINT_SENDS`]). Not one that timed out,
/// which has had its time; and not a refusal or a rate limit, which the next
/// send is answered the same.
async fn resent(
    request: impl Fn() -> reqwest::RequestBuilder,
) -> reqwest::Result<reqwest::Response> {
    let mut pause = MINT_RESEND;
    for _ in 1..MINT_SENDS {
        match request().send().await {
            Ok(res) if res.status().is_server_error() => {
                tracing::debug!(status = %res.status(), "GitHub failed a mint's call: sent again");
            }
            Err(e) if e.is_connect() => {
                tracing::debug!(error = %e, "GitHub did not take a mint's call: sent again");
            }
            answered => return answered,
        }
        tokio::time::sleep(pause).await;
        pause *= 2;
    }
    request().send().await
}

/// Whether a repository shares its actions with workflows of its owner's
/// other repositories, as GitHub decides whether a workflow may use one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Sharing {
    /// A public repository's: anyone's workflow may use them, and they are
    /// fetched with no credential.
    Public,
    /// A private repository whose Actions access is `user`, `organization`
    /// or `enterprise`.
    Shared,
    /// A private repository that shares none (`none`, GitHub's default), or
    /// one the App may not ask: why.
    Not(String),
}

/// Whether `full_name` shares its actions ([`Sharing`]), asked of GitHub
/// with a token of `app`'s for that repository alone, which reads its
/// metadata and its administration, and is revoked once it is answered: its
/// visibility (`GET /repos/{owner}/{name}`), and a private one's Actions
/// access (`GET …/actions/permissions/access`). Whatever does not say it is
/// shared is taken for one that shares none.
pub(crate) async fn shares_actions(
    http: &reqwest::Client,
    api: &str,
    app: &GitHubApp,
    full_name: &str,
) -> Result<Sharing, MintError> {
    let permissions = serde_json::json!({"metadata": "read", "administration": "read"});
    let minted = match installation_token(http, api, app, full_name, permissions).await {
        Ok(minted) => minted,
        Err(MintError::NotInstalled) => {
            return Ok(Sharing::Not(format!(
                "the GitHub App is not installed on {full_name}"
            )))
        }
        Err(MintError::Refused(why)) => {
            return Ok(Sharing::Not(format!(
                "the GitHub App may not read {full_name}'s administration ({why}); give it \
                 Administration: read"
            )))
        }
        Err(e) => return Err(e),
    };
    let asked = async {
        let res = github(
            http,
            api,
            reqwest::Method::GET,
            &format!("/repos/{full_name}"),
            &minted.token,
        )
        .send()
        .await
        .map_err(|e| MintError::Failed(format!("GitHub could not be reached: {e}")))?;
        #[derive(Deserialize)]
        struct Visibility {
            private: bool,
        }
        if !res.status().is_success() {
            return Ok(Sharing::Not(format!(
                "GitHub answered {} for {full_name}",
                res.status().as_u16()
            )));
        }
        let visibility: Visibility = read(res, "the repository").await?;
        if !visibility.private {
            return Ok(Sharing::Public);
        }
        let res = github(
            http,
            api,
            reqwest::Method::GET,
            &format!("/repos/{full_name}/actions/permissions/access"),
            &minted.token,
        )
        .send()
        .await
        .map_err(|e| MintError::Failed(format!("GitHub could not be reached: {e}")))?;
        if !res.status().is_success() {
            return Ok(Sharing::Not(format!(
                "GitHub answered {} for {full_name}'s Actions access; give the App \
                 Administration: read",
                res.status().as_u16()
            )));
        }
        #[derive(Deserialize)]
        struct Access {
            access_level: String,
        }
        let access: Access = read(res, "the repository's Actions access").await?;
        Ok(match access.access_level.as_str() {
            "user" | "organization" | "enterprise" => Sharing::Shared,
            level => Sharing::Not(format!(
                "{full_name}'s Actions access is {level:?}: its settings share its actions \
                 with none of its owner's other repositories"
            )),
        })
    }
    .await;
    // Asked; the token goes, answered or not.
    let _ = github(
        http,
        api,
        reqwest::Method::DELETE,
        "/installation/token",
        &minted.token,
    )
    .send()
    .await;
    asked
}

#[derive(Debug, Deserialize)]
pub(crate) struct Minted {
    pub(crate) token: String,
    #[serde(default)]
    pub(crate) expires_at: Option<String>,
    /// The forge's own id, for a credential it revokes by id: a GitLab
    /// project access token's. GitHub's tokens revoke themselves.
    #[serde(skip)]
    pub(crate) forge_token_id: Option<String>,
}

#[derive(Debug)]
pub(crate) enum MintError {
    /// The App has no installation that covers the repository.
    NotInstalled,
    /// GitHub answered, and will not mint this token: it does not know the
    /// App's key, the installation is gone or suspended, or the App was not
    /// given what the token asks for. The same request gets the same answer.
    Refused(String),
    /// Nothing to go on: GitHub was not reached, failed, or is limiting
    /// requests. The next request may be answered.
    Failed(String),
}

async fn read<T: serde::de::DeserializeOwned>(
    res: reqwest::Response,
    what: &str,
) -> Result<T, MintError> {
    answer(res, "GitHub", what).await
}

/// [`read`], for an answer from `who`.
async fn answer<T: serde::de::DeserializeOwned>(
    res: reqwest::Response,
    who: &str,
    what: &str,
) -> Result<T, MintError> {
    let status = res.status();
    if !status.is_success() {
        let headers = res.headers().clone();
        let said = said(res).await;
        let why = format!("{who} answered {status} for {what}: {said}");
        return Err(match is_refusal(status.as_u16(), &headers, &said) {
            true => MintError::Refused(why),
            false => MintError::Failed(why),
        });
    }
    res.json()
        .await
        .map_err(|e| MintError::Failed(format!("{who}'s answer for {what} did not read: {e}")))
}

/// Whether an answer that is not a success is GitHub's last word on a mint:
/// a key it does not know (401), an installation that is gone (404), or
/// permissions the App was not given (422). A 403 is one too (the
/// installation is suspended), unless it is a rate limit, which its headers
/// say, or its words alone. Anything else may be answered next time.
fn is_refusal(status: u16, headers: &reqwest::header::HeaderMap, said: &str) -> bool {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
    };
    match status {
        401 | 404 | 422 => true,
        403 => {
            header("retry-after").is_none()
                && header("x-ratelimit-remaining") != Some("0")
                && !said.to_ascii_lowercase().contains("rate limit")
        }
        _ => false,
    }
}

/// What GitHub said, cut to a line that fits an error.
async fn said(res: reqwest::Response) -> String {
    let text = res.text().await.unwrap_or_default();
    text.trim().chars().take(200).collect()
}

#[derive(Deserialize)]
struct MintRequest {
    repository: String,
    commit: String,
    run_id: String,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    attempt: Option<i64>,
    /// For a forge token a job holds (its `GITHUB_TOKEN`), what it asks
    /// to do; absent for a checkout's credential.
    #[serde(default)]
    permissions: Option<BTreeMap<String, String>>,
    /// For the fetch of an action a job's steps use: `repository` is the
    /// action's, and `commit` the ref it is fetched at.
    #[serde(default)]
    action: bool,
    /// For a patched checkout: the link to the run's patch
    /// bundle, `{patch_url}`, rather than a credential.
    #[serde(default)]
    patch: bool,
}

/// The permissions a job's forge token may be asked for: a GitHub App's
/// names for a repository's, as dagron-core's `forge_token` takes them.
pub const FORGE_TOKEN_PERMISSIONS: &[&str] = &[
    "actions",
    "checks",
    "contents",
    "deployments",
    "discussions",
    "issues",
    "metadata",
    "pages",
    "pull_requests",
    "security_events",
    "statuses",
];

/// What a job's forge token is given of what it `asked` for: each permission
/// at what it asked, and `metadata: read`, which GitHub gives every token;
/// for a run that is not trusted, read alone, as GitHub gives a fork's pull
/// request a token that reads. Or why the ask is refused.
pub fn forge_permissions(
    asked: &BTreeMap<String, String>,
    trust: &str,
) -> Result<BTreeMap<String, String>, String> {
    if asked.is_empty() {
        return Err("a forge token asks for no permissions".into());
    }
    let mut given = BTreeMap::new();
    for (name, level) in asked {
        if !FORGE_TOKEN_PERMISSIONS.contains(&name.as_str()) {
            return Err(format!(
                "a forge token may not be asked for {name:?}: the permissions are {}",
                FORGE_TOKEN_PERMISSIONS.join(", ")
            ));
        }
        let level = match level.as_str() {
            "read" => "read",
            "write" if trust == "trusted" => "write",
            "write" => "read",
            other => return Err(format!("{name} is asked at {other:?}: read or write")),
        };
        given.insert(name.clone(), level.to_string());
    }
    given
        .entry("metadata".to_string())
        .or_insert_with(|| "read".to_string());
    Ok(given)
}

#[derive(Deserialize)]
struct RevokeRequest {
    password: String,
}

fn digest(secret: &str) -> [u8; 32] {
    Sha256::digest(secret.as_bytes()).into()
}

/// The endpoint's state and the engine asking, or why there is none to
/// answer with.
fn checkouts(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(Arc<Checkouts>, Engine), Box<Response>> {
    let Some(checkouts) = state.checkouts.clone() else {
        return Err(Box::new(problem(
            StatusCode::NOT_FOUND,
            "Not Found",
            "this dagron-ci gives no checkout credentials: neither \
             CI_CHECKOUT_TOKEN_SECRET_FILE nor CI_CHECKOUT_TOKEN_SECRETS_DIR is set",
        )));
    };
    let Some(engine) = checkouts.engine(headers) else {
        state.metrics.checkout_token(CheckoutOutcome::Refused);
        return Err(Box::new(problem(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "a checkout credential is for an engine holding a checkout secret \
             (CI_CHECKOUT_TOKEN_SECRET_FILE, or its cell's in CI_CHECKOUT_TOKEN_SECRETS_DIR)",
        )));
    };
    Ok((checkouts, engine))
}

/// What the run `run_id` is for, as `look` reads it, looked for again until
/// `until` while it is not on record: a run dagron-ci submitted a moment ago
/// may not be yet (see [`RECORD_WAIT`]). `None` once that is out: it is not
/// one dagron-ci submitted.
///
/// No look outlasts `answer_by`, and none is begun at it: a look that could
/// not be answered in time is not made, and the last one's word stands. A
/// database that has not answered by then is an `Err`, as one that cannot be
/// read is: what was not read is never taken for a run that is not there.
async fn recorded<F, Fut>(
    look: F,
    run_id: &str,
    until: tokio::time::Instant,
    answer_by: tokio::time::Instant,
) -> Result<Option<RunCommit>, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = sqlx::Result<Option<RunCommit>>>,
{
    let asked = tokio::time::Instant::now();
    let mut pause = RECORD_POLL;
    loop {
        let found = match tokio::time::timeout_at(answer_by, look()).await {
            Ok(Ok(found)) => found,
            Ok(Err(e)) => return Err(format!("the database could not be read: {e}")),
            Err(_) => {
                return Err(format!(
                    "the database did not answer within {:.1} s",
                    asked.elapsed().as_secs_f32()
                ))
            }
        };
        if let Some(run) = found {
            if pause > RECORD_POLL {
                tracing::debug!(
                    run_id,
                    waited_ms = asked.elapsed().as_millis() as u64,
                    "a checkout credential was asked for before its run was on record"
                );
            }
            return Ok(Some(run));
        }
        let now = tokio::time::Instant::now();
        let left = until.saturating_duration_since(now);
        if left.is_zero() || now + pause.min(left) >= answer_by {
            return Ok(None);
        }
        tokio::time::sleep(pause.min(left)).await;
        pause *= 2;
    }
}

/// The forge host and `owner/name` a checkout's repository URL names, as a
/// connected repository is kept (`ci_repos`): what tells whether the URL is
/// of a repository dagron-ci serves. Read loosely, since the question is
/// "could this be one of ours" and a miss waves a run through: the scheme, a
/// user before the host, a trailing slash or `.git`, the scheme's own port
/// (`github.com:443` over https), a dot after the host, and the scp form
/// (`git@host:owner/name`) all name the same repository. `None` for
/// something that names no host and path.
pub(crate) fn repository_name(url: &str) -> Option<(String, String)> {
    let url = url.trim();
    let (scheme, rest, schemed) = match url.split_once("://") {
        Some((scheme, rest)) => (scheme, rest, true),
        None => ("", url, false),
    };
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    // `host/path`, or `host:path` in the scp form, which has no scheme.
    let cut = if schemed {
        rest.find('/')
    } else {
        rest.find([':', '/'])
    }?;
    let (authority, path) = (&rest[..cut], &rest[cut + 1..]);
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    // The port a scheme uses when none is written is no other host. Any
    // other port is kept: it is part of how that forge is named.
    let own_port = match scheme.to_ascii_lowercase().as_str() {
        "https" => ":443",
        "http" => ":80",
        "ssh" => ":22",
        "git" => ":9418",
        _ => "",
    };
    let host = match own_port {
        "" => host,
        port => host.strip_suffix(port).unwrap_or(host),
    };
    let host = host.strip_suffix('.').unwrap_or(host);
    let path = path.trim_matches('/');
    let path = path
        .strip_suffix(".git")
        .unwrap_or(path)
        .trim_end_matches('/');
    (!host.is_empty() && !path.is_empty()).then(|| (host.to_string(), path.to_string()))
}

/// `POST /ci/v1/checkout-tokens`: a credential for one attempt's checkout.
pub(crate) async fn mint(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (checkouts, engine) = match checkouts(&state, &headers) {
        Ok(c) => c,
        Err(res) => return *res,
    };
    let req: MintRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "Bad Request",
                format!("{{repository, commit, run_id}} as JSON: {e}"),
            )
        }
    };
    let refuse = |status: StatusCode, title: &str, detail: String| {
        state.metrics.checkout_token(CheckoutOutcome::Refused);
        tracing::warn!(run_id = %req.run_id, %detail, "checkout credential refused");
        problem(status, title, detail)
    };
    // One clock for the whole answer: what looking for the run takes, the
    // mint does not have.
    let asked = tokio::time::Instant::now();
    let answer_by = asked + checkouts.budget;
    let look_until = answer_by.min(asked + checkouts.record_wait);
    let look = || state.db.run_commit(&req.run_id);
    let run = match recorded(look, &req.run_id, look_until, answer_by).await {
        Ok(Some(run)) => run,
        // Not on record, having been looked for. What the engine is told
        // turns on whose repository the checkout names.
        Ok(None) => {
            let served = match repository_name(&req.repository) {
                Some((host, name)) => {
                    match tokio::time::timeout_at(answer_by, state.db.serves_repo(&host, &name))
                        .await
                    {
                        Ok(Ok(served)) => served,
                        Ok(Err(e)) => {
                            return problem(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "Service Unavailable",
                                format!("the database could not be read: {e}"),
                            )
                        }
                        Err(_) => {
                            return problem(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "Service Unavailable",
                                format!(
                                    "the database did not answer within {:.1} s",
                                    asked.elapsed().as_secs_f32()
                                ),
                            )
                        }
                    }
                }
                // Not a name a connected repository could have.
                None => false,
            };
            if served {
                return refuse(
                    StatusCode::NOT_FOUND,
                    "Not Found",
                    format!(
                        "run {} is not one dagron-ci submitted, and {} is a repository it \
                         serves: that repository's runs are dagron-ci's to submit",
                        req.run_id, req.repository
                    ),
                );
            }
            state.metrics.checkout_token(CheckoutOutcome::Anonymous);
            tracing::info!(
                run_id = %req.run_id, repository = %req.repository,
                "not a run dagron-ci submitted, nor a repository it serves: its checkout takes no credential"
            );
            return StatusCode::NO_CONTENT.into_response();
        }
        Err(why) => return problem(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable", why),
    };
    if let Engine::Cell(cell) = &engine {
        if run.cell != *cell {
            return refuse(
                StatusCode::FORBIDDEN,
                "Forbidden",
                format!(
                    "run {} was submitted to cell {:?}, and this engine's secret is cell {cell:?}'s",
                    req.run_id, run.cell
                ),
            );
        }
    }
    // A run outlives its repository's disconnect, and is given nothing of
    // the repository's after it: a job of it that has yet to check out
    // fails here, with this for its reason.
    if !run.connected {
        return refuse(
            StatusCode::FORBIDDEN,
            "Forbidden",
            format!(
                "run {}'s repository, {}/{}, was disconnected: no credential is minted for it",
                req.run_id, run.repo.forge_host, run.repo.full_name
            ),
        );
    }
    if run.status != "submitted" {
        // On record as over, and an engine is starting a job of it. It may be
        // running again: dagron-ci asks the run's cell itself, and re-opens
        // the record on what the cell says, not on this request.
        let running = match &state.reruns {
            Some(reruns) => {
                let look = reruns.follow(&state.db, &run.cell, &req.run_id, Reopened::Credential);
                match tokio::time::timeout_at(answer_by, look).await {
                    Ok(seen) => seen,
                    Err(_) => Err("dagron-api did not answer in time".to_string()),
                }
            }
            None => Ok(false),
        };
        match running {
            Ok(true) => {}
            Ok(false) => {
                return refuse(
                    StatusCode::CONFLICT,
                    "Conflict",
                    format!("run {} is over ({})", req.run_id, run.status),
                )
            }
            Err(why) => {
                tracing::warn!(run_id = %req.run_id, %why, "a run on record as over could not be looked at");
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Service Unavailable",
                    format!(
                        "run {} is on record as over ({}), and its cell could not be asked whether \
                         it is running again: {why}",
                        req.run_id, run.status
                    ),
                );
            }
        }
    }
    let own = format!("https://{}/{}.git", run.repo.forge_host, run.repo.full_name);
    // The repository the credential is for, and its name on the forge: the
    // run's own, at its commit; or, for an action's fetch, one of the same
    // owner's, for a trusted run alone.
    let (repository, full_name, commit) = if req.action {
        match action_repository(&req, &run) {
            Ok(full_name) => (req.repository.clone(), full_name, req.commit.clone()),
            Err(why) => return refuse(StatusCode::FORBIDDEN, "Forbidden", why),
        }
    } else {
        if req.repository != own {
            return refuse(
                StatusCode::FORBIDDEN,
                "Forbidden",
                format!(
                    "run {}'s repository is {own}, not {}",
                    req.run_id, req.repository
                ),
            );
        }
        if req.commit != run.sha {
            return refuse(
                StatusCode::FORBIDDEN,
                "Forbidden",
                format!(
                    "run {}'s commit is {}, not {}",
                    req.run_id, run.sha, req.commit
                ),
            );
        }
        (own, run.repo.full_name.clone(), run.sha.clone())
    };

    // A patched checkout's patch: a link to the run's bundle, good for an
    // hour, for the run's own repository and commit alone.
    if req.patch {
        if req.action || req.permissions.is_some() {
            return refuse(
                StatusCode::BAD_REQUEST,
                "Bad Request",
                "a patch is the checkout's, not an action's or a forge token's".into(),
            );
        }
        let (Some(id), Some(links)) = (run.bundle_id.as_deref(), state.bundle_links.as_ref())
        else {
            return refuse(
                StatusCode::NOT_FOUND,
                "Not Found",
                format!(
                    "run {} is not of a patch bundle this dagron-ci keeps",
                    req.run_id
                ),
            );
        };
        state.metrics.checkout_token(CheckoutOutcome::Minted);
        tracing::info!(run_id = %req.run_id, bundle = id, "a patch's link given to a checkout");
        return (
            StatusCode::OK,
            axum::Json(serde_json::json!({ "patch_url": links.link(id) })),
        )
            .into_response();
    }
    // A job's forge token: what it is given of what it asked, by the run's
    // trust. A checkout's credential asks for nothing, and reads.
    let permissions = match req
        .permissions
        .as_ref()
        .map(|asked| forge_permissions(asked, &run.trust))
    {
        None => None,
        Some(Ok(given)) => Some(given),
        Some(Err(why)) => return refuse(StatusCode::BAD_REQUEST, "Bad Request", why),
    };
    let anonymous = |why: &str| {
        state.metrics.checkout_token(CheckoutOutcome::Anonymous);
        tracing::debug!(run_id = %req.run_id, repository = %repository, why, "checkout without a credential");
        StatusCode::NO_CONTENT.into_response()
    };
    let gitlab = crate::forge::gitlab::is_gitlab(&run.repo);
    let minted = if gitlab {
        // A project access token of its own, minted with the token the
        // operator keeps for the host, which goes to no job itself.
        if req.action {
            return anonymous("an action's credential is a GitHub App's");
        }
        if permissions.is_some() {
            return anonymous("a job's forge token is a GitHub App's");
        }
        if checkouts.gitlab_token(&run.repo.forge_host).is_none() {
            return anonymous("CI_GITLAB_TOKENS_DIR keeps no token for this GitLab host");
        }
        let minting = checkouts.project_token(&run.repo, &req.run_id);
        let left = answer_by.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout_at(answer_by, minting).await {
            Ok(minted) => minted,
            Err(_) => Err(MintError::Failed(format!(
                "GitLab did not answer within {:.1} s",
                left.as_secs_f32()
            ))),
        }
    } else {
        let app = match &checkouts.app {
            Some(app)
                if run.repo.forge == "github"
                    && run.repo.forge_host == crate::github::DEFAULT_HOST =>
            {
                app
            }
            Some(_) => return anonymous("the App is github.com's"),
            None => return anonymous("no App"),
        };
        // An action's repository is the run's owner's: it is given a credential
        // only where it shares its actions, as GitHub lets a workflow use them.
        if req.action {
            let asking = shares_actions(
                checkouts.http.to(checkouts.api()),
                checkouts.api(),
                app,
                &full_name,
            );
            match tokio::time::timeout_at(answer_by, asking).await {
                Ok(Ok(Sharing::Shared)) => {}
                Ok(Ok(Sharing::Public)) => return anonymous("a public action takes no credential"),
                Ok(Ok(Sharing::Not(why))) => {
                    return refuse(
                        StatusCode::FORBIDDEN,
                        "Forbidden",
                        format!("{full_name} does not share its actions: {why}"),
                    )
                }
                Ok(Err(MintError::Failed(why) | MintError::Refused(why))) => {
                    state.metrics.checkout_token(CheckoutOutcome::Failed);
                    return problem(StatusCode::BAD_GATEWAY, "Bad Gateway", why);
                }
                Ok(Err(MintError::NotInstalled)) => return anonymous("not installed"),
                Err(_) => {
                    state.metrics.checkout_token(CheckoutOutcome::Failed);
                    return problem(
                        StatusCode::BAD_GATEWAY,
                        "Bad Gateway",
                        "GitHub did not say in time whether the action's repository shares its \
                         actions"
                            .to_string(),
                    );
                }
            }
        }
        let minting = checkouts.installation_token(app, &full_name, permissions.as_ref());
        let left = answer_by.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout_at(answer_by, minting).await {
            Ok(minted) => minted,
            Err(_) => Err(MintError::Failed(format!(
                "GitHub did not answer within {:.1} s",
                left.as_secs_f32()
            ))),
        }
    };
    match minted {
        Ok(minted) => {
            // Its row first: no row, no credential.
            let row = CheckoutCredential {
                token_sha256: hex::encode(digest(&minted.token)),
                run_id: req.run_id.clone(),
                task_id: req.task_id.clone(),
                attempt: req.attempt,
                repository: repository.clone(),
                commit,
                cell: run.cell.clone(),
                expires_at: minted.expires_at.clone(),
                revoked: false,
                permissions: permissions
                    .as_ref()
                    .map(|given| serde_json::json!(given).to_string()),
                forge_token_id: minted.forge_token_id.clone(),
            };
            let writing = state.db.record_credential(&row);
            let written = match tokio::time::timeout(RECORD_WRITE, writing).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(format!("the database could not be written: {e}")),
                Err(_) => Err(format!(
                    "the database did not answer within {} s",
                    RECORD_WRITE.as_secs()
                )),
            };
            if let Err(why) = written {
                state.metrics.checkout_token(CheckoutOutcome::Failed);
                tracing::warn!(
                    run_id = %req.run_id, repository = %repository, %why,
                    "a checkout credential was minted and could not be recorded: it is revoked, and not handed over"
                );
                // Not handed over, and not left alive either. On a task of
                // its own: the engine may have stopped waiting by now, and
                // the revoke is made all the same.
                let (checkouts, token, id, repository) = (
                    checkouts.clone(),
                    minted.token,
                    minted.forge_token_id.clone(),
                    repository.clone(),
                );
                tokio::spawn(async move {
                    if let Err(why) = checkouts
                        .revoke_minted(&repository, &token, id.as_deref())
                        .await
                    {
                        tracing::warn!(%why, "an unrecorded checkout credential could not be revoked; it expires on its own");
                    }
                });
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Service Unavailable",
                    format!("the credential could not be recorded, and is not handed over: {why}"),
                );
            }
            state.metrics.checkout_token(CheckoutOutcome::Minted);
            crate::audit::record(state.audit.as_ref(), || {
                crate::audit::Entry::new(crate::audit::kind::CHECKOUT_CREDENTIAL)
                    .with("token_sha256", row.token_sha256.as_str())
                    .with("run_id", row.run_id.as_str())
                    .with("task_id", row.task_id.clone())
                    .with("attempt", row.attempt)
                    .with("repository", row.repository.as_str())
                    .with("commit", row.commit.as_str())
                    .with("cell", row.cell.as_str())
                    .with("expires_at", row.expires_at.clone())
                    .with("permissions", row.permissions.clone())
            });
            tracing::info!(
                run_id = %req.run_id,
                task_id = req.task_id.as_deref().unwrap_or("-"),
                attempt = req.attempt.unwrap_or_default(),
                repository = %repository,
                expires_at = minted.expires_at.as_deref().unwrap_or("-"),
                permissions = ?permissions,
                "{} minted",
                if permissions.is_some() {
                    "a job's forge token"
                } else if req.action {
                    "an action's credential"
                } else {
                    "checkout credential"
                }
            );
            (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "username": if gitlab { crate::forge::gitlab::GIT_USER } else { GITHUB_TOKEN_USER },
                    "password": minted.token,
                    "expires_at": minted.expires_at,
                })),
            )
                .into_response()
        }
        Err(MintError::NotInstalled) if gitlab => {
            tracing::warn!(
                repository = %repository,
                "the token CI_GITLAB_TOKENS_DIR keeps for this host may not mint a project access \
                 token here (it must be a personal access token of the project's Maintainer): its \
                 checkout is fetched without a credential"
            );
            anonymous("may not mint")
        }
        Err(MintError::NotInstalled) => {
            tracing::warn!(
                repository = %repository,
                "the GitHub App is not installed on this repository: its checkout is fetched without a credential"
            );
            anonymous("not installed")
        }
        Err(MintError::Refused(why) | MintError::Failed(why)) => {
            state.metrics.checkout_token(CheckoutOutcome::Failed);
            tracing::warn!(run_id = %req.run_id, repository = %repository, %why, "checkout credential failed");
            problem(StatusCode::BAD_GATEWAY, "Bad Gateway", why)
        }
    }
}

/// The name on the forge of the repository an action's fetch asks a
/// credential for, where it may have one: a trusted run's; a repository of
/// the run's repository's owner's, on its forge, written as the checkout's
/// is (`https://<host>/<owner>/<name>.git`); asking to read it alone. Why
/// not, otherwise.
fn action_repository(req: &MintRequest, run: &RunCommit) -> Result<String, String> {
    if run.trust != "trusted" {
        return Err(format!(
            "run {} is {}: an action's credential is given to a trusted run alone",
            req.run_id, run.trust
        ));
    }
    if req.permissions.is_some() {
        return Err("an action's credential reads its repository, and asks for nothing".into());
    }
    let owner = run
        .repo
        .full_name
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let host = &run.repo.forge_host;
    let named = req
        .repository
        .strip_prefix(&format!("https://{host}/"))
        .and_then(|rest| rest.strip_suffix(".git"))
        .and_then(|name| name.split_once('/'))
        .filter(|(of, name)| {
            of.eq_ignore_ascii_case(&owner)
                && !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
                && *name != "."
                && *name != ".."
        });
    match named {
        Some((of, name)) => Ok(format!("{of}/{name}")),
        None => Err(format!(
            "{} is not a repository of {owner}'s on {host} (https://{host}/{owner}/<name>.git): \
             an action's credential is for one of the run's repository's owner's alone",
            req.repository
        )),
    }
}

/// `DELETE /ci/v1/checkout-tokens`: a used credential, revoked.
pub(crate) async fn revoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Any engine the endpoint answers may revoke: a credential is its own
    // proof that it was minted, and revoking it takes nothing from anyone.
    let (checkouts, _) = match checkouts(&state, &headers) {
        Ok(c) => c,
        Err(res) => return *res,
    };
    let req: RevokeRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return problem(
                StatusCode::BAD_REQUEST,
                "Bad Request",
                format!("{{username, password}} as JSON: {e}"),
            )
        }
    };
    let sha256 = hex::encode(digest(&req.password));
    // A GitLab project access token is revoked by its id, which its row
    // keeps; GitHub's token revokes itself. Which one it is is the row's to
    // say, whatever this process was started with: a GitLab token is never
    // sent to GitHub, so a row that cannot be read is not guessed at.
    let by_id = match tokio::time::timeout(RECORD_WRITE, state.db.forge_token_of(&sha256)).await {
        Ok(Ok(found)) => found,
        Ok(Err(e)) => {
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable",
                format!("the database could not be read: {e}"),
            )
        }
        Err(_) => {
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable",
                "the database did not answer in time".to_string(),
            )
        }
    };
    let revoked = match by_id {
        // Revoked before: there is nothing to do, and nothing to mark.
        Some((_, _, true)) => return StatusCode::NO_CONTENT.into_response(),
        Some((repository, id, false)) => checkouts.revoke_project_token(&repository, &id).await,
        // Without an App, none was minted: nothing to revoke.
        None if checkouts.app.is_none() => return StatusCode::NO_CONTENT.into_response(),
        None => checkouts.revoke(&req.password).await,
    };
    match revoked {
        Ok(()) => {
            state.metrics.checkout_token(CheckoutOutcome::Revoked);
            // Its row says when. A row that cannot be marked is said in the
            // log: the credential is revoked all the same, and its row then
            // reads as one that lapsed on its own, which is later.
            let marking = state.db.credential_revoked(&sha256);
            match tokio::time::timeout(RECORD_WRITE, marking).await {
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => tracing::debug!(
                    "a revoked checkout credential had no row to mark, or was marked before"
                ),
                Ok(Err(e)) => tracing::warn!(
                    error = %e,
                    "a checkout credential was revoked, and its row could not be marked"
                ),
                Err(_) => tracing::warn!(
                    "a checkout credential was revoked, and its row was not marked in time"
                ),
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(why) => {
            tracing::warn!(%why, "a checkout credential could not be revoked; it expires on its own");
            problem(StatusCode::BAD_GATEWAY, "Bad Gateway", why)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
