//! What dagron-ci reads from forges.
//!
//! - **Refs**, from any forge, with git's own ref listing over smart HTTP
//!   ([`crate::git`]): the poll sweep's branches and tags, and the commit a
//!   dispatched ref points at. And a change's files where the compare API
//!   cannot list them, from a shallow fetch of its two commits.
//! - From **GitHub's REST API** (github.com, `*.ghe.com`, Enterprise Server):
//!   the open pull requests a sweep reads; a commit's pipelines, the
//!   `*.yaml` and `*.yml` files in `.dagron/ci` (the contents API); and the
//!   files a change touched, for a paths filter (the compare API).
//! - From **GitLab's REST API** (`/api/v4` of the project's host): the same,
//!   its open merge requests for pull requests ([`gitlab`]).
//!
//! And what it writes: a run's **commit statuses**, through GitHub's Statuses
//! API, for a repository on github.com, with the token (see `report`), and
//! through GitLab's, for a project whose host CI_GITLAB_TOKENS_DIR keeps a
//! token for.
//!
//! **Credentials.** CI_GITHUB_TOKEN_FILE's token goes to github.com and to no
//! other host, whatever a repository's record says. A GitLab host's token
//! (CI_GITLAB_TOKENS_DIR) goes to that host and to no other. Other hosts are
//! read without credentials, so only their public repositories can be read.
//!
//! **Limits.** A forge's rate limit is its own error, [`ForgeError::RateLimited`],
//! with the wait the forge asked for. Every answer is read to a cap, so a
//! hostile or broken forge cannot exhaust memory.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::header::{ACCEPT, CONTENT_TYPE, ETAG, IF_NONE_MATCH, LINK, RETRY_AFTER};
use reqwest::Url;
use serde::Deserialize;

use crate::checkout::{self, GitHubApp, MintError};
use crate::db::Repo;
use crate::event::{is_commit_id, NewEvent};
use crate::git::pack::{self, ObjectFormat};
use crate::git::{self, Advertisement};
use crate::github;
use crate::pipeline::compile::Changes;

pub(crate) mod gitlab;

/// Where a commit's pipelines live.
pub const PIPELINE_DIR: &str = ".dagron/ci";

/// The most pipelines read at one commit. The rest are reported, not read.
const MAX_PIPELINES: usize = 32;

/// The largest pipeline file read.
const MAX_PIPELINE_BYTES: usize = 256 * 1024;

/// The most files GitHub's compare API lists for a change. A list this long
/// may be cut short, so it says nothing about the files it leaves out.
const COMPARE_FILE_LIMIT: usize = 300;

/// The largest comparison read. Each changed file comes with its patch.
const MAX_COMPARISON: usize = 16 * 1024 * 1024;

/// The largest answer read to a shallow fetch of a change's two commits
/// ([`Forge::changed_files`]): their trees, and on a server that cannot
/// leave them out (no `filter`), their files too.
const MAX_FETCHED_PACK: usize = 32 * 1024 * 1024;

/// How far that fetch's pack is read. Two commits' trees, with the deepest
/// and largest repositories', come inside each: a million files' trees
/// inflate to about 45 MiB. With the pack, a fetch then holds about 160 MiB
/// at most (the trees kept, about 300 bytes for each object besides, and the
/// paths listed), inside the 256 MiB the charts give dagron-ci.
const FETCH_LIMITS: pack::Limits = pack::Limits {
    objects: 200_000,
    inflated: 64 * 1024 * 1024,
    // git's own most (`pack.depth`).
    delta_depth: 4095,
    tree_depth: 256,
    walked: 64 * 1024 * 1024,
    paths: 200_000,
    // Linux's PATH_MAX: a longer path cannot be checked out there.
    path_len: 4096,
    listed: 16 * 1024 * 1024,
    until: None,
};

/// How long that fetch may take, all told, with the merge base asked for
/// where the comparison did not name it: well inside an event's lease
/// (CI_EVENT_LEASE_SECS, 60 s), with the compare API's call before it. A
/// third of a shorter lease ([`Forge::with_event_lease`]).
const FETCH_FOR: Duration = Duration::from_secs(20);

/// Shallow fetches at once in a replica. Each may hold what
/// [`FETCH_LIMITS`] allows, so one at a time: another waits its turn within
/// its own time.
const FETCHES_AT_ONCE: usize = 1;

const JSON: &str = "application/vnd.github+json";

/// The contents API answers a file's bytes as they are, rather than base64
/// inside JSON.
const RAW: &str = "application/vnd.github.raw+json";

/// A commit as its id alone.
const SHA: &str = "application/vnd.github.sha";

const API_VERSION: &str = "2022-11-28";

/// How long one request to a forge may take.
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest ref listing read. A hundred thousand branches and tags are
/// about 10 MB.
const MAX_REF_LISTING: usize = 32 * 1024 * 1024;

/// The largest page of pull requests read. GitHub's run to about 30 KB each.
const MAX_API_PAGE: usize = 16 * 1024 * 1024;

const PULLS_PER_PAGE: usize = 100;

/// Pages of open pull requests read per sweep. A repository with more has
/// the rest picked up from its webhooks.
const MAX_PULL_PAGES: usize = 30;

/// The user name GitHub takes a token under, for git over HTTPS.
const GITHUB_GIT_USER: &str = "x-access-token";

/// How long an installation token is used for after it is minted. GitHub
/// expires one an hour after; the ten minutes between are for a request made
/// with it at the last moment, and a clock that is not GitHub's.
const APP_TOKEN_FOR: Duration = Duration::from_secs(50 * 60);

/// How long "the App is not installed on this repository" is gone on, before
/// GitHub is asked again: someone may install it.
const NOT_INSTALLED_FOR: Duration = Duration::from_secs(5 * 60);

/// How long after GitHub refused to mint a token the next one is asked for:
/// its answer changes when someone changes the App.
const MINT_AGAIN_AFTER: Duration = Duration::from_secs(60);

/// How long an installation token has been held before a 401 to a status
/// posted with it is taken for the token's age (GitHub expired it early, or
/// someone revoked it) and the status tried again with another. A 401 to one
/// minted more lately is GitHub refusing the status, as it is for
/// CI_GITHUB_TOKEN_FILE's token: trying again would mint, and be refused, for
/// as long as the run is followed.
const STALE_AFTER: Duration = Duration::from_secs(60);

/// How long after GitHub did not answer a mint (it was not reached, failed,
/// or is limiting requests) the next one is asked for. Short: a call that
/// waits on it is tried again in seconds.
const UNANSWERED_FOR: Duration = Duration::from_secs(5);

/// What an installation token for dagron-ci's own calls may do, and no more:
/// read a repository's files and refs, list its pull requests, and post
/// commit statuses. An App that was not given one of these is refused the
/// token, and [`Forge`] falls back.
fn own_permissions() -> serde_json::Value {
    serde_json::json!({
        "contents": "read",
        "pull_requests": "read",
        "statuses": "write",
    })
}

/// What an installation token for a repository's check runs may do, and no
/// more. It is a token of its own: an App that was not given `checks:
/// write` is refused this one and keeps the other, so the repository's
/// reads and its commit statuses go on as they were.
fn checks_permissions() -> serde_json::Value {
    serde_json::json!({ "checks": "write" })
}

/// What an installation token for a repository's runners may do: register a
/// self-hosted runner for it, and remove one. GitHub puts both under a
/// repository's `administration`, which is more than the two calls, so the
/// token is one of its own, minted for the fleet alone and carried by
/// nothing else dagron-ci does.
fn runners_permissions() -> serde_json::Value {
    serde_json::json!({ "administration": "write" })
}

/// What an installation token for an organization's runners may do:
/// register one for the organization, and remove one. An organization's
/// permission and no repository's, so the token is minted for the
/// installation and not for one repository of it.
fn org_runners_permissions() -> serde_json::Value {
    serde_json::json!({ "organization_self_hosted_runners": "write" })
}

/// What an installation token for reading a repository's workflow runs and
/// jobs may do: GitHub puts both under `actions`. Fleet mode asks it whether
/// a job's workflow run is a fork's, and how a job stands where its delivery
/// never came.
fn actions_permissions() -> serde_json::Value {
    serde_json::json!({ "actions": "read" })
}

/// What an installation token for running a repository's workflow jobs
/// again may do: GitHub puts that under `actions` too, written. A token of
/// its own, so that an App given `actions: read` alone still tells a fork's
/// job apart.
fn retries_permissions() -> serde_json::Value {
    serde_json::json!({ "actions": "write" })
}

/// What an installation token for a pull request's comment may do: write
/// to a repository's pull requests, which is what GitHub asks for a comment
/// on one. A token of its own for the same reason a check run's is: an App
/// that was not given `pull requests: write` is refused this one and keeps
/// the others.
fn comments_permissions() -> serde_json::Value {
    serde_json::json!({ "pull_requests": "write" })
}

/// What an installation token for an issue of a job that flakes may do:
/// write a repository's issues. A token of its own, as a comment's is: an
/// App that was not given `issues: write` files none, and keeps the rest.
fn issues_permissions() -> serde_json::Value {
    serde_json::json!({ "issues": "write" })
}

/// Which of the App's tokens for a repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Own {
    /// For dagron-ci's own calls: [`own_permissions`].
    Calls,
    /// For the repository's check runs: [`checks_permissions`].
    Checks,
    /// For the repository's runners, in fleet mode: [`runners_permissions`].
    Runners,
    /// For its owner's runners, an organization's, in fleet mode:
    /// [`org_runners_permissions`]. Held by the owner, and not the
    /// repository.
    OrgRunners,
    /// For reading its workflow runs and jobs, in fleet mode:
    /// [`actions_permissions`].
    Actions,
    /// For running a job of its again, in fleet mode:
    /// [`retries_permissions`].
    Retries,
    /// For a pull request's comment: [`comments_permissions`].
    Comments,
    /// For an issue of a job that flakes: [`issues_permissions`].
    Issues,
}

/// The App, and the tokens it has minted for each repository: one for its
/// calls, and one for its check runs.
struct AppTokens {
    app: GitHubApp,
    /// By the repository's `owner/name`, in lower case.
    held: Mutex<HashMap<String, Held>>,
    /// The same, of the tokens for check runs.
    checks: Mutex<HashMap<String, Held>>,
    /// The same, of the tokens for runners.
    runners: Mutex<HashMap<String, Held>>,
    /// By the organization, as `org:{owner}` in lower case, of the tokens
    /// for an organization's runners.
    org_runners: Mutex<HashMap<String, Held>>,
    /// By the repository, of the tokens for reading its workflow runs.
    actions: Mutex<HashMap<String, Held>>,
    /// The same, of the tokens for running its jobs again.
    retries: Mutex<HashMap<String, Held>>,
    /// The same, of the tokens for pull requests' comments.
    comments: Mutex<HashMap<String, Held>>,
    /// The same, of the tokens for the issues of jobs that flake.
    issues: Mutex<HashMap<String, Held>>,
}

impl AppTokens {
    fn of(&self, own: Own) -> &Mutex<HashMap<String, Held>> {
        match own {
            Own::Calls => &self.held,
            Own::Checks => &self.checks,
            Own::Runners => &self.runners,
            Own::OrgRunners => &self.org_runners,
            Own::Actions => &self.actions,
            Own::Retries => &self.retries,
            Own::Comments => &self.comments,
            Own::Issues => &self.issues,
        }
    }
}

/// What a token of `own` for `repo` is held by: the repository, in lower
/// case, or its owner for a token that is an organization's.
fn token_key(own: Own, repo: &Repo) -> String {
    match own {
        Own::OrgRunners => format!("org:{}", owner_of(repo)),
        _ => repo.full_name.to_ascii_lowercase(),
    }
}

/// A repository's owner, as its `owner/name` has it, in lower case, as
/// GitHub takes it in any: a user or an organization.
pub(crate) fn owner_of(repo: &Repo) -> String {
    repo.full_name
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// What is known of the App's token for one repository, and until when.
#[derive(Clone)]
enum Held {
    Token {
        token: String,
        until: Instant,
    },
    /// None to be had: the App is not installed there, or GitHub would not
    /// mint one. Asked again after `until`.
    None {
        until: Instant,
    },
    /// Not known: GitHub did not answer the mint, and why. Asked again after
    /// `until`.
    Unknown {
        why: String,
        until: Instant,
    },
}

/// What dagron-ci reads from forges: refs, through git's smart HTTP; and
/// from GitHub's API, open pull requests, a commit's pipelines, and the files
/// a change touched. The poll sweep, the run API and the event workers read
/// with it.
///
/// **Whose authority a call carries.** For a repository on github.com, the
/// App's, where an App is configured and installed on the repository: an
/// installation token minted for that repository alone, with
/// [`own_permissions`], kept for [`APP_TOKEN_FOR`] and then minted again.
/// GitHub refuses it for any other repository, so what one repository's calls
/// carry cannot read another's. Where the App is not installed on the
/// repository, or GitHub will not mint the token, CI_GITHUB_TOKEN_FILE's
/// token, if there is one; and otherwise none, which a public repository does
/// not need. Where GitHub did not answer the mint, that token too; and
/// without one the call fails, and is not made as nobody: GitHub answers an
/// anonymous caller as if a private repository held nothing, and that would
/// be taken for a commit with no pipelines. Neither goes to any host but
/// github.com.
pub struct Forge {
    /// Chosen for each request by where it goes: one to this machine
    /// (CI_GITHUB_API_URL as plain http, a sidecar) goes by no proxy.
    http: crate::config::Clients,
    /// CI_GITHUB_TOKEN_FILE's token. Sent to github.com and nowhere else.
    github_token: Option<String>,
    /// CI_GITHUB_APP_ID's App, whose installation tokens are used first.
    app: Option<AppTokens>,
    /// CI_GITHUB_API_URL: where github.com's REST API is reached, when not
    /// at its own address.
    github_api: Option<String>,
    /// Where every repository's git and API answer instead of its own host:
    /// a test's stub.
    origin: Option<String>,
    /// CI_GITHUB_CHECKS: whether a job is reported as a check run, where the
    /// App has a token for the repository's check runs.
    checks: bool,
    /// CI_GITHUB_PR_COMMENT: whether a pull request is given a comment that
    /// says how its runs went, where the App has a token for one.
    pr_comment: bool,
    /// CI_GITLAB_MR_NOTES: whether a GitLab merge request is given a note
    /// that says the same, with its host's token.
    mr_notes: bool,
    /// CI_FLAKE_ISSUES: whether a job that keeps passing on a later attempt
    /// is filed an issue, with the App's token for one or the GitLab host's.
    flake_issues: bool,
    /// CI_GITLAB_TOKENS_DIR's tokens, by the GitLab host each is for, in
    /// lower case. Each is sent to its host and nowhere else.
    gitlab_tokens: HashMap<String, String>,
    /// A turn at a shallow fetch ([`FETCHES_AT_ONCE`]), which the work of
    /// reading its pack holds until that work ends.
    fetches: Arc<tokio::sync::Semaphore>,
    /// How long a shallow fetch may take ([`FETCH_FOR`]).
    fetch_for: Duration,
}

impl std::fmt::Debug for Forge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forge")
            .field(
                "github_token",
                &self.github_token.as_ref().map(|_| "<redacted>"),
            )
            .field("app", &self.app.as_ref().map(|tokens| tokens.app.id()))
            .field("github_api", &self.github_api)
            .field("origin", &self.origin)
            .field(
                "gitlab_tokens",
                &self.gitlab_tokens.keys().collect::<BTreeSet<_>>(),
            )
            .finish()
    }
}

/// Why a forge could not be read.
#[derive(Debug, PartialEq, Eq)]
pub enum ForgeError {
    /// The forge's rate limit is spent: try again once `wait` has passed.
    RateLimited { wait: Duration, why: String },
    /// Anything else, and why. Worth trying again later.
    Failed(String),
}

/// Why a commit status was not posted.
#[derive(Debug, PartialEq, Eq)]
pub enum StatusError {
    /// The forge's rate limit is spent: try again once `wait` has passed.
    RateLimited { wait: Duration, why: String },
    /// The forge refused it, and would again: a token without the
    /// permission, a repository it cannot see, a commit it does not have.
    /// It would refuse any other status of that commit the same way.
    Refused(String),
    /// The forge refused this status for itself, and would again: under its
    /// context it keeps no more on the commit, or the status is not one it
    /// takes. Another status of the same commit may still be taken.
    ThisStatus(String),
    /// Anything else, and why. Worth trying again later.
    Failed(String),
}

/// How long a status waits on a rate limit GitHub named no wait for. GitHub
/// answers a secondary limit with a 403 that may carry neither `retry-after`
/// nor a spent `x-ratelimit-remaining`, and asks for at least a minute then.
const UNNAMED_RATE_LIMIT_WAIT: Duration = Duration::from_secs(60);

/// The most of a refusal's body read to see whether it is a rate limit.
const REFUSAL_BODY_MAX: usize = 16 * 1024;

/// The most of what GitHub said of a status it would not take that is kept
/// in the reason.
const REFUSAL_EXCERPT: usize = 200;

/// What became of a pull request's comment that was posted.
#[derive(Debug, PartialEq, Eq)]
pub enum Commented {
    /// It is written, under this id of GitHub's.
    Written(u64),
    /// The comment that was to be brought up to date is not there: someone
    /// deleted it.
    Gone,
}

/// The longest comment GitHub takes, in characters. A body is cut to it.
pub const COMMENT_MAX: usize = 65_536;

/// What came of writing an issue of a job that flakes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Issued {
    /// It is open and says what it was given, under this number of the
    /// forge's.
    Written(String),
    /// A person closed it: what it was of is dealt with, and the job's
    /// flakes are counted again from when it was, which the forge says
    /// where it does.
    Closed(Option<String>),
    /// It is not there: someone deleted it.
    Gone,
}

/// `body`, as much of it as a comment takes. One is built to be under
/// that, and one that is not is still a comment.
fn comment_text(body: &str) -> &str {
    match body.char_indices().nth(COMMENT_MAX) {
        Some((end, _)) => &body[..end],
        None => body,
    }
}

/// One commit status, as GitHub's Statuses API takes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitStatus {
    /// `pending`, `success`, `failure` or `error`.
    pub state: &'static str,
    pub context: String,
    /// At most 140 characters, as GitHub takes it.
    pub description: String,
    pub target_url: Option<String>,
    /// The branch or tag the status is of, which GitLab is told
    /// ([`gitlab::status_ref`]): it keeps a commit's statuses of one ref in
    /// one pipeline, which the branch and its merge request show. Left out,
    /// GitLab takes the first branch it finds the commit on. GitHub is told
    /// none.
    pub pipeline_ref: Option<String>,
}

/// What a check run says of a place in the commit's files, as GitHub's
/// Checks API takes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Annotation {
    /// The file, from the repository's root.
    pub path: String,
    pub start_line: u64,
    pub end_line: u64,
    /// `failure`, `warning` or `notice`.
    pub level: &'static str,
    /// Left out where it is empty.
    pub title: String,
    pub message: String,
}

/// How many annotations GitHub takes with one writing of a check run.
pub const CHECK_ANNOTATIONS_MAX: usize = 50;

/// How many characters GitHub takes of a check run's summary, and of its
/// text: one that is longer is refused whole.
const CHECK_TEXT_MAX: usize = 65_535;

/// `text`, as much of it as a check run takes.
fn check_text(text: &str) -> &str {
    match text.char_indices().nth(CHECK_TEXT_MAX) {
        Some((end, _)) => &text[..end],
        None => text,
    }
}

/// One check run, as GitHub's Checks API takes it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckRun {
    /// What branch protection requires, and what a later run of the same
    /// job is posted under.
    pub name: String,
    /// `queued`, `in_progress` or `completed`.
    pub status: &'static str,
    /// How it ended, where it is `completed`: `success`, `failure`,
    /// `cancelled` or `skipped`.
    pub conclusion: Option<&'static str>,
    /// The line beside the check.
    pub title: String,
    /// What the check's own page says, as Markdown.
    pub summary: String,
    /// What stands under the summary there, as Markdown. Left out where it
    /// is empty.
    pub text: String,
    /// What it says of places in the commit's files: the first
    /// [`CHECK_ANNOTATIONS_MAX`] are sent. GitHub adds them to those the
    /// check run has, so they go with one writing of it and no other.
    pub annotations: Vec<Annotation>,
    pub details_url: Option<String>,
    /// The engine run it is of: what a "Re-run" of the check names. Empty
    /// for one that is of no run.
    pub external_id: String,
}

/// What GitHub said to running a job again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Retried {
    /// It took it: a new attempt of the job is queued.
    Asked,
    /// Not now, and why: GitHub runs a job again once its workflow run is
    /// over, and limits its callers.
    NotYet(String),
    /// Never, and why: GitHub has no such job.
    Refused(String),
}

/// A just-in-time runner GitHub registered.
pub struct Jit {
    /// GitHub's id for the registration: what it is removed by.
    pub runner_id: u64,
    /// The configuration the runner's process is started with. A secret.
    pub config: String,
}

impl std::fmt::Debug for Jit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Jit")
            .field("runner_id", &self.runner_id)
            .field("config", &"<redacted>")
            .finish()
    }
}

/// How a post GitHub refused reads, by what was posted.
struct Refusals {
    /// The token the post carried.
    own: Own,
    /// What a 401 or a 403 says is missing.
    denied: &'static str,
    /// What a 404 says.
    missing: &'static str,
    /// What a 422 says that is not about the commit.
    this: &'static str,
}

const STATUS: Refusals = Refusals {
    own: Own::Calls,
    denied: "the token needs `statuses: write` on this repository",
    missing: "the token cannot see this repository",
    this: "GitHub takes no more statuses under this context on this commit, or does not take \
           this one",
};

const CHECK: Refusals = Refusals {
    own: Own::Checks,
    denied: "the App needs `checks: write` on this repository",
    missing: "GitHub has no such repository for the App, or no such check run",
    this: "GitHub does not take this check run",
};

const ISSUE: Refusals = Refusals {
    own: Own::Issues,
    denied: "the App needs `issues: write` on this repository",
    missing: "GitHub has no such repository for the App, or no such issue",
    this: "GitHub does not take this issue",
};

const COMMENT: Refusals = Refusals {
    own: Own::Comments,
    denied: "the App needs `pull requests: write` on this repository, and the pull request's \
             conversation must not be locked",
    missing: "GitHub has no such pull request for the App",
    this: "GitHub does not take this comment",
};

impl From<git::ProtocolError> for ForgeError {
    fn from(e: git::ProtocolError) -> ForgeError {
        ForgeError::Failed(format!("listing refs: {e}"))
    }
}

/// The open pull requests, as far as a sweep knows them.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Pulls {
    /// Not read: this forge's pull requests are not polled.
    NotListed,
    /// None changed since the ETag the last sweep kept.
    Unchanged,
    /// Read: each one's event, whether every page was read, and the ETag to
    /// keep (only when they fit on one page).
    Listed {
        events: Vec<NewEvent>,
        complete: bool,
        etag: Option<String>,
    },
}

impl Forge {
    pub fn new(github_token: Option<String>) -> Forge {
        Forge {
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(REQUEST_TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            github_token,
            app: None,
            github_api: None,
            origin: None,
            checks: false,
            pr_comment: false,
            mr_notes: false,
            flake_issues: false,
            gitlab_tokens: HashMap::new(),
            fetches: Arc::new(tokio::sync::Semaphore::new(FETCHES_AT_ONCE)),
            fetch_for: FETCH_FOR,
        }
    }

    /// Fit a shallow fetch's time to an event's lease (CI_EVENT_LEASE_SECS):
    /// [`FETCH_FOR`], or a third of a shorter lease, so that a fetch is done
    /// or given up before another worker can take the event over.
    pub fn with_event_lease(mut self, lease: Duration) -> Forge {
        self.fetch_for = FETCH_FOR.min(lease / 3);
        self
    }

    /// Keep one note on each GitLab merge request that says how its runs
    /// went (CI_GITLAB_MR_NOTES=on), for a project on a host
    /// CI_GITLAB_TOKENS_DIR keeps a token for. Any other's are given none.
    pub fn with_mr_notes(mut self, on: bool) -> Forge {
        self.mr_notes = on;
        self
    }

    /// Report a job as a check run and not a commit status
    /// (CI_GITHUB_CHECKS=on), for a repository the App is installed on with
    /// `checks: write`. Any other repository's jobs are statuses still.
    pub fn with_checks(mut self, on: bool) -> Forge {
        self.checks = on;
        self
    }

    /// Keep one comment on each pull request that says how its runs went
    /// (CI_GITHUB_PR_COMMENT=on), for a repository the App is installed on
    /// with `pull requests: write`. Any other repository's pull requests
    /// are given none.
    pub fn with_pr_comment(mut self, on: bool) -> Forge {
        self.pr_comment = on;
        self
    }

    /// File an issue of a job that keeps passing on a later attempt
    /// (CI_FLAKE_ISSUES), where the App or the GitLab host's token can.
    pub fn with_flake_issues(mut self, on: bool) -> Forge {
        self.flake_issues = on;
        self
    }

    /// Make a github.com repository's calls as `app`, where it is installed
    /// on the repository (CI_GITHUB_APP_ID, CI_GITHUB_APP_KEY_FILE): the App
    /// that mints each checkout's credential. The token [`Forge::new`] was
    /// given is then what a repository without the App is read with.
    pub fn with_app(mut self, app: Option<GitHubApp>) -> Forge {
        self.app = app.map(|app| AppTokens {
            app,
            held: Mutex::default(),
            checks: Mutex::default(),
            runners: Mutex::default(),
            org_runners: Mutex::default(),
            actions: Mutex::default(),
            retries: Mutex::default(),
            comments: Mutex::default(),
            issues: Mutex::default(),
        });
        self
    }

    /// Reach github.com's REST API at `api` (CI_GITHUB_API_URL), when set:
    /// an egress proxy or a cache in front of it. The token goes there.
    pub fn with_github_api(mut self, api: Option<String>) -> Forge {
        self.github_api = api;
        self
    }

    /// Read and tell a GitLab project with the token `tokens` keeps for its
    /// host (CI_GITLAB_TOKENS_DIR: each file a host, its text the token), a
    /// group's or a project's access token with the api scope. A host
    /// without one is read as nobody.
    pub fn with_gitlab_tokens(mut self, tokens: &BTreeMap<String, String>) -> Forge {
        self.gitlab_tokens = tokens
            .iter()
            .map(|(host, token)| (host.to_ascii_lowercase(), token.clone()))
            .collect();
        self
    }

    /// The REST API `repo` is read through.
    fn api_for(&self, repo: &Repo) -> String {
        if let Some(origin) = &self.origin {
            return origin.clone();
        }
        if gitlab::is_gitlab(repo) {
            return format!("https://{}{}", repo.forge_host, gitlab::API_PATH);
        }
        match &self.github_api {
            Some(api) if repo.forge_host == github::DEFAULT_HOST => api.clone(),
            _ => github_api(&repo.forge_host),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_origin(mut self, origin: &str) -> Forge {
        self.origin = Some(origin.to_string());
        self
    }

    /// The commit `git_ref` points at now, or `None` when the repository has
    /// no such ref. For an annotated tag that is the commit the tag names, not
    /// the tag's own id. The error says why the forge could not be read.
    pub async fn resolve(&self, repo: &Repo, git_ref: &str) -> Result<Option<String>, String> {
        if !is_url_safe(repo) {
            return Err(format!(
                "{}/{} cannot be put in a URL as it is; reconnect it",
                repo.forge_host, repo.full_name
            ));
        }
        match self.list_refs(repo, &[git_ref]).await {
            // A prefix: refs/heads/main also lists refs/heads/main-old.
            Ok(mut refs) => Ok(refs
                .peeled
                .remove(git_ref)
                .or_else(|| refs.ids.remove(git_ref))),
            Err(ForgeError::Failed(why) | ForgeError::RateLimited { why, .. }) => Err(why),
        }
    }

    /// Whether the commit `sha` is on `git_ref`: the ref's own commit now,
    /// or one the ref has behind it. A caller who names both a ref and a
    /// commit is asked this, so that a run for a ref is of a commit that was
    /// on it: any commit of a fork is reachable in the repository it forks.
    ///
    /// A GitHub repository is asked through its API alone, which is where
    /// `CI_GITHUB_API_URL` points: the ref's commit, then how the two stand.
    /// On another forge the ref is read as [`Forge::resolve`] reads it, and
    /// only the ref's own commit is known to be on it. The error says why
    /// the forge could not be read.
    pub async fn on_ref(&self, repo: &Repo, git_ref: &str, sha: &str) -> Result<OnRef, String> {
        const WHAT: &str = "asking whether a commit is on a ref";
        let said = |e: ForgeError| match e {
            ForgeError::Failed(why) | ForgeError::RateLimited { why, .. } => why,
        };
        if repo.forge != "github" || !is_url_safe(repo) {
            let Some(head) = self.resolve(repo, git_ref).await? else {
                return Ok(OnRef::NoRef);
            };
            if head == sha {
                return Ok(OnRef::Yes);
            }
            return Ok(OnRef::No(format!(
                "{git_ref} is at {}, and only that commit is known to be on it: {} is on {}, \
                 whose history dagron-ci cannot ask",
                short(&head),
                repo.full_name,
                repo.forge
            )));
        }
        // The ref's commit: "get a commit" takes `heads/NAME` and
        // `tags/NAME`, and answers with the id alone when asked for it. For
        // an annotated tag that is the commit the tag names.
        let Some(named) = git_ref.strip_prefix("refs/") else {
            return Ok(OnRef::NoRef);
        };
        let mut segments = vec!["commits"];
        segments.extend(named.split('/'));
        let (response, credentialed) = self
            .api_get(repo, &segments, &[], SHA, WHAT)
            .await
            .map_err(said)?;
        let head = match response.status().as_u16() {
            200 => {
                let Some(body) = read_up_to(response, 256, WHAT).await.map_err(said)? else {
                    return Err(format!(
                        "{WHAT}: the forge's answer for {git_ref} is no commit id"
                    ));
                };
                let head = String::from_utf8_lossy(&body).trim().to_string();
                if !crate::event::is_commit_id(&head) {
                    return Err(format!(
                        "{WHAT}: the forge's answer for {git_ref} is no commit id"
                    ));
                }
                head
            }
            // No such ref, and what GitHub answers for a repository with no
            // commits at all.
            404 | 409 | 422 => return Ok(OnRef::NoRef),
            _ => return Err(said(api_error(&response, repo, credentialed, WHAT))),
        };
        if head == sha {
            return Ok(OnRef::Yes);
        }
        // What `head` has since `sha`: ahead of it, or the same, where
        // `sha` is behind it. The files of the first page are not wanted.
        let range = format!("{sha}...{head}");
        let (response, credentialed) = self
            .api_get(
                repo,
                &["compare", &range],
                &[("per_page", "1"), ("page", "2")],
                JSON,
                WHAT,
            )
            .await
            .map_err(said)?;
        match response.status().as_u16() {
            200 => {}
            404 | 422 => {
                return Ok(OnRef::No(format!(
                    "the forge knows no commit {} that shares history with {git_ref}",
                    short(sha)
                )))
            }
            _ => return Err(said(api_error(&response, repo, credentialed, WHAT))),
        }
        let Some(body) = read_up_to(response, MAX_COMPARISON, WHAT)
            .await
            .map_err(said)?
        else {
            return Err(format!(
                "{WHAT}: the comparison is over {}",
                size(MAX_COMPARISON)
            ));
        };
        let comparison: Comparison =
            serde_json::from_slice(&body).map_err(|e| format!("{WHAT}: {e}"))?;
        Ok(match comparison.status.as_deref() {
            Some("ahead" | "identical") => OnRef::Yes,
            Some(how @ ("behind" | "diverged")) => OnRef::No(format!(
                "{} is not on {git_ref}: the ref is at {}, which {} it",
                short(sha),
                short(&head),
                if how == "behind" {
                    "is behind"
                } else {
                    "has diverged from"
                }
            )),
            other => {
                return Err(format!(
                    "{WHAT}: the forge said {:?} of {} and {}",
                    other.unwrap_or("nothing"),
                    short(sha),
                    short(&head)
                ))
            }
        })
    }

    /// Whether `repo` is one a credential goes to: on github.com, and no
    /// other host.
    fn on_github_com(repo: &Repo) -> bool {
        repo.forge == "github" && repo.forge_host == github::DEFAULT_HOST
    }

    /// Whether there is anything to make `repo`'s calls with: an App, which
    /// may be installed on it, or the token. Known without asking GitHub.
    fn credentialed(&self, repo: &Repo) -> bool {
        Self::on_github_com(repo) && (self.app.is_some() || self.github_token.is_some())
    }

    /// What `repo`'s calls are made with, when it is on github.com: the
    /// App's token for it, or else CI_GITHUB_TOKEN_FILE's. Never sent to any
    /// other host.
    ///
    /// An error where it is not known whether the App has a token for it
    /// (GitHub did not answer the mint) and there is no other: the call is
    /// not made as nobody.
    async fn token_for(&self, repo: &Repo) -> Result<Option<String>, String> {
        if !Self::on_github_com(repo) {
            return Ok(None);
        }
        match (self.app_token(repo).await, &self.github_token) {
            (Ok(Some(token)), _) => Ok(Some(token)),
            (_, Some(token)) => Ok(Some(token.clone())),
            (Ok(None), None) => Ok(None),
            (Err(why), None) => Err(format!(
                "no installation token for this repository ({why}), and no \
                 CI_GITHUB_TOKEN_FILE to make the call with instead"
            )),
        }
    }

    /// The App's installation token for `repo`: the one held, or one minted
    /// now. `None` without an App, where the App is not installed on the
    /// repository, and where GitHub would not mint one, which is said once
    /// and asked again after [`MINT_AGAIN_AFTER`]. An error, with why, where
    /// GitHub did not answer the mint: asked again after [`UNANSWERED_FOR`].
    ///
    /// The lock is not held across the request to GitHub. Two callers that
    /// find nothing held both mint, and the later one's token is the one kept:
    /// each is good for its hour.
    async fn app_token(&self, repo: &Repo) -> Result<Option<String>, String> {
        self.app_token_of(repo, Own::Calls).await
    }

    /// Whether `repo`, a repository of an action, shares its actions with its
    /// owner's other repositories' workflows, as GitHub decides it
    /// ([`checkout::shares_actions`]): a public one does; a private one where
    /// its settings say so. None without an App, which cannot ask.
    pub async fn shares_actions(&self, repo: &Repo) -> Result<bool, ForgeError> {
        let Some(tokens) = &self.app else {
            return Ok(false);
        };
        if !is_url_safe(repo) {
            return Ok(false);
        }
        let api = self.api_for(repo);
        match checkout::shares_actions(self.http.to(&api), &api, &tokens.app, &repo.full_name).await
        {
            Ok(checkout::Sharing::Public | checkout::Sharing::Shared) => Ok(true),
            Ok(checkout::Sharing::Not(why)) => {
                tracing::info!(repo = %repo.full_name, %why, "an action's repository shares none of its actions: its file is not read");
                Ok(false)
            }
            Err(MintError::Failed(why)) => Err(ForgeError::Failed(why)),
            Err(MintError::Refused(_) | MintError::NotInstalled) => Ok(false),
        }
    }

    /// [`Self::app_token`], of either token: the one for the repository's
    /// calls, or the one for its check runs.
    async fn app_token_of(&self, repo: &Repo, own: Own) -> Result<Option<String>, String> {
        let Some(tokens) = &self.app else {
            return Ok(None);
        };
        if !is_url_safe(repo) {
            return Ok(None);
        }
        let key = token_key(own, repo);
        let now = Instant::now();
        let held = tokens
            .of(own)
            .lock()
            .ok()
            .and_then(|held| held.get(&key).cloned());
        match held {
            Some(Held::Token { token, until }) if until > now => return Ok(Some(token)),
            Some(Held::None { until }) if until > now => return Ok(None),
            Some(Held::Unknown { why, until }) if until > now => return Err(why),
            _ => {}
        }
        let api = self.api_for(repo);
        let minted = checkout::installation_token_of(
            self.http.to(&api),
            &api,
            &tokens.app,
            &repo.full_name,
            match own {
                Own::Calls => own_permissions(),
                Own::Checks => checks_permissions(),
                Own::Runners => runners_permissions(),
                Own::OrgRunners => org_runners_permissions(),
                Own::Actions => actions_permissions(),
                Own::Retries => retries_permissions(),
                Own::Comments => comments_permissions(),
                Own::Issues => issues_permissions(),
            },
            own != Own::OrgRunners,
        )
        .await;
        let held = match minted {
            Ok(minted) => Held::Token {
                token: minted.token,
                until: now + APP_TOKEN_FOR,
            },
            Err(MintError::NotInstalled) => {
                tracing::debug!(
                    repo = %repo.full_name,
                    "the App is not installed on this repository: its calls are made without the App"
                );
                Held::None {
                    until: now + NOT_INSTALLED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::Runners => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's runners: \
                     none is started for its jobs. The App needs administration: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::Runners => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's runners yet: none is started \
                     until GitHub answers"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::OrgRunners => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's owner's \
                     runners: none is started for its jobs. The App needs the organization \
                     permission self-hosted runners: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::OrgRunners => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's owner's runners yet: none is \
                     started until GitHub answers"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::Actions => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's workflow \
                     runs: a fork's job is not told from another's, and is given no runner \
                     where its label refuses one. The App needs actions: read"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::Actions => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's workflow runs yet: its jobs \
                     are not told apart until GitHub answers"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::Retries => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for running this repository's \
                     jobs again: a job whose runner died is not. The App needs actions: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::Retries => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for running this repository's jobs again yet"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::Issues => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's issues: \
                     no issue is filed of a job that flakes. The App needs issues: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::Issues => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's issues yet: none is filed \
                     until GitHub answers"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::Comments => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's pull \
                     request comments: none is written. The App needs pull requests: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::Comments => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's pull request comments yet: \
                     none is written until GitHub answers"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) if own == Own::Checks => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's check \
                     runs: its jobs are reported as commit statuses. The App needs checks: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) if own == Own::Checks => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's check runs yet: its jobs are \
                     not reported until GitHub answers"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
            Err(MintError::Refused(why)) => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "GitHub will not mint an installation token for this repository's calls: \
                     they are made with CI_GITHUB_TOKEN_FILE's token, if there is one. The App \
                     needs contents: read, pull requests: read and commit statuses: write"
                );
                Held::None {
                    until: now + MINT_AGAIN_AFTER,
                }
            }
            Err(MintError::Failed(why)) => {
                tracing::warn!(
                    repo = %repo.full_name, %why,
                    "no installation token for this repository's calls yet: they are made with \
                     CI_GITHUB_TOKEN_FILE's token, if there is one, and fail without one"
                );
                Held::Unknown {
                    why,
                    until: now + UNANSWERED_FOR,
                }
            }
        };
        let answer = match &held {
            Held::Token { token, .. } => Ok(Some(token.clone())),
            Held::None { .. } => Ok(None),
            Held::Unknown { why, .. } => Err(why.clone()),
        };
        if let Ok(mut all) = tokens.of(own).lock() {
            // A token another caller minted meanwhile is not lost to this
            // one's worse luck.
            let good = matches!(all.get(&key), Some(Held::Token { until, .. }) if *until > now);
            if matches!(held, Held::Token { .. }) || !good {
                all.insert(key, held);
            }
        }
        answer
    }

    /// GitHub said the credential `repo`'s call carried is not one it knows
    /// (401). Where that is the installation token held for the repository,
    /// expired or revoked, it is not used again: the next call mints another.
    /// A 401 to anything else leaves what is held as it is.
    ///
    /// How long the token dropped had been held, or `None` where none was.
    fn forget_token(&self, repo: &Repo, carried: Option<&str>) -> Option<Duration> {
        self.forget_token_of(Own::Calls, repo, carried)
    }

    /// [`Self::forget_token`], of either token.
    fn forget_token_of(&self, own: Own, repo: &Repo, carried: Option<&str>) -> Option<Duration> {
        let (tokens, carried) = (self.app.as_ref()?, carried?);
        let mut held = tokens.of(own).lock().ok()?;
        let key = token_key(own, repo);
        let until = match held.get(&key) {
            Some(Held::Token { token, until }) if token == carried => *until,
            _ => return None,
        };
        held.remove(&key);
        Some((Instant::now() + APP_TOKEN_FOR).saturating_duration_since(until))
    }

    /// `git ls-remote` for the refs under `prefixes`, over smart HTTP: each
    /// ref's id, and for an annotated tag the commit it names as well.
    pub(crate) async fn list_refs(
        &self,
        repo: &Repo,
        prefixes: &[&str],
    ) -> Result<git::Refs, ForgeError> {
        const WHAT: &str = "listing refs";
        let url = self.git_url(repo);
        let advertised = self
            .git_request(
                repo,
                self.http
                    .to(&url)
                    .get(format!("{url}/{}", git::ADVERTISEMENT_PATH)),
                WHAT,
                MAX_REF_LISTING,
            )
            .await?;
        match git::read_advertisement(&advertised, prefixes)? {
            Advertisement::V0(refs) => Ok(refs),
            Advertisement::V2 { object_format, .. } => {
                let body = git::ls_refs_request(prefixes, object_format.as_deref());
                let listed = self
                    .git_request(repo, self.git_command(&url, body), WHAT, MAX_REF_LISTING)
                    .await?;
                Ok(git::read_ls_refs(&listed, prefixes)?)
            }
        }
    }

    /// Where `repo`'s git is served: `https://{host}/{full name}.git`.
    fn git_url(&self, repo: &Repo) -> String {
        let origin = self
            .origin
            .clone()
            .unwrap_or_else(|| format!("https://{}", repo.forge_host));
        format!("{origin}/{}.git", repo.full_name)
    }

    /// A v2 command, `body`, to the git at `url`.
    fn git_command(&self, url: &str, body: Vec<u8>) -> reqwest::RequestBuilder {
        self.http
            .to(url)
            .post(format!("{url}/{}", git::COMMAND_PATH))
            .header(CONTENT_TYPE, git::COMMAND_CONTENT_TYPE)
            .header(ACCEPT, git::COMMAND_ACCEPT)
            .body(body)
    }

    /// Send `request`, for `what`, to `repo`'s git, with its credential:
    /// the answer, read up to `max` bytes.
    async fn git_request(
        &self,
        repo: &Repo,
        request: reqwest::RequestBuilder,
        what: &str,
        max: usize,
    ) -> Result<Vec<u8>, ForgeError> {
        let mut request = request.header(git::PROTOCOL_HEADER, git::PROTOCOL_VERSION_2);
        let (user, token) = if gitlab::is_gitlab(repo) {
            (
                gitlab::GIT_USER,
                self.gitlab_token(repo).map(str::to_string),
            )
        } else {
            let token = self
                .token_for(repo)
                .await
                .map_err(|why| ForgeError::Failed(format!("{what}: {why}")))?;
            (GITHUB_GIT_USER, token)
        };
        if let Some(token) = &token {
            request = request.basic_auth(user, Some(token));
        }
        let response = request
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        match response.status().as_u16() {
            200 => read_capped(response, max, what).await,
            status @ (401 | 403 | 404) => {
                if status == 401 {
                    self.forget_token(repo, token.as_deref());
                }
                Err(ForgeError::Failed(format!(
                    "{what}: HTTP {status}{}",
                    refused_hint(repo, token.is_some(), status)
                )))
            }
            status => Err(ForgeError::Failed(format!("{what}: HTTP {status}"))),
        }
    }

    /// The commit merge request `iid` of a GitLab project is compared with
    /// (its `diff_refs.base_sha`), which GitLab's listing leaves out.
    pub(crate) async fn merge_request_base(
        &self,
        repo: &Repo,
        iid: i64,
    ) -> Result<Option<String>, ForgeError> {
        self.gitlab_merge_request_base(repo, iid).await
    }

    /// `GET /repos/{owner}/{repo}/pulls?state=open`, every page.
    pub(crate) async fn list_pulls(
        &self,
        repo: &Repo,
        etag: Option<&str>,
    ) -> Result<Pulls, ForgeError> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_merge_requests(repo).await;
        }
        const WHAT: &str = "listing pull requests";
        let api = self.api_for(repo);
        let token = self
            .token_for(repo)
            .await
            .map_err(|why| ForgeError::Failed(format!("{WHAT}: {why}")))?;
        let mut events = Vec::new();
        for page in 1..=MAX_PULL_PAGES {
            let mut request = self
                .http
                .to(&api)
                .get(format!(
                    "{api}/repos/{}/pulls?state=open&per_page={PULLS_PER_PAGE}&page={page}",
                    repo.full_name
                ))
                .header(ACCEPT, "application/vnd.github+json")
                .header("x-github-api-version", "2022-11-28");
            if let Some(token) = &token {
                request = request.bearer_auth(token);
            }
            if let (1, Some(etag)) = (page, etag) {
                request = request.header(IF_NONE_MATCH, etag);
            }
            let response = request
                .send()
                .await
                .map_err(|e| ForgeError::Failed(format!("listing pull requests: {e}")))?;
            match response.status().as_u16() {
                304 if page == 1 => return Ok(Pulls::Unchanged),
                200 => {}
                status => {
                    if status == 401 {
                        self.forget_token(repo, token.as_deref());
                    }
                    return Err(api_error(&response, repo, token.is_some(), WHAT));
                }
            }
            let header = |name| {
                response
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            };
            let more = header(LINK).is_some_and(|l| l.contains("rel=\"next\""));
            let page_etag = header(ETAG);
            let body = read_capped(response, MAX_API_PAGE, "listing pull requests").await?;
            events.extend(
                github::parse_open_pulls(&body, &repo.full_name)
                    .map_err(|e| ForgeError::Failed(format!("listing pull requests: {e}")))?,
            );
            if !more {
                return Ok(Pulls::Listed {
                    events,
                    complete: true,
                    etag: if page == 1 { page_etag } else { None },
                });
            }
        }
        tracing::warn!(
            repo = %repo.full_name,
            "more than {} open pull requests; the rest are left to webhooks",
            MAX_PULL_PAGES * PULLS_PER_PAGE
        );
        Ok(Pulls::Listed {
            events,
            complete: false,
            etag: None,
        })
    }
}

/// A pipeline file at a commit: its path, and its text or why it was not read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineFile {
    /// `.dagron/ci/engine.yaml`.
    pub path: String,
    pub text: Result<String, String>,
}

/// An entry of a directory, as the contents API lists it.
#[derive(Deserialize)]
struct Entry {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    path: String,
    #[serde(default)]
    size: u64,
}

/// What the compare API says of a change: its files, unless it lists none,
/// how the second commit stands to the first (`ahead`, `behind`,
/// `identical`, `diverged`), and the commit it compared from, the two's
/// merge base.
#[derive(Deserialize)]
struct Comparison {
    files: Option<Vec<ComparedFile>>,
    status: Option<String>,
    merge_base_commit: Option<MergeBase>,
}

#[derive(Deserialize)]
struct MergeBase {
    sha: String,
}

/// Whether a commit is on a ref ([`Forge::on_ref`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnRef {
    /// The ref's own commit, or one its history holds.
    Yes,
    /// Not on it, and why.
    No(String),
    /// The repository has no such ref.
    NoRef,
}

#[derive(Deserialize)]
struct ComparedFile {
    filename: String,
    previous_filename: Option<String>,
}

impl Forge {
    /// Whether `repo` is read with a credential: the App's token where the
    /// App is installed on it, or CI_GITHUB_TOKEN_FILE's. Without one, GitHub
    /// answers a private repository's files as if there were none.
    ///
    /// With the App alone, said of what the last call for the repository
    /// found: not where the App was not installed on it, or GitHub would not
    /// mint its token. Before any call, that an App is configured.
    pub fn has_token(&self, repo: &Repo) -> bool {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_token(repo).is_some();
        }
        if !Self::on_github_com(repo) {
            return false;
        }
        if self.github_token.is_some() {
            return true;
        }
        let Some(tokens) = &self.app else {
            return false;
        };
        let key = repo.full_name.to_ascii_lowercase();
        let none = tokens
            .held
            .lock()
            .is_ok_and(|held| matches!(held.get(&key), Some(Held::None { .. })));
        !none
    }

    /// Whether `repo`'s pipelines can be read: GitHub's and GitLab's APIs
    /// serve a commit's files. The other forges' wait for their APIs.
    pub fn reads_pipelines(repo: &Repo) -> bool {
        matches!(repo.forge.as_str(), "github" | "gitlab") && is_url_safe(repo)
    }

    /// The pipelines at commit `sha`: each `*.yaml` and `*.yml` file directly
    /// in `.dagron/ci`, in path order. `None` when there is none, or no such
    /// directory. A file that cannot be taken (too large, not text, past the
    /// most a commit may hold) is listed with why, so it fails loudly rather
    /// than never running.
    pub async fn pipelines(
        &self,
        repo: &Repo,
        sha: &str,
    ) -> Result<Option<Vec<PipelineFile>>, ForgeError> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_pipelines(repo, sha).await;
        }
        const WHAT: &str = "listing .dagron/ci";
        let dir: Vec<&str> = std::iter::once("contents")
            .chain(PIPELINE_DIR.split('/'))
            .collect();
        let (response, credentialed) = self
            .api_get(repo, &dir, &[("ref", sha)], JSON, WHAT)
            .await?;
        match response.status().as_u16() {
            200 => {}
            // No such directory at this commit. (GitHub answers an anonymous
            // caller 404 for a private repository too.)
            404 => return Ok(None),
            _ => {
                return Err(api_error(&response, repo, credentialed, WHAT));
            }
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
        let listed: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        if !listed.is_array() {
            return Ok(Some(vec![PipelineFile {
                path: PIPELINE_DIR.into(),
                text: Err(
                    "is a file; pipelines are the *.yaml files in the directory .dagron/ci".into(),
                ),
            }]));
        }
        let entries: Vec<Entry> = serde_json::from_value(listed)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        let mut entries: Vec<Entry> = entries
            .into_iter()
            .filter(|e| e.kind == "file" && (e.name.ends_with(".yaml") || e.name.ends_with(".yml")))
            .collect();
        if entries.is_empty() {
            return Ok(None);
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        let mut files = Vec::with_capacity(entries.len());
        for (i, entry) in entries.into_iter().enumerate() {
            let direct = entry
                .path
                .strip_prefix(PIPELINE_DIR)
                .and_then(|p| p.strip_prefix('/'));
            if direct != Some(entry.name.as_str())
                || entry.name.contains(|c: char| c.is_control() || c == '/')
            {
                files.push(PipelineFile {
                    path: format!("{:?}", entry.path),
                    text: Err("is not a file name dagron-ci takes: it must be printable".into()),
                });
                continue;
            }
            let text = if i >= MAX_PIPELINES {
                Err(format!(
                    "not read: a commit may hold at most {MAX_PIPELINES} pipelines"
                ))
            } else if entry.size > MAX_PIPELINE_BYTES as u64 {
                Err(format!(
                    "is {} bytes, over the {} a pipeline may be",
                    entry.size,
                    size(MAX_PIPELINE_BYTES)
                ))
            } else {
                self.pipeline_text(repo, &entry.name, sha).await?
            };
            files.push(PipelineFile {
                path: entry.path,
                text,
            });
        }
        Ok(Some(files))
    }

    /// The text at `url`, a file of an action's repository as its host
    /// serves it raw ([`crate::pipeline::github::ActionSource::file_url`]):
    /// `None` where there is none (404), and why where it cannot be taken,
    /// a refusal of its host's that lasts (another 4xx) among it, and a host
    /// that cannot be reached at all, which is where dagron-ci runs and not
    /// a moment of the host's. A host that fails as it answers (a 5xx, a
    /// connection lost) is an error, for the event to be handled again. No
    /// credential is sent: an action is a public repository's, or the
    /// operator's mirror's.
    pub async fn action_file(
        &self,
        url: &str,
    ) -> Result<Option<Result<String, String>>, ForgeError> {
        let what = format!("reading {url}");
        let response = match self.http.to(url).get(url).send().await {
            Ok(response) => response,
            Err(e) if e.is_connect() => {
                return Ok(Some(Err(format!("its host could not be reached: {e}"))));
            }
            Err(e) => return Err(ForgeError::Failed(format!("{what}: {e}"))),
        };
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            // Its host's limit: waited for, as a forge's is.
            429 => {
                let wait = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map_or(Duration::from_secs(60), Duration::from_secs);
                return Err(ForgeError::RateLimited {
                    wait,
                    why: format!("{what}: its host's rate limit is spent"),
                });
            }
            status @ 400..=499 => {
                return Ok(Some(Err(format!("its host refused it ({status})"))));
            }
            status => return Err(ForgeError::Failed(format!("{what}: {status}"))),
        }
        Ok(Some(
            match read_up_to(response, MAX_PIPELINE_BYTES, &what).await? {
                Some(bytes) => {
                    String::from_utf8(bytes).map_err(|_| "is not UTF-8 text".to_string())
                }
                None => Err(format!(
                    "is over the {} an action's file may be",
                    size(MAX_PIPELINE_BYTES)
                )),
            },
        ))
    }

    /// The text of the file at `path` in commit `sha`: `None` where the
    /// commit has no such file, and why where it has one that cannot be
    /// taken (too large, not text). For a file that is asked for by its
    /// name: the repository's settings, and a workflow file they list.
    pub async fn file(
        &self,
        repo: &Repo,
        path: &str,
        sha: &str,
    ) -> Result<Option<Result<String, String>>, ForgeError> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_file(repo, path, sha).await;
        }
        let what = format!("reading {path}");
        let segments: Vec<&str> = std::iter::once("contents").chain(path.split('/')).collect();
        let (response, credentialed) = self
            .api_get(repo, &segments, &[("ref", sha)], RAW, &what)
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            _ => return Err(api_error(&response, repo, credentialed, &what)),
        }
        Ok(Some(
            match read_up_to(response, MAX_PIPELINE_BYTES, &what).await? {
                Some(bytes) => {
                    String::from_utf8(bytes).map_err(|_| "is not UTF-8 text".to_string())
                }
                None => Err(format!(
                    "is over the {} a pipeline may be",
                    size(MAX_PIPELINE_BYTES)
                )),
            },
        ))
    }

    /// One pipeline's text at `sha`, or why it is not one.
    async fn pipeline_text(
        &self,
        repo: &Repo,
        name: &str,
        sha: &str,
    ) -> Result<Result<String, String>, ForgeError> {
        let what = format!("reading {PIPELINE_DIR}/{name}");
        let path: Vec<&str> = std::iter::once("contents")
            .chain(PIPELINE_DIR.split('/'))
            .chain(std::iter::once(name))
            .collect();
        let (response, credentialed) = self
            .api_get(repo, &path, &[("ref", sha)], RAW, &what)
            .await?;
        match response.status().as_u16() {
            200 => {}
            // Listed, and not served: a symbolic link to nothing, say.
            404 => return Ok(Err("is listed, and the forge serves no file there".into())),
            _ => {
                return Err(api_error(&response, repo, credentialed, &what));
            }
        }
        Ok(
            match read_up_to(response, MAX_PIPELINE_BYTES, &what).await? {
                Some(bytes) => {
                    String::from_utf8(bytes).map_err(|_| "is not UTF-8 text".to_string())
                }
                None => Err(format!(
                    "is over the {} a pipeline may be",
                    size(MAX_PIPELINE_BYTES)
                )),
            },
        )
    }

    /// The files the change from `base` to `head` touched, a renamed file
    /// under both its names, from the compare API (GitHub's three-dot
    /// comparison: what `head` added since it left `base`). Where the API
    /// compared the two and cannot list the files (a change past what it
    /// lists, a comparison too large to read), from a shallow fetch of the
    /// merge base the API compared from and `head`
    /// ([`Forge::fetched_changes`]). Where neither can, [`Changes::Unlisted`]
    /// with why; and where the forge cannot compare the two at all (no
    /// shared history, a commit it does not have), with that alone.
    pub async fn changed_files(
        &self,
        repo: &Repo,
        base: &str,
        head: &str,
    ) -> Result<Changes, ForgeError> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_changed_files(repo, base, head).await;
        }
        const WHAT: &str = "comparing commits";
        let range = format!("{base}...{head}");
        let (response, credentialed) = self
            .api_get(repo, &["compare", &range], &[], JSON, WHAT)
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 | 422 => {
                return Ok(Changes::Unlisted(format!(
                    "the forge cannot compare {} with {}",
                    short(base),
                    short(head)
                )))
            }
            _ => {
                return Err(api_error(&response, repo, credentialed, WHAT));
            }
        }
        let Some(body) = read_up_to(response, MAX_COMPARISON, WHAT).await? else {
            let why = format!("the comparison is over {}", size(MAX_COMPARISON));
            return Ok(self.fetched_or(repo, base, head, None, why).await);
        };
        let comparison: Comparison = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        // What GitHub compared from: a fetch compares from it too.
        let from = comparison
            .merge_base_commit
            .map(|c| c.sha)
            .filter(|sha| is_commit_id(sha));
        let Some(files) = comparison.files else {
            let why = "the forge listed no files for the change".to_string();
            return Ok(self.fetched_or(repo, base, head, from, why).await);
        };
        if files.len() >= COMPARE_FILE_LIMIT {
            let why = format!(
                "the change touches {COMPARE_FILE_LIMIT} files or more, more than the forge lists"
            );
            return Ok(self.fetched_or(repo, base, head, from, why).await);
        }
        let mut names = BTreeSet::new();
        for file in files {
            names.extend(file.previous_filename);
            names.insert(file.filename);
        }
        Ok(Changes::Listed(names.into_iter().collect()))
    }

    /// [`Changes::Listed`] from a shallow fetch of the change from `base`
    /// to `head`, where the forge's compare API could not list it, for
    /// `why`; from `merge_base` where the comparison named it. Where the
    /// fetch cannot either, [`Changes::Unlisted`], saying why of both.
    async fn fetched_or(
        &self,
        repo: &Repo,
        base: &str,
        head: &str,
        merge_base: Option<String>,
        why: String,
    ) -> Changes {
        let started = Instant::now();
        match self.fetched_changes(repo, base, head, merge_base).await {
            Ok(files) => {
                tracing::info!(
                    repo = %repo.full_name,
                    files = files.len(),
                    ms = started.elapsed().as_millis() as u64,
                    "{why}; listed them through a shallow fetch"
                );
                Changes::Listed(files)
            }
            Err(e) => Changes::Unlisted(format!("{why}; nor could a shallow fetch: {e}")),
        }
    }

    /// The paths where `head` differs from its merge base with `base`, as
    /// `git diff --name-only` lists them (a rename under both names), from
    /// git itself: the two commits fetched at depth 1 over protocol v2,
    /// without their files' contents where the server can leave them out
    /// (`filter blob:none`), and their trees compared ([`pack`]). With the
    /// repository's credential, as [`Forge::list_refs`] reads it. From the
    /// merge base, as the compare API compares: from `base` itself, a forced
    /// push would leave out what its branch changed alike in its old tip and
    /// its new. `merge_base` where the comparison named it, and otherwise
    /// asked for ([`Forge::merge_base`]). Within the fetch's time
    /// ([`FETCH_FOR`]), [`MAX_FETCHED_PACK`] and [`FETCH_LIMITS`], one of
    /// [`FETCHES_AT_ONCE`] in the replica. The error says why it could not.
    async fn fetched_changes(
        &self,
        repo: &Repo,
        base: &str,
        head: &str,
        merge_base: Option<String>,
    ) -> Result<Vec<String>, String> {
        if !is_commit_id(base) || !is_commit_id(head) {
            return Err("the event does not name both commits by their ids".into());
        }
        let until = tokio::time::Instant::now() + self.fetch_for;
        let late = || format!("not done within {} s", self.fetch_for.as_secs());
        let from = match merge_base {
            Some(from) => from,
            None => tokio::time::timeout_at(until, self.merge_base(repo, base, head))
                .await
                .map_err(|_| late())??,
        };
        let turn = tokio::time::timeout_at(until, self.fetches.clone().acquire_owned())
            .await
            .map_err(|_| late())?
            .map_err(|e| e.to_string())?;
        let (pack, format) =
            tokio::time::timeout_at(until, self.fetch_commits(repo, &[&from, head]))
                .await
                .map_err(|_| late())??;
        let limits = pack::Limits {
            until: Some(until.into_std()),
            ..FETCH_LIMITS
        };
        let head = head.to_string();
        // Reading the pack is work, not waiting: off the runtime's threads,
        // and bounded by the limits' own time. The turn goes with the work,
        // and the pack once it is read; the event's worker is let go at the
        // time, however the work stands.
        let work = tokio::task::spawn_blocking(move || {
            let _turn = turn;
            let objects = pack::read(&pack, format, &limits)?;
            drop(pack);
            objects.changed(&from, &head, &limits)
        });
        tokio::time::timeout_at(until, work)
            .await
            .map_err(|_| late())?
            .map_err(|e| format!("reading the pack: {e}"))?
            .map_err(|e| match tokio::time::Instant::now() < until {
                true => format!("reading the pack: {e}"),
                false => late(),
            })
    }

    /// The merge base of `base` and `head`, which the compare API compares
    /// from, where the comparison did not name it. GitHub's from a page of
    /// the comparison past its first, which lists no files, for a first too
    /// large to read; GitLab's, whose comparison names none, from its own
    /// endpoint. The error says why not.
    async fn merge_base(&self, repo: &Repo, base: &str, head: &str) -> Result<String, String> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_merge_base(repo, base, head).await;
        }
        const WHAT: &str = "finding the merge base";
        let said = |e: ForgeError| match e {
            ForgeError::Failed(why) | ForgeError::RateLimited { why, .. } => why,
        };
        let range = format!("{base}...{head}");
        let (response, credentialed) = self
            .api_get(
                repo,
                &["compare", &range],
                &[("per_page", "1"), ("page", "2")],
                JSON,
                WHAT,
            )
            .await
            .map_err(said)?;
        if response.status().as_u16() != 200 {
            return Err(said(api_error(&response, repo, credentialed, WHAT)));
        }
        let body = read_capped(response, MAX_COMPARISON, WHAT)
            .await
            .map_err(said)?;
        let comparison: Comparison =
            serde_json::from_slice(&body).map_err(|e| format!("{WHAT}: {e}"))?;
        comparison
            .merge_base_commit
            .map(|c| c.sha)
            .filter(|sha| is_commit_id(sha))
            .ok_or_else(|| format!("{WHAT}: the forge names none"))
    }

    /// Fetch `wants`, commits of `repo`, at depth 1: the pack its git sends,
    /// and the format its objects are named in. A server that speaks only
    /// protocol v0, or fetches no shallow history, is not asked.
    async fn fetch_commits(
        &self,
        repo: &Repo,
        wants: &[&str],
    ) -> Result<(Vec<u8>, ObjectFormat), String> {
        const WHAT: &str = "fetching the commits";
        if !is_url_safe(repo) {
            return Err(format!(
                "{}/{} cannot be put in a URL as it is",
                repo.forge_host, repo.full_name
            ));
        }
        let said = |e: ForgeError| match e {
            ForgeError::Failed(why) | ForgeError::RateLimited { why, .. } => why,
        };
        let url = self.git_url(repo);
        let advertised = self
            .git_request(
                repo,
                self.http
                    .to(&url)
                    .get(format!("{url}/{}", git::ADVERTISEMENT_PATH)),
                WHAT,
                MAX_REF_LISTING,
            )
            .await
            .map_err(said)?;
        let (object_format, features) = match git::read_advertisement(&advertised, &[]) {
            Ok(Advertisement::V2 {
                object_format,
                fetch: Some(features),
            }) => (object_format, features),
            Ok(Advertisement::V2 { fetch: None, .. }) => {
                return Err(format!("{WHAT}: the server offers no fetch"))
            }
            Ok(Advertisement::V0(_)) => {
                return Err(format!("{WHAT}: the server speaks only git's protocol v0"))
            }
            Err(e) => return Err(format!("{WHAT}: {e}")),
        };
        if !features.iter().any(|f| f == "shallow") {
            return Err(format!("{WHAT}: the server fetches no shallow history"));
        }
        let format =
            ObjectFormat::named(object_format.as_deref()).map_err(|e| format!("{WHAT}: {e}"))?;
        let filter = features.iter().any(|f| f == "filter");
        let body = git::fetch_request(wants, object_format.as_deref(), filter);
        let answer = self
            .git_request(repo, self.git_command(&url, body), WHAT, MAX_FETCHED_PACK)
            .await
            .map_err(said)?;
        let pack = git::read_fetch(answer).map_err(|e| format!("{WHAT}: {e}"))?;
        Ok((pack, format))
    }

    /// Whether `repo`'s commits can be given statuses: through GitHub's API,
    /// with the App's token for the repository or CI_GITHUB_TOKEN_FILE's,
    /// which go to github.com and nowhere else; or through GitLab's, with the
    /// token CI_GITLAB_TOKENS_DIR keeps for the project's host. Said of what
    /// is configured: where the App turns out not to be installed on the
    /// repository and there is no token, the post itself is refused, and says
    /// so.
    pub fn posts_statuses(&self, repo: &Repo) -> bool {
        if gitlab::is_gitlab(repo) {
            return is_url_safe(repo) && self.gitlab_token(repo).is_some();
        }
        repo.forge == "github" && is_url_safe(repo) && self.credentialed(repo)
    }

    /// Post `status` on commit `sha` of `repo`: `POST
    /// {api}/repos/{owner}/{name}/statuses/{sha}`.
    pub async fn post_status(
        &self,
        repo: &Repo,
        sha: &str,
        status: &CommitStatus,
    ) -> Result<(), StatusError> {
        if gitlab::is_gitlab(repo) && is_url_safe(repo) {
            return self.gitlab_post_status(repo, sha, status).await;
        }
        let what = format!("posting {:?} on {}", status.context, short(sha));
        let token = match is_url_safe(repo) {
            true => self
                .token_for(repo)
                .await
                .map_err(|why| StatusError::Failed(format!("{what}: {why}")))?,
            false => None,
        };
        let Some(token) = token else {
            return Err(StatusError::Refused(format!(
                "{what}: statuses go to github.com repositories, with the App installed on the \
                 repository (CI_GITHUB_APP_ID) or with CI_GITHUB_TOKEN_FILE's token"
            )));
        };
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| StatusError::Failed(format!("{what}: {api}: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| StatusError::Failed(format!("{what}: {api} takes no path")))?
            .pop_if_empty()
            .push("repos")
            .extend(repo.full_name.split('/'))
            .extend(["statuses", sha]);
        let mut body = serde_json::json!({
            "state": status.state,
            "context": status.context,
            "description": status.description,
        });
        if let Some(target) = &status.target_url {
            body["target_url"] = serde_json::json!(target);
        }
        let response = self
            .http
            .to(&api)
            .post(url)
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(self.not_taken(response, repo, &what, &token, &STATUS).await)
    }

    /// Whether `repo`'s jobs are reported as check runs: CI_GITHUB_CHECKS
    /// is on, and the App has a token for the repository's check runs. An
    /// error, with why, where GitHub did not answer the mint: it is not
    /// known yet, and a job is not reported as a status meanwhile.
    pub async fn checks(&self, repo: &Repo) -> Result<bool, String> {
        if !self.checks || !Self::on_github_com(repo) {
            return Ok(false);
        }
        Ok(self.app_token_of(repo, Own::Checks).await?.is_some())
    }

    /// Post `check` on commit `sha` of `repo`, and return its id: a new
    /// check run (`POST {api}/repos/{owner}/{name}/check-runs`), or the one
    /// `id` names brought up to date (`PATCH …/check-runs/{id}`).
    pub async fn post_check(
        &self,
        repo: &Repo,
        sha: &str,
        check: &CheckRun,
        id: Option<u64>,
    ) -> Result<u64, StatusError> {
        let what = format!("posting the check {:?} on {}", check.name, short(sha));
        let token = match is_url_safe(repo) {
            true => self
                .app_token_of(repo, Own::Checks)
                .await
                .map_err(|why| StatusError::Failed(format!("{what}: {why}")))?,
            false => None,
        };
        let Some(token) = token else {
            return Err(StatusError::Refused(format!(
                "{what}: check runs go to github.com repositories the App is installed on \
                 (CI_GITHUB_APP_ID), with `checks: write`"
            )));
        };
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| StatusError::Failed(format!("{what}: {api}: {e}")))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| StatusError::Failed(format!("{what}: {api} takes no path")))?;
            path.pop_if_empty()
                .push("repos")
                .extend(repo.full_name.split('/'))
                .push("check-runs");
            if let Some(id) = id {
                path.push(&id.to_string());
            }
        }
        let mut body = serde_json::json!({
            "name": check.name,
            "status": check.status,
            "output": { "title": check.title, "summary": check_text(&check.summary) },
        });
        if !check.text.is_empty() {
            body["output"]["text"] = serde_json::json!(check_text(&check.text));
        }
        if !check.annotations.is_empty() {
            let annotations: Vec<serde_json::Value> = check
                .annotations
                .iter()
                .take(CHECK_ANNOTATIONS_MAX)
                .map(|a| {
                    let mut said = serde_json::json!({
                        "path": a.path,
                        "start_line": a.start_line,
                        "end_line": a.end_line,
                        "annotation_level": a.level,
                        "message": a.message,
                    });
                    if !a.title.is_empty() {
                        said["title"] = serde_json::json!(a.title);
                    }
                    said
                })
                .collect();
            body["output"]["annotations"] = serde_json::json!(annotations);
        }
        if !check.external_id.is_empty() {
            body["external_id"] = serde_json::json!(check.external_id);
        }
        if let Some(conclusion) = check.conclusion {
            body["conclusion"] = serde_json::json!(conclusion);
        }
        if let Some(details) = &check.details_url {
            body["details_url"] = serde_json::json!(details);
        }
        let client = self.http.to(&api);
        let request = match id {
            Some(_) => client.patch(url),
            None => {
                body["head_sha"] = serde_json::json!(sha);
                client.post(url)
            }
        };
        let response = request
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        if !response.status().is_success() {
            return Err(self.not_taken(response, repo, &what, &token, &CHECK).await);
        }
        if let Some(id) = id {
            return Ok(id);
        }
        #[derive(Deserialize)]
        struct Made {
            id: u64,
        }
        response
            .json::<Made>()
            .await
            .map(|made| made.id)
            .map_err(|e| {
                StatusError::Failed(format!("{what}: GitHub's answer names no check run: {e}"))
            })
    }

    /// Whether any pull request is given a comment: it is asked for
    /// (CI_GITHUB_PR_COMMENT), and there is an App to write one as; or any
    /// merge request a note (CI_GITLAB_MR_NOTES), with a token to write it
    /// with.
    pub fn pr_comments(&self) -> bool {
        (self.pr_comment && self.app.is_some()) || (self.mr_notes && !self.gitlab_tokens.is_empty())
    }

    /// Whether `repo`'s pull requests may be given a comment: it is asked
    /// for (CI_GITHUB_PR_COMMENT), there is an App, and the repository is
    /// on github.com. Said of what is configured, without asking GitHub:
    /// where the App turns out not to be installed on the repository, or
    /// was not given the permission, the comment itself is refused, and
    /// says so.
    pub fn comments(&self, repo: &Repo) -> bool {
        if gitlab::is_gitlab(repo) {
            return self.mr_notes && self.gitlab_token(repo).is_some() && is_url_safe(repo);
        }
        self.pr_comment && self.app.is_some() && Self::on_github_com(repo) && is_url_safe(repo)
    }

    /// Whether `repo` may be filed an issue of a job that flakes: it is
    /// asked for (CI_FLAKE_ISSUES), and there is the App, for a repository
    /// on github.com, or the GitLab host's token. Said of what is
    /// configured, as [`Self::comments`] is.
    pub fn issues(&self, repo: &Repo) -> bool {
        if gitlab::is_gitlab(repo) {
            return self.flake_issues && self.gitlab_token(repo).is_some() && is_url_safe(repo);
        }
        self.flake_issues && self.app.is_some() && Self::on_github_com(repo) && is_url_safe(repo)
    }

    /// Open an issue of `repo` titled `title` that says `body`, or, where
    /// `number` names one, write `body` in its place
    /// (`POST {api}/repos/{owner}/{name}/issues`, `PATCH …/issues/{number}`).
    /// One a person closed is left closed, and said to be.
    pub async fn write_issue(
        &self,
        repo: &Repo,
        title: &str,
        body: &str,
        number: Option<&str>,
    ) -> Result<Issued, StatusError> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_write_issue(repo, title, body, number).await;
        }
        let what = match number {
            Some(n) => format!("writing issue #{n}"),
            None => "opening an issue".to_string(),
        };
        if number.is_some_and(|n| n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit())) {
            return Err(StatusError::Failed(format!(
                "{what}: not an issue's number"
            )));
        }
        let token = match self.issues(repo) {
            true => self
                .app_token_of(repo, Own::Issues)
                .await
                .map_err(|why| StatusError::Failed(format!("{what}: {why}")))?,
            false => None,
        };
        let Some(token) = token else {
            return Err(StatusError::Refused(format!(
                "{what}: an issue goes to a github.com repository the App is installed on \
                 (CI_GITHUB_APP_ID), with `issues: write`"
            )));
        };
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| StatusError::Failed(format!("{what}: {api}: {e}")))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| StatusError::Failed(format!("{what}: {api} takes no path")))?;
            path.pop_if_empty()
                .push("repos")
                .extend(repo.full_name.split('/'))
                .push("issues")
                .extend(number);
        }
        let client = self.http.to(&api);
        let (request, sent) = match number {
            Some(_) => (
                client.patch(url),
                serde_json::json!({ "body": comment_text(body) }),
            ),
            None => (
                client.post(url),
                serde_json::json!({ "title": title, "body": comment_text(body) }),
            ),
        };
        let response = request
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .json(&sent)
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        // Deleted, or moved to a repository the App cannot see.
        if number.is_some() && matches!(response.status().as_u16(), 404 | 410) {
            return Ok(Issued::Gone);
        }
        // A repository with its issues turned off: asked again, it would say
        // so again, until someone turns them on.
        if number.is_none() && response.status().as_u16() == 410 {
            return Err(StatusError::Refused(format!(
                "{what}: HTTP 410; the repository has its issues turned off"
            )));
        }
        if !response.status().is_success() {
            return Err(self.not_taken(response, repo, &what, &token, &ISSUE).await);
        }
        #[derive(Deserialize)]
        struct Made {
            number: u64,
            state: String,
            closed_at: Option<String>,
        }
        let made = response.json::<Made>().await.map_err(|e| {
            StatusError::Failed(format!("{what}: GitHub's answer names no issue: {e}"))
        })?;
        Ok(match made.state.as_str() {
            "closed" => Issued::Closed(made.closed_at),
            _ => Issued::Written(made.number.to_string()),
        })
    }

    /// Write `body` as a comment on pull request `pr` of `repo`: a new one
    /// (`POST {api}/repos/{owner}/{name}/issues/{pr}/comments`), or the one
    /// `id` names with `body` in place of what it said (`PATCH
    /// …/issues/comments/{id}`). A pull request's conversation is an
    /// issue's, which is why its comments are under `issues`.
    pub async fn post_comment(
        &self,
        repo: &Repo,
        pr: i64,
        body: &str,
        id: Option<&str>,
    ) -> Result<Commented, StatusError> {
        if gitlab::is_gitlab(repo) {
            return self.gitlab_post_note(repo, pr, body, id).await;
        }
        let what = format!("writing the comment on pull request {pr}");
        let token = match self.comments(repo) {
            true => self
                .app_token_of(repo, Own::Comments)
                .await
                .map_err(|why| StatusError::Failed(format!("{what}: {why}")))?,
            false => None,
        };
        let Some(token) = token else {
            return Err(StatusError::Refused(format!(
                "{what}: a comment goes to a github.com repository the App is installed on \
                 (CI_GITHUB_APP_ID), with `pull requests: write`"
            )));
        };
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| StatusError::Failed(format!("{what}: {api}: {e}")))?;
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| StatusError::Failed(format!("{what}: {api} takes no path")))?;
            path.pop_if_empty()
                .push("repos")
                .extend(repo.full_name.split('/'))
                .push("issues");
            match id {
                Some(id) => path.extend(["comments", id]),
                None => path.extend([pr.to_string().as_str(), "comments"]),
            };
        }
        let client = self.http.to(&api);
        let request = match id {
            Some(_) => client.patch(url),
            None => client.post(url),
        };
        let response = request
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .json(&serde_json::json!({ "body": comment_text(body) }))
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        if id.is_some() && response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Commented::Gone);
        }
        if !response.status().is_success() {
            return Err(self
                .not_taken(response, repo, &what, &token, &COMMENT)
                .await);
        }
        #[derive(Deserialize)]
        struct Made {
            id: u64,
        }
        response
            .json::<Made>()
            .await
            .map(|made| Commented::Written(made.id))
            .map_err(|e| {
                StatusError::Failed(format!("{what}: GitHub's answer names no comment: {e}"))
            })
    }

    /// Say of a job that was not run what `status` says, under the job's
    /// own name, where the repository's jobs are said: as a check run that
    /// is over and `skipped` where they are check runs ([`Self::checks`]),
    /// and as the commit status it is anywhere else.
    ///
    /// A status and a check run of one name are two lines on a commit, and
    /// neither replaces the other. A run's own jobs are posted after what is
    /// said here so as to be the last word under their names
    /// ([`crate::db::Db::report_pipeline_again`]), which they are only
    /// where both are the same kind of thing.
    pub async fn post_skipped_job(
        &self,
        repo: &Repo,
        sha: &str,
        status: &CommitStatus,
    ) -> Result<(), StatusError> {
        match self.checks(repo).await {
            Ok(false) => self.post_status(repo, sha, status).await,
            Ok(true) => {
                let check = CheckRun {
                    name: status.context.clone(),
                    status: "completed",
                    conclusion: Some("skipped"),
                    title: status.description.clone(),
                    summary: "dagron-ci did not run this job.".into(),
                    details_url: status.target_url.clone(),
                    ..CheckRun::default()
                };
                self.post_check(repo, sha, &check, None).await.map(drop)
            }
            Err(why) => Err(StatusError::Failed(format!(
                "posting {:?} on {}: whether the repository takes check runs is not known \
                 yet: {why}",
                status.context,
                short(sha)
            ))),
        }
    }

    /// The token `repo`'s runners are registered and removed with, or why
    /// there is none: the repository's own, or for `org`'s runners its
    /// owner's.
    async fn runners_token(
        &self,
        repo: &Repo,
        org: bool,
        what: &str,
    ) -> Result<String, ForgeError> {
        if !Self::on_github_com(repo) || !is_url_safe(repo) {
            return Err(ForgeError::Failed(format!(
                "{what}: runners are registered for github.com repositories"
            )));
        }
        let own = match org {
            true => Own::OrgRunners,
            false => Own::Runners,
        };
        match self.app_token_of(repo, own).await {
            Ok(Some(token)) => Ok(token),
            Ok(None) if org => Err(ForgeError::Failed(format!(
                "{what}: the App has no token for the organization's runners; it is installed \
                 on the organization with `organization_self_hosted_runners: write`, or no \
                 runner is started"
            ))),
            Ok(None) => Err(ForgeError::Failed(format!(
                "{what}: the App has no token for this repository's runners; it is installed \
                 on the repository with `administration: write`, or no runner is started"
            ))),
            Err(why) => Err(ForgeError::Failed(format!("{what}: {why}"))),
        }
    }

    /// `{api}/{under…}/{rest…}`: a path of GitHub's REST API, from segments.
    fn api_url(
        &self,
        repo: &Repo,
        under: &[&str],
        rest: &[&str],
        what: &str,
    ) -> Result<(String, Url), ForgeError> {
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| ForgeError::Failed(format!("{what}: {api}: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| ForgeError::Failed(format!("{what}: {api} takes no path")))?
            .pop_if_empty()
            .extend(under)
            .extend(rest);
        Ok((api, url))
    }

    /// `{api}/repos/{owner}/{name}/actions/runners/{rest…}`, or for `org`
    /// `{api}/orgs/{org}/actions/runners/{rest…}`.
    fn runners_url(
        &self,
        repo: &Repo,
        org: Option<&str>,
        rest: &[&str],
        what: &str,
    ) -> Result<(String, Url), ForgeError> {
        let mut under: Vec<&str> = match org {
            Some(org) => vec!["orgs", org],
            None => std::iter::once("repos")
                .chain(repo.full_name.split('/'))
                .collect(),
        };
        under.extend(["actions", "runners"]);
        self.api_url(repo, &under, rest, what)
    }

    /// Register a just-in-time runner named `name` for `repo`, with
    /// `labels`: `POST …/actions/runners/generate-jitconfig`. The runner
    /// takes one job that asks for those labels, and is then gone from
    /// GitHub. What comes back is the configuration its process is started
    /// with, which is a secret: whoever holds it is the runner.
    ///
    /// With `group`, the runner is the repository's owner's, an
    /// organization's, in its runner group of that id: GitHub may give it a
    /// job of any repository the group lets in.
    pub async fn jit_runner(
        &self,
        repo: &Repo,
        name: &str,
        labels: &[String],
        group: Option<u64>,
    ) -> Result<Jit, ForgeError> {
        let what = format!("registering the runner {name:?}");
        let org = group.map(|_| owner_of(repo));
        let token = self.runners_token(repo, org.is_some(), &what).await?;
        let (api, url) = self.runners_url(repo, org.as_deref(), &["generate-jitconfig"], &what)?;
        let response = self
            .http
            .to(&api)
            .post(url)
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .json(&serde_json::json!({
                "name": name,
                // A repository's runners are in its one group, which every
                // repository has; an organization's in the class's.
                "runner_group_id": group.unwrap_or(1),
                "labels": labels,
                "work_folder": "_work",
            }))
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        let own = match org {
            Some(_) => Own::OrgRunners,
            None => Own::Runners,
        };
        if !response.status().is_success() {
            if response.status().as_u16() == 401 {
                self.forget_token_of(own, repo, Some(&token));
            }
            return Err(api_error(&response, repo, true, &what));
        }
        #[derive(Deserialize)]
        struct Made {
            runner: MadeRunner,
            encoded_jit_config: String,
        }
        #[derive(Deserialize)]
        struct MadeRunner {
            id: u64,
        }
        let made: Made = response.json().await.map_err(|e| {
            ForgeError::Failed(format!("{what}: GitHub's answer is no runner's: {e}"))
        })?;
        if made.encoded_jit_config.is_empty() {
            return Err(ForgeError::Failed(format!(
                "{what}: GitHub's answer has no configuration"
            )));
        }
        Ok(Jit {
            runner_id: made.runner.id,
            config: made.encoded_jit_config,
        })
    }

    /// Take the runner `runner_id` of `repo` back: `DELETE
    /// …/actions/runners/{id}`, of the organization `org` where it is one's.
    /// `true` once GitHub has none by that id, whether it removed it now or
    /// had not got it. `false` where it is running a job, and GitHub keeps
    /// it until the job is over.
    pub async fn remove_runner(
        &self,
        repo: &Repo,
        runner_id: &str,
        org: Option<&str>,
    ) -> Result<bool, ForgeError> {
        let what = format!("removing the runner {runner_id}");
        if runner_id.is_empty() || !runner_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(true);
        }
        let token = self.runners_token(repo, org.is_some(), &what).await?;
        let (api, url) = self.runners_url(repo, org, &[runner_id], &what)?;
        let response = self
            .http
            .to(&api)
            .delete(url)
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        let own = match org {
            Some(_) => Own::OrgRunners,
            None => Own::Runners,
        };
        match response.status().as_u16() {
            200..=299 | 404 => Ok(true),
            422 => Ok(false),
            401 => {
                self.forget_token_of(own, repo, Some(&token));
                Err(api_error(&response, repo, true, &what))
            }
            _ => Err(api_error(&response, repo, true, &what)),
        }
    }

    /// A GET of `repo`'s Actions API, with the token for its workflow runs:
    /// the answer's body, or `None` where GitHub has no such thing (404).
    async fn actions_get(
        &self,
        repo: &Repo,
        rest: &[&str],
        what: &str,
    ) -> Result<Option<Vec<u8>>, ForgeError> {
        if !Self::on_github_com(repo) || !is_url_safe(repo) {
            return Err(ForgeError::Failed(format!(
                "{what}: workflow runs are read of github.com repositories"
            )));
        }
        let token = match self.app_token_of(repo, Own::Actions).await {
            Ok(Some(token)) => token,
            Ok(None) => {
                return Err(ForgeError::Failed(format!(
                    "{what}: the App has no token for this repository's workflow runs; it is \
                     installed on the repository with `actions: read`"
                )))
            }
            Err(why) => return Err(ForgeError::Failed(format!("{what}: {why}"))),
        };
        let mut under: Vec<&str> = std::iter::once("repos")
            .chain(repo.full_name.split('/'))
            .collect();
        under.push("actions");
        let (api, url) = self.api_url(repo, &under, rest, what)?;
        let response = self
            .http
            .to(&api)
            .get(url)
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        match response.status().as_u16() {
            200 => read_capped(response, MAX_API_PAGE, what).await.map(Some),
            404 => Ok(None),
            code => {
                if code == 401 {
                    self.forget_token_of(Own::Actions, repo, Some(&token));
                }
                Err(api_error(&response, repo, true, what))
            }
        }
    }

    /// How the job `job_id` of `repo` stands, as GitHub answers for it
    /// (`GET …/actions/jobs/{id}`): what a delivery of it would say now.
    /// `None` where GitHub has no such job, as for one whose workflow run
    /// was deleted.
    pub async fn workflow_job(
        &self,
        repo: &Repo,
        job_id: &str,
    ) -> Result<Option<github::Job>, ForgeError> {
        let what = format!("reading the workflow job {job_id}");
        if job_id.is_empty() || !job_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(None);
        }
        let Some(body) = self.actions_get(repo, &["jobs", job_id], &what).await? else {
            return Ok(None);
        };
        github::job_answer(&repo.forge_host, &repo.full_name, &body)
            .map(Some)
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))
    }

    /// Run the job `job_id` of `repo` again, as a new attempt of its
    /// workflow run: `POST …/actions/jobs/{id}/rerun`, with the token for
    /// running its jobs again. What GitHub refuses for now (its run is still
    /// going, a limit, a token it would not mint) is [`Retried::NotYet`]:
    /// asked again later.
    pub async fn retry_job(&self, repo: &Repo, job_id: &str) -> Result<Retried, ForgeError> {
        let what = format!("running the workflow job {job_id} again");
        if job_id.is_empty() || !job_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(Retried::Refused(format!(
                "{what}: {job_id:?} is no job's id"
            )));
        }
        if !Self::on_github_com(repo) || !is_url_safe(repo) {
            return Ok(Retried::Refused(format!(
                "{what}: jobs are run again on github.com repositories"
            )));
        }
        let token = match self.app_token_of(repo, Own::Retries).await {
            Ok(Some(token)) => token,
            Ok(None) => {
                return Ok(Retried::NotYet(format!(
                    "{what}: the App has no token for running this repository's jobs again; it \
                     is installed on the repository with `actions: write`"
                )))
            }
            Err(why) => return Ok(Retried::NotYet(format!("{what}: {why}"))),
        };
        let mut under: Vec<&str> = std::iter::once("repos")
            .chain(repo.full_name.split('/'))
            .collect();
        under.push("actions");
        let (api, url) = self.api_url(repo, &under, &["jobs", job_id, "rerun"], &what)?;
        let response = self
            .http
            .to(&api)
            .post(url)
            .header(ACCEPT, JSON)
            .header("x-github-api-version", API_VERSION)
            .bearer_auth(&token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        let code = response.status().as_u16();
        match code {
            200..=299 => Ok(Retried::Asked),
            404 => Ok(Retried::Refused(format!("{what}: GitHub has no such job"))),
            _ => {
                if code == 401 {
                    self.forget_token_of(Own::Retries, repo, Some(&token));
                }
                let said: String = response
                    .text()
                    .await
                    .unwrap_or_default()
                    .trim()
                    .chars()
                    .take(300)
                    .collect();
                Ok(Retried::NotYet(format!("{what}: HTTP {code}: {said}")))
            }
        }
    }

    /// Whether the workflow run `run_id` of `repo` has its head in the
    /// repository: `Some(false)` for one whose head is another's, a fork's
    /// pull request, or is gone; `None` where GitHub has no such run.
    pub async fn workflow_run_is_own(
        &self,
        repo: &Repo,
        run_id: &str,
    ) -> Result<Option<bool>, ForgeError> {
        let what = format!("reading the workflow run {run_id}");
        if run_id.is_empty() || !run_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(None);
        }
        let Some(body) = self.actions_get(repo, &["runs", run_id], &what).await? else {
            return Ok(None);
        };
        #[derive(Deserialize)]
        struct Run {
            #[serde(default)]
            head_repository: Option<Named>,
            repository: Named,
        }
        #[derive(Deserialize)]
        struct Named {
            full_name: String,
        }
        let run: Run = serde_json::from_slice(&body).map_err(|e| {
            ForgeError::Failed(format!("{what}: GitHub's answer is no workflow run's: {e}"))
        })?;
        Ok(Some(run.head_repository.is_some_and(|head| {
            head.full_name
                .eq_ignore_ascii_case(&run.repository.full_name)
        })))
    }

    /// Why GitHub did not take a status or a check run, from its answer
    /// `response`, which is no success. `token` is what the post carried.
    async fn not_taken(
        &self,
        response: reqwest::Response,
        repo: &Repo,
        what: &str,
        token: &str,
        how: &Refusals,
    ) -> StatusError {
        let code = response.status().as_u16();
        if code == 401 {
            // An installation token held a while is no longer good, which is
            // nothing GitHub has against the status: the next post mints
            // another. (A read's 401 is tried again whatever it carried.)
            let held = self.forget_token_of(how.own, repo, Some(token));
            if held.is_some_and(|held| held >= STALE_AFTER) {
                return StatusError::Failed(format!(
                    "{what}: HTTP 401 to an installation token held for {} s; another is minted",
                    held.unwrap_or_default().as_secs()
                ));
            }
        }
        if let ForgeError::RateLimited { wait, why } = api_error(&response, repo, true, what) {
            return StatusError::RateLimited { wait, why };
        }
        // Two answers have two meanings each, and only the body tells them
        // apart. A 403 is a token without the permission, or a secondary rate
        // limit whose headers name no wait. A 422 is a commit GitHub does not
        // have, or a status it will not take for itself. A refusal is not
        // asked again, so the body is read before one is declared.
        let words = if matches!(code, 403 | 422) {
            match refusal_said(response, what).await {
                Ok(words) => words,
                // Which of the two it was is not known, so it is not taken
                // for one that is never asked again.
                Err(ForgeError::Failed(why) | ForgeError::RateLimited { why, .. }) => {
                    return StatusError::Failed(format!("{why}, reading what an HTTP {code} said"));
                }
            }
        } else {
            String::new()
        };
        let said = words.to_ascii_lowercase();
        if code == 403 && said.contains("rate limit") {
            let secs = UNNAMED_RATE_LIMIT_WAIT.as_secs();
            return StatusError::RateLimited {
                wait: UNNAMED_RATE_LIMIT_WAIT,
                why: format!(
                    "{what}: HTTP 403, a rate limit GitHub named no wait for; retry after {secs} s"
                ),
            };
        }
        let refused = |hint: &str| StatusError::Refused(format!("{what}: HTTP {code}; {hint}"));
        match code {
            401 | 403 => refused(how.denied),
            404 => refused(how.missing),
            // GitHub's words for a commit it does not have. No status of it
            // can be posted.
            422 if said.contains("no commit found") => refused("GitHub has no such commit"),
            // Any other 422 is about this status: GitHub keeps a thousand
            // under one context on a commit and then takes no more, or the
            // status itself is not one it takes. Read as that where the body
            // says nothing, too: taking it for the commit would silence
            // every other status of the run, and taking a missing commit for
            // this costs one refused post for each of the run's contexts.
            422 => {
                // In GitHub's own words where it gave any, so that the run's
                // record says which of the two it was.
                let detail = match refusal_detail(&words) {
                    Some(detail) => format!(": {detail}"),
                    None => String::new(),
                };
                StatusError::ThisStatus(format!("{what}: HTTP 422; {}{detail}", how.this))
            }
            _ => StatusError::Failed(format!("{what}: HTTP {code}")),
        }
    }

    /// `GET {api}/repos/{owner}/{name}/{segments…}?{query}`, each segment
    /// encoded as one. The response, whatever its status, and whether the
    /// request carried a credential.
    async fn api_get(
        &self,
        repo: &Repo,
        segments: &[&str],
        query: &[(&str, &str)],
        accept: &str,
        what: &str,
    ) -> Result<(reqwest::Response, bool), ForgeError> {
        if !is_url_safe(repo) {
            return Err(ForgeError::Failed(format!(
                "{}/{} cannot be put in a URL as it is; reconnect it",
                repo.forge_host, repo.full_name
            )));
        }
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| ForgeError::Failed(format!("{what}: {api}: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| ForgeError::Failed(format!("{what}: {api} takes no path")))?
            .pop_if_empty()
            .push("repos")
            .extend(repo.full_name.split('/'))
            .extend(segments);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        let mut request = self
            .http
            .to(&api)
            .get(url)
            .header(ACCEPT, accept)
            .header("x-github-api-version", API_VERSION);
        let token = self
            .token_for(repo)
            .await
            .map_err(|why| ForgeError::Failed(format!("{what}: {why}")))?;
        if let Some(token) = &token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            self.forget_token(repo, token.as_deref());
        }
        Ok((response, token.is_some()))
    }
}

/// A commit id cut to the twelve digits a person reads.
fn short(sha: &str) -> &str {
    &sha[..sha.len().min(12)]
}

/// The API of the GitHub a repository lives on: github.com's, a GitHub
/// Enterprise Cloud tenant's (`*.ghe.com`), or a GitHub Enterprise Server's.
fn github_api(host: &str) -> String {
    match host {
        github::DEFAULT_HOST => "https://api.github.com".into(),
        h if h.ends_with(".ghe.com") => format!("https://api.{h}"),
        h => format!("https://{h}/api/v3"),
    }
}

/// A host and full name that go into a URL as they are: no userinfo, query,
/// fragment or dot segment can come of them.
pub(crate) fn is_url_safe(repo: &Repo) -> bool {
    let name = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    let host = repo
        .forge_host
        .split_once(':')
        .map_or(repo.forge_host.as_str(), |(host, port)| {
            if port.bytes().all(|b| b.is_ascii_digit()) && !port.is_empty() {
                host
            } else {
                ""
            }
        });
    name(host) && repo.full_name.split('/').all(name)
}

/// What to do about a 401, 403 or 404. GitHub answers 404 to an anonymous
/// request for a private repository through its API, and 401 through git.
fn refused_hint(repo: &Repo, has_token: bool, status: u16) -> &'static str {
    if gitlab::is_gitlab(repo) {
        return match has_token {
            true => "; the token CI_GITLAB_TOKENS_DIR keeps for this host cannot read this project",
            false => {
                "; a private project needs a token for its host in CI_GITLAB_TOKENS_DIR, one \
                 that can read it"
            }
        };
    }
    let github_com = repo.forge_host == github::DEFAULT_HOST && repo.forge == "github";
    match (github_com, has_token, status) {
        (false, _, _) => {
            "; dagron-ci sends no credentials to this host, so only a public repository can be read"
        }
        (true, true, _) => "; the credential cannot read this repository",
        (true, false, 403) => "; the forge refused a request without credentials",
        (true, false, _) => {
            "; a private repository needs the App installed on it (CI_GITHUB_APP_ID), or \
             CI_GITHUB_TOKEN_FILE, a token that can read it"
        }
    }
}

/// How a non-200 API answer to `what` fails a read. GitHub's rate limits answer
/// 403 or 429, with `retry-after` (a secondary limit) or a spent
/// `x-ratelimit-remaining` and when it resets.
fn api_error(response: &reqwest::Response, repo: &Repo, has_token: bool, what: &str) -> ForgeError {
    let status = response.status().as_u16();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
    };
    if matches!(status, 403 | 429) {
        if let Some(secs) = header(RETRY_AFTER.as_str()).and_then(|v| v.parse::<u64>().ok()) {
            return ForgeError::RateLimited {
                wait: Duration::from_secs(secs),
                why: format!("{what}: HTTP {status}, a secondary rate limit; retry after {secs} s"),
            };
        }
        if header("x-ratelimit-remaining") == Some("0") || status == 429 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            let wait = header("x-ratelimit-reset")
                .and_then(|v| v.parse::<u64>().ok())
                .map_or(60, |reset| reset.saturating_sub(now).max(1));
            let hint = if has_token {
                ""
            } else if gitlab::is_gitlab(repo) {
                "; a token for the host in CI_GITLAB_TOKENS_DIR is given a limit of its own"
            } else {
                "; install the App on the repository (CI_GITHUB_APP_ID) or set \
                 CI_GITHUB_TOKEN_FILE for more than 60 requests an hour"
            };
            return ForgeError::RateLimited {
                wait: Duration::from_secs(wait),
                why: format!("{what}: HTTP {status}, the rate limit is spent for {wait} s{hint}"),
            };
        }
    }
    let hint = match status {
        401 | 403 | 404 => refused_hint(repo, has_token, status),
        _ => "",
    };
    ForgeError::Failed(format!("{what}: HTTP {status}{hint}"))
}

/// What a refusal's body says: GitHub's message for a rate limit ("You have
/// exceeded a secondary rate limit", "API rate limit exceeded") or for a
/// commit it does not have ("No commit found for SHA"). A body over
/// [`REFUSAL_BODY_MAX`] is not GitHub's message, and says nothing. One that
/// cannot be read is an error: the refusal is not known for what it is.
async fn refusal_said(response: reqwest::Response, what: &str) -> Result<String, ForgeError> {
    let body = read_up_to(response, REFUSAL_BODY_MAX, what).await?;
    Ok(body
        .map(|body| String::from_utf8_lossy(&body).into_owned())
        .unwrap_or_default())
}

/// GitHub's reasons in a refusal's body, for a reason of dagron-ci's own:
/// each validation error's message, or the body's own where it lists none.
/// At most [`REFUSAL_EXCERPT`] characters, on one line. `None` where the
/// body is not GitHub's JSON, or gives no message.
fn refusal_detail(words: &str) -> Option<String> {
    let body: serde_json::Value = serde_json::from_str(words).ok()?;
    let errors: Vec<&str> = body["errors"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| e["message"].as_str())
        .collect();
    let detail = if errors.is_empty() {
        body["message"].as_str()?.to_string()
    } else {
        errors.join("; ")
    };
    let detail: String = detail
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(REFUSAL_EXCERPT)
        .collect();
    (!detail.is_empty()).then_some(detail)
}

async fn read_capped(
    response: reqwest::Response,
    max: usize,
    what: &str,
) -> Result<Vec<u8>, ForgeError> {
    read_up_to(response, max, what)
        .await?
        .ok_or_else(|| ForgeError::Failed(format!("{what}: the answer is over {}", size(max))))
}

/// The body, or `None` when it is over `max` bytes.
async fn read_up_to(
    mut response: reqwest::Response,
    max: usize,
    what: &str,
) -> Result<Option<Vec<u8>>, ForgeError> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?
    {
        if body.len() + chunk.len() > max {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// `bytes`, in MiB or KiB, whichever it is a whole number of.
fn size(bytes: usize) -> String {
    if bytes >= 1 << 20 && bytes.is_multiple_of(1 << 20) {
        format!("{} MiB", bytes >> 20)
    } else {
        format!("{} KiB", bytes >> 10)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::Router;
    use serde_json::json;

    use super::*;

    /// A GitHub serving `acme/widgets`: the contents API for `.dagron/ci`, the
    /// compare API, and the Statuses API.
    #[derive(Default)]
    pub(crate) struct GithubState {
        /// `.dagron/ci` at every commit, `(name, text)`; `None`: no such
        /// directory.
        pub(crate) pipelines: Option<Vec<(String, String)>>,
        /// More entries listed in it, `(type, name, size)`, served as nothing.
        pub(crate) listed: Vec<(&'static str, String, u64)>,
        /// Files anywhere else in the repository, by path, at every commit.
        pub(crate) files: Vec<(String, String)>,
        /// Files of other repositories, served raw at `/raw/<rest>`, by rest.
        pub(crate) raw: Vec<(String, String)>,
        /// Files of other repositories, served by the contents API, by
        /// `owner/repo/path` and the ref asked for.
        pub(crate) theirs: Vec<(String, String, String)>,
        /// Other repositories' visibility and Actions access, by
        /// `owner/repo`: whether it is private, and its `access_level`.
        pub(crate) shared: Vec<(String, bool, String)>,
        /// The files the compare API lists, `(filename, previous_filename)`.
        pub(crate) changed: Vec<(String, Option<String>)>,
        /// A status the compare API answers instead.
        pub(crate) compare_answer: Option<u16>,
        /// A status and headers every request is answered instead.
        pub(crate) answer: Option<(u16, Vec<(&'static str, String)>)>,
        /// The body that answer carries.
        pub(crate) answer_body: String,
        /// A status and headers each status post is answered instead, while
        /// every other request is served.
        pub(crate) status_answer: Option<(u16, Vec<(&'static str, String)>)>,
        /// Statuses the next status posts are answered instead, one each.
        pub(crate) status_answers: std::collections::VecDeque<u16>,
        /// A gate a status post waits at: it says it has arrived (the first
        /// of the pair), and is taken once it is let in (the second). For a
        /// test of what happens while a status is on its way.
        pub(crate) status_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        /// Each request: its path and query, Authorization, and Accept.
        pub(crate) seen: Vec<(String, String, String)>,
        /// Each status taken: its commit, Authorization, and body.
        pub(crate) statuses: Vec<(String, String, serde_json::Value)>,
        /// The repositories the App is installed on, `owner/name`.
        pub(crate) installed: BTreeSet<String>,
        /// Each installation token minted: the repository it names, the
        /// request's body, and its Authorization. The token is
        /// `ghs_{name}_{n}`, the n-th this GitHub minted.
        pub(crate) minted: Vec<(String, serde_json::Value, String)>,
        /// A status the mint is answered instead.
        pub(crate) mint_answer: Option<u16>,
        /// The App was not given `checks: write`: a mint that asks for it is
        /// refused, and any other is served.
        pub(crate) no_checks: bool,
        /// Each check run taken: its id, the commit a new one names (none,
        /// for one brought up to date), Authorization, and body.
        pub(crate) checks: Vec<(u64, Option<String>, String, serde_json::Value)>,
        /// A status each check run is answered instead.
        pub(crate) check_answer: Option<u16>,
        /// The App was not given a repository's `administration`: a mint
        /// that asks for it is refused.
        pub(crate) no_runners: bool,
        /// Each runner registered: its id, name, labels, and Authorization.
        pub(crate) runners: Vec<(u64, String, Vec<String>, String)>,
        /// The ids of the runners asked to be removed, in order.
        pub(crate) removed: Vec<u64>,
        /// A runner that is running a job, and is not removed (422).
        pub(crate) busy_runner: Option<u64>,
        /// Each runner registered for the organization `acme`: its id,
        /// name, labels, Authorization, and runner group.
        pub(crate) org_runners: Vec<(u64, String, Vec<String>, String, u64)>,
        /// The ids of the organization's runners asked to be removed.
        pub(crate) org_removed: Vec<u64>,
        /// The App was not given `actions: read`: a mint that asks for it
        /// is refused.
        pub(crate) no_actions: bool,
        /// The workflow runs whose head is a fork's. Any other run of
        /// `acme/widgets` is its own, but those in `gone_runs`.
        pub(crate) fork_runs: BTreeSet<u64>,
        pub(crate) gone_runs: BTreeSet<u64>,
        /// The workflow runs GitHub answers with an error, as in an outage.
        pub(crate) broken_runs: BTreeSet<u64>,
        /// Each workflow job GitHub has, as `GET …/actions/jobs/{id}`
        /// answers it; any other is not found.
        pub(crate) jobs: BTreeMap<u64, serde_json::Value>,
        /// The jobs asked to be run again, in order, and the status each
        /// ask is answered instead of 201.
        pub(crate) reruns: Vec<u64>,
        pub(crate) rerun_answer: Option<(u16, &'static str)>,
        /// The repository whose git `acme/widgets.git` serves, through the
        /// system's git ([`serve_git`]); none is served without one.
        pub(crate) git: Option<std::path::PathBuf>,
        /// What `acme/widgets.git` advertises instead of its git's own.
        pub(crate) advertisement: Option<Vec<u8>>,
        /// What `acme/widgets.git` answers a fetch with instead of its git.
        pub(crate) fetched: Option<Vec<u8>>,
        /// The commit the compare API names as the merge base.
        pub(crate) merge_base: Option<String>,
        /// The body the compare API answers instead of its files.
        pub(crate) comparison: Option<serde_json::Value>,
    }

    pub(crate) type StubGithub = Arc<Mutex<GithubState>>;

    pub(crate) async fn stub_github() -> (String, StubGithub) {
        async fn handle(
            State(state): State<StubGithub>,
            method: axum::http::Method,
            uri: Uri,
            headers: HeaderMap,
            body: axum::body::Bytes,
        ) -> Response {
            let h = |n: &str| {
                headers
                    .get(n)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            // A status post waits at the gate, if there is one, before
            // anything of it is taken.
            let gate = match uri.path().starts_with("/repos/acme/widgets/statuses/") {
                true => state.lock().unwrap().status_gate.clone(),
                false => None,
            };
            if let Some((reached, go)) = gate {
                reached.notify_one();
                go.notified().await;
            }
            let mut gh = state.lock().unwrap();
            gh.seen.push((
                uri.path_and_query()
                    .map(|p| p.to_string())
                    .unwrap_or_default(),
                h("authorization"),
                h("accept"),
            ));
            if let Some((code, extra)) = &gh.answer {
                let mut res =
                    (StatusCode::from_u16(*code).unwrap(), gh.answer_body.clone()).into_response();
                for (name, value) in extra {
                    res.headers_mut().insert(*name, value.parse().unwrap());
                }
                return res;
            }
            let path = uri.path();
            // The App's own endpoints, for any repository.
            if let Some(full_name) = path
                .strip_prefix("/repos/")
                .and_then(|rest| rest.strip_suffix("/installation"))
            {
                // GitHub knows a repository by its name in any case.
                return match gh.installed.contains(&full_name.to_ascii_lowercase()) {
                    true => axum::Json(json!({ "id": 77 })).into_response(),
                    false => StatusCode::NOT_FOUND.into_response(),
                };
            }
            if path == "/app/installations/77/access_tokens" {
                if let Some(code) = gh.mint_answer {
                    return (
                        StatusCode::from_u16(code).unwrap(),
                        axum::Json(json!({ "message": "The permissions requested are not granted to this installation." })),
                    )
                        .into_response();
                }
                let asked: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let lacks =
                    |what: &str, lacked: bool| lacked && asked["permissions"].get(what).is_some();
                if lacks("administration", gh.no_runners) || lacks("actions", gh.no_actions) {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        axum::Json(json!({ "message": "The permissions requested are not granted to this installation." })),
                    )
                        .into_response();
                }
                if gh.no_checks && asked["permissions"].get("checks").is_some() {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        axum::Json(json!({ "message": "The permissions requested are not granted to this installation." })),
                    )
                        .into_response();
                }
                let name = asked["repositories"][0].as_str().unwrap_or("").to_string();
                let token = format!("ghs_{name}_{}", gh.minted.len() + 1);
                gh.minted.push((name, asked, h("authorization")));
                return (
                    StatusCode::CREATED,
                    axum::Json(json!({ "token": token, "expires_at": "2099-01-01T00:00:00Z" })),
                )
                    .into_response();
            }
            if path == "/repos/acme/widgets/actions/runners/generate-jitconfig" {
                let asked: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let id = gh.runners.len() as u64 + 20;
                let name = asked["name"].as_str().unwrap_or("").to_string();
                let labels = asked["labels"]
                    .as_array()
                    .map(|l| {
                        l.iter()
                            .filter_map(|l| l.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                gh.runners
                    .push((id, name.clone(), labels, h("authorization")));
                return (
                    StatusCode::CREATED,
                    axum::Json(json!({
                        "runner": { "id": id, "name": name, "status": "offline", "busy": false },
                        "encoded_jit_config": format!("jit-of-{name}"),
                    })),
                )
                    .into_response();
            }
            if path == "/orgs/acme/actions/runners/generate-jitconfig" {
                let asked: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let id = gh.org_runners.len() as u64 + 500;
                let name = asked["name"].as_str().unwrap_or("").to_string();
                let labels = asked["labels"]
                    .as_array()
                    .map(|l| {
                        l.iter()
                            .filter_map(|l| l.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                let group = asked["runner_group_id"].as_u64().unwrap_or(0);
                gh.org_runners
                    .push((id, name.clone(), labels, h("authorization"), group));
                return (
                    StatusCode::CREATED,
                    axum::Json(json!({
                        "runner": { "id": id, "name": name, "status": "offline", "busy": false },
                        "encoded_jit_config": format!("jit-of-{name}"),
                    })),
                )
                    .into_response();
            }
            if let Some(id) = path.strip_prefix("/orgs/acme/actions/runners/") {
                let id: u64 = id.parse().unwrap();
                gh.org_removed.push(id);
                if gh.busy_runner == Some(id) {
                    return StatusCode::UNPROCESSABLE_ENTITY.into_response();
                }
                return StatusCode::NO_CONTENT.into_response();
            }
            if let Some(id) = path.strip_prefix("/repos/acme/widgets/actions/runs/") {
                let Ok(id) = id.parse::<u64>() else {
                    return StatusCode::NOT_FOUND.into_response();
                };
                if gh.gone_runs.contains(&id) {
                    return StatusCode::NOT_FOUND.into_response();
                }
                if gh.broken_runs.contains(&id) {
                    return StatusCode::BAD_GATEWAY.into_response();
                }
                let head = match gh.fork_runs.contains(&id) {
                    true => "stranger/widgets",
                    false => "acme/widgets",
                };
                return axum::Json(json!({
                    "id": id,
                    "event": "pull_request",
                    "repository": { "full_name": "acme/widgets" },
                    "head_repository": { "full_name": head },
                }))
                .into_response();
            }
            if let Some(id) = path
                .strip_prefix("/repos/acme/widgets/actions/jobs/")
                .and_then(|rest| rest.strip_suffix("/rerun"))
            {
                gh.reruns.push(id.parse().unwrap());
                return match gh.rerun_answer {
                    Some((code, message)) => (
                        StatusCode::from_u16(code).unwrap(),
                        axum::Json(json!({ "message": message })),
                    )
                        .into_response(),
                    None => StatusCode::CREATED.into_response(),
                };
            }
            if let Some(id) = path.strip_prefix("/repos/acme/widgets/actions/jobs/") {
                return match id.parse::<u64>().ok().and_then(|id| gh.jobs.get(&id)) {
                    Some(job) => axum::Json(job.clone()).into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                };
            }
            if let Some(id) = path.strip_prefix("/repos/acme/widgets/actions/runners/") {
                if method != axum::http::Method::DELETE {
                    return StatusCode::METHOD_NOT_ALLOWED.into_response();
                }
                let id: u64 = id.parse().unwrap();
                gh.removed.push(id);
                if gh.busy_runner == Some(id) {
                    return StatusCode::UNPROCESSABLE_ENTITY.into_response();
                }
                return match gh.runners.iter().any(|(known, ..)| *known == id) {
                    true => StatusCode::NO_CONTENT.into_response(),
                    false => StatusCode::NOT_FOUND.into_response(),
                };
            }
            if let Some(rest) = path.strip_prefix("/repos/acme/widgets/check-runs") {
                if let Some(code) = gh.check_answer {
                    return StatusCode::from_u16(code).unwrap().into_response();
                }
                let check: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let id = match rest.strip_prefix('/') {
                    Some(id) if method == axum::http::Method::PATCH => id.parse().unwrap(),
                    None if method == axum::http::Method::POST => {
                        gh.checks
                            .iter()
                            .filter(|(_, sha, ..)| sha.is_some())
                            .count() as u64
                            + 1
                    }
                    _ => return StatusCode::METHOD_NOT_ALLOWED.into_response(),
                };
                let sha = check["head_sha"].as_str().map(str::to_string);
                gh.checks.push((id, sha, h("authorization"), check));
                return (StatusCode::CREATED, axum::Json(json!({ "id": id }))).into_response();
            }
            if let Some(sha) = path.strip_prefix("/repos/acme/widgets/statuses/") {
                if method != axum::http::Method::POST {
                    return StatusCode::METHOD_NOT_ALLOWED.into_response();
                }
                if let Some(code) = gh.status_answers.pop_front() {
                    return StatusCode::from_u16(code).unwrap().into_response();
                }
                if let Some((code, extra)) = &gh.status_answer {
                    let mut res = StatusCode::from_u16(*code).unwrap().into_response();
                    for (name, value) in extra {
                        res.headers_mut().insert(*name, value.parse().unwrap());
                    }
                    return res;
                }
                let status = serde_json::from_slice(&body).unwrap();
                gh.statuses
                    .push((sha.to_string(), h("authorization"), status));
                return (StatusCode::CREATED, axum::Json(json!({}))).into_response();
            }
            if path == "/repos/acme/widgets/contents/.dagron/ci" {
                let Some(files) = &gh.pipelines else {
                    return StatusCode::NOT_FOUND.into_response();
                };
                let mut entries: Vec<serde_json::Value> = files
                    .iter()
                    .map(|(name, text)| {
                        json!({ "type": "file", "name": name, "path": format!(".dagron/ci/{name}"), "size": text.len() })
                    })
                    .collect();
                entries.extend(gh.listed.iter().map(|(kind, name, size)| {
                    json!({ "type": kind, "name": name, "path": format!(".dagron/ci/{name}"), "size": size })
                }));
                return axum::Json(entries).into_response();
            }
            if let Some(rest) = path.strip_prefix("/raw/") {
                return match gh.raw.iter().find(|(p, _)| p == rest) {
                    // `status:<code>` is an answer of that status.
                    Some((_, text)) => match text.strip_prefix("status:") {
                        Some(code) => StatusCode::from_u16(code.parse().unwrap())
                            .unwrap()
                            .into_response(),
                        None => text.clone().into_response(),
                    },
                    None => StatusCode::NOT_FOUND.into_response(),
                };
            }
            // A repository's visibility, and a private one's Actions access.
            if let Some(full) = path.strip_prefix("/repos/") {
                let (full, access) = match full.strip_suffix("/actions/permissions/access") {
                    Some(full) => (full, true),
                    None => (full, false),
                };
                if let Some((_, private, level)) = gh.shared.iter().find(|(r, ..)| r == full) {
                    return match access {
                        false => axum::Json(json!({ "full_name": full, "private": private }))
                            .into_response(),
                        true => axum::Json(json!({ "access_level": level })).into_response(),
                    };
                }
            }
            if path == "/installation/token" && method == axum::http::Method::DELETE {
                return StatusCode::NO_CONTENT.into_response();
            }
            let theirs = path
                .strip_prefix("/repos/")
                .and_then(|rest| rest.split_once("/contents/"))
                .filter(|(repo, _)| *repo != "acme/widgets")
                .and_then(|(repo, file)| {
                    let asked = uri
                        .query()
                        .unwrap_or_default()
                        .split('&')
                        .find_map(|kv| kv.strip_prefix("ref="))
                        .unwrap_or_default();
                    let at = format!("{repo}/{file}");
                    gh.theirs
                        .iter()
                        .find(|(p, r, _)| *p == at && r == asked)
                        .map(|(_, _, text)| text.clone())
                });
            if let Some(text) = theirs {
                return match text.strip_prefix("status:") {
                    Some(code) => StatusCode::from_u16(code.parse().unwrap())
                        .unwrap()
                        .into_response(),
                    None => text.into_response(),
                };
            }
            if let Some(text) = path
                .strip_prefix("/repos/acme/widgets/contents/")
                .and_then(|file| gh.files.iter().find(|(p, _)| p == file))
                .map(|(_, text)| text.clone())
            {
                if h("accept") != RAW {
                    return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
                }
                return text.into_response();
            }
            if let Some(name) = path.strip_prefix("/repos/acme/widgets/contents/.dagron/ci/") {
                if h("accept") != RAW {
                    return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
                }
                let found = gh
                    .pipelines
                    .iter()
                    .flatten()
                    .find(|(n, _)| n == name)
                    .map(|(_, text)| text.clone());
                return match found {
                    Some(text) => text.into_response(),
                    None => StatusCode::NOT_FOUND.into_response(),
                };
            }
            if path.starts_with("/acme/widgets.git/") {
                if let (Some(said), true) = (&gh.advertisement, path.ends_with("/info/refs")) {
                    return said.clone().into_response();
                }
                if let (Some(said), true) = (&gh.fetched, path.ends_with("/git-upload-pack")) {
                    return said.clone().into_response();
                }
                let answer = gh
                    .git
                    .as_deref()
                    .and_then(|dir| serve_git(dir, &method, &uri, &headers, &body));
                return answer.unwrap_or_else(|| StatusCode::NOT_FOUND.into_response());
            }
            if path.starts_with("/repos/acme/widgets/compare/") {
                if let Some(code) = gh.compare_answer {
                    return StatusCode::from_u16(code).unwrap().into_response();
                }
                // A page past the first, as GitHub answers one: the
                // comparison without its files.
                if uri.query().is_some_and(|q| q.contains("page=2")) {
                    let mut later = json!({ "status": "ahead", "commits": [] });
                    if let Some(sha) = &gh.merge_base {
                        later["merge_base_commit"] = json!({ "sha": sha });
                    }
                    return axum::Json(later).into_response();
                }
                if let Some(body) = &gh.comparison {
                    return axum::Json(body.clone()).into_response();
                }
                let files: Vec<serde_json::Value> = gh
                    .changed
                    .iter()
                    .map(|(name, previous)| match previous {
                        Some(p) => json!({ "filename": name, "previous_filename": p, "status": "renamed", "patch": "@@" }),
                        None => json!({ "filename": name, "status": "modified", "patch": "@@" }),
                    })
                    .collect();
                let mut comparison = json!({ "status": "ahead", "files": files });
                if let Some(sha) = &gh.merge_base {
                    comparison["merge_base_commit"] = json!({ "sha": sha });
                }
                return axum::Json(comparison).into_response();
            }
            StatusCode::NOT_FOUND.into_response()
        }
        let gh: StubGithub = Arc::default();
        let app = Router::new().fallback(handle).with_state(gh.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (origin, gh)
    }

    /// git's smart HTTP for the repository at `dir`, as `git http-backend`
    /// serves it, through the system's `git upload-pack` (a test's oracle,
    /// [`crate::git::pack::tests`]): its advertisement, and its answer to a
    /// command, in protocol v2, which dagron-ci asks for. `None` for any
    /// other path.
    pub(crate) fn serve_git(
        dir: &std::path::Path,
        method: &axum::http::Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Option<Response> {
        use crate::git::pack::tests::upload_pack;
        let v2 = headers
            .get(git::PROTOCOL_HEADER)
            .is_some_and(|v| v == git::PROTOCOL_VERSION_2);
        let path = uri.path();
        let (kind, answer) = if path.ends_with(".git/info/refs")
            && uri.query() == Some("service=git-upload-pack")
            && method == axum::http::Method::GET
        {
            ("advertisement", upload_pack(dir, None))
        } else if path.ends_with(".git/git-upload-pack") && method == axum::http::Method::POST {
            ("result", upload_pack(dir, Some(body)))
        } else {
            return None;
        };
        if !v2 {
            return Some(StatusCode::BAD_REQUEST.into_response());
        }
        let kind = format!("application/x-git-upload-pack-{kind}");
        Some(([(axum::http::header::CONTENT_TYPE, kind)], answer).into_response())
    }

    /// A repository as one connected before there were modes is served:
    /// its forge's own files read wherever the build reads them.
    pub(crate) fn repo(host: &str, name: &str) -> Repo {
        Repo {
            id: "r".into(),
            forge: "github".into(),
            forge_host: host.into(),
            full_name: name.into(),
            workflow: None,
            cell: "default".into(),
            mode: crate::db::Mode::Compat,
        }
    }

    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

    fn forge(origin: &str) -> Forge {
        Forge::new(Some("ghp_test".into())).with_origin(origin)
    }

    fn files(pairs: &[(&str, &str)]) -> Option<Vec<(String, String)>> {
        Some(
            pairs
                .iter()
                .map(|(n, t)| (n.to_string(), t.to_string()))
                .collect(),
        )
    }

    #[test]
    fn the_api_is_found_from_the_host() {
        assert_eq!(github_api("github.com"), "https://api.github.com");
        assert_eq!(github_api("octo.ghe.com"), "https://api.octo.ghe.com");
        assert_eq!(
            github_api("git.corp.example"),
            "https://git.corp.example/api/v3"
        );
    }

    #[test]
    fn only_a_host_and_names_go_into_a_url() {
        let repo = |host: &str, name: &str| Repo {
            id: "r".into(),
            forge: "github".into(),
            forge_host: host.into(),
            full_name: name.into(),
            workflow: None,
            cell: "default".into(),
            mode: crate::db::Mode::Native,
        };
        assert!(is_url_safe(&repo("github.com", "acme/widgets")));
        assert!(is_url_safe(&repo("ghe.example:8443", "g/sub/p.js")));
        for (host, name) in [
            ("github.com", "acme/../admin"),
            ("github.com", "acme/w?x"),
            ("user@github.com", "acme/widgets"),
            ("github.com:", "acme/widgets"),
            ("github.com", "acme//widgets"),
        ] {
            assert!(!is_url_safe(&repo(host, name)), "{host} {name}");
        }
    }

    #[tokio::test]
    async fn the_pipelines_are_the_yaml_files_in_dagron_ci_at_the_commit() {
        let (origin, gh) = stub_github().await;
        gh.lock().unwrap().pipelines = files(&[
            ("engine.yaml", "name: engine\n"),
            ("docs.yml", "name: docs\n"),
            ("README.md", "not a pipeline"),
        ]);
        gh.lock().unwrap().listed = vec![
            ("dir", "shared".into(), 0),
            ("symlink", "old.yaml".into(), 9),
        ];
        let got = forge(&origin)
            .pipelines(&repo("github.com", "acme/widgets"), SHA)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            got,
            vec![
                PipelineFile {
                    path: ".dagron/ci/docs.yml".into(),
                    text: Ok("name: docs\n".into())
                },
                PipelineFile {
                    path: ".dagron/ci/engine.yaml".into(),
                    text: Ok("name: engine\n".into())
                },
            ],
            "the YAML files, by path; no directory, link or other file"
        );
        let seen = gh.lock().unwrap().seen.clone();
        assert_eq!(seen.len(), 3, "one listing, then one read a file: {seen:?}");
        assert_eq!(
            seen[0].0,
            format!("/repos/acme/widgets/contents/.dagron/ci?ref={SHA}")
        );
        assert_eq!(
            seen[1].0,
            format!("/repos/acme/widgets/contents/.dagron/ci/docs.yml?ref={SHA}")
        );
        assert!(seen.iter().all(|(_, auth, _)| auth == "Bearer ghp_test"));
        assert_eq!((seen[0].2.as_str(), seen[1].2.as_str()), (JSON, RAW));
    }

    #[tokio::test]
    async fn a_commit_without_pipelines_has_none() {
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let widgets = repo("github.com", "acme/widgets");
        assert_eq!(
            forge.pipelines(&widgets, SHA).await.unwrap(),
            None,
            "no directory"
        );
        gh.lock().unwrap().pipelines = files(&[("README.md", "#")]);
        assert_eq!(
            forge.pipelines(&widgets, SHA).await.unwrap(),
            None,
            "no YAML in it"
        );
    }

    #[tokio::test]
    async fn what_cannot_be_a_pipeline_is_listed_with_why() {
        let (origin, gh) = stub_github().await;
        let mut many: Vec<(String, String)> = (0..MAX_PIPELINES + 1)
            .map(|n| (format!("p{n:02}.yaml"), format!("name: p{n}\n")))
            .collect();
        many.push(("big.yaml".into(), "#".repeat(MAX_PIPELINE_BYTES + 1)));
        many.push(("empty.yaml".into(), String::new()));
        gh.lock().unwrap().pipelines = Some(many);
        let got = forge(&origin)
            .pipelines(&repo("github.com", "acme/widgets"), SHA)
            .await
            .unwrap()
            .unwrap();
        let why = |path: &str| {
            got.iter()
                .find(|f| f.path == format!(".dagron/ci/{path}"))
                .unwrap()
                .text
                .clone()
                .unwrap_err()
        };
        assert!(
            why("big.yaml").contains("over the 256 KiB"),
            "{}",
            why("big.yaml")
        );
        assert!(
            why("p32.yaml").contains("at most 32 pipelines"),
            "{}",
            why("p32.yaml")
        );
        assert_eq!(
            got.iter().filter(|f| f.text.is_ok()).count(),
            MAX_PIPELINES - 1
        );
        let reads = gh.lock().unwrap().seen.len();
        assert_eq!(
            reads,
            1 + MAX_PIPELINES - 1,
            "nothing past the cap or over the size is fetched"
        );
    }

    #[tokio::test]
    async fn a_spent_rate_limit_and_a_refusal_are_errors() {
        let (origin, gh) = stub_github().await;
        let widgets = repo("github.com", "acme/widgets");
        gh.lock().unwrap().answer = Some((
            403,
            vec![
                ("x-ratelimit-remaining", "0".into()),
                ("x-ratelimit-reset", "1".into()),
            ],
        ));
        let Err(ForgeError::RateLimited { why, .. }) =
            forge(&origin).pipelines(&widgets, SHA).await
        else {
            panic!("a spent limit is its own error");
        };
        assert!(
            why.starts_with("listing .dagron/ci: HTTP 403, the rate limit is spent"),
            "{why}"
        );

        gh.lock().unwrap().answer = Some((401, vec![]));
        let anonymous = Forge::new(None).with_origin(&origin);
        assert_eq!(
            anonymous.changed_files(&widgets, SHA, SHA).await,
            Err(ForgeError::Failed(
                "comparing commits: HTTP 401; a private repository needs the App installed \
                 on it (CI_GITHUB_APP_ID), or CI_GITHUB_TOKEN_FILE, a token that can read it"
                    .into()
            ))
        );
    }

    #[tokio::test]
    async fn the_token_goes_to_github_com_and_nowhere_else() {
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        for host in ["github.com", "octo.ghe.com", "git.corp.example"] {
            let _ = forge.pipelines(&repo(host, "acme/widgets"), SHA).await;
        }
        let auth: Vec<String> = gh
            .lock()
            .unwrap()
            .seen
            .iter()
            .map(|s| s.1.clone())
            .collect();
        assert_eq!(auth, ["Bearer ghp_test", "", ""]);
        assert!(!Forge::reads_pipelines(&Repo {
            forge: "gitea".into(),
            ..repo("gitea.com", "acme/widgets")
        }));
        assert!(!Forge::reads_pipelines(&repo("github.com", "acme/w?x")));
        assert!(Forge::reads_pipelines(&repo(
            "git.corp.example",
            "acme/widgets"
        )));
    }

    #[tokio::test]
    async fn a_change_lists_its_files_or_says_why_it_cannot() {
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let widgets = repo("github.com", "acme/widgets");
        let base = "a".repeat(40);
        gh.lock().unwrap().changed = vec![
            ("src/lib.rs".into(), None),
            ("docs/new.md".into(), Some("docs/old.md".into())),
        ];
        assert_eq!(
            forge.changed_files(&widgets, &base, SHA).await.unwrap(),
            Changes::Listed(vec![
                "docs/new.md".into(),
                "docs/old.md".into(),
                "src/lib.rs".into()
            ]),
            "a rename counts under both names"
        );
        assert_eq!(
            gh.lock().unwrap().seen.last().unwrap().0,
            format!("/repos/acme/widgets/compare/{base}...{SHA}")
        );

        gh.lock().unwrap().changed = (0..COMPARE_FILE_LIMIT)
            .map(|n| (format!("f{n}"), None))
            .collect();
        let Changes::Unlisted(why) = forge.changed_files(&widgets, &base, SHA).await.unwrap()
        else {
            panic!("a list as long as the forge's limit may be cut short");
        };
        assert!(why.contains("300 files or more"), "{why}");

        for code in [404, 422] {
            gh.lock().unwrap().compare_answer = Some(code);
            let Changes::Unlisted(why) = forge.changed_files(&widgets, &base, SHA).await.unwrap()
            else {
                panic!("{code}: commits the forge cannot compare");
            };
            assert_eq!(
                why,
                "the forge cannot compare aaaaaaaaaaaa with 4b4708e725f2"
            );
        }
    }

    /// A change larger than the compare API lists is listed through a
    /// shallow fetch of its two commits, from the merge base GitHub named,
    /// with the repository's credential; what git itself lists.
    #[tokio::test]
    async fn a_change_the_compare_api_cannot_list_is_fetched_and_listed() {
        let Some(scratch) = crate::git::pack::tests::Scratch::new(ObjectFormat::Sha1) else {
            return;
        };
        let (base, head) = crate::git::pack::tests::a_large_change(&scratch);
        scratch.git(&["config", "uploadpack.allowFilter", "true"]);
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let widgets = repo("github.com", "acme/widgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.git = Some(scratch.path().to_path_buf());
            gh.changed = (0..COMPARE_FILE_LIMIT)
                .map(|n| (format!("f{n}"), None))
                .collect();
            gh.merge_base = Some(base.clone());
        }
        // The event's base is a commit the repository does not have: what
        // GitHub compared from is fetched in its place.
        let tip = "b".repeat(40);
        let expected = Changes::Listed(scratch.diff(&base, &head));
        assert_eq!(
            forge.changed_files(&widgets, &tip, &head).await.unwrap(),
            expected
        );
        let git: Vec<(String, String)> = gh
            .lock()
            .unwrap()
            .seen
            .iter()
            .filter(|s| s.0.starts_with("/acme/widgets.git/"))
            .map(|s| (s.0.clone(), s.1.clone()))
            .collect();
        let credential = "Basic eC1hY2Nlc3MtdG9rZW46Z2hwX3Rlc3Q=".to_string();
        assert_eq!(
            git,
            [
                (
                    "/acme/widgets.git/info/refs?service=git-upload-pack".to_string(),
                    credential.clone()
                ),
                ("/acme/widgets.git/git-upload-pack".into(), credential),
            ],
            "x-access-token and the token, to the repository's git"
        );

        // A comparison that lists no files and names no merge base: the
        // merge base from a page of it past the first, and from a server
        // that sends the files too.
        scratch.git(&["config", "uploadpack.allowFilter", "false"]);
        gh.lock().unwrap().comparison = Some(json!({ "status": "ahead" }));
        assert_eq!(
            forge.changed_files(&widgets, &tip, &head).await.unwrap(),
            expected
        );
        // Where none names one, nothing is fetched: from the event's base,
        // a change could leave out files the compare API would list.
        gh.lock().unwrap().merge_base = None;
        let Changes::Unlisted(why) = forge.changed_files(&widgets, &base, &head).await.unwrap()
        else {
            panic!("no merge base to fetch from");
        };
        assert_eq!(
            why,
            "the forge listed no files for the change; nor could a shallow fetch: finding the \
             merge base: the forge names none"
        );
    }

    /// A comparison too large to read names no merge base, so it is asked
    /// of a page of the comparison past the first, which lists no files,
    /// and the fetch compares from it. After a rebase and a forced push the
    /// event's base, the branch's old tip, is not where the branch left
    /// main: from it, the branch's own file, the same in both tips, would
    /// be left out.
    #[tokio::test]
    async fn a_comparison_too_large_to_read_is_fetched_from_its_merge_base() {
        use crate::git::pack::tests::{a_rebased_branch, Scratch};
        let Some(scratch) = Scratch::new(ObjectFormat::Sha1) else {
            return;
        };
        let (old, left, new) = a_rebased_branch(&scratch);
        let own = "docs/a.md".to_string();
        assert!(!scratch.diff(&old, &new).contains(&own));
        let expected = scratch.diff(&left, &new);
        assert!(expected.contains(&own) && expected.len() > 400);
        scratch.git(&["config", "uploadpack.allowFilter", "true"]);
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let widgets = repo("github.com", "acme/widgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.git = Some(scratch.path().to_path_buf());
            gh.merge_base = Some(left.clone());
            let patch = "+".repeat(MAX_COMPARISON);
            gh.comparison = Some(json!({
                "status": "diverged",
                "files": [{ "filename": "src/0.rs", "status": "added", "patch": patch }],
            }));
        }
        assert_eq!(
            forge.changed_files(&widgets, &old, &new).await.unwrap(),
            Changes::Listed(expected)
        );
        let compared: Vec<String> = gh
            .lock()
            .unwrap()
            .seen
            .iter()
            .map(|s| s.0.clone())
            .filter(|path| path.starts_with("/repos/acme/widgets/compare/"))
            .collect();
        let range = format!("/repos/acme/widgets/compare/{old}...{new}");
        assert_eq!(
            compared,
            [range.clone(), format!("{range}?per_page=1&page=2")]
        );

        gh.lock().unwrap().merge_base = None;
        let Changes::Unlisted(why) = forge.changed_files(&widgets, &old, &new).await.unwrap()
        else {
            panic!("no merge base to fetch from");
        };
        assert_eq!(
            why,
            "the comparison is over 16 MiB; nor could a shallow fetch: finding the merge base: \
             the forge names none"
        );
    }

    /// One fetch at a time in a replica: another waits its turn within its
    /// own time, and gives up with it. The time is a third of a lease
    /// shorter than three times [`FETCH_FOR`].
    #[tokio::test]
    async fn a_fetch_waits_its_turn_within_its_time() {
        assert_eq!(Forge::new(None).fetch_for, FETCH_FOR);
        let long = Forge::new(None).with_event_lease(Duration::from_secs(600));
        assert_eq!(long.fetch_for, FETCH_FOR);
        let forge = Forge::new(None).with_event_lease(Duration::from_secs(3));
        assert_eq!(forge.fetch_for, Duration::from_secs(1));
        let _held = forge.fetches.clone().acquire_owned().await.unwrap();
        let base = "a".repeat(40);
        let widgets = repo("github.com", "acme/widgets");
        assert_eq!(
            forge
                .fetched_changes(&widgets, &base, SHA, Some(base.clone()))
                .await,
            Err("not done within 1 s".into())
        );
    }

    /// Where the fetch cannot list the change either, the pipeline runs,
    /// and the reason says why of both.
    #[tokio::test]
    async fn a_change_neither_can_list_says_why_of_both() {
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let widgets = repo("github.com", "acme/widgets");
        let base = "a".repeat(40);
        {
            let mut gh = gh.lock().unwrap();
            gh.changed = (0..COMPARE_FILE_LIMIT)
                .map(|n| (format!("f{n}"), None))
                .collect();
            gh.merge_base = Some(base.clone());
        }
        let unlisted = || async {
            match forge.changed_files(&widgets, &base, SHA).await.unwrap() {
                Changes::Unlisted(why) => why,
                other => panic!("{other:?}"),
            }
        };
        let too_many = "the change touches 300 files or more, more than the forge lists; nor \
                        could a shallow fetch: fetching the commits:";
        assert_eq!(
            unlisted().await,
            format!("{too_many} HTTP 404; the credential cannot read this repository")
        );

        let advertised = |lines: &[&str]| Some(crate::git::tests::pkts(lines));
        gh.lock().unwrap().advertisement =
            advertised(&["version 2\n", "ls-refs\n", "fetch=wait-for-done\n", "0000"]);
        assert_eq!(
            unlisted().await,
            format!("{too_many} the server fetches no shallow history")
        );
        gh.lock().unwrap().advertisement = advertised(&["version 2\n", "ls-refs\n", "0000"]);
        assert_eq!(
            unlisted().await,
            format!("{too_many} the server offers no fetch")
        );
        gh.lock().unwrap().advertisement = advertised(&[
            "# service=git-upload-pack\n",
            "0000",
            &format!("{SHA} refs/heads/main\0multi_ack\n"),
            "0000",
        ]);
        assert_eq!(
            unlisted().await,
            format!("{too_many} the server speaks only git's protocol v0")
        );

        // A commit the repository does not have: its server refuses it.
        let Some(scratch) = crate::git::pack::tests::Scratch::new(ObjectFormat::Sha1) else {
            return;
        };
        let (_, head) = crate::git::pack::tests::a_large_change(&scratch);
        {
            let mut gh = gh.lock().unwrap();
            gh.advertisement = None;
            gh.git = Some(scratch.path().to_path_buf());
        }
        let why = match forge.changed_files(&widgets, &base, &head).await.unwrap() {
            Changes::Unlisted(why) => why,
            other => panic!("{other:?}"),
        };
        assert!(
            why.starts_with(&format!("{too_many} the server refused: ")),
            "{why}"
        );
        // An answer larger than a fetch reads.
        gh.lock().unwrap().fetched = Some(vec![b'0'; MAX_FETCHED_PACK + 1]);
        assert_eq!(
            unlisted().await,
            format!("{too_many} the answer is over 32 MiB")
        );
        // And an event that names a commit by anything but its id.
        let why = match forge.changed_files(&widgets, "main", &head).await.unwrap() {
            Changes::Unlisted(why) => why,
            other => panic!("{other:?}"),
        };
        assert!(
            why.ends_with(
                "nor could a shallow fetch: the event does not name both commits by their ids"
            ),
            "{why}"
        );
    }

    /// A forge whose github.com calls are an App's first, and `token`'s where
    /// the App is not installed. `None`, and the test skipped, where there is
    /// no `openssl` to make the App's key with.
    pub(crate) fn app_forge(origin: &str, token: Option<&str>) -> Option<Forge> {
        let dir = tempfile::tempdir().unwrap();
        let (private, _) = crate::checkout::tests::rsa_key(dir.path())?;
        let app = GitHubApp::new(4242, &private).unwrap();
        Some(
            Forge::new(token.map(str::to_string))
                .with_origin(origin)
                .with_app(Some(app)),
        )
    }

    /// What each request under `prefix` carried as its Authorization, in
    /// order, the App's own endpoints left out.
    fn carried(gh: &StubGithub, prefix: &str) -> Vec<String> {
        gh.lock()
            .unwrap()
            .seen
            .iter()
            .filter(|(path, ..)| path.starts_with(prefix) && !path.ends_with("/installation"))
            .map(|(_, authorization, _)| authorization.clone())
            .collect()
    }

    /// How many installation tokens GitHub was asked for.
    fn mints(gh: &StubGithub) -> usize {
        gh.lock()
            .unwrap()
            .seen
            .iter()
            .filter(|(path, ..)| path.ends_with("/access_tokens"))
            .count()
    }

    /// Make the installation token held for `full_name` one minted `ago`.
    fn held_for(forge: &Forge, full_name: &str, ago: Duration) {
        let tokens = forge.app.as_ref().expect("an App");
        let mut held = tokens.held.lock().unwrap();
        let Some(Held::Token { until, .. }) = held.get_mut(full_name) else {
            panic!("no token is held for {full_name}");
        };
        *until -= ago;
    }

    pub(crate) fn a_status() -> CommitStatus {
        CommitStatus {
            state: "pending",
            context: "dagron-ci/ci/test (push)".into(),
            description: "queued".into(),
            target_url: None,
            pipeline_ref: None,
        }
    }

    /// Where the App is installed on a repository, dagron-ci's own calls for
    /// it are the App's: its pipelines are read and its statuses posted with
    /// an installation token minted for that repository alone, with what
    /// those calls need and no more, and with no CI_GITHUB_TOKEN_FILE.
    #[tokio::test]
    async fn a_repository_the_app_is_installed_on_is_read_and_told_as_the_app() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.pipelines = files(&[("ci.yaml", "name: ci\n")]);
        }
        let widgets = repo("github.com", "acme/widgets");
        assert!(forge.has_token(&widgets) && forge.posts_statuses(&widgets));

        let read = forge.pipelines(&widgets, SHA).await.unwrap().unwrap();
        assert_eq!(read.len(), 1);
        forge.post_status(&widgets, SHA, &a_status()).await.unwrap();
        forge.pipelines(&widgets, SHA).await.unwrap().unwrap();

        // One token, minted as the App, for the one repository.
        {
            let gh = gh.lock().unwrap();
            assert_eq!(gh.minted.len(), 1, "kept for the calls after the first");
            let (name, asked, authorization) = &gh.minted[0];
            assert_eq!(name, "widgets");
            assert_eq!(
                asked,
                &json!({
                    "repositories": ["widgets"],
                    "permissions": {
                        "contents": "read",
                        "pull_requests": "read",
                        "statuses": "write",
                    },
                })
            );
            let jwt = authorization.strip_prefix("Bearer ").expect("a bearer");
            assert_eq!(jwt.split('.').count(), 3, "the App's own token is a JWT");
            assert_eq!(gh.statuses[0].1, "Bearer ghs_widgets_1");
        }
        let all = carried(&gh, "/repos/acme/widgets/");
        assert!(all.len() >= 5, "{all:?}");
        assert!(all.iter().all(|a| a == "Bearer ghs_widgets_1"), "{all:?}");

        // On another host the App is nothing: no token is minted or sent.
        let elsewhere = repo("git.corp.example", "acme/widgets");
        assert!(!forge.has_token(&elsewhere) && !forge.posts_statuses(&elsewhere));
        let before = gh.lock().unwrap().seen.len();
        let _ = forge.pipelines(&elsewhere, SHA).await;
        let gh = gh.lock().unwrap();
        assert_eq!(gh.minted.len(), 1);
        assert!(gh.seen[before..].iter().all(|(_, a, _)| a.is_empty()));
    }

    /// A token is one repository's. Each repository's calls carry the one
    /// minted for it, which names it alone, so GitHub refuses it anywhere
    /// else: what reads one repository cannot read another.
    #[tokio::test]
    async fn each_repository_s_calls_carry_a_token_minted_for_it_alone() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, Some("ghp_test")) else {
            return;
        };
        for full_name in ["acme/widgets", "acme/gadgets"] {
            gh.lock().unwrap().installed.insert(full_name.into());
        }
        let widgets = repo("github.com", "acme/widgets");
        let gadgets = repo("github.com", "Acme/Gadgets");
        for _ in 0..2 {
            let _ = forge.pipelines(&widgets, SHA).await;
            let _ = forge.pipelines(&gadgets, SHA).await;
        }
        {
            let gh = gh.lock().unwrap();
            let named: Vec<(&str, &serde_json::Value)> = gh
                .minted
                .iter()
                .map(|(name, asked, _)| (name.as_str(), &asked["repositories"]))
                .collect();
            assert_eq!(
                named,
                [
                    ("widgets", &json!(["widgets"])),
                    ("Gadgets", &json!(["Gadgets"]))
                ],
                "one each, and each names its repository alone"
            );
        }
        let of = |prefix: &str| {
            let mut all = carried(&gh, prefix);
            all.dedup();
            all
        };
        assert_eq!(of("/repos/acme/widgets/"), ["Bearer ghs_widgets_1"]);
        assert_eq!(of("/repos/Acme/Gadgets/"), ["Bearer ghs_Gadgets_2"]);
    }

    /// Where the App is not installed on a repository, its calls are made
    /// with CI_GITHUB_TOKEN_FILE's token, the fallback; and with nothing where
    /// there is none, which a status cannot be posted with.
    #[tokio::test]
    async fn a_repository_without_the_app_falls_back_to_the_token() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, Some("ghp_test")) else {
            return;
        };
        let widgets = repo("github.com", "acme/widgets");
        for _ in 0..3 {
            let _ = forge.pipelines(&widgets, SHA).await;
        }
        forge.post_status(&widgets, SHA, &a_status()).await.unwrap();
        // A 401 to the token is not the App's to forget: GitHub is not asked
        // again whether the App is installed.
        gh.lock().unwrap().answer = Some((401, vec![]));
        assert!(forge.pipelines(&widgets, SHA).await.is_err());
        gh.lock().unwrap().answer = None;
        let _ = forge.pipelines(&widgets, SHA).await;
        let all = carried(&gh, "/repos/acme/widgets/");
        assert!(all.len() >= 6, "{all:?}");
        assert!(all.iter().all(|a| a == "Bearer ghp_test"), "{all:?}");
        {
            let gh = gh.lock().unwrap();
            assert!(gh.minted.is_empty());
            let asked = gh
                .seen
                .iter()
                .filter(|(path, ..)| path.ends_with("/installation"))
                .count();
            assert_eq!(
                asked, 1,
                "GitHub is not asked at every call whether it is installed"
            );
        }

        // No App there and no token: read as anyone, and no status to post.
        let (origin, gh) = stub_github().await;
        let forge = app_forge(&origin, None).unwrap();
        assert!(forge.has_token(&widgets), "an App that may be installed");
        let _ = forge.pipelines(&widgets, SHA).await;
        assert_eq!(carried(&gh, "/repos/acme/widgets/"), [""]);
        assert!(!forge.has_token(&widgets), "it is not: read as anyone");
        // And a refusal is told as what the call carried: nothing.
        gh.lock().unwrap().answer = Some((403, vec![]));
        let refused = forge.pipelines(&widgets, SHA).await.unwrap_err();
        gh.lock().unwrap().answer = None;
        assert!(
            matches!(&refused, ForgeError::Failed(why)
                if why.ends_with("the forge refused a request without credentials")),
            "{refused:?}"
        );
        let refusal = forge.post_status(&widgets, SHA, &a_status()).await;
        assert!(
            matches!(&refusal, Err(StatusError::Refused(why))
                if why.contains("CI_GITHUB_APP_ID") && why.contains("CI_GITHUB_TOKEN_FILE")),
            "{refusal:?}"
        );
        assert!(gh.lock().unwrap().statuses.is_empty());
    }

    /// A token GitHub will not mint (the App was not given what the calls
    /// need) leaves the repository to the fallback, without asking again at
    /// every call. One GitHub no longer knows is not used again: the next
    /// call mints another.
    #[tokio::test]
    async fn a_token_that_cannot_be_had_falls_back_and_one_no_longer_good_is_minted_again() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, Some("ghp_test")) else {
            return;
        };
        let widgets = repo("github.com", "acme/widgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.mint_answer = Some(422);
        }
        for _ in 0..3 {
            let _ = forge.pipelines(&widgets, SHA).await;
        }
        let all = carried(&gh, "/repos/acme/widgets/");
        assert!(all.iter().all(|a| a == "Bearer ghp_test"), "{all:?}");
        assert_eq!(mints(&gh), 1, "asked once, and not again at once");

        // Minted, then answered 401 with it: forgotten, and minted again.
        let (origin, gh) = stub_github().await;
        let forge = app_forge(&origin, None).unwrap();
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        let _ = forge.pipelines(&widgets, SHA).await;
        assert_eq!(mints(&gh), 1);
        gh.lock().unwrap().answer = Some((401, vec![]));
        assert!(forge.pipelines(&widgets, SHA).await.is_err());
        gh.lock().unwrap().answer = None;
        let _ = forge.pipelines(&widgets, SHA).await;
        assert_eq!(mints(&gh), 2);
        assert_eq!(
            carried(&gh, "/repos/acme/widgets/")
                .last()
                .map(String::as_str),
            Some("Bearer ghs_widgets_2")
        );
    }

    /// A status's 401 to an installation token held a while is the token's
    /// age, and nothing GitHub has against the status: it is tried again, and
    /// the next post mints another. A 401 to a token just minted is GitHub's
    /// refusal, not asked again, or every look at the run would mint one more.
    #[tokio::test]
    async fn a_status_s_401_to_a_token_held_a_while_is_tried_again_with_another() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let widgets = repo("github.com", "acme/widgets");
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        forge.post_status(&widgets, SHA, &a_status()).await.unwrap();
        assert_eq!(mints(&gh), 1);

        held_for(&forge, "acme/widgets", Duration::from_secs(120));
        gh.lock().unwrap().status_answers.push_back(401);
        let stale = forge.post_status(&widgets, SHA, &a_status()).await;
        assert!(
            matches!(&stale, Err(StatusError::Failed(why))
                if why.contains("HTTP 401") && why.contains("held for 12")),
            "{stale:?}"
        );
        forge.post_status(&widgets, SHA, &a_status()).await.unwrap();
        assert_eq!(mints(&gh), 2, "minted again, for the post after");
        assert_eq!(
            gh.lock().unwrap().statuses.last().unwrap().1,
            "Bearer ghs_widgets_2"
        );

        gh.lock().unwrap().status_answers.push_back(401);
        let refused = forge.post_status(&widgets, SHA, &a_status()).await;
        assert!(
            matches!(&refused, Err(StatusError::Refused(why)) if why.contains("HTTP 401")),
            "one just minted: {refused:?}"
        );
    }

    /// A mint GitHub does not answer is not a repository without the App.
    /// With no token to fall back on, the call fails, to be tried again, and
    /// is not made as nobody: GitHub answers an anonymous caller as if a
    /// private repository held nothing, which would be taken for a commit
    /// with no pipelines. With a token, the token makes the call.
    #[tokio::test]
    async fn a_mint_github_does_not_answer_fails_the_call_and_is_not_made_as_nobody() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let widgets = repo("github.com", "acme/widgets");
        let unanswered = |gh: &StubGithub| {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.pipelines = files(&[("ci.yaml", "name: ci\n")]);
            gh.mint_answer = Some(502);
        };
        unanswered(&gh);
        let failed = forge.pipelines(&widgets, SHA).await.unwrap_err();
        assert!(
            matches!(&failed, ForgeError::Failed(why)
                if why.starts_with("listing .dagron/ci: no installation token")
                    && why.contains("502")
                    && why.contains("CI_GITHUB_TOKEN_FILE")),
            "{failed:?}"
        );
        let posted = forge.post_status(&widgets, SHA, &a_status()).await;
        assert!(
            matches!(&posted, Err(StatusError::Failed(why)) if why.contains("no installation token")),
            "tried again, not refused for good: {posted:?}"
        );
        assert!(
            carried(&gh, "/repos/acme/widgets/").is_empty(),
            "nothing was asked as nobody"
        );
        assert!(forge.has_token(&widgets), "nor is it said to have none");
        assert_eq!(
            mints(&gh),
            crate::checkout::MINT_SENDS as usize,
            "one mint's sends, and GitHub is not asked again at once"
        );

        // With a token to fall back on, the token makes the call.
        let (origin, gh) = stub_github().await;
        let forge = app_forge(&origin, Some("ghp_test")).unwrap();
        unanswered(&gh);
        let read = forge.pipelines(&widgets, SHA).await.unwrap().unwrap();
        assert_eq!(read.len(), 1);
        let all = carried(&gh, "/repos/acme/widgets/");
        assert!(all.iter().all(|a| a == "Bearer ghp_test"), "{all:?}");
    }

    /// A status post's 403 is a token without the permission, or a secondary
    /// rate limit. GitHub may put the wait in no header, and says which it is
    /// in the body: a refusal is not asked again, so the two must not be
    /// taken for each other.
    #[tokio::test]
    async fn a_status_s_403_that_says_it_is_a_rate_limit_is_waited_out() {
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let repo = repo("github.com", "acme/widgets");
        let status = CommitStatus {
            state: "pending",
            context: "dagron-ci/ci/test (push)".into(),
            description: "queued".into(),
            target_url: None,
            pipeline_ref: None,
        };
        let answers = |headers: Vec<(&'static str, String)>, message: Option<&str>| {
            let mut gh = gh.lock().unwrap();
            gh.answer = Some((403, headers));
            gh.answer_body =
                message.map_or_else(String::new, |m| json!({ "message": m }).to_string());
        };

        // Nothing in the body, or something else: the token's refusal.
        for message in [None, Some("Resource not accessible by integration")] {
            answers(vec![], message);
            let refusal = forge.post_status(&repo, SHA, &status).await;
            assert!(
                matches!(&refusal, Err(StatusError::Refused(why)) if why.contains("statuses: write")),
                "{message:?}: {refusal:?}"
            );
        }
        // A rate limit that names no wait: a minute, as GitHub asks.
        for message in [
            "You have exceeded a secondary rate limit. Please wait a few minutes before you try again.",
            "API rate limit exceeded for installation ID 4242.",
        ] {
            answers(vec![], Some(message));
            let Err(StatusError::RateLimited { wait, why }) =
                forge.post_status(&repo, SHA, &status).await
            else {
                panic!("{message}: a rate limit");
            };
            assert_eq!(wait, Duration::from_secs(60), "{why}");
            assert!(why.contains("named no wait"), "{why}");
        }
        // One whose headers name the wait is waited that long, as before.
        answers(
            vec![("retry-after", "7".into())],
            Some("You have exceeded a secondary rate limit."),
        );
        assert!(matches!(
            forge.post_status(&repo, SHA, &status).await,
            Err(StatusError::RateLimited { wait, .. }) if wait == Duration::from_secs(7)
        ));
        // A body too long to be GitHub's message says nothing.
        {
            let mut gh = gh.lock().unwrap();
            gh.answer = Some((403, vec![]));
            gh.answer_body = format!("{} rate limit", "x".repeat(REFUSAL_BODY_MAX));
        }
        assert!(matches!(
            forge.post_status(&repo, SHA, &status).await,
            Err(StatusError::Refused(_))
        ));
        assert!(gh.lock().unwrap().statuses.is_empty(), "none was taken");
    }

    /// A status's 422 is a commit GitHub does not have, or a status it will
    /// not take for itself: its context holds a thousand on the commit
    /// already. The first silences the run's statuses; the second must not.
    #[tokio::test]
    async fn a_status_s_422_is_the_commit_s_only_when_github_says_so() {
        let (origin, gh) = stub_github().await;
        let forge = forge(&origin);
        let repo = repo("github.com", "acme/widgets");
        let status = CommitStatus {
            state: "pending",
            context: "dagron-ci/ci/test (push)".into(),
            description: "queued".into(),
            target_url: None,
            pipeline_ref: None,
        };
        let answers = |body: serde_json::Value| {
            let mut gh = gh.lock().unwrap();
            gh.answer = Some((422, vec![]));
            gh.answer_body = body.to_string();
        };

        answers(json!({ "message": format!("No commit found for SHA: {SHA}"), "status": "422" }));
        let refusal = forge.post_status(&repo, SHA, &status).await;
        assert!(
            matches!(&refusal, Err(StatusError::Refused(why)) if why.contains("no such commit")),
            "{refusal:?}"
        );
        // The context is full, as GitHub says it; a status it does not take;
        // and a 422 that says nothing at all.
        for body in [
            json!({ "message": "Validation Failed", "errors": [{ "resource": "Status", "code": "custom",
                "message": "This SHA and context has reached the maximum number of statuses." }] }),
            json!({ "message": "Validation Failed" }),
            json!(null),
        ] {
            answers(body.clone());
            let refusal = forge.post_status(&repo, SHA, &status).await;
            assert!(
                matches!(&refusal, Err(StatusError::ThisStatus(why)) if why.contains("under this context")),
                "{body}: {refusal:?}"
            );
        }
        // GitHub's own reason is kept, so that the record says which it was.
        answers(
            json!({ "message": "Validation Failed", "errors": [{ "resource": "Status", "code": "custom",
            "message": "This SHA and context has reached the maximum number of statuses." }] }),
        );
        let Err(StatusError::ThisStatus(why)) = forge.post_status(&repo, SHA, &status).await else {
            panic!("a full context");
        };
        assert!(
            why.ends_with(": This SHA and context has reached the maximum number of statuses."),
            "{why}"
        );
        // Where it lists no error, the message; and at most so much of it.
        answers(json!({ "message": format!("Validation Failed {}", "x".repeat(500)) }));
        let Err(StatusError::ThisStatus(why)) = forge.post_status(&repo, SHA, &status).await else {
            panic!("a status GitHub does not take");
        };
        let kept = why.rsplit(": ").next().unwrap();
        assert!(
            kept.starts_with("Validation Failed x") && kept.chars().count() == 200,
            "{why}"
        );
    }

    /// A 403 whose body stops short of what its headers promised: which of a
    /// refusal and a rate limit it was is not known, so the status is tried
    /// again, where a refusal would never be.
    #[tokio::test]
    async fn a_status_s_403_whose_body_cannot_be_read_is_tried_again() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = [0_u8; 4096];
                let _ = socket.read(&mut request).await;
                // A hundred bytes promised, a few sent, and the line dropped.
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\n\
                          content-length: 100\r\n\r\n{\"message\":\"You have",
                    )
                    .await;
                let _ = socket.shutdown().await;
            }
        });
        let status = CommitStatus {
            state: "pending",
            context: "dagron-ci/ci/test (push)".into(),
            description: "queued".into(),
            target_url: None,
            pipeline_ref: None,
        };
        let answer = forge(&origin)
            .post_status(&repo("github.com", "acme/widgets"), SHA, &status)
            .await;
        assert!(
            matches!(&answer, Err(StatusError::Failed(why)) if why.contains("an HTTP 403 said")),
            "{answer:?}"
        );
    }

    #[test]
    fn sizes_read_in_the_unit_they_are_set_in() {
        assert_eq!(size(16 * 1024 * 1024), "16 MiB");
        assert_eq!(size(256 * 1024), "256 KiB");
    }

    fn a_check(status: &'static str, conclusion: Option<&'static str>, title: &str) -> CheckRun {
        CheckRun {
            name: "dagron-ci/ci/test (push)".into(),
            status,
            conclusion,
            title: title.into(),
            summary: "Engine run `r-1`.".into(),
            details_url: Some("https://ci.example/runs/r-1".into()),
            external_id: "r-1".into(),
            ..CheckRun::default()
        }
    }

    /// A check run goes up as the App, with a token that is for check runs
    /// and nothing else, and is brought up to date by its id.
    #[tokio::test]
    async fn a_check_run_goes_up_with_a_token_for_check_runs_alone() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let forge = forge.with_checks(true);
        let repo = repo("github.com", "acme/widgets");
        gh.lock().unwrap().installed.insert("acme/widgets".into());

        assert_eq!(forge.checks(&repo).await, Ok(true));
        let id = forge
            .post_check(&repo, SHA, &a_check("in_progress", None, "running"), None)
            .await
            .unwrap();
        // What a job said of itself goes with the writing that ends its
        // check run: its text, and no more annotations than GitHub takes.
        let at = |line: u64, title: &str| Annotation {
            path: "src/lib.rs".into(),
            start_line: line,
            end_line: line + 1,
            level: "failure",
            title: title.into(),
            message: "left != right".into(),
        };
        let over = CheckRun {
            text: "**2 tests**: 1 failed, 0 errors, 0 skipped.".into(),
            summary: "s".repeat(CHECK_TEXT_MAX + 9),
            annotations: (1..=CHECK_ANNOTATIONS_MAX as u64 + 3)
                .map(|line| at(line, if line == 2 { "" } else { "t.bad" }))
                .collect(),
            ..a_check("completed", Some("failure"), "failed")
        };
        assert_eq!(forge.post_check(&repo, SHA, &over, Some(id)).await, Ok(id));

        let gh = gh.lock().unwrap();
        assert_eq!(gh.minted.len(), 1, "one token, held for both");
        assert_eq!(
            gh.minted[0].1,
            json!({ "repositories": ["widgets"], "permissions": { "checks": "write" } })
        );
        let [(first, sha, carried, made), (second, none, _, updated)] = &gh.checks[..] else {
            panic!("{:?}", gh.checks);
        };
        assert_eq!(
            (*first, sha.as_deref(), carried.as_str()),
            (id, Some(SHA), "Bearer ghs_widgets_1")
        );
        assert_eq!(
            (&made["name"], &made["status"], &made["external_id"]),
            (
                &json!("dagron-ci/ci/test (push)"),
                &json!("in_progress"),
                &json!("r-1")
            )
        );
        assert_eq!(made["details_url"], "https://ci.example/runs/r-1");
        assert_eq!(made["output"]["title"], "running");
        assert_eq!(made["output"]["summary"], "Engine run `r-1`.");
        assert!(made.get("conclusion").is_none(), "{made}");
        assert!(
            made["output"].get("text").is_none() && made["output"].get("annotations").is_none(),
            "a check run with nothing more to say says no more: {made}"
        );
        assert_eq!(
            (*second, none),
            (id, &None),
            "the same check run, by its id"
        );
        assert_eq!(
            (&updated["status"], &updated["conclusion"]),
            (&json!("completed"), &json!("failure"))
        );
        let output = &updated["output"];
        assert_eq!(
            output["text"],
            "**2 tests**: 1 failed, 0 errors, 0 skipped."
        );
        assert_eq!(
            output["summary"].as_str().map(str::len),
            Some(CHECK_TEXT_MAX),
            "cut to what GitHub takes, which refuses a longer one whole"
        );
        let annotations = output["annotations"].as_array().unwrap();
        assert_eq!(annotations.len(), CHECK_ANNOTATIONS_MAX);
        assert_eq!(
            annotations[0],
            json!({
                "path": "src/lib.rs", "start_line": 1, "end_line": 2,
                "annotation_level": "failure", "title": "t.bad", "message": "left != right",
            })
        );
        assert!(annotations[1].get("title").is_none(), "{}", annotations[1]);
    }

    /// An App that was not given check runs is refused the token for them,
    /// and keeps the one its reads and statuses are made with.
    #[tokio::test]
    async fn an_app_without_check_runs_keeps_its_statuses() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let forge = forge.with_checks(true);
        let repo = repo("github.com", "acme/widgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.no_checks = true;
        }
        assert_eq!(forge.checks(&repo).await, Ok(false));
        assert_eq!(forge.checks(&repo).await, Ok(false));
        let status = CommitStatus {
            state: "pending",
            context: "dagron-ci/ci/test (push)".into(),
            description: "queued".into(),
            target_url: None,
            pipeline_ref: None,
        };
        forge.post_status(&repo, SHA, &status).await.unwrap();
        let refused = forge
            .post_check(&repo, SHA, &a_check("queued", None, "queued"), None)
            .await;
        assert!(
            matches!(&refused, Err(StatusError::Refused(why)) if why.contains("`checks: write`")),
            "{refused:?}"
        );

        let gh = gh.lock().unwrap();
        let mints = gh
            .seen
            .iter()
            .filter(|(path, ..)| path == "/app/installations/77/access_tokens")
            .count();
        assert_eq!(mints, 2, "asked once for each token, and the refusal kept");
        assert_eq!(gh.minted.len(), 1);
        assert_eq!(gh.minted[0].1["permissions"], own_permissions());
        assert_eq!(gh.statuses.len(), 1);
        assert_eq!(gh.statuses[0].1, "Bearer ghs_widgets_1");
        assert!(gh.checks.is_empty());
    }

    /// Check runs are an operator's to turn on, and an App's to post.
    #[tokio::test]
    async fn check_runs_are_off_unless_asked_for_and_need_the_app() {
        let (origin, gh) = stub_github().await;
        let elsewhere = repo("git.example.com", "acme/widgets");
        let repo = repo("github.com", "acme/widgets");
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        if let Some(forge) = app_forge(&origin, None) {
            assert_eq!(forge.checks(&repo).await, Ok(false), "not asked for");
        }
        let token_alone = forge(&origin).with_checks(true);
        assert_eq!(token_alone.checks(&repo).await, Ok(false), "no App");
        assert!(
            gh.lock().unwrap().seen.is_empty(),
            "GitHub was asked nothing"
        );
        if let Some(forge) = app_forge(&origin, None) {
            assert_eq!(forge.with_checks(true).checks(&elsewhere).await, Ok(false));
        }
    }

    /// What GitHub says of a check run it does not take is read as what it
    /// says of a status is.
    #[tokio::test]
    async fn a_check_run_github_refuses_says_what_the_app_lacks() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let forge = forge.with_checks(true);
        let repo = repo("github.com", "acme/widgets");
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        let check = a_check("queued", None, "queued");
        for (code, refused) in [(403, true), (404, true), (500, false)] {
            gh.lock().unwrap().check_answer = Some(code);
            let said = forge.post_check(&repo, SHA, &check, None).await;
            match (&said, refused) {
                (Err(StatusError::Refused(why)), true) => {
                    assert!(why.contains(&format!("HTTP {code}")), "{why}")
                }
                (Err(StatusError::Failed(_)), false) => {}
                _ => panic!("{code}: {said:?}"),
            }
        }
    }

    /// What is said of a job that was not run goes where the repository's
    /// jobs go: a check run that is over where they are check runs, which
    /// names no run, and the status it was anywhere else.
    #[tokio::test]
    async fn a_job_that_was_not_run_is_said_where_the_repository_s_jobs_are() {
        let said = CommitStatus {
            state: "success",
            context: "dagron-ci/ci/docs (push)".into(),
            description: "skipped: no path it watches changed".into(),
            target_url: None,
            pipeline_ref: None,
        };
        let repo = repo("github.com", "acme/widgets");
        for app_has_checks in [true, false] {
            let (origin, gh) = stub_github().await;
            let Some(forge) = app_forge(&origin, None) else {
                return;
            };
            let forge = forge.with_checks(true);
            {
                let mut gh = gh.lock().unwrap();
                gh.installed.insert("acme/widgets".into());
                gh.no_checks = !app_has_checks;
            }
            forge.post_skipped_job(&repo, SHA, &said).await.unwrap();
            let gh = gh.lock().unwrap();
            if !app_has_checks {
                assert_eq!((gh.statuses.len(), gh.checks.len()), (1, 0));
                continue;
            }
            assert!(gh.statuses.is_empty(), "{:?}", gh.statuses);
            let [(_, sha, _, check)] = &gh.checks[..] else {
                panic!("{:?}", gh.checks);
            };
            assert_eq!(sha.as_deref(), Some(SHA));
            assert_eq!(
                (&check["name"], &check["status"], &check["conclusion"]),
                (
                    &json!("dagron-ci/ci/docs (push)"),
                    &json!("completed"),
                    &json!("skipped")
                )
            );
            assert_eq!(
                check["output"]["title"],
                "skipped: no path it watches changed"
            );
            assert!(check.get("external_id").is_none(), "{check}");
        }
    }

    /// A runner is registered for one repository, with a token of the App's
    /// that is for that repository's runners and nothing else, and is taken
    /// back by the id GitHub gave it.
    #[tokio::test]
    async fn a_runner_is_registered_and_taken_back_with_a_token_for_runners_alone() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let repo = repo("github.com", "acme/widgets");
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        let labels = vec!["dagron".to_string(), "self-hosted".to_string()];

        let jit = forge
            .jit_runner(&repo, "dagron-aaa", &labels, None)
            .await
            .unwrap();
        assert_eq!(
            (jit.runner_id, jit.config.as_str()),
            (20, "jit-of-dagron-aaa")
        );
        assert!(
            !format!("{jit:?}").contains("jit-of"),
            "its configuration is not printed"
        );
        assert_eq!(forge.remove_runner(&repo, "20", None).await, Ok(true));
        assert_eq!(
            forge.remove_runner(&repo, "99", None).await,
            Ok(true),
            "gone already"
        );
        gh.lock().unwrap().busy_runner = Some(20);
        assert_eq!(
            forge.remove_runner(&repo, "20", None).await,
            Ok(false),
            "it has a job"
        );
        assert_eq!(
            forge.remove_runner(&repo, "", None).await,
            Ok(true),
            "never registered"
        );

        let gh = gh.lock().unwrap();
        assert_eq!(gh.minted.len(), 1, "one token, for all of it");
        assert_eq!(
            gh.minted[0].1,
            json!({ "repositories": ["widgets"], "permissions": { "administration": "write" } })
        );
        assert_eq!(
            gh.runners,
            [(
                20,
                "dagron-aaa".to_string(),
                labels,
                "Bearer ghs_widgets_1".to_string()
            )]
        );
        assert_eq!(gh.removed, [20, 99, 20]);
    }

    /// An App that was not given a repository's administration registers no
    /// runner for it, and what it was given is as it was.
    #[tokio::test]
    async fn an_app_without_a_repository_s_administration_registers_no_runner() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let elsewhere = repo("git.example.com", "acme/widgets");
        let repo = repo("github.com", "acme/widgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.no_runners = true;
        }
        let refused = forge.jit_runner(&repo, "dagron-aaa", &[], None).await;
        assert!(
            matches!(&refused, Err(ForgeError::Failed(why)) if why.contains("`administration: write`")),
            "{refused:?}"
        );
        let refused = forge.jit_runner(&elsewhere, "dagron-aaa", &[], None).await;
        assert!(matches!(&refused, Err(ForgeError::Failed(why)) if why.contains("github.com")));
        let gh = gh.lock().unwrap();
        assert!(gh.runners.is_empty() && gh.minted.is_empty());
    }

    /// An organization's runner is registered in the runner group asked for,
    /// with a token of the App's that is the organization's runners' alone,
    /// named for no repository, and minted once for every repository of the
    /// organization; it is removed at the organization.
    #[tokio::test]
    async fn an_organization_s_runner_is_registered_in_its_group_with_a_token_of_its_own() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let widgets = repo("github.com", "acme/widgets");
        let gadgets = repo("github.com", "Acme/gadgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.installed.insert("acme/gadgets".into());
        }
        let labels = vec!["dagron".to_string()];
        let jit = forge
            .jit_runner(&widgets, "dagron-aaa", &labels, Some(3))
            .await
            .unwrap();
        assert_eq!(jit.runner_id, 500);
        forge
            .jit_runner(&gadgets, "dagron-bbb", &labels, Some(3))
            .await
            .unwrap();
        assert_eq!(
            forge.remove_runner(&gadgets, "500", Some("acme")).await,
            Ok(true)
        );
        let gh = gh.lock().unwrap();
        assert_eq!(
            gh.minted.iter().map(|m| m.1.clone()).collect::<Vec<_>>(),
            [json!({ "permissions": { "organization_self_hosted_runners": "write" } })],
            "one token, of the organization's"
        );
        assert_eq!(
            gh.org_runners
                .iter()
                .map(|r| (r.0, r.4))
                .collect::<Vec<_>>(),
            [(500, 3), (501, 3)]
        );
        assert!(gh.runners.is_empty(), "no repository's runner");
        assert_eq!(gh.org_removed, [500]);
    }

    /// A job is run again with a token for that alone; what GitHub will not
    /// do yet is told from what it never will.
    #[tokio::test]
    async fn a_job_is_run_again_with_a_token_for_that_alone() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let repo = repo("github.com", "acme/widgets");
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        assert_eq!(forge.retry_job(&repo, "5").await, Ok(Retried::Asked));
        gh.lock().unwrap().rerun_answer = Some((403, "the workflow run is still running"));
        let said = forge.retry_job(&repo, "5").await;
        assert!(
            matches!(&said, Ok(Retried::NotYet(why)) if why.contains("still running")),
            "{said:?}"
        );
        gh.lock().unwrap().rerun_answer = Some((404, "Not Found"));
        assert!(matches!(
            forge.retry_job(&repo, "5").await,
            Ok(Retried::Refused(_))
        ));
        assert!(matches!(
            forge.retry_job(&repo, "5/x").await,
            Ok(Retried::Refused(_))
        ));
        let gh = gh.lock().unwrap();
        assert_eq!(gh.reruns, [5, 5, 5]);
        assert_eq!(
            gh.minted[0].1,
            json!({ "repositories": ["widgets"], "permissions": { "actions": "write" } })
        );
    }

    /// A workflow run is its repository's own, a fork's or gone; a workflow
    /// job reads as a delivery of it would. Both with a token for the
    /// repository's workflow runs alone.
    #[tokio::test]
    async fn a_workflow_run_and_a_job_are_read_with_a_token_for_actions_alone() {
        let (origin, gh) = stub_github().await;
        let Some(forge) = app_forge(&origin, None) else {
            return;
        };
        let repo = repo("github.com", "acme/widgets");
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.fork_runs.insert(78);
            gh.gone_runs.insert(79);
            gh.jobs.insert(
                5,
                json!({
                    "id": 5, "run_id": 77, "run_attempt": 1, "status": "completed",
                    "conclusion": "cancelled", "name": "build", "workflow_name": "CI",
                    "labels": ["dagron"], "runner_name": null,
                    "head_sha": "4b4708e725f29891d22b4c96af9f0a403ff51c7c",
                }),
            );
        }
        assert_eq!(forge.workflow_run_is_own(&repo, "77").await, Ok(Some(true)));
        assert_eq!(
            forge.workflow_run_is_own(&repo, "78").await,
            Ok(Some(false))
        );
        assert_eq!(forge.workflow_run_is_own(&repo, "79").await, Ok(None));
        let job = forge.workflow_job(&repo, "5").await.unwrap().unwrap();
        assert_eq!(
            (
                job.action,
                job.conclusion.as_deref(),
                job.full_name.as_str()
            ),
            (
                crate::github::JobAction::Completed,
                Some("cancelled"),
                "acme/widgets"
            )
        );
        assert_eq!(forge.workflow_job(&repo, "6").await, Ok(None));
        assert_eq!(forge.workflow_job(&repo, "../x").await, Ok(None));
        {
            let gh = gh.lock().unwrap();
            assert_eq!(gh.minted.len(), 1);
            assert_eq!(
                gh.minted[0].1,
                json!({ "repositories": ["widgets"], "permissions": { "actions": "read" } })
            );
        }

        // An App without `actions: read` reads neither, and says so.
        let (origin, gh) = stub_github().await;
        let forge = app_forge(&origin, None).unwrap();
        {
            let mut gh = gh.lock().unwrap();
            gh.installed.insert("acme/widgets".into());
            gh.no_actions = true;
        }
        let refused = forge.workflow_run_is_own(&repo, "77").await;
        assert!(
            matches!(&refused, Err(ForgeError::Failed(why)) if why.contains("`actions: read`")),
            "{refused:?}"
        );
    }
}
