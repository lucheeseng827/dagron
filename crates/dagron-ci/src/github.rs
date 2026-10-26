//! The GitHub door: what `POST /hooks/github` verifies and how it reads a
//! payload. The handler that writes the event is in `server.rs`.
//!
//! The order is fixed: the signature is checked over the raw body before
//! anything parses it, so an unsigned request costs a hash and nothing else.
//! Only then does the payload become a [`NewEvent`], with its trust class
//! decided here from fields GitHub vouches for.
//!
//! The receiver's shape (an HMAC check over the raw body, a delivery id as the
//! replay key) follows AGAS's GitHub receiver (MIT); see `NOTICE`. Its replay
//! cache lived in memory. This one is a table, so a restart forgets nothing.

use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;

use crate::event::{is_commit_id, is_storable_ref, Kind, NewEvent, Trust};

pub const SIGNATURE_HEADER: &str = "x-hub-signature-256";
pub const EVENT_HEADER: &str = "x-github-event";
pub const DELIVERY_HEADER: &str = "x-github-delivery";

/// The host recorded for repositories on github.com. A GitHub Enterprise
/// Server repository is recorded under its own host, read from the signed
/// payload rather than from a header anyone could add.
pub const DEFAULT_HOST: &str = "github.com";

/// Why a signature was refused. Every variant answers 401 and writes nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum SignatureError {
    Missing,
    Malformed,
    Mismatch,
}

impl std::fmt::Display for SignatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SignatureError::Missing => "no X-Hub-Signature-256 header",
            SignatureError::Malformed => "X-Hub-Signature-256 is not sha256=<64 hex digits>",
            SignatureError::Mismatch => "X-Hub-Signature-256 does not match the body",
        })
    }
}

/// Check `X-Hub-Signature-256: sha256=<hex>` against the HMAC-SHA256 of the
/// raw body under the webhook secret. `verify_slice` compares in constant
/// time, so a mismatch reveals nothing about how much of it matched.
pub fn verify_signature(
    secret: &[u8],
    header: Option<&str>,
    body: &[u8],
) -> Result<(), SignatureError> {
    let header = header.ok_or(SignatureError::Missing)?;
    let hex_digest = header
        .trim()
        .strip_prefix("sha256=")
        .ok_or(SignatureError::Malformed)?;
    let expected = hex::decode(hex_digest).map_err(|_| SignatureError::Malformed)?;
    if expected.len() != 32 {
        return Err(SignatureError::Malformed);
    }
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(body);
    mac.verify_slice(&expected)
        .map_err(|_| SignatureError::Mismatch)
}

/// What a verified delivery turned out to be.
#[derive(Debug, PartialEq, Eq)]
pub enum Parsed {
    /// An event for the queue, for the repository `full_name` on `forge_host`
    /// (both lowercased).
    Event {
        forge_host: String,
        full_name: String,
        event: NewEvent,
    },
    /// Someone asked for a check run to be run again: "Re-run", beside
    /// the check.
    Rerequested(Rerequested),
    /// A job of one of GitHub Actions' own workflows was queued, taken by a
    /// runner, or ended: fleet mode's ([`crate::fleet`]).
    Job(Job),
    /// A GitLab merge request's pipeline, which is its merge train's only
    /// where GitLab says so when asked ([`crate::gitlab::Train`]).
    Train(crate::gitlab::Train),
    /// Acknowledged and dropped: nothing in it can lead to a run.
    Ignored(String),
}

/// What became of a workflow job, as its delivery's `action` says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobAction {
    /// It waits for a runner with its labels.
    Queued,
    /// A runner took it.
    InProgress,
    /// It is over, however it ended.
    Completed,
}

/// A job of a GitHub Actions workflow, as a `workflow_job` delivery has it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    /// The repository's host and name, both lowercased.
    pub forge_host: String,
    pub full_name: String,
    pub action: JobAction,
    /// GitHub's id for the job: one a job, across its deliveries.
    pub id: u64,
    /// The workflow run it is a job of, and which attempt of that run.
    pub run_id: u64,
    pub run_attempt: u32,
    /// The job's name, and its workflow's, to show.
    pub name: String,
    pub workflow: Option<String>,
    /// What its `runs-on` asks for.
    pub labels: Vec<String>,
    /// The commit it runs at.
    pub head_sha: String,
    /// The runner that has it, once one does.
    pub runner_id: Option<u64>,
    pub runner_name: Option<String>,
    /// How it ended, once it has: `success`, `failure`, `cancelled`
    /// or `skipped`.
    pub conclusion: Option<String>,
}

/// What someone asked to have run again: one check run, or all of a commit's.
#[derive(Debug, PartialEq, Eq)]
pub enum Of {
    /// A check's "Re-run": what the check run carries as its
    /// `external_id`, which is the engine run dagron-ci posted it for,
    /// where the check run is one of dagron-ci's.
    Run(String),
    /// "Re-run all checks": the commit the App's check suite is of. A suite
    /// names no run, so the commit is all there is to go by.
    Commit(String),
}

impl Of {
    /// The run or the commit, for a log line.
    pub fn as_str(&self) -> &str {
        match self {
            Of::Run(id) | Of::Commit(id) => id,
        }
    }
}

/// Checks someone asked to have run again.
#[derive(Debug, PartialEq, Eq)]
pub struct Rerequested {
    /// The repository's host and name, both lowercased.
    pub forge_host: String,
    pub full_name: String,
    /// Which: one check run's run, or a commit's.
    pub of: Of,
    /// The App the check run or the suite is of, where the delivery says.
    pub app_id: Option<u64>,
    /// Who asked.
    pub sender: Option<String>,
    /// Whether GitHub calls who asked a bot.
    pub bot: bool,
}

/// Why a verified payload could not be read. Answers 400: the sender holds the
/// secret, so this is a misconfiguration (or a GitHub change), not an attack.
#[derive(Debug, PartialEq, Eq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Deserialize)]
struct Repository {
    id: Option<i64>,
    full_name: String,
    html_url: Option<String>,
}

#[derive(Deserialize)]
struct Account {
    login: String,
    /// `User`, `Bot` or `Organization`, where the payload says.
    #[serde(default, rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct WorkflowJobPayload {
    action: String,
    workflow_job: WorkflowJob,
    repository: Repository,
}

#[derive(Deserialize)]
struct WorkflowJob {
    id: u64,
    run_id: u64,
    #[serde(default)]
    run_attempt: Option<u32>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    labels: Vec<String>,
    head_sha: String,
    #[serde(default)]
    runner_id: Option<u64>,
    #[serde(default)]
    runner_name: Option<String>,
    #[serde(default)]
    conclusion: Option<String>,
}

/// The most labels a job is read with, and the longest a label or a name
/// is kept: what a workflow's author writes, and is stored.
const MAX_JOB_LABELS: usize = 32;
const MAX_JOB_TEXT: usize = 255;

#[derive(Deserialize)]
struct CheckRunPayload {
    action: String,
    check_run: CheckRunOf,
    repository: Repository,
    sender: Option<Account>,
}

#[derive(Deserialize)]
struct CheckRunOf {
    #[serde(default)]
    external_id: Option<String>,
    app: Option<AppOf>,
}

#[derive(Deserialize)]
struct AppOf {
    id: u64,
}

#[derive(Deserialize)]
struct CheckSuitePayload {
    action: String,
    check_suite: CheckSuiteOf,
    repository: Repository,
    sender: Option<Account>,
}

#[derive(Deserialize)]
struct CheckSuiteOf {
    #[serde(default)]
    head_sha: Option<String>,
    app: Option<AppOf>,
}

#[derive(Deserialize)]
struct PushPayload {
    #[serde(rename = "ref")]
    git_ref: String,
    before: String,
    after: String,
    #[serde(default)]
    deleted: bool,
    /// The commit `after` names. For a pushed annotated tag `after` is the
    /// tag's own id, and this is the commit the tag points at.
    head_commit: Option<HeadCommit>,
    repository: Repository,
    sender: Option<Account>,
}

#[derive(Deserialize)]
struct HeadCommit {
    id: String,
}

#[derive(Deserialize)]
struct PullRequestPayload {
    action: String,
    number: i64,
    pull_request: PullRequest,
    repository: Repository,
    sender: Option<Account>,
}

#[derive(Deserialize)]
struct PullRequest {
    head: Branch,
    base: Branch,
    author_association: Option<String>,
}

#[derive(Deserialize)]
struct Branch {
    /// The branch's name, `main`.
    #[serde(rename = "ref")]
    git_ref: Option<String>,
    sha: String,
    /// `null` when the fork the pull request came from has been deleted.
    repo: Option<Repository>,
}

#[derive(Deserialize)]
struct MergeGroupPayload {
    action: String,
    /// Why a group was destroyed: `merged`, `invalidated` or `dequeued`.
    #[serde(default)]
    reason: Option<String>,
    merge_group: MergeGroup,
    repository: Repository,
    sender: Option<Account>,
}

#[derive(Deserialize)]
struct MergeGroup {
    head_sha: String,
    head_ref: String,
    base_sha: String,
    /// `refs/heads/main`.
    base_ref: Option<String>,
}

/// Read a verified delivery. `event_type` is the `X-GitHub-Event` header.
///
/// Handled, as the event queue needs them:
/// - `push`: a branch push, a tag push, or a deletion of either;
/// - `pull_request`: `opened`, `synchronize`, `reopened`, and `closed`;
/// - `merge_group`: `checks_requested`.
///
/// Everything else, `ping` included, is [`Parsed::Ignored`].
pub fn parse(event_type: &str, body: &[u8]) -> Result<Parsed, ParseError> {
    match event_type {
        "push" => parse_push(body),
        "pull_request" => parse_pull_request(body),
        "merge_group" => parse_merge_group(body),
        "check_run" => parse_check_run(body),
        "check_suite" => parse_check_suite(body),
        "workflow_job" => parse_workflow_job(body),
        other => Ok(Parsed::Ignored(format!("{other} events are not handled"))),
    }
}

fn read<'a, T: Deserialize<'a>>(event_type: &str, body: &'a [u8]) -> Result<T, ParseError> {
    serde_json::from_slice(body).map_err(|e| ParseError(format!("not a {event_type} payload: {e}")))
}

fn parse_push(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: PushPayload = read("push", body)?;
    let zero = |sha: &str| sha.bytes().all(|b| b == b'0');
    let (kind, sha, base_sha) = if p.deleted || zero(&p.after) {
        (Kind::Delete, p.before, None)
    } else if p.git_ref.starts_with("refs/tags/") {
        (Kind::Tag, p.after, None)
    } else if p.git_ref.starts_with("refs/heads/") {
        // The commit the branch pointed at before; a new branch had none.
        let before = (!zero(&p.before)).then_some(p.before);
        (Kind::Push, p.after, before)
    } else {
        return Ok(Parsed::Ignored(format!(
            "a push to {}, which is neither a branch nor a tag",
            p.git_ref
        )));
    };
    // An annotated tag is pushed as an object of its own: `after` is the
    // tag's id, which stays the event's, and `head_commit` is the commit.
    let commit = p
        .head_commit
        .map(|c| c.id)
        .filter(|id| kind == Kind::Tag && *id != sha && is_commit_id(id));
    // Only someone who can write to the repository can push to it.
    event(
        &p.repository,
        NewEvent {
            kind,
            git_ref: p.git_ref,
            sha,
            commit,
            base_sha,
            base_ref: None,
            pr: None,
            actor: p.sender.map(|s| s.login),
            trust: Trust::Trusted,
            association: None,
            head_ref: None,
        },
    )
}

/// A workflow job's delivery. `waiting` (a job held by an environment's
/// protection rule) is dropped: it is not asking for a runner yet, and is
/// delivered again as `queued` once it does.
fn parse_workflow_job(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: WorkflowJobPayload = read("workflow_job", body)?;
    let action = match p.action.as_str() {
        "queued" => JobAction::Queued,
        "in_progress" => JobAction::InProgress,
        "completed" => JobAction::Completed,
        other => {
            return Ok(Parsed::Ignored(format!(
                "a workflow job that is {other}: one is taken when it is queued"
            )))
        }
    };
    if p.repository.full_name.is_empty() || !is_storable_ref(&p.repository.full_name) {
        return Err(ParseError("the repository has no usable full_name".into()));
    }
    job_of(
        host_of(p.repository.html_url.as_deref()),
        p.repository.full_name.to_ascii_lowercase(),
        action,
        p.workflow_job,
    )
    .map(Parsed::Job)
}

/// A job as GitHub's API answers for it (`GET …/actions/jobs/{id}`), of
/// the repository `full_name` on `forge_host`: what a delivery of it would
/// say now. A job held by an environment's rule, or not yet asking for a
/// runner, is read as one that waits.
pub fn job_answer(forge_host: &str, full_name: &str, body: &[u8]) -> Result<Job, ParseError> {
    #[derive(Deserialize)]
    struct Answer {
        status: String,
        #[serde(flatten)]
        job: WorkflowJob,
    }
    let answer: Answer = read("a workflow job", body)?;
    let action = match answer.status.as_str() {
        "in_progress" => JobAction::InProgress,
        "completed" => JobAction::Completed,
        _ => JobAction::Queued,
    };
    job_of(
        forge_host.to_ascii_lowercase(),
        full_name.to_ascii_lowercase(),
        action,
        answer.job,
    )
}

/// A job, from what a delivery or the API says of it, with what an author
/// wrote held to what is kept.
fn job_of(
    forge_host: String,
    full_name: String,
    action: JobAction,
    job: WorkflowJob,
) -> Result<Job, ParseError> {
    if !is_commit_id(&job.head_sha) {
        return Err(ParseError(format!(
            "{:?} is not a full commit id",
            job.head_sha
        )));
    }
    if job.labels.len() > MAX_JOB_LABELS {
        return Err(ParseError(format!(
            "the job asks for {} labels, and {MAX_JOB_LABELS} are read",
            job.labels.len()
        )));
    }
    let kept = |text: String| -> String {
        text.chars()
            .filter(|c| !c.is_control())
            .take(MAX_JOB_TEXT)
            .collect()
    };
    Ok(Job {
        forge_host,
        full_name,
        action,
        id: job.id,
        run_id: job.run_id,
        run_attempt: job.run_attempt.unwrap_or(1).max(1),
        name: kept(job.name.unwrap_or_default()),
        workflow: job.workflow_name.map(kept).filter(|name| !name.is_empty()),
        labels: job.labels.into_iter().map(kept).collect(),
        head_sha: job.head_sha,
        runner_id: job.runner_id,
        runner_name: job.runner_name.map(kept).filter(|name| !name.is_empty()),
        conclusion: job
            .conclusion
            .filter(|c| matches!(c.as_str(), "success" | "failure" | "cancelled" | "skipped")),
    })
}

/// A check run's delivery. GitHub sends the App one whenever a check run of
/// its own is made or changes, which is dagron-ci's own doing and is
/// dropped; `rerequested` is a person asking, and is the one that is read.
fn parse_check_run(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: CheckRunPayload = read("check_run", body)?;
    if p.action != "rerequested" {
        return Ok(Parsed::Ignored(format!(
            "a check run that was {}: only a re-run asked of one is taken",
            p.action
        )));
    }
    if p.repository.full_name.is_empty() || !is_storable_ref(&p.repository.full_name) {
        return Err(ParseError("the repository has no usable full_name".into()));
    }
    Ok(Parsed::Rerequested(Rerequested {
        forge_host: host_of(p.repository.html_url.as_deref()),
        full_name: p.repository.full_name.to_ascii_lowercase(),
        of: Of::Run(p.check_run.external_id.unwrap_or_default()),
        app_id: p.check_run.app.map(|app| app.id),
        bot: p
            .sender
            .as_ref()
            .is_some_and(|s| s.kind.as_deref() == Some("Bot")),
        sender: p.sender.map(|s| s.login),
    }))
}

/// A check suite's delivery. GitHub makes a suite for each App at a commit,
/// and sends the App one when the suite is made and when it is over, which
/// is dropped; `rerequested` is a person asking for all of the commit's
/// checks to be run again ("Re-run all checks"), and is the one that is
/// read. A suite names no run of dagron-ci's: the commit is what it has.
fn parse_check_suite(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: CheckSuitePayload = read("check_suite", body)?;
    if p.action != "rerequested" {
        return Ok(Parsed::Ignored(format!(
            "a check suite that was {}: only a re-run asked of one is taken",
            p.action
        )));
    }
    if p.repository.full_name.is_empty() || !is_storable_ref(&p.repository.full_name) {
        return Err(ParseError("the repository has no usable full_name".into()));
    }
    let sha = p.check_suite.head_sha.unwrap_or_default();
    if !is_commit_id(&sha) {
        return Err(ParseError("the check suite names no commit".into()));
    }
    Ok(Parsed::Rerequested(Rerequested {
        forge_host: host_of(p.repository.html_url.as_deref()),
        full_name: p.repository.full_name.to_ascii_lowercase(),
        of: Of::Commit(sha),
        app_id: p.check_suite.app.map(|app| app.id),
        bot: p
            .sender
            .as_ref()
            .is_some_and(|s| s.kind.as_deref() == Some("Bot")),
        sender: p.sender.map(|s| s.login),
    }))
}

fn parse_pull_request(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: PullRequestPayload = read("pull_request", body)?;
    let kind = match p.action.as_str() {
        "opened" | "synchronize" | "reopened" => Kind::PullRequest,
        "closed" => Kind::PullRequestClosed,
        other => {
            return Ok(Parsed::Ignored(format!(
                "pull_request {other} is not handled"
            )))
        }
    };
    let trust = pull_request_trust(
        &p.pull_request.head,
        &p.repository,
        p.pull_request.author_association.as_deref(),
    );
    event(
        &p.repository,
        NewEvent {
            kind,
            git_ref: format!("refs/pull/{}/head", p.number),
            sha: p.pull_request.head.sha,
            commit: None,
            base_sha: Some(p.pull_request.base.sha),
            base_ref: p.pull_request.base.git_ref,
            pr: Some(p.number),
            actor: p.sender.map(|s| s.login),
            trust,
            association: crate::event::association(p.pull_request.author_association.as_deref()),
            head_ref: p.pull_request.head.git_ref.filter(|r| is_storable_ref(r)),
        },
    )
}

/// A pull request is trusted only when both hold: its head is a branch of the
/// repository itself, and the person who opened it is an owner, a member or a
/// collaborator. A fork's pull request, one whose fork is gone, and one opened
/// by anyone else are untrusted, whatever their branch is called. The webhook
/// and the poll sweep both decide it here.
fn pull_request_trust(head: &Branch, repository: &Repository, association: Option<&str>) -> Trust {
    let same_repo = match &head.repo {
        Some(head) => match (head.id, repository.id) {
            (Some(a), Some(b)) => a == b,
            _ => head.full_name.eq_ignore_ascii_case(&repository.full_name),
        },
        None => false,
    };
    let member = matches!(association, Some("OWNER" | "MEMBER" | "COLLABORATOR"));
    if same_repo && member {
        Trust::Trusted
    } else {
        Trust::Untrusted
    }
}

fn parse_merge_group(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: MergeGroupPayload = read("merge_group", body)?;
    let kind = match (p.action.as_str(), p.reason.as_deref()) {
        ("checks_requested", _) => Kind::MergeGroup,
        // A group that merged was let in by its checks: they are over.
        ("destroyed", Some("merged")) => {
            return Ok(Parsed::Ignored(
                "merge_group destroyed as merged: its checks are what let it merge".into(),
            ))
        }
        // Invalidated (a group ahead of it failed) or dequeued: nobody waits
        // on its checks now, and its runs are cancelled.
        ("destroyed", Some("invalidated" | "dequeued")) => Kind::MergeGroupDestroyed,
        // A reason GitHub has not said it sends, or none: nothing is
        // cancelled on a guess.
        ("destroyed", reason) => {
            return Ok(Parsed::Ignored(format!(
                "merge_group destroyed for {}: only invalidated and dequeued are let go of",
                reason.map_or("no reason".to_string(), |r| format!("{r:?}"))
            )))
        }
        (action, _) => {
            return Ok(Parsed::Ignored(format!(
                "merge_group {action} is not handled"
            )))
        }
    };
    // A merge group is made by the queue from pull requests a writer approved.
    event(
        &p.repository,
        NewEvent {
            kind,
            git_ref: p.merge_group.head_ref,
            sha: p.merge_group.head_sha,
            commit: None,
            base_sha: Some(p.merge_group.base_sha),
            base_ref: p.merge_group.base_ref,
            pr: None,
            actor: p.sender.map(|s| s.login),
            trust: Trust::Trusted,
            association: None,
            head_ref: None,
        },
    )
}

fn event(repository: &Repository, event: NewEvent) -> Result<Parsed, ParseError> {
    if !is_storable_ref(&event.git_ref) {
        return Err(ParseError(
            "the ref is empty, too long, or not printable".into(),
        ));
    }
    if event
        .base_ref
        .as_deref()
        .is_some_and(|r| !is_storable_ref(r))
    {
        return Err(ParseError(
            "the base branch is empty, too long, or not printable".into(),
        ));
    }
    for sha in std::iter::once(&event.sha).chain(event.base_sha.iter()) {
        if !is_commit_id(sha) {
            return Err(ParseError(format!("{sha:?} is not a full commit id")));
        }
    }
    if repository.full_name.is_empty() || !is_storable_ref(&repository.full_name) {
        return Err(ParseError("the repository has no usable full_name".into()));
    }
    Ok(Parsed::Event {
        forge_host: host_of(repository.html_url.as_deref()),
        full_name: repository.full_name.to_ascii_lowercase(),
        event,
    })
}

/// The host of the repository's web URL, lowercased, or github.com when the
/// payload names none.
fn host_of(html_url: Option<&str>) -> String {
    html_url
        .and_then(|u| u.split_once("://").map(|(_, rest)| rest))
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .map(|authority| authority.rsplit('@').next().unwrap_or(authority))
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| DEFAULT_HOST.to_string())
}

/// One open pull request, as `GET /repos/{owner}/{repo}/pulls` lists it.
#[derive(Deserialize)]
struct ListedPullRequest {
    number: i64,
    head: Branch,
    base: Branch,
    author_association: Option<String>,
    user: Option<Account>,
}

/// Read one page of `GET /repos/{owner}/{repo}/pulls?state=open`, for the poll
/// sweep: each open pull request as the event its head would queue, its trust
/// decided as for a webhook's. `full_name` is the repository listed, for an
/// entry that does not name its base repository. An entry without a usable
/// number or commit is left out rather than failing the page.
pub(crate) fn parse_open_pulls(body: &[u8], full_name: &str) -> Result<Vec<NewEvent>, ParseError> {
    let listed: Vec<ListedPullRequest> = serde_json::from_slice(body)
        .map_err(|e| ParseError(format!("not a list of pull requests: {e}")))?;
    let polled = Repository {
        id: None,
        full_name: full_name.to_string(),
        html_url: None,
    };
    Ok(listed
        .into_iter()
        .filter(|pr| pr.number > 0 && is_commit_id(&pr.head.sha) && is_commit_id(&pr.base.sha))
        .filter(|pr| pr.base.git_ref.as_deref().is_none_or(is_storable_ref))
        .map(|pr| {
            let repository = pr.base.repo.as_ref().unwrap_or(&polled);
            let trust = pull_request_trust(&pr.head, repository, pr.author_association.as_deref());
            NewEvent {
                kind: Kind::PullRequest,
                git_ref: format!("refs/pull/{}/head", pr.number),
                sha: pr.head.sha,
                commit: None,
                base_sha: Some(pr.base.sha),
                base_ref: pr.base.git_ref,
                pr: Some(pr.number),
                actor: pr.user.map(|u| u.login),
                trust,
                association: crate::event::association(pr.author_association.as_deref()),
                head_ref: pr.head.git_ref.filter(|r| is_storable_ref(r)),
            }
        })
        .collect())
}

/// The `X-Hub-Signature-256` value GitHub would send for `body`. Tests and
/// local tooling use it to sign deliveries; the receiver never does.
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) const HEAD: &str = "1111111111111111111111111111111111111111";
    pub(crate) const BASE: &str = "2222222222222222222222222222222222222222";

    pub(crate) fn push_body(git_ref: &str) -> Vec<u8> {
        json!({
            "ref": git_ref,
            "before": BASE,
            "after": HEAD,
            "created": false,
            "deleted": false,
            "repository": { "id": 7, "full_name": "Acme/Widgets", "html_url": "https://github.com/Acme/Widgets" },
            "sender": { "login": "octocat" },
        })
        .to_string()
        .into_bytes()
    }

    fn pull_request_body(action: &str, head_repo: serde_json::Value, association: &str) -> Vec<u8> {
        json!({
            "action": action,
            "number": 42,
            "pull_request": {
                "head": { "ref": "topic", "sha": HEAD, "repo": head_repo },
                "base": { "ref": "main", "sha": BASE, "repo": { "id": 7, "full_name": "acme/widgets" } },
                "author_association": association,
                "title": "$(curl evil)",
            },
            "repository": { "id": 7, "full_name": "acme/widgets", "html_url": "https://github.com/acme/widgets" },
            "sender": { "login": "contributor" },
        })
        .to_string()
        .into_bytes()
    }

    fn same_repo() -> serde_json::Value {
        json!({ "id": 7, "full_name": "acme/widgets" })
    }

    fn fork() -> serde_json::Value {
        json!({ "id": 99, "full_name": "contributor/widgets" })
    }

    fn event_of(parsed: Parsed) -> (String, String, NewEvent) {
        match parsed {
            Parsed::Event {
                forge_host,
                full_name,
                event,
            } => (forge_host, full_name, event),
            Parsed::Ignored(why) => panic!("ignored: {why}"),
            Parsed::Rerequested(asked) => panic!("a re-run, and no event: {asked:?}"),
            Parsed::Job(job) => panic!("a workflow job, and no event: {job:?}"),
            Parsed::Train(train) => panic!("a GitLab pipeline, and no event: {train:?}"),
        }
    }

    #[test]
    fn a_signature_made_with_the_secret_verifies() {
        let body = b"{\"zen\":\"Keep it logically awesome.\"}";
        let header = sign(b"s3cret", body);
        assert_eq!(verify_signature(b"s3cret", Some(&header), body), Ok(()));
    }

    #[test]
    fn github_s_published_example_verifies() {
        // docs.github.com, "Validating webhook deliveries": secret "It's a
        // Secret to Everybody", payload "Hello, World!".
        let header = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
        assert_eq!(
            verify_signature(
                b"It's a Secret to Everybody",
                Some(header),
                b"Hello, World!"
            ),
            Ok(())
        );
    }

    #[test]
    fn a_wrong_secret_or_a_changed_body_is_a_mismatch() {
        let body = b"{}";
        let header = sign(b"s3cret", body);
        assert_eq!(
            verify_signature(b"other", Some(&header), body),
            Err(SignatureError::Mismatch)
        );
        assert_eq!(
            verify_signature(b"s3cret", Some(&header), b"{ }"),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn a_missing_or_malformed_header_is_refused() {
        assert_eq!(
            verify_signature(b"k", None, b"{}"),
            Err(SignatureError::Missing)
        );
        for header in ["", "sha1=00", "sha256=zz", "sha256=abcd", "deadbeef"] {
            assert_eq!(
                verify_signature(b"k", Some(header), b"{}"),
                Err(SignatureError::Malformed),
                "{header:?}"
            );
        }
    }

    #[test]
    fn a_branch_push_is_a_trusted_push_event() {
        let (host, name, ev) = event_of(parse("push", &push_body("refs/heads/main")).unwrap());
        assert_eq!(host, "github.com");
        assert_eq!(name, "acme/widgets", "lowercased");
        assert_eq!(ev.kind, Kind::Push);
        assert_eq!(ev.git_ref, "refs/heads/main");
        assert_eq!(ev.sha, HEAD);
        assert_eq!(ev.trust, Trust::Trusted);
        assert_eq!(ev.actor.as_deref(), Some("octocat"));
        assert_eq!(
            ev.base_sha.as_deref(),
            Some(BASE),
            "measured against where the branch was"
        );

        // A new branch was nowhere before.
        let mut body: serde_json::Value =
            serde_json::from_slice(&push_body("refs/heads/topic")).unwrap();
        body["before"] = json!("0".repeat(40));
        body["created"] = json!(true);
        let (_, _, ev) = event_of(parse("push", body.to_string().as_bytes()).unwrap());
        assert_eq!((ev.kind, ev.base_sha), (Kind::Push, None));
    }

    #[test]
    fn a_tag_push_is_a_tag_event() {
        let (_, _, ev) = event_of(parse("push", &push_body("refs/tags/v1.0.0")).unwrap());
        assert_eq!(ev.kind, Kind::Tag);
        assert_eq!(ev.sha, HEAD);
        assert_eq!(ev.base_sha, None, "a tag is measured against nothing");
        assert_eq!(ev.commit, None, "no head_commit: the id is all there is");
    }

    /// GitHub pushes an annotated tag as the tag object: `after` is its id,
    /// and `head_commit` the commit it points at. The event keeps the tag's
    /// id, which a poll sweep lists too, and carries the commit beside it.
    #[test]
    fn an_annotated_tag_s_push_names_the_commit_beside_the_tag_s_id() {
        const COMMIT: &str = "3333333333333333333333333333333333333333";
        let tag = |head_commit: serde_json::Value| {
            let mut body: serde_json::Value =
                serde_json::from_slice(&push_body("refs/tags/v1.0.0")).unwrap();
            body["head_commit"] = head_commit;
            let (_, _, ev) = event_of(parse("push", body.to_string().as_bytes()).unwrap());
            (ev.kind, ev.sha, ev.commit)
        };
        assert_eq!(
            tag(json!({ "id": COMMIT, "message": "release" })),
            (Kind::Tag, HEAD.to_string(), Some(COMMIT.to_string()))
        );
        // A lightweight tag is its commit's own id; and what is no commit id
        // is not kept.
        for head_commit in [json!({ "id": HEAD }), json!({ "id": "main" }), json!(null)] {
            assert_eq!(tag(head_commit), (Kind::Tag, HEAD.to_string(), None));
        }
        // A branch's head_commit is its `after`, and says nothing more.
        let mut body: serde_json::Value =
            serde_json::from_slice(&push_body("refs/heads/main")).unwrap();
        body["head_commit"] = json!({ "id": COMMIT });
        let (_, _, ev) = event_of(parse("push", body.to_string().as_bytes()).unwrap());
        assert_eq!((ev.kind, ev.commit), (Kind::Push, None));
    }

    #[test]
    fn a_deletion_records_the_commit_the_ref_last_pointed_at() {
        let mut body: serde_json::Value =
            serde_json::from_slice(&push_body("refs/heads/topic")).unwrap();
        body["deleted"] = json!(true);
        body["after"] = json!("0".repeat(40));
        let (_, _, ev) = event_of(parse("push", body.to_string().as_bytes()).unwrap());
        assert_eq!(ev.kind, Kind::Delete);
        assert_eq!(ev.sha, BASE);
    }

    #[test]
    fn a_push_to_another_ref_namespace_is_ignored() {
        assert!(matches!(
            parse("push", &push_body("refs/notes/commits")).unwrap(),
            Parsed::Ignored(_)
        ));
    }

    #[test]
    fn a_member_s_branch_pull_request_is_trusted() {
        let body = pull_request_body("opened", same_repo(), "MEMBER");
        let (_, _, ev) = event_of(parse("pull_request", &body).unwrap());
        assert_eq!(ev.kind, Kind::PullRequest);
        assert_eq!(ev.git_ref, "refs/pull/42/head");
        assert_eq!(ev.sha, HEAD);
        assert_eq!(ev.base_sha.as_deref(), Some(BASE));
        assert_eq!(ev.base_ref.as_deref(), Some("main"));
        assert_eq!(ev.pr, Some(42));
        assert_eq!(ev.trust, Trust::Trusted);
    }

    #[test]
    fn a_fork_s_pull_request_is_untrusted_even_from_a_member() {
        let body = pull_request_body("synchronize", fork(), "MEMBER");
        let (_, _, ev) = event_of(parse("pull_request", &body).unwrap());
        assert_eq!(ev.trust, Trust::Untrusted);
    }

    #[test]
    fn a_non_member_s_pull_request_is_untrusted_even_from_a_branch() {
        for association in [
            "CONTRIBUTOR",
            "FIRST_TIME_CONTRIBUTOR",
            "FIRST_TIMER",
            "NONE",
            "MANNEQUIN",
        ] {
            let body = pull_request_body("opened", same_repo(), association);
            let (_, _, ev) = event_of(parse("pull_request", &body).unwrap());
            assert_eq!(ev.trust, Trust::Untrusted, "{association}");
        }
    }

    #[test]
    fn a_pull_request_whose_fork_is_gone_is_untrusted() {
        let body = pull_request_body("reopened", serde_json::Value::Null, "OWNER");
        let (_, _, ev) = event_of(parse("pull_request", &body).unwrap());
        assert_eq!(ev.trust, Trust::Untrusted);
    }

    #[test]
    fn a_closed_pull_request_is_its_own_kind() {
        let body = pull_request_body("closed", same_repo(), "MEMBER");
        let (_, _, ev) = event_of(parse("pull_request", &body).unwrap());
        assert_eq!(ev.kind, Kind::PullRequestClosed);
    }

    #[test]
    fn other_pull_request_actions_are_ignored() {
        for action in ["edited", "labeled", "assigned", "review_requested"] {
            let body = pull_request_body(action, same_repo(), "MEMBER");
            assert!(
                matches!(parse("pull_request", &body).unwrap(), Parsed::Ignored(_)),
                "{action}"
            );
        }
    }

    #[test]
    fn a_merge_group_asking_for_checks_is_trusted() {
        let body = json!({
            "action": "checks_requested",
            "merge_group": {
                "head_sha": HEAD,
                "head_ref": "refs/heads/gh-readonly-queue/main/pr-42-2222222222222222222222222222222222222222",
                "base_sha": BASE,
                "base_ref": "refs/heads/main",
            },
            "repository": { "id": 7, "full_name": "acme/widgets" },
            "sender": { "login": "github-merge-queue" },
        })
        .to_string();
        let (host, _, ev) = event_of(parse("merge_group", body.as_bytes()).unwrap());
        assert_eq!(host, "github.com", "no html_url falls back to github.com");
        assert_eq!(ev.kind, Kind::MergeGroup);
        assert_eq!(ev.trust, Trust::Trusted);
        assert_eq!(ev.base_sha.as_deref(), Some(BASE));
        assert_eq!(ev.base_ref.as_deref(), Some("refs/heads/main"));
    }

    /// A group the queue lets go of without merging it is an event of its
    /// own, whose runs are cancelled; one that merged had its checks, and is
    /// nothing to do.
    #[test]
    fn a_merge_group_let_go_of_unmerged_is_an_event_and_a_merged_one_is_not() {
        let destroyed = |reason: &str| {
            json!({
                "action": "destroyed",
                "reason": reason,
                "merge_group": {
                    "head_sha": HEAD,
                    "head_ref": "refs/heads/gh-readonly-queue/main/pr-42-2222222222222222222222222222222222222222",
                    "base_sha": BASE,
                    "base_ref": "refs/heads/main",
                },
                "repository": { "id": 7, "full_name": "acme/widgets" },
                "sender": { "login": "github-merge-queue" },
            })
            .to_string()
        };
        for reason in ["invalidated", "dequeued"] {
            let (_, _, ev) = event_of(parse("merge_group", destroyed(reason).as_bytes()).unwrap());
            assert_eq!(ev.kind, Kind::MergeGroupDestroyed, "{reason}");
            assert_eq!(
                ev.git_ref,
                "refs/heads/gh-readonly-queue/main/pr-42-2222222222222222222222222222222222222222"
            );
            assert_eq!(ev.sha, HEAD);
        }
        assert!(matches!(
            parse("merge_group", destroyed("merged").as_bytes()).unwrap(),
            Parsed::Ignored(why) if why.contains("as merged")
        ));
        // A reason GitHub has not said it sends, and none at all.
        assert!(matches!(
            parse("merge_group", destroyed("rebased").as_bytes()).unwrap(),
            Parsed::Ignored(why) if why.contains("\"rebased\"")
        ));
        let mut none: serde_json::Value = serde_json::from_str(&destroyed("x")).unwrap();
        none.as_object_mut().unwrap().remove("reason");
        assert!(matches!(
            parse("merge_group", none.to_string().as_bytes()).unwrap(),
            Parsed::Ignored(why) if why.contains("no reason")
        ));
    }

    #[test]
    fn an_enterprise_server_repository_is_recorded_under_its_host() {
        let mut body: serde_json::Value =
            serde_json::from_slice(&push_body("refs/heads/main")).unwrap();
        body["repository"]["html_url"] = json!("https://GHE.example.internal:8443/acme/widgets");
        let (host, _, _) = event_of(parse("push", body.to_string().as_bytes()).unwrap());
        assert_eq!(host, "ghe.example.internal:8443");
    }

    #[test]
    fn unhandled_events_are_ignored_and_ping_among_them() {
        for event_type in ["ping", "issues", "star", "workflow_run"] {
            assert!(
                matches!(parse(event_type, b"{}").unwrap(), Parsed::Ignored(_)),
                "{event_type}"
            );
        }
    }

    #[test]
    fn a_check_run_s_re_run_is_read_and_its_other_news_dropped() {
        let delivery = |action: &str, sender: &str| {
            json!({
                "action": action,
                "check_run": {
                    "id": 9,
                    "name": "dagron-ci/ci/test (push)",
                    "external_id": "run-1",
                    "app": { "id": 4242 },
                },
                "repository": { "id": 7, "full_name": "Acme/Widgets", "html_url": "https://github.com/Acme/Widgets" },
                "sender": { "login": "octocat", "type": sender },
            })
            .to_string()
        };
        assert_eq!(
            parse("check_run", delivery("rerequested", "User").as_bytes()).unwrap(),
            Parsed::Rerequested(Rerequested {
                forge_host: "github.com".into(),
                full_name: "acme/widgets".into(),
                of: Of::Run("run-1".into()),
                app_id: Some(4242),
                sender: Some("octocat".into()),
                bot: false,
            })
        );
        match parse("check_run", delivery("rerequested", "Bot").as_bytes()).unwrap() {
            Parsed::Rerequested(asked) => assert!(asked.bot),
            other => panic!("{other:?}"),
        }
        // What dagron-ci's own posts come back as, and a button it has not.
        for action in ["created", "completed", "requested_action"] {
            let parsed = parse("check_run", delivery(action, "User").as_bytes()).unwrap();
            assert!(matches!(parsed, Parsed::Ignored(_)), "{action}: {parsed:?}");
        }
        assert!(parse("check_run", b"{}").is_err());
    }

    #[test]
    fn a_re_run_of_all_checks_is_read_by_its_commit_and_a_suite_s_other_news_dropped() {
        let sha = "c".repeat(40);
        let delivery = |action: &str, sender: &str, sha: &str| {
            json!({
                "action": action,
                "check_suite": { "id": 5, "head_sha": sha, "app": { "id": 4242 } },
                "repository": { "id": 7, "full_name": "Acme/Widgets", "html_url": "https://github.com/Acme/Widgets" },
                "sender": { "login": "octocat", "type": sender },
            })
            .to_string()
        };
        assert_eq!(
            parse(
                "check_suite",
                delivery("rerequested", "User", &sha).as_bytes()
            )
            .unwrap(),
            Parsed::Rerequested(Rerequested {
                forge_host: "github.com".into(),
                full_name: "acme/widgets".into(),
                of: Of::Commit(sha.clone()),
                app_id: Some(4242),
                sender: Some("octocat".into()),
                bot: false,
            })
        );
        match parse(
            "check_suite",
            delivery("rerequested", "Bot", &sha).as_bytes(),
        )
        .unwrap()
        {
            Parsed::Rerequested(asked) => assert!(asked.bot),
            other => panic!("{other:?}"),
        }
        // What GitHub says of a suite by itself: made, and over.
        for action in ["requested", "completed"] {
            let parsed = parse("check_suite", delivery(action, "User", &sha).as_bytes()).unwrap();
            assert!(matches!(parsed, Parsed::Ignored(_)), "{action}: {parsed:?}");
        }
        // A suite that names no commit names nothing.
        assert!(parse(
            "check_suite",
            delivery("rerequested", "User", "main").as_bytes()
        )
        .is_err());
        assert!(parse("check_suite", b"{}").is_err());
    }

    #[test]
    fn a_workflow_job_is_read_as_it_is_queued_taken_and_ended() {
        let delivery = |action: &str| {
            json!({
                "action": action,
                "workflow_job": {
                    "id": 31_415_926_535u64,
                    "run_id": 27_182_818_284u64,
                    "run_attempt": 2,
                    "workflow_name": "CI",
                    "name": "build (stable)",
                    "head_branch": "main",
                    "head_sha": HEAD,
                    "status": action,
                    "conclusion": if action == "completed" { json!("failure") } else { json!(null) },
                    "labels": ["self-hosted", "Linux", "X64", "dagron-large"],
                    "runner_id": if action == "queued" { json!(null) } else { json!(21) },
                    "runner_name": if action == "queued" { json!(null) } else { json!("dagron-31415926535-1") },
                },
                "repository": { "id": 7, "full_name": "Acme/Widgets", "html_url": "https://github.com/Acme/Widgets" },
                "sender": { "login": "octocat", "type": "User" },
                "installation": { "id": 4242 },
            })
        };
        let read =
            |action: &str| match parse("workflow_job", delivery(action).to_string().as_bytes())
                .unwrap()
            {
                Parsed::Job(job) => job,
                other => panic!("{action}: {other:?}"),
            };
        let queued = read("queued");
        assert_eq!(
            queued,
            Job {
                forge_host: "github.com".into(),
                full_name: "acme/widgets".into(),
                action: JobAction::Queued,
                id: 31_415_926_535,
                run_id: 27_182_818_284,
                run_attempt: 2,
                name: "build (stable)".into(),
                workflow: Some("CI".into()),
                labels: vec![
                    "self-hosted".into(),
                    "Linux".into(),
                    "X64".into(),
                    "dagron-large".into()
                ],
                head_sha: HEAD.into(),
                runner_id: None,
                runner_name: None,
                conclusion: None,
            }
        );
        let taken = read("in_progress");
        assert_eq!(
            (taken.action, taken.runner_id, taken.runner_name.as_deref()),
            (
                JobAction::InProgress,
                Some(21),
                Some("dagron-31415926535-1")
            )
        );
        let ended = read("completed");
        assert_eq!(
            (ended.action, ended.conclusion.as_deref()),
            (JobAction::Completed, Some("failure"))
        );

        // Held by an environment's rule: not asking for a runner yet.
        let waiting = delivery("waiting").to_string();
        assert!(matches!(
            parse("workflow_job", waiting.as_bytes()).unwrap(),
            Parsed::Ignored(_)
        ));
        // What is not a job's delivery, or names no commit, is not read.
        assert!(parse("workflow_job", b"{}").is_err());
        let mut short = delivery("queued");
        short["workflow_job"]["head_sha"] = json!("4b4708e");
        assert!(parse("workflow_job", short.to_string().as_bytes()).is_err());
        let mut many = delivery("queued");
        many["workflow_job"]["labels"] = json!(vec!["l"; 33]);
        assert!(parse("workflow_job", many.to_string().as_bytes()).is_err());
    }

    #[test]
    fn a_payload_that_is_not_json_or_lacks_fields_is_a_parse_error() {
        assert!(parse("push", b"payload=%7B%7D").is_err(), "form-encoded");
        assert!(parse("push", b"{}").is_err(), "no ref");
    }

    #[test]
    fn a_short_or_uppercase_sha_is_refused() {
        let mut body: serde_json::Value =
            serde_json::from_slice(&push_body("refs/heads/main")).unwrap();
        body["after"] = json!("ABCDEF");
        assert!(parse("push", body.to_string().as_bytes()).is_err());
    }

    /// One entry of `GET /repos/acme/widgets/pulls`, trimmed to what is read.
    pub(crate) fn listed_pull(
        number: i64,
        head: &str,
        head_repo: serde_json::Value,
        association: &str,
    ) -> serde_json::Value {
        json!({
            "number": number,
            "state": "open",
            "title": "$(curl evil)",
            "user": { "login": "contributor" },
            "author_association": association,
            "head": { "ref": "topic", "sha": head, "repo": head_repo },
            "base": { "ref": "main", "sha": BASE, "repo": { "id": 7, "full_name": "acme/widgets" } },
        })
    }

    #[test]
    fn a_listed_pull_request_queues_what_its_webhook_would() {
        let page = json!([
            listed_pull(42, HEAD, same_repo(), "MEMBER"),
            listed_pull(43, HEAD, fork(), "OWNER"),
            listed_pull(44, HEAD, same_repo(), "CONTRIBUTOR"),
            listed_pull(45, HEAD, serde_json::Value::Null, "MEMBER"),
            listed_pull(46, "not-a-sha", same_repo(), "MEMBER"),
        ])
        .to_string();
        let listed = parse_open_pulls(page.as_bytes(), "acme/widgets").unwrap();
        assert_eq!(
            listed.iter().map(|e| (e.pr, e.trust)).collect::<Vec<_>>(),
            [
                (Some(42), Trust::Trusted),
                (Some(43), Trust::Untrusted),
                (Some(44), Trust::Untrusted),
                (Some(45), Trust::Untrusted),
            ],
            "a fork, a non-member and a gone fork are untrusted; a bad entry is left out"
        );

        // The same commit key as the webhook's for that head, so the two
        // doors share one run.
        let (_, _, hooked) = event_of(
            parse(
                "pull_request",
                &pull_request_body("synchronize", same_repo(), "MEMBER"),
            )
            .unwrap(),
        );
        assert_eq!(
            crate::event::commit_key("r1", &listed[0]),
            crate::event::commit_key("r1", &hooked)
        );
        assert_eq!(
            (listed[0].base_sha.as_deref(), listed[0].actor.as_deref()),
            (Some(BASE), Some("contributor"))
        );
        assert_eq!(listed[0], hooked, "the same event, base branch and all");

        // An entry that names no base repository is measured against the
        // repository listed.
        let mut bare = listed_pull(47, HEAD, json!({ "full_name": "Acme/Widgets" }), "MEMBER");
        bare["base"]["repo"] = serde_json::Value::Null;
        let listed =
            parse_open_pulls(json!([bare]).to_string().as_bytes(), "acme/widgets").unwrap();
        assert_eq!(listed[0].trust, Trust::Trusted);
        assert!(parse_open_pulls(b"{\"message\":\"Not Found\"}", "acme/widgets").is_err());
    }
}
