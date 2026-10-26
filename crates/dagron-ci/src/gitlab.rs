//! The GitLab door: what `POST /hooks/gitlab` checks and how it reads a
//! delivery.
//!
//! GitLab signs nothing. A project's webhook carries the secret token it was
//! given in `X-Gitlab-Token`, and the receiver compares it with
//! CI_GITLAB_WEBHOOK_SECRET_FILE's, in constant time, before anything reads
//! the body. A delivery is named by `webhook-id`, which GitLab keeps across
//! its retries (`Idempotency-Key` where it sends that alone), and by
//! `X-Gitlab-Event-UUID` only where it sends neither: a webhook another
//! webhook set off shares that one, and would be taken for its duplicate.
//!
//! Three events lead to a run: a push to a branch (`Push Hook`), a tag
//! pushed (`Tag Push Hook`), and a merge request opened, reopened, or given
//! new commits (`Merge Request Hook`), which is a pull request's event at
//! `refs/merge-requests/{iid}/head`. A merge request closed or merged is a
//! closed pull request's event, which runs nothing, as GitHub's does; its
//! runs still going are not cancelled. A job's (`Job Hook`) is fleet mode's
//! ([`crate::fleet`]): what became of one job, by its id, which names no
//! tag of the job's. Any other is acknowledged and dropped.
//!
//! **A merge train** is GitLab's merge queue. Each merge
//! request on one has a pipeline of GitLab's at
//! `refs/merge-requests/{iid}/train`, of the commit that merges it with the
//! target branch and every request ahead of it; GitLab merges it once that
//! pipeline passes. A `Pipeline Hook` does not say which of a merge request's
//! pipelines it is of: its `ref` is the request's source branch whichever it
//! is (GitLab's `source_ref`). So a merge request's pipeline is read as a
//! [`Train`] of the request's, and the receiver asks GitLab's pipelines API,
//! by the pipeline's id, whether that is the ref it ran at. One that is, and
//! has yet to end, is a merge group's event, as GitHub's `merge_group`
//! `checks_requested` is: the
//! pipelines that take `on.merge_group` run at that commit, trusted, as only
//! someone who may merge puts a request on the train. Their statuses go to
//! that commit under the train's ref, so GitLab counts them in the train's
//! pipeline and waits on them. A train pipeline cancelled, as the train
//! drops the request or makes its pipeline again behind one that failed, is
//! the group let go of (`merge_group_destroyed`): its runs are cancelled. One
//! that passed, failed or was skipped is over, and nothing is done.

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::event::{is_commit_id, is_storable_ref, Kind, NewEvent};
use crate::forge::gitlab::merge_request_trust;
use crate::github::{ParseError, Parsed};

pub const TOKEN_HEADER: &str = "x-gitlab-token";
pub const EVENT_HEADER: &str = "x-gitlab-event";
/// The headers a delivery is named by, the first GitLab sends.
pub const DELIVERY_HEADERS: [&str; 3] = ["webhook-id", "idempotency-key", "x-gitlab-event-uuid"];

/// Whether `said` is the webhook's secret token. Both are hashed first, so
/// the comparison takes as long whatever either's length.
pub fn token_matches(secret: &[u8], said: Option<&str>) -> bool {
    let Some(said) = said else {
        return false;
    };
    let (a, b) = (Sha256::digest(secret), Sha256::digest(said.as_bytes()));
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[derive(Deserialize)]
struct Project {
    web_url: Option<String>,
    path_with_namespace: String,
}

#[derive(Deserialize)]
struct PushPayload {
    #[serde(rename = "ref")]
    git_ref: String,
    before: String,
    after: String,
    checkout_sha: Option<String>,
    user_username: Option<String>,
    project: Project,
}

#[derive(Deserialize)]
struct MergeRequestPayload {
    user: Option<User>,
    project: Project,
    object_attributes: MergeRequestAttributes,
}

#[derive(Deserialize)]
struct User {
    username: String,
}

#[derive(Deserialize)]
struct MergeRequestAttributes {
    iid: i64,
    action: Option<String>,
    /// The merge request's head before an update: there only where the
    /// update brought new commits.
    oldrev: Option<String>,
    source_branch: Option<String>,
    target_branch: Option<String>,
    source_project_id: Option<i64>,
    target_project_id: Option<i64>,
    last_commit: Option<Commit>,
    diff_refs: Option<DiffRefs>,
}

#[derive(Deserialize)]
struct Commit {
    id: String,
}

#[derive(Deserialize)]
struct DiffRefs {
    base_sha: Option<String>,
}

fn read<'a, T: Deserialize<'a>>(event_type: &str, body: &'a [u8]) -> Result<T, ParseError> {
    serde_json::from_slice(body).map_err(|e| ParseError(format!("{event_type}: {e}")))
}

/// A verified delivery of `event_type` (`X-Gitlab-Event`), read.
pub fn parse(event_type: &str, body: &[u8]) -> Result<Parsed, ParseError> {
    match event_type {
        "Push Hook" | "Tag Push Hook" => parse_push(event_type, body),
        "Merge Request Hook" => parse_merge_request(body),
        "Job Hook" => parse_job(body),
        "Pipeline Hook" => parse_pipeline(body),
        other => Ok(Parsed::Ignored(format!("{other} is not handled"))),
    }
}

fn parse_push(event_type: &str, body: &[u8]) -> Result<Parsed, ParseError> {
    let p: PushPayload = read(event_type, body)?;
    let zero = |sha: &str| sha.bytes().all(|b| b == b'0');
    let (kind, sha, base_sha) = if zero(&p.after) {
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
    // tag's id, which stays the event's, and `checkout_sha` the commit.
    let commit = p
        .checkout_sha
        .filter(|id| kind == Kind::Tag && *id != sha && is_commit_id(id));
    // Only someone who can write to the project can push to it.
    event(
        &p.project,
        NewEvent {
            kind,
            git_ref: p.git_ref,
            sha,
            commit,
            base_sha,
            base_ref: None,
            pr: None,
            actor: p.user_username,
            trust: crate::event::Trust::Trusted,
            association: None,
            head_ref: None,
        },
    )
}

fn parse_merge_request(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: MergeRequestPayload = read("Merge Request Hook", body)?;
    let mr = p.object_attributes;
    let action = mr.action.as_deref().unwrap_or("");
    let kind = match action {
        "open" | "reopen" => Kind::PullRequest,
        // An update is a new head only where it says what the head was.
        "update" if mr.oldrev.is_some() => Kind::PullRequest,
        "close" | "merge" => Kind::PullRequestClosed,
        "update" => {
            return Ok(Parsed::Ignored(
                "a merge request updated without new commits".into(),
            ))
        }
        other => {
            return Ok(Parsed::Ignored(format!(
                "a merge request's {other:?} is not handled"
            )))
        }
    };
    let Some(sha) = mr.last_commit.map(|c| c.id) else {
        return Err(ParseError("the merge request names no last commit".into()));
    };
    let trust = merge_request_trust(mr.source_project_id, mr.target_project_id);
    event(
        &p.project,
        NewEvent {
            kind,
            git_ref: format!("refs/merge-requests/{}/head", mr.iid),
            sha,
            commit: None,
            base_sha: mr
                .diff_refs
                .and_then(|d| d.base_sha)
                .filter(|s| is_commit_id(s)),
            base_ref: mr.target_branch,
            pr: Some(mr.iid),
            actor: p.user.map(|u| u.username),
            trust,
            association: None,
            head_ref: mr.source_branch.filter(|r| is_storable_ref(r)),
        },
    )
}

#[derive(Deserialize)]
struct PipelinePayload {
    object_attributes: PipelineAttributes,
    #[serde(default)]
    merge_request: Option<PipelineMergeRequest>,
    user: Option<User>,
    project: Project,
}

/// Just whether a pipeline's delivery names a merge request.
#[derive(Deserialize)]
struct PipelineOf {
    #[serde(default)]
    merge_request: Option<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct PipelineAttributes {
    #[serde(default)]
    id: Option<u64>,
    sha: String,
    status: String,
}

/// A merge request's pipeline, read as its merge train's: which it is only
/// once GitLab says the pipeline `pipeline` ran at `event`'s ref
/// (`refs/merge-requests/{iid}/train`). The hook cannot say: its `ref` is
/// the request's source branch, whichever of the request's pipelines it is.
#[derive(Debug, PartialEq, Eq)]
pub struct Train {
    pub forge_host: String,
    pub full_name: String,
    /// The pipeline's id, which GitLab is asked about.
    pub pipeline: String,
    /// The merge group's event it is, where it is the train's.
    pub event: NewEvent,
}

#[derive(Deserialize)]
struct PipelineMergeRequest {
    iid: i64,
    target_branch: Option<String>,
}

/// The merge request whose merge train `git_ref` is
/// (`refs/merge-requests/{iid}/train`).
pub fn train_of(git_ref: &str) -> Option<i64> {
    let iid = git_ref
        .strip_prefix("refs/merge-requests/")?
        .strip_suffix("/train")?;
    if iid.is_empty() || iid.len() > 18 || !iid.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    iid.parse().ok()
}

/// Whether a pipeline that is `status` has yet to end: its checks are
/// waited on, or are about to be.
pub(crate) fn waits(status: &str) -> bool {
    matches!(
        status,
        "created" | "waiting_for_resource" | "preparing" | "pending" | "running" | "scheduled"
    )
}

/// A pipeline's delivery: a merge request's, as what may be its merge
/// train's ([`Train`]), and any other's dropped (a push or a merge request
/// is said by its own hook).
fn parse_pipeline(body: &[u8]) -> Result<Parsed, ParseError> {
    // Whether it is a merge request's is read first, and alone: a pipeline of
    // none is dropped whatever else its delivery carries, so it is
    // acknowledged and not refused for a field only a train's needs.
    let of: PipelineOf = read("Pipeline Hook", body)?;
    if of.merge_request.is_none() {
        return Ok(Parsed::Ignored(
            "a pipeline of no merge request: a push or a merge request is said by its own hook"
                .into(),
        ));
    }
    let p: PipelinePayload = read("Pipeline Hook", body)?;
    let at = p.object_attributes;
    let Some(mr) = p.merge_request else {
        return Err(ParseError(
            "a merge request's pipeline names no merge request".into(),
        ));
    };
    let Some(pipeline) = at.id else {
        return Err(ParseError("a merge request's pipeline names no id".into()));
    };
    if mr.iid <= 0 {
        return Err(ParseError(format!("!{} names no merge request", mr.iid)));
    }
    let kind = match at.status.as_str() {
        status if waits(status) => Kind::MergeGroup,
        // The train let go of it: nobody waits on its checks now.
        "canceled" | "canceling" => Kind::MergeGroupDestroyed,
        other => {
            return Ok(Parsed::Ignored(format!(
                "a merge request's pipeline that is {other:?}: it is over, or waits on a person"
            )))
        }
    };
    let made = event(
        &p.project,
        NewEvent {
            kind,
            git_ref: format!("refs/merge-requests/{}/train", mr.iid),
            sha: at.sha,
            commit: None,
            base_sha: None,
            base_ref: mr.target_branch.filter(|b| !b.is_empty()),
            pr: None,
            actor: p.user.map(|u| u.username),
            trust: crate::event::Trust::Trusted,
            association: None,
            head_ref: None,
        },
    )?;
    Ok(match made {
        Parsed::Event {
            forge_host,
            full_name,
            event,
        } => Parsed::Train(Train {
            forge_host,
            full_name,
            pipeline: pipeline.to_string(),
            event,
        }),
        other => other,
    })
}

#[derive(Deserialize)]
struct JobPayload {
    build_id: u64,
    build_status: String,
    #[serde(default)]
    build_name: Option<String>,
    #[serde(default)]
    build_stage: Option<String>,
    pipeline_id: u64,
    sha: String,
    #[serde(default)]
    runner: Option<JobRunner>,
    project: Project,
}

#[derive(Deserialize)]
struct JobRunner {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    description: Option<String>,
}

/// A job's delivery: what became of it, as fleet mode's job, which is a
/// workflow job's shape (a pipeline is a workflow run). It names no tag:
/// its `labels` are empty, and the look asks GitLab for them. A job that is
/// not asking for a runner and may never (`manual`, `scheduled`) is dropped.
fn parse_job(body: &[u8]) -> Result<Parsed, ParseError> {
    let p: JobPayload = read("Job Hook", body)?;
    let Some((action, conclusion)) = crate::forge::gitlab::job_action(&p.build_status) else {
        return Ok(Parsed::Ignored(format!(
            "a job that is {}: one is taken when it waits for a runner",
            p.build_status
        )));
    };
    if !is_commit_id(&p.sha) {
        return Err(ParseError(format!("{:?} is not a full commit id", p.sha)));
    }
    let full_name = &p.project.path_with_namespace;
    if full_name.is_empty() || !is_storable_ref(full_name) {
        return Err(ParseError(
            "the project has no usable path_with_namespace".into(),
        ));
    }
    let Some(forge_host) = host_of(p.project.web_url.as_deref()) else {
        return Err(ParseError(
            "the project names no web_url to tell its host by".into(),
        ));
    };
    let kept = crate::forge::gitlab::kept;
    let (runner_id, runner_name) = match p.runner {
        Some(runner) => (runner.id, runner.description.map(kept)),
        None => (None, None),
    };
    Ok(Parsed::Job(crate::github::Job {
        forge_host,
        full_name: full_name.to_ascii_lowercase(),
        action,
        id: p.build_id,
        run_id: p.pipeline_id,
        run_attempt: 1,
        name: kept(p.build_name.unwrap_or_default()),
        workflow: p.build_stage.map(kept).filter(|s| !s.is_empty()),
        labels: Vec::new(),
        head_sha: p.sha,
        runner_id,
        runner_name: runner_name.filter(|n| !n.is_empty()),
        conclusion: conclusion.map(str::to_string),
    }))
}

/// The event, for the project the payload names, once what is stored of it
/// is known to be storable.
fn event(project: &Project, event: NewEvent) -> Result<Parsed, ParseError> {
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
            "the target branch is empty, too long, or not printable".into(),
        ));
    }
    for sha in std::iter::once(&event.sha).chain(event.base_sha.iter()) {
        if !is_commit_id(sha) {
            return Err(ParseError(format!("{sha:?} is not a full commit id")));
        }
    }
    let full_name = &project.path_with_namespace;
    if full_name.is_empty() || !is_storable_ref(full_name) {
        return Err(ParseError(
            "the project has no usable path_with_namespace".into(),
        ));
    }
    let Some(forge_host) = host_of(project.web_url.as_deref()) else {
        return Err(ParseError(
            "the project names no web_url to tell its host by".into(),
        ));
    };
    Ok(Parsed::Event {
        forge_host,
        full_name: full_name.to_ascii_lowercase(),
        event,
    })
}

/// The host of the project's web URL, lowercased: GitLab is self-hosted as
/// often as not, so no host is assumed.
fn host_of(web_url: Option<&str>) -> Option<String> {
    web_url
        .and_then(|u| u.split_once("://").map(|(_, rest)| rest))
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .map(|authority| authority.rsplit('@').next().unwrap_or(authority))
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Trust;

    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const ZERO: &str = "0000000000000000000000000000000000000000";

    fn project() -> serde_json::Value {
        serde_json::json!({
            "id": 7,
            "web_url": "https://GitLab.Example/Group/Sub/App",
            "path_with_namespace": "Group/Sub/App",
        })
    }

    fn parsed(event_type: &str, body: serde_json::Value) -> (String, String, NewEvent) {
        match parse(event_type, body.to_string().as_bytes()).unwrap() {
            Parsed::Event {
                forge_host,
                full_name,
                event,
            } => (forge_host, full_name, event),
            Parsed::Ignored(why) => panic!("ignored: {why}"),
            _ => panic!("not an event"),
        }
    }

    fn ignored(event_type: &str, body: serde_json::Value) -> String {
        match parse(event_type, body.to_string().as_bytes()).unwrap() {
            Parsed::Ignored(why) => why,
            _ => panic!("not ignored"),
        }
    }

    #[test]
    fn the_token_is_the_secret_and_nothing_else() {
        assert!(token_matches(b"s3cret", Some("s3cret")));
        assert!(!token_matches(b"s3cret", Some("s3cre")));
        assert!(!token_matches(b"s3cret", Some("s3cret ")));
        assert!(!token_matches(b"s3cret", Some("")));
        assert!(!token_matches(b"s3cret", None));
    }

    #[test]
    fn a_push_is_the_branch_s_with_the_commit_it_was_at() {
        let (host, name, ev) = parsed(
            "Push Hook",
            serde_json::json!({
                "object_kind": "push", "ref": "refs/heads/main",
                "before": A, "after": B, "checkout_sha": B,
                "user_username": "dev", "project": project(),
            }),
        );
        assert_eq!(
            (host.as_str(), name.as_str()),
            ("gitlab.example", "group/sub/app")
        );
        assert_eq!(ev.kind, Kind::Push);
        assert_eq!((ev.sha.as_str(), ev.base_sha.as_deref()), (B, Some(A)));
        assert_eq!(ev.trust, Trust::Trusted);
        assert_eq!(ev.actor.as_deref(), Some("dev"));

        let (_, _, new) = parsed(
            "Push Hook",
            serde_json::json!({
                "ref": "refs/heads/new", "before": ZERO, "after": B,
                "project": project(),
            }),
        );
        assert_eq!(new.base_sha, None, "a new branch has no earlier commit");

        let (_, _, gone) = parsed(
            "Push Hook",
            serde_json::json!({
                "ref": "refs/heads/old", "before": A, "after": ZERO,
                "checkout_sha": null, "project": project(),
            }),
        );
        assert_eq!((gone.kind, gone.sha.as_str()), (Kind::Delete, A));
    }

    #[test]
    fn an_annotated_tag_keeps_its_own_id_and_names_its_commit() {
        let (_, _, ev) = parsed(
            "Tag Push Hook",
            serde_json::json!({
                "object_kind": "tag_push", "ref": "refs/tags/v1",
                "before": ZERO, "after": C, "checkout_sha": B,
                "project": project(),
            }),
        );
        assert_eq!(ev.kind, Kind::Tag);
        assert_eq!((ev.sha.as_str(), ev.commit.as_deref()), (C, Some(B)));
    }

    fn merge_request(action: &str, oldrev: Option<&str>, source: i64) -> serde_json::Value {
        serde_json::json!({
            "object_kind": "merge_request",
            "user": {"username": "dev"},
            "project": project(),
            "object_attributes": {
                "iid": 12, "action": action, "oldrev": oldrev,
                "source_branch": "feature/x", "target_branch": "main",
                "source_project_id": source, "target_project_id": 7,
                "last_commit": {"id": B},
            },
        })
    }

    #[test]
    fn a_merge_request_is_a_pull_request_s_event_at_its_own_ref() {
        for (action, oldrev) in [("open", None), ("reopen", None), ("update", Some(A))] {
            let (_, _, ev) = parsed("Merge Request Hook", merge_request(action, oldrev, 7));
            assert_eq!(ev.kind, Kind::PullRequest, "{action}");
            assert_eq!(ev.git_ref, "refs/merge-requests/12/head");
            assert_eq!((ev.sha.as_str(), ev.pr), (B, Some(12)));
            assert_eq!(ev.base_ref.as_deref(), Some("main"));
            assert_eq!(ev.head_ref.as_deref(), Some("feature/x"));
            assert_eq!(ev.trust, Trust::Trusted, "the project's own branch");
            assert_eq!(ev.association, None);
        }
        for action in ["close", "merge"] {
            let (_, _, ev) = parsed("Merge Request Hook", merge_request(action, None, 7));
            assert_eq!(ev.kind, Kind::PullRequestClosed, "{action}");
        }
    }

    #[test]
    fn a_fork_s_merge_request_is_untrusted() {
        let (_, _, ev) = parsed("Merge Request Hook", merge_request("open", None, 99));
        assert_eq!(ev.trust, Trust::Untrusted);
    }

    #[test]
    fn what_brings_no_new_commit_is_dropped() {
        ignored("Merge Request Hook", merge_request("update", None, 7));
        ignored("Merge Request Hook", merge_request("approved", None, 7));
        ignored("Note Hook", serde_json::json!({}));
        ignored(
            "Push Hook",
            serde_json::json!({
                "ref": "refs/notes/x", "before": A, "after": B, "project": project(),
            }),
        );
    }

    #[test]
    fn a_job_is_read_as_it_waits_is_taken_and_ends() {
        let delivery = |status: &str, runner: serde_json::Value| {
            serde_json::json!({
                "object_kind": "build", "build_id": 501, "build_status": status,
                "build_name": "test", "build_stage": "test", "pipeline_id": 77, "sha": A,
                "runner": runner, "project": project(),
            })
        };
        let job = |status: &str, runner: serde_json::Value| match parse(
            "Job Hook",
            delivery(status, runner).to_string().as_bytes(),
        )
        .unwrap()
        {
            Parsed::Job(job) => job,
            other => panic!("{other:?}"),
        };
        let waits = job("pending", serde_json::Value::Null);
        assert_eq!(
            (
                waits.action,
                waits.id,
                waits.run_id,
                waits.full_name.as_str()
            ),
            (crate::github::JobAction::Queued, 501, 77, "group/sub/app")
        );
        assert!(waits.labels.is_empty(), "a job's delivery names no tag");
        let runner = serde_json::json!({ "id": 9, "description": "dagron-0a1b2c3d4e5f" });
        let taken = job("running", runner.clone());
        assert_eq!(
            (taken.action, taken.runner_name.as_deref()),
            (
                crate::github::JobAction::InProgress,
                Some("dagron-0a1b2c3d4e5f")
            )
        );
        let failed = job("failed", runner);
        assert_eq!(
            (failed.action, failed.conclusion.as_deref()),
            (crate::github::JobAction::Completed, Some("failure"))
        );
        assert!(matches!(
            parse(
                "Job Hook",
                delivery("manual", serde_json::Value::Null)
                    .to_string()
                    .as_bytes()
            ),
            Ok(Parsed::Ignored(_))
        ));
        let mut short = delivery("pending", serde_json::Value::Null);
        short["sha"] = serde_json::json!("abc");
        assert!(parse("Job Hook", short.to_string().as_bytes()).is_err());
    }

    /// A merge request's pipeline as GitLab tells of it: its `ref` is the
    /// request's source branch, whichever of the request's pipelines it is.
    fn pipeline(status: &str, iid: i64) -> serde_json::Value {
        serde_json::json!({
            "object_kind": "pipeline",
            "object_attributes": {
                "id": 31, "iid": 4, "ref": "feature-x", "sha": A, "before_sha": ZERO,
                "status": status, "source": "merge_request_event",
            },
            "merge_request": {
                "id": 900, "iid": iid, "source_branch": "feature-x", "target_branch": "main",
            },
            "user": { "name": "Mae", "username": "mae" },
            "project": project(),
        })
    }

    fn train(body: serde_json::Value) -> Train {
        match parse("Pipeline Hook", body.to_string().as_bytes()).unwrap() {
            Parsed::Train(train) => train,
            other => panic!("not a train: {other:?}"),
        }
    }

    #[test]
    fn a_merge_request_s_pipeline_is_what_may_be_its_merge_group() {
        assert_eq!(train_of("refs/merge-requests/12/train"), Some(12));
        for not in [
            "refs/merge-requests/12/head",
            "refs/merge-requests/12/merge",
            "refs/merge-requests//train",
            "refs/merge-requests/1a/train",
            "refs/heads/train",
        ] {
            assert_eq!(train_of(not), None, "{not}");
        }

        // GitLab is asked by the pipeline's id whether it is at the train's
        // ref: the hook's own ref is the source branch.
        let train_ref = "refs/merge-requests/12/train";
        for status in ["created", "pending", "running"] {
            let Train {
                forge_host,
                full_name,
                pipeline,
                event: ev,
            } = train(pipeline(status, 12));
            assert_eq!(
                (forge_host.as_str(), full_name.as_str(), pipeline.as_str()),
                ("gitlab.example", "group/sub/app", "31")
            );
            assert_eq!(ev.kind, Kind::MergeGroup, "{status}");
            assert_eq!(
                (ev.git_ref.as_str(), ev.sha.as_str(), ev.base_ref.as_deref()),
                (train_ref, A, Some("main"))
            );
            assert_eq!(
                ev.trust,
                Trust::Trusted,
                "only one who may merge puts it on"
            );
            assert_eq!((ev.actor.as_deref(), ev.pr), (Some("mae"), None));
        }
        for status in ["canceled", "canceling"] {
            assert_eq!(
                train(pipeline(status, 12)).event.kind,
                Kind::MergeGroupDestroyed,
                "{status}"
            );
        }
        for status in ["success", "failed", "skipped", "manual"] {
            let why = ignored("Pipeline Hook", pipeline(status, 12));
            assert!(why.contains("it is over"), "{status}: {why}");
        }
        // A branch's or a tag's pipeline is said by its push's hook.
        let mut of_a_push = pipeline("pending", 12);
        of_a_push["merge_request"] = serde_json::Value::Null;
        of_a_push["object_attributes"]["ref"] = serde_json::json!("refs/merge-requests/12/train");
        let why = ignored("Pipeline Hook", of_a_push);
        assert!(why.contains("no merge request"), "{why}");
        // However little else it carries: such a delivery is acknowledged,
        // not refused for a field only a train's pipeline needs.
        for bare in [
            serde_json::json!({"object_kind": "pipeline"}),
            serde_json::json!({"object_kind": "pipeline", "merge_request": null}),
            serde_json::json!({"object_kind": "pipeline", "object_attributes": {"status": "running"}}),
        ] {
            let why = ignored("Pipeline Hook", bare.clone());
            assert!(why.contains("no merge request"), "{bare}: {why}");
        }
        assert!(parse("Pipeline Hook", b"not json").is_err());
        for broken in [
            ("/object_attributes/id", serde_json::Value::Null),
            ("/merge_request/iid", serde_json::json!(0)),
            ("/object_attributes/sha", serde_json::json!("abc")),
        ] {
            let mut body = pipeline("pending", 12);
            *body.pointer_mut(broken.0).unwrap() = broken.1;
            assert!(
                parse("Pipeline Hook", body.to_string().as_bytes()).is_err(),
                "{}",
                broken.0
            );
        }
    }

    #[test]
    fn a_payload_without_a_host_or_a_commit_is_refused() {
        let mut body = merge_request("open", None, 7);
        body["project"]["web_url"] = serde_json::Value::Null;
        assert!(parse("Merge Request Hook", body.to_string().as_bytes()).is_err());
        let mut body = merge_request("open", None, 7);
        body["object_attributes"]["last_commit"] = serde_json::json!({"id": "main"});
        assert!(parse("Merge Request Hook", body.to_string().as_bytes()).is_err());
        assert!(parse("Push Hook", b"not json").is_err());
    }
}
