//! What a forge event becomes: one row in `ci_events`, whichever door it came
//! through. Nothing here knows which forge sent it.

/// Who may run what, decided once from the verified event and recorded with
/// it. Opening a pull request must not be the same as authoring a workflow, so
/// a fork's pull request, or one opened by a non-member, is `Untrusted`. It is
/// refused, or, where the operator has such run, run as an untrusted run,
/// which dagron holds by its record ([`crate::forks`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trust {
    /// A push or tag by a writer, a merge-queue group, a dispatch by a writer.
    Trusted,
    /// A pull request from a fork, or from someone who is not a member.
    Untrusted,
    /// A run whose actor is an agent. Only the API door can produce it.
    Agent,
}

impl Trust {
    pub fn as_str(self) -> &'static str {
        match self {
            Trust::Trusted => "trusted",
            Trust::Untrusted => "untrusted",
            Trust::Agent => "agent",
        }
    }
}

/// The events that may lead to a run, or cancel one. Each forge maps onto
/// these; anything else is acknowledged and dropped at the door.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A push to a branch.
    Push,
    /// A tag pushed or moved.
    Tag,
    /// A branch or tag deleted. `sha` is the commit it last pointed at.
    Delete,
    /// A pull request opened, updated or reopened.
    PullRequest,
    /// A pull request closed or merged.
    PullRequestClosed,
    /// A merge-queue group asking for checks: GitHub's merge group, or a
    /// GitLab merge train's pipeline.
    MergeGroup,
    /// A merge-queue group the queue let go of without merging it
    /// (invalidated, or dequeued; a train's pipeline cancelled): its runs
    /// still going are cancelled.
    MergeGroupDestroyed,
    /// A run a caller asked for through the run API, at a ref. Not a commit
    /// event: two dispatches of one commit are two runs, unless they share
    /// an `Idempotency-Key`.
    Dispatch,
    /// A workflow's `on.schedule` cron firing, at the head of the default
    /// branch: one workflow file's, named as the event's pipeline, with the
    /// cron as its `schedule` input ([`crate::schedule`]).
    Schedule,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Push => "push",
            Kind::Tag => "tag",
            Kind::Delete => "delete",
            Kind::PullRequest => "pull_request",
            Kind::PullRequestClosed => "pull_request_closed",
            Kind::MergeGroup => "merge_group",
            Kind::MergeGroupDestroyed => "merge_group_destroyed",
            Kind::Dispatch => "dispatch",
            Kind::Schedule => "schedule",
        }
    }
}

/// An event as the receivers hand it to the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewEvent {
    pub kind: Kind,
    /// The ref the event is about: `refs/heads/…`, `refs/tags/…`, or a pull
    /// request's `refs/pull/{n}/head`.
    pub git_ref: String,
    /// The id the event came with: a commit's, or an annotated tag's own.
    pub sha: String,
    /// The commit `sha` names, when `sha` is an annotated tag's own id: what
    /// runs, is checked out and is reported on. Each door fills it from what
    /// it was shown (a delivery's `head_commit`, a sweep's peeled listing).
    /// `None` when `sha` is the commit. It is no part of the key.
    pub commit: Option<String>,
    /// The commit the event is measured against: a pull request's or merge
    /// group's base, or the commit a pushed branch pointed at before (none
    /// for a new branch). A paths filter compares the event's commit with it.
    pub base_sha: Option<String>,
    /// The branch a pull request or merge group merges into, `main` or
    /// `refs/heads/main` as the forge names it.
    pub base_ref: Option<String>,
    pub pr: Option<i64>,
    /// The forge account that caused the event, for the record only: it never
    /// decides trust on its own and never reaches a shell.
    pub actor: Option<String>,
    pub trust: Trust,
    /// What the forge says a pull request's author is to the repository, as
    /// it said it ([`association`]). `None` for an event that is no pull
    /// request's, and where the forge said nothing it is known to say. The
    /// event's trust was decided with it where the event was read; kept, it
    /// says whether an untrusted pull request is its author's first
    /// ([`first_contribution`]). It is no part of the key.
    pub association: Option<String>,
    /// A pull request's head branch, as the forge names it (`feature/x`):
    /// what a workflow reads as `github.head_ref`. The pull request's
    /// author chose it, so it is never pasted into a shell. `None` for any
    /// other event. It is no part of the key.
    pub head_ref: Option<String>,
}

/// What a forge may say a pull request's author is to its repository, in
/// GitHub's words.
pub const ASSOCIATIONS: &[&str] = &[
    "OWNER",
    "MEMBER",
    "COLLABORATOR",
    "CONTRIBUTOR",
    "FIRST_TIME_CONTRIBUTOR",
    "FIRST_TIMER",
    "MANNEQUIN",
    "NONE",
];

/// The association a forge `said`, where it is one of [`ASSOCIATIONS`]: what
/// is kept on an event. Anything else is not kept, and reads as nothing said.
pub fn association(said: Option<&str>) -> Option<String> {
    said.filter(|said| ASSOCIATIONS.contains(said))
        .map(str::to_string)
}

/// Whether a pull request with this association is, for all the forge said,
/// its author's first to the repository: every answer but the ones that say
/// the author has a commit in it or a place on it. Nothing said is a first.
pub fn first_contribution(association: Option<&str>) -> bool {
    !matches!(
        association,
        Some("OWNER" | "MEMBER" | "COLLABORATOR" | "CONTRIBUTOR")
    )
}

/// The commit-level key, `ci:{repo}:{kind}:{ref}:{sha}`. A webhook and a poll
/// that later sees the same commit derive the same key, so between them they
/// produce one run. A dispatch's key also ends in its event id (see
/// [`Kind::Dispatch`]); the queue appends it.
///
/// For an annotated tag `sha` is the tag's own id, never the commit it names
/// ([`NewEvent::commit`]): a door that could not name the commit still derives
/// the key of one that could.
pub fn commit_key(repo_id: &str, event: &NewEvent) -> String {
    format!(
        "ci:{repo_id}:{}:{}:{}",
        event.kind.as_str(),
        event.git_ref,
        event.sha
    )
}

/// A full commit id: 40 hex digits (SHA-1) or 64 (SHA-256), lowercase as
/// forges send them.
pub fn is_commit_id(s: &str) -> bool {
    matches!(s.len(), 40 | 64)
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A ref this queue will store: not empty, at most 1 KiB, and free of control
/// characters. Git allows far less than that; this only keeps what reaches a
/// key or a log line printable.
pub fn is_storable_ref(s: &str) -> bool {
    !s.is_empty() && s.len() <= 1024 && !s.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push() -> NewEvent {
        NewEvent {
            kind: Kind::Push,
            git_ref: "refs/heads/main".into(),
            sha: "a".repeat(40),
            commit: None,
            base_sha: None,
            base_ref: None,
            pr: None,
            actor: Some("octocat".into()),
            trust: Trust::Trusted,
            association: None,
            head_ref: None,
        }
    }

    /// An annotated tag's key is its own id's, whether or not the door that
    /// saw it could name its commit: a webhook and a sweep make one run.
    #[test]
    fn the_key_is_the_tag_s_own_id_whatever_commit_it_names() {
        let tag = |commit: Option<String>| NewEvent {
            kind: Kind::Tag,
            git_ref: "refs/tags/v1".into(),
            commit,
            ..push()
        };
        assert_eq!(
            commit_key("r1", &tag(Some("c".repeat(40)))),
            commit_key("r1", &tag(None))
        );
        assert_eq!(
            commit_key("r1", &tag(Some("c".repeat(40)))),
            format!("ci:r1:tag:refs/tags/v1:{}", "a".repeat(40))
        );
    }

    #[test]
    fn the_commit_key_names_repo_kind_ref_and_sha() {
        assert_eq!(
            commit_key("r1", &push()),
            format!("ci:r1:push:refs/heads/main:{}", "a".repeat(40))
        );
    }

    #[test]
    fn the_key_ignores_who_sent_it_and_their_trust() {
        let mut other = push();
        other.actor = Some("someone-else".into());
        other.trust = Trust::Untrusted;
        assert_eq!(commit_key("r1", &push()), commit_key("r1", &other));
    }

    /// What is kept of the forge's word on an author is one of its own
    /// words, and an author is a first-timer until it says otherwise.
    #[test]
    fn an_author_is_a_first_timer_until_the_forge_says_otherwise() {
        for said in ASSOCIATIONS {
            assert_eq!(association(Some(said)).as_deref(), Some(*said));
        }
        for not_kept in [
            "",
            "member",
            "Contributor",
            "ADMIN",
            "OWNER\n",
            "x".repeat(4096).as_str(),
        ] {
            assert_eq!(association(Some(not_kept)), None, "{not_kept:?}");
        }
        assert_eq!(association(None), None);

        for known in ["OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR"] {
            assert!(!first_contribution(Some(known)), "{known}");
        }
        for first in [
            "FIRST_TIME_CONTRIBUTOR",
            "FIRST_TIMER",
            "NONE",
            "MANNEQUIN",
            "anything",
        ] {
            assert!(first_contribution(Some(first)), "{first}");
        }
        assert!(first_contribution(None));
        // It is no part of the key: a webhook and a sweep make one event.
        let mut other = push();
        other.association = Some("CONTRIBUTOR".into());
        assert_eq!(commit_key("r1", &push()), commit_key("r1", &other));
    }

    #[test]
    fn commit_ids_are_full_lowercase_hex() {
        assert!(is_commit_id(&"0123456789abcdef".repeat(4)[..40]));
        assert!(is_commit_id(&"f".repeat(64)));
        assert!(!is_commit_id(&"A".repeat(40)), "uppercase");
        assert!(!is_commit_id(&"a".repeat(39)), "short");
        assert!(!is_commit_id(&"g".repeat(40)), "not hex");
        assert!(!is_commit_id(""));
    }

    #[test]
    fn refs_must_be_printable_and_bounded() {
        assert!(is_storable_ref("refs/heads/feature/x"));
        assert!(!is_storable_ref(""));
        assert!(!is_storable_ref("refs/heads/a\nb"));
        assert!(!is_storable_ref(&"x".repeat(1025)));
    }
}
