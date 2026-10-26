//! Pull requests from forks and from non-members: whether this dagron-ci
//! runs them, and the approval that stands in front of one.
//!
//! Such a pull request's event is `untrusted` ([`crate::event::Trust`]): its
//! pipelines are its author's. By default it is refused, and its head commit
//! says so. With `CI_FORK_PRS=untrusted` it runs, and what holds it is not
//! here: the run is recorded as `untrusted`, and dagron confines a run of
//! that class (no secret an environment's policy does not open to it, none
//! of the engine's own, nothing the engine would do on a workflow's word,
//! and the engine's floor for such runs, on an engine that has one).
//!
//! What is here is the approval. A pull request that needs one
//! ([`Forks::gated`]) has no pipeline of it submitted until a person this
//! installation names has said so. The approval is a run of its own, made of
//! one `type: approval` task that dagron-ci writes ([`Forks::gate_workflow`]):
//! nothing in it is the pull request's but its number and its commit, which
//! its run's record carries too. The pull request's pipelines are submitted
//! once that run has succeeded, and never if it has not.
//!
//! It is a run of its own, and not a task in front of the pipeline's, for
//! what a rejected task leaves: dagron skips the tasks that needed it to
//! succeed, and runs the ones a pipeline wrote to follow a failure
//! (`trigger_rule: one_failed`, `all_done`, a `hook`). The pipeline is the
//! pull request's to write.

use crate::db::Claimed;

/// The name the approval's run is keyed under, where a pipeline's path is:
/// no pipeline file can be called this.
pub const GATE_PIPELINE: &str = "(approval)";

/// The context of the status that says where a pull request's approval
/// stands. It names no pipeline's job, so no run's status writes over it.
pub const GATE_CONTEXT: &str = "dagron-ci/approval (pull_request)";

/// The approval's workflow, and its one task.
pub const GATE_WORKFLOW: &str = "dagron-ci-approval";
pub const GATE_TASK: &str = "approve";

/// How long an approval waits before it is taken as refused: a week.
pub const DEFAULT_APPROVAL_TIMEOUT_SECS: u64 = 7 * 24 * 3600;

/// Which untrusted pull requests wait for an approval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Approval {
    /// Those whose author has, for all the forge says, no commit in the
    /// repository and no place on it ([`crate::event::first_contribution`]).
    FirstTime,
    /// Every one.
    Always,
    /// None.
    Never,
}

/// What this dagron-ci does with a pull request from a fork or a non-member,
/// where it runs them (`CI_FORK_PRS=untrusted`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forks {
    pub approval: Approval,
    /// Who may approve, as dagron's `approvers:` takes them: an email, a
    /// subject, or `group:<name>`. Never empty where an approval is asked:
    /// an approval with no approvers is one anybody signed in may give.
    pub approvers: Vec<String>,
    pub approval_timeout_secs: u64,
}

impl Forks {
    /// Read `CI_FORK_PRS` and what goes with it. `None` is `refuse`, the
    /// default: such a pull request is not run.
    pub fn from_lookup(get: &impl Fn(&str) -> Option<String>) -> anyhow::Result<Option<Forks>> {
        let set = |name: &str| {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let runs = match set("CI_FORK_PRS").as_deref() {
            None | Some("refuse") => false,
            Some("untrusted") => true,
            Some(other) => anyhow::bail!("CI_FORK_PRS={other:?} must be refuse or untrusted"),
        };
        let approval = match set("CI_FORK_APPROVAL").as_deref() {
            None | Some("first-time") => Approval::FirstTime,
            Some("always") => Approval::Always,
            Some("never") => Approval::Never,
            Some(other) => {
                anyhow::bail!("CI_FORK_APPROVAL={other:?} must be first-time, always or never")
            }
        };
        let approvers: Vec<String> = set("CI_FORK_APPROVERS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect();
        for entry in &approvers {
            approver(entry).map_err(|why| anyhow::anyhow!("CI_FORK_APPROVERS: {why}"))?;
        }
        if approvers.len() > APPROVERS_MAX {
            anyhow::bail!("CI_FORK_APPROVERS names more than {APPROVERS_MAX} approvers");
        }
        let approval_timeout_secs = match set("CI_FORK_APPROVAL_TIMEOUT_SECS") {
            None => DEFAULT_APPROVAL_TIMEOUT_SECS,
            Some(raw) => raw
                .parse::<u64>()
                .ok()
                .filter(|secs| (60..=90 * 24 * 3600).contains(secs))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "CI_FORK_APPROVAL_TIMEOUT_SECS={raw:?} must be a number of seconds, \
                         from a minute to ninety days"
                    )
                })?,
        };
        if !runs {
            return Ok(None);
        }
        if approval != Approval::Never && approvers.is_empty() {
            anyhow::bail!(
                "CI_FORK_PRS=untrusted asks for an approval (CI_FORK_APPROVAL), and \
                 CI_FORK_APPROVERS names nobody who may give one: an approval with no \
                 approvers is one anybody signed in may give. Name them (an email, or \
                 group:<name>), or set CI_FORK_APPROVAL=never"
            );
        }
        Ok(Some(Forks {
            approval,
            approvers,
            approval_timeout_secs,
        }))
    }

    /// Whether an untrusted pull request whose author the forge says this of
    /// waits for an approval.
    pub fn gated(&self, association: Option<&str>) -> bool {
        match self.approval {
            Approval::Always => true,
            Approval::Never => false,
            Approval::FirstTime => crate::event::first_contribution(association),
        }
    }

    /// The approval's workflow for the event: one task, which waits for one
    /// of [`Forks::approvers`] and is taken as refused when nobody answers.
    ///
    /// It is made of the pull request's number and its commit, and of this
    /// setting, and of nothing else of the event. So it is the same workflow
    /// whichever door the event came through, and one commit has one
    /// approval: a delivery names whoever pushed, a sweep the pull request's
    /// author, and the two may not be one person. Nothing in it is text the
    /// pull request's author chose.
    pub fn gate_workflow(&self, ev: &Claimed) -> String {
        let number = ev.pr.map(|n| format!(" #{n}")).unwrap_or_default();
        let message = format!(
            "Pull request{number} of {repo} comes from outside the repository. Approving \
             runs its pipelines at {sha}, as its author wrote them, as an untrusted run. \
             Read what it changes first.",
            repo = ev.repo.full_name,
            sha = &ev.sha[..ev.sha.len().min(12)],
        );
        let workflow = serde_json::json!({
            "name": GATE_WORKFLOW,
            "tags": ["dagron-ci"],
            "tasks": [{
                "name": GATE_TASK,
                "type": "approval",
                "approvers": self.approvers,
                "approval_message": message,
                "approval_timeout_secs": self.approval_timeout_secs,
                "approval_on_timeout": "reject",
            }],
        });
        serde_yaml::to_string(&workflow).unwrap_or_default()
    }
}

/// Most approvers an approval may name, as dagron has it.
const APPROVERS_MAX: usize = 64;

/// One approver, by dagron's rule for an `approvers:` entry: a non-empty
/// identity or `group:<name>`, with no whitespace or control characters.
fn approver(entry: &str) -> Result<(), String> {
    let name = entry.strip_prefix("group:").unwrap_or(entry);
    if name.is_empty()
        || entry.chars().count() > 254
        || entry.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(format!(
            "{entry:?} is not an approver: an email or a subject, or group:<name>"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};

    fn forks(vars: &[(&str, &str)]) -> anyhow::Result<Option<Forks>> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Forks::from_lookup(&|name: &str| vars.get(name).cloned())
    }

    fn pull(actor: &str, association: Option<&str>) -> Claimed {
        Claimed {
            id: "e-1".into(),
            repo: crate::db::Repo {
                full_name: "acme/widgets".into(),
                ..Default::default()
            },
            kind: "pull_request".into(),
            git_ref: "refs/pull/8/head".into(),
            sha: "a1b2c3d4e5f6".repeat(4)[..40].to_string(),
            base_sha: None,
            base_ref: Some("main".into()),
            pr: Some(8),
            actor: Some(actor.into()),
            trust: "untrusted".into(),
            association: association.map(str::to_string),
            head_ref: None,
            commit_key: "k".into(),
            attempts: 1,
            inputs: BTreeMap::new(),
            pipeline: None,
            bundle_id: None,
            patch: None,
        }
    }

    #[test]
    fn fork_pull_requests_are_refused_until_an_operator_says_otherwise() {
        assert_eq!(forks(&[]).unwrap(), None);
        assert_eq!(forks(&[("CI_FORK_PRS", "refuse")]).unwrap(), None);
        assert_eq!(forks(&[("CI_FORK_PRS", " ")]).unwrap(), None);
        for bad in ["allow", "trusted", "yes", "Untrusted"] {
            let why = forks(&[("CI_FORK_PRS", bad)]).unwrap_err().to_string();
            assert!(why.contains("must be refuse or untrusted"), "{why}");
        }
        // What goes with it is checked whether or not they run: a mistake in
        // it is found when it is written, not when the setting is turned on.
        assert!(forks(&[("CI_FORK_APPROVAL", "sometimes")]).is_err());
        assert!(forks(&[("CI_FORK_APPROVERS", "group:")]).is_err());
        assert!(forks(&[("CI_FORK_APPROVAL_TIMEOUT_SECS", "soon")]).is_err());
        // And approvers named for a setting that is off turn nothing on.
        assert_eq!(
            forks(&[("CI_FORK_APPROVERS", "group:maintainers")]).unwrap(),
            None
        );
    }

    #[test]
    fn running_them_takes_approvers_unless_no_approval_is_asked() {
        let why = forks(&[("CI_FORK_PRS", "untrusted")])
            .unwrap_err()
            .to_string();
        assert!(
            why.contains("CI_FORK_APPROVERS names nobody")
                && why.contains("CI_FORK_APPROVAL=never"),
            "{why}"
        );
        assert!(forks(&[("CI_FORK_PRS", "untrusted"), ("CI_FORK_APPROVAL", "always")]).is_err());

        let named = forks(&[
            ("CI_FORK_PRS", "untrusted"),
            (
                "CI_FORK_APPROVERS",
                " group:maintainers, alice@example.com ,",
            ),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(named.approval, Approval::FirstTime);
        assert_eq!(named.approvers, ["group:maintainers", "alice@example.com"]);
        assert_eq!(named.approval_timeout_secs, 7 * 24 * 3600);

        let none = forks(&[("CI_FORK_PRS", "untrusted"), ("CI_FORK_APPROVAL", "never")])
            .unwrap()
            .unwrap();
        assert_eq!((none.approval, none.approvers.len()), (Approval::Never, 0));

        let all = forks(&[
            ("CI_FORK_PRS", "untrusted"),
            ("CI_FORK_APPROVAL", "always"),
            ("CI_FORK_APPROVERS", "group:maintainers"),
            ("CI_FORK_APPROVAL_TIMEOUT_SECS", "3600"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            (all.approval, all.approval_timeout_secs),
            (Approval::Always, 3600)
        );
        for bad in ["0", "59", "7776001", "-1", "1h"] {
            assert!(
                forks(&[("CI_FORK_APPROVAL_TIMEOUT_SECS", bad)]).is_err(),
                "{bad}"
            );
        }
        for bad in ["a b", "group:", "group: x", "x\ty", ""] {
            if bad.is_empty() {
                continue;
            }
            assert!(forks(&[("CI_FORK_APPROVERS", bad)]).is_err(), "{bad:?}");
        }
        let many = vec!["a@example.com"; 65].join(",");
        assert!(forks(&[("CI_FORK_APPROVERS", &many)]).is_err());
    }

    /// An approver here is one dagron takes, and one dagron refuses is
    /// refused here: the approval's run would not be made otherwise.
    #[test]
    fn an_approver_is_one_dagron_takes() {
        for entry in [
            "alice@example.com",
            "group:maintainers",
            "auth0|abc123",
            "group:",
            "",
            "a b",
            "group:a\nb",
            &"x".repeat(254),
            &"x".repeat(255),
        ] {
            assert_eq!(
                approver(entry).is_ok(),
                dagron_core::dag::validate_approver(entry).is_ok(),
                "{entry:?}"
            );
        }
        assert_eq!(APPROVERS_MAX, dagron_core::dag::APPROVERS_MAX);
    }

    #[test]
    fn which_pull_requests_wait_is_the_setting_s_and_the_forge_s_word() {
        let with = |approval| Forks {
            approval,
            approvers: vec!["group:maintainers".into()],
            approval_timeout_secs: 3600,
        };
        for said in [
            None,
            Some("NONE"),
            Some("FIRST_TIMER"),
            Some("FIRST_TIME_CONTRIBUTOR"),
        ] {
            assert!(with(Approval::FirstTime).gated(said), "{said:?}");
        }
        for said in [
            Some("CONTRIBUTOR"),
            Some("MEMBER"),
            Some("COLLABORATOR"),
            Some("OWNER"),
        ] {
            assert!(!with(Approval::FirstTime).gated(said), "{said:?}");
            assert!(with(Approval::Always).gated(said));
            assert!(!with(Approval::Never).gated(said));
        }
        assert!(with(Approval::Always).gated(None));
        assert!(!with(Approval::Never).gated(None));
    }

    /// The approval's workflow is one dagron runs: an approval, with the
    /// approvers named, refused when nobody answers. It is the same for a
    /// commit whichever door its event came through, and nothing in it is
    /// the pull request's to write.
    #[test]
    fn the_approval_is_a_workflow_of_one_task_that_dagron_takes() {
        let forks = Forks {
            approval: Approval::Always,
            approvers: vec!["group:maintainers".into(), "alice@example.com".into()],
            approval_timeout_secs: 3600,
        };
        let yaml = forks.gate_workflow(&pull("mallory", Some("FIRST_TIME_CONTRIBUTOR")));
        let dag =
            dagron_core::dag::DagGraph::from_yaml(&yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"));
        assert_eq!(dag.spec.name, GATE_WORKFLOW);
        assert_eq!(dag.spec.tasks.len(), 1);
        let task = dag.task_spec(GATE_TASK).unwrap();
        assert!(task.is_approval());
        assert_eq!(task.approvers, forks.approvers);
        assert_eq!(
            (
                task.approval_timeout_secs,
                task.approval_on_timeout.as_deref()
            ),
            (Some(3600), Some("reject"))
        );
        assert_eq!(
            task.approval_message.as_deref(),
            Some(
                "Pull request #8 of acme/widgets comes from outside the repository. \
                 Approving runs its pipelines at a1b2c3d4e5f6, as its author wrote them, \
                 as an untrusted run. Read what it changes first."
            )
        );

        // One commit, one approval. A delivery names whoever pushed and a
        // sweep the author, the forge may say something else of the author
        // by then, and a name can be anything: none of it is in the workflow.
        let hostile =
            "x\"\napprovers: []\n  - name: run\n    command: [\"sh\"]\n{{ secrets }} `id` <b>";
        for (actor, association) in [
            ("a-maintainer", Some("FIRST_TIME_CONTRIBUTOR")),
            ("mallory", Some("CONTRIBUTOR")),
            ("mallory", None),
            (hostile, None),
        ] {
            assert_eq!(
                forks.gate_workflow(&pull(actor, association)),
                yaml,
                "{actor:?} {association:?}"
            );
        }
        // Another commit, or another setting, is another approval.
        let mut later = pull("mallory", None);
        later.sha = "f".repeat(40);
        assert_ne!(forks.gate_workflow(&later), yaml);
        let others = Forks {
            approvers: vec!["group:security".into()],
            ..forks.clone()
        };
        assert_ne!(others.gate_workflow(&pull("mallory", None)), yaml);
    }
}
