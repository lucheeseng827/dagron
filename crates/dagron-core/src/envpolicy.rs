//! Who may use an environment: its policy, and the one rule that reads it.
//!
//! An environment holds the variables and the secrets a workflow names
//! (`environment: prod`). Any run that named one had it, which was right
//! while every run was written by someone who could already read the
//! environment. It stops being right once a run can come from a pull request:
//! the run's definition is then the pull request's, and naming an
//! environment would be all it took to read one.
//!
//! A policy says which runs an environment admits: by **trust class**, by
//! **ref** and by **pipeline**. An environment with no policy admits
//! `trusted` runs and no others, so a class that is not `trusted` reads no
//! environment until an operator says it may.
//!
//! The rule is [`admits`], and it is asked twice:
//! - when a run is made through a gateway or by the engine itself, so that a
//!   run which may not use the environment it names is refused before its
//!   variables are put into anything that is stored;
//! - when a task's secrets are resolved, at dispatch. This is the check that
//!   holds, whatever made the run and whenever the policy was written.
//!
//! What it is not. A person who may submit a run is `trusted`, and writes the
//! pipeline's name: against them a policy is a guard against a mistake, not a
//! boundary. The boundary it draws is against a run whose facts come from a
//! forge's verified event: its class and its ref are the CI service's word,
//! not the run's author's.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::dag::DagGraph;
use crate::models::RunTrigger;

/// A run written or asked for by someone who may: the installation's own
/// people and schedules, a push, a member's pull request.
pub const TRUSTED: &str = "trusted";
/// A run whose definition is a stranger's: a pull request from a fork, or
/// from someone who is not a member.
pub const UNTRUSTED: &str = "untrusted";
/// A run an agent asked for.
pub const AGENT: &str = "agent";
/// Every class a policy may name.
pub const TRUST_CLASSES: [&str; 3] = [TRUSTED, UNTRUSTED, AGENT];

/// How many patterns one list of a policy may hold, and how long each may be.
const MAX_PATTERNS: usize = 64;
const MAX_PATTERN_LEN: usize = 256;

/// Which runs an environment admits. A run is admitted when it passes every
/// part that is set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentPolicy {
    /// The trust classes admitted ([`TRUST_CLASSES`]). Empty admits no run:
    /// the environment is closed.
    pub allowed_trust: Vec<String>,
    /// Ref patterns ([`matches`]): `refs/heads/main`, `refs/tags/v*`. `None`
    /// is any ref, and a run for no ref at all. Set, it admits only a run
    /// that was made for a ref, and for one of these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_refs: Option<Vec<String>>,
    /// Patterns for the workflow's name. `None` is any. A name is its
    /// author's to choose, so this narrows beside the other two and is no
    /// boundary alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_pipelines: Option<Vec<String>>,
    /// The classes besides `trusted` whose tasks in the environment may hold
    /// an identity (OIDC tokens a cloud trades for credentials). `agent`
    /// alone may be named, and only where the policy admits it: a stranger's
    /// run never holds one. Empty: none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_identity: Vec<String>,
    /// A protected environment, to a run that is not trusted: who must
    /// approve such a run's use of it, `email` or `group:<name>`, before the
    /// task that names it runs. Each such task waits on a gate of its own,
    /// which the engine puts before it as the run is made. Empty: none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approvers: Vec<String>,
}

impl Default for EnvironmentPolicy {
    /// What an environment with no policy has: `trusted` runs, any ref, any
    /// pipeline.
    fn default() -> Self {
        EnvironmentPolicy {
            allowed_trust: vec![TRUSTED.to_string()],
            allowed_refs: None,
            allowed_pipelines: None,
            allowed_identity: Vec::new(),
            approvers: Vec::new(),
        }
    }
}

impl EnvironmentPolicy {
    /// Whether this is a policy a gateway may store: classes it knows, and
    /// patterns that are text. A pattern list that is set and empty is
    /// refused, since it would admit nothing while reading as "any".
    pub fn validate(&self) -> Result<(), String> {
        for class in &self.allowed_trust {
            if !TRUST_CLASSES.contains(&class.as_str()) {
                return Err(format!(
                    "allowed_trust: {class:?} is not a trust class ({})",
                    TRUST_CLASSES.join(", ")
                ));
            }
        }
        for class in &self.allowed_identity {
            if class != AGENT {
                return Err(format!(
                    "allowed_identity: {class:?} may not hold an identity: `{AGENT}` alone is \
                     named here (a trusted run holds one where its task asks, and an untrusted \
                     one never does)"
                ));
            }
            if !self.allowed_trust.contains(class) {
                return Err(format!(
                    "allowed_identity names {class:?}, which allowed_trust does not admit"
                ));
            }
        }
        if self.approvers.len() > crate::dag::APPROVERS_MAX {
            return Err(format!(
                "approvers lists more than {}",
                crate::dag::APPROVERS_MAX
            ));
        }
        for entry in &self.approvers {
            crate::dag::validate_approver(entry).map_err(|e| format!("approvers: {e}"))?;
        }
        for (field, patterns) in [
            ("allowed_refs", &self.allowed_refs),
            ("allowed_pipelines", &self.allowed_pipelines),
        ] {
            let Some(patterns) = patterns else { continue };
            if patterns.is_empty() {
                return Err(format!(
                    "{field} is an empty list, which admits nothing: leave it out to admit any, \
                     or set allowed_trust to [] to close the environment"
                ));
            }
            if patterns.len() > MAX_PATTERNS {
                return Err(format!("{field} holds more than {MAX_PATTERNS} patterns"));
            }
            for pattern in patterns {
                if pattern.is_empty()
                    || pattern.len() > MAX_PATTERN_LEN
                    || pattern.chars().any(char::is_control)
                {
                    return Err(format!(
                        "{field}: {pattern:?} is not a pattern (1 to {MAX_PATTERN_LEN} bytes, \
                         no control characters)"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Whether a task of a run of `class` may hold an identity in an environment
/// with `policy` (`None`: it has none): a `trusted` run's may; an `agent`
/// run's where the policy names `agent` in `allowed_identity`; no other's.
pub fn identity_admits(
    environment: Option<&str>,
    policy: Option<&EnvironmentPolicy>,
    class: &str,
) -> Result<(), String> {
    if class == TRUSTED {
        return Ok(());
    }
    if class != AGENT {
        return Err(format!("a run that is {class} is given no identity"));
    }
    let named = policy.is_some_and(|p| p.allowed_identity.iter().any(|c| c == class));
    match (environment, named) {
        (Some(_), true) => Ok(()),
        (None, _) => Err(
            "an agent's run is given an identity only in an environment whose policy names \
             `agent` in allowed_identity, and this task names no environment"
                .to_string(),
        ),
        (Some(env), false) => Err(format!(
            "environment '{env}' gives no identity to an agent's run: its policy does not name \
             `agent` in allowed_identity"
        )),
    }
}

/// Who must approve a run of `class`'s use of an environment with `policy`
/// before a task of it uses the environment: the policy's `approvers`, for a
/// run that is not trusted. Empty: none must.
pub fn approvers_for<'p>(policy: Option<&'p EnvironmentPolicy>, class: &str) -> &'p [String] {
    match policy {
        Some(p) if class != TRUSTED => &p.approvers,
        _ => &[],
    }
}

/// What a policy is asked about: the facts of one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFacts {
    /// The run's trust class ([`class_of`]).
    pub trust: String,
    /// The ref the run was made for, where it was made for one.
    pub git_ref: Option<String>,
    /// The workflow's name.
    pub pipeline: String,
}

impl RunFacts {
    /// The facts of a run with this trigger record, of the workflow `pipeline`.
    pub fn of(trigger: Option<&RunTrigger>, pipeline: &str) -> RunFacts {
        RunFacts {
            trust: class_of(trigger).to_string(),
            git_ref: trigger.and_then(|t| t.git_ref.clone()),
            pipeline: pipeline.to_string(),
        }
    }
}

/// The trust class of a run, from what started it.
///
/// A record says its class where a CI service wrote one. Every other run is
/// the installation's own: a person's submit, a schedule's fire, a dataset's,
/// a call on the engine's API. Those are `trusted`, and so is a run with no
/// record, made before every run had one.
///
/// A CI record that names no class is not taken for `trusted`: the service
/// that wrote it knew the event's class, and a record without it is one that
/// did not say.
pub fn class_of(trigger: Option<&RunTrigger>) -> &str {
    match trigger {
        None => TRUSTED,
        Some(t) => match t.trust.as_deref() {
            Some(class) => class,
            None if t.kind == crate::models::trigger_kind::CI => UNTRUSTED,
            None => TRUSTED,
        },
    }
}

/// The words every [`EnvironmentRefused`] is written with. A caller that has
/// only the text of one knows it by them: a CI service, reading a gateway's
/// `403`, tells a policy's answer from its own token's being refused.
pub const REFUSAL_MARK: &str = "may not be used by this run";

/// A run that may not use an environment, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentRefused {
    pub environment: String,
    pub reason: String,
}

impl std::fmt::Display for EnvironmentRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "environment '{}' {REFUSAL_MARK}: {}",
            self.environment, self.reason
        )
    }
}

impl std::error::Error for EnvironmentRefused {}

/// The environment a repository's secrets are in, whose workflow files read
/// them as `secrets.NAME`: the repository's address, `<host>/<owner>/<repo>`
/// in lower case (`github.com/acme/widgets`). Its forge's host is in it, so
/// that one of the same name on another forge is another's.
pub fn repository_environment(forge_host: &str, full_name: &str) -> String {
    format!("{forge_host}/{full_name}").to_ascii_lowercase()
}

/// Whether `name` is a repository's environment's ([`repository_environment`]):
/// a host, an owner and a repository, in lower case. A plain environment's
/// name has no `/`.
pub fn is_repository_environment(name: &str) -> bool {
    let parts: Vec<&str> = name.split('/').collect();
    let [host, owner, repo] = parts.as_slice() else {
        return false;
    };
    let host_ok = !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '-' | ':'));
    let part_ok = |p: &str| {
        !p.is_empty()
            && p != "."
            && p != ".."
            && p.chars().all(|c| {
                c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '.' | '-')
            })
    };
    name.len() <= 255 && host_ok && part_ok(owner) && part_ok(repo)
}

/// Whether a run started by `trigger` may use the environment `name`, as far
/// as whose it is goes: a repository's ([`is_repository_environment`]) is
/// that repository's runs' alone, as dagron-ci recorded them (a record only
/// a CI service may write); any other environment is its policy's to say.
pub fn repository_admits(
    name: &str,
    trigger: Option<&RunTrigger>,
) -> Result<(), EnvironmentRefused> {
    if !name.contains('/') {
        return Ok(());
    }
    let own = trigger.and_then(|t| {
        let ci = t.kind == "ci" && t.principal.starts_with("ci:");
        match (ci, t.forge_host.as_deref(), t.repo.as_deref()) {
            (true, Some(host), Some(repo)) => Some(repository_environment(host, repo)),
            _ => None,
        }
    });
    if own.as_deref() == Some(name) {
        return Ok(());
    }
    Err(EnvironmentRefused {
        environment: name.to_string(),
        reason: "it is a repository's, and this run is not one of that repository's".into(),
    })
}

/// Whether the environment `environment`, with this policy, admits a run of
/// these facts. `None` is an environment with no policy, which has the
/// default one.
pub fn admits(
    environment: &str,
    policy: Option<&EnvironmentPolicy>,
    facts: &RunFacts,
) -> Result<(), EnvironmentRefused> {
    let default;
    let policy = match policy {
        Some(p) => p,
        None => {
            default = EnvironmentPolicy::default();
            &default
        }
    };
    let refused = |reason: String| {
        Err(EnvironmentRefused {
            environment: environment.to_string(),
            reason,
        })
    };
    if !policy.allowed_trust.iter().any(|c| c == &facts.trust) {
        let admitted = match policy.allowed_trust.is_empty() {
            true => "no run".to_string(),
            false => format!("{} runs", policy.allowed_trust.join(", ")),
        };
        return refused(format!(
            "its policy admits {admitted}, and this one is {}",
            facts.trust
        ));
    }
    if let Some(patterns) = &policy.allowed_refs {
        match &facts.git_ref {
            Some(r) if patterns.iter().any(|p| matches(p, r)) => {}
            Some(r) => {
                return refused(format!(
                    "its policy admits the refs {}, and this run is for {r}",
                    patterns.join(", ")
                ))
            }
            None => {
                return refused(format!(
                    "its policy admits the refs {}, and this run was made for no ref",
                    patterns.join(", ")
                ))
            }
        }
    }
    if let Some(patterns) = &policy.allowed_pipelines {
        if !patterns.iter().any(|p| matches(p, &facts.pipeline)) {
            return refused(format!(
                "its policy admits the pipelines {}, and this one is {}",
                patterns.join(", "),
                facts.pipeline
            ));
        }
    }
    Ok(())
}

/// Every environment a workflow names: its own, and each task's.
pub fn named_by(dag: &DagGraph) -> BTreeSet<String> {
    let mut names: BTreeSet<String> = dag.spec.environment.iter().cloned().collect();
    names.extend(dag.spec.tasks.iter().filter_map(|t| t.environment.clone()));
    names
}

/// Whether `text` matches the pattern: `*` is any run of characters within a
/// segment (it stops at `/`), `**` is any run of characters, `?` is one
/// character that is not `/`, and every other character is itself. The whole
/// text must match. It is what a branch filter of a pipeline's `on:` takes,
/// without its character classes.
pub fn matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    glob(&pattern, &text)
}

fn glob(pattern: &[char], text: &[char]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some(('*', rest)) => {
            let (across, rest) = match rest.split_first() {
                Some(('*', after)) => (true, after),
                _ => (false, rest),
            };
            // The star takes nothing, then one character more at a time, for
            // as long as it may take the next one.
            let mut taken = 0;
            loop {
                if glob(rest, &text[taken..]) {
                    return true;
                }
                match text.get(taken) {
                    Some(c) if across || *c != '/' => taken += 1,
                    _ => return false,
                }
            }
        }
        Some(('?', rest)) => match text.split_first() {
            Some((c, text)) if *c != '/' => glob(rest, text),
            _ => false,
        },
        Some((p, rest)) => match text.split_first() {
            Some((c, text)) if c == p => glob(rest, text),
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(trust: &str, git_ref: Option<&str>, pipeline: &str) -> RunFacts {
        RunFacts {
            trust: trust.to_string(),
            git_ref: git_ref.map(str::to_string),
            pipeline: pipeline.to_string(),
        }
    }

    fn policy(
        trust: &[&str],
        refs: Option<&[&str]>,
        pipelines: Option<&[&str]>,
    ) -> EnvironmentPolicy {
        let list = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        EnvironmentPolicy {
            allowed_trust: list(trust),
            allowed_refs: refs.map(list),
            allowed_pipelines: pipelines.map(list),
            allowed_identity: Vec::new(),
            approvers: Vec::new(),
        }
    }

    /// An agent holds an identity where the policy says so, and only an
    /// agent; and who approves a confined run's use is the policy's word.
    #[test]
    fn an_identity_and_an_approval_are_the_policy_s_to_give() {
        let mut open = policy(&[TRUSTED, AGENT], None, None);
        assert_eq!(identity_admits(Some("cloud"), None, TRUSTED), Ok(()));
        assert!(identity_admits(Some("cloud"), Some(&open), AGENT)
            .unwrap_err()
            .contains("allowed_identity"));
        open.allowed_identity = vec![AGENT.into()];
        assert_eq!(open.validate(), Ok(()));
        assert_eq!(identity_admits(Some("cloud"), Some(&open), AGENT), Ok(()));
        assert!(identity_admits(None, Some(&open), AGENT)
            .unwrap_err()
            .contains("names no environment"));
        assert!(identity_admits(Some("cloud"), Some(&open), UNTRUSTED).is_err());

        open.allowed_identity = vec![UNTRUSTED.into()];
        assert!(open
            .validate()
            .unwrap_err()
            .contains("may not hold an identity"));
        let mut closed = policy(&[TRUSTED], None, None);
        closed.allowed_identity = vec![AGENT.into()];
        assert!(closed.validate().unwrap_err().contains("does not admit"));

        let mut gated = policy(&[TRUSTED, AGENT], None, None);
        gated.approvers = vec!["group:release".into(), "ops@acme.example".into()];
        assert_eq!(gated.validate(), Ok(()));
        assert_eq!(
            approvers_for(Some(&gated), AGENT),
            gated.approvers.as_slice()
        );
        assert!(
            approvers_for(Some(&gated), TRUSTED).is_empty(),
            "a trusted run is not asked"
        );
        assert!(approvers_for(None, AGENT).is_empty());
        gated.approvers = vec!["two words".into()];
        assert!(gated.validate().unwrap_err().starts_with("approvers:"));
        // Read from the policy's JSON, as dagron-api takes it.
        let read: EnvironmentPolicy = serde_json::from_value(serde_json::json!({
            "allowed_trust": ["trusted", "agent"],
            "allowed_identity": ["agent"],
            "approvers": ["group:release"],
        }))
        .unwrap();
        assert_eq!((read.allowed_identity.len(), read.approvers.len()), (1, 1));
    }

    /// The property the whole design rests on: an environment nobody wrote a
    /// policy for is read by `trusted` runs and by no other class.
    #[test]
    fn an_environment_with_no_policy_admits_trusted_runs_alone() {
        assert_eq!(
            admits("prod", None, &facts(TRUSTED, None, "deploy")),
            Ok(())
        );
        for class in [UNTRUSTED, AGENT, "", "Trusted", "anything"] {
            let refused = admits("prod", None, &facts(class, None, "deploy")).unwrap_err();
            assert_eq!(refused.environment, "prod");
            assert!(
                refused.reason.contains("admits trusted runs"),
                "{class}: {}",
                refused.reason
            );
        }
    }

    #[test]
    fn a_policy_admits_the_classes_it_names_and_an_empty_one_admits_none() {
        let both = policy(&[TRUSTED, UNTRUSTED], None, None);
        assert_eq!(
            admits("ci", Some(&both), &facts(UNTRUSTED, None, "p")),
            Ok(())
        );
        assert!(admits("ci", Some(&both), &facts(AGENT, None, "p")).is_err());
        let closed = policy(&[], None, None);
        let refused = admits("old", Some(&closed), &facts(TRUSTED, None, "p")).unwrap_err();
        assert!(
            refused.reason.contains("admits no run"),
            "{}",
            refused.reason
        );
    }

    #[test]
    fn refs_admit_a_run_made_for_one_of_them_and_no_run_made_for_none() {
        let p = policy(&[TRUSTED], Some(&["refs/heads/main", "refs/tags/v*"]), None);
        for r in ["refs/heads/main", "refs/tags/v1.2.0"] {
            assert_eq!(
                admits("prod", Some(&p), &facts(TRUSTED, Some(r), "p")),
                Ok(()),
                "{r}"
            );
        }
        for r in [
            "refs/heads/main2",
            "refs/pull/7/head",
            "refs/tags/v1/x",
            "refs/heads/feature/main",
        ] {
            let refused = admits("prod", Some(&p), &facts(TRUSTED, Some(r), "p")).unwrap_err();
            assert!(refused.reason.contains(r), "{}", refused.reason);
        }
        // A person's own submit, a schedule's fire: no ref, so not admitted
        // by a policy that names refs. Otherwise naming refs would hold a
        // pull request and nobody else.
        let refused = admits("prod", Some(&p), &facts(TRUSTED, None, "p")).unwrap_err();
        assert!(
            refused.reason.contains("made for no ref"),
            "{}",
            refused.reason
        );
    }

    #[test]
    fn every_part_that_is_set_has_to_pass() {
        let p = policy(
            &[TRUSTED],
            Some(&["refs/heads/main"]),
            Some(&["release", "deploy-*"]),
        );
        let main = Some("refs/heads/main");
        assert_eq!(
            admits("prod", Some(&p), &facts(TRUSTED, main, "deploy-eu")),
            Ok(())
        );
        let refused = admits("prod", Some(&p), &facts(TRUSTED, main, "ci")).unwrap_err();
        assert!(
            refused.reason.contains("and this one is ci"),
            "{}",
            refused.reason
        );
        assert!(admits("prod", Some(&p), &facts(UNTRUSTED, main, "release")).is_err());
        assert!(admits(
            "prod",
            Some(&p),
            &facts(TRUSTED, Some("refs/heads/x"), "release")
        )
        .is_err());
        assert_eq!(
            refused.to_string(),
            "environment 'prod' may not be used by this run: its policy admits the pipelines \
             release, deploy-*, and this one is ci"
        );
    }

    #[test]
    fn a_run_s_class_is_its_record_s_and_a_ci_record_that_names_none_is_not_trusted() {
        assert_eq!(class_of(None), TRUSTED);
        let mut t = RunTrigger::engine(crate::models::trigger_kind::SCHEDULE, "schedule:nightly");
        assert_eq!(class_of(Some(&t)), TRUSTED);
        t.kind = crate::models::trigger_kind::CI.to_string();
        assert_eq!(class_of(Some(&t)), UNTRUSTED);
        t.trust = Some(TRUSTED.to_string());
        assert_eq!(class_of(Some(&t)), TRUSTED);
        t.trust = Some(UNTRUSTED.to_string());
        t.git_ref = Some("refs/pull/8/head".to_string());
        let f = RunFacts::of(Some(&t), "ci");
        assert_eq!(
            (f.trust.as_str(), f.git_ref.as_deref(), f.pipeline.as_str()),
            (UNTRUSTED, Some("refs/pull/8/head"), "ci")
        );
    }

    /// A run's class is not lost by calling a sub-workflow. The child's
    /// record has a kind of its own, and a CI record that names no class is
    /// read by its kind: copied as it stood, the child would be `trusted`.
    #[test]
    fn what_a_run_calls_is_of_its_class_whatever_its_record_left_unsaid() {
        let mut ci = RunTrigger::engine(crate::models::trigger_kind::CI, "source:forge");
        ci.git_ref = Some("refs/pull/8/head".to_string());
        assert_eq!(class_of(Some(&ci)), UNTRUSTED);
        let child = RunTrigger::child_of(Some(&ci), "run-1", "call");
        assert_eq!(child.kind, crate::models::trigger_kind::WORKFLOW);
        assert_eq!(class_of(Some(&child)), UNTRUSTED);
        // And its child's, which reads the class the first was given.
        let grandchild = RunTrigger::child_of(Some(&child), "run-2", "call");
        assert_eq!(class_of(Some(&grandchild)), UNTRUSTED);
        assert_eq!(
            RunFacts::of(Some(&grandchild), "child").git_ref.as_deref(),
            Some("refs/pull/8/head")
        );

        // A class that was said is kept as it was said.
        for said in [TRUSTED, UNTRUSTED, AGENT] {
            ci.trust = Some(said.to_string());
            let child = RunTrigger::child_of(Some(&ci), "run-1", "call");
            assert_eq!(class_of(Some(&child)), said);
        }
        // A record that is no CI service's says nothing of a class, and
        // neither does what it calls: both are the installation's own.
        let own = RunTrigger::engine(crate::models::trigger_kind::SCHEDULE, "schedule:nightly");
        let child = RunTrigger::child_of(Some(&own), "run-1", "call");
        assert_eq!(
            (child.trust.as_deref(), class_of(Some(&child))),
            (None, TRUSTED)
        );
        let orphan = RunTrigger::child_of(None, "run-0", "call");
        assert_eq!(
            (orphan.trust.as_deref(), class_of(Some(&orphan))),
            (None, TRUSTED)
        );
    }

    #[test]
    fn a_star_stops_at_a_slash_and_two_do_not() {
        for (pattern, text, yes) in [
            ("refs/heads/main", "refs/heads/main", true),
            ("refs/heads/main", "refs/heads/main/x", false),
            ("refs/heads/*", "refs/heads/main", true),
            ("refs/heads/*", "refs/heads/release/1", false),
            ("refs/heads/**", "refs/heads/release/1", true),
            ("refs/heads/release/*", "refs/heads/release/1", true),
            ("refs/tags/v*", "refs/tags/v", true),
            ("refs/tags/v?", "refs/tags/v", false),
            ("refs/tags/v?", "refs/tags/v1", true),
            ("refs/tags/v?", "refs/tags/v/", false),
            ("*", "main", true),
            ("*", "a/b", false),
            ("**", "a/b", true),
            ("**/main", "refs/heads/main", true),
            ("*-prod", "eu-prod", true),
            ("*-prod", "eu-prod-2", false),
            ("", "", true),
            ("", "x", false),
            ("a*b*c", "a-b-b-c", true),
            // Not a class: each bracket is itself.
            ("v[0-9]", "v1", false),
            ("v[0-9]", "v[0-9]", true),
        ] {
            assert_eq!(matches(pattern, text), yes, "{pattern:?} against {text:?}");
        }
    }

    #[test]
    fn a_policy_a_gateway_may_store_names_known_classes_and_real_patterns() {
        assert_eq!(EnvironmentPolicy::default().validate(), Ok(()));
        assert_eq!(policy(&[], None, None).validate(), Ok(()));
        assert!(policy(&["admin"], None, None)
            .validate()
            .unwrap_err()
            .contains("not a trust class"));
        assert!(policy(&[TRUSTED], Some(&[]), None)
            .validate()
            .unwrap_err()
            .contains("empty list"));
        assert!(policy(&[TRUSTED], None, Some(&[""])).validate().is_err());
        assert!(policy(&[TRUSTED], Some(&["refs/heads/\n"]), None)
            .validate()
            .is_err());
        let many: Vec<String> = (0..65).map(|i| format!("p{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(policy(&[TRUSTED], None, Some(&many)).validate().is_err());
        // What is stored reads back, and an unknown key is not taken for a
        // policy with one part fewer.
        let p = policy(&[TRUSTED], Some(&["refs/heads/main"]), None);
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(
            json,
            r#"{"allowed_trust":["trusted"],"allowed_refs":["refs/heads/main"]}"#
        );
        assert_eq!(serde_json::from_str::<EnvironmentPolicy>(&json).unwrap(), p);
        assert!(serde_json::from_str::<EnvironmentPolicy>(
            r#"{"allowed_trust":[],"allowed_ref":[]}"#
        )
        .is_err());
    }
}
