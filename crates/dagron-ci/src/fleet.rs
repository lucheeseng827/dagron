//! Fleet mode: GitHub Actions stays the coordinator of a repository's
//! workflows, and a job of its that asks for one of an operator's labels is
//! run by GitHub's own runner, started as a dagron task for that one job.
//! The team changes nothing but `runs-on`.
//!
//! This module is what an operator says is served: **the label map**
//! (CI_GITHUB_FLEET_FILE), a YAML file.
//!
//! ```yaml
//! labels:
//!   dagron:
//!     image: ghcr.io/actions/actions-runner:2.321.0
//!   dagron-large:
//!     image: ghcr.io/actions/actions-runner:2.321.0
//!     runner_class: large
//!     pool: ci-large
//!     priority: 5
//!     timeout_secs: 7200
//!     with: [self-hosted, linux, x64]
//!     scope: organization      # registered for the repository's owner
//!     runner_group: 3
//!     max_runners: 20          # out at once, for every repository together
//!     max_per_repository: 5
//!     untrusted: floor         # a fork's job: an untrusted run (refuse unless said)
//!     docker: true             # a Docker daemon beside the runner
//!     retry: false             # a job whose runner died is not run again
//!     warm: 2                  # runners kept ready beyond the jobs that wait
//!     warm_for_secs: 1800      # after a repository's last job of the label
//! ```
//!
//! **It is the operator's, and no repository's.** A label decides which
//! image is run, on which of the engine's runner classes and in which pool:
//! what a repository's own settings file may say of a job dagron-ci runs
//! itself (`.dagron/ci.yaml`) it may not say of capacity. A repository
//! chooses among the labels by writing one in `runs-on`.
//!
//! **Which jobs are served.** GitHub gives a job to a runner that has every
//! label the job asks for. So a job is served when exactly one of its
//! labels is a class here, and each of its others is one that class is
//! registered `with` (by default `self-hosted`, `linux`, `x64`). A job with
//! any other label is another runner's, and is left alone, as is a job that
//! names no class (`ubuntu-latest`).
//!
//! **GitLab's** label map is a file of its own (CI_GITLAB_FLEET_FILE), of
//! the same keys, for a job's tags: GitLab gives a job to a runner that has
//! every tag the job names, as GitHub does with labels, so a job is served
//! as a workflow job is. Its runners are GitLab's own (`gitlab-runner
//! run-single`), made for the project, or for its group with `scope:
//! organization`. GitLab gives a runner no tags of its own: a label goes
//! `with` none unless it says, may be any name of its own, and takes no
//! `runner_group`.
//!
//! **A fork's job.** A job of a workflow run whose head is in another
//! repository (a fork's pull request) is `untrusted`, and its label says
//! what it is given ([`Untrusted`]): nothing, unless said; a runner started
//! as a run that is not trusted, which the engine holds to its floor for
//! such runs; or, where a repository's forks are all its own people's, a
//! runner as any job's. GitHub gives a waiting job to any runner with its
//! labels, so what is started for a fork's job is not the only runner that
//! may take it: [`crate::fleet::worker`] says what is done about that.

use std::collections::BTreeMap;

use serde::Deserialize;

pub mod store;
pub mod usage;
pub mod worker;

/// The labels GitHub gives a self-hosted runner itself, which a job may
/// carry beside a class's own. None of them may be a class: a job that asks
/// for `self-hosted` alone is asking for somebody's runner, and not
/// necessarily this fleet's.
pub const GIVEN: [&str; 7] = [
    "self-hosted",
    "linux",
    "windows",
    "macos",
    "x64",
    "arm",
    "arm64",
];

/// The longest a job is given, where its class says nothing: six hours,
/// GitHub's own limit for a job on a runner it hosts.
pub const DEFAULT_TIMEOUT_SECS: u64 = 6 * 60 * 60;

/// The longest a class may give a job: five days, GitHub's limit for a job
/// on a self-hosted runner.
pub const MAX_TIMEOUT_SECS: u64 = 5 * 24 * 60 * 60;

/// The most classes a fleet file may name.
pub const MAX_CLASSES: usize = 64;

/// The most runners a class may keep out at once, for one repository or
/// for all of them.
pub const MAX_RUNNERS: u32 = 10_000;

/// The most runners a class keeps warm for one repository.
pub const MAX_WARM: u32 = 64;

/// How long after a repository's last job of a class its runners are kept
/// warm, unless the class says: an hour.
pub const DEFAULT_WARM_FOR_SECS: u64 = 60 * 60;

/// The longest a class may keep a repository's runners warm after its last
/// job: a week.
pub const MAX_WARM_FOR_SECS: u64 = 7 * 24 * 60 * 60;

/// The runner group an organization's runner is registered in, unless its
/// class says: GitHub's `Default`, which every organization has.
pub const DEFAULT_RUNNER_GROUP: u64 = 1;

/// Where a class's runners are registered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// For the repository whose job it was started for, and no other.
    #[default]
    Repository,
    /// For the repository's owner, an organization: GitHub may give it a job
    /// of any repository its runner group lets in. The group is where an
    /// organization says which repositories and workflows its runners
    /// serve, which a repository's runner has no place for.
    Organization,
}

/// What a class gives a job of a fork's pull request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Untrusted {
    /// No runner is started for it. The default: the job's workflow is its
    /// author's to write, and so is what it runs.
    #[default]
    Refuse,
    /// A runner started as a run that is not trusted: the engine holds it to
    /// its floor for such runs, withholds its class's pool and priority,
    /// and gives it no Docker daemon. Its configuration is kept in an
    /// environment that admits such runs (CI_FLEET_UNTRUSTED_ENVIRONMENT).
    Floor,
    /// A runner as any job's, at the class's own runner class: for a
    /// repository whose forks are all its own people's. Nothing is asked of
    /// GitHub to tell one job from another.
    Serve,
}

/// What a job that asks for a label is run with.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Class {
    /// The image the runner is started from: GitHub's own runner image, or
    /// one built on it.
    pub image: String,
    /// The labels a job may carry beside this one, and the runner is
    /// registered with: [`default_with`] unless said.
    #[serde(default = "default_with")]
    pub with: Vec<String>,
    /// The engine's runner class the task asks for (`runner_class`).
    #[serde(default)]
    pub runner_class: Option<String>,
    /// The engine's pool the task is queued in (`pool`).
    #[serde(default)]
    pub pool: Option<String>,
    /// The task's priority among those of its pool (`priority`).
    #[serde(default)]
    pub priority: Option<i64>,
    /// How long the runner may hold its task.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// What the task runs, for an image that starts its runner some other
    /// way than GitHub's own does (`/home/runner/run.sh`).
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Where its runners are registered: [`Scope::Repository`] unless said.
    #[serde(default)]
    pub scope: Scope,
    /// The organization's runner group its runners are registered in, by
    /// GitHub's id ([`DEFAULT_RUNNER_GROUP`] unless said). Of an
    /// organization's runners alone: a repository's have no group.
    #[serde(default)]
    pub runner_group: Option<u64>,
    /// The most of its runners out at once, for every repository together.
    /// A job past it waits for one to end.
    #[serde(default)]
    pub max_runners: Option<u32>,
    /// The most of its runners out at once for one repository, so that one
    /// repository's jobs do not hold every runner of [`Class::max_runners`].
    #[serde(default)]
    pub max_per_repository: Option<u32>,
    /// What a job of a fork's pull request is given ([`Untrusted`]).
    #[serde(default)]
    pub untrusted: Untrusted,
    /// The engine's runner class a runner started for such a job asks for,
    /// with [`Untrusted::Floor`]: the class's own unless said.
    #[serde(default)]
    pub untrusted_runner_class: Option<String>,
    /// A Docker daemon beside the runner (`docker_daemon`), for a job that
    /// builds or runs containers. The engine starts one where its operator
    /// offers it, and never for a run that is not trusted.
    #[serde(default)]
    pub docker: bool,
    /// Whether a job that failed because its runner died, and not because
    /// of what it ran, is run again, once, through GitHub's API: unless said.
    #[serde(default = "default_retry")]
    pub retry: bool,
    /// How many runners are kept ready beyond the jobs that wait, for a
    /// repository that has had a job of the label within
    /// [`Class::warm_for`]: a job is given one at once, and another is
    /// started in its place. None unless said, and none for a class that
    /// tells a fork's job apart unless it names a `runner_group`: a warm
    /// runner is a trusted run, which the forge would give a fork's job.
    #[serde(default)]
    pub warm: u32,
    /// How long after a repository's last job of the label its runners are
    /// kept warm, with `warm`: [`DEFAULT_WARM_FOR_SECS`] unless said.
    #[serde(default)]
    pub warm_for_secs: Option<u64>,
}

impl Class {
    /// Whether a job of this class is to be told trusted or not before it
    /// is given a runner: where the class does not give a fork's job what it
    /// gives any other.
    pub fn tells_forks_apart(&self) -> bool {
        self.untrusted != Untrusted::Serve
    }

    /// How long after a repository's last job of this class its runners are
    /// kept warm, in seconds.
    pub fn warm_for(&self) -> u64 {
        self.warm_for_secs.unwrap_or(DEFAULT_WARM_FOR_SECS)
    }

    /// The runner group its runners are registered in, where they are an
    /// organization's: `None` for a repository's.
    pub fn group(&self) -> Option<u64> {
        match self.scope {
            Scope::Repository => None,
            Scope::Organization => Some(self.runner_group.unwrap_or(DEFAULT_RUNNER_GROUP)),
        }
    }
}

fn default_with() -> Vec<String> {
    vec!["self-hosted".into(), "linux".into(), "x64".into()]
}

fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

fn default_retry() -> bool {
    true
}

/// Whose jobs a label map serves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Of {
    /// GitHub Actions' workflow jobs (CI_GITHUB_FLEET_FILE).
    #[default]
    GitHub,
    /// GitLab CI's jobs (CI_GITLAB_FLEET_FILE).
    GitLab,
}

/// The label map: each class by its label, in lower case.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fleet {
    classes: BTreeMap<String, Class>,
    of: Of,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    labels: BTreeMap<String, Class>,
}

/// A label as GitHub takes one for a runner: letters, digits, `-`, `_` and
/// `.`, and no more than 64 of them.
fn is_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 64
        && label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

impl Fleet {
    /// Read a fleet file. `Err` says what in it is not a label map, by the
    /// label it is under.
    pub fn parse(text: &str) -> Result<Fleet, String> {
        Fleet::parse_of(text, Of::GitHub)
    }

    /// Read a GitLab fleet file: [`Fleet::parse`], for GitLab's jobs.
    pub fn parse_gitlab(text: &str) -> Result<Fleet, String> {
        Fleet::parse_of(text, Of::GitLab)
    }

    fn parse_of(text: &str, of: Of) -> Result<Fleet, String> {
        let file: File =
            serde_yaml::from_str(text).map_err(|e| format!("is not a fleet file: {e}"))?;
        // Which labels say what they go `with`: one of GitLab's that does not
        // goes with nothing, where one of GitHub's goes with what GitHub gives.
        let said_with: Vec<String> = serde_yaml::from_str::<serde_yaml::Value>(text)
            .ok()
            .and_then(|v| v.get("labels").and_then(|l| l.as_mapping()).cloned())
            .map(|labels| {
                labels
                    .iter()
                    .filter(|(_, class)| class.get("with").is_some())
                    .filter_map(|(label, _)| label.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if file.labels.is_empty() {
            return Err("names no label under `labels:`: nothing would be served".into());
        }
        if file.labels.len() > MAX_CLASSES {
            return Err(format!(
                "names {} labels, and a fleet file takes {MAX_CLASSES}",
                file.labels.len()
            ));
        }
        let mut classes = BTreeMap::new();
        for (label, mut class) in file.labels {
            let at = format!("labels.{label}");
            let name = label.to_ascii_lowercase();
            if !is_label(&label) {
                return Err(format!(
                    "{at}: a label is letters, digits, '-', '_' and '.', at most 64"
                ));
            }
            if of == Of::GitHub && GIVEN.contains(&name.as_str()) {
                return Err(format!(
                    "{at}: is a label GitHub gives every such runner, and a job that asks \
                     for it alone is not this fleet's to take; name a label of your own"
                ));
            }
            if of == Of::GitLab {
                if !said_with.contains(&label) {
                    class.with = Vec::new();
                }
                if class.runner_group.is_some() {
                    return Err(format!(
                        "{at}.runner_group: is GitHub's; GitLab's runner of a group is its \
                         project's namespace's, with `scope: organization`"
                    ));
                }
            }
            if class.image.trim().is_empty() || class.image.contains(char::is_whitespace) {
                return Err(format!("{at}.image: is the image the runner is run from"));
            }
            if !(60..=MAX_TIMEOUT_SECS).contains(&class.timeout_secs) {
                return Err(format!(
                    "{at}.timeout_secs: is 60 to {MAX_TIMEOUT_SECS}, not {}",
                    class.timeout_secs
                ));
            }
            for (what, value) in [
                ("runner_class", &class.runner_class),
                ("pool", &class.pool),
                ("untrusted_runner_class", &class.untrusted_runner_class),
            ] {
                if value.as_deref().is_some_and(|v| !is_label(v)) {
                    return Err(format!(
                        "{at}.{what}: is a name of letters, digits, '-', '_' and '.'"
                    ));
                }
            }
            if class.untrusted_runner_class.is_some() && class.untrusted != Untrusted::Floor {
                return Err(format!(
                    "{at}.untrusted_runner_class: is the runner class of a fork's job's runner, \
                     which is started only with `untrusted: floor`"
                ));
            }
            match (class.scope, class.runner_group) {
                (Scope::Repository, Some(_)) => {
                    return Err(format!(
                        "{at}.runner_group: is an organization's, and a repository's runner has \
                         none: say `scope: organization`"
                    ))
                }
                (Scope::Organization, Some(0)) => {
                    return Err(format!(
                        "{at}.runner_group: is GitHub's id for a group, from 1"
                    ))
                }
                _ => {}
            }
            for (what, value) in [
                ("max_runners", class.max_runners),
                ("max_per_repository", class.max_per_repository),
            ] {
                if value.is_some_and(|n| !(1..=MAX_RUNNERS).contains(&n)) {
                    return Err(format!("{at}.{what}: is 1 to {MAX_RUNNERS}"));
                }
            }
            if class.warm > MAX_WARM {
                return Err(format!("{at}.warm: is 0 to {MAX_WARM}"));
            }
            match class.warm_for_secs {
                Some(_) if class.warm == 0 => {
                    return Err(format!(
                        "{at}.warm_for_secs: is how long runners are kept warm, and `warm` keeps \
                         none"
                    ))
                }
                Some(secs) if !(60..=MAX_WARM_FOR_SECS).contains(&secs) => {
                    return Err(format!(
                        "{at}.warm_for_secs: is 60 to {MAX_WARM_FOR_SECS}, not {secs}"
                    ))
                }
                _ => {}
            }
            // A warm runner is a trusted run waiting idle, and the forge gives
            // an idle runner any job that asks for its labels, a fork's among
            // them: kept warm, a label that refuses a fork's job would serve it
            // on a trusted runner until a look stops it.
            if class.warm > 0 && class.tells_forks_apart() && class.runner_group.is_none() {
                let or = match of {
                    Of::GitHub => {
                        ", or name a `runner_group` restricted to selected workflows, which \
                         is given no pull request's job"
                    }
                    Of::GitLab => "",
                };
                return Err(format!(
                    "{at}.warm: keeps trusted runners idle, which the forge would give a \
                     fork's job: say `untrusted: serve`{or}"
                ));
            }
            if let (Some(all), Some(one)) = (class.max_runners, class.max_per_repository) {
                if one > all {
                    return Err(format!(
                        "{at}.max_per_repository: is {one}, more than max_runners ({all}) lets \
                         out for every repository together"
                    ));
                }
            }
            if class
                .command
                .as_ref()
                .is_some_and(|c| c.is_empty() || c[0].is_empty())
            {
                return Err(format!(
                    "{at}.command: is what the runner is started with, and has a first word"
                ));
            }
            if let Some(bad) = class.with.iter().find(|l| !is_label(l)) {
                return Err(format!("{at}.with: {bad:?} is not a label"));
            }
            class.with = class.with.iter().map(|l| l.to_ascii_lowercase()).collect();
            if class.with.contains(&name) {
                return Err(format!("{at}.with: names the label itself"));
            }
            if classes.insert(name, class).is_some() {
                return Err(format!(
                    "{at}: is named twice, in two cases; GitHub takes them for one label"
                ));
            }
        }
        // A label one class is registered with is not another's name: a job
        // that asked for both would be either's.
        for (label, class) in &classes {
            if let Some(other) = class.with.iter().find(|l| classes.contains_key(*l)) {
                return Err(format!(
                    "labels.{label}.with: {other:?} is a label of this file's own"
                ));
            }
        }
        Ok(Fleet { classes, of })
    }

    /// The class that serves a job with `labels`, by its label: the one
    /// label of the job's that is a class, where each of the job's others
    /// is one that class is registered with. `None` for a job that is not
    /// this fleet's.
    pub fn serves(&self, labels: &[String]) -> Option<(&str, &Class)> {
        let asked: Vec<String> = labels.iter().map(|l| l.to_ascii_lowercase()).collect();
        let mut named = asked
            .iter()
            .filter_map(|label| self.classes.get_key_value(label.as_str()));
        let (name, class) = named.next()?;
        if named.next().is_some() {
            return None;
        }
        asked
            .iter()
            .all(|label| label == name || class.with.contains(label))
            .then_some((name.as_str(), class))
    }

    /// Every label a runner of the class `name` is registered with: its
    /// own, then those it goes with.
    pub fn registered(&self, name: &str) -> Vec<String> {
        let mut labels = vec![name.to_string()];
        if let Some(class) = self.classes.get(name) {
            labels.extend(class.with.iter().cloned());
        }
        labels
    }

    /// The class named `name`, in lower case.
    pub fn class(&self, name: &str) -> Option<&Class> {
        self.classes.get(name)
    }

    /// The labels this fleet serves, in order.
    pub fn labels(&self) -> impl Iterator<Item = &str> {
        self.classes.keys().map(String::as_str)
    }

    /// Whose jobs it serves.
    pub fn of(&self) -> Of {
        self.of
    }

    /// Whether a class of this fleet tells a fork's job from another's.
    pub fn tells_forks_apart(&self) -> bool {
        self.classes.values().any(Class::tells_forks_apart)
    }

    /// Whether a class of this fleet starts a fork's job's runner as a run
    /// that is not trusted.
    pub fn has_floor(&self) -> bool {
        self.classes
            .values()
            .any(|class| class.untrusted == Untrusted::Floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: &str = r#"
labels:
  dagron:
    image: ghcr.io/actions/actions-runner:2.321.0
  Dagron-Large:
    image: ghcr.io/actions/actions-runner:2.321.0
    runner_class: large
    pool: ci-large
    priority: 5
    timeout_secs: 7200
    with: [self-hosted, Linux, ARM64]
"#;

    fn job(labels: &[&str]) -> Vec<String> {
        labels.iter().map(|l| l.to_string()).collect()
    }

    #[test]
    fn a_job_is_served_by_the_one_class_it_names_and_the_labels_that_go_with_it() {
        let fleet = Fleet::parse(FILE).unwrap();
        assert_eq!(
            fleet.labels().collect::<Vec<_>>(),
            ["dagron", "dagron-large"]
        );
        let (name, class) = fleet
            .serves(&job(&["self-hosted", "linux", "x64", "dagron"]))
            .unwrap();
        assert_eq!(
            (name, class.timeout_secs, class.pool.as_deref()),
            ("dagron", DEFAULT_TIMEOUT_SECS, None)
        );
        // In whatever case the workflow writes them, and alone.
        assert_eq!(fleet.serves(&job(&["Dagron"])).unwrap().0, "dagron");
        let (name, class) = fleet
            .serves(&job(&["SELF-HOSTED", "dagron-large", "arm64"]))
            .unwrap();
        assert_eq!(
            (name, class.runner_class.as_deref(), class.priority),
            ("dagron-large", Some("large"), Some(5))
        );
        assert_eq!(
            fleet.registered("dagron-large"),
            ["dagron-large", "self-hosted", "linux", "arm64"]
        );

        // Another runner's: a label no class goes with, no class at all, two
        // classes at once, or one GitHub hosts.
        for theirs in [
            &["self-hosted", "linux", "x64", "dagron", "gpu"][..],
            &["self-hosted", "linux", "x64"],
            &["dagron", "dagron-large"],
            &["dagron-large", "x64"],
            &["ubuntu-latest"],
            &[],
        ] {
            assert_eq!(fleet.serves(&job(theirs)), None, "{theirs:?}");
        }
    }

    #[test]
    fn a_fleet_file_that_would_take_another_runner_s_jobs_is_refused() {
        let refused = |text: &str| Fleet::parse(text).unwrap_err();
        let class = |label: &str, rest: &str| {
            format!("labels:\n  {label}:\n    image: ghcr.io/actions/actions-runner:2\n{rest}")
        };
        assert!(Fleet::parse(&class("dagron", "")).is_ok());
        for given in GIVEN {
            let why = refused(&class(given, ""));
            assert!(
                why.contains("GitHub gives every such runner"),
                "{given}: {why}"
            );
        }
        assert!(refused(&class("Self-Hosted", "")).contains("GitHub gives"));
        for (rest, says) in [
            ("    timeout_secs: 5\n", "timeout_secs"),
            ("    timeout_secs: 999999999\n", "timeout_secs"),
            ("    pool: \"a pool\"\n", "pool"),
            ("    runner_class: \"../x\"\n", "runner_class"),
            ("    with: [\"self hosted\"]\n", "with"),
            ("    with: [dagron]\n", "names the label itself"),
            ("    privileged: true\n", "not a fleet file"),
        ] {
            let why = refused(&class("dagron", rest));
            assert!(why.contains(says), "{rest:?}: {why}");
        }
        assert!(refused("labels: {}\n").contains("names no label"));
        assert!(refused("label: {}\n").contains("not a fleet file"));
        assert!(refused(&class("a label", "")).contains("a label is letters"));
        let no_image = "labels:\n  dagron:\n    image: \"\"\n";
        assert!(refused(no_image).contains("image"));
        let twice = "labels:\n  dagron: { image: a }\n  DAGRON: { image: b }\n";
        assert!(refused(twice).contains("named twice"));
        let both = "labels:\n  dagron: { image: a, with: [big] }\n  big: { image: b }\n";
        assert!(refused(both).contains("a label of this file's own"));
    }

    /// Where a label's runners are registered, how many may be out, what a
    /// fork's job is given and whether a daemon goes beside the runner, and
    /// what of them does not go together.
    #[test]
    fn a_label_says_where_its_runners_go_how_many_and_what_a_fork_s_job_gets() {
        let fleet = Fleet::parse(
            "labels:\n  dagron:\n    image: a\n  big:\n    image: b\n    scope: organization\n    \
             runner_group: 3\n    max_runners: 20\n    max_per_repository: 5\n    \
             untrusted: floor\n    untrusted_runner_class: sandbox\n    docker: true\n  \
             open:\n    image: c\n    untrusted: serve\n",
        )
        .unwrap();
        let plain = fleet.class("dagron").unwrap();
        assert_eq!(
            (plain.scope, plain.group(), plain.untrusted, plain.docker),
            (Scope::Repository, None, Untrusted::Refuse, false),
            "a repository's runner, and no fork's job served, unless said"
        );
        assert!(
            plain.retry,
            "a job whose runner died is run again unless said"
        );
        assert_eq!(
            (plain.warm, plain.warm_for()),
            (0, DEFAULT_WARM_FOR_SECS),
            "nothing kept warm unless said"
        );
        assert!(plain.tells_forks_apart());
        let big = fleet.class("big").unwrap();
        assert_eq!(
            (big.group(), big.max_runners, big.max_per_repository),
            (Some(3), Some(20), Some(5))
        );
        assert_eq!(
            (
                big.untrusted,
                big.untrusted_runner_class.as_deref(),
                big.docker
            ),
            (Untrusted::Floor, Some("sandbox"), true)
        );
        assert!(!fleet.class("open").unwrap().tells_forks_apart());
        assert!(fleet.tells_forks_apart() && fleet.has_floor());
        let org = Fleet::parse("labels:\n  x:\n    image: a\n    scope: organization\n").unwrap();
        assert_eq!(
            org.class("x").unwrap().group(),
            Some(DEFAULT_RUNNER_GROUP),
            "an organization's Default group unless said"
        );

        let refused =
            |rest: &str| Fleet::parse(&format!("labels:\n  x:\n    image: a\n{rest}")).unwrap_err();
        for (rest, says) in [
            ("    runner_group: 3\n", "say `scope: organization`"),
            ("    scope: organization\n    runner_group: 0\n", "from 1"),
            ("    scope: org\n", "not a fleet file"),
            ("    max_runners: 0\n", "max_runners: is 1 to"),
            (
                "    max_per_repository: 100000\n",
                "max_per_repository: is 1 to",
            ),
            (
                "    max_runners: 2\n    max_per_repository: 3\n",
                "more than max_runners",
            ),
            ("    untrusted: yes\n", "not a fleet file"),
            (
                "    untrusted_runner_class: sandbox\n",
                "only with `untrusted: floor`",
            ),
            (
                "    untrusted: floor\n    untrusted_runner_class: \"a b\"\n",
                "untrusted_runner_class: is a name",
            ),
            ("    docker: \"yes\"\n", "not a fleet file"),
            ("    retry: 1\n", "not a fleet file"),
            ("    warm: 65\n", "warm: is 0 to"),
            (
                "    warm: 1\n",
                "say `untrusted: serve`, or name a `runner_group`",
            ),
            (
                "    warm: 1\n    untrusted: floor\n    scope: organization\n",
                "keeps trusted runners idle",
            ),
            ("    warm_for_secs: 600\n", "`warm` keeps none"),
            (
                "    warm: 1\n    warm_for_secs: 5\n",
                "warm_for_secs: is 60 to",
            ),
        ] {
            let why = refused(rest);
            assert!(why.contains(says), "{rest:?}: {why}");
        }
    }

    /// GitLab's map is read for a job's tags: no tag is GitLab's own, a
    /// label goes with none unless it says, and a group's runner takes no
    /// group of GitHub's.
    #[test]
    fn a_gitlab_fleet_file_is_read_for_a_job_s_tags() {
        let fleet = Fleet::parse_gitlab(
            "labels:\n  linux:\n    image: gitlab/gitlab-runner:v17.5.0\n  big:\n    \
             image: gitlab/gitlab-runner:v17.5.0\n    with: [docker]\n    scope: organization\n",
        )
        .unwrap();
        assert_eq!(fleet.of(), Of::GitLab);
        assert_eq!(fleet.serves(&job(&["linux"])).unwrap().0, "linux");
        assert_eq!(
            fleet.serves(&job(&["linux", "self-hosted"])),
            None,
            "no tag of GitLab's own"
        );
        assert_eq!(fleet.serves(&job(&["big", "docker"])).unwrap().0, "big");
        assert_eq!(fleet.registered("linux"), ["linux"]);
        assert_eq!(Fleet::parse(FILE).unwrap().of(), Of::GitHub);
        let why = Fleet::parse_gitlab(
            "labels:\n  x:\n    image: a\n    scope: organization\n    runner_group: 3\n",
        )
        .unwrap_err();
        assert!(why.contains("runner_group: is GitHub's"), "{why}");
        let why = Fleet::parse_gitlab("labels:\n  x:\n    image: a\n    warm: 1\n").unwrap_err();
        assert!(why.ends_with("say `untrusted: serve`"), "{why}");
        assert!(Fleet::parse_gitlab(
            "labels:\n  x:\n    image: a\n    warm: 1\n    untrusted: serve\n"
        )
        .is_ok());
    }

    /// The chart writes the map as JSON (its `ci.fleet.labels`), which is
    /// YAML too: a whole number written whole, and `&`, `<` and `>` in a
    /// string as the escapes Go's encoder makes of them.
    #[test]
    fn a_fleet_file_written_as_json_is_read() {
        let fleet = Fleet::parse(
            r#"{
  "labels": {
    "Dagron-Large": {
      "command": [
        "sh",
        "-c",
        "a && b <x>"
      ],
      "image": "ghcr.io/actions/actions-runner:2.321.0",
      "pool": "ci-large",
      "priority": 1000000,
      "runner_class": null,
      "timeout_secs": 7200,
      "with": [
        "self-hosted",
        "Linux"
      ]
    },
    "dagron": {
      "image": "ghcr.io/actions/actions-runner:2.321.0"
    }
  }
}"#,
        )
        .unwrap();
        let (name, class) = fleet.serves(&job(&["dagron-large", "linux"])).unwrap();
        assert_eq!(
            (
                name,
                class.priority,
                class.timeout_secs,
                &class.runner_class
            ),
            ("dagron-large", Some(1_000_000), 7200, &None)
        );
        assert_eq!(
            class.command.as_deref().unwrap(),
            ["sh", "-c", "a && b <x>"]
        );
        assert!(fleet.serves(&job(&["dagron"])).is_some());
    }

    /// The chart refuses at render a map this would not read, so that a pod
    /// does not start again and again on one: its checks are this file's,
    /// said twice. What can be compared is, so that a key a label gains
    /// here is not one the chart goes on refusing. Skipped where the chart
    /// is not beside the crate.
    #[test]
    fn the_chart_refuses_a_label_map_by_this_file_s_rules() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let Some(chart) = ["terraform/charts/dagron", "charts/dagron"]
            .iter()
            .map(|dir| root.join(dir).join("templates/ci.yaml"))
            .find(|path| path.is_file())
        else {
            eprintln!("the chart is not beside this crate - skipping");
            return;
        };
        let chart = std::fs::read_to_string(chart).unwrap();
        // Words as a template writes a list of them.
        let words = |of: &[&str]| {
            let quoted: Vec<String> = of.iter().map(|one| format!("{one:?}")).collect();
            quoted.join(" ")
        };
        let list = |of: &[&str]| format!("(list {})", words(of));

        // A label's keys, as the reader itself names them when it is given
        // one it does not know.
        let unknown = "labels:\n  dagron:\n    image: a\n    no_such_key: 1\n";
        let why = Fleet::parse(unknown).unwrap_err();
        let known = why
            .split_once("expected one of ")
            .unwrap_or_else(|| panic!("{why}"))
            .1;
        let keys: Vec<&str> = known.split('`').skip(1).step_by(2).collect();
        assert!(keys.contains(&"image") && keys.len() >= 7, "{why}");
        assert!(chart.contains(&list(&keys)), "a label's keys: {keys:?}");

        assert!(chart.contains(&list(&GIVEN)), "the labels GitHub gives");
        let with = default_with();
        let with: Vec<&str> = with.iter().map(String::as_str).collect();
        for bound in [
            format!("if gt (len $labels) {MAX_CLASSES} }}}}"),
            format!("(ge (int64 $t) 60) (le (int64 $t) {MAX_TIMEOUT_SECS})"),
            // `is_label`, and the labels a job may name unless said.
            r#""^[A-Za-z0-9._-]{1,64}$""#.to_string(),
            format!("$with := list {} }}}}", words(&with)),
        ] {
            assert!(chart.contains(&bound), "the chart has no {bound:?}");
        }
    }
}
