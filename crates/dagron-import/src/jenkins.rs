//! A Jenkins Declarative Pipeline, as a pipeline of `.dagron/ci`.
//!
//! [`jenkins_to_pipeline`] reads a `Jenkinsfile` written as a Declarative
//! Pipeline (`pipeline { … }`) and writes the pipeline it would be: its
//! stages as the steps of jobs, the images its agents name, its
//! `environment`, `parameters`, `options`, `when` and `post`, its matrices
//! and its gates (`input`). A Scripted Pipeline (`node { … }`) is Groovy
//! through and through, and is refused by name. `docs/CI_MIGRATING.md` has
//! the same map as a table, for a person doing it by hand.
//!
//! The report is the GitHub importer's ([`crate::common`]): what has no
//! place of its own is *changed*, *left* or *to do*, and a job with
//! something to do starts with a step that fails and says what.
//!
//! **Stages share a workspace; jobs do not.** Jenkins runs every stage
//! under one agent in one workspace, so a stage reads what the one before
//! it built. Here a job's workspace is its own. So the stages that share an
//! agent are the steps of one job, one after another, and a stage with an
//! agent of its own is a job of its own, as it had a workspace of its own
//! there too. Parallel stages that share the pipeline's agent are steps of
//! that job, one after another, for the same reason: the report says so.
//!
//! **A `when` is a test at the head of the stage's steps**, and of its
//! `post`'s, which the shell reads as Jenkins did (`branch`, `tag`,
//! `buildingTag()`, `changeRequest()`, `environment`, a plain `expression`,
//! and `not`, `allOf`, `anyOf` of them): where it is false the steps end
//! there, and the job passes. A gate (`input`) whose stage has a `when`
//! dagron can read is given it, so that it asks only where the stage would
//! run; the jobs after it wait for what it waited for too, as a run leaves
//! out a gate whose `when` is false.
//!
//! **A stage's secrets are its steps' alone**, as Jenkins bound them: the
//! job holds each by a name of its own, and the stage's steps set the
//! stage's name from it at their head.

mod groovy;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Result};
use serde_yaml::{Mapping, Value};

pub use crate::common::Imported;
use crate::common::*;
use groovy::{Arg, Block, Expr, GStr, Part, Stmt};

/// What the importer is told beside the file.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// What the pipeline is called: the file's name, say. `ci` unless one
    /// is given.
    pub name: Option<String>,
    /// The image an agent's label means (`agent { label 'linux' }`), and
    /// `any` for `agent any`.
    pub images: BTreeMap<String, String>,
}

/// Jenkins's variables a step here can be given: each is set at the head of
/// the step that reads it, from dagron-ci's own.
const DERIVED: &[(&str, &str)] = &[
    (
        "BRANCH_NAME",
        "case \"$CI_EVENT\" in pull_request) export BRANCH_NAME=\"PR-$CI_PR\" ;; *) export BRANCH_NAME=\"${CI_REF#refs/*/}\" ;; esac",
    ),
    (
        "GIT_BRANCH",
        "case \"$CI_EVENT\" in pull_request) export GIT_BRANCH=\"PR-$CI_PR\" ;; *) export GIT_BRANCH=\"${CI_REF#refs/*/}\" ;; esac",
    ),
    ("GIT_COMMIT", "export GIT_COMMIT=\"$CI_SHA\""),
    (
        "GIT_PREVIOUS_COMMIT",
        "[ -z \"$CI_BASE_SHA\" ] || export GIT_PREVIOUS_COMMIT=\"$CI_BASE_SHA\"",
    ),
    (
        "TAG_NAME",
        "case \"$CI_REF\" in refs/tags/*) export TAG_NAME=\"${CI_REF#refs/tags/}\" ;; esac",
    ),
    ("CHANGE_ID", "[ -z \"$CI_PR\" ] || export CHANGE_ID=\"$CI_PR\""),
    (
        "CHANGE_TARGET",
        "[ -z \"$CI_BASE_REF\" ] || export CHANGE_TARGET=\"${CI_BASE_REF#refs/heads/}\"",
    ),
    ("WORKSPACE", "export WORKSPACE=\"$PWD\""),
];

/// Jenkins's variables no step here is given.
const NOT_GIVEN: &[&str] = &[
    "BUILD_DISPLAY_NAME",
    "BUILD_ID",
    "BUILD_NUMBER",
    "BUILD_TAG",
    "BUILD_URL",
    "CHANGE_AUTHOR",
    "CHANGE_AUTHOR_DISPLAY_NAME",
    "CHANGE_AUTHOR_EMAIL",
    "CHANGE_BRANCH",
    "CHANGE_FORK",
    "CHANGE_TITLE",
    "CHANGE_URL",
    "EXECUTOR_NUMBER",
    "GIT_AUTHOR_EMAIL",
    "GIT_AUTHOR_NAME",
    "GIT_COMMITTER_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_LOCAL_BRANCH",
    "GIT_PREVIOUS_SUCCESSFUL_COMMIT",
    "GIT_URL",
    "HUDSON_HOME",
    "HUDSON_URL",
    "JENKINS_HOME",
    "JENKINS_URL",
    "JOB_BASE_NAME",
    "JOB_NAME",
    "JOB_URL",
    "NODE_LABELS",
    "NODE_NAME",
    "RUN_ARTIFACTS_DISPLAY_URL",
    "RUN_CHANGES_DISPLAY_URL",
    "RUN_DISPLAY_URL",
    "RUN_TESTS_DISPLAY_URL",
    "TAG_DATE",
    "TAG_TIMESTAMP",
    "TAG_UNIXTIME",
];

/// The steps that report on a build, keep its files, or clean up after it:
/// what each is here.
const LEFT_STEPS: &[(&str, &str)] = &[
    (
        "archiveArtifacts",
        "a job's files are not kept: its workspace goes with it",
    ),
    (
        "cleanWs",
        "a job's workspace goes with it, and is not cleaned",
    ),
    ("cobertura", "a coverage report is not read here"),
    ("fingerprint", "a file's fingerprint is not kept here"),
    ("jacoco", "a coverage report is not read here"),
    (
        "milestone",
        "an older run is not stopped at a milestone here",
    ),
    ("publishHTML", "an HTML report is not published here"),
    ("recordCoverage", "a coverage report is not read here"),
    (
        "recordIssues",
        "a linter's report is not read here: one in SARIF is (report: sarif)",
    ),
];

/// The steps that call a service, or wrap their own in a plugin's setting:
/// each is left to do where it stands.
const PLUGIN_STEPS: &[(&str, &str)] = &[
    ("build", "another Jenkins job is not started here: make what it runs a job of this pipeline, or a pipeline of its own"),
    ("emailext", "a mail is not sent from here: dagron's notify sends word of a run, or a step calls the service"),
    ("mail", "a mail is not sent from here: dagron's notify sends word of a run, or a step calls the service"),
    ("slackSend", "Slack is not told from here: dagron's notify sends word of a run, or a step calls Slack's API"),
    ("input", "a person's answer in the middle of a stage is not asked for here: make it the stage's input, which is a gate before it"),
    ("lock", "a lock that outlives the job is not here: a pipeline's runs of one concurrency key run one at a time (concurrency: { key: … })"),
    ("container", "a container of a Kubernetes agent's pod is not here: the job's image is its one container"),
    ("bat", "a Windows command is not run here: a job is a Linux container"),
    ("powershell", "PowerShell is not run here: write the step for sh, or call pwsh in an image that has it"),
    ("pwsh", "PowerShell is not run here: write the step for sh, or call pwsh in an image that has it"),
    ("parallel", "parallel branches in a step are not taken: make them stages of a parallel block"),
    ("checkout", "a checkout of another repository is not made here: clone it in a step"),
    ("git", "a checkout of another repository is not made here: clone it in a step"),
    ("readFile", "a file's text is read in a step's script here"),
    ("unstable", "a run that is unstable is not one here: a step passes or fails"),
    ("tool", "a tool Jenkins installed is not here: the job's image has to hold it"),
];

/// Wrappers a plugin gives, whose steps are imported where they stand,
/// after a step that says the wrapper is not.
const PLUGIN_WRAPPERS: &[&str] = &[
    "ansiColor",
    "configFileProvider",
    "nodejs",
    "sshagent",
    "timestamps",
    "withAWS",
    "withDockerRegistry",
    "withGradle",
    "withKubeConfig",
    "withMaven",
    "withSonarQubeEnv",
];

/// What is said of stages of the agent's that are a job of their own.
const WORKSPACE_OF_ITS_OWN: &str = "its stages start in a workspace of their own, a checkout of the event's commit, where on Jenkins they had what the stages before them left in the agent's: what they need of it is made again, or the stages are steps of one job";

/// A stage's directives, read.
#[derive(Default)]
struct Stage<'s> {
    name: String,
    at: String,
    agent: Option<&'s Stmt>,
    environment: Option<&'s Block>,
    options: Option<&'s Block>,
    when: Option<&'s Block>,
    input: Option<&'s Block>,
    tools: Option<&'s Block>,
    steps: Option<&'s Block>,
    stages: Option<&'s Block>,
    parallel: Option<&'s Block>,
    matrix: Option<&'s Block>,
    post: Option<&'s Block>,
    /// `failFast true`, of its parallel stages.
    fail_fast: bool,
    others: Vec<&'s Stmt>,
}

/// Where a stage's steps run: an image, or none known; and what is left to
/// do of the agent, for every job made under it.
#[derive(Clone, Default)]
struct Scope {
    /// `agent none`: no stage runs here without an agent of its own.
    none: bool,
    image: Option<String>,
    daemon: bool,
    pull_always: bool,
    /// Variables the agent's container was given (`-e`).
    env: Vec<(String, String)>,
    todo: Vec<(String, String)>,
    left: Vec<(String, String)>,
}

/// What a stage's steps are given, from the stages around them.
#[derive(Clone, Default)]
struct Ctx {
    /// The stage, by its name, and where it is.
    stage: String,
    at: String,
    /// Variables with a plain value, and those set at the head of each step
    /// (by name, and the line that sets it).
    env: Vec<(String, String)>,
    exports: Vec<(String, String)>,
    /// Secrets, by the job's variable that holds each: the pipeline's by
    /// their own names, a stage's by names of their own, which its steps
    /// export as the names it gave them ([`Importer::held`]).
    secrets: Vec<(String, String)>,
    /// The variables `credentials()` gave the stages around a step.
    credentials: Vec<String>,
    /// Lines that end a step where its stage's `when` is false.
    guards: Vec<String>,
    dir: Option<String>,
    timeout: Option<u64>,
    continue_on_error: bool,
    /// The names a step is given a variable of.
    defined: BTreeSet<String>,
    /// What is left to do of the stages around a step: said by the job
    /// that holds it.
    todo: Vec<(String, String)>,
}

/// A condition of `when`.
#[derive(Clone, Debug)]
enum Cond {
    Branch(Pattern),
    /// A tag's build; of a tag the pattern matches, where there is one.
    Tag(Option<Pattern>),
    /// A pull request's build, into a branch the pattern matches.
    Change(Option<Pattern>),
    /// A variable's value.
    Env(String, String),
    /// A variable that is set and not empty: a string Groovy takes as true.
    Set(String),
    /// What holds, or does not, whatever the build: `return true`.
    Lit(bool),
    Not(Box<Cond>),
    All(Vec<Cond>),
    Any(Vec<Cond>),
    /// Not read: taken to hold or not, whichever runs the stage, and said.
    Unread(String),
}

#[derive(Clone, Debug)]
enum Pattern {
    Exact(String),
    Glob(String),
}

/// A job, as it is made.
struct Task {
    name: String,
    at: String,
    fields: Vec<(&'static str, Value)>,
    steps: Vec<Value>,
    secrets: Vec<(String, String)>,
    todo: Vec<String>,
    stands: bool,
    /// The steps that stand for one not imported, by where they are.
    stood: Vec<usize>,
    deps: Vec<usize>,
    junit: Vec<String>,
    /// Its matrix's axes, and whether it is of one axis alone.
    axes: Vec<String>,
    single: Option<String>,
    /// It checks out the event's commit in a step (`checkout scm`).
    checks_out: bool,
    skip_checkout: bool,
    /// Variables of its agent's.
    env: Vec<(String, String)>,
}

/// The order a task's keys are written in.
const ORDER: &[&str] = &[
    "name",
    "type",
    "docker_image",
    "image_pull_policy",
    "depends_on",
    "when",
    "trigger_rule",
    "approval_message",
    "with_items",
    "env",
    "workspace",
    "docker_daemon",
    "timeout_secs",
    "max_attempts",
    "report",
    "steps",
];

impl Task {
    fn set(&mut self, key: &'static str, value: Value) {
        self.fields.retain(|(k, _)| *k != key);
        self.fields.push((key, value));
    }
}

/// Where the stages read so far end: the jobs the next must wait for, and
/// the job whose workspace the next stage of the same agent shares.
#[derive(Clone, Default)]
struct Cursor {
    frontier: Vec<usize>,
    open: Option<usize>,
}

/// A name a secret may have: letters, digits and `_`.
fn as_secret(id: &str) -> String {
    let out: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .take(128)
        .collect();
    if out.is_empty() {
        "secret".to_string()
    } else {
        out
    }
}

/// A shell word that is `text` and nothing else.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

/// A glob of Jenkins's (`*`, `**`, `?`), as a shell's `case` pattern, with
/// every other character as itself.
fn case_pattern(pattern: &Pattern) -> String {
    match pattern {
        Pattern::Exact(text) => quoted(text),
        Pattern::Glob(glob) => {
            let mut out = String::new();
            let mut literal = String::new();
            let mut chars = glob.chars().peekable();
            let flush = |literal: &mut String, out: &mut String| {
                if !literal.is_empty() {
                    out.push_str(&quoted(literal));
                    literal.clear();
                }
            };
            while let Some(c) = chars.next() {
                match c {
                    '*' => {
                        flush(&mut literal, &mut out);
                        while chars.peek() == Some(&'*') {
                            chars.next();
                        }
                        out.push('*');
                    }
                    '?' => {
                        flush(&mut literal, &mut out);
                        out.push('?');
                    }
                    c => literal.push(c),
                }
            }
            flush(&mut literal, &mut out);
            out
        }
    }
}

/// Whether a name may be written in a `when:` as it is: of the characters
/// `{{ ci_group }}` keeps.
fn group_holds(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 110
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'))
}

impl Cond {
    /// The test a shell reads it as. What is not read is taken to hold
    /// where the test holds with it, and not to where the test holds
    /// without it (under a `not`): so the stage runs wherever it might
    /// have, and more often than it did, never less.
    fn shell(&self) -> String {
        self.shell_where(true)
    }

    /// The test, as a part that the whole holds with (`plain`), or holds
    /// without (under a `not`).
    fn shell_where(&self, plain: bool) -> String {
        let case = |subject: &str, pattern: &Pattern| {
            format!(
                "case \"{subject}\" in {}) true ;; *) false ;; esac",
                case_pattern(pattern)
            )
        };
        match self {
            Cond::Branch(p) => case("$BRANCH_NAME", p),
            Cond::Tag(None) => "[ \"$CI_EVENT\" = tag ]".to_string(),
            Cond::Tag(Some(p)) => format!(
                "[ \"$CI_EVENT\" = tag ] && {}",
                case("${CI_REF#refs/tags/}", p)
            ),
            Cond::Change(None) => "[ \"$CI_EVENT\" = pull_request ]".to_string(),
            Cond::Change(Some(p)) => format!(
                "[ \"$CI_EVENT\" = pull_request ] && {}",
                case("${CI_BASE_REF#refs/heads/}", p)
            ),
            Cond::Env(name, value) => format!("[ \"${{{name}:-}}\" = {} ]", quoted(value)),
            Cond::Set(name) => format!("[ -n \"${{{name}:-}}\" ]"),
            Cond::Lit(holds) => holds.to_string(),
            Cond::Not(c) => format!("! {{ {}; }}", c.shell_where(!plain)),
            // All of none holds, and any of none does not.
            Cond::All(cs) if cs.is_empty() => "true".to_string(),
            Cond::Any(cs) if cs.is_empty() => "false".to_string(),
            Cond::All(cs) => cs
                .iter()
                .map(|c| format!("{{ {}; }}", c.shell_where(plain)))
                .collect::<Vec<_>>()
                .join(" && "),
            Cond::Any(cs) => cs
                .iter()
                .map(|c| format!("{{ {}; }}", c.shell_where(plain)))
                .collect::<Vec<_>>()
                .join(" || "),
            Cond::Unread(_) => plain.to_string(),
        }
    }

    /// The same condition, without what holds, or does not, whatever the
    /// build: `return true`, or an `allOf` of nothing.
    fn simplified(self) -> Cond {
        match self {
            Cond::Not(c) => match c.simplified() {
                Cond::Lit(holds) => Cond::Lit(!holds),
                c => Cond::Not(Box::new(c)),
            },
            Cond::All(cs) => {
                let mut kept = Vec::new();
                for c in cs {
                    match c.simplified() {
                        Cond::Lit(true) => {}
                        Cond::Lit(false) => return Cond::Lit(false),
                        c => kept.push(c),
                    }
                }
                match kept.len() {
                    0 => Cond::Lit(true),
                    1 => kept.remove(0),
                    _ => Cond::All(kept),
                }
            }
            Cond::Any(cs) => {
                let mut kept = Vec::new();
                for c in cs {
                    match c.simplified() {
                        Cond::Lit(false) => {}
                        Cond::Lit(true) => return Cond::Lit(true),
                        c => kept.push(c),
                    }
                }
                match kept.len() {
                    0 => Cond::Lit(false),
                    1 => kept.remove(0),
                    _ => Cond::Any(kept),
                }
            }
            other => other,
        }
    }

    /// As a `when:` of dagron's, where it can be one exactly: of the event's
    /// kind and its branch or tag, `and` within `or`.
    fn dagron(&self) -> Option<String> {
        let event = |kind: &str, equal: bool| {
            format!(
                "{} {} {kind}",
                tpl("ci_event"),
                if equal { "==" } else { "!=" }
            )
        };
        match self {
            Cond::Branch(Pattern::Exact(name)) if group_holds(name) && !name.starts_with("PR-") => {
                Some(format!(
                    "{} in [refs/heads/{name}, refs/tags/{name}]",
                    tpl("ci_group")
                ))
            }
            Cond::Tag(None) => Some(event("tag", true)),
            Cond::Tag(Some(Pattern::Exact(name))) if group_holds(name) => {
                Some(format!("{} == refs/tags/{name}", tpl("ci_group")))
            }
            Cond::Change(None) => Some(event("pull_request", true)),
            Cond::Not(inner) => match inner.as_ref() {
                Cond::Branch(Pattern::Exact(name))
                    if group_holds(name) && !name.starts_with("PR-") =>
                {
                    Some(format!(
                        "{} not in [refs/heads/{name}, refs/tags/{name}]",
                        tpl("ci_group")
                    ))
                }
                Cond::Tag(None) => Some(event("tag", false)),
                Cond::Change(None) => Some(event("pull_request", false)),
                _ => None,
            },
            Cond::All(cs) if !cs.is_empty() => {
                let parts: Option<Vec<String>> = cs.iter().map(Cond::dagron).collect();
                let parts = parts?;
                (!parts.iter().any(|p| p.contains(" or "))).then(|| parts.join(" and "))
            }
            Cond::Any(cs) if !cs.is_empty() => {
                let parts: Option<Vec<String>> = cs.iter().map(Cond::dagron).collect();
                Some(parts?.join(" or "))
            }
            _ => None,
        }
    }

    /// What of it could not be read.
    fn unread(&self, out: &mut Vec<String>) {
        match self {
            Cond::Unread(what) => out.push(what.clone()),
            Cond::Not(c) => c.unread(out),
            Cond::All(cs) | Cond::Any(cs) => cs.iter().for_each(|c| c.unread(out)),
            _ => {}
        }
    }

    /// Whether it reads `*` in a branch's or a tag's name, which a shell's
    /// pattern reads across a `/` too.
    fn globs(&self) -> bool {
        match self {
            Cond::Branch(Pattern::Glob(g))
            | Cond::Tag(Some(Pattern::Glob(g)))
            | Cond::Change(Some(Pattern::Glob(g))) => g.contains('*'),
            Cond::Not(c) => c.globs(),
            Cond::All(cs) | Cond::Any(cs) => cs.iter().any(Cond::globs),
            _ => false,
        }
    }
}

fn arg<'a>(args: &'a [Arg], name: &str) -> Option<&'a Expr> {
    args.iter().find_map(|a| match a {
        Arg::Named(k, v) if k == name => Some(v),
        _ => None,
    })
}

fn first(args: &[Arg]) -> Option<&Expr> {
    args.iter().find_map(|a| match a {
        Arg::Pos(v) => Some(v),
        Arg::Named(..) => None,
    })
}

/// The text of a statement on one line, short enough to say.
fn shown(text: &str) -> String {
    let line = one_line(text);
    match line.chars().count() > 80 {
        true => format!("{}…", line.chars().take(80).collect::<String>()),
        false => line,
    }
}

/// A value as the file wrote it, near enough to say: a string with what it
/// interpolates.
fn shown_expr(value: &Expr) -> String {
    match value {
        Expr::Str(s) => {
            let mut out = String::from("\"");
            for part in &s.parts {
                match part {
                    Part::Text(text) => out.push_str(text),
                    Part::Expr(expr) => out.push_str(&format!("${{{expr}}}")),
                }
            }
            out.push('"');
            shown(&out)
        }
        Expr::Other(text) => shown(text),
        Expr::Path(path) => path.clone(),
        other => other
            .text()
            .map(|text| shown(&text))
            .unwrap_or_else(|| "a value of Groovy's".to_string()),
    }
}

/// A timeout's `time` and `unit` (minutes unless said), in seconds.
fn timeout_secs(args: &[Arg]) -> Option<u64> {
    let time: u64 = arg(args, "time")
        .or_else(|| first(args))?
        .text()?
        .parse()
        .ok()?;
    let unit = match arg(args, "unit").and_then(Expr::text) {
        None => 60,
        Some(unit) => match unit.to_ascii_uppercase().as_str() {
            "SECONDS" => 1,
            "MINUTES" => 60,
            "HOURS" => 3600,
            "DAYS" => 86_400,
            "MILLISECONDS" => return Some(time.div_ceil(1000).max(1)),
            _ => return None,
        },
    };
    time.checked_mul(unit).filter(|s| *s > 0)
}

struct Importer<'a> {
    options: &'a Options,
    report: Report,
    tasks: Vec<Task>,
    /// What every job of the pipeline is given.
    env: Vec<(String, String)>,
    secrets: Vec<(String, String)>,
    params: Vec<String>,
    attempts: Option<u64>,
    skip_checkout: bool,
    /// The job each `stash` was made in, by its name.
    stashes: BTreeMap<String, usize>,
    /// The parameters that are booleans (`booleanParam`).
    booleans: BTreeSet<String>,
    /// The jobs' variables that hold a stage's secrets: for each, the
    /// variable the stage named and the secret.
    held: BTreeMap<String, (String, String)>,
}

impl<'a> Importer<'a> {
    /// The job's variable that holds `secret` for a stage's `var`: a name
    /// of its own, so that two stages that give one variable two secrets
    /// have both, each where it was given.
    fn held(&mut self, var: &str, secret: &str) -> String {
        let wanted = format!("{var}__{secret}");
        let mut name = wanted.clone();
        let mut n = 2;
        loop {
            let taken = self
                .env
                .iter()
                .chain(&self.secrets)
                .any(|(k, _)| *k == name)
                || self.params.contains(&name);
            match self.held.get(&name) {
                None if !taken => {
                    self.held
                        .insert(name.clone(), (var.to_string(), secret.to_string()));
                    return name;
                }
                Some((v, s)) if v == var && s == secret => return name,
                _ => {
                    name = format!("{wanted}_{n}");
                    n += 1;
                }
            }
        }
    }

    fn task(&mut self, name: &str, at: &str, scope: &Scope, deps: Vec<usize>) -> usize {
        let mut wanted = as_name(name);
        if wanted.is_empty() {
            wanted = "stage".to_string();
        }
        let mut name = wanted.clone();
        let mut n = 2;
        while self.tasks.iter().any(|t| t.name == name) {
            // A job's name is at most 100 characters, the suffix with it.
            let suffix = format!("-{n}");
            let kept: String = wanted.chars().take(100 - suffix.len()).collect();
            name = format!("{}{suffix}", kept.trim_end_matches('-'));
            n += 1;
        }
        let mut task = Task {
            name,
            at: at.to_string(),
            fields: Vec::new(),
            steps: Vec::new(),
            secrets: Vec::new(),
            todo: Vec::new(),
            stands: false,
            stood: Vec::new(),
            deps,
            junit: Vec::new(),
            axes: Vec::new(),
            single: None,
            checks_out: false,
            skip_checkout: self.skip_checkout,
            env: scope.env.clone(),
        };
        if let Some(image) = &scope.image {
            task.set("docker_image", Value::String(image.clone()));
        }
        if scope.pull_always {
            task.set("image_pull_policy", key("always"));
        }
        if scope.daemon {
            task.set("docker_daemon", Value::Bool(true));
        }
        for (place, text) in &scope.todo {
            self.report.todo(&mut task.todo, place, text.clone());
        }
        for (place, text) in &scope.left {
            self.report.note(place, Kind::Left, text.clone());
        }
        self.tasks.push(task);
        self.tasks.len() - 1
    }

    /// The job the next stage of this agent's adds its steps to: the open
    /// one, or a new one after what came before.
    /// `split`: the stages of the agent before it were a job of their own,
    /// whose workspace this one does not share.
    fn open(
        &mut self,
        cur: &mut Cursor,
        name: &str,
        at: &str,
        scope: &Scope,
        split: bool,
    ) -> usize {
        if let Some(open) = cur.open {
            return open;
        }
        let made = self.task(name, at, scope, cur.frontier.clone());
        if split && !cur.frontier.is_empty() {
            self.report.note(at, Kind::Changed, WORKSPACE_OF_ITS_OWN);
        }
        cur.open = Some(made);
        cur.frontier = vec![made];
        made
    }

    /// An agent, as the scope a stage's steps run in.
    fn agent(&mut self, stmt: &Stmt, at: &str, axes: &[String]) -> Scope {
        let at = format!("{at}.agent");
        let mut scope = Scope::default();
        let (args, block) = match stmt {
            Stmt::Call { args, block, .. } => (args.as_slice(), block.as_ref()),
            other => {
                scope.todo.push((
                    at,
                    format!(
                        "`{}` is not an agent that is read here: name the job's docker_image",
                        shown(other.text())
                    ),
                ));
                return scope;
            }
        };
        let label_image = |this: &Self, label: &str, scope: &mut Scope, at: &str| {
            match this.options.images.get(label) {
            Some(image) => scope.image = Some(image.clone()),
            None => scope.todo.push((at.to_string(), format!("no image is known for the agent {label}: name the job's docker_image (an image that holds what its steps use), or give the importer one (--image {label}=IMAGE)"))),
        }
        };
        if let Some(Expr::Path(word)) = first(args) {
            match word.as_str() {
                "none" => scope.none = true,
                "any" => label_image(self, "any", &mut scope, &at),
                other => scope.todo.push((at, format!("`agent {other}` is not an agent that is read here: name the job's docker_image"))),
            }
            return scope;
        }
        if let Some(label) = arg(args, "label")
            .or_else(|| first(args))
            .and_then(Expr::text)
        {
            label_image(self, &label, &mut scope, &at);
            return scope;
        }
        let Some(block) = block else {
            scope.todo.push((
                at,
                "an agent that names nothing: name the job's docker_image".to_string(),
            ));
            return scope;
        };
        for stmt in &block.stmts {
            let Stmt::Call {
                name,
                args,
                block,
                text,
            } = stmt
            else {
                scope.todo.push((
                    at.clone(),
                    format!(
                        "`{}` is not read: name the job's docker_image",
                        shown(stmt.text())
                    ),
                ));
                continue;
            };
            let place = format!("{at}.{name}");
            match name.as_str() {
                "label" => match first(args).and_then(Expr::text) {
                    Some(label) => label_image(self, &label, &mut scope, &place),
                    None => scope.todo.push((place, "the label is not plain text: name the job's docker_image".into())),
                },
                "node" => {
                    let label = block.as_ref().and_then(|b| {
                        b.stmts.iter().find_map(|s| match s {
                            Stmt::Call { name, args, .. } if name == "label" => first(args).and_then(Expr::text),
                            _ => None,
                        })
                    });
                    match label {
                        Some(label) => label_image(self, &label, &mut scope, &place),
                        None => scope.todo.push((place.clone(), "a node with no label: name the job's docker_image".into())),
                    }
                    if block.as_ref().is_some_and(|b| b.stmts.iter().any(|s| matches!(s, Stmt::Call { name, .. } if name == "customWorkspace"))) {
                        scope.left.push((format!("{place}.customWorkspace"), "a job's workspace is a directory dagron makes for it".into()));
                    }
                }
                "docker" => {
                    let mut image = first(args).cloned();
                    let mut docker_args: Option<String> = None;
                    if let Some(block) = block {
                        for s in &block.stmts {
                            let Stmt::Call { name: key, args, .. } = s else {
                                scope.todo.push((place.clone(), format!("`{}` is not read", shown(s.text()))));
                                continue;
                            };
                            let at = format!("{place}.{key}");
                            match key.as_str() {
                                "image" => image = first(args).cloned(),
                                "args" => match first(args).map(|a| (a, a.text())) {
                                    Some((_, Some(text))) => docker_args = Some(text),
                                    Some((a, None)) => scope.todo.push((at, format!("what Docker was told ({}) interpolates Groovy, which is not run here, and is not read: give the job what it needs another way (its env, its image)", shown_expr(a)))),
                                    None => {}
                                },
                                "alwaysPull" => scope.pull_always = first(args) == Some(&Expr::Bool(true)),
                                "reuseNode" | "label" | "customWorkspace" => scope.left.push((at, "where the container runs is the engine's to say".into())),
                                "registryUrl" | "registryCredentialsId" => scope.todo.push((at, "a registry's credentials are the engine's (its image pull secret), and not a job's: name the image in full, and have the operator give the engine what pulls it".into())),
                                _ => scope.todo.push((at, "a docker agent has no such setting here".into())),
                            }
                        }
                    }
                    match image.as_ref().map(|i| self.image(i, axes)) {
                        Some(Ok(image)) => scope.image = Some(image),
                        Some(Err(why)) => scope.todo.push((format!("{place}.image"), why)),
                        None => scope.todo.push((place.clone(), "a docker agent that names no image".into())),
                    }
                    if let Some(docker_args) = docker_args {
                        self.docker_args(&docker_args, &format!("{place}.args"), &mut scope);
                    }
                }
                "dockerfile" => scope.todo.push((place, "an image built from the repository's Dockerfile is not built here: build it and push it, and name it as the job's docker_image".into())),
                "kubernetes" => scope.todo.push((place, "a pod of a Kubernetes agent is not read: name the job's docker_image, and give it services for the pod's other containers".into())),
                _ => scope.todo.push((place, format!("`{}` is not an agent that is read here: name the job's docker_image", shown(text)))),
            }
        }
        scope
    }

    /// A docker agent's `args`: a Docker socket is a daemon of the job's
    /// own; a user is the image's; the rest is the engine's to say.
    fn docker_args(&mut self, text: &str, at: &str, scope: &mut Scope) {
        let words: Vec<&str> = text.split_whitespace().collect();
        let mut i = 0;
        let mut left = Vec::new();
        while i < words.len() {
            let word = words[i];
            let value = words.get(i + 1).copied().unwrap_or_default();
            match word {
                "-v" | "--volume" if value.contains("docker.sock") => {
                    scope.daemon = true;
                    scope.left.push((at.to_string(), "the host's Docker socket is a Docker daemon of the job's own here (docker_daemon), where the engine's operator offers one; a run that is not trusted is refused it".into()));
                    i += 2;
                }
                "-e" | "--env"
                    if value
                        .split_once('=')
                        .is_some_and(|(n, _)| is_shell_name(n) && !n.starts_with("CI_")) =>
                {
                    let (name, set) = value.split_once('=').unwrap_or_default();
                    scope.env.push((name.to_string(), set.to_string()));
                    scope.left.push((
                        at.to_string(),
                        "the variables the agent's container was given are the job's".into(),
                    ));
                    i += 2;
                }
                "-u" | "--user" | "-e" | "--env" | "-v" | "--volume" | "--network" | "--net"
                | "-w" | "--workdir" | "--entrypoint" | "--group-add" => {
                    left.push(format!("{word} {value}"));
                    i += 2;
                }
                other => {
                    left.push(other.to_string());
                    i += 1;
                }
            }
        }
        if !left.is_empty() {
            scope.todo.push((at.to_string(), format!("what Docker was told ({}) is the engine's to say here, and not a job's: give the job what it needs another way (its env, its image)", left.join(" "))));
        }
    }

    /// An image's name: as written, or with its matrix's axes as the item's.
    fn image(&mut self, image: &Expr, axes: &[String]) -> Result<String, String> {
        let Expr::Str(s) = image else {
            return image
                .text()
                .ok_or_else(|| "the image is not plain text: name it".to_string());
        };
        let mut out = String::new();
        for part in &s.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Expr(expr) => {
                    let name = expr.strip_prefix("env.").unwrap_or(expr);
                    if axes.iter().any(|a| a == name) {
                        out.push_str(&tpl(&format!("item.{name}")));
                    } else {
                        return Err(format!("the image reads `{expr}`, which is not known before the job runs: name it in full"));
                    }
                }
            }
        }
        Ok(out)
    }

    /// The Groovy of a string as the shell text it would have been: what it
    /// interpolates is a variable of the shell's.
    fn shell_text(&mut self, task: usize, at: &str, s: &GStr, ctx: &Ctx) -> String {
        let mut out = String::new();
        let mut filled = false;
        let mut quoted_in = false;
        for part in &s.parts {
            match part {
                Part::Text(text) => out.push_str(text),
                Part::Expr(expr) => {
                    let name = expr
                        .strip_prefix("env.")
                        .or_else(|| expr.strip_prefix("params."))
                        .unwrap_or(expr);
                    let named = is_shell_name(name)
                        && (expr.starts_with("env.")
                            || expr.starts_with("params.")
                            || ctx.defined.contains(name)
                            || self.params.iter().any(|p| p == name)
                            || name
                                .chars()
                                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'));
                    quoted_in = quoted_in || in_single_quotes(&out);
                    if named {
                        filled = true;
                        out.push_str(&format!("${{{name}}}"));
                    } else {
                        let said: String = expr
                            .chars()
                            .map(|c| {
                                if c.is_ascii_alphanumeric() || "._-".contains(c) {
                                    c
                                } else {
                                    ' '
                                }
                            })
                            .collect();
                        out.push_str(&format!(
                            "${{IMPORT_TODO:?not imported, {}}}",
                            one_line(&said)
                        ));
                        let text = format!("`{expr}` is Groovy that the step's text held, and is not run here: the step stops where it is read. Write what it stood for in the script");
                        let todo = &mut self.tasks[task].todo;
                        self.report.todo(todo, at, text);
                    }
                }
            }
        }
        if filled {
            self.report.note(at, Kind::Changed, "what the string interpolated is a variable of the shell's here, which the shell fills in as it runs the script");
        }
        if quoted_in {
            let todo = &mut self.tasks[task].todo;
            self.report.todo(todo, at, "an interpolation stood inside the shell's single quotes, where the shell fills nothing in: the variable in its place is read there as it is written. Close the quotes around it");
        }
        if filled && has_quoted_heredoc(&out) {
            let todo = &mut self.tasks[task].todo;
            self.report.todo(todo, at, "the script has a here-document whose mark is quoted, in which the shell fills nothing in: a variable that stands for what Groovy interpolated there is read as it is written. Unquote the mark, or write the value another way");
        }
        out
    }

    /// A string as a shell word in double quotes, what it interpolates as
    /// the shell's variables. `None`: it interpolates Groovy.
    fn word(&self, s: &GStr, ctx: &Ctx) -> Option<String> {
        let mut out = String::from("\"");
        for part in &s.parts {
            match part {
                Part::Text(text) => {
                    for c in text.chars() {
                        if matches!(c, '"' | '\\' | '`' | '$') {
                            out.push('\\');
                        }
                        out.push(c);
                    }
                }
                Part::Expr(expr) => {
                    let name = expr
                        .strip_prefix("env.")
                        .or_else(|| expr.strip_prefix("params."))
                        .unwrap_or(expr);
                    let named = is_shell_name(name)
                        && (expr.starts_with("env.")
                            || expr.starts_with("params.")
                            || ctx.defined.contains(name)
                            || name
                                .chars()
                                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'));
                    if !named {
                        return None;
                    }
                    out.push_str(&format!("${{{name}}}"));
                }
            }
        }
        out.push('"');
        Some(out)
    }

    /// A step, added to a job: its script with what it reads of Jenkins's
    /// variables set at its head, the stage's test, and its directory.
    /// What is left to do of the stages around a step, said by its job.
    fn pending(&mut self, task: usize, ctx: &Ctx) {
        for (at, text) in &ctx.todo {
            let line = format!("{}: {}", said(at), said(text));
            if !self.tasks[task].todo.contains(&line) {
                let todo = &mut self.tasks[task].todo;
                self.report.todo(todo, at, text.clone());
            }
        }
    }

    fn step(&mut self, task: usize, ctx: &Ctx, at: &str, name: Option<&str>, body: &str) {
        self.pending(task, ctx);
        let mut head: Vec<String> = Vec::new();
        let mut text = String::new();
        for (_, line) in &ctx.exports {
            text.push_str(line);
            text.push('\n');
        }
        for guard in &ctx.guards {
            text.push_str(guard);
            text.push('\n');
        }
        let reads: BTreeSet<String> = variables(&format!("{text}{body}"))
            .into_iter()
            .map(str::to_string)
            .collect();
        let mut set = Vec::new();
        for (var, line) in DERIVED {
            if reads.contains(*var) && !ctx.defined.contains(*var) {
                head.push(line.to_string());
                set.push(format!("${var}"));
            }
        }
        if reads.contains("STAGE_NAME") && !ctx.defined.contains("STAGE_NAME") {
            head.push(format!("export STAGE_NAME={}", quoted(&ctx.stage)));
            set.push("$STAGE_NAME".into());
        }
        if !set.is_empty() {
            self.report.note(
                at,
                Kind::Changed,
                format!(
                    "it reads {}: each is set at the head of its script",
                    set.join(", ")
                ),
            );
        }
        // Said of every pipeline, as a program a step runs may read one of
        // them where the step's own text reads none.
        self.report.said_once(at, Kind::Changed, "Jenkins's variables a step reads are set at its head from dagron-ci's own (CI_SHA, CI_REF, …): a program it runs that reads one the step does not is not given it");
        if reads
            .iter()
            .any(|r| r == "BRANCH_NAME" || r == "GIT_BRANCH")
            && !ctx.defined.contains("BRANCH_NAME")
        {
            self.report.said_once(at, Kind::Changed, "$BRANCH_NAME is a branch's or a tag's name, and PR-<number> for a pull request, as a multibranch pipeline has it");
        }
        let not_given: Vec<String> = reads
            .iter()
            .filter(|r| NOT_GIVEN.contains(&r.as_str()) && !ctx.defined.contains(r.as_str()))
            .map(|r| format!("${r}"))
            .collect();
        if !not_given.is_empty() {
            let todo = &mut self.tasks[task].todo;
            self.report.todo(
                todo,
                at,
                format!(
                    "the step reads {}, which a step here is not given",
                    not_given.join(", ")
                ),
            );
        }
        let credentials: Vec<String> = ctx
            .credentials
            .iter()
            .cloned()
            .chain(self.secrets.iter().map(|(v, _)| v.clone()))
            .collect();
        for var in &credentials {
            for suffix in ["_USR", "_PSW"] {
                let wanted = format!("{var}{suffix}");
                if reads.contains(&wanted) {
                    let todo = &mut self.tasks[task].todo;
                    self.report.todo(todo, at, format!("the step reads ${wanted}, which Jenkins gave a username and password's credentials: a secret here is one value, so keep each in a secret of its own, and give it to the job in its env"));
                }
            }
        }
        let mut script = head.join("\n");
        if !script.is_empty() {
            script.push('\n');
        }
        script.push_str(&text);
        if let Some(dir) = &ctx.dir {
            script.push_str(&format!("mkdir -p {0} && cd {0}\n", quoted(dir)));
        }
        script.push_str(body);
        let mut step = Mapping::new();
        if let Some(name) = name {
            step.insert(
                key("name"),
                Value::String(step_name(name).chars().take(100).collect()),
            );
        }
        if !ctx.env.is_empty() {
            let env: Mapping = ctx
                .env
                .iter()
                .map(|(k, v)| (key(k), Value::String(v.clone())))
                .collect();
            step.insert(key("env"), Value::Mapping(env));
        }
        step.insert(
            key("run"),
            Value::String(script.trim_end_matches('\n').to_string()),
        );
        if let Some(secs) = ctx.timeout {
            step.insert(key("timeout_secs"), Value::Number(secs.into()));
        }
        if ctx.continue_on_error {
            step.insert(key("continue_on_error"), Value::Bool(true));
        }
        let task = &mut self.tasks[task];
        for secret in &ctx.secrets {
            if !task.secrets.contains(secret) {
                task.secrets.push(secret.clone());
            }
        }
        task.steps.push(Value::Mapping(step));
    }

    /// A step that fails and says why, in the place of one not imported.
    fn stand(&mut self, task: usize, at: &str, name: &str, text: &str) {
        let line = format!("{}: {}", one_line(at), one_line(text));
        self.report.note(at, Kind::Todo, text.to_string());
        let task = &mut self.tasks[task];
        task.stands = true;
        task.stood.push(task.steps.len());
        task.steps.push(failing_step(&step_name(name), &[line]));
    }

    /// `environment { … }`, into what a stage's steps are given.
    fn environment(&mut self, at: &str, block: &Block, ctx: &mut Ctx, pipeline: bool) {
        // What is wrong with the pipeline's is every job's; with a stage's,
        // its own.
        let mut problems: Vec<(String, String)> = Vec::new();
        let mut problem = |at: &str, text: String| problems.push((at.to_string(), text));
        for stmt in &block.stmts {
            let Stmt::Assign { name, value, .. } = stmt else {
                problem(
                    at,
                    format!(
                        "`{}` is not a variable: it is not imported",
                        shown(stmt.text())
                    ),
                );
                continue;
            };
            let place = format!("{at}.{name}");
            if !is_shell_name(name) || name.starts_with("CI_") {
                let why = match name.starts_with("CI_") {
                    true => "a variable that begins CI_ is set from the event here: give it another name",
                    false => "the variable's name is not one a shell's variable may have: it is left out",
                };
                problem(&place, why.to_string());
                continue;
            }
            // A value of the stage's own is the one its steps read: what the
            // stages around it set the variable to at a step's head is not.
            let plain = |ctx: &mut Ctx, text: String| {
                ctx.env.retain(|(k, _)| k != name);
                ctx.exports.retain(|(k, _)| k != name);
                ctx.env.push((name.clone(), text));
                ctx.defined.insert(name.clone());
            };
            match value {
                Expr::Call { name: call, args, .. } if call == "credentials" => match first(args).and_then(Expr::text) {
                    Some(id) => {
                        let secret = as_secret(&id);
                        let said = format!("credentials('{id}') is the secret {secret} of the job's environment: where it was a username and password, it is one value here, and where it was a file, the job is given what the file holds and not a path to it");
                        ctx.defined.insert(name.clone());
                        match pipeline {
                            // The pipeline's are every job's, by their own
                            // names.
                            true => {
                                ctx.secrets.push((name.clone(), secret));
                                self.report.note(&place, Kind::Changed, said);
                            }
                            // A stage's are its steps' alone on Jenkins: the
                            // job holds each by a name of its own, and the
                            // stage's steps set the stage's name from it.
                            false => {
                                let held = self.held(name, &secret);
                                ctx.secrets.push((held.clone(), secret));
                                ctx.env.retain(|(k, _)| k != name);
                                ctx.exports.retain(|(k, _)| k != name);
                                ctx.exports.push((name.clone(), format!("export {name}=\"${held}\"")));
                                if !ctx.credentials.contains(name) {
                                    ctx.credentials.push(name.clone());
                                }
                                self.report.note(&place, Kind::Changed, format!("{said}. The job holds it as ${held}, which each of the stage's steps sets ${name} from at its head: every step of the job has it under that name, where Jenkins gave it to the stage's steps alone"));
                            }
                        }
                    }
                    None => problem(&place, "credentials() that names no plain id: name the secret".into()),
                },
                Expr::Str(s) => match s.plain() {
                    Some(text) => plain(ctx, text),
                    None => match self.word(s, ctx) {
                        Some(word) => {
                            ctx.exports.push((name.clone(), format!("export {name}={word}")));
                            ctx.defined.insert(name.clone());
                            self.report.note(&place, Kind::Changed, "its value names other variables: it is set at the head of each step, where the shell fills them in as Groovy did");
                        }
                        None => problem(&place, "its value interpolates Groovy, which is not run here: it is left out. Set it in a step's script".into()),
                    },
                },
                Expr::Num(_) | Expr::Bool(_) => {
                    let text = value.text().unwrap_or_default();
                    plain(ctx, text);
                }
                other => {
                    let shown_value = match other {
                        Expr::Other(text) => shown(text),
                        _ => "a value of Groovy's".to_string(),
                    };
                    problem(&place, format!("its value, {shown_value}, is Groovy, which is not run here: it is left out. Set it in a step's script"));
                }
            }
        }
        match pipeline {
            true => problems
                .into_iter()
                .for_each(|(at, text)| self.report.todo_all(&at, text)),
            false => ctx.todo.extend(problems),
        }
        if pipeline {
            self.env = ctx.env.clone();
            self.secrets = ctx.secrets.clone();
            // A job's own variables are dagron's, and a step's are not
            // written again.
            ctx.env.clear();
            ctx.secrets.clear();
        }
    }

    /// A `when`'s conditions, all of which hold.
    fn when(&mut self, at: &str, block: &Block) -> Cond {
        let mut all = Vec::new();
        for stmt in &block.stmts {
            if let Some(cond) = self.cond(at, stmt) {
                all.push(cond);
            }
        }
        Cond::All(all).simplified()
    }

    /// A `when` as the test at the head of each step it holds over, and
    /// what is said of it. `stage`: a stage's, where the alternative is a
    /// matrix's, which each instance tests.
    fn guard(&mut self, at: &str, when: &Block, cond: &Cond, ctx: &mut Ctx, stage: bool) {
        let mut unread = Vec::new();
        cond.unread(&mut unread);
        ctx.guards.push(format!(
            "# when {{ {} }}\n{{ {}; }} || exit 0",
            shown(&when.text),
            cond.shell()
        ));
        self.report.note(at, Kind::Changed, match stage {
            true => "its when is a test at the head of each of its steps, which end there where it is false: the job passes, where Jenkins skipped the stage",
            false => "its when is a test at the head of each step of an instance, which ends there where it is false",
        });
        if cond.globs() {
            self.report.note(
                at,
                Kind::Changed,
                "a `*` in a branch's or a tag's pattern is a shell's here, which takes a `/` too",
            );
        }
        if !unread.is_empty() {
            ctx.todo.push((at.to_string(), format!("{} cannot be read before the stage runs: it is taken to hold, or not, whichever runs the stage, and the stage runs where it might have, which is more than it did. Test it in the stage's steps", unread.join(", "))));
        }
    }

    fn pattern(args: &[Arg]) -> Option<Result<Pattern, String>> {
        let pattern = arg(args, "pattern").or_else(|| first(args))?;
        Some(Self::compared(pattern, args))
    }

    /// A pattern, as its `comparator` reads it: a glob unless said.
    fn compared(pattern: &Expr, args: &[Arg]) -> Result<Pattern, String> {
        let Some(text) = pattern.text() else {
            return Err("the pattern is not plain text".into());
        };
        let comparator = match arg(args, "comparator") {
            None => "GLOB".to_string(),
            Some(given) => given
                .text()
                .ok_or_else(|| "the comparator is not plain text".to_string())?,
        };
        match comparator.as_str() {
            "EQUALS" => Ok(Pattern::Exact(text)),
            "GLOB" if !text.contains(['*', '?']) => Ok(Pattern::Exact(text)),
            "GLOB" => Ok(Pattern::Glob(text)),
            other => Err(format!("a {other} comparator is not read")),
        }
    }

    fn cond(&mut self, at: &str, stmt: &Stmt) -> Option<Cond> {
        let Stmt::Call {
            name,
            args,
            block,
            text,
        } = stmt
        else {
            return Some(Cond::Unread(shown(stmt.text())));
        };
        let unread = || Cond::Unread(shown(text));
        let nested = |this: &mut Self, block: &Option<Block>| -> Vec<Cond> {
            block
                .as_ref()
                .map(|b| b.stmts.iter().filter_map(|s| this.cond(at, s)).collect())
                .unwrap_or_default()
        };
        Some(match name.as_str() {
            "branch" => match Self::pattern(args) {
                Some(Ok(p)) => Cond::Branch(p),
                _ => unread(),
            },
            "tag" => match Self::pattern(args) {
                None => Cond::Tag(None),
                Some(Ok(p)) => Cond::Tag(Some(p)),
                Some(Err(_)) => unread(),
            },
            "buildingTag" => Cond::Tag(None),
            "changeRequest" => {
                let mut target = None;
                for a in args {
                    match a {
                        Arg::Named(k, v) if k == "target" => match Self::compared(v, args) {
                            Ok(p) => target = Some(p),
                            Err(_) => return Some(unread()),
                        },
                        Arg::Named(k, _) if k == "comparator" => {}
                        _ => return Some(unread()),
                    }
                }
                Cond::Change(target)
            }
            "environment" => match (
                arg(args, "name").and_then(Expr::text),
                arg(args, "value").and_then(Expr::text),
            ) {
                (Some(n), Some(v)) if is_shell_name(&n) => Cond::Env(n, v),
                _ => unread(),
            },
            "expression" => match block {
                Some(b) => expression(&b.text, &self.booleans).unwrap_or_else(unread),
                None => unread(),
            },
            "not" => match nested(self, block).as_slice() {
                [one] => Cond::Not(Box::new(one.clone())),
                _ => unread(),
            },
            "allOf" => Cond::All(nested(self, block)),
            "anyOf" => Cond::Any(nested(self, block)),
            "beforeAgent" | "beforeOptions" => {
                self.report.note(
                    &format!("{at}.{name}"),
                    Kind::Left,
                    "a job's image is had before its steps, and its test is its first line",
                );
                return None;
            }
            "beforeInput" if first(args) == Some(&Expr::Bool(false)) => return None,
            "beforeInput" => {
                self.report.note(
                    &format!("{at}.{name}"),
                    Kind::Left,
                    "a gate before the stage is given its when where it can be said, and asks only where the when holds, as beforeInput true had it",
                );
                return None;
            }
            "changeset" | "changelog" => {
                Cond::Unread(format!("{name}, which reads what the build changed"))
            }
            "triggeredBy" => Cond::Unread(format!(
                "{}, which reads what started the build",
                shown(text)
            )),
            _ => unread(),
        })
    }

    /// A list of steps, into a job, with what the stage gives them.
    #[allow(clippy::too_many_lines)]
    fn steps(&mut self, task: usize, at: &str, block: &Block, ctx: &Ctx) {
        for (n, stmt) in block.stmts.iter().enumerate() {
            let at = format!("{at}[{n}]");
            let stage = ctx.stage.clone();
            self.pending(task, ctx);
            let Stmt::Call {
                name,
                args,
                block: inner,
                text,
            } = stmt
            else {
                self.stand(
                    task,
                    &at,
                    &stage,
                    &format!(
                        "`{}` is Groovy, which is not run here: write what it does as a step",
                        shown(stmt.text())
                    ),
                );
                continue;
            };
            match name.as_str() {
                "sh" => {
                    let script = arg(args, "script").or_else(|| first(args));
                    let label = arg(args, "label").and_then(Expr::text);
                    for k in ["returnStdout", "returnStatus", "encoding"] {
                        if arg(args, k).is_some() {
                            self.report.note(
                                &format!("{at}.{k}"),
                                Kind::Left,
                                "what a step printed or how it ended is not handed to Groovy here",
                            );
                        }
                    }
                    let Some(Expr::Str(s)) = script else {
                        self.stand(
                            task,
                            &at,
                            &stage,
                            "the script is not a string: write it out",
                        );
                        continue;
                    };
                    let body = self.shell_text(task, &at, s, ctx);
                    if !body.starts_with("#!") {
                        if let Some(seen) = bashism(&body) {
                            let todo = &mut self.tasks[task].todo;
                            self.report.todo(todo, &at, format!("the script is bash's ({seen}), and a step here is run by `sh -e`, as Jenkins's `sh` is: call bash in it, or write it for sh"));
                        }
                    }
                    let body = self.shebang(&at, &body);
                    // `returnStatus: true`: how the script ended was the
                    // pipeline's to read, and did not fail the stage.
                    let status = arg(args, "returnStatus") == Some(&Expr::Bool(true));
                    let mut step_ctx = ctx.clone();
                    if status {
                        step_ctx.continue_on_error = true;
                        self.report.note(
                            &format!("{at}.returnStatus"),
                            Kind::Changed,
                            "the script that fails does not fail the job (continue_on_error), as its status was returned to the pipeline and failed nothing",
                        );
                    }
                    self.step(
                        task,
                        &step_ctx,
                        &at,
                        Some(label.as_deref().unwrap_or(&stage)),
                        &body,
                    );
                }
                "echo" => {
                    match first(args) {
                        Some(Expr::Str(s)) => {
                            let body = match s.plain() {
                                Some(text) => format!("printf '%s\\n' {}", quoted(&text)),
                                None => match self.word(s, ctx) {
                                    Some(word) => format!("printf '%s\\n' {word}"),
                                    None => {
                                        self.stand(task, &at, &stage, "the message interpolates Groovy, which is not run here");
                                        continue;
                                    }
                                },
                            };
                            self.step(task, ctx, &at, Some(&stage), &body);
                        }
                        other => {
                            let said = other.and_then(Expr::text).unwrap_or_default();
                            self.step(
                                task,
                                ctx,
                                &at,
                                Some(&stage),
                                &format!("printf '%s\\n' {}", quoted(&said)),
                            );
                        }
                    }
                }
                "error" => {
                    let message = first(args)
                        .and_then(Expr::text)
                        .unwrap_or_else(|| "error".into());
                    self.step(
                        task,
                        ctx,
                        &at,
                        Some(&stage),
                        &format!("printf '%s\\n' {} >&2\nexit 1", quoted(&message)),
                    );
                }
                "sleep" => match arg(args, "time")
                    .or_else(|| first(args))
                    .and_then(Expr::text)
                {
                    Some(time) => {
                        let unit = arg(args, "unit")
                            .and_then(Expr::text)
                            .unwrap_or_else(|| "SECONDS".into());
                        let secs = time.parse::<u64>().ok().and_then(|t| match unit.as_str() {
                            "SECONDS" => Some(t),
                            "MINUTES" => t.checked_mul(60),
                            "HOURS" => t.checked_mul(3600),
                            _ => None,
                        });
                        match secs {
                            Some(secs) => {
                                self.step(task, ctx, &at, Some(&stage), &format!("sleep {secs}"))
                            }
                            None => self.stand(task, &at, &stage, "how long to sleep is not read"),
                        }
                    }
                    None => self.stand(task, &at, &stage, "how long to sleep is not read"),
                },
                "writeFile" => match (arg(args, "file").and_then(Expr::text), arg(args, "text")) {
                    (Some(file), Some(Expr::Str(s))) => {
                        let content = match s.plain() {
                            Some(text) => Some(quoted(&text)),
                            None => self.word(s, ctx),
                        };
                        match content {
                            Some(content) => self.step(
                                task,
                                ctx,
                                &at,
                                Some(&stage),
                                &format!("printf '%s' {content} > {}", quoted(&file)),
                            ),
                            None => self.stand(
                                task,
                                &at,
                                &stage,
                                "the file's text interpolates Groovy, which is not run here",
                            ),
                        }
                    }
                    _ => self.stand(task, &at, &stage, "the file and its text are not plain"),
                },
                "junit" => {
                    let paths = arg(args, "testResults")
                        .or_else(|| first(args))
                        .and_then(Expr::text);
                    self.junit(task, &at, paths);
                }
                "stash" => {
                    let stash = arg(args, "name")
                        .or_else(|| first(args))
                        .and_then(Expr::text)
                        .unwrap_or_default();
                    self.stashes.insert(stash, task);
                    self.report.note(&at, Kind::Left, "a stash is not kept: what a later stage of the same job takes of it is in the job's workspace already");
                }
                "unstash" => {
                    let stash = first(args)
                        .or_else(|| arg(args, "name"))
                        .and_then(Expr::text)
                        .unwrap_or_default();
                    match self.stashes.get(&stash) {
                        Some(from) if *from == task => self.report.note(&at, Kind::Left, "what was stashed is in the job's workspace already: its stash was made by a stage of the same job"),
                        Some(from) => {
                            let from = self.tasks[*from].name.clone();
                            let todo = &mut self.tasks[task].todo;
                            self.report.todo(todo, &at, format!("the files stashed as {stash:?} in the job {from} are not handed to this one: a job's files are not kept. Make the stages that share them steps of one job"));
                        }
                        None => {
                            let todo = &mut self.tasks[task].todo;
                            self.report.todo(todo, &at, format!("no stage before it stashed {stash:?}: what it took is not here"));
                        }
                    }
                }
                "deleteDir" => {
                    self.step(task, ctx, &at, Some(&stage), "find . -mindepth 1 -delete")
                }
                "dir" => {
                    let Some(path) = first(args).and_then(Expr::text) else {
                        self.stand(task, &at, &stage, "the directory is not plain text");
                        continue;
                    };
                    let mut inner_ctx = ctx.clone();
                    inner_ctx.dir = Some(match &ctx.dir {
                        Some(outer) => format!("{outer}/{path}"),
                        None => path,
                    });
                    if let Some(b) = inner {
                        self.steps(task, &at, b, &inner_ctx);
                    }
                }
                "withEnv" => {
                    let mut inner_ctx = ctx.clone();
                    let pairs = match first(args) {
                        Some(Expr::List(items)) => items.clone(),
                        _ => Vec::new(),
                    };
                    for pair in pairs {
                        let Expr::Str(s) = &pair else { continue };
                        let Some(Part::Text(head)) = s.parts.first() else {
                            continue;
                        };
                        let Some((name, _)) = head.split_once('=') else {
                            continue;
                        };
                        let name = name.to_string();
                        let rest = GStr {
                            parts: std::iter::once(Part::Text(head[name.len() + 1..].to_string()))
                                .chain(s.parts[1..].iter().cloned())
                                .collect(),
                        };
                        if !is_shell_name(&name) || name.starts_with("CI_") {
                            self.report.todo_all(&at, format!("the variable {name} is not one a step here is given: give it another name"));
                            continue;
                        }
                        match rest.plain() {
                            // The block's value is the one its steps read,
                            // and not what the stages around it set.
                            Some(value) => {
                                inner_ctx.env.retain(|(k, _)| *k != name);
                                inner_ctx.exports.retain(|(k, _)| *k != name);
                                inner_ctx.env.push((name.clone(), value));
                            }
                            None => match self.word(&rest, ctx) {
                                Some(word) => inner_ctx
                                    .exports
                                    .push((name.clone(), format!("export {name}={word}"))),
                                None => {
                                    let todo = &mut self.tasks[task].todo;
                                    self.report.todo(todo, &at, format!("{name}'s value interpolates Groovy, which is not run here"));
                                }
                            },
                        }
                        inner_ctx.defined.insert(name);
                    }
                    if let Some(b) = inner {
                        self.steps(task, &at, b, &inner_ctx);
                    }
                }
                "withCredentials" => {
                    let mut inner_ctx = ctx.clone();
                    let bindings = match first(args) {
                        Some(Expr::List(items)) => items.clone(),
                        _ => Vec::new(),
                    };
                    for binding in &bindings {
                        let Expr::Call {
                            name: kind,
                            args: b,
                            ..
                        } = binding
                        else {
                            continue;
                        };
                        let id = match arg(b, "credentialsId") {
                            Some(given) => match given.text() {
                                Some(id) => id,
                                None => {
                                    let todo = &mut self.tasks[task].todo;
                                    self.report.todo(todo, &at, format!("{kind}'s credentialsId, {}, is Groovy, which is not run here: the secret is not given. Name the secret it stood for", shown_expr(given)));
                                    continue;
                                }
                            },
                            None => {
                                let todo = &mut self.tasks[task].todo;
                                self.report.todo(
                                    todo,
                                    &at,
                                    format!(
                                        "{kind}() names no credentialsId: the secret is not given"
                                    ),
                                );
                                continue;
                            }
                        };
                        let var = |k: &str| arg(b, k).and_then(Expr::text);
                        let pairs: Vec<(String, String)> = match (
                            kind.as_str(),
                            var("variable"),
                            var("usernameVariable"),
                            var("passwordVariable"),
                        ) {
                            ("string" | "usernameColonPassword", Some(variable), ..) => {
                                vec![(variable, as_secret(&id))]
                            }
                            ("usernamePassword", _, Some(user), Some(password)) => {
                                self.report.note(&at, Kind::Changed, format!("the username and password of {id} are two secrets here, {} and {}", as_secret(&format!("{id}_username")), as_secret(&format!("{id}_password"))));
                                vec![
                                    (user, as_secret(&format!("{id}_username"))),
                                    (password, as_secret(&format!("{id}_password"))),
                                ]
                            }
                            (other, ..) => {
                                let todo = &mut self.tasks[task].todo;
                                self.report.todo(todo, &at, format!("{other}({id}) is not given: a secret here is a variable's value, and not a file or a key on disk. Write it to a file in a step, from a secret"));
                                continue;
                            }
                        };
                        for (variable, secret) in pairs {
                            if !is_shell_name(&variable) || variable.starts_with("CI_") {
                                let todo = &mut self.tasks[task].todo;
                                self.report.todo(todo, &at, format!("the variable {variable} is not one a step here is given: give the secret {secret} another name"));
                                continue;
                            }
                            // The block's steps alone had it on Jenkins: the
                            // job holds it by a name of its own, and they
                            // set the block's name from it.
                            let held = self.held(&variable, &secret);
                            inner_ctx.env.retain(|(k, _)| *k != variable);
                            inner_ctx.exports.retain(|(k, _)| *k != variable);
                            inner_ctx
                                .exports
                                .push((variable.clone(), format!("export {variable}=\"${held}\"")));
                            inner_ctx.secrets.push((held.clone(), secret.clone()));
                            self.report.note(&at, Kind::Changed, format!("the secret {secret} is held by the job as ${held}, which each step of the block sets ${variable} from at its head: every step of the job has it under that name, where Jenkins gave it to the block's steps alone"));
                            inner_ctx.defined.insert(variable);
                        }
                    }
                    if let Some(b) = inner {
                        self.steps(task, &at, b, &inner_ctx);
                    }
                }
                "timeout" => {
                    let mut inner_ctx = ctx.clone();
                    match timeout_secs(args) {
                        Some(secs) => {
                            inner_ctx.timeout = Some(secs);
                            self.report.note(&at, Kind::Changed, "the time is each step's of the block, where Jenkins gave it to the block as a whole");
                        }
                        None => {
                            let todo = &mut self.tasks[task].todo;
                            self.report.todo(
                                todo,
                                &at,
                                "the timeout is not read: say it in seconds (timeout_secs)",
                            );
                        }
                    }
                    if let Some(b) = inner {
                        self.steps(task, &at, b, &inner_ctx);
                    }
                }
                "retry" => {
                    let count = first(args)
                        .or_else(|| arg(args, "count"))
                        .and_then(Expr::text)
                        .and_then(|c| c.parse::<u64>().ok());
                    match count {
                        Some(count) if count > 1 => {
                            let attempts = self.tasks[task]
                                .fields
                                .iter()
                                .find(|(k, _)| *k == "max_attempts")
                                .and_then(|(_, v)| v.as_u64())
                                .unwrap_or(1);
                            self.tasks[task]
                                .set("max_attempts", Value::Number(attempts.max(count).into()));
                            self.report.note(&at, Kind::Changed, format!("the job is tried {count} times, as a whole, where Jenkins tried the block again"));
                        }
                        _ => {}
                    }
                    if let Some(b) = inner {
                        self.steps(task, &at, b, ctx);
                    }
                }
                "catchError" | "warnError" => {
                    // The block ends at the first of its steps that fails,
                    // and the stage goes on after it: its steps are one
                    // step here, which may fail.
                    let from = self.tasks[task].steps.len();
                    if let Some(b) = inner {
                        self.steps(task, &at, b, ctx);
                    }
                    self.caught(task, from, &stage);
                    self.report.note(&at, Kind::Changed, "the block is one step here, which ends at the first of its steps that fails, as the block did, and does not fail the job (continue_on_error): the job goes on after it, and is not marked for it");
                }
                "script" => {
                    // Steps alone in a script block are steps; Groovy is not.
                    let only_steps = inner.as_ref().is_some_and(|b| {
                        b.stmts.iter().all(|s| matches!(s, Stmt::Call { name, block: None, .. } if matches!(name.as_str(), "sh" | "echo")))
                    });
                    match (only_steps, inner) {
                        (true, Some(b)) => {
                            self.report.note(
                                &at,
                                Kind::Changed,
                                "the script block held steps alone, which are steps here",
                            );
                            self.steps(task, &at, b, ctx);
                        }
                        _ => {
                            let groovy = inner.as_ref().map(|b| shown(&b.text)).unwrap_or_default();
                            self.stand(task, &at, &format!("{stage}: script"), &format!("a script block is Groovy, which is not run here ({groovy}): write what it does as steps"));
                        }
                    }
                }
                "checkout" if first(args) == Some(&Expr::Path("scm".into())) => {
                    self.tasks[task].checks_out = true;
                    self.report.note(
                        &at,
                        Kind::Left,
                        "the job's workspace is a checkout of the event's commit already",
                    );
                }
                _ => {
                    if let Some((_, why)) = LEFT_STEPS.iter().find(|(n, _)| n == name) {
                        self.report.note(&at, Kind::Left, *why);
                        continue;
                    }
                    if let Some((_, why)) = PLUGIN_STEPS.iter().find(|(n, _)| n == name) {
                        self.stand(task, &at, &format!("{stage}: {name}"), why);
                        continue;
                    }
                    if PLUGIN_WRAPPERS.contains(&name.as_str()) {
                        self.stand(task, &at, &format!("{stage}: {name}"), &format!("{name} is a plugin's, which is not here: give the job what it set up another way. Its steps follow"));
                        if let Some(b) = inner {
                            self.steps(task, &at, b, ctx);
                        }
                        continue;
                    }
                    self.stand(task, &at, &format!("{stage}: {name}"), &format!("`{}` is a step that is not imported: write the commands it stood for in its place", shown(text)));
                }
            }
        }
    }

    /// The steps of a `catchError` block, made from `from` on, as one step
    /// that may fail: each runs in a subshell of its own, as it ran in a
    /// shell of its own, and the first that fails ends the block. A step
    /// that stands for one not imported stays a step of its own, and fails
    /// the job, as something is left to do there.
    fn caught(&mut self, task: usize, from: usize, stage: &str) {
        let made: Vec<Value> = self.tasks[task].steps.drain(from..).collect();
        let stood: Vec<usize> = self.tasks[task]
            .stood
            .iter()
            .filter(|n| **n >= from)
            .map(|n| n - from)
            .collect();
        self.tasks[task].stood.retain(|n| *n < from);
        let mut run: Vec<Mapping> = Vec::new();
        for (n, step) in made.into_iter().enumerate() {
            let Value::Mapping(step) = step else {
                continue;
            };
            if stood.contains(&n) {
                self.joined(task, std::mem::take(&mut run), stage);
                let at = self.tasks[task].steps.len();
                self.tasks[task].stood.push(at);
                self.tasks[task].steps.push(Value::Mapping(step));
            } else {
                run.push(step);
            }
        }
        self.joined(task, run, stage);
    }

    /// Steps of a `catchError` block, written as one that may fail.
    fn joined(&mut self, task: usize, steps: Vec<Mapping>, stage: &str) {
        let mut steps = steps;
        if steps.len() <= 1 {
            if let Some(mut step) = steps.pop() {
                step.insert(key("continue_on_error"), Value::Bool(true));
                self.tasks[task].steps.push(Value::Mapping(step));
            }
            return;
        }
        let text = |step: &Mapping, name: &str| get(step, name).and_then(scalar);
        let mut script = String::new();
        let mut timeout: Option<u64> = Some(0);
        for step in &steps {
            let mut part = String::new();
            if let Some(Value::Mapping(env)) = get(step, "env") {
                for (k, v) in env {
                    if let (Some(k), Some(v)) = (scalar(k), scalar(v)) {
                        part.push_str(&format!("export {k}={}\n", quoted(&v)));
                    }
                }
            }
            part.push_str(&text(step, "run").unwrap_or_default());
            // One that may fail fails none of the block's steps after it.
            match get(step, "continue_on_error") == Some(&Value::Bool(true)) {
                true => script.push_str(&format!("set +e\n(\nset -e\n{part}\n)\nset -e\n")),
                false => script.push_str(&format!("(\n{part}\n)\n")),
            }
            timeout = match (timeout, get(step, "timeout_secs").and_then(Value::as_u64)) {
                (Some(sum), Some(secs)) => sum.checked_add(secs),
                _ => None,
            };
        }
        let name = text(&steps[0], "name").unwrap_or_else(|| stage.to_string());
        let mut step = Mapping::new();
        step.insert(key("name"), Value::String(name));
        step.insert(
            key("run"),
            Value::String(script.trim_end_matches('\n').to_string()),
        );
        if let Some(secs) = timeout.filter(|s| *s > 0) {
            step.insert(key("timeout_secs"), Value::Number(secs.into()));
        }
        step.insert(key("continue_on_error"), Value::Bool(true));
        self.tasks[task].steps.push(Value::Mapping(step));
    }

    /// A script that names its interpreter (`#!/bin/bash`), as one that
    /// runs under it.
    fn shebang(&mut self, at: &str, body: &str) -> String {
        let Some(first) = body.lines().next().filter(|l| l.starts_with("#!")) else {
            return body.to_string();
        };
        let interpreter: Vec<&str> = first[2..].split_whitespace().collect();
        let command = match interpreter.as_slice() {
            ["/usr/bin/env", rest @ ..] => rest.to_vec(),
            [path, rest @ ..] => {
                let mut c = vec![path.rsplit('/').next().unwrap_or(path)];
                c.extend(rest);
                c
            }
            [] => return body.to_string(),
        };
        let rest: String = body.lines().skip(1).collect::<Vec<_>>().join("\n");
        self.report.note(
            at,
            Kind::Changed,
            format!(
                "the script named its interpreter: it is run as `{} -c` here",
                command.join(" ")
            ),
        );
        format!("{} -c {}", command.join(" "), quoted(&rest))
    }

    /// A test report, where it is written out.
    fn junit(&mut self, task: usize, at: &str, paths: Option<String>) {
        let paths: Vec<String> = paths
            .map(|p| {
                p.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let plain = !paths.is_empty()
            && paths.iter().all(|p| {
                !p.starts_with('/')
                    && p.len() <= 240
                    && p.bytes().all(|b| {
                        b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/')
                    })
                    && p.split('/').all(|c| !c.is_empty() && c != "." && c != "..")
            });
        let task = &mut self.tasks[task];
        if plain && task.junit.len() + paths.len() <= 8 {
            for p in paths {
                if !task.junit.contains(&p) {
                    task.junit.push(p);
                }
            }
            self.report.note(
                at,
                Kind::Changed,
                "the test report is the job's report: what it holds is said with the job",
            );
        } else {
            self.report.note(at, Kind::Left, "a report here is up to 8 paths of the workspace, written out: a pattern is not read, and the job's tests are not shown");
        }
    }

    /// A stage's directives.
    fn read_stage<'s>(&mut self, stmt: &'s Stmt, parent: &str) -> Option<Stage<'s>> {
        let Stmt::Call {
            name,
            args,
            block: Some(block),
            ..
        } = stmt
        else {
            return None;
        };
        if name != "stage" {
            return None;
        }
        let stage_name = first(args)
            .and_then(Expr::text)
            .unwrap_or_else(|| "stage".into());
        let at = match parent {
            "" => format!("stage('{stage_name}')"),
            parent => format!("{parent}.stage('{stage_name}')"),
        };
        let mut stage = Stage {
            name: stage_name,
            at,
            ..Stage::default()
        };
        for s in &block.stmts {
            let (name, args, block) = match s {
                Stmt::Call {
                    name, args, block, ..
                } => (name.as_str(), args, block.as_ref()),
                _ => {
                    stage.others.push(s);
                    continue;
                }
            };
            match (name, block) {
                ("agent", _) => stage.agent = Some(s),
                ("environment", Some(b)) => stage.environment = Some(b),
                ("options", Some(b)) => stage.options = Some(b),
                ("when", Some(b)) => stage.when = Some(b),
                ("input", Some(b)) => stage.input = Some(b),
                ("tools", Some(b)) => stage.tools = Some(b),
                ("steps", Some(b)) => stage.steps = Some(b),
                ("stages", Some(b)) => stage.stages = Some(b),
                ("parallel", Some(b)) => stage.parallel = Some(b),
                ("matrix", Some(b)) => stage.matrix = Some(b),
                ("post", Some(b)) => stage.post = Some(b),
                ("failFast", _) => stage.fail_fast = first(args) != Some(&Expr::Bool(false)),
                _ => stage.others.push(s),
            }
        }
        Some(stage)
    }

    /// A list of stages, one after another.
    fn sequence(
        &mut self,
        block: &Block,
        parent: &str,
        scope: &Scope,
        ctx: &Ctx,
        mut cur: Cursor,
    ) -> Cursor {
        for stmt in &block.stmts {
            match self.read_stage(stmt, parent) {
                Some(stage) => cur = self.stage(&stage, scope, ctx, cur),
                None => self.report.todo_all(
                    parent,
                    format!("`{}` is no stage: it is not imported", shown(stmt.text())),
                ),
            }
        }
        cur
    }

    /// A stage: its steps into the job its agent's stages share, or into a
    /// job of its own.
    #[allow(clippy::too_many_lines)]
    fn stage(&mut self, stage: &Stage<'_>, scope: &Scope, outer: &Ctx, mut cur: Cursor) -> Cursor {
        let at = stage.at.clone();
        let mut ctx = outer.clone();
        ctx.stage = stage.name.clone();
        ctx.at = at.clone();
        ctx.dir = None;
        if let Some(env) = stage.environment {
            self.environment(&format!("{at}.environment"), env, &mut ctx, false);
        }
        for other in &stage.others {
            ctx.todo.push((
                at.clone(),
                format!(
                    "`{}` is not a stage's directive here: it is not imported",
                    shown(other.text())
                ),
            ));
        }
        if let Some(tools) = stage.tools {
            self.tools(&format!("{at}.tools"), tools);
        }
        // Its `when`: the test its steps start with, and what a gate before
        // it is given. One that holds whatever the build is no test.
        let mut tested = None;
        if let Some(when) = stage.when {
            let cond = self.when(&format!("{at}.when"), when);
            if !matches!(cond, Cond::Lit(true)) {
                self.guard(&format!("{at}.when"), when, &cond, &mut ctx, true);
                tested = Some(cond);
            }
        }
        // Its gate.
        if let Some(input) = stage.input {
            let gate = self.input(
                &format!("{at}.input"),
                input,
                &stage.name,
                cur.frontier.clone(),
            );
            if let Some(cond) = &tested {
                match cond.dagron() {
                    Some(when) => {
                        self.tasks[gate].set("when", Value::String(when));
                        // Jenkins asked before it tested the `when`, unless
                        // told `beforeInput true`.
                        let before_input = stage.when.is_some_and(|w| {
                            w.stmts.iter().any(|s| matches!(s, Stmt::Call { name, args, .. } if name == "beforeInput" && first(args) != Some(&Expr::Bool(false))))
                        });
                        if !before_input {
                            self.report.note(&format!("{at}.input"), Kind::Changed, "the gate asks only where the stage's when holds, where Jenkins asked first and tested the when after (as it does unless told beforeInput true)");
                        }
                    }
                    None => {
                        let todo = &mut self.tasks[gate].todo;
                        self.report.todo(todo, &format!("{at}.input"), "the stage's when cannot be said as the gate's: the gate asks on every run, where Jenkins asked where the stage ran. Write its when:");
                    }
                }
            }
            // The jobs after the gate wait for what it waited for too: where
            // its when is false, the gate is left out of the run, and they
            // would wait for nothing.
            let mut frontier = cur.frontier.clone();
            frontier.push(gate);
            cur = Cursor {
                frontier,
                open: None,
            };
        }
        // Its options.
        let mut stage_timeout = None;
        let mut stage_retry = None;
        let mut skip_checkout = false;
        if let Some(options) = stage.options {
            for s in &options.stmts {
                let Stmt::Call { name, args, .. } = s else {
                    continue;
                };
                let place = format!("{at}.options.{name}");
                match name.as_str() {
                    "timeout" => match timeout_secs(args) {
                        Some(secs) => stage_timeout = Some(secs),
                        None => ctx.todo.push((
                            place,
                            "the timeout is not read: say it in seconds (timeout_secs)".into(),
                        )),
                    },
                    "retry" => {
                        stage_retry = first(args)
                            .and_then(Expr::text)
                            .and_then(|c| c.parse::<u64>().ok())
                    }
                    "skipDefaultCheckout" => {
                        skip_checkout = first(args) != Some(&Expr::Bool(false))
                    }
                    _ => self.report.note(
                        &place,
                        Kind::Left,
                        "a stage's option that has no place here",
                    ),
                }
            }
        }
        // Where its steps run: an agent of its own is a job of its own, made
        // as its first step is.
        let own = stage.agent.map(|a| self.agent(a, &at, &[]));
        let own_job = own.as_ref().is_some_and(|o| !o.none);
        let scope = own.unwrap_or_else(|| scope.clone());
        let made_from = self.tasks.len();
        let mut inner = match stage.agent {
            Some(_) => Cursor {
                frontier: cur.frontier.clone(),
                open: None,
            },
            None => cur.clone(),
        };
        // A job of the agent's that was split from the one before it.
        let split = !own_job && cur.open.is_none() && !cur.frontier.is_empty();
        if stage_timeout.is_some() && !own_job {
            ctx.timeout = stage_timeout;
            self.report.note(
                &format!("{at}.options.timeout"),
                Kind::Changed,
                "the stage's time is each of its steps' here",
            );
        }
        if let Some(steps) = stage.steps {
            let task = self.open(&mut inner, &stage.name, &at, &scope, split);
            if scope.none {
                let todo = &mut self.tasks[task].todo;
                self.report.todo(
                    todo,
                    &at,
                    "the stage runs its steps with no agent: name the job's docker_image",
                );
            }
            self.steps(task, &format!("{at}.steps"), steps, &ctx);
            if steps.stmts.is_empty() {
                self.report
                    .note(&at, Kind::Left, "a stage with no step runs nothing");
            }
        } else if let Some(stages) = stage.stages {
            inner = self.sequence(stages, &at, &scope, &ctx, inner);
        } else if let Some(parallel) = stage.parallel {
            if stage.fail_fast {
                self.report.note(&format!("{at}.failFast"), Kind::Left, "a parallel stage that fails does not stop the others here: one that is a job of its own runs to its end, and the jobs after them are skipped");
            }
            inner = self.parallel(parallel, &at, &scope, &ctx, inner);
        } else if let Some(matrix) = stage.matrix {
            inner = self.matrix(matrix, &stage.name, &at, &scope, &ctx, inner);
        } else {
            self.report.todo_all(
                &at,
                "a stage with no steps, stages, parallel or matrix runs nothing",
            );
        }
        if let Some(post) = stage.post {
            match inner.open {
                Some(task) => self.stage_post(task, &format!("{at}.post"), post, &ctx),
                None => self.report.todo_all(&format!("{at}.post"), "the stage's post is not imported: its stages are jobs of their own, and what runs after them is a job of its own"),
            }
        }
        // What the stage's options say of the job it is.
        if let Some(task) = inner.open.or(inner.frontier.first().copied()) {
            if own_job && task >= made_from {
                if let Some(secs) = stage_timeout {
                    self.tasks[task].set("timeout_secs", Value::Number(secs.into()));
                }
                if skip_checkout {
                    self.tasks[task].skip_checkout = true;
                }
            }
            if let Some(count) = stage_retry.filter(|c| *c > 1) {
                self.tasks[task].set("max_attempts", Value::Number(count.into()));
                if !own_job {
                    self.report.note(&format!("{at}.options.retry"), Kind::Changed, format!("the job is tried {count} times, as a whole, where Jenkins tried the stage again"));
                }
            }
        }
        if skip_checkout && !own_job {
            self.report.note(
                &format!("{at}.options.skipDefaultCheckout"),
                Kind::Left,
                "the stage shares its job's workspace, which is a checkout",
            );
        }
        match own_job || scope.none {
            // A stage with an agent of its own leaves the agent's job of the
            // stages around it: the next starts a job of its own.
            true => Cursor {
                frontier: inner.frontier,
                open: None,
            },
            false => inner,
        }
    }

    /// A stage's `input`: a gate before it.
    fn input(&mut self, at: &str, block: &Block, stage: &str, deps: Vec<usize>) -> usize {
        let scope = Scope::default();
        let gate = self.task(&format!("approve-{stage}"), at, &scope, deps);
        self.tasks[gate].set("type", key("approval"));
        let mut message = format!("Run {stage}?");
        for s in &block.stmts {
            let Stmt::Call { name, args, .. } = s else {
                continue;
            };
            let place = format!("{at}.{name}");
            match name.as_str() {
                "message" => {
                    if let Some(text) = first(args).and_then(Expr::text) {
                        message = text;
                    }
                }
                "submitter" => {
                    let todo = &mut self.tasks[gate].todo;
                    self.report.todo(todo, &place, "who may answer is named here by an email, or `group:<name>` (approvers): write the gate's approvers");
                }
                "parameters" => {
                    let todo = &mut self.tasks[gate].todo;
                    self.report.todo(todo, &place, "a gate here asks yes or no, and takes no values: what was asked for is a dispatch's input, or a step's");
                }
                _ => self.report.note(
                    &place,
                    Kind::Left,
                    "a gate here asks its question and takes a yes or a no",
                ),
            }
        }
        self.tasks[gate].set("approval_message", Value::String(one_line(&message)));
        self.report.note(at, Kind::Changed, "the stage's input is a gate before it (type: approval), which a person approves or rejects");
        gate
    }

    /// Parallel stages: those of the agent the stage runs under are steps
    /// of its job, one after another; those with agents of their own are
    /// jobs of their own, side by side.
    fn parallel(
        &mut self,
        block: &Block,
        at: &str,
        scope: &Scope,
        ctx: &Ctx,
        cur: Cursor,
    ) -> Cursor {
        let mut frontier = Vec::new();
        let mut shared = Cursor {
            frontier: cur.frontier.clone(),
            open: cur.open,
        };
        let mut flattened = Vec::new();
        let mut own = false;
        for stmt in &block.stmts {
            let Some(stage) = self.read_stage(stmt, at) else {
                self.report.todo_all(
                    at,
                    format!("`{}` is no stage: it is not imported", shown(stmt.text())),
                );
                continue;
            };
            let has_own = stage.agent.is_some() || stage.matrix.is_some();
            if has_own || scope.none {
                own = true;
                let ended = self.stage(
                    &stage,
                    scope,
                    ctx,
                    Cursor {
                        frontier: cur.frontier.clone(),
                        open: None,
                    },
                );
                frontier.extend(ended.frontier);
            } else {
                flattened.push(stage.name.clone());
                shared = self.stage(&stage, scope, ctx, shared);
            }
        }
        if flattened.len() > 1 {
            self.report.note(at, Kind::Changed, format!("the parallel stages {} run one after another here, as steps of one job, so that each has the files of the stages before it; one that fails ends the job. A stage that needs none of them can be a job of its own", flattened.join(", ")));
        }
        if let Some(open) = shared.open {
            if !frontier.contains(&open) {
                frontier.insert(0, open);
            }
        }
        match own {
            true => Cursor {
                frontier,
                open: None,
            },
            false => shared,
        }
    }

    /// A matrix: one job of as many instances as its axes make.
    #[allow(clippy::too_many_arguments)]
    fn matrix(
        &mut self,
        block: &Block,
        name: &str,
        at: &str,
        scope: &Scope,
        ctx: &Ctx,
        cur: Cursor,
    ) -> Cursor {
        let at = format!("{at}.matrix");
        let mut axes: Vec<(String, Vec<String>)> = Vec::new();
        let mut excludes: Vec<Vec<(String, bool, Vec<String>)>> = Vec::new();
        let mut agent = None;
        let mut stages = None;
        let mut when = None;
        let mut post = None;
        let mut environment = None;
        // An axis: its name and its values, or the values it is not.
        let read_axis = |b: &Block| -> Option<(String, bool, Vec<String>)> {
            let mut name = None;
            let mut values = Vec::new();
            let mut not = false;
            for s in &b.stmts {
                let Stmt::Call { name: k, args, .. } = s else {
                    return None;
                };
                match k.as_str() {
                    "name" => name = first(args).and_then(Expr::text),
                    "values" | "notValues" => {
                        not = k == "notValues";
                        values = args
                            .iter()
                            .filter_map(|a| match a {
                                Arg::Pos(v) => v.text(),
                                Arg::Named(..) => None,
                            })
                            .collect();
                    }
                    _ => return None,
                }
            }
            Some((name?, not, values))
        };
        for s in &block.stmts {
            let Stmt::Call {
                name: k, block: b, ..
            } = s
            else {
                self.report
                    .todo_all(&at, format!("`{}` is not read", shown(s.text())));
                continue;
            };
            match (k.as_str(), b) {
                ("axes", Some(b)) => {
                    for a in &b.stmts {
                        match a {
                            Stmt::Call {
                                name,
                                block: Some(ab),
                                ..
                            } if name == "axis" => match read_axis(ab) {
                                Some((n, false, v)) if !v.is_empty() => axes.push((n, v)),
                                _ => self.report.todo_all(
                                    &format!("{at}.axes"),
                                    "an axis that is not a name and its values is not read",
                                ),
                            },
                            other => self.report.todo_all(
                                &format!("{at}.axes"),
                                format!("`{}` is no axis", shown(other.text())),
                            ),
                        }
                    }
                }
                ("excludes", Some(b)) => {
                    for e in &b.stmts {
                        let Stmt::Call {
                            block: Some(eb), ..
                        } = e
                        else {
                            continue;
                        };
                        let mut one = Vec::new();
                        for a in &eb.stmts {
                            if let Stmt::Call {
                                block: Some(ab), ..
                            } = a
                            {
                                if let Some(axis) = read_axis(ab) {
                                    one.push(axis);
                                }
                            }
                        }
                        excludes.push(one);
                    }
                }
                ("agent", _) => agent = Some(s),
                ("stages", Some(b)) => stages = Some(b),
                ("when", Some(b)) => when = Some(b),
                ("post", Some(b)) => post = Some(b),
                ("environment", Some(b)) => environment = Some(b),
                _ => self.report.todo_all(
                    &format!("{at}.{k}"),
                    "a matrix's directive that is not imported",
                ),
            }
        }
        let names: Vec<String> = axes.iter().map(|(n, _)| n.clone()).collect();
        let mut combos: Vec<Vec<(String, String)>> = vec![Vec::new()];
        for (n, values) in &axes {
            combos = combos
                .iter()
                .flat_map(|c| {
                    values.iter().map(move |v| {
                        let mut next = c.clone();
                        next.push((n.clone(), v.clone()));
                        next
                    })
                })
                .collect();
        }
        combos.retain(|combo| {
            !excludes.iter().any(|exclude| {
                !exclude.is_empty()
                    && exclude.iter().all(|(axis, not, values)| {
                        let value = combo.iter().find(|(n, _)| n == axis).map(|(_, v)| v);
                        value.is_some_and(|v| values.contains(v) != *not)
                    })
            })
        });
        let scope = match agent {
            Some(a) => self.agent(a, &at, &names),
            None => scope.clone(),
        };
        let task = self.task(name, &at, &scope, cur.frontier.clone());
        // Of the agent the stages before it ran under, and a job of its own.
        if agent.is_none() && !cur.frontier.is_empty() {
            self.report.note(&at, Kind::Changed, WORKSPACE_OF_ITS_OWN);
        }
        if axes.is_empty() || combos.is_empty() || combos.len() > 256 {
            let todo = &mut self.tasks[task].todo;
            self.report.todo(
                todo,
                &at,
                "the matrix has no instance, or more than 256: it is not written out",
            );
        } else {
            let single = names.len() == 1;
            let items: Vec<Value> = combos
                .iter()
                .map(|combo| match single {
                    true => Value::String(combo[0].1.clone()),
                    false => Value::Mapping(
                        combo
                            .iter()
                            .map(|(k, v)| (key(k), Value::String(v.clone())))
                            .collect(),
                    ),
                })
                .collect();
            self.tasks[task].set("with_items", Value::Sequence(items));
            self.tasks[task].axes = names.clone();
            if single {
                self.tasks[task].single = names.first().cloned();
            }
            self.report.note(&at, Kind::Changed, format!("the matrix is one job of {} instances (with_items), each given its axes as variables", combos.len()));
        }
        let mut ctx = ctx.clone();
        for n in &names {
            ctx.defined.insert(n.clone());
        }
        if let Some(env) = environment {
            self.environment(&format!("{at}.environment"), env, &mut ctx, false);
        }
        if let Some(when) = when {
            let cond = self.when(&format!("{at}.when"), when);
            if !matches!(cond, Cond::Lit(true)) {
                self.guard(&format!("{at}.when"), when, &cond, &mut ctx, false);
            }
        }
        let mut inner = Cursor {
            frontier: vec![task],
            open: Some(task),
        };
        if let Some(stages) = stages {
            for stmt in &stages.stmts {
                let Some(stage) = self.read_stage(stmt, &at) else {
                    continue;
                };
                if stage.agent.is_some() || stage.parallel.is_some() || stage.matrix.is_some() {
                    let todo = &mut self.tasks[task].todo;
                    self.report.todo(todo, &stage.at, "a stage of a matrix is a step of the matrix's job here, and runs under its agent: its own agent, parallel or matrix is not read");
                }
                let mut flat = stage;
                flat.agent = None;
                flat.parallel = None;
                flat.matrix = None;
                inner = self.stage(&flat, &scope, &ctx, inner);
            }
        }
        if let Some(post) = post {
            self.stage_post(task, &format!("{at}.post"), post, &ctx);
        }
        Cursor {
            frontier: vec![task],
            open: None,
        }
    }

    /// `tools { … }`: what the job's image has to hold.
    fn tools(&mut self, at: &str, block: &Block) {
        for s in &block.stmts {
            if let Stmt::Call { name, args, .. } = s {
                let installed = first(args).and_then(Expr::text).unwrap_or_default();
                self.report.note(&format!("{at}.{name}"), Kind::Left, format!("a tool Jenkins installed ({name} {installed}) is not here: the job's image has to hold it"));
            }
        }
    }

    /// A stage's `post`: what runs after its steps, in its job.
    fn stage_post(&mut self, task: usize, at: &str, block: &Block, ctx: &Ctx) {
        for s in &block.stmts {
            let Stmt::Call {
                name,
                block: Some(b),
                ..
            } = s
            else {
                continue;
            };
            let place = format!("{at}.{name}");
            match name.as_str() {
                "always" | "cleanup" | "success" => {
                    if name != "success" && b.stmts.iter().any(|s| !matches!(s, Stmt::Call { name, .. } if name == "junit" || LEFT_STEPS.iter().any(|(l, _)| l == name))) {
                        self.report.note(&place, Kind::Changed, "what runs after the stage's steps runs where they succeeded, and not after one that failed: a step here runs only after the steps before it succeed");
                    }
                    // The stage's `when` holds over its post as over its
                    // steps: a stage Jenkins skipped ran no post.
                    self.steps(task, &place, b, ctx);
                }
                _ => {
                    let todo = &mut self.tasks[task].todo;
                    self.report.todo(todo, &place, format!("what ran on the stage's {name} is not here: a step runs after the steps before it succeed, and a job of trigger_rule: one_failed runs where one before it failed"));
                }
            }
        }
    }

    /// The pipeline's `post`: test reports for its jobs, and jobs of their
    /// own after the others.
    fn pipeline_post(&mut self, at: &str, block: &Block, scope: &Scope, ctx: &Ctx) {
        let leaves: Vec<usize> = (0..self.tasks.len())
            .filter(|n| !self.tasks.iter().any(|t| t.deps.contains(n)))
            .collect();
        let all: Vec<usize> = (0..self.tasks.len()).collect();
        for s in &block.stmts {
            let Stmt::Call {
                name,
                block: Some(b),
                ..
            } = s
            else {
                continue;
            };
            let place = format!("{at}.{name}");
            // Reports and what is left: said of the jobs, not run after them.
            let (reports, steps): (Vec<&Stmt>, Vec<&Stmt>) = b.stmts.iter().partition(|s| {
                matches!(s, Stmt::Call { name, .. } if name == "junit" || name == "cleanWs" || name == "deleteDir" || LEFT_STEPS.iter().any(|(l, _)| l == name))
            });
            for r in reports {
                let Stmt::Call {
                    name: step, args, ..
                } = r
                else {
                    continue;
                };
                if step == "junit" {
                    let last = (0..self.tasks.len())
                        .rev()
                        .find(|n| !self.tasks[*n].steps.is_empty());
                    if let Some(last) = last {
                        let paths = arg(args, "testResults")
                            .or_else(|| first(args))
                            .and_then(Expr::text);
                        self.junit(last, &place, paths);
                        if self.tasks.len() > 1 {
                            let named = self.tasks[last].name.clone();
                            self.report.note(&place, Kind::Changed, format!("the test report is read of the job {named}, the last: where another job writes it, move report: there"));
                        }
                    }
                } else if let Some((_, why)) = LEFT_STEPS.iter().find(|(l, _)| l == step) {
                    self.report.note(&place, Kind::Left, *why);
                } else {
                    self.report.note(
                        &place,
                        Kind::Left,
                        "a job's workspace goes with it, and is not cleaned",
                    );
                }
            }
            if steps.is_empty() {
                continue;
            }
            let (rule, deps, said) = match name.as_str() {
                "always" | "cleanup" => (
                    Some("all_done"),
                    leaves.clone(),
                    "whatever became of the jobs before it",
                ),
                "success" => (None, leaves.clone(), "where every job before it succeeded"),
                "failure" | "unsuccessful" => (
                    Some("one_failed"),
                    all.clone(),
                    "where a job before it failed",
                ),
                _ => {
                    self.report.todo_all(&place, format!("what ran on {name} is not here: a job runs after the others whatever became of them (trigger_rule: all_done), where one failed (one_failed), or where all succeeded"));
                    continue;
                }
            };
            let block = Block {
                stmts: steps.into_iter().cloned().collect(),
                text: b.text.clone(),
            };
            let task = self.task(&format!("post-{name}"), &place, scope, deps);
            if scope.none {
                let todo = &mut self.tasks[task].todo;
                self.report.todo(
                    todo,
                    &place,
                    "the pipeline's post ran on no agent: name the job's docker_image",
                );
            }
            if let Some(rule) = rule {
                self.tasks[task].set("trigger_rule", key(rule));
            }
            self.report.note(&place, Kind::Changed, format!("what ran on {name} is a job of its own, after the others, {said}, in a workspace of its own"));
            let mut post_ctx = ctx.clone();
            post_ctx.stage = format!("post {name}");
            self.steps(task, &place, &block, &post_ctx);
        }
    }

    /// `options { … }` of the pipeline.
    fn options(&mut self, block: &Block, out: &mut Mapping) {
        for s in &block.stmts {
            let Stmt::Call { name, args, .. } = s else {
                self.report.todo_all(
                    "options",
                    format!("`{}` is not an option that is read", shown(s.text())),
                );
                continue;
            };
            let at = format!("options.{name}");
            match name.as_str() {
                "timeout" => match timeout_secs(args) {
                    Some(secs) => {
                        out.insert(key("run_timeout_secs"), Value::Number(secs.into()));
                        self.report.note(&at, Kind::Changed, "the run's time is run_timeout_secs");
                    }
                    None => self.report.todo_all(&at, "the timeout is not read: say it in seconds (run_timeout_secs)"),
                },
                "retry" => {
                    if let Some(count) = first(args).and_then(Expr::text).and_then(|c| c.parse::<u64>().ok()).filter(|c| *c > 1) {
                        self.attempts = Some(count);
                        self.report.note(&at, Kind::Changed, format!("each job is tried {count} times, where Jenkins tried the pipeline again"));
                    }
                }
                "disableConcurrentBuilds" => {
                    let abort = arg(args, "abortPrevious") == Some(&Expr::Bool(true));
                    let mut concurrency = Mapping::new();
                    concurrency.insert(key("key"), Value::String(tpl("ci_group")));
                    if abort {
                        concurrency.insert(key("cancel_superseded"), Value::Bool(true));
                    }
                    out.insert(key("concurrency"), Value::Mapping(concurrency));
                    self.report.note(&at, Kind::Changed, match abort {
                        true => "a newer run of a branch or pull request takes the older one's place (concurrency)",
                        false => "a run of a branch or pull request waits for the one before it (concurrency)",
                    });
                }
                "skipDefaultCheckout" => {
                    if first(args) != Some(&Expr::Bool(false)) {
                        self.skip_checkout = true;
                    }
                }
                "checkoutToSubdirectory" => self.report.todo_all(&at, "the checkout is the workspace itself here: change into a directory in each step, or move the files"),
                _ => self.report.note(&at, Kind::Left, "an option of Jenkins's that has no place here"),
            }
        }
    }

    /// `parameters { … }`: a dispatch's inputs, and what every other run is
    /// given of each.
    fn parameters(&mut self, block: &Block, inputs: &mut Mapping, defaults: &mut Mapping) {
        for s in &block.stmts {
            let Stmt::Call {
                name: kind, args, ..
            } = s
            else {
                continue;
            };
            let Some(name) = arg(args, "name").and_then(Expr::text) else {
                self.report.todo_all(
                    "parameters",
                    format!("`{}` names no parameter", shown(s.text())),
                );
                continue;
            };
            let at = format!("parameters.{name}");
            let parameter = as_parameter(&name);
            if parameter.starts_with("ci_") || parameter.len() > 64 || parameter != name {
                self.report.todo_all(&at, "a parameter here is named as a shell's variable is, in at most 64 characters, and not beginning ci_: it is left out. Give it another name");
                continue;
            }
            let default = match kind.as_str() {
                "choice" => {
                    let choices: Vec<String> = match arg(args, "choices") {
                        Some(Expr::List(items)) => items.iter().filter_map(Expr::text).collect(),
                        Some(other) => other
                            .text()
                            .map(|t| t.lines().map(str::to_string).collect())
                            .unwrap_or_default(),
                        None => Vec::new(),
                    };
                    self.report.note(
                        &at,
                        Kind::Left,
                        "its choices are not checked here: a run asked for may send any value",
                    );
                    choices.into_iter().next().unwrap_or_default()
                }
                "password" => {
                    self.report.todo_all(
                        &at,
                        "a password is an environment's secret here, and not an input",
                    );
                    continue;
                }
                "string" | "text" | "booleanParam" => {
                    if kind == "booleanParam" {
                        self.booleans.insert(parameter.clone());
                    }
                    let unset = match kind.as_str() {
                        "booleanParam" => "false".to_string(),
                        _ => String::new(),
                    };
                    match arg(args, "defaultValue") {
                        None => unset,
                        Some(given) => match given.text() {
                            Some(text) => text,
                            None => {
                                self.report.todo_all(&at, format!("its default, {}, is Groovy, which is not run here: it is {unset:?} here. Write the default it stood for", shown_expr(given)));
                                unset
                            }
                        },
                    }
                }
                other => {
                    self.report
                        .todo_all(&at, format!("a {other} parameter is not read"));
                    continue;
                }
            };
            let mut input = Mapping::new();
            if let Some(description) = arg(args, "description")
                .and_then(Expr::text)
                .filter(|d| !d.is_empty())
            {
                input.insert(key("description"), Value::String(one_line(&description)));
            }
            input.insert(key("default"), Value::String(default.clone()));
            inputs.insert(key(&parameter), Value::Mapping(input));
            defaults.insert(key(&parameter), Value::String(default));
            self.params.push(parameter);
        }
        if !self.params.is_empty() {
            self.report.note("parameters", Kind::Changed, "a parameter is a dispatch's input, and every other run is given its default: each job is given it as a variable of the same name");
        }
    }
}

/// A plain `expression { … }` as a condition: a parameter or a variable,
/// compared with a value or alone, `true` and `false`, and `&&`, `||` and
/// `!` of those. Alone, a value holds as Groovy takes it: a boolean
/// parameter (`params.X` of a `booleanParam`) where it is `true`, and any
/// other, a string, where it is set and not empty.
fn expression(text: &str, booleans: &BTreeSet<String>) -> Option<Cond> {
    let text = text.trim();
    let text = text.strip_suffix(';').map(str::trim).unwrap_or(text);
    let text = text.strip_prefix("return ").map(str::trim).unwrap_or(text);
    if text.contains('(') || text.contains('\n') {
        return None;
    }
    if text.contains("||") {
        return text
            .split("||")
            .map(|part| expression(part, booleans))
            .collect::<Option<Vec<_>>>()
            .map(Cond::Any);
    }
    if text.contains("&&") {
        return text
            .split("&&")
            .map(|part| expression(part, booleans))
            .collect::<Option<Vec<_>>>()
            .map(Cond::All);
    }
    match text {
        "true" => return Some(Cond::Lit(true)),
        "false" => return Some(Cond::Lit(false)),
        _ => {}
    }
    let name_of = |side: &str| -> Option<String> {
        let side = side.trim();
        let name = side
            .strip_prefix("params.")
            .or_else(|| side.strip_prefix("env."))
            .unwrap_or(side);
        is_shell_name(name).then(|| name.to_string())
    };
    // A value alone: what Groovy takes as true of it.
    let truth = |side: &str| -> Option<Cond> {
        let side = side.trim();
        let name = name_of(side)?;
        Some(
            match side.starts_with("params.") && booleans.contains(&name) {
                true => Cond::Env(name, "true".into()),
                false => Cond::Set(name),
            },
        )
    };
    let value_of = |side: &str| -> Option<String> {
        let side = side.trim();
        match side {
            "true" | "false" => Some(side.to_string()),
            _ => side
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .or_else(|| side.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
                .filter(|s| !s.contains(['$', '\'', '"']))
                .map(str::to_string)
                .or_else(|| side.parse::<i64>().ok().map(|n| n.to_string())),
        }
    };
    if let Some((left, right)) = text.split_once("!=") {
        return Some(Cond::Not(Box::new(Cond::Env(
            name_of(left)?,
            value_of(right)?,
        ))));
    }
    if let Some((left, right)) = text.split_once("==") {
        if right.starts_with('~') {
            return None;
        }
        return Some(Cond::Env(name_of(left)?, value_of(right)?));
    }
    if let Some(inner) = text.strip_prefix('!') {
        return match inner.trim() {
            "true" => Some(Cond::Lit(false)),
            "false" => Some(Cond::Lit(true)),
            inner => Some(Cond::Not(Box::new(truth(inner)?))),
        };
    }
    truth(text)
}

/// Read a `Jenkinsfile` written as a Declarative Pipeline as a pipeline.
#[allow(clippy::too_many_lines)]
pub fn jenkins_to_pipeline(text: &str, options: &Options) -> Result<Imported> {
    // The two characters the importer's own templates are marked with are
    // no text a file means anything by ([`clean`]).
    let text = text.replace([OPEN, CLOSE], "");
    let stmts = groovy::parse(&text)
        .map_err(|e| anyhow!("the file is not Groovy a pipeline can be read from: {e}"))?;
    let mut pipeline: Option<&Block> = None;
    let mut outside = Vec::new();
    let mut scripted = false;
    for stmt in &stmts {
        match stmt {
            Stmt::Call {
                name,
                block: Some(block),
                ..
            } if name == "pipeline" && pipeline.is_none() => pipeline = Some(block),
            Stmt::Call { name, .. } if name == "node" => scripted = true,
            other => outside.push(other),
        }
    }
    let Some(pipeline) = pipeline else {
        if scripted {
            bail!("a Scripted Pipeline (`node {{ … }}`) is not imported: it is Groovy, run as a program, and a pipeline here is a file of jobs and steps. Only a Declarative Pipeline (`pipeline {{ … }}`) is; write it as one, or as a pipeline of .dagron/ci by hand");
        }
        bail!("the file has no `pipeline {{ … }}`: only a Declarative Pipeline is imported");
    };
    let mut importer = Importer {
        options,
        report: Report::default(),
        tasks: Vec::new(),
        env: Vec::new(),
        secrets: Vec::new(),
        params: Vec::new(),
        attempts: None,
        skip_checkout: false,
        stashes: BTreeMap::new(),
        booleans: BTreeSet::new(),
        held: BTreeMap::new(),
    };
    for other in outside {
        let text = other.text();
        let what = match text.trim_start().starts_with('@') || text.contains("library") {
            true => "a shared library is not here: what the pipeline calls of it is left to do where it is called",
            false => "Groovy outside the pipeline is not run here",
        };
        importer
            .report
            .todo_all("outside the pipeline", format!("`{}`: {what}", shown(text)));
    }

    // The pipeline's sections.
    let mut sections: BTreeMap<&str, &Stmt> = BTreeMap::new();
    for stmt in &pipeline.stmts {
        match stmt {
            Stmt::Call { name, .. }
                if matches!(
                    name.as_str(),
                    "agent"
                        | "environment"
                        | "options"
                        | "parameters"
                        | "triggers"
                        | "tools"
                        | "stages"
                        | "post"
                        | "libraries"
                ) =>
            {
                sections.insert(name.as_str(), stmt);
            }
            other => importer.report.todo_all(
                "pipeline",
                format!(
                    "`{}` is not a section of a pipeline that is imported",
                    shown(other.text())
                ),
            ),
        }
    }
    let block_of = |name: &str| match sections.get(name) {
        Some(Stmt::Call { block: Some(b), .. }) => Some(b),
        _ => None,
    };
    let mut head = Mapping::new();
    let mut on = Mapping::new();
    on.insert(key("push"), Value::Mapping(Mapping::new()));
    on.insert(key("pull_request"), Value::Mapping(Mapping::new()));
    let mut inputs = Mapping::new();
    let mut defaults = Mapping::new();
    if let Some(params) = block_of("parameters") {
        importer.parameters(params, &mut inputs, &mut defaults);
    }
    let mut dispatch = Mapping::new();
    if !inputs.is_empty() {
        dispatch.insert(key("inputs"), Value::Mapping(inputs));
    }
    on.insert(key("dispatch"), Value::Mapping(dispatch));
    importer.report.note("on", Kind::Changed, "a multibranch pipeline's builds are each branch and tag pushed, each pull request, and a run asked for (dispatch) here: say on: where it ran for fewer");
    if let Some(options) = block_of("options") {
        importer.options(options, &mut head);
    }
    if let Some(triggers) = block_of("triggers") {
        for s in &triggers.stmts {
            let Stmt::Call { name, .. } = s else { continue };
            let why = match name.as_str() {
                "cron" | "parameterizedCron" => "a pipeline has no schedule: ask for a run from a scheduler of your own, through the run API (on.dispatch)",
                "pollSCM" => "nothing to poll: dagron-ci looks at a connected repository itself, and takes a webhook",
                "upstream" => "a pipeline here is not started by another's end",
                _ => "a trigger of Jenkins's that has no place here",
            };
            importer
                .report
                .note(&format!("triggers.{name}"), Kind::Left, why);
        }
    }
    if let Some(libraries) = block_of("libraries") {
        importer.report.todo_all("libraries", format!("a shared library is not here ({}): what the pipeline calls of it is left to do where it is called", shown(&libraries.text)));
    }
    if let Some(tools) = block_of("tools") {
        importer.tools("tools", tools);
    }
    let mut ctx = Ctx::default();
    for p in importer.params.clone() {
        ctx.defined.insert(p);
    }
    if let Some(env) = block_of("environment") {
        importer.environment("environment", env, &mut ctx, true);
    }
    for (name, _) in importer.env.iter().chain(&importer.secrets) {
        ctx.defined.insert(name.clone());
    }
    let scope = match sections.get("agent") {
        Some(agent) => importer.agent(agent, "pipeline", &[]),
        None => {
            importer
                .report
                .todo_all("pipeline", "the pipeline names no agent");
            Scope::default()
        }
    };
    let Some(stages) = block_of("stages") else {
        bail!(
            "the pipeline has no `stages {{ … }}`: a Declarative Pipeline has at least one stage"
        );
    };
    let mut ctx_top = ctx.clone();
    ctx_top.at = "pipeline".into();
    importer.sequence(stages, "", &scope, &ctx_top, Cursor::default());
    if importer.tasks.is_empty() {
        bail!("no stage of the pipeline runs a step");
    }
    if let Some(post) = block_of("post") {
        importer.pipeline_post("post", post, &scope, &ctx_top);
    }
    if !importer.params.is_empty() {
        head.insert(key("parameters"), Value::Mapping(defaults));
    }

    // What is left to do of a gate is said by the jobs after it: a gate
    // runs no step to say it with. A brace of the file's in what it asks
    // is parted, as a job's is, and said so.
    for gate in 0..importer.tasks.len() {
        let is_gate = importer.tasks[gate]
            .fields
            .iter()
            .any(|(k, v)| *k == "type" && v.as_str() == Some("approval"));
        if !is_gate {
            continue;
        }
        let mut written = Value::Mapping(
            importer.tasks[gate]
                .fields
                .iter()
                .map(|(k, v)| (key(k), v.clone()))
                .collect(),
        );
        let mut parted = Vec::new();
        settle(&mut written, "", None, &mut parted);
        let at = importer.tasks[gate].at.clone();
        for place in parted {
            let todo = &mut importer.tasks[gate].todo;
            importer
                .report
                .todo(todo, &format!("{at}: {place}"), PARTED);
        }
        if importer.tasks[gate].todo.is_empty() {
            continue;
        }
        let lines = importer.tasks[gate].todo.clone();
        for task in importer.tasks.iter_mut().filter(|t| t.deps.contains(&gate)) {
            task.todo.extend(lines.iter().cloned());
        }
    }

    // The pipeline's own keys, written: a brace of the file's in them (a
    // parameter's default, say) is parted, and every job says so.
    let name = options
        .name
        .as_deref()
        .map(as_name)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "ci".to_string());
    let mut top = Mapping::new();
    top.insert(key("name"), Value::String(name));
    top.insert(key("on"), Value::Mapping(on));
    for (k, v) in head {
        top.insert(k, v);
    }
    let mut top = Value::Mapping(top);
    let mut parted = Vec::new();
    settle(&mut top, "", None, &mut parted);
    for place in parted {
        importer.report.todo_all(&place, PARTED);
    }

    // Each job, written.
    let names: Vec<String> = importer.tasks.iter().map(|t| t.name.clone()).collect();
    let mut tasks = Vec::new();
    let pipeline_env = importer.env.clone();
    let pipeline_secrets = importer.secrets.clone();
    let params = importer.params.clone();
    let attempts = importer.attempts;
    for n in 0..importer.tasks.len() {
        let mut task = std::mem::replace(
            &mut importer.tasks[n],
            Task {
                name: String::new(),
                at: String::new(),
                fields: Vec::new(),
                steps: Vec::new(),
                secrets: Vec::new(),
                todo: Vec::new(),
                stands: false,
                stood: Vec::new(),
                deps: Vec::new(),
                junit: Vec::new(),
                axes: Vec::new(),
                single: None,
                checks_out: false,
                skip_checkout: false,
                env: Vec::new(),
            },
        );
        if !task.deps.is_empty() {
            task.set(
                "depends_on",
                list(task.deps.iter().map(|d| names[*d].clone())),
            );
        }
        let mut env: Vec<Value> = Vec::new();
        let pair = |name: &str, value: Value, from: bool| {
            let mut pair = Mapping::new();
            pair.insert(key("name"), Value::String(name.to_string()));
            pair.insert(key(if from { "value_from" } else { "value" }), value);
            Value::Mapping(pair)
        };
        let is_gate = task
            .fields
            .iter()
            .any(|(k, v)| *k == "type" && v.as_str() == Some("approval"));
        if !is_gate {
            for (name, value) in pipeline_env.iter().chain(&task.env) {
                env.push(pair(name, Value::String(value.clone()), false));
            }
            for p in &params {
                env.push(pair(p, Value::String(tpl(p)), false));
            }
            for axis in &task.axes {
                if is_shell_name(axis) && !axis.starts_with("CI_") {
                    env.push(pair(
                        axis,
                        Value::String(tpl(&format!("item.{axis}"))),
                        false,
                    ));
                }
            }
            let mut secrets = pipeline_secrets.clone();
            for s in &task.secrets {
                if !secrets.contains(s) {
                    secrets.push(s.clone());
                }
            }
            for (var, secret) in &secrets {
                let mut from = Mapping::new();
                from.insert(key("secret"), Value::String(secret.clone()));
                env.push(pair(var, Value::Mapping(from), true));
            }
            if !secrets.is_empty() {
                let listed: Vec<String> = secrets
                    .iter()
                    .map(|(_, s)| s.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                // A stage's secrets are the job's: every step of the job can
                // read them, where Jenkins gave them to the stage alone, and
                // that is for someone to accept before the job runs.
                let wider: Vec<String> = secrets
                    .iter()
                    .filter(|(var, _)| importer.held.contains_key(var))
                    .map(|(var, _)| format!("${var}"))
                    .collect();
                let scope = match wider.is_empty() {
                    true => String::new(),
                    false => format!(", and accept that every step of the job can read {} (each a stage's secret, which Jenkins gave to that stage's steps alone), or give the stage a job of its own", wider.join(", ")),
                };
                let at = task.at.clone();
                importer.report.todo(&mut task.todo, &at, format!("the credentials it reads are an environment's secrets here ({}): name the environment that holds them (environment:){scope}", listed.join(", ")));
            }
            if task.skip_checkout && !task.checks_out {
                let mut workspace = Mapping::new();
                workspace.insert(key("checkout"), Value::Bool(false));
                task.set("workspace", Value::Mapping(workspace));
            }
            if let Some(count) = attempts {
                if !task.fields.iter().any(|(k, _)| *k == "max_attempts") {
                    task.set("max_attempts", Value::Number(count.into()));
                }
            }
            if !task.junit.is_empty() {
                let mut report = Mapping::new();
                report.insert(key("junit"), list(task.junit.clone()));
                task.set("report", Value::Mapping(report));
            }
            if task.steps.is_empty() {
                let at = task.at.clone();
                importer.report.todo(
                    &mut task.todo,
                    &at,
                    "the job has no step that runs a command",
                );
            }
        }
        if !env.is_empty() {
            task.set("env", Value::Sequence(env));
        }
        let mut out = Mapping::new();
        out.insert(key("name"), Value::String(task.name.clone()));
        let mut fields: Vec<&(&'static str, Value)> = task.fields.iter().collect();
        fields.sort_by_key(|(k, _)| ORDER.iter().position(|o| o == k).unwrap_or(ORDER.len()));
        for (k, v) in fields {
            out.insert(key(k), v.clone());
        }
        if !is_gate {
            out.insert(key("steps"), Value::Sequence(task.steps.clone()));
        }
        let at = task.at.clone();
        let todo = std::mem::take(&mut task.todo);
        let finished = match is_gate {
            // A gate runs nothing to say what is left with: it is said in
            // the report, and in the jobs after it.
            true => {
                let mut settled = Value::Mapping(out);
                settle(&mut settled, "", None, &mut Vec::new());
                settled
            }
            false => {
                importer
                    .report
                    .finish(&at, out, todo, task.stands, task.single.as_deref(), None)
            }
        };
        tasks.push(finished);
    }

    if let Value::Mapping(top) = &mut top {
        top.insert(key("tasks"), Value::Sequence(tasks));
    }
    let pipeline = serde_yaml::to_string(&top).map_err(|e| anyhow!("writing the pipeline: {e}"))?;
    Ok(Imported {
        source: "a Jenkinsfile",
        pipeline,
        notes: importer.report.into_notes(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn import(text: &str) -> Imported {
        jenkins_to_pipeline(text, &Options::default()).unwrap_or_else(|e| panic!("{e:#}"))
    }

    fn pipeline(imported: &Imported) -> Value {
        serde_yaml::from_str(&imported.pipeline).unwrap()
    }

    fn notes(imported: &Imported, kind: Kind) -> Vec<String> {
        imported
            .notes
            .iter()
            .filter(|n| n.kind == kind)
            .map(|n| format!("{}: {}", n.at, n.text))
            .collect()
    }

    #[test]
    fn a_scripted_pipeline_is_refused_by_name() {
        let refused = |text: &str| {
            jenkins_to_pipeline(text, &Options::default())
                .unwrap_err()
                .to_string()
        };
        let scripted = refused("node('linux') {\n  stage('Build') {\n    sh 'make'\n  }\n}\n");
        assert!(
            scripted.contains("a Scripted Pipeline (`node { … }`) is not imported"),
            "{scripted}"
        );
        assert!(refused("echo 'hi'\n").contains("has no `pipeline { … }`"));
        assert!(refused("pipeline {\n  agent any\n}\n").contains("no `stages { … }`"));
        assert!(
            refused("pipeline {\n  stages {\n").contains("not Groovy a pipeline can be read from")
        );
    }

    #[test]
    fn stages_of_one_agent_are_steps_of_one_job() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  stages {\n    stage('A') { steps { sh 'a' } }\n    stage('B') { steps { sh 'b' } }\n    stage('C') {\n      agent { docker { image 'busybox:1' } }\n      steps { sh 'c' }\n    }\n    stage('D') { steps { sh 'd' } }\n  }\n}\n";
        let imported = import(text);
        let read = pipeline(&imported);
        let tasks = read["tasks"].as_sequence().unwrap();
        let names: Vec<&str> = tasks.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["a", "c", "d"]);
        assert_eq!(tasks[0]["steps"].as_sequence().unwrap().len(), 2);
        assert_eq!(tasks[1]["docker_image"], "busybox:1");
        assert_eq!(
            serde_yaml::to_string(&tasks[2]["depends_on"]).unwrap(),
            "- c\n"
        );
        // D shared A's workspace on Jenkins, and starts in its own here.
        assert!(notes(&imported, Kind::Changed)
            .iter()
            .any(|n| n.starts_with("stage('D'): its stages start in a workspace of their own")));
        assert_eq!(imported.todo(), 0, "{:#?}", notes(&imported, Kind::Todo));
    }

    #[test]
    fn a_when_is_a_test_and_a_gate_s_when_is_dagron_s() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  stages {\n    stage('Ship') {\n      when { allOf { branch 'main'; not { buildingTag() } } }\n      input { message 'Ship it?' }\n      steps { sh './ship' }\n    }\n  }\n}\n";
        let read = pipeline(&import(text));
        let gate = &read["tasks"][0];
        assert_eq!(gate["type"], "approval");
        assert_eq!(gate["approval_message"], "Ship it?");
        assert_eq!(
            gate["when"],
            "{{ ci_group }} in [refs/heads/main, refs/tags/main] and {{ ci_event }} != tag"
        );
        let run = read["tasks"][1]["steps"][0]["run"].as_str().unwrap();
        assert!(run.contains("{ { case \"$BRANCH_NAME\" in 'main') true ;; *) false ;; esac; } && { ! { [ \"$CI_EVENT\" = tag ]; }; }; } || exit 0"), "{run}");
    }

    #[test]
    fn an_expression_is_read_where_it_is_plain() {
        let booleans: BTreeSet<String> = ["DEPLOY".to_string(), "SKIP".to_string()].into();
        let said = |text: &str| {
            expression(text, &booleans)
                .map(Cond::simplified)
                .map(|c| c.shell())
        };
        assert_eq!(
            said("return params.DEPLOY").as_deref(),
            Some("[ \"${DEPLOY:-}\" = 'true' ]")
        );
        assert_eq!(
            said("params.ENV != 'prod'").as_deref(),
            Some("! { [ \"${ENV:-}\" = 'prod' ]; }")
        );
        assert_eq!(
            said("!params.SKIP").as_deref(),
            Some("! { [ \"${SKIP:-}\" = 'true' ]; }")
        );
        // A string alone holds where it is set and not empty, as Groovy
        // takes it: a variable's, or a parameter's that is no boolean.
        assert_eq!(
            said("env.CHANGE_ID").as_deref(),
            Some("[ -n \"${CHANGE_ID:-}\" ]")
        );
        assert_eq!(
            said("return params.RELEASE_VERSION;").as_deref(),
            Some("[ -n \"${RELEASE_VERSION:-}\" ]")
        );
        assert_eq!(said("DEPLOY").as_deref(), Some("[ -n \"${DEPLOY:-}\" ]"));
        assert_eq!(said("return true").as_deref(), Some("true"));
        assert_eq!(said("!true").as_deref(), Some("false"));
        assert_eq!(
            said("false || params.DEPLOY").as_deref(),
            Some("[ \"${DEPLOY:-}\" = 'true' ]")
        );
        assert!(said("env.BRANCH_NAME ==~ /release.*/").is_none());
        assert!(said("currentBuild.result == null || isUnix()").is_none());
    }

    #[test]
    fn what_a_when_cannot_read_runs_the_stage() {
        let unread = || Cond::Unread("changeset".into());
        // Taken to hold, and under a `not`, not to: the stage runs.
        assert_eq!(unread().shell(), "true");
        assert_eq!(Cond::Not(Box::new(unread())).shell(), "! { false; }");
        assert_eq!(
            Cond::Not(Box::new(Cond::All(vec![
                Cond::Tag(None),
                Cond::Not(Box::new(unread()))
            ])))
            .shell(),
            "! { { [ \"$CI_EVENT\" = tag ]; } && { ! { true; }; }; }"
        );
        // All of nothing holds, and any of nothing does not.
        assert!(matches!(Cond::All(vec![]).simplified(), Cond::Lit(true)));
        assert!(matches!(Cond::Any(vec![]).simplified(), Cond::Lit(false)));
        assert_eq!(Cond::All(vec![]).shell(), "true");
        assert_eq!(Cond::Any(vec![]).shell(), "false");
        assert!(Cond::All(vec![]).dagron().is_none());
        // A when of nothing to test is no test.
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  stages {\n    stage('A') {\n      when { beforeAgent true }\n      input { message 'Go?' }\n      steps { sh 'a' }\n    }\n    stage('B') {\n      when { anyOf { } }\n      steps { sh 'b' }\n    }\n  }\n}\n";
        let read = pipeline(&import(text));
        assert!(read["tasks"][0].get("when").is_none(), "{read:?}");
        let steps = read["tasks"][1]["steps"].as_sequence().unwrap();
        assert_eq!(steps[0]["run"], "a");
        assert!(steps[1]["run"]
            .as_str()
            .unwrap()
            .contains("{ false; } || exit 0"));
    }

    #[test]
    fn the_jobs_after_a_gate_wait_for_what_it_waited_for() {
        // Where the gate's when is false, the run leaves it out: the job
        // after it waits for the build all the same.
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  stages {\n    stage('Build') { steps { sh './build' } }\n    stage('Release') {\n      when { branch 'main' }\n      input { message 'Release?' }\n      steps { sh './release' }\n      post { always { sh './notify' } }\n    }\n  }\n}\n";
        let imported = import(text);
        let read = pipeline(&imported);
        let tasks = read["tasks"].as_sequence().unwrap();
        assert_eq!(tasks[1]["name"], "approve-release");
        assert_eq!(
            serde_yaml::to_string(&tasks[2]["depends_on"]).unwrap(),
            "- build\n- approve-release\n"
        );
        // The stage's post is under its when too.
        let steps = tasks[2]["steps"].as_sequence().unwrap();
        assert_eq!(steps.len(), 2);
        for step in steps {
            assert!(
                step["run"].as_str().unwrap().contains("|| exit 0"),
                "{step:?}"
            );
        }
        assert!(notes(&imported, Kind::Changed)
            .iter()
            .any(|n| n.contains("the gate asks only where the stage's when holds")));
    }

    #[test]
    fn a_stage_s_credentials_are_its_steps_alone() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  environment { NPMRC = credentials('npmrc') }\n  stages {\n    stage('Test') { steps { sh 'npm test' } }\n    stage('Staging') {\n      environment { TOKEN = credentials('staging-token') }\n      steps { sh './deploy staging' }\n    }\n    stage('Prod') {\n      environment { TOKEN = credentials('prod-token') }\n      steps { sh './deploy prod' }\n    }\n    stage('Publish') {\n      steps {\n        withCredentials([string(credentialsId: 'npm-token', variable: 'TOKEN'), string(credentialsId: \"${params.X}\", variable: 'OTHER')]) {\n          sh 'npm publish'\n        }\n      }\n    }\n  }\n}\n";
        let imported = import(text);
        let read = pipeline(&imported);
        let task = &read["tasks"][0];
        let env: Vec<(String, String)> = task["env"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e["name"].as_str().unwrap().to_string(),
                    serde_yaml::to_string(&e["value_from"]).unwrap(),
                )
            })
            .collect();
        let names: BTreeSet<&str> = env.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names.len(), env.len(), "a name twice: {env:?}");
        assert!(names.contains("NPMRC"), "{env:?}");
        assert!(names.contains("TOKEN__staging_token"), "{env:?}");
        assert!(names.contains("TOKEN__prod_token"), "{env:?}");
        assert!(names.contains("TOKEN__npm_token"), "{env:?}");
        assert!(!names.contains("TOKEN"), "{env:?}");
        let runs: Vec<&str> = task["steps"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|s| s["run"].as_str().unwrap())
            .collect();
        // The step that says what is left, then each stage's.
        assert_eq!(runs[1], "npm test");
        assert_eq!(
            runs[2],
            "export TOKEN=\"$TOKEN__staging_token\"\n./deploy staging"
        );
        assert_eq!(
            runs[3],
            "export TOKEN=\"$TOKEN__prod_token\"\n./deploy prod"
        );
        assert_eq!(runs[4], "export TOKEN=\"$TOKEN__npm_token\"\nnpm publish");
        let todo = notes(&imported, Kind::Todo);
        assert!(
            todo.iter()
                .any(|n| n.contains("string's credentialsId, \"${params.X}\", is Groovy")),
            "{todo:#?}"
        );
    }

    #[test]
    fn a_stage_s_own_values_are_the_ones_its_steps_read() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  environment {\n    REGION = 'eu'\n    TARGET = \"app-${REGION}\"\n  }\n  stages {\n    stage('A') {\n      environment { TARGET = 'app-us' }\n      steps {\n        sh 'echo $TARGET'\n        withEnv(['TARGET=app-asia']) { sh 'echo $TARGET' }\n      }\n    }\n    stage('B') { steps { sh 'echo $TARGET' } }\n  }\n}\n";
        let read = pipeline(&import(text));
        let steps = read["tasks"][0]["steps"].as_sequence().unwrap();
        assert_eq!(steps[0]["env"]["TARGET"], "app-us");
        assert_eq!(steps[0]["run"], "echo $TARGET");
        assert_eq!(steps[1]["env"]["TARGET"], "app-asia");
        assert_eq!(steps[1]["run"], "echo $TARGET");
        assert_eq!(
            steps[2]["run"],
            "export TARGET=\"app-${REGION}\"\necho $TARGET"
        );
    }

    #[test]
    fn a_catch_error_block_ends_at_its_first_failure() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  stages {\n    stage('Lint') {\n      steps {\n        catchError(buildResult: 'SUCCESS') {\n          sh './lint'\n          sh(script: './maybe', returnStatus: true)\n          withEnv(['A=b']) { sh './report' }\n        }\n        sh './after'\n      }\n    }\n  }\n}\n";
        let imported = import(text);
        let read = pipeline(&imported);
        let steps = read["tasks"][0]["steps"].as_sequence().unwrap();
        assert_eq!(steps.len(), 2, "{steps:#?}");
        assert_eq!(steps[0]["continue_on_error"], true);
        assert_eq!(
            steps[0]["run"],
            "(\n./lint\n)\nset +e\n(\nset -e\n./maybe\n)\nset -e\n(\nexport A='b'\n./report\n)"
        );
        assert_eq!(steps[1]["run"], "./after");
        assert!(steps[1].get("continue_on_error").is_none());
        // The block, as a shell runs it: it ends at the first that fails.
        let script = steps[0]["run"]
            .as_str()
            .unwrap()
            .replace("./lint", "false")
            .replace("./maybe", "true")
            .replace("./report", "echo reached");
        let ran = std::process::Command::new("sh")
            .args(["-ec", &script])
            .output()
            .unwrap();
        assert!(!ran.status.success());
        assert!(!String::from_utf8_lossy(&ran.stdout).contains("reached"));
    }

    #[test]
    fn what_the_shell_would_not_fill_in_is_left_to_do() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  parameters { string(name: 'VERSION', defaultValue: \"${env.BUILD_NUMBER}\") }\n  stages {\n    stage('A') {\n      steps {\n        sh \"\"\"\ncat > v.env <<'EOF'\nVERSION=${params.VERSION}\nEOF\n\"\"\"\n      }\n    }\n  }\n}\n";
        let imported = import(text);
        let todo = notes(&imported, Kind::Todo);
        assert!(
            todo.iter()
                .any(|n| n.contains("a here-document whose mark is quoted")),
            "{todo:#?}"
        );
        assert!(
            todo.iter().any(|n| n.starts_with(
                "parameters.VERSION: its default, \"${env.BUILD_NUMBER}\", is Groovy"
            )),
            "{todo:#?}"
        );
    }

    #[test]
    fn names_are_at_most_100_characters() {
        let long = "x".repeat(120);
        let text = format!("pipeline {{\n  agent none\n  stages {{\n    stage('{long}') {{ agent {{ docker {{ image 'a' }} }}\n steps {{ sh 'a' }} }}\n    stage('{long}y') {{ agent {{ docker {{ image 'b' }} }}\n steps {{ sh 'b' }} }}\n  }}\n}}\n");
        let read = pipeline(&import(&text));
        let names: Vec<&str> = read["tasks"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names[0].len(), 100);
        assert_eq!(names[1].len(), 100);
        assert!(names[1].ends_with("x-2"), "{names:?}");
    }

    #[test]
    fn the_importer_s_own_marks_are_no_text_of_a_file() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  stages {\n    stage('A') { steps { sh 'echo \u{1}ci_sha\u{2}' } }\n  }\n}\n";
        let read = pipeline(&import(text));
        assert_eq!(read["tasks"][0]["steps"][0]["run"], "echo ci_sha");
    }

    #[test]
    fn globs_are_a_shell_s_patterns() {
        assert_eq!(
            case_pattern(&Pattern::Glob("release/**".into())),
            "'release/'*"
        );
        assert_eq!(case_pattern(&Pattern::Glob("v?.*".into())), "'v'?'.'*");
        assert_eq!(case_pattern(&Pattern::Exact("it's".into())), "'it'\\''s'");
        assert_eq!(as_secret("docker-hub.creds"), "docker_hub_creds");
    }

    #[test]
    fn parameters_are_a_dispatch_s_inputs_and_every_run_s_defaults() {
        let text = "pipeline {\n  agent { docker { image 'alpine:3' } }\n  parameters {\n    string(name: 'TARGET', defaultValue: 'staging')\n    password(name: 'SECRET', defaultValue: '')\n  }\n  stages {\n    stage('A') { steps { sh \"echo ${params.TARGET}\" } }\n  }\n}\n";
        let imported = import(text);
        let read = pipeline(&imported);
        assert_eq!(
            read["on"]["dispatch"]["inputs"]["TARGET"]["default"],
            "staging"
        );
        assert_eq!(read["parameters"]["TARGET"], "staging");
        assert_eq!(read["tasks"][0]["env"][0]["value"], "{{ TARGET }}");
        assert_eq!(read["tasks"][0]["steps"][1]["run"], "echo ${TARGET}");
        assert!(notes(&imported, Kind::Todo)
            .iter()
            .any(|n| n.starts_with("parameters.SECRET: a password is an environment's secret")));
    }
}
