//! The compiler: a pipeline and an event in, and out the dagron workflow a
//! run submits with its parameters, or why the pipeline does not run.
//!
//! A pure function: the same pipeline, event and settings give the same
//! bytes, so `dagron-ci compile` shows a reviewer exactly what will run.
//!
//! **What it decides.** `on:` against the event: its kind, then the branch,
//! tag or base branch, then the files it changed. A paths filter needs those
//! files; without them the answer is [`Outcome::NeedsChangedFiles`], never a
//! guess.
//!
//! **What it writes.**
//! - The pipeline's own keys, as written, with `concurrency:` as dagron's
//!   `concurrency_key` (and `max_active_runs: 1` unless the pipeline sets it,
//!   since dagron scopes that cap by the key). dagron counts a cap per
//!   workflow name and key, and every repository on a cell may have a
//!   pipeline called `ci`: the key is prefixed with the repository
//!   (`{{ ci_forge_host }}/{{ ci_repo }}:`), and a `max_active_runs` set
//!   alone gets the repository as its key, so no two repositories share one.
//! - `tags:` with [`CI_TAG`] added to the pipeline's own, so that a list of
//!   workflows can be narrowed to the ones CI compiled without their authors
//!   having to say so.
//! - Every `ci_*` parameter declared with an empty default, and the run's
//!   values as its parameters. Event data reaches tasks as `CI_*` variables
//!   in `task_defaults.env`, which dagron fills in from those parameters, so
//!   no event text is ever part of a command.
//! - A timeout on every task: the pipeline's `task_defaults.timeout_secs`, or
//!   [`Settings::default_timeout_secs`] when it sets none.
//! - A task with `steps:` runs one command, which runs each step in its own
//!   `sh -e`, in order. The steps travel as `CI_STEP_*` variables, the step
//!   protocol ([`STEP_PROTOCOL`]; dagron-ci-exec's README): each step's
//!   script, name and environment are values, so nothing a pipeline author or
//!   an event wrote is parsed by the command, which is the compiler's alone.
//!   The command hands the steps to `dagron-ci-exec` where the image has one
//!   that speaks the protocol, and runs them itself, a POSIX `sh` script,
//!   where it does not. A step that fails ends the task, unless it sets
//!   `continue_on_error`; a step's `timeout_secs` is kept by dagron-ci-exec,
//!   or by `timeout(1)` where the image has it. Markers
//!   (`::dagron-ci::step::N::start::NAME`, `…::end::CODE`) split the log by
//!   step, on stderr: a task's output is its steps' stdout.
//! - A task with `steps:` has a workspace (`workspace: {}`) unless it sets
//!   one itself, or `null` for none: a directory made for each attempt, which
//!   it starts in, with dagron-ci-exec at `/dagron/bin` where the engine's
//!   operator set `DAGRON_CI_EXEC_IMAGE`. That is where the command looks
//!   first, so the binary is in every image and the `sh` fallback is the
//!   exception.
//! - A workspace starts with the event's commit checked out in it
//!   (`workspace.checkout`, [`CHECKOUT_REPOSITORY`] at `{{ ci_sha }}`),
//!   unless the pipeline's `workspace:` says `checkout: false`. The engine
//!   fetches it before the task runs, with a credential for that attempt
//!   alone, which `POST /ci/v1/checkout-tokens` mints.

use std::collections::BTreeMap;

use serde_json::{Map, Value};
use serde_yaml::{Mapping, Value as Yaml};

use super::glob::Patterns;
use super::{Pipeline, Step, Task};

/// The step protocol compiled tasks speak: how their steps reach whatever
/// runs them (dagron-ci-exec's README has it whole).
pub const STEP_PROTOCOL: u32 = 1;

/// The step protocol of a task whose steps have what version 1 has no word
/// for: a shell, a directory, a condition, an id, a value evaluated as the
/// job runs ([`Step::needs_the_runner`]). A GitHub workflow's steps do.
/// Only dagron-ci-exec speaks it: there is no shell fallback for it.
pub const STEP_PROTOCOL_2: u32 = 2;

/// The step protocol of a task that is a GitHub workflow's job: version 2,
/// and what a job is beside its steps ([`super::JobSpec`]): the jobs it
/// needs, its own `if:`, its outputs. Such a job writes everything it
/// prints to one stream, and its last line is its own word to the jobs
/// after it.
pub const STEP_PROTOCOL_3: u32 = 3;

/// The step protocol of a job one of whose steps uses a local action:
/// version 3, and that step's `uses:` and `with:`. dagron-ci-exec reads the
/// action from the workspace as the step starts. A job none of whose steps
/// uses one is handed on in version 3 still, which a dagron-ci-exec of
/// before this one speaks.
pub const STEP_PROTOCOL_4: u32 = 4;

/// The step protocol of a GitLab job that has what the step runner does
/// around its steps ([`super::TaskHooks`]): its artifacts, and its
/// `allow_failure`. Version 2 and those, and not version 3 or 4, which are
/// a GitHub job's. A dagron-ci-exec of before it does none of them, and so
/// refuses it rather than run the job without them.
pub const STEP_PROTOCOL_5: u32 = 5;

/// What the engine's cache service reads after a job's own ref: the ref of
/// `ci_base_ref`, empty for a push.
pub const ACTIONS_CACHE_RESTORE_FROM: &str = "DAGRON_ACTIONS_CACHE_RESTORE_FROM";

/// The protocol a task is handed on in: its steps', and at least version 3
/// where it is a job with what only that version has words for.
fn protocol_of(t: &Task) -> u32 {
    match t.job {
        Some(_) => protocol(&t.steps).max(STEP_PROTOCOL_3),
        None if !t.hooks.is_empty() => STEP_PROTOCOL_5,
        None => protocol(&t.steps),
    }
}

/// The protocol a task's steps are handed on in.
pub fn protocol(steps: &[Step]) -> u32 {
    if steps.iter().any(|step| step.uses.is_some()) {
        STEP_PROTOCOL_4
    } else if steps.iter().any(Step::needs_the_runner) {
        STEP_PROTOCOL_2
    } else {
        STEP_PROTOCOL
    }
}

/// What scopes a compiled pipeline's concurrency in dagron: its repository.
const REPOSITORY_KEY: &str = "{{ ci_forge_host }}/{{ ci_repo }}";

/// The parameters dagron-ci sets from an event, in the order they are listed.
pub const CI_PARAMETERS: &[&str] = &[
    "ci_event",
    "ci_ref",
    "ci_sha",
    "ci_base_sha",
    "ci_base_ref",
    "ci_pr",
    "ci_repo",
    "ci_forge_host",
    "ci_group",
    "ci_trust",
];

/// The input a `schedule` event carries its cron in: what a workflow reads as
/// `github.event.schedule`.
pub const SCHEDULE_INPUT: &str = "schedule";

/// What a `failed` event says of the run that failed, each in
/// its inputs under its own name, and given to a pipeline that takes
/// `on.failed` as a parameter, and to its tasks as the variable of its name
/// in capitals (`CI_FAILED_RUN`): the run's id, its pipeline (its file's
/// name), the job that failed first, and that job's fault class and its
/// disposition, where the engine said them.
pub const FAILED_PARAMETERS: &[&str] = &[
    "ci_failed_run",
    "ci_failed_pipeline",
    "ci_failed_job",
    "ci_failed_fault_class",
    "ci_failed_disposition",
];

/// The `failed` event's input that names the pipeline whose run failed.
pub const FAILED_PIPELINE: &str = "ci_failed_pipeline";

/// What the compiler reads of an event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Event {
    /// `push`, `tag`, `pull_request`, `merge_group`, `dispatch`, `schedule`
    /// or `failed` (another pipeline's run failed). Anything else
    /// (`delete`, `pull_request_closed`) runs no pipeline.
    pub kind: String,
    pub git_ref: String,
    pub sha: String,
    pub base_sha: Option<String>,
    /// The branch a pull request or merge group merges into, `main` or
    /// `refs/heads/main`.
    pub base_ref: Option<String>,
    pub pr: Option<i64>,
    pub trust: String,
    /// `owner/name`.
    pub repo: String,
    /// The forge's kind (`github`, `gitea`, `forgejo`, …), where the event
    /// says: where its API is. Empty is GitHub's.
    pub forge: String,
    pub forge_host: String,
    /// A dispatch's inputs.
    pub inputs: BTreeMap<String, String>,
    /// The forge account that caused the event, where its door said:
    /// `github.actor`. Its owner chose it, so it is never pasted into a
    /// shell.
    pub actor: Option<String>,
    /// A pull request's head branch: `github.head_ref`. Never pasted
    /// either.
    pub head_ref: Option<String>,
    /// The repository's default branch, `main`, where it is known: what a
    /// GitLab pipeline reads as `CI_DEFAULT_BRANCH`.
    pub default_branch: Option<String>,
    /// The numbers of the run a workflow becomes: `github.run_id` and
    /// `github.run_number` ([`crate::db::Db::run_numbers`]). `None` while
    /// it is decided whether the workflow runs at all, and for a run that
    /// is no dagron-ci record's (`dagron-ci run`).
    pub run_numbers: Option<(i64, i64)>,
    /// The files the event changed, as far as they are known. Only a paths
    /// filter reads them.
    pub changed: Changes,
    /// The forge's own payload of the event, as its webhook delivered it,
    /// where it did and dagron-ci kept it (at most 64 KiB): what a GitHub
    /// workflow reads as `github.event`, under dagron-ci's own fields. None
    /// for an event dagron-ci made itself (a poll, a schedule, a request).
    pub payload: Option<serde_json::Value>,
}

/// The files an event changed, as far as they are known.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Changes {
    /// Not looked up: a paths filter answers [`Outcome::NeedsChangedFiles`].
    #[default]
    Unknown,
    /// Looked up, and not to be had, for the reason given: a new branch has
    /// nothing to be compared with, a change can be larger than the forge
    /// lists and a shallow fetch can read. A paths filter lets the pipeline
    /// run, since skipping it could skip a check a merge waits for.
    Unlisted(String),
    /// The files the event changed.
    Listed(Vec<String>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings {
    /// The timeout of a task whose pipeline sets none (CI_DEFAULT_TIMEOUT_SECS).
    pub default_timeout_secs: u64,
    /// Whether a workspace starts with the event's commit checked out in it.
    /// `dagron-ci run` turns it off: its tasks work in the tree they were
    /// run from.
    pub checkout: bool,
    /// Whether a workspace's checkout lays the run's patch bundle over the
    /// commit: a run API request named one.
    pub patched: bool,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            default_timeout_secs: 3600,
            checkout: true,
            patched: false,
        }
    }
}

/// The repository a workspace's checkout fetches: the event's, on the
/// forge it came from, filled in by dagron from the run's parameters.
pub const CHECKOUT_REPOSITORY: &str = "https://{{ ci_forge_host }}/{{ ci_repo }}.git";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The pipeline runs.
    Run(Compiled),
    /// The pipeline does not run for this event, and why.
    Skip(String),
    /// The pipeline takes this event, and none of the files it changed is
    /// one its `paths:` names. It does not run. Its jobs are named, for
    /// whoever says so where a check of theirs is waited for: a branch that
    /// requires one would wait for ever on a pipeline that said nothing.
    Unchanged { why: String, jobs: Vec<String> },
    /// A paths filter decides, and the event's changed files are not known.
    NeedsChangedFiles,
}

/// What a run submits: `POST /api/runs {yaml, parameters}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compiled {
    pub yaml: String,
    pub parameters: BTreeMap<String, String>,
}

/// Compile `pipeline` for `event`. An error is a request the pipeline
/// refuses: a dispatch input it does not take, or a required one missing.
pub fn compile(pipeline: &Pipeline, event: &Event, settings: &Settings) -> Result<Outcome, String> {
    match decide(pipeline, event) {
        Decision::Run => {}
        Decision::Skip(why) => return Ok(Outcome::Skip(why)),
        Decision::Unchanged(why) => {
            return Ok(Outcome::Unchanged {
                why,
                jobs: pipeline.tasks.iter().map(|t| t.name.clone()).collect(),
            })
        }
        Decision::NeedsChangedFiles => return Ok(Outcome::NeedsChangedFiles),
    }
    let inputs = inputs(pipeline, event)?;
    let mut parameters = inputs.clone();
    parameters.extend(ci_parameters(event));
    if pipeline.on.failed.is_some() {
        for name in FAILED_PARAMETERS {
            let value = match event.kind.as_str() {
                "failed" => event.inputs.get(*name).cloned().unwrap_or_default(),
                _ => String::new(),
            };
            parameters.insert(name.to_string(), value);
        }
    }
    let yaml = serde_yaml::to_string(&workflow(pipeline, &inputs, settings))
        .map_err(|e| format!("the workflow does not serialize: {e}"))?;
    Ok(Outcome::Run(Compiled { yaml, parameters }))
}

/// Why a job [`left_out`] names has no task: what its passing status says,
/// after `skipped: `.
pub const LEFT_OUT_BY_WHEN: &str = "its when: leaves it out of this event";

/// The jobs of `compiled` with a `when:` of their own: the ones its run can
/// leave out. Each with whether it fans out (`with_items`, `with_param`, or
/// a template's tasks), when what it makes is `<job>.<label>`.
fn conditional(compiled: &Compiled) -> Vec<(String, bool)> {
    let Ok(spec) = serde_yaml::from_str::<Yaml>(&compiled.yaml) else {
        return Vec::new();
    };
    let tasks = spec.get("tasks").and_then(Yaml::as_sequence);
    tasks
        .into_iter()
        .flatten()
        .filter(|task| task.get("when").is_some())
        .filter_map(|task| {
            let name = task.get("name")?.as_str()?.to_string();
            let fans = ["with_items", "with_param", "template"]
                .iter()
                .any(|fan| task.get(*fan).is_some());
            Some((name, fans))
        })
        .collect()
}

/// Whether the run of `compiled` can leave a job out: whether one has a
/// `when:` of its own, which dagron decides from the run's parameters as it
/// makes the run.
pub fn can_leave_out(compiled: &Compiled) -> bool {
    !conditional(compiled).is_empty()
}

/// The jobs of `compiled` that its run left out for their `when:`, where
/// `made` names the run's tasks: those dagron made no task of. One that fans
/// out is left out where no instance of it was made.
pub fn left_out(compiled: &Compiled, made: &[String]) -> Vec<String> {
    conditional(compiled)
        .into_iter()
        .filter(|(job, fans)| {
            let instance = |name: &String| {
                name.strip_prefix(job.as_str())
                    .is_some_and(|rest| rest.starts_with('.'))
            };
            !made.contains(job) && !(*fans && made.iter().any(instance))
        })
        .map(|(job, _)| job)
        .collect()
}

/// The `ci_*` parameters of `event`. Each is a pure function of the commit
/// key's fields and the repository, so every event for one commit submits the
/// same request, and dagron-api replays rather than refuses its key.
pub fn ci_parameters(event: &Event) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("ci_event".into(), event.kind.clone()),
        ("ci_ref".into(), event.git_ref.clone()),
        ("ci_sha".into(), event.sha.clone()),
        (
            "ci_base_sha".into(),
            event.base_sha.clone().unwrap_or_default(),
        ),
        ("ci_base_ref".into(), base_ref(event)),
        (
            "ci_pr".into(),
            event.pr.map(|n| n.to_string()).unwrap_or_default(),
        ),
        ("ci_repo".into(), event.repo.clone()),
        ("ci_forge_host".into(), event.forge_host.clone()),
        ("ci_group".into(), group(event)),
        ("ci_trust".into(), event.trust.clone()),
    ])
}

/// `ci_base_ref`: the branch a pull request or merge group merges into, as a
/// full ref (`refs/heads/main`), the spelling a push's `ci_ref` has; empty for
/// any other event, and for a name of more than the plain characters of a
/// branch. A job's caches restore from it after their own ref.
fn base_ref(event: &Event) -> String {
    let Some(name) = event.base_ref.as_deref().filter(|r| !r.is_empty()) else {
        return String::new();
    };
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-'))
    {
        return String::new();
    }
    if name.starts_with("refs/") {
        name.to_string()
    } else {
        format!("refs/heads/{name}")
    }
}

/// `ci_group`, what a `concurrency_key` groups runs by: `pr-{n}` for a pull
/// request, else the ref. Safe in a command and a `when:`: the ref keeps only
/// `[A-Za-z0-9._/-]`, every other character becomes `_`, and it is cut to 128.
pub fn group(event: &Event) -> String {
    if let (Some(pr), "pull_request" | "pull_request_closed") = (event.pr, event.kind.as_str()) {
        return format!("pr-{pr}");
    }
    event
        .git_ref
        .chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '.' | '_' | '/' | '-' => c,
            _ => '_',
        })
        .take(128)
        .collect()
}

enum Decision {
    Run,
    Skip(String),
    /// Skipped by a paths filter alone: everything else of `on:` took it.
    Unchanged(String),
    NeedsChangedFiles,
}

fn decide(p: &Pipeline, ev: &Event) -> Decision {
    let skip = |why: String| Decision::Skip(why);
    match ev.kind.as_str() {
        "push" => {
            let Some(f) = &p.on.push else {
                return skip("on: does not take push".into());
            };
            let branch = ev
                .git_ref
                .strip_prefix("refs/heads/")
                .unwrap_or(&ev.git_ref);
            match (&f.branches, &f.tags) {
                (Some(branches), _) if !branches.matches(branch) => {
                    skip(format!("branch {branch:?} is not in on.push.branches"))
                }
                (None, Some(_)) => skip("on.push takes tags, not branches".into()),
                _ => paths(&f.paths, ev, "on.push.paths"),
            }
        }
        // A tag push: paths are not read, as GitHub reads none for one.
        "tag" => {
            let Some(f) = &p.on.push else {
                return skip("on: does not take push".into());
            };
            let tag = ev.git_ref.strip_prefix("refs/tags/").unwrap_or(&ev.git_ref);
            match (&f.tags, &f.branches) {
                (Some(tags), _) if !tags.matches(tag) => {
                    skip(format!("tag {tag:?} is not in on.push.tags"))
                }
                (None, Some(_)) => skip("on.push takes branches, not tags".into()),
                _ => Decision::Run,
            }
        }
        "pull_request" => {
            let Some(f) = &p.on.pull_request else {
                return skip("on: does not take pull_request".into());
            };
            match base(&f.branches, ev, "on.pull_request.branches") {
                Decision::Run => paths(&f.paths, ev, "on.pull_request.paths"),
                other => other,
            }
        }
        "merge_group" => match &p.on.merge_group {
            Some(f) => base(&f.branches, ev, "on.merge_group.branches"),
            None => skip("on: does not take merge_group".into()),
        },
        "dispatch" => match &p.on.dispatch {
            Some(_) => Decision::Run,
            None => skip("on: does not take dispatch".into()),
        },
        // A run of another pipeline failed: this runs where it names that
        // pipeline, for the failed run's branch.
        "failed" => {
            let Some(f) = &p.on.failed else {
                return skip("on: does not take failed".into());
            };
            let failed = ev
                .inputs
                .get(FAILED_PIPELINE)
                .map(String::as_str)
                .unwrap_or("");
            if !f.pipelines.iter().any(|name| name == failed) {
                return skip(format!("on.failed does not name {failed:?}"));
            }
            // A pull request's run, and a merge group's, are of the branch
            // they merge into: a merge group's ref is a branch of GitHub's
            // queue (`gh-readonly-queue/main/pr-42-…`), not the one it is of.
            let into = ev
                .base_ref
                .as_deref()
                .filter(|r| !r.is_empty())
                .map(|r| r.strip_prefix("refs/heads/").unwrap_or(r).to_string());
            let branch = match ev.git_ref.strip_prefix("refs/heads/") {
                Some(queued) if queued.starts_with("gh-readonly-queue/") => into,
                Some(branch) => Some(branch.to_string()),
                None => into,
            };
            match (&f.branches, branch) {
                (None, _) => Decision::Run,
                (Some(branches), Some(branch)) if branches.matches(&branch) => Decision::Run,
                (Some(_), Some(branch)) => {
                    skip(format!("branch {branch:?} is not in on.failed.branches"))
                }
                (Some(_), None) => {
                    skip("on.failed names branches, and the run that failed was of none".into())
                }
            }
        }
        // A schedule's cron fired: the workflow runs if it still has it.
        "schedule" => {
            let cron = ev.inputs.get(SCHEDULE_INPUT).map(String::as_str);
            match cron {
                Some(cron) if p.on.schedule.iter().any(|c| c.trim() == cron.trim()) => {
                    Decision::Run
                }
                Some(cron) => skip(format!("on.schedule no longer has {cron:?}")),
                None => skip("a schedule event that names no cron".into()),
            }
        }
        other => skip(format!("a {other} event runs no pipeline")),
    }
}

/// The branch a pull request or merge group merges into, against `branches`.
fn base(branches: &Option<Patterns>, ev: &Event, filter: &str) -> Decision {
    let Some(branches) = branches else {
        return Decision::Run;
    };
    match ev.base_ref.as_deref() {
        Some(base) => {
            let base = base.strip_prefix("refs/heads/").unwrap_or(base);
            if branches.matches(base) {
                Decision::Run
            } else {
                Decision::Skip(format!("base branch {base:?} is not in {filter}"))
            }
        }
        None => Decision::Skip(format!(
            "the base branch is not known, and {filter} needs it"
        )),
    }
}

fn paths(filter: &Option<Patterns>, ev: &Event, name: &str) -> Decision {
    let Some(filter) = filter else {
        return Decision::Run;
    };
    match &ev.changed {
        Changes::Unknown => Decision::NeedsChangedFiles,
        Changes::Unlisted(_) => Decision::Run,
        Changes::Listed(changed) => {
            let changed: Vec<&str> = changed.iter().map(String::as_str).collect();
            if filter.any(&changed) {
                Decision::Run
            } else {
                Decision::Unchanged(format!("no changed file matches {name}"))
            }
        }
    }
}

/// A dispatch's inputs, as the run gets them: each the pipeline declares, the
/// caller's value or its default. Empty for every other event.
fn inputs(p: &Pipeline, ev: &Event) -> Result<BTreeMap<String, String>, String> {
    if ev.kind != "dispatch" {
        return Ok(BTreeMap::new());
    }
    let declared =
        p.on.dispatch
            .as_ref()
            .map(|d| &d.inputs)
            .cloned()
            .unwrap_or_default();
    if let Some(unknown) = ev.inputs.keys().find(|k| !declared.contains_key(*k)) {
        return Err(format!(
            "pipeline {} takes no input {unknown:?} (on.dispatch.inputs)",
            p.name
        ));
    }
    let mut out = BTreeMap::new();
    for (name, input) in &declared {
        match ev.inputs.get(name).or(input.default.as_ref()) {
            Some(value) => {
                out.insert(name.clone(), value.clone());
            }
            None if input.required => {
                return Err(format!("pipeline {} needs input {name:?}", p.name));
            }
            None => {
                out.insert(name.clone(), String::new());
            }
        }
    }
    Ok(out)
}

fn yaml(value: &Value) -> Yaml {
    serde_yaml::to_value(value).unwrap_or(Yaml::Null)
}

fn key(k: &str) -> Yaml {
    Yaml::String(k.to_string())
}

fn env_entry(name: &str, value: &str) -> Yaml {
    let mut entry = Mapping::new();
    entry.insert(key("name"), key(name));
    entry.insert(key("value"), key(value));
    Yaml::Mapping(entry)
}

/// The tag every compiled workflow carries, beside its author's own.
pub const CI_TAG: &str = "ci";

/// The workflow's `tags`: the pipeline's own as written, then [`CI_TAG`]
/// unless it is among them. A `tags:` that is no list is passed on as it is,
/// for dagron to refuse in its own words.
fn tags(p: &Pipeline) -> Yaml {
    let mut tags = match p.spec.get("tags") {
        Some(Value::Array(own)) => own.iter().map(yaml).collect(),
        Some(Value::Null) | None => Vec::new(),
        Some(other) => return yaml(other),
    };
    if !tags.iter().any(|t| t.as_str() == Some(CI_TAG)) {
        tags.push(key(CI_TAG));
    }
    Yaml::Sequence(tags)
}

/// The dagron workflow, keys in an order a reader expects: name, parameters,
/// concurrency, tags, the pipeline's other keys, task defaults, tasks.
fn workflow(p: &Pipeline, inputs: &BTreeMap<String, String>, settings: &Settings) -> Yaml {
    let mut spec = Mapping::new();
    spec.insert(key("name"), key(&p.name));

    let mut parameters = Mapping::new();
    for name in CI_PARAMETERS {
        parameters.insert(key(name), key(""));
    }
    if let Some(Value::Object(own)) = p.spec.get("parameters") {
        for (name, default) in own {
            parameters.insert(key(name), yaml(default));
        }
    }
    if p.on.failed.is_some() {
        for name in FAILED_PARAMETERS {
            parameters.insert(key(name), key(""));
        }
    }
    for name in inputs.keys() {
        let default =
            p.on.dispatch
                .as_ref()
                .and_then(|d| d.inputs.get(name))
                .and_then(|i| i.default.clone())
                .unwrap_or_default();
        parameters.insert(key(name), key(&default));
    }
    spec.insert(key("parameters"), Yaml::Mapping(parameters));

    if let Some(concurrency) = &p.concurrency {
        spec.insert(
            key("concurrency_key"),
            key(&format!("{REPOSITORY_KEY}:{}", concurrency.key)),
        );
        if !p.spec.contains_key("max_active_runs") {
            spec.insert(key("max_active_runs"), Yaml::Number(1.into()));
        }
        // A run over its key is made, and waits in the engine where its
        // commit can be told it waits; or it takes the place of the run
        // before it.
        let when_full = match concurrency.cancel_superseded {
            true => "supersede",
            false => "queue",
        };
        spec.insert(key("when_full"), key(when_full));
    } else if p
        .spec
        .get("max_active_runs")
        .and_then(Value::as_u64)
        .is_some_and(|cap| cap >= 1)
    {
        spec.insert(key("concurrency_key"), key(REPOSITORY_KEY));
        spec.insert(key("when_full"), key("queue"));
    }
    spec.insert(key("tags"), tags(p));
    for (k, v) in &p.spec {
        if !matches!(k.as_str(), "parameters" | "task_defaults" | "tags") {
            spec.insert(key(k), yaml(v));
        }
    }

    let mut defaults = match p.spec.get("task_defaults") {
        Some(Value::Object(d)) => d.clone(),
        _ => Map::new(),
    };
    let mut env: Vec<Yaml> = CI_PARAMETERS
        .iter()
        .map(|name| env_entry(&name.to_ascii_uppercase(), &format!("{{{{ {name} }}}}")))
        .collect();
    if p.on.failed.is_some() {
        env.extend(
            FAILED_PARAMETERS
                .iter()
                .map(|name| env_entry(&name.to_ascii_uppercase(), &format!("{{{{ {name} }}}}"))),
        );
    }
    if let Some(Value::Array(own)) = defaults.remove("env") {
        env.extend(own.iter().map(yaml));
    }
    let mut task_defaults = Mapping::new();
    for (k, v) in &defaults {
        task_defaults.insert(key(k), yaml(v));
    }
    if !defaults.contains_key("timeout_secs") {
        task_defaults.insert(
            key("timeout_secs"),
            Yaml::Number(settings.default_timeout_secs.into()),
        );
    }
    task_defaults.insert(key("env"), Yaml::Sequence(env));
    spec.insert(key("task_defaults"), Yaml::Mapping(task_defaults));

    spec.insert(
        key("tasks"),
        Yaml::Sequence(p.tasks.iter().map(|t| task(t, settings)).collect()),
    );
    Yaml::Mapping(spec)
}

fn task(t: &Task, settings: &Settings) -> Yaml {
    let mut out = Mapping::new();
    out.insert(key("name"), key(&t.name));
    if !t.depends_on.is_empty() {
        out.insert(
            key("depends_on"),
            Yaml::Sequence(t.depends_on.iter().map(|d| key(d)).collect()),
        );
    }
    for (k, v) in &t.fields {
        if k != "env" && k != "workspace" {
            out.insert(key(k), yaml(v));
        }
    }
    let mut env: Vec<Yaml> = match t.fields.get("env") {
        Some(Value::Array(own)) => own.iter().map(yaml).collect(),
        _ => Vec::new(),
    };
    if !t.steps.is_empty() {
        let speaks = protocol_of(t);
        for (name, value) in variables(&t.steps, speaks) {
            env.push(env_entry(&name, &value));
        }
        // The reports its steps write, for the step runner to read when
        // they are over: paths, one to a line. The shell that stands in for
        // the step runner where an image has none reads no report.
        for (name, paths) in [
            ("CI_REPORT_JUNIT", &t.reports.junit),
            ("CI_REPORT_SARIF", &t.reports.sarif),
        ] {
            if !paths.is_empty() {
                env.push(env_entry(name, &paths.join("\n")));
            }
        }
        // Its artifacts: what it fetches before its first step, and what it
        // uploads when they are over, which the step runner does around
        // them.
        if !t.hooks.download.is_empty() {
            env.push(env_entry(
                "CI_ARTIFACT_DOWNLOAD",
                &t.hooks.download.join("\n"),
            ));
        }
        if let Some(up) = &t.hooks.upload {
            env.push(env_entry("CI_ARTIFACT_UPLOAD_NAME", &up.name));
            env.push(env_entry("CI_ARTIFACT_UPLOAD_PATHS", &up.paths.join("\n")));
            if !up.exclude.is_empty() {
                env.push(env_entry(
                    "CI_ARTIFACT_UPLOAD_EXCLUDE",
                    &up.exclude.join("\n"),
                ));
            }
            env.push(env_entry("CI_ARTIFACT_UPLOAD_WHEN", &up.when));
            if let Some(days) = up.expire_days {
                env.push(env_entry(
                    "CI_ARTIFACT_UPLOAD_EXPIRE_DAYS",
                    &days.to_string(),
                ));
            }
        }
        if t.hooks.allow_failure {
            env.push(env_entry("CI_JOB_ALLOW_FAILURE", "true"));
        }
        // A job that uses actions may use `actions/cache`, which the engine's
        // cache service answers: its restores read the job's own ref, then
        // the branch a pull request merges into, as a path cache's do.
        if speaks == STEP_PROTOCOL_4 {
            env.push(env_entry(ACTIONS_CACHE_RESTORE_FROM, "{{ ci_base_ref }}"));
        }
        if let Some(job) = &t.job {
            for (name, value) in job_variables(job) {
                env.push(env_entry(&name, &value));
            }
            // What each task it needs came to is the engine's to give it,
            // as the job is started: the task's status, and the last line
            // it printed, which is the job's own word (dagron's `env_from`).
            let mut given: Vec<Yaml> = match t.fields.get("env_from") {
                Some(Value::Array(own)) => own.iter().map(yaml).collect(),
                _ => Vec::new(),
            };
            for (k, (_, needed)) in job.needs.iter().enumerate() {
                let Some(needed) = needed else { continue };
                for (what, read) in [("STATUS", "status"), ("SAID", "last_line")] {
                    given.push(yaml(&serde_json::json!({
                        "name": format!("CI_NEED_{}_{what}", k + 1),
                        "task": needed,
                        "read": read,
                    })));
                }
            }
            if !given.is_empty() {
                out.insert(key("env_from"), Yaml::Sequence(given));
            }
        }
        let command = match speaks {
            STEP_PROTOCOL => shell_driver(&t.steps),
            speaks => driver_for(speaks),
        };
        out.insert(
            key("command"),
            Yaml::Sequence(vec![key("sh"), key("-c"), key(&command)]),
        );
    }
    // A job runs in a workspace of its own, where the engine installs
    // dagron-ci-exec beside it: the task's own (a size), `{}` for a task with
    // steps that sets none, or none at all (null). It starts with the event's
    // commit checked out, unless it says `checkout: false`.
    let workspace = match t.fields.get("workspace") {
        Some(Value::Object(own)) => Some(own.clone()),
        Some(_) => None,
        None if !t.steps.is_empty() => Some(Map::new()),
        None => None,
    };
    if let Some(mut ws) = workspace {
        // `false` for none; `{ depth: N }` for how much of its history comes
        // with the commit. The repository and the commit are the event's.
        let asked = ws.remove("checkout");
        let wanted = !matches!(asked, Some(Value::Bool(false)));
        if wanted && settings.checkout {
            let mut checkout =
                serde_json::json!({"repository": CHECKOUT_REPOSITORY, "commit": "{{ ci_sha }}"});
            if let Some(depth) = asked.as_ref().and_then(|asked| asked.get("depth")) {
                checkout["depth"] = depth.clone();
            }
            if settings.patched {
                checkout["patched"] = Value::Bool(true);
            }
            ws.insert("checkout".to_string(), checkout);
        }
        // A cache restores from the job's own ref, then from the branch a
        // pull request or merge group merges into, which it reads and never
        // writes. A push has none: `ci_base_ref` is empty, which dagron
        // reads as none.
        if let Some(Value::Array(caches)) = ws.get_mut("cache") {
            for cache in caches.iter_mut() {
                if let Value::Object(cache) = cache {
                    cache.insert(
                        "restore_from".to_string(),
                        Value::String("{{ ci_base_ref }}".to_string()),
                    );
                }
            }
        }
        // A forge token is for the repository the workspace checks out: a
        // job that checks out nothing (`dagron-ci run`, whose tree is the
        // code) is given none.
        if !ws.contains_key("checkout") {
            out.remove(key("forge_token"));
        }
        out.insert(key("workspace"), yaml(&Value::Object(ws)));
    } else {
        out.remove(key("forge_token"));
    }
    if !env.is_empty() {
        out.insert(key("env"), Yaml::Sequence(env));
    }
    Yaml::Mapping(out)
}

/// The variables that carry what a job is beside its steps: step protocol
/// [`STEP_PROTOCOL_3`]. What a needed task came to is not among them: the
/// engine gives that when the job starts. A job that was left out when the
/// run was made has no task to ask, and is said skipped here.
fn job_variables(job: &super::JobSpec) -> Vec<(String, String)> {
    let mut vars = Vec::new();
    if let Some(condition) = &job.condition {
        vars.push(("CI_JOB_IF".to_string(), condition.clone()));
    }
    if !job.needs.is_empty() {
        vars.push(("CI_NEED_COUNT".to_string(), job.needs.len().to_string()));
    }
    for (k, (id, task)) in job.needs.iter().enumerate() {
        vars.push((format!("CI_NEED_{}_JOB", k + 1), id.clone()));
        if task.is_none() {
            vars.push((format!("CI_NEED_{}_STATUS", k + 1), "skipped".to_string()));
        }
    }
    if !job.outputs.is_empty() {
        vars.push(("CI_OUTPUT_COUNT".to_string(), job.outputs.len().to_string()));
    }
    for (k, (name, value)) in job.outputs.iter().enumerate() {
        vars.push((format!("CI_OUTPUT_{}_NAME", k + 1), name.clone()));
        vars.push((format!("CI_OUTPUT_{}_VALUE", k + 1), value.clone()));
    }
    vars
}

/// The variables that carry `steps` to whatever runs them: step protocol
/// [`STEP_PROTOCOL`], in the order the workflow lists them.
#[cfg(test)]
fn step_variables(steps: &[Step]) -> Vec<(String, String)> {
    variables(steps, protocol(steps))
}

/// [`step_variables`], said in the protocol `speaks`.
fn variables(steps: &[Step], speaks: u32) -> Vec<(String, String)> {
    let mut vars = vec![
        ("CI_STEP_PROTOCOL".to_string(), speaks.to_string()),
        ("CI_STEP_COUNT".to_string(), steps.len().to_string()),
    ];
    for (i, step) in steps.iter().enumerate() {
        let n = i + 1;
        vars.push((format!("CI_STEP_{n}_NAME"), step_name(step)));
        vars.push((format!("CI_STEP_{n}_RUN"), step.run.clone()));
        if let Some(secs) = step.timeout_secs {
            vars.push((format!("CI_STEP_{n}_TIMEOUT_SECS"), secs.to_string()));
        }
        if step.continue_on_error {
            vars.push((format!("CI_STEP_{n}_CONTINUE_ON_ERROR"), "true".into()));
        }
        for (name, value) in &step.env {
            vars.push((format!("CI_STEP_{n}_ENV_{name}"), value.clone()));
        }
        // Step protocol 2, each only where the step has it.
        for (key, value) in [
            ("ID", &step.id),
            ("SHELL", &step.shell),
            ("WORKDIR", &step.working_directory),
            ("IF", &step.condition),
        ] {
            if let Some(value) = value {
                vars.push((format!("CI_STEP_{n}_{key}"), value.clone()));
            }
        }
        for (name, value) in &step.late_env {
            vars.push((format!("CI_STEP_{n}_ENVX_{name}"), value.clone()));
        }
        for (k, expression) in step.exprs.iter().enumerate() {
            vars.push((format!("CI_STEP_{n}_EXPR_{}", k + 1), expression.clone()));
        }
        // Step protocol 4.
        if let Some(uses) = &step.uses {
            vars.push((format!("CI_STEP_{n}_USES"), uses.clone()));
        }
        for (k, (name, value)) in step.with.iter().enumerate() {
            vars.push((format!("CI_STEP_{n}_WITH_{}_NAME", k + 1), name.clone()));
            vars.push((format!("CI_STEP_{n}_WITH_{}_VALUE", k + 1), value.clone()));
        }
    }
    vars
}

/// A step's name, or the first line of its script, or the action it uses,
/// as GitHub names one.
pub(super) fn step_name(step: &Step) -> String {
    if let Some(name) = &step.name {
        return name.clone();
    }
    if let Some(uses) = &step.uses {
        return format!("Run {uses}");
    }
    let first = step.run.lines().next().unwrap_or_default().trim();
    let mut name: String = first.chars().take(60).collect();
    if first.chars().count() > 60 {
        name.push('…');
    }
    name
}

/// The task's command: hand the steps to dagron-ci-exec where the image has
/// one that speaks [`STEP_PROTOCOL`] (asked with `--speaks`, so one that does
/// not is passed over rather than misreads them), else run each in order
/// here. Only numbers, variable names and the compiler's own text appear in
/// it; every script, name and value is read from a `CI_STEP_*` variable.
///
/// It is one argument, `sh -c`'s, which the kernel refuses at 128 KiB, so a
/// step adds one short line to it: what each runs is in functions written
/// once. The validator holds a task to that size
/// ([`super::MAX_COMMAND_BYTES`]).
pub(super) fn driver(steps: &[Step]) -> String {
    match protocol(steps) {
        STEP_PROTOCOL => shell_driver(steps),
        speaks => driver_for(speaks),
    }
}

/// The command of a task only dagron-ci-exec can run, one that speaks the
/// protocol `speaks`: no shell runs such steps in its place.
fn driver_for(speaks: u32) -> String {
    format!(
        "# dagron-ci: this task's steps, run by dagron-ci-exec (step protocol {speaks}), at\n\
         # /dagron/bin or on PATH. They name what a shell alone cannot run for them.\n\
         for ci_exec in /dagron/bin/dagron-ci-exec \"$(command -v dagron-ci-exec 2>/dev/null)\"; do\n\
         \x20 if [ -x \"$ci_exec\" ] && \"$ci_exec\" --speaks {speaks} >/dev/null 2>&1; then\n\
         \x20   exec \"$ci_exec\"\n\
         \x20 fi\n\
         done\n\
         printf '%s\\n' \"::dagron-ci::error::this job's steps need dagron-ci-exec (step \
         protocol {speaks}), and none is at /dagron/bin or on PATH: the engine \
         installs it where its operator set DAGRON_CI_EXEC_IMAGE\" >&2\n\
         exit 125\n"
    )
}

/// The command of the task that plans a job's instances, where an earlier
/// job computes its matrix: dagron-ci-exec's `matrix`, which prints them
/// and nothing else. What it prints is the whole of what the engine reads,
/// so no line of this command's own goes to its standard output.
pub(super) fn planner() -> String {
    format!(
        "# dagron-ci: this job's instances, planned by dagron-ci-exec (step protocol \
         {STEP_PROTOCOL_3}), at\n\
         # /dagron/bin or on PATH, from what the jobs it needs came to.\n\
         for ci_exec in /dagron/bin/dagron-ci-exec \"$(command -v dagron-ci-exec 2>/dev/null)\"; do\n\
         \x20 if [ -x \"$ci_exec\" ] && \"$ci_exec\" --speaks {STEP_PROTOCOL_3} >/dev/null 2>&1; then\n\
         \x20   exec \"$ci_exec\" matrix\n\
         \x20 fi\n\
         done\n\
         printf '%s\\n' \"::dagron-ci::error::this job's matrix needs dagron-ci-exec (step \
         protocol {STEP_PROTOCOL_3}), and none is at /dagron/bin or on PATH: the engine \
         installs it where its operator set DAGRON_CI_EXEC_IMAGE\" >&2\n\
         exit 125\n"
    )
}

/// The command of a task whose steps a shell can run where the image has no
/// dagron-ci-exec: step protocol [`STEP_PROTOCOL`].
fn shell_driver(steps: &[Step]) -> String {
    let mut script = format!(
        "# dagron-ci: this task's steps, in order, each in its own `sh -e`. They run\n\
         # under dagron-ci-exec (step protocol {STEP_PROTOCOL}) where the image has it, at\n\
         # /dagron/bin or on PATH, and under the lines below where it does not.\n\
         # Either way the markers go to stderr: the output is the steps'.\n\
         for ci_exec in /dagron/bin/dagron-ci-exec \"$(command -v dagron-ci-exec 2>/dev/null)\"; do\n\
         \x20 if [ -x \"$ci_exec\" ] && \"$ci_exec\" --speaks {STEP_PROTOCOL} >/dev/null 2>&1; then\n\
         \x20   exec \"$ci_exec\"\n\
         \x20 fi\n\
         done\n\
         ci_run() {{\n\
         \x20 if [ \"$1\" -gt 0 ]; then\n\
         \x20   if command -v timeout >/dev/null 2>&1; then\n\
         \x20     timeout -k 10 \"$1\" sh -ec \"$2\"\n\
         \x20     return $?\n\
         \x20   fi\n\
         \x20   printf '%s\\n' \"::dagron-ci::warning::no timeout(1) in this image; the step runs without its $1 s limit\" >&2\n\
         \x20 fi\n\
         \x20 sh -ec \"$2\"\n\
         }}\n\
         ci_start() {{\n\
         \x20 printf '%s\\n' \"::dagron-ci::step::$1::start::$2\" >&2\n\
         }}\n\
         # A failed step ends the task, unless it may fail (its third argument, 1).\n\
         ci_end() {{\n\
         \x20 printf '%s\\n' \"::dagron-ci::step::$1::end::$2\" >&2\n\
         \x20 [ \"$2\" -eq 0 ] && return 0\n\
         \x20 [ \"$3\" = 1 ] || exit \"$2\"\n\
         \x20 printf '%s\\n' \"::dagron-ci::step::$1::continued\" >&2\n\
         }}\n"
    );
    for (i, step) in steps.iter().enumerate() {
        let n = i + 1;
        let limit = step.timeout_secs.unwrap_or(0);
        let exports: String = step
            .env
            .keys()
            .map(|name| format!("export {name}=\"$CI_STEP_{n}_ENV_{name}\"; "))
            .collect();
        let may_fail = u8::from(step.continue_on_error);
        script.push_str(&format!(
            "ci_start {n} \"$CI_STEP_{n}_NAME\"; ( {exports}ci_run {limit} \"$CI_STEP_{n}_RUN\" ); \
             ci_end {n} $? {may_fail}\n"
        ));
    }
    script.push_str("exit 0\n");
    script
}

#[cfg(test)]
mod tests;
