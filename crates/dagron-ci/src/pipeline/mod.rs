//! The native pipeline format, `.dagron/ci/*.yaml`, read into the IR: the
//! form every front-end produces and the compiler takes.
//!
//! A pipeline is a dagron workflow with two additions:
//! - **`on:`** says which events run it: `push` (branches and tags),
//!   `pull_request`, `merge_group` and `dispatch` (the run API), each with
//!   optional filters on branches, tags and paths;
//! - **`steps:`** on a task: commands run in order inside it, in one
//!   workspace, retried as a unit. The engine sees one task with one command.
//!
//! A task's **`services:`** is dagron's own key, passed through as the
//! rest are: containers the job has beside it while it runs, each started
//! before its first step and seen ready, and reached by its name.
//!
//! and one changed key: **`concurrency:`** (`key`, `cancel_superseded`), which
//! compiles to dagron's `concurrency_key`. Every other key is dagron's own and
//! passes through as written; dagron checks its values when the run is
//! submitted. A key that is neither dagron's nor the pipeline's is an error,
//! since dagron would drop it without a word.
//!
//! **What a command may name.** Event data reaches steps as environment
//! variables (`CI_REF`, `CI_SHA`, …), never through a template. A `{{ … }}`
//! in a `run:`, a `command:` or a `when:` may name only values no outsider
//! chooses: `ci_event`, `ci_group`, `ci_sha`, `ci_base_sha`, `ci_pr`,
//! `ci_trust`, `ci_repo`, `ci_forge_host`, the pipeline's own `parameters`, a
//! matrix's `item` and `index`, and, in a `when:`, an upstream task's output
//! (`tasks.X.output`). A branch name (`ci_ref`) or a dispatch's inputs may
//! appear in an `env:` value, where no shell reads them.
//!
//! The published JSON Schema is [`SCHEMA`] (`dagron-ci schema`).

pub mod compile;
pub mod github;
pub mod gitlab;
/// The globs `on:` filters take: dagron-ci-expr's, which the step runner
/// reads `hashFiles` with too.
pub use dagron_ci_expr::glob;

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use glob::Patterns;

/// The native format's JSON Schema, draft 2020-12, for editors and linters.
pub const SCHEMA: &str = include_str!("../../schema/pipeline.v1.json");

/// The events `on:` takes.
pub const TRIGGERS: &[&str] = &["dispatch", "failed", "merge_group", "pull_request", "push"];

/// The keys a step takes.
pub const STEP_KEYS: &[&str] = &["continue_on_error", "env", "name", "run", "timeout_secs"];

/// The longest step script: a step's script reaches its task through an
/// environment variable, and the kernel caps one at 128 KiB.
pub const MAX_RUN_BYTES: usize = 64 * 1024;

/// The most steps a task may have: what dagron-ci-exec takes.
pub const MAX_STEPS: usize = 1000;

/// The most a task's steps may carry, scripts and their variables together.
/// All of it travels in the task's environment, which the kernel caps for an
/// exec (2 MiB by default, arguments included) and Kubernetes for a pod
/// (etcd's 1.5 MiB object): past it a task fails to start, late and
/// obscurely, where this refuses it at once.
pub const MAX_STEPS_BYTES: usize = 512 * 1024;

/// The longest command a task's steps may compile to. It is one argument,
/// `sh -c`'s, and the kernel refuses one of 128 KiB or more (its closing NUL
/// counted): each step adds a line to it, longer for each of its variables.
pub const MAX_COMMAND_BYTES: usize = 128 * 1024 - 1;

/// `ci_*` parameters a command or a `when:` may name: dagron-ci computes each
/// from the verified event, and none is an outsider's text.
pub const SAFE_PARAMETERS: &[&str] = &[
    "ci_base_sha",
    "ci_event",
    "ci_forge_host",
    "ci_group",
    "ci_pr",
    "ci_repo",
    "ci_sha",
    "ci_trust",
];

/// dagron's task keys (`dagron-core`'s `TaskSpec`, `type` for `task_type`).
/// Kept in step with it by `tests/dagron_keys.rs`, which reads that struct.
pub const DAGRON_TASK_KEYS: &[&str] = &[
    "allow_failure",
    "artifact_api",
    "approval_message",
    "approval_on_timeout",
    "approval_show",
    "approval_timeout_secs",
    "approvers",
    "arguments",
    "binds",
    "cache",
    "command",
    "defer",
    "depends_on",
    "docker_daemon",
    "docker_image",
    "env",
    "env_from",
    "environment",
    "forge_token",
    "gang",
    "gang_member",
    "hook",
    "identity",
    "image_pull_policy",
    "input",
    "instance_key",
    "isolation",
    "max_attempts",
    "name",
    "not_triggerer",
    "pool",
    "priority",
    "privileged",
    "produces",
    "repeat",
    "resources",
    "retry_budgets",
    "retry_delay_secs",
    "retry_max_delay_secs",
    "retry_on_timeout",
    "runner_class",
    "service_account",
    "services",
    "template",
    "timeout_secs",
    "tokens",
    "trigger_rule",
    "type",
    "wait",
    "when",
    "with_items",
    "with_output_of",
    "with_param",
    "workflow",
    "workspace",
];

/// dagron's workflow keys (`DagSpec`, `use` for `uses`). Kept in step the
/// same way.
pub const DAGRON_SPEC_KEYS: &[&str] = &[
    "budget",
    "concurrency_key",
    "datasets_mode",
    "deadline",
    "environment",
    "max_active_runs",
    "name",
    "notify",
    "on_datasets",
    "param_schema",
    "parameters",
    "result_from",
    "run_timeout_secs",
    "runner_class",
    "tags",
    "task_defaults",
    "tasks",
    "templates",
    "use",
    "when_full",
];

/// The task keys that are the pipeline's own, beside `steps`.
const OWN_TASK_KEYS: &[&str] = &["report"];

/// The kinds of report a task may say it writes, and how many of each.
const REPORT_KINDS: &[&str] = &["junit", "sarif"];
const MAX_REPORTS: usize = 8;
const MAX_REPORT_PATH: usize = 240;

/// The reports a task's steps write (`report:`), each a path in the task's
/// workspace. They are read when the steps are over, by what runs the
/// steps, and what they hold is said with the job.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reports {
    pub junit: Vec<String>,
    pub sarif: Vec<String>,
}

impl Reports {
    pub fn is_empty(&self) -> bool {
        self.junit.is_empty() && self.sarif.is_empty()
    }
}

/// Whether `path` is a report's path: in the workspace, with no way out of
/// it, and of characters that are themselves in every place the path goes
/// (a variable of the task, a line of several).
pub(crate) fn is_report_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= MAX_REPORT_PATH
        && !path.starts_with('/')
        && path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// The keys a service takes (dagron's `ServiceSpec`).
pub const SERVICE_KEYS: &[&str] = &[
    "args",
    "env",
    "image",
    "name",
    "ready",
    "ready_timeout_secs",
    "resources",
];

/// The most services a task may have, and the longest a service's `ready`
/// may be given, as dagron has them.
pub const MAX_SERVICES: usize = 8;

/// The name of a task's Docker daemon among its services, as dagron names
/// it (`dagron_core::docker_daemon::NAME`).
pub const DOCKER_SERVICE: &str = "docker";
pub const MAX_READY_TIMEOUT_SECS: u64 = 600;

/// A checked pipeline: the IR.
#[derive(Clone, Debug, PartialEq)]
pub struct Pipeline {
    pub name: String,
    pub on: Triggers,
    pub concurrency: Option<Concurrency>,
    pub tasks: Vec<Task>,
    /// dagron's own workflow keys (`parameters`, `task_defaults`, `notify`,
    /// …), as written. Never `name`, `tasks` or `concurrency_key`.
    pub spec: Map<String, Value>,
}

/// `on:`. An absent event never runs the pipeline.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Triggers {
    pub push: Option<PushFilter>,
    pub pull_request: Option<PullRequestFilter>,
    pub merge_group: Option<MergeGroupFilter>,
    pub dispatch: Option<Dispatch>,
    /// `on.failed`: a run of another pipeline of the repository failed.
    pub failed: Option<FailedFilter>,
    /// `on.schedule`: each cron, as written. A GitHub workflow's only.
    pub schedule: Vec<String>,
}

/// `on.failed`: the pipelines whose failed runs run this one, by the name
/// their checks carry (their file's name), and the branches it runs for: a
/// push's branch, or the branch a pull request merges into.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FailedFilter {
    pub pipelines: Vec<String>,
    pub branches: Option<Patterns>,
}

/// `on.push`. With neither `branches` nor `tags`, every branch and tag
/// pushes; with one of them, only that kind. `paths` is not read for tags.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PushFilter {
    pub branches: Option<Patterns>,
    pub tags: Option<Patterns>,
    pub paths: Option<Patterns>,
}

/// `on.pull_request`. `branches` is the branch a pull request merges into.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PullRequestFilter {
    pub branches: Option<Patterns>,
    pub paths: Option<Patterns>,
}

/// `on.merge_group`. `branches` is the branch the queue merges into.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MergeGroupFilter {
    pub branches: Option<Patterns>,
}

/// `on.dispatch`: runs asked for through the run API, and the inputs they
/// take.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Dispatch {
    pub inputs: BTreeMap<String, Input>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Input {
    pub description: Option<String>,
    pub default: Option<String>,
    pub required: bool,
}

/// `concurrency:`.
#[derive(Clone, Debug, PartialEq)]
pub struct Concurrency {
    /// A template; the one place an outsider's text, such as `ci_ref`, may go.
    pub key: String,
    /// A newer run of the key takes the place of the ones before it: they
    /// are cancelled, and it runs when what they had running has stopped
    /// (dagron's `when_full: supersede`). Without it a newer run waits its
    /// turn (`when_full: queue`).
    pub cancel_superseded: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Task {
    pub name: String,
    /// The task's steps; empty for a task that runs dagron's own `command:`,
    /// or none at all (an approval, a call, a wait).
    pub steps: Vec<Step>,
    pub depends_on: Vec<String>,
    /// dagron's other task keys, as written.
    pub fields: Map<String, Value>,
    /// The reports its steps write (`report:`).
    pub reports: Reports,
    /// What a GitHub workflow's job is beside its steps, which the native
    /// format has no key for. A task that has it is run by dagron-ci-exec
    /// alone, under step protocol 3.
    pub job: Option<JobSpec>,
    /// What the step runner does around its steps for a GitLab job: the
    /// artifacts it hands on and takes, and how a failure of its own ends.
    /// A task that has any of it is handed on in step protocol 5.
    pub hooks: TaskHooks,
}

/// What the step runner does around a GitLab job's steps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskHooks {
    /// Its artifacts, uploaded to the engine's artifact service, by name
    /// for the run, when its steps are over.
    pub upload: Option<ArtifactUpload>,
    /// The artifacts, by name, put in the workspace before the first step.
    pub download: Vec<String>,
    /// Whether a failure of its own ends it as succeeded, said to have
    /// failed: GitLab's `allow_failure`, after which the stages that follow
    /// run and an `on_failure` job does not.
    pub allow_failure: bool,
}

impl TaskHooks {
    pub fn is_empty(&self) -> bool {
        self.upload.is_none() && self.download.is_empty() && !self.allow_failure
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArtifactUpload {
    /// The artifact's name in the run: the task's own.
    pub name: String,
    /// Paths and globs of the workspace, and what of them is left out.
    pub paths: Vec<String>,
    pub exclude: Vec<String>,
    /// `on_success`, `on_failure` or `always`: as the steps went.
    pub when: String,
    /// How long it is kept, in days; the operator's longest where `None`.
    pub expire_days: Option<u64>,
}

/// What a GitHub workflow's job is beside its steps: what it reads of the
/// jobs it needs, its own `if:`, and the outputs it hands on. All of it is
/// the step runner's, as the job starts and as it ends.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobSpec {
    /// Its `if:`, which the step runner decides before the first step.
    pub condition: Option<String>,
    /// The tasks it needs, each with the id of the job it is, or is an
    /// instance of. `None` for a job that was left out when the run was
    /// made: it is skipped, and has no task.
    pub needs: Vec<(String, Option<String>)>,
    /// Its outputs: a name, and a text that may hold expressions.
    pub outputs: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Step {
    pub name: Option<String>,
    pub run: String,
    pub continue_on_error: bool,
    pub timeout_secs: Option<u64>,
    pub env: BTreeMap<String, String>,
    // What follows is a GitHub workflow's step's, which the native format
    // has no key for. A step that has any of it is run by dagron-ci-exec
    // alone, under step protocol 2 ([`Step::needs_the_runner`]).
    /// Its `id:`, which later steps read its outputs by.
    pub id: Option<String>,
    /// Its shell: `bash`, `sh`, `python`, `pwsh`, or a command with `{0}`
    /// where the script's file goes.
    pub shell: Option<String>,
    /// Where it starts, in the workspace. May hold expressions.
    pub working_directory: Option<String>,
    /// Its `if:`, evaluated as the job runs.
    pub condition: Option<String>,
    /// Variables whose values hold expressions, evaluated as the job runs.
    pub late_env: BTreeMap<String, String>,
    /// The expressions the script's `__CI_E<n>` variables stand for, from 1.
    pub exprs: Vec<String>,
    /// The local action it uses in place of a script: `./` and a directory
    /// of the workspace, read as the step starts (step protocol 4).
    pub uses: Option<String>,
    /// The inputs it gives that action: a name, and a text that may hold
    /// expressions.
    pub with: Vec<(String, String)>,
}

impl Step {
    /// Whether the step has what only dagron-ci-exec can run: anything the
    /// shell fallback of step protocol 1 has no word for.
    pub fn needs_the_runner(&self) -> bool {
        self.id.is_some()
            || self.shell.is_some()
            || self.working_directory.is_some()
            || self.condition.is_some()
            || !self.late_env.is_empty()
            || !self.exprs.is_empty()
            || self.uses.is_some()
    }
}

/// One thing wrong with a pipeline, and where.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Problem {
    /// `tasks[2].steps[0].run`, or empty for the file as a whole.
    pub path: String,
    pub message: String,
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.path, self.message)
        }
    }
}

/// Read a pipeline file. Every problem found is returned, not just the first.
pub fn parse(text: &str) -> Result<Pipeline, Vec<Problem>> {
    let value: Value = match serde_yaml::from_str(text) {
        Ok(value) => value,
        Err(e) => {
            return Err(vec![Problem {
                path: String::new(),
                message: format!("not YAML a pipeline can be read from: {e}"),
            }])
        }
    };
    let mut check = Check::default();
    let pipeline = check.pipeline(&value);
    match (pipeline, check.problems.is_empty()) {
        (Some(pipeline), true) => Ok(pipeline),
        _ => Err(check.problems),
    }
}

#[derive(Default)]
struct Check {
    problems: Vec<Problem>,
}

impl Check {
    fn problem(&mut self, path: &str, message: impl Into<String>) {
        self.problems.push(Problem {
            path: path.to_string(),
            message: message.into(),
        });
    }

    fn pipeline(&mut self, value: &Value) -> Option<Pipeline> {
        let Some(top) = value.as_object() else {
            self.problem("", "a pipeline is a mapping with name, on and tasks");
            return None;
        };
        for key in top.keys() {
            let ours = matches!(key.as_str(), "on" | "concurrency");
            if key == "concurrency_key" {
                self.problem(
                    key,
                    "set concurrency: { key: … } instead; dagron-ci compiles it to concurrency_key",
                );
            } else if key == "when_full" {
                // What a full key does is dagron-ci's to say, from
                // `concurrency`: one written here would say it twice.
                self.problem(
                    key,
                    "is dagron-ci's to say: a run over its key waits for a turn, and with \
                     concurrency: { cancel_superseded: true } it takes the place of the run before it",
                );
            } else if !ours && !DAGRON_SPEC_KEYS.contains(&key.as_str()) {
                let mut known = DAGRON_SPEC_KEYS.to_vec();
                known.extend(["on", "concurrency"]);
                self.problem(key, unknown_key("a pipeline", key, &known));
            }
        }

        let name = match top.get("name") {
            Some(Value::String(name)) if is_name(name) => Some(name.clone()),
            Some(Value::String(name)) => {
                self.problem(
                    "name",
                    format!("{name:?} must be letters, digits, '.', '_' or '-', at most 100"),
                );
                None
            }
            Some(_) => {
                self.problem("name", "must be a string");
                None
            }
            None => {
                self.problem("name", "is required");
                None
            }
        };
        let on = match top.get("on") {
            Some(on) => self.triggers(on),
            None => {
                self.problem("on", "is required: which events run this pipeline");
                None
            }
        };
        // A run of a pipeline on its own failure runs nothing new, and a
        // failure's own run raises no failure: one would fail twice.
        if let (Some(name), Some(failed)) = (&name, on.as_ref().and_then(|on| on.failed.as_ref())) {
            if failed.pipelines.iter().any(|p| p == name) {
                self.problem(
                    "on.failed",
                    format!(
                        "names {name:?}, this pipeline: a pipeline does not run on its own failure"
                    ),
                );
            }
        }
        let concurrency = top.get("concurrency").and_then(|c| self.concurrency(c));
        // dagron refuses a key without a cap at submit: catch it here.
        let cap = top.get("max_active_runs");
        if concurrency.is_some() && cap.is_some_and(|c| c.as_u64().is_none_or(|n| n == 0)) {
            self.problem(
                "max_active_runs",
                "must be at least 1 beside concurrency: a concurrency key only scopes that cap",
            );
        }
        // One run takes another's place, and not one of several: dagron
        // refuses the two together at submit, so it is caught here.
        if concurrency.as_ref().is_some_and(|c| c.cancel_superseded)
            && cap.is_some_and(|c| c.as_u64().is_some_and(|n| n > 1))
        {
            self.problem(
                "max_active_runs",
                "must be 1, or left out, beside concurrency.cancel_superseded: a new run takes \
                 the place of the one before it",
            );
        }

        let parameters: BTreeSet<String> = match top.get("parameters") {
            None | Some(Value::Null) => BTreeSet::new(),
            Some(Value::Object(map)) => {
                for name in map.keys().filter(|n| n.starts_with("ci_")) {
                    self.problem(
                        &format!("parameters.{name}"),
                        "names starting ci_ are dagron-ci's, set from the event",
                    );
                }
                map.keys().cloned().collect()
            }
            Some(_) => {
                self.problem("parameters", "must be a mapping of names to defaults");
                BTreeSet::new()
            }
        };
        // The compiler writes task_defaults again, with the event's
        // variables in its env: one that is not a mapping would be left out
        // of the run without a word.
        match top.get("task_defaults") {
            None | Some(Value::Null) => {}
            Some(Value::Object(defaults)) => {
                self.dagron_env("task_defaults.env", defaults.get("env"));
                self.env_from("task_defaults.env_from", defaults.get("env_from"));
            }
            Some(_) => self.problem(
                "task_defaults",
                "must be a mapping of the settings every task starts from",
            ),
        }
        let inputs: BTreeSet<String> = on
            .as_ref()
            .and_then(|on: &Triggers| on.dispatch.as_ref())
            .map(|d| d.inputs.keys().cloned().collect())
            .unwrap_or_default();
        let nameable: BTreeSet<String> = parameters.difference(&inputs).cloned().collect();

        let tasks = match top.get("tasks") {
            Some(Value::Array(tasks)) if !tasks.is_empty() => self.tasks(tasks, &nameable),
            Some(Value::Array(_)) => {
                self.problem("tasks", "needs at least one task");
                Vec::new()
            }
            Some(_) => {
                self.problem("tasks", "must be a list of tasks");
                Vec::new()
            }
            None => {
                self.problem("tasks", "is required");
                Vec::new()
            }
        };
        // Calls expand into tasks this file does not name; dagron checks those.
        if !top.contains_key("templates") && !top.contains_key("use") {
            self.graph(&tasks);
        }

        let spec: Map<String, Value> = top
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "name" | "on" | "concurrency" | "tasks"))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Some(Pipeline {
            name: name?,
            on: on?,
            concurrency,
            tasks,
            spec,
        })
    }

    fn triggers(&mut self, on: &Value) -> Option<Triggers> {
        let Some(map) = on.as_object().filter(|m| !m.is_empty()) else {
            self.problem(
                "on",
                format!(
                    "must map at least one event ({}) to its filters",
                    TRIGGERS.join(", ")
                ),
            );
            return None;
        };
        let mut triggers = Triggers::default();
        for (event, filters) in map {
            let path = format!("on.{event}");
            // A list, or a mapping of its own: read below.
            if event == "failed" {
                continue;
            }
            let filters = match filters {
                Value::Null => Map::new(),
                Value::Object(m) => m.clone(),
                _ => {
                    self.problem(&path, "must be a mapping of filters, or empty");
                    continue;
                }
            };
            match event.as_str() {
                "push" => {
                    self.only(
                        &path,
                        &filters,
                        &[
                            "branches",
                            "branches_ignore",
                            "tags",
                            "tags_ignore",
                            "paths",
                            "paths_ignore",
                        ],
                    );
                    triggers.push = Some(PushFilter {
                        branches: self.patterns(&path, &filters, "branches"),
                        tags: self.patterns(&path, &filters, "tags"),
                        paths: self.patterns(&path, &filters, "paths"),
                    });
                }
                "pull_request" => {
                    self.only(
                        &path,
                        &filters,
                        &["branches", "branches_ignore", "paths", "paths_ignore"],
                    );
                    triggers.pull_request = Some(PullRequestFilter {
                        branches: self.patterns(&path, &filters, "branches"),
                        paths: self.patterns(&path, &filters, "paths"),
                    });
                }
                "merge_group" => {
                    self.only(&path, &filters, &["branches", "branches_ignore"]);
                    triggers.merge_group = Some(MergeGroupFilter {
                        branches: self.patterns(&path, &filters, "branches"),
                    });
                }
                "dispatch" => {
                    self.only(&path, &filters, &["inputs"]);
                    triggers.dispatch = Some(Dispatch {
                        inputs: self.inputs(&path, filters.get("inputs")),
                    });
                }
                other => self.problem(&path, unknown_key("on", other, TRIGGERS)),
            }
        }
        // `on.failed` is a list of pipelines, or a mapping of them and the
        // branches: read apart, as the other events' filters are mappings.
        if let Some(failed) = map.get("failed") {
            triggers.failed = self.failed("on.failed", failed);
        }
        Some(triggers)
    }

    /// `on.failed`: `[ci]`, or `{ pipelines: [ci], branches: [main] }`.
    fn failed(&mut self, path: &str, value: &Value) -> Option<FailedFilter> {
        let (pipelines, branches) = match value {
            Value::Array(_) | Value::String(_) => (self.strings(path, value)?, None),
            Value::Object(filters) => {
                self.only(path, filters, &["branches", "branches_ignore", "pipelines"]);
                let Some(pipelines) = filters.get("pipelines") else {
                    self.problem(
                        &format!("{path}.pipelines"),
                        "is required: the pipelines whose failed runs run this one",
                    );
                    return None;
                };
                let pipelines = self.strings(&format!("{path}.pipelines"), pipelines)?;
                (pipelines, self.patterns(path, filters, "branches"))
            }
            _ => {
                self.problem(
                    path,
                    "must list the pipelines whose failed runs run this one, or map pipelines \
                     and branches",
                );
                return None;
            }
        };
        if pipelines.is_empty() {
            self.problem(path, "names no pipeline: nothing would run this one");
            return None;
        }
        for (i, name) in pipelines.iter().enumerate() {
            if !is_name(name) {
                self.problem(
                    &format!("{path}[{i}]"),
                    format!(
                        "{name:?} is no pipeline's name: a pipeline is named by its file in \
                         .dagron/ci, letters, digits, '.', '_' and '-'"
                    ),
                );
            }
        }
        Some(FailedFilter {
            pipelines,
            branches,
        })
    }

    fn only(&mut self, path: &str, map: &Map<String, Value>, allowed: &[&str]) {
        for key in map.keys() {
            if !allowed.contains(&key.as_str()) {
                self.problem(&format!("{path}.{key}"), unknown_key(path, key, allowed));
            }
        }
    }

    /// `name` or `name_ignore`, never both.
    fn patterns(&mut self, path: &str, map: &Map<String, Value>, name: &str) -> Option<Patterns> {
        let ignore = format!("{name}_ignore");
        let (key, list) = match (map.get(name), map.get(&ignore)) {
            (Some(_), Some(_)) => {
                self.problem(
                    &format!("{path}.{ignore}"),
                    format!("set {name} or {ignore}, not both; {name} takes ! patterns"),
                );
                return None;
            }
            (Some(list), None) => (name.to_string(), list),
            (None, Some(list)) => (ignore, list),
            (None, None) => return None,
        };
        let path = format!("{path}.{key}");
        let strings = self.strings(&path, list)?;
        if strings.is_empty() {
            self.problem(
                &path,
                "is empty, so nothing matches; leave it out to match everything",
            );
            return None;
        }
        let compiled = if key == name {
            Patterns::include(&strings)
        } else {
            Patterns::ignore(&strings)
        };
        compiled.map_err(|e| self.problem(&path, e)).ok()
    }

    /// A task's `report:`: for each kind, a path or a list of them.
    fn reports(&mut self, path: &str, value: &Value, no_steps: bool) -> Reports {
        let mut reports = Reports::default();
        let Value::Object(map) = value else {
            self.problem(
                path,
                "must be a mapping: junit and sarif, each a path or a list",
            );
            return reports;
        };
        if no_steps {
            self.problem(path, "a task with no steps writes no report");
        }
        for (kind, value) in map {
            let at = format!("{path}.{kind}");
            if !REPORT_KINDS.contains(&kind.as_str()) {
                self.problem(&at, unknown_key("a report", kind, REPORT_KINDS));
                continue;
            }
            let Some(paths) = self.strings(&at, value) else {
                continue;
            };
            if paths.is_empty() || paths.len() > MAX_REPORTS {
                self.problem(&at, format!("names 1 to {MAX_REPORTS} files"));
                continue;
            }
            if let Some(bad) = paths.iter().find(|p| !is_report_path(p)) {
                self.problem(
                    &at,
                    format!(
                        "{bad:?} is not a path in the workspace: letters, digits, '.', '_', \
                         '-' and '/', with no '..', at most {MAX_REPORT_PATH}"
                    ),
                );
                continue;
            }
            match kind.as_str() {
                "junit" => reports.junit = paths,
                _ => reports.sarif = paths,
            }
        }
        reports
    }

    fn strings(&mut self, path: &str, value: &Value) -> Option<Vec<String>> {
        let list = match value {
            Value::String(s) => vec![s.clone()],
            Value::Array(items) => {
                let mut list = Vec::with_capacity(items.len());
                for (i, item) in items.iter().enumerate() {
                    match item {
                        Value::String(s) => list.push(s.clone()),
                        _ => {
                            self.problem(&format!("{path}[{i}]"), "must be a string");
                            return None;
                        }
                    }
                }
                list
            }
            _ => {
                self.problem(path, "must be a list of strings");
                return None;
            }
        };
        Some(list)
    }

    fn inputs(&mut self, path: &str, value: Option<&Value>) -> BTreeMap<String, Input> {
        let mut inputs = BTreeMap::new();
        let map = match value {
            None | Some(Value::Null) => return inputs,
            Some(Value::Object(map)) => map,
            Some(_) => {
                self.problem(
                    &format!("{path}.inputs"),
                    "must map input names to their settings",
                );
                return inputs;
            }
        };
        for (name, settings) in map {
            let at = format!("{path}.inputs.{name}");
            if !is_parameter(name) || name.starts_with("ci_") {
                self.problem(
                    &at,
                    "an input is a parameter name: a letter or _, then letters, digits or _, not starting ci_",
                );
                continue;
            }
            let settings = match settings {
                Value::Null => Map::new(),
                Value::Object(m) => m.clone(),
                _ => {
                    self.problem(&at, "must be a mapping (description, default, required)");
                    continue;
                }
            };
            self.only(&at, &settings, &["default", "description", "required"]);
            let text = |v: Option<&Value>| match v {
                None | Some(Value::Null) => Some(None),
                Some(Value::String(s)) => Some(Some(s.clone())),
                Some(Value::Number(n)) => Some(Some(n.to_string())),
                Some(Value::Bool(b)) => Some(Some(b.to_string())),
                Some(_) => None,
            };
            let (Some(description), Some(default)) = (
                text(settings.get("description")),
                text(settings.get("default")),
            ) else {
                self.problem(&at, "description and default must be text");
                continue;
            };
            let required = match settings.get("required") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(_) => {
                    self.problem(&format!("{at}.required"), "must be true or false");
                    false
                }
            };
            inputs.insert(
                name.clone(),
                Input {
                    description,
                    default,
                    required,
                },
            );
        }
        inputs
    }

    fn concurrency(&mut self, value: &Value) -> Option<Concurrency> {
        let Some(map) = value.as_object() else {
            self.problem(
                "concurrency",
                "must be a mapping: { key, cancel_superseded }",
            );
            return None;
        };
        self.only("concurrency", map, &["cancel_superseded", "key"]);
        let key = match map.get("key") {
            Some(Value::String(key)) if !key.trim().is_empty() => key.clone(),
            _ => {
                self.problem(
                    "concurrency.key",
                    "is required: a template such as \"{{ ci_group }}\"",
                );
                return None;
            }
        };
        let cancel_superseded = match map.get("cancel_superseded") {
            None | Some(Value::Null) | Some(Value::Bool(false)) => false,
            Some(Value::Bool(true)) => true,
            Some(_) => {
                self.problem("concurrency.cancel_superseded", "must be true or false");
                false
            }
        };
        Some(Concurrency {
            key,
            cancel_superseded,
        })
    }

    fn tasks(&mut self, tasks: &[Value], nameable: &BTreeSet<String>) -> Vec<Task> {
        let mut out = Vec::with_capacity(tasks.len());
        let mut seen = BTreeSet::new();
        for (i, value) in tasks.iter().enumerate() {
            let path = format!("tasks[{i}]");
            let Some(map) = value.as_object() else {
                self.problem(&path, "a task is a mapping with a name");
                continue;
            };
            let name = match map.get("name") {
                Some(Value::String(n)) if is_name(n) => n.clone(),
                _ => {
                    self.problem(
                        &format!("{path}.name"),
                        "is required: letters, digits, '.', '_' or '-', at most 100",
                    );
                    continue;
                }
            };
            let path = format!("tasks[{i}] ({name})");
            if !seen.insert(name.clone()) {
                self.problem(&path, "a second task by this name");
            }
            for key in map.keys() {
                if key != "steps"
                    && !OWN_TASK_KEYS.contains(&key.as_str())
                    && !DAGRON_TASK_KEYS.contains(&key.as_str())
                {
                    let mut known = DAGRON_TASK_KEYS.to_vec();
                    known.push("steps");
                    known.extend(OWN_TASK_KEYS);
                    self.problem(&format!("{path}.{key}"), unknown_key("a task", key, &known));
                }
            }
            let runs_nothing = ["template", "workflow"]
                .iter()
                .find(|k| map.contains_key(**k))
                .map(|k| k.to_string())
                .or_else(|| match map.get("type") {
                    Some(Value::String(t)) if t != "task" => Some(format!("type: {t}")),
                    _ => None,
                });
            self.dagron_env(&format!("{path}.env"), map.get("env"));
            self.env_from(&format!("{path}.env_from"), map.get("env_from"));
            self.workspace(&format!("{path}.workspace"), map.get("workspace"));
            self.identity(&format!("{path}.identity"), map.get("identity"));
            self.forge_token(&format!("{path}.forge_token"), map);
            match (&runs_nothing, map.get("services")) {
                (Some(kind), Some(services)) if !services.is_null() => self.problem(
                    &format!("{path}.services"),
                    format!("a service is beside a command while it runs, and a task with {kind} runs none"),
                ),
                _ => self.services(&format!("{path}.services"), map.get("services"), nameable),
            }
            self.docker_daemon(&path, map, runs_nothing.as_deref());
            let steps = match map.get("steps") {
                None => Vec::new(),
                Some(_) if map.contains_key("command") => {
                    self.problem(&path, "set steps or command, not both");
                    Vec::new()
                }
                Some(_) if runs_nothing.is_some() => {
                    self.problem(
                        &path,
                        format!(
                            "steps run a command, and a task with {} runs none",
                            runs_nothing.unwrap_or_default()
                        ),
                    );
                    Vec::new()
                }
                Some(Value::Array(steps)) if !steps.is_empty() => {
                    self.steps(&path, steps, nameable)
                }
                Some(_) => {
                    self.problem(
                        &format!("{path}.steps"),
                        "must be a list of at least one step",
                    );
                    Vec::new()
                }
            };
            if let Some(command) = map.get("command") {
                let mut texts = Vec::new();
                collect_strings(command, &mut texts);
                for text in texts {
                    self.names(&format!("{path}.command"), text, nameable, false);
                }
            }
            if let Some(Value::String(when)) = map.get("when") {
                self.names(&format!("{path}.when"), when, nameable, true);
            }
            let depends_on = match map.get("depends_on") {
                None | Some(Value::Null) => Vec::new(),
                Some(value) => self
                    .strings(&format!("{path}.depends_on"), value)
                    .unwrap_or_default(),
            };
            let reports = match map.get("report") {
                None | Some(Value::Null) => Reports::default(),
                Some(value) => self.reports(&format!("{path}.report"), value, steps.is_empty()),
            };
            let fields = map
                .iter()
                .filter(|(k, _)| {
                    !matches!(k.as_str(), "name" | "steps" | "depends_on")
                        && !OWN_TASK_KEYS.contains(&k.as_str())
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            out.push(Task {
                name,
                steps,
                depends_on,
                fields,
                reports,
                job: None,
                hooks: TaskHooks::default(),
            });
        }
        out
    }

    fn steps(&mut self, task: &str, steps: &[Value], nameable: &BTreeSet<String>) -> Vec<Step> {
        if steps.len() > MAX_STEPS {
            self.problem(
                &format!("{task}.steps"),
                format!(
                    "has {} steps; a task may have at most {MAX_STEPS}",
                    steps.len()
                ),
            );
        }
        let mut out = Vec::with_capacity(steps.len());
        for (i, value) in steps.iter().enumerate() {
            let path = format!("{task}.steps[{i}]");
            let Some(map) = value.as_object() else {
                self.problem(&path, "a step is a mapping with run");
                continue;
            };
            self.only(&path, map, STEP_KEYS);
            let run = match map.get("run") {
                Some(Value::String(run)) if !run.trim().is_empty() => run.clone(),
                _ => {
                    self.problem(&format!("{path}.run"), "is required: the command to run");
                    continue;
                }
            };
            if run.len() > MAX_RUN_BYTES {
                self.problem(
                    &format!("{path}.run"),
                    format!(
                        "is over {} KiB; put a long script in a file in the repository",
                        MAX_RUN_BYTES >> 10
                    ),
                );
            }
            self.names(&format!("{path}.run"), &run, nameable, false);
            let name = match map.get("name") {
                None | Some(Value::Null) => None,
                Some(Value::String(n)) if !n.contains(char::is_control) => Some(n.clone()),
                Some(_) => {
                    self.problem(&format!("{path}.name"), "must be one line of text");
                    None
                }
            };
            let continue_on_error = match map.get("continue_on_error") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(_) => {
                    self.problem(
                        &format!("{path}.continue_on_error"),
                        "must be true or false",
                    );
                    false
                }
            };
            let timeout_secs = match map.get("timeout_secs") {
                None | Some(Value::Null) => None,
                Some(v) => match v.as_u64().filter(|&n| n >= 1) {
                    Some(n) => Some(n),
                    None => {
                        self.problem(
                            &format!("{path}.timeout_secs"),
                            "must be a whole number of at least 1",
                        );
                        None
                    }
                },
            };
            let env = self.env(&format!("{path}.env"), map.get("env"));
            out.push(Step {
                name,
                run,
                continue_on_error,
                timeout_secs,
                env,
                ..Step::default()
            });
        }
        let carried: usize = out
            .iter()
            .map(|s| s.run.len() + s.env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
            .sum();
        if carried > MAX_STEPS_BYTES {
            self.problem(
                &format!("{task}.steps"),
                format!(
                    "carry {} KiB of scripts and variables, over the {} KiB a task's environment \
                     can hold; put long scripts in files in the repository",
                    carried.div_ceil(1024),
                    MAX_STEPS_BYTES >> 10
                ),
            );
        }
        let command = compile::driver(&out).len();
        if command > MAX_COMMAND_BYTES {
            self.problem(
                &format!("{task}.steps"),
                format!(
                    "compile to a {} KiB command, over the 128 KiB one argument may be; \
                     split the task, or give its steps fewer variables",
                    command.div_ceil(1024)
                ),
            );
        }
        out
    }

    fn env(&mut self, path: &str, value: Option<&Value>) -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
        let map = match value {
            None | Some(Value::Null) => return env,
            Some(Value::Object(map)) => map,
            Some(_) => {
                self.problem(path, "must map variable names to values");
                return env;
            }
        };
        for (name, value) in map {
            let valid = name
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !valid {
                self.problem(&format!("{path}.{name}"), "is not a variable name");
                continue;
            }
            if name.starts_with("CI_") {
                self.problem(
                    &format!("{path}.{name}"),
                    "CI_ variables are set by dagron-ci from the event",
                );
                continue;
            }
            if name == compile::ACTIONS_CACHE_RESTORE_FROM {
                self.problem(&format!("{path}.{name}"), github::RESERVED_RESTORE_FROM);
                continue;
            }
            let text = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => {
                    self.problem(&format!("{path}.{name}"), "must be text");
                    continue;
                }
            };
            env.insert(name.clone(), text);
        }
        env
    }

    /// dagron's own `env:` list, on a task or `task_defaults`: no `CI_` names,
    /// which dagron-ci sets from the event. Its shape is dagron's to check.
    /// `workspace:` is dagron's (`size`), or `null` for none: a task with
    /// steps has one unless it says so. A size is checked here as dagron
    /// checks it, so that `validate` finds what a submit would be refused for.
    /// `checkout` is dagron-ci's: `false` for a workspace that starts empty
    /// rather than with the event's commit.
    /// `identity:` is dagron's own (`dagron_core::oidc`): OIDC tokens for the
    /// audiences named, which a trusted run's job asks dagron-api's issuer
    /// for. Checked here as dagron checks it.
    fn identity(&mut self, path: &str, value: Option<&Value>) {
        let Some(value) = value else { return };
        let only = |v: &Value, key: &str| {
            v.as_object()
                .filter(|m| m.len() == 1)
                .and_then(|m| m.get(key))
                .cloned()
        };
        let audiences: Option<Vec<String>> =
            match only(value, "oidc").and_then(|oidc| only(&oidc, "audience")) {
                Some(Value::String(one)) => Some(vec![one]),
                Some(Value::Array(many)) => many
                    .iter()
                    .map(|a| a.as_str().map(str::to_string))
                    .collect(),
                _ => None,
            };
        let ok = audiences.is_some_and(|list| {
            !list.is_empty()
                && list.len() <= 8
                && list.iter().all(|a| {
                    !a.is_empty()
                        && a.len() <= 256
                        && !a.contains("{{")
                        && !a.chars().any(|c| c.is_whitespace() || c.is_control())
                })
        });
        if !ok {
            self.problem(
                path,
                "is `{ oidc: { audience: … } }`: one audience, or a list of up to 8, each the name a cloud \
                 gives this installation's tokens (sts.amazonaws.com for AWS)",
            );
        }
    }

    /// `forge_token: { permissions: { contents: write, … } }`: a token for
    /// the repository the job checks out, as GitHub gives a workflow's job its
    /// `GITHUB_TOKEN`, by a GitHub App's names for what it may do, each `read`
    /// or `write`. The token service gives a run that is not trusted read
    /// alone, and revokes each once its attempt is over.
    fn forge_token(&mut self, path: &str, task: &Map<String, Value>) {
        let Some(value) = task.get("forge_token") else {
            return;
        };
        let permissions = value
            .as_object()
            .filter(|m| m.len() == 1)
            .and_then(|m| m.get("permissions"))
            .and_then(Value::as_object)
            .filter(|p| !p.is_empty());
        let ok = permissions.is_some_and(|p| {
            p.iter().all(|(name, level)| {
                crate::checkout::FORGE_TOKEN_PERMISSIONS.contains(&name.as_str())
                    && matches!(level.as_str(), Some("read" | "write"))
            })
        });
        if !ok {
            self.problem(
                path,
                format!(
                    "is `{{ permissions: {{ contents: write }} }}`: each of {} at read or write",
                    crate::checkout::FORGE_TOKEN_PERMISSIONS.join(", ")
                ),
            );
            return;
        }
        // The token is for the repository the job checks out: one with steps
        // or a workspace does, unless it says `checkout: false`.
        let checks_out = match task.get("workspace") {
            Some(Value::Object(ws)) => ws.get("checkout") != Some(&Value::Bool(false)),
            Some(_) => false,
            None => task.contains_key("steps"),
        };
        if !checks_out {
            self.problem(
                path,
                "is a token for the repository the job checks out, and this job checks out none: \
                 give it steps, or a workspace",
            );
        }
    }

    fn workspace(&mut self, path: &str, value: Option<&Value>) {
        let Some(map) = (match value {
            None | Some(Value::Null) => return,
            Some(Value::Object(map)) => Some(map),
            Some(_) => None,
        }) else {
            self.problem(
                path,
                "is a mapping (`{}`, `{ size: 20Gi }`, `{ checkout: false }`), or null for none",
            );
            return;
        };
        for (key, value) in map {
            match (key.as_str(), value) {
                ("size", Value::String(size)) if is_size(size) => {}
                ("size", _) => self.problem(
                    &format!("{path}.size"),
                    "is a size: a number and a unit, Ki Mi Gi Ti Pi Ei or k M G T P E (20Gi, 500M)",
                ),
                ("checkout", Value::Bool(_)) => {}
                // How deep the commit's history comes with it: dagron's own
                // `workspace.checkout.depth`, and nothing else of that key,
                // which names a repository and a commit that are the event's.
                ("checkout", Value::Object(checkout))
                    if checkout.len() == 1
                        && checkout
                            .get("depth")
                            .and_then(Value::as_u64)
                            .is_some_and(|depth| u32::try_from(depth).is_ok()) => {}
                ("checkout", _) => self.problem(
                    &format!("{path}.checkout"),
                    "is true or false, whether the workspace starts with the event's commit (true \
                     unless set), or `{ depth: N }` for how much of its history comes with it: 1 \
                     is the commit alone, and 0 is all of it",
                ),
                ("cache", Value::Array(caches)) => self.caches(&format!("{path}.cache"), caches),
                ("cache", _) => self.problem(
                    &format!("{path}.cache"),
                    "is a list of caches: `[{ prefix: cargo, key_files: [Cargo.lock], paths: \
                     [/usr/local/cargo/registry] }]`",
                ),
                (key, _) => self.problem(
                    &format!("{path}.{key}"),
                    unknown_key("a workspace", key, &["size", "checkout", "cache"]),
                ),
            }
        }
    }

    /// `workspace.cache`: directories a job keeps between runs, as dagron's
    /// own `workspace.cache` takes them (`dagron_core::workspace`), checked
    /// here as it checks them. Where a job restores from is dagron-ci's to
    /// say, not the pipeline's: its own ref, then the base branch of a pull
    /// request or merge group (`restore_from`, which the compiler writes).
    fn caches(&mut self, path: &str, caches: &[Value]) {
        if caches.len() > 8 {
            self.problem(path, format!("names {} caches; at most 8", caches.len()));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (i, cache) in caches.iter().enumerate() {
            let at = format!("{path}[{i}]");
            let Value::Object(map) = cache else {
                self.problem(&at, "is a mapping: `{ prefix, key_files, paths }`");
                continue;
            };
            let names = |v: &Value, most: usize, ok: &dyn Fn(&str) -> bool| {
                v.as_array().is_some_and(|a| {
                    a.len() <= most
                        && a.iter()
                            .all(|v| v.as_str().is_some_and(|s| !s.is_empty() && ok(s)))
                })
            };
            let climbs = |s: &str| s.split('/').any(|c| c == "..");
            for (key, value) in map {
                match (key.as_str(), value) {
                    ("prefix", Value::String(p))
                        if !p.is_empty()
                            && p.len() <= 64
                            && p.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')) =>
                    {
                        if !seen.insert(p.clone()) {
                            self.problem(&format!("{at}.prefix"), format!("{p:?} names another cache of this job too"));
                        }
                    }
                    ("prefix", _) => self.problem(
                        &format!("{at}.prefix"),
                        "is the cache's name, and the start of its keys: 1 to 64 letters, digits, '.', '_' or '-'",
                    ),
                    ("paths", v) if names(v, 16, &|s| !climbs(s)) && !v.as_array().is_some_and(Vec::is_empty) => {}
                    ("paths", _) => self.problem(
                        &format!("{at}.paths"),
                        "is a list of 1 to 16 directories the job keeps: absolute, `~/…` in its home, or \
                         relative to the workspace; none with '..'",
                    ),
                    ("key_files", v) if names(v, 32, &|s| !s.starts_with('/') && !climbs(s)) => {}
                    ("key_files", _) => self.problem(
                        &format!("{at}.key_files"),
                        "is a list of at most 32 globs of files in the workspace whose contents make the \
                         cache's key (`Cargo.lock`, `**/package-lock.json`)",
                    ),
                    ("restore_from", _) => self.problem(
                        &format!("{at}.restore_from"),
                        "is dagron-ci's to say: a job restores from its own ref, then from the base branch \
                         of a pull request or merge group",
                    ),
                    (key, _) => self.problem(
                        &format!("{at}.{key}"),
                        unknown_key("a cache", key, &["prefix", "key_files", "paths"]),
                    ),
                }
            }
            for needed in ["prefix", "paths"] {
                if !map.contains_key(needed) {
                    self.problem(&at, format!("names no {needed}"));
                }
            }
        }
    }

    /// A task's `docker_daemon:`, dagron's own key
    /// (`dagron_core::docker_daemon`), checked as dagron checks it: `true` or
    /// `false`, on a task that runs a command, and one more service.
    fn docker_daemon(&mut self, path: &str, map: &Map<String, Value>, runs_nothing: Option<&str>) {
        let at = format!("{path}.docker_daemon");
        match map.get("docker_daemon") {
            None | Some(Value::Bool(false)) => return,
            Some(Value::Bool(true)) => {}
            Some(_) => {
                self.problem(&at, "is true or false");
                return;
            }
        }
        if let Some(kind) = runs_nothing {
            self.problem(
                &at,
                format!(
                    "a daemon is beside a command while it runs, and a task with {kind} runs none"
                ),
            );
            return;
        }
        let services = map
            .get("services")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if services
            .iter()
            .any(|s| s.get("name").and_then(Value::as_str) == Some(DOCKER_SERVICE))
        {
            self.problem(
                &at,
                format!("the daemon is the service named '{DOCKER_SERVICE}', and the task has one of its own"),
            );
        } else if services.len() >= MAX_SERVICES {
            self.problem(
                &at,
                format!(
                    "the daemon is one more service, and the task has {}: a task may have {MAX_SERVICES}",
                    services.len()
                ),
            );
        }
    }

    /// A task's `services:`, which is dagron's own key
    /// (`dagron_core::service`, which dagron-ci cannot link; a test holds the
    /// two to one answer). Checked here as dagron checks it, so that
    /// `validate` finds what a submit would be refused for, and for what a
    /// service's image, arguments and `ready` may name: each is run, as a
    /// command is, so a `{{ … }}` in one is held as a command's is.
    fn services(&mut self, path: &str, value: Option<&Value>, nameable: &BTreeSet<String>) {
        let services = match value {
            None => return,
            Some(Value::Array(services)) => services,
            Some(_) => {
                self.problem(path, "is a list of services, each with a name and an image");
                return;
            }
        };
        if services.len() > MAX_SERVICES {
            self.problem(
                path,
                format!(
                    "has {} services; a task may have {MAX_SERVICES}",
                    services.len()
                ),
            );
        }
        let mut seen = BTreeSet::new();
        for (i, value) in services.iter().enumerate() {
            let at = format!("{path}[{i}]");
            let Some(map) = value.as_object() else {
                self.problem(&at, "a service is a mapping with a name and an image");
                continue;
            };
            self.only(&at, map, SERVICE_KEYS);
            match map.get("name") {
                Some(Value::String(name)) if is_service_name(name) => {
                    if !seen.insert(name.clone()) {
                        self.problem(
                            &format!("{at}.name"),
                            format!("a second service named {name:?}"),
                        );
                    }
                }
                Some(Value::String(name)) if name == "localhost" => self.problem(
                    &format!("{at}.name"),
                    "\"localhost\" is every container's name for itself",
                ),
                _ => self.problem(
                    &format!("{at}.name"),
                    "is required: lowercase letters, digits and '-', beginning with a letter \
                     and not ending with '-', at most 40. The task reaches the service by it",
                ),
            }
            match map.get("image") {
                Some(Value::String(image)) if is_image(image) => {
                    self.names(&format!("{at}.image"), image, nameable, false);
                }
                _ => self.problem(
                    &format!("{at}.image"),
                    "is required: the image the service runs",
                ),
            }
            match map.get("args") {
                None => {}
                Some(Value::Array(args)) if args.iter().all(is_text) => {
                    for arg in args.iter().filter_map(Value::as_str) {
                        self.names(&format!("{at}.args"), arg, nameable, false);
                    }
                }
                Some(_) => self.problem(
                    &format!("{at}.args"),
                    "is a list of strings: what the image's entrypoint is given",
                ),
            }
            self.service_env(&format!("{at}.env"), map.get("env"));
            let ready = match map.get("ready") {
                None | Some(Value::Null) => false,
                Some(Value::String(line)) if !line.trim().is_empty() => {
                    self.names(&format!("{at}.ready"), line, nameable, false);
                    true
                }
                Some(Value::Array(argv))
                    if !argv.is_empty()
                        && argv.iter().all(is_text)
                        && argv[0]
                            .as_str()
                            .is_none_or(|program| !program.trim().is_empty()) =>
                {
                    for arg in argv.iter().filter_map(Value::as_str) {
                        self.names(&format!("{at}.ready"), arg, nameable, false);
                    }
                    true
                }
                Some(_) => {
                    self.problem(
                        &format!("{at}.ready"),
                        "is a command that exits 0 once the service is ready: one line for \
                         `sh -c`, or a list of a program and its arguments. Leave it out for a \
                         service that is ready once started",
                    );
                    true
                }
            };
            match map.get("ready_timeout_secs") {
                None | Some(Value::Null) => {}
                Some(_) if !ready => self.problem(
                    &format!("{at}.ready_timeout_secs"),
                    "is how long ready has to succeed, and the service has no ready",
                ),
                Some(secs) => {
                    if !secs
                        .as_u64()
                        .is_some_and(|n| (1..=MAX_READY_TIMEOUT_SECS).contains(&n))
                    {
                        self.problem(
                            &format!("{at}.ready_timeout_secs"),
                            format!(
                                "is a whole number of seconds from 1 to {MAX_READY_TIMEOUT_SECS}"
                            ),
                        );
                    }
                }
            }
            match map.get("resources") {
                None | Some(Value::Null) => {}
                Some(Value::Object(resources)) => {
                    let at = format!("{at}.resources");
                    self.only(&at, resources, &["limits", "requests"]);
                    for (kind, amounts) in resources {
                        let texts = amounts.as_object().is_some_and(|m| m.values().all(is_text));
                        if !texts {
                            self.problem(
                                &format!("{at}.{kind}"),
                                "maps a resource to an amount, as text: { cpu: \"250m\", memory: \"256Mi\" }",
                            );
                        }
                    }
                }
                Some(_) => self.problem(
                    &format!("{at}.resources"),
                    "is a mapping: { requests: { … }, limits: { … } }",
                ),
            }
        }
    }

    /// A service's `env:`, in dagron's form: a list of names and values, and
    /// plain values alone, since no secret is given to a service.
    fn service_env(&mut self, path: &str, value: Option<&Value>) {
        let entries = match value {
            None => return,
            Some(Value::Array(entries)) => entries,
            Some(_) => {
                self.problem(
                    path,
                    "is a list of names and values, as dagron reads it: \
                     `- { name: X, value: \"y\" }`",
                );
                return;
            }
        };
        let mut seen = BTreeSet::new();
        for (i, entry) in entries.iter().enumerate() {
            let at = format!("{path}[{i}]");
            let Some(map) = entry.as_object() else {
                self.problem(&at, "is a name and a value: `{ name: X, value: \"y\" }`");
                continue;
            };
            self.only(&at, map, &["name", "value", "value_from"]);
            let named = map.get("name").and_then(Value::as_str).filter(|name| {
                name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                    && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            });
            let Some(name) = named else {
                self.problem(&at, "is a name and a value: `{ name: X, value: \"y\" }`");
                continue;
            };
            if !seen.insert(name.to_string()) {
                self.problem(&at, format!("{name} is set twice"));
            }
            if map.get("value_from").is_some_and(|v| !v.is_null()) {
                self.problem(
                    &at,
                    format!(
                        "{name} has a value_from: a service's variables are plain values, and \
                         no secret is given to a service"
                    ),
                );
            }
            if !map.get("value").is_none_or(is_text) {
                self.problem(&at, format!("{name}: its value is text"));
            }
        }
    }

    /// A task's `env:`, or the defaults', which is dagron's own: a list of
    /// names and values. It is checked here because the compiler builds a
    /// task's environment itself (the steps travel in it) and adds only to a
    /// list. Written as a map, the way a step's `env:` is, it would be left
    /// out of the workflow without a word.
    fn dagron_env(&mut self, path: &str, value: Option<&Value>) {
        let entries = match value {
            None | Some(Value::Null) => return,
            Some(Value::Array(entries)) => entries,
            Some(_) => {
                self.problem(
                    path,
                    "is a list of names and values, as dagron reads it: \
                     `- { name: X, value: \"y\" }` (a step's `env:` is a map)",
                );
                return;
            }
        };
        for (i, entry) in entries.iter().enumerate() {
            let at = format!("{path}[{i}]");
            let Some(Value::String(name)) = entry.get("name") else {
                self.problem(&at, "is a name and a value: `{ name: X, value: \"y\" }`");
                continue;
            };
            if name.starts_with("CI_") {
                self.problem(
                    &at,
                    format!("{name}: CI_ variables are set by dagron-ci from the event"),
                );
            }
            if name == compile::ACTIONS_CACHE_RESTORE_FROM {
                self.problem(&at, format!("{name} {}", github::RESERVED_RESTORE_FROM));
            }
        }
    }

    /// A task's `env_from`, dagron's own key: a variable given what an
    /// earlier task came to. dagron checks it when the run is asked for.
    /// Here it is held to what a task's `env` is: it names no `CI_`
    /// variable. Those carry a job's steps to what runs them, and one given
    /// from an earlier task would be a script, or a limit, that the task's
    /// last line chose.
    fn env_from(&mut self, path: &str, value: Option<&Value>) {
        let Some(Value::Array(entries)) = value else {
            return;
        };
        for (i, entry) in entries.iter().enumerate() {
            if let Some(Value::String(name)) = entry.get("name") {
                if name.starts_with("CI_") {
                    self.problem(
                        &format!("{path}[{i}]"),
                        format!("{name}: CI_ variables are set by dagron-ci from the event"),
                    );
                }
                if name == compile::ACTIONS_CACHE_RESTORE_FROM {
                    self.problem(
                        &format!("{path}[{i}]"),
                        format!("{name} {}", github::RESERVED_RESTORE_FROM),
                    );
                }
            }
        }
    }

    /// Refuse a `{{ … }}` in a command or a `when:` that names anything an
    /// outsider could choose, or anything unknown.
    fn names(&mut self, path: &str, text: &str, nameable: &BTreeSet<String>, in_when: bool) {
        for name in template_names(text) {
            let mut parts = name.split(['.', '[', ' ', '|']);
            let root = parts.next().unwrap_or_default();
            // dagron substitutes names only, and leaves anything else (a Helm
            // or Go template's `{{ .Values.x }}`) as written.
            if !root.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') {
                continue;
            }
            // `{{ params.x }}` names x; `{{ n - 1 }}` names n.
            let named = if root == "params" {
                parts.next().unwrap_or_default()
            } else {
                root
            };
            // A matrix's item and index, and an upstream task's output, come
            // from this pipeline's own tasks, not from the event. dagron reads
            // a task's output in `when:` alone.
            if root == "tasks" && !in_when {
                self.problem(
                    path,
                    "a task's output is read in when:, or fanned out with with_output_of; \
                     dagron substitutes it nowhere else",
                );
                continue;
            }
            let allowed = matches!(root, "item" | "index" | "tasks")
                || SAFE_PARAMETERS.contains(&named)
                || nameable.contains(named);
            if !allowed {
                let root = named;
                let hint = if root == "ci_ref" {
                    "a branch name is its pusher's text; read $CI_REF in the step instead"
                        .to_string()
                } else {
                    format!(
                        "{{{{ {root} }}}} is not a value this pipeline may put in a command; \
                         pass it as a step's env: and read it as a variable"
                    )
                };
                self.problem(path, hint);
            }
        }
    }

    /// Every `depends_on` names a task here, and the tasks form no cycle.
    fn graph(&mut self, tasks: &[Task]) {
        let names: BTreeSet<&str> = tasks.iter().map(|t| t.name.as_str()).collect();
        for task in tasks {
            for dep in &task.depends_on {
                if dep == &task.name {
                    self.problem(&format!("tasks ({})", task.name), "depends on itself");
                } else if !names.contains(dep.as_str()) {
                    self.problem(
                        &format!("tasks ({}).depends_on", task.name),
                        format!("names no task: {dep:?}"),
                    );
                }
            }
        }
        // Kahn's algorithm: whatever is left over sits on a cycle. A self-
        // dependency and an unknown name are reported above and not counted.
        let counted = |task: &Task, dep: &str| dep != task.name && names.contains(dep);
        let mut indegree: BTreeMap<&str, usize> = names.iter().map(|n| (*n, 0)).collect();
        for task in tasks {
            let n = task.depends_on.iter().filter(|d| counted(task, d)).count();
            *indegree.entry(task.name.as_str()).or_default() += n;
        }
        let mut ready: Vec<&str> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(n, _)| *n)
            .collect();
        while let Some(done) = ready.pop() {
            indegree.remove(done);
            for task in tasks {
                let hits = task
                    .depends_on
                    .iter()
                    .filter(|d| d.as_str() == done && counted(task, d))
                    .count();
                if let Some(d) = indegree.get_mut(task.name.as_str()).filter(|_| hits > 0) {
                    *d = d.saturating_sub(hits);
                    if *d == 0 {
                        ready.push(task.name.as_str());
                    }
                }
            }
        }
        let stuck: Vec<&str> = indegree.keys().copied().collect();
        if !stuck.is_empty() {
            self.problem(
                "tasks",
                format!("depends_on forms a cycle through {}", stuck.join(", ")),
            );
        }
    }
}

/// A pipeline or task name: what goes into a run's name and a log line.
pub fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 100
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// A value dagron reads as text: a string, or a number or a boolean written
/// bare, which YAML hands over as the text it was written with.
fn is_text(value: &Value) -> bool {
    matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_))
}

/// A service's name as dagron takes one (`dagron_core::service::check_name`):
/// a host name's label that begins with a letter, and not `localhost`.
pub(crate) fn is_service_name(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !s.ends_with('-')
        && s.len() <= 40
        && s != "localhost"
}

/// An image's name as dagron takes one for a service
/// (`dagron_core::service::check_image`), but for a parameter in it, which
/// is filled in when the run is made.
pub(crate) fn is_image(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 512
        && !s.chars().any(|c| c.is_control())
        && (s.contains("{{") || !s.chars().any(char::is_whitespace))
}

/// A workspace size as dagron takes one (`dagron_core::workspace`, which
/// dagron-ci cannot link; a test holds the two to one answer): a decimal
/// number and an optional binary or decimal unit, more than nothing, and
/// within what dagron counts in.
pub(crate) fn is_size(s: &str) -> bool {
    let end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (number, unit) = s.split_at(end);
    let multiplier: u128 = match unit {
        "" => 1,
        "Ki" => 1 << 10,
        "Mi" => 1 << 20,
        "Gi" => 1 << 30,
        "Ti" => 1 << 40,
        "Pi" => 1 << 50,
        "Ei" => 1 << 60,
        "k" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        "T" => 1_000_000_000_000,
        "P" => 1_000_000_000_000_000,
        "E" => 1_000_000_000_000_000_000,
        _ => return false,
    };
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() || fraction.contains('.') || fraction.len() > 18 {
        return false;
    }
    let parse = |p: &str| {
        if p.is_empty() {
            Some(0)
        } else {
            p.parse::<u128>().ok()
        }
    };
    let scale = 10u128.pow(fraction.len() as u32);
    let bytes = parse(whole)
        .and_then(|w| w.checked_mul(scale))
        .and_then(|w| w.checked_add(parse(fraction)?))
        .and_then(|n| n.checked_mul(multiplier));
    bytes.is_some_and(|b| b > 0)
}

/// A parameter name: a letter or `_`, then letters, digits or `_`.
pub fn is_parameter(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && s.len() <= 64
}

/// The names inside each `{{ … }}` of `text`, trimmed.
fn template_names(text: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find("{{") {
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            break;
        };
        names.push(after[..close].trim());
        rest = &after[close + 2..];
    }
    names
}

fn collect_strings<'a>(value: &'a Value, out: &mut Vec<&'a str>) {
    match value {
        Value::String(s) => out.push(s),
        Value::Array(items) => items.iter().for_each(|v| collect_strings(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect_strings(v, out)),
        _ => {}
    }
}

/// "unknown key …", with the nearest known key when one is close. Among
/// keys as close, one the typo merely drops or adds letters to wins: `taks`
/// is `tasks`, not `tags`.
fn unknown_key(what: &str, key: &str, known: &[&str]) -> String {
    let near = known
        .iter()
        .map(|k| {
            (
                distance(key, k),
                !(subsequence(key, k) || subsequence(k, key)),
                *k,
            )
        })
        .filter(|(d, _, _)| *d <= 2)
        .min();
    match near {
        Some((_, _, k)) => format!("{what} has no key {key:?}; did you mean {k:?}?"),
        None => format!("{what} has no key {key:?}"),
    }
}

/// Edit distance counting a swap of two neighbours as one edit (optimal
/// string alignment), for "did you mean": `rnu` is one edit from `run`.
fn distance(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut d = vec![vec![0_usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            d[i][j] = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
            }
        }
    }
    d[a.len()][b.len()]
}

/// Whether `short`'s characters appear in `long`, in order.
fn subsequence(short: &str, long: &str) -> bool {
    let mut long = long.chars();
    short.chars().all(|c| long.any(|l| l == c))
}

#[cfg(test)]
mod tests;
