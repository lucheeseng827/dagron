//! The GitLab CI front-end, level 1: a project's `.gitlab-ci.yml` read into
//! the same IR a native pipeline is, for the event at hand.
//!
//! Level 1 is the file itself, and the files it includes from the project:
//! its stages and jobs, their scripts, `rules`, `needs`, `parallel`,
//! `artifacts`, `cache`, `environment`, `image`, `services`, `tags` and
//! `variables`; and what makes a file of many jobs short, `default`,
//! `include: local`, `extends`, `!reference` and YAML's own anchors. What
//! comes from outside the project (`include: project`, `remote`,
//! `template`), child pipelines, `id_tokens` and `secrets` are level 2 and
//! 3, and are refused, with what is refused: nothing in a file is passed
//! over without a word.
//!
//! **Which file.** None, until the project says so. `.dagron/ci.yaml`
//! names it (`gitlab: pipeline: .gitlab-ci.yml`), and the operator turns the
//! front-end on (`CI_GITLAB_PIPELINES=on`), since a project that has one may
//! have GitLab running it already.
//!
//! **Two readings.** [`parse`] reads the file for what does not depend on an
//! event: its shape, its includes, its templates and references resolved,
//! and every `rules:if` read. [`Ci::lower`] then reads it for an event: the
//! predefined variables GitLab gives a pipeline, `workflow:rules`, each
//! job's `rules`, its stage, `needs` and `parallel`, as GitLab makes a
//! pipeline before any job runs.
//!
//! **What GitLab does that is done the same way.**
//! - A job without `rules` is in a branch's or a tag's pipeline and not in a
//!   merge request's, as GitLab's default `only: [branches, tags]` has it.
//! - A job waits for every job of the stages before its own, unless it
//!   `needs` others, and then it waits for those alone.
//! - Its `before_script` and `script` run in one shell, which stops at the
//!   first command that fails; its `after_script` runs after, whatever
//!   became of them.
//! - The artifacts of the jobs it waits for (its `needs`, else every earlier
//!   stage's, else its `dependencies`) are in its workspace before its
//!   script starts.
//! - A variable is a variable of the job's shell, and `$NAME` in a script is
//!   the shell's to expand.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};
use serde_yaml::Value as Yaml;

use super::compile::{Changes, Event};
use super::glob::Patterns;
use super::{Pipeline, Problem, Reports, Step, Task, TaskHooks, Triggers};

pub mod rules;

/// The file a project's pipeline is in, unless its settings say otherwise.
pub const DEFAULT_FILE: &str = ".gitlab-ci.yml";

/// The appendix that says what of GitLab CI is taken: named in refusals.
const MATRIX: &str = "docs/CI_GITLAB.md";

/// The most files a pipeline may be made of, itself and what it includes,
/// as GitLab has it (150), and how deep includes may nest.
pub const MAX_FILES: usize = 150;
const MAX_INCLUDE_DEPTH: usize = 100;

/// How deep `extends` and `!reference` may nest, as GitLab has them.
const MAX_EXTENDS: usize = 11;
const MAX_REFERENCE: usize = 10;

/// The most jobs one pipeline may become, `parallel` expanded.
pub const MAX_JOBS: usize = 256;

/// GitLab's own ceiling on `parallel`.
const MAX_PARALLEL: u64 = 200;

/// The stages a pipeline has when it names none, and the two that are
/// always first and last.
const DEFAULT_STAGES: &[&str] = &["build", "test", "deploy"];
const PRE: &str = ".pre";
const POST: &str = ".post";

/// Top-level keys that are no job.
const GLOBAL_KEYS: &[&str] = &[
    "after_script",
    "before_script",
    "cache",
    "default",
    "image",
    "include",
    "services",
    "stages",
    "variables",
    "workflow",
];

/// What `default:` may set for every job.
const DEFAULT_KEYS: &[&str] = &[
    "after_script",
    "artifacts",
    "before_script",
    "cache",
    "image",
    "interruptible",
    "retry",
    "services",
    "tags",
    "timeout",
];

/// A job's keys that are taken.
const JOB_KEYS: &[&str] = &[
    "after_script",
    "allow_failure",
    "artifacts",
    "before_script",
    "cache",
    "coverage",
    "dependencies",
    "environment",
    "extends",
    "image",
    "interruptible",
    "needs",
    "parallel",
    "retry",
    "rules",
    "script",
    "services",
    "stage",
    "tags",
    "timeout",
    "variables",
    "when",
];

/// A job's keys that are refused, each with why.
const REFUSED_JOB_KEYS: &[(&str, &str)] = &[
    ("except", "is GitLab's older form: write it as rules:"),
    ("only", "is GitLab's older form: write it as rules:"),
    ("hooks", "is not taken"),
    ("id_tokens", "is level 3, not taken yet"),
    ("identity", "is not taken"),
    ("inherit", "is not taken: a job has every default"),
    ("manual_confirmation", "is not taken"),
    ("pages", "is GitLab Pages, which dagron has not"),
    ("release", "is GitLab's release page, which dagron has not"),
    ("resource_group", "is not taken yet"),
    ("secrets", "is level 3, not taken yet"),
    ("start_in", "is a delayed job, which is not taken"),
    (
        "trigger",
        "is a child or downstream pipeline, which is level 2",
    ),
    ("dast_configuration", "is not taken"),
    ("run", "is GitLab's CI steps, which are not taken"),
];

/// A repository's settings for this front-end: `.dagron/ci.yaml`, under
/// `gitlab:`.
///
/// ```yaml
/// gitlab:
///   pipeline: .gitlab-ci.yml
///   tags:
///     docker: docker.io/library/alpine:3.20
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Settings {
    /// The file the pipeline is in, from the repository's root. None: the
    /// project runs no `.gitlab-ci.yml` here.
    pub pipeline: Option<String>,
    /// The image a runner tag means, for a job that names no `image:`.
    pub tags: BTreeMap<String, String>,
    /// The files the pipeline includes, by path, as read at the commit.
    /// Filled by whoever reads the pipeline: no settings file sets it.
    pub included: BTreeMap<String, Result<String, String>>,
}

/// `.dagron/ci.yaml` read for its `gitlab:` alone, whatever else it holds.
pub fn settings_in(text: &str) -> Result<Settings, Vec<Problem>> {
    let value: Value = serde_yaml::from_str(text).map_err(|e| {
        vec![Problem {
            path: String::new(),
            message: format!("not YAML settings can be read from: {e}"),
        }]
    })?;
    match value.get("gitlab") {
        Some(gitlab) => settings(gitlab),
        None => Ok(Settings::default()),
    }
}

/// Read `gitlab:` of `.dagron/ci.yaml`.
pub fn settings(value: &Value) -> Result<Settings, Vec<Problem>> {
    let mut check = Check::default();
    let mut out = Settings::default();
    let map = match value {
        Value::Null => return check.done(out),
        Value::Object(map) => map,
        _ => {
            check.problem("gitlab", "is a mapping: pipeline, tags");
            return check.done(out);
        }
    };
    check.only("gitlab", map, &["pipeline", "tags"]);
    match map.get("pipeline") {
        None | Some(Value::Null) => {}
        Some(Value::String(path)) => match include_path(path) {
            Ok(path) if path.starts_with(".dagron/") || path.starts_with(".github/") => check
                .problem(
                    "gitlab.pipeline",
                    format!(
                        "{path:?} is where dagron-ci's own pipelines and GitHub's workflow files \
                         are: a GitLab pipeline is a file of its own"
                    ),
                ),
            Ok(path) if !super::is_name(crate::report::pipeline_name(&path)) => check.problem(
                "gitlab.pipeline",
                format!(
                    "{path:?}: its file's name is what its checks are called by, and may have \
                     letters, digits, '.', '_' and '-' alone"
                ),
            ),
            Ok(path) => out.pipeline = Some(path),
            Err(why) => check.problem("gitlab.pipeline", why),
        },
        Some(_) => check.problem(
            "gitlab.pipeline",
            format!("is the pipeline's file, from the project's root: {DEFAULT_FILE}"),
        ),
    }
    match map.get("tags") {
        None | Some(Value::Null) => {}
        Some(Value::Object(tags)) => {
            for (tag, image) in tags {
                match image
                    .as_str()
                    .filter(|image| super::is_image(image) && !image.contains("{{"))
                {
                    Some(image) => {
                        out.tags.insert(tag.clone(), image.to_string());
                    }
                    None => check.problem(
                        &format!("gitlab.tags.{tag}"),
                        "is the image a job with that tag runs in",
                    ),
                }
            }
        }
        Some(_) => check.problem("gitlab.tags", "is a mapping of runner tags to images"),
    }
    check.done(out)
}

/// When a job runs, of how the jobs it waits for went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum When {
    OnSuccess,
    OnFailure,
    Always,
    Manual,
    Never,
}

impl When {
    fn read(text: &str) -> Option<When> {
        Some(match text {
            "on_success" => When::OnSuccess,
            "on_failure" => When::OnFailure,
            "always" => When::Always,
            "manual" => When::Manual,
            "never" => When::Never,
            _ => return None,
        })
    }
}

/// One of a job's (or the workflow's) `rules`.
#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    pub condition: Option<rules::Expr>,
    /// Its `changes`: the files any of which the event must have changed.
    pub changes: Option<Patterns>,
    pub when: Option<When>,
    pub allow_failure: Option<bool>,
    pub variables: Vec<(String, String)>,
}

/// A job another needs, as `needs:` names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Need {
    pub job: String,
    /// Whether its artifacts are fetched (`artifacts: false` says not).
    pub artifacts: bool,
    /// Whether a job not in the pipeline is passed over (`optional: true`).
    pub optional: bool,
}

/// A job's `parallel`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parallel {
    One,
    Count(u64),
    /// `parallel: matrix`, each entry a variable and its values.
    Matrix(Vec<Vec<(String, Vec<String>)>>),
}

/// When a job's artifacts are uploaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Upload {
    OnSuccess,
    OnFailure,
    Always,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Artifacts {
    pub paths: Vec<String>,
    pub exclude: Vec<String>,
    /// Its `expire_in`, in days, at least one.
    pub expire_days: Option<u64>,
    pub when: Upload,
    pub junit: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cache {
    /// What the cache is named by: its `key` (or `key: prefix`).
    pub prefix: String,
    /// Its `key: files`, whose contents name it too.
    pub key_files: Vec<String>,
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    pub image: String,
    /// The name the job reaches it by: its `alias`, else one made of its
    /// image as GitLab makes one.
    pub alias: String,
    pub variables: Vec<(String, String)>,
}

/// A job, with its templates, its references and `default:` resolved.
#[derive(Clone, Debug, PartialEq)]
pub struct Job {
    pub name: String,
    pub stage: String,
    pub before_script: Vec<String>,
    pub script: Vec<String>,
    pub after_script: Vec<String>,
    pub image: Option<String>,
    pub services: Vec<Service>,
    pub variables: Vec<(String, String)>,
    pub rules: Option<Vec<Rule>>,
    pub when: When,
    /// Its `allow_failure`, where it says.
    pub allow_failure: Option<bool>,
    /// Its `needs`: `None` where it has none, and then it waits for the
    /// stages before its own.
    pub needs: Option<Vec<Need>>,
    pub dependencies: Option<Vec<String>>,
    pub parallel: Parallel,
    pub artifacts: Option<Artifacts>,
    pub cache: Vec<Cache>,
    pub environment: Option<String>,
    pub tags: Vec<String>,
    pub timeout_secs: Option<u64>,
    pub retry: Option<u64>,
    /// Its variables that `expand: false` keeps as written.
    pub literal: BTreeSet<String>,
}

/// A `.gitlab-ci.yml`, read.
#[derive(Clone, Debug, PartialEq)]
pub struct Ci {
    pub stages: Vec<String>,
    pub variables: Vec<(String, String)>,
    pub workflow_rules: Option<Vec<Rule>>,
    /// Its variables that `expand: false` keeps as written.
    pub literal: BTreeSet<String>,
    /// In the order the file has them.
    pub jobs: Vec<Job>,
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

    fn done<T>(self, value: T) -> Result<T, Vec<Problem>> {
        if self.problems.is_empty() {
            Ok(value)
        } else {
            Err(self.problems)
        }
    }

    fn only(&mut self, path: &str, map: &Map<String, Value>, allowed: &[&str]) {
        for key in map.keys() {
            if !allowed.contains(&key.as_str()) {
                let at = join(path, key);
                self.problem(
                    &at,
                    format!("is not a key here: {} are ({MATRIX})", allowed.join(", ")),
                );
            }
        }
    }

    /// A string, or a list of strings (`script:`, `tags:`), flattened as
    /// GitLab flattens nested lists in a script.
    fn strings(&mut self, path: &str, value: Option<&Value>) -> Vec<String> {
        let mut out = Vec::new();
        fn walk(check: &mut Check, path: &str, value: &Value, depth: usize, out: &mut Vec<String>) {
            match value {
                Value::Null => {}
                Value::String(s) => out.push(s.clone()),
                Value::Number(n) => out.push(n.to_string()),
                Value::Bool(b) => out.push(b.to_string()),
                Value::Array(items) if depth < MAX_REFERENCE => {
                    for item in items {
                        walk(check, path, item, depth + 1, out);
                    }
                }
                _ => check.problem(path, "is a string, or a list of them"),
            }
        }
        if let Some(value) = value {
            walk(self, path, value, 0, &mut out);
        }
        out
    }

    fn bool(&mut self, path: &str, value: Option<&Value>) -> Option<bool> {
        match value {
            None | Some(Value::Null) => None,
            Some(Value::Bool(b)) => Some(*b),
            Some(_) => {
                self.problem(path, "is true or false");
                None
            }
        }
    }
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

/// The text a scalar is, as GitLab reads a variable's value: a number or a
/// boolean is its text.
fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

/// Whether `name` is a variable's name GitLab takes.
fn is_variable_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.len() <= 255
}

/// `variables:`: a name and a value, or a name and `{value, description}`.
fn variables(check: &mut Check, path: &str, value: Option<&Value>) -> Vec<(String, String)> {
    variables_and(check, path, value, &mut BTreeSet::new())
}

/// [`variables`], with the names of those `expand: false` keeps as they are
/// written added to `literal`.
fn variables_and(
    check: &mut Check,
    path: &str,
    value: Option<&Value>,
    literal: &mut BTreeSet<String>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    match value {
        None | Some(Value::Null) => {}
        Some(Value::Object(map)) => {
            for (name, value) in map {
                let at = join(path, name);
                if !is_variable_name(name) {
                    check.problem(&at, "is not a variable's name: letters, digits and _");
                    continue;
                }
                let text = match value {
                    Value::Object(full) => {
                        check.only(&at, full, &["description", "expand", "options", "value"]);
                        if full.get("expand") == Some(&Value::Bool(false)) {
                            literal.insert(name.clone());
                        }
                        full.get("value").and_then(scalar)
                    }
                    other => scalar(other),
                };
                match text {
                    Some(text) => out.push((name.clone(), text)),
                    None => check.problem(&at, "is a value: a string, a number or true/false"),
                }
            }
        }
        Some(_) => check.problem(path, "is a mapping of names to values"),
    }
    out
}

/// The paths a pipeline's `include:` names in the project (`include:
/// local`, or a bare path), in the order given. Anything else it includes
/// is refused when the pipeline is read ([`parse`]); here it is passed over.
pub fn local_includes(text: &str) -> Result<Vec<String>, String> {
    let yaml: Yaml = serde_yaml::from_str(text).map_err(|e| format!("is not YAML: {e}"))?;
    let Some(include) = yaml.as_mapping().and_then(|m| m.get("include")) else {
        return Ok(Vec::new());
    };
    let items: Vec<&Yaml> = match include {
        Yaml::Sequence(items) => items.iter().collect(),
        other => vec![other],
    };
    let mut out = Vec::new();
    for item in items {
        let path = match item {
            Yaml::String(path) if !path.contains("://") => Some(path.as_str()),
            Yaml::Mapping(map) => map.get("local").and_then(Yaml::as_str),
            _ => None,
        };
        // One that is no path of the project is the reading's to refuse:
        // the others are read all the same.
        if let Some(path) = path.and_then(|p| include_path(p).ok()) {
            out.push(path);
        }
    }
    Ok(out)
}

/// An included file's path, from the root: GitLab writes it with a leading
/// `/` or none. No way out of the project, and no wildcard.
fn include_path(path: &str) -> Result<String, String> {
    let trimmed = path.trim().trim_start_matches('/');
    let fits = !trimmed.is_empty()
        && (trimmed.ends_with(".yml") || trimmed.ends_with(".yaml"))
        && !trimmed.contains(['*', '?', '[', '\\'])
        && trimmed
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..");
    if fits {
        Ok(trimmed.to_string())
    } else {
        Err(format!(
            "{path:?} is not a file of the project this reads: a .yml file's path, from the root, \
             with no wildcard"
        ))
    }
}

/// `include:` as read in `file`: the local paths, in order, and a problem
/// for each other kind.
fn includes_of(check: &mut Check, file: &str, map: &serde_yaml::Mapping) -> Vec<String> {
    let at = |key: &str| {
        if file.is_empty() {
            key.to_string()
        } else {
            format!("{file}: {key}")
        }
    };
    let Some(include) = map.get("include") else {
        return Vec::new();
    };
    let items: Vec<&Yaml> = match include {
        Yaml::Sequence(items) => items.iter().collect(),
        other => vec![other],
    };
    let mut out = Vec::new();
    for item in items {
        match item {
            Yaml::String(path) if path.contains("://") => check.problem(
                &at("include"),
                format!("{path:?} is a remote file, which is level 2 and not taken yet"),
            ),
            Yaml::String(path) => match include_path(path) {
                Ok(path) => out.push(path),
                Err(why) => check.problem(&at("include"), why),
            },
            Yaml::Mapping(entry) => {
                let keys: Vec<&str> = entry.keys().filter_map(Yaml::as_str).collect();
                if let Some(path) = entry.get("local").and_then(Yaml::as_str) {
                    if keys
                        .iter()
                        .any(|k| !matches!(*k, "local" | "rules" | "inputs"))
                    {
                        check.problem(&at("include"), "a local include takes local: alone");
                    } else if keys.contains(&"rules") || keys.contains(&"inputs") {
                        check.problem(
                            &at("include"),
                            "an include's rules and inputs are not taken yet",
                        );
                    } else {
                        match include_path(path) {
                            Ok(path) => out.push(path),
                            Err(why) => check.problem(&at("include"), why),
                        }
                    }
                } else {
                    let kind = keys.first().copied().unwrap_or("it");
                    check.problem(
                        &at("include"),
                        format!(
                            "include: {kind} is from outside the project, which is level 2 and \
                             not taken yet: include: local is"
                        ),
                    );
                }
            }
            _ => check.problem(&at("include"), "is a path, or local: and a path"),
        }
    }
    out
}

/// YAML's merge keys (`<<: *anchor`), applied at every level.
fn merge_keys(value: &mut Yaml) -> Result<(), String> {
    match value {
        Yaml::Mapping(_) => {
            value
                .apply_merge()
                .map_err(|e| format!("a merge key (<<) cannot be applied: {e}"))?;
            if let Yaml::Mapping(map) = value {
                for (_, v) in map.iter_mut() {
                    merge_keys(v)?;
                }
            }
        }
        Yaml::Sequence(items) => {
            for item in items {
                merge_keys(item)?;
            }
        }
        Yaml::Tagged(tagged) => merge_keys(&mut tagged.value)?,
        _ => {}
    }
    Ok(())
}

/// A deep merge as GitLab merges an included file under the one that
/// includes it, and a template under the job that `extends` it: mappings
/// key by key, anything else replaced.
fn deep_merge(base: &mut Yaml, over: &Yaml) {
    match (base, over) {
        (Yaml::Mapping(base), Yaml::Mapping(over)) => {
            for (key, value) in over {
                match base.get_mut(key) {
                    Some(existing) => deep_merge(existing, value),
                    None => {
                        base.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base, over) => *base = over.clone(),
    }
}

/// The most YAML nodes a pipeline's references and templates may make, all
/// together: what a small file of references to references, or of
/// templates named many times over, cannot grow past.
const MAX_NODES: usize = 1_000_000;

/// How many nodes `value` is.
fn nodes(value: &Yaml) -> usize {
    match value {
        Yaml::Mapping(map) => 1 + map.iter().map(|(_, v)| nodes(v)).sum::<usize>(),
        Yaml::Sequence(items) => 1 + items.iter().map(nodes).sum::<usize>(),
        Yaml::Tagged(tagged) => 1 + nodes(&tagged.value),
        _ => 1,
    }
}

/// What `!reference` has resolved, by the names it gave, and how many
/// nodes they may still make.
struct Referenced {
    /// Each, and how many nodes it is.
    done: BTreeMap<Vec<String>, (Yaml, usize)>,
    budget: usize,
    /// The budget is spent: every reference after is nothing, said once.
    spent: bool,
}

/// Every `!reference [job, key, …]` replaced by what it names, in the
/// document as its templates made it; a reference whose target holds one is
/// resolved in turn, ten deep. Each is resolved once.
fn references(
    doc: &Yaml,
    value: &mut Yaml,
    depth: usize,
    seen: &mut Referenced,
    check: &mut Check,
) {
    match value {
        Yaml::Tagged(tagged) if tagged.tag == "!reference" || tagged.tag == "reference" => {
            if depth >= MAX_REFERENCE {
                check.problem(
                    "",
                    format!("!reference nests more than {MAX_REFERENCE} deep, or in a circle"),
                );
                *value = Yaml::Null;
                return;
            }
            let names: Option<Vec<String>> = tagged
                .value
                .as_sequence()
                .map(|items| {
                    items
                        .iter()
                        .map(|i| i.as_str().map(str::to_string))
                        .collect()
                })
                .and_then(|names: Option<Vec<String>>| names.filter(|n| !n.is_empty()));
            let Some(names) = names else {
                check.problem("", "!reference takes a list of names: [job, key, …]");
                *value = Yaml::Null;
                return;
            };
            if seen.spent {
                *value = Yaml::Null;
                return;
            }
            if let Some((done, size)) = seen.done.get(&names) {
                if *size > seen.budget {
                    check.problem(
                        "",
                        format!("its references make more than {MAX_NODES} nodes"),
                    );
                    seen.spent = true;
                    *value = Yaml::Null;
                    return;
                }
                seen.budget -= size;
                *value = done.clone();
                return;
            }
            let mut found = doc;
            for name in &names {
                match found.as_mapping().and_then(|m| m.get(name.as_str())) {
                    Some(next) => found = next,
                    None => {
                        check.problem(
                            "",
                            format!(
                                "!reference [{}] names nothing in the pipeline",
                                names.join(", ")
                            ),
                        );
                        *value = Yaml::Null;
                        return;
                    }
                }
            }
            let mut resolved = found.clone();
            references(doc, &mut resolved, depth + 1, seen, check);
            if seen.spent {
                *value = Yaml::Null;
                return;
            }
            let size = nodes(&resolved);
            if size > seen.budget {
                check.problem(
                    "",
                    format!("its references make more than {MAX_NODES} nodes"),
                );
                seen.spent = true;
                *value = Yaml::Null;
                return;
            }
            seen.budget -= size;
            seen.done.insert(names, (resolved.clone(), size));
            *value = resolved;
        }
        Yaml::Tagged(tagged) => {
            check.problem(
                "",
                format!("{} is not a tag GitLab has: !reference is", tagged.tag),
            );
            *value = Yaml::Null;
        }
        Yaml::Mapping(map) => {
            for (_, v) in map.iter_mut() {
                references(doc, v, depth, seen, check);
            }
        }
        Yaml::Sequence(items) => {
            for item in items {
                references(doc, item, depth, seen, check);
            }
        }
        _ => {}
    }
}

/// A file of the pipeline, read as YAML with its merge keys applied.
fn load(path: &str, text: &str) -> Result<Yaml, String> {
    let mut value: Yaml = serde_yaml::from_str(text).map_err(|e| {
        if path.is_empty() {
            format!("is not YAML: {e}")
        } else {
            format!("{path} is not YAML: {e}")
        }
    })?;
    merge_keys(&mut value)?;
    match value {
        Yaml::Mapping(_) => Ok(value),
        Yaml::Null => Ok(Yaml::Mapping(Default::default())),
        _ => Err(format!(
            "{} is a mapping of jobs and keys",
            if path.is_empty() { "the file" } else { path }
        )),
    }
}

/// The pipeline as GitLab merges it: each file it includes first, in
/// order, each under the files that include it, and the pipeline's own
/// file last, over all.
fn merged(
    text: &str,
    included: &BTreeMap<String, Result<String, String>>,
    check: &mut Check,
) -> Yaml {
    let main = match load("", text) {
        Ok(main) => main,
        Err(why) => {
            check.problem("", why);
            return Yaml::Null;
        }
    };
    let mut seen = BTreeSet::new();
    let mut doc = Yaml::Mapping(Default::default());
    fn include(
        file: &str,
        value: &Yaml,
        included: &BTreeMap<String, Result<String, String>>,
        seen: &mut BTreeSet<String>,
        depth: usize,
        doc: &mut Yaml,
        check: &mut Check,
    ) {
        let Some(map) = value.as_mapping() else {
            return;
        };
        for path in includes_of(check, file, map) {
            if !seen.insert(path.clone()) {
                continue;
            }
            if seen.len() > MAX_FILES || depth >= MAX_INCLUDE_DEPTH {
                check.problem(
                    "include",
                    format!("the pipeline is more than {MAX_FILES} files, or nests them too deep"),
                );
                return;
            }
            let text = match included.get(&path) {
                Some(Ok(text)) => text,
                Some(Err(why)) => {
                    check.problem("include", format!("{path} {why}"));
                    continue;
                }
                None => {
                    check.problem("include", format!("{path} was not read"));
                    continue;
                }
            };
            match load(&path, text) {
                Ok(value) => {
                    include(&path, &value, included, seen, depth + 1, doc, check);
                    let mut own = value.clone();
                    if let Yaml::Mapping(map) = &mut own {
                        map.remove("include");
                    }
                    deep_merge(doc, &own);
                }
                Err(why) => check.problem("include", why),
            }
        }
    }
    include("", &main, included, &mut seen, 0, &mut doc, check);
    let mut own = main.clone();
    if let Yaml::Mapping(map) = &mut own {
        map.remove("include");
    }
    deep_merge(&mut doc, &own);
    doc
}

/// The document with each job and template's `extends` merged under it, its
/// templates' own templates first, each template made once. GitLab does
/// this before it resolves `!reference`, which then reads jobs as their
/// templates made them.
fn extended(doc: &Yaml, check: &mut Check) -> Yaml {
    let Some(map) = doc.as_mapping() else {
        return doc.clone();
    };
    fn resolve(
        doc: &serde_yaml::Mapping,
        name: &str,
        value: &Yaml,
        depth: usize,
        made: &mut BTreeMap<String, Yaml>,
        check: &mut Check,
    ) -> Yaml {
        if let Some(done) = made.get(name) {
            return done.clone();
        }
        let parents: Vec<String> = match value.as_mapping().and_then(|m| m.get("extends")) {
            None => return value.clone(),
            Some(Yaml::String(one)) => vec![one.clone()],
            Some(Yaml::Sequence(many)) => many
                .iter()
                .filter_map(|p| p.as_str().map(str::to_string))
                .collect(),
            Some(_) => {
                check.problem(
                    &format!("{name}.extends"),
                    "is a job's name, or a list of them",
                );
                return value.clone();
            }
        };
        if depth >= MAX_EXTENDS {
            check.problem(
                &format!("{name}.extends"),
                format!("nests more than {MAX_EXTENDS} deep, or in a circle"),
            );
            return value.clone();
        }
        let mut out = Yaml::Mapping(Default::default());
        let mut named = BTreeSet::new();
        for parent in &parents {
            // A template named twice is merged once: the second changes
            // nothing.
            if !named.insert(parent.clone()) {
                continue;
            }
            match doc.get(parent.as_str()) {
                Some(found) if found.is_mapping() => {
                    let base = resolve(doc, parent, found, depth + 1, made, check);
                    deep_merge(&mut out, &base);
                }
                _ => check.problem(
                    &format!("{name}.extends"),
                    format!("{parent:?} is no job or template of the pipeline"),
                ),
            }
        }
        deep_merge(&mut out, value);
        if let Yaml::Mapping(m) = &mut out {
            m.remove("extends");
        }
        made.insert(name.to_string(), out.clone());
        out
    }
    let mut made = BTreeMap::new();
    let mut out = serde_yaml::Mapping::new();
    for (key, value) in map {
        let Some(name) = key.as_str() else {
            check.problem("", "a job's name is a string");
            continue;
        };
        let value = if GLOBAL_KEYS.contains(&name) {
            value.clone()
        } else {
            resolve(map, name, value, 0, &mut made, check)
        };
        out.insert(key.clone(), value);
    }
    Yaml::Mapping(out)
}

/// The jobs of a document: every key that is no template (`.name`) and no
/// global key, in the file's order.
fn jobs_of(doc: &Yaml) -> Vec<(String, Yaml)> {
    doc.as_mapping()
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| k.as_str().map(|k| (k, v)))
                .filter(|(name, _)| !name.starts_with('.') && !GLOBAL_KEYS.contains(name))
                .map(|(name, v)| (name.to_string(), v.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn to_json(path: &str, value: &Yaml, check: &mut Check) -> Value {
    match serde_json::to_value(value) {
        Ok(v) => v,
        Err(e) => {
            check.problem(path, format!("cannot be read: {e}"));
            Value::Null
        }
    }
}

/// Read a pipeline, with the files it includes as read at the commit
/// (`included`, by path; see [`local_includes`]).
pub fn parse(
    text: &str,
    included: &BTreeMap<String, Result<String, String>>,
) -> Result<Ci, Vec<Problem>> {
    let mut check = Check::default();
    let doc = merged(text, included, &mut check);
    if !check.problems.is_empty() {
        return Err(check.problems);
    }
    let mut doc = extended(&doc, &mut check);
    if nodes(&doc) > MAX_NODES {
        check.problem(
            "",
            format!("its templates make more than {MAX_NODES} nodes"),
        );
        return Err(check.problems);
    }
    let snapshot = doc.clone();
    let mut seen = Referenced {
        done: BTreeMap::new(),
        budget: MAX_NODES,
        spent: false,
    };
    references(&snapshot, &mut doc, 0, &mut seen, &mut check);
    let jobs_yaml = jobs_of(&doc);
    let top = to_json("", &doc, &mut check);
    let Some(top) = top.as_object() else {
        check.problem("", "is a mapping of jobs and keys");
        return Err(check.problems);
    };

    let stages: Vec<String> = match top.get("stages") {
        None | Some(Value::Null) => DEFAULT_STAGES.iter().map(|s| s.to_string()).collect(),
        Some(value) => check.strings("stages", Some(value)),
    };
    let mut ordered = vec![PRE.to_string()];
    for stage in stages {
        if stage != PRE && stage != POST && !ordered.contains(&stage) {
            ordered.push(stage);
        }
    }
    ordered.push(POST.to_string());

    let mut literal = BTreeSet::new();
    let variables_global =
        variables_and(&mut check, "variables", top.get("variables"), &mut literal);
    let workflow_rules = match top.get("workflow") {
        None | Some(Value::Null) => None,
        Some(Value::Object(workflow)) => {
            check.only("workflow", workflow, &["name", "rules", "auto_cancel"]);
            if workflow.contains_key("auto_cancel") {
                check.problem("workflow.auto_cancel", "is not taken yet");
            }
            workflow
                .get("rules")
                .map(|r| rules_of(&mut check, "workflow.rules", r, true))
        }
        Some(_) => {
            check.problem("workflow", "is a mapping: rules");
            None
        }
    };

    // `default:`, with the older top-level keys under it.
    let mut defaults = Map::new();
    for key in [
        "after_script",
        "before_script",
        "cache",
        "image",
        "services",
    ] {
        if let Some(value) = top.get(key) {
            defaults.insert(key.to_string(), value.clone());
        }
    }
    match top.get("default") {
        None | Some(Value::Null) => {}
        Some(Value::Object(default)) => {
            check.only("default", default, DEFAULT_KEYS);
            for (k, v) in default {
                defaults.insert(k.clone(), v.clone());
            }
        }
        Some(_) => check.problem("default", "is a mapping of what every job has"),
    }

    let mut jobs = Vec::new();
    for (name, value) in &jobs_yaml {
        let value = to_json(name, value, &mut check);
        let Some(map) = value.as_object() else {
            check.problem(name, "is a job: a mapping with a script");
            continue;
        };
        if let Some(job) = job(&mut check, name, map, &defaults, &ordered) {
            jobs.push(job);
        }
    }
    if jobs.is_empty() && check.problems.is_empty() {
        check.problem("", "has no job: a job is a key with a script");
    }
    check.done(Ci {
        stages: ordered,
        variables: variables_global,
        literal,
        workflow_rules,
        jobs,
    })
}

/// A job's `rules:` (or `workflow:rules:`).
fn rules_of(check: &mut Check, path: &str, value: &Value, workflow: bool) -> Vec<Rule> {
    let Some(items) = value.as_array() else {
        check.problem(path, "is a list of rules");
        return Vec::new();
    };
    let mut out = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let at = format!("{path}[{i}]");
        let Some(map) = item.as_object() else {
            check.problem(&at, "is a rule: if, changes, when, variables");
            continue;
        };
        let allowed: &[&str] = if workflow {
            &["changes", "if", "variables", "when", "auto_cancel"]
        } else {
            &[
                "allow_failure",
                "changes",
                "if",
                "variables",
                "when",
                "needs",
                "start_in",
                "exists",
                "interruptible",
            ]
        };
        check.only(&at, map, allowed);
        for refused in [
            "exists",
            "needs",
            "start_in",
            "auto_cancel",
            "interruptible",
        ] {
            if map.contains_key(refused) {
                check.problem(&join(&at, refused), "is not taken yet");
            }
        }
        let condition = match map.get("if") {
            None | Some(Value::Null) => None,
            Some(Value::String(text)) => {
                match rules::parse(text).and_then(|e| e.check().map(|()| e)) {
                    Ok(expr) => Some(expr),
                    Err(why) => {
                        check.problem(&join(&at, "if"), why);
                        None
                    }
                }
            }
            Some(_) => {
                check.problem(&join(&at, "if"), "is an expression, in a string");
                None
            }
        };
        let changes = match map.get("changes") {
            None | Some(Value::Null) => None,
            Some(Value::Object(full)) => {
                check.only(&join(&at, "changes"), full, &["paths", "compare_to"]);
                if full.contains_key("compare_to") {
                    check.problem(&join(&at, "changes.compare_to"), "is not taken yet");
                }
                let list = check.strings(&join(&at, "changes.paths"), full.get("paths"));
                patterns(check, &join(&at, "changes.paths"), &list)
            }
            Some(value) => {
                let list = check.strings(&join(&at, "changes"), Some(value));
                patterns(check, &join(&at, "changes"), &list)
            }
        };
        let when = match map.get("when").and_then(Value::as_str) {
            None => None,
            Some(text) => match When::read(text) {
                Some(When::Always | When::Never) if workflow => When::read(text),
                Some(_) if workflow => {
                    check.problem(
                        &join(&at, "when"),
                        "is always or never, for a workflow's rule",
                    );
                    None
                }
                Some(when) => Some(when),
                None if text == "delayed" => {
                    check.problem(&join(&at, "when"), "delayed is not taken");
                    None
                }
                None => {
                    check.problem(
                        &join(&at, "when"),
                        "is on_success, on_failure, always, manual or never",
                    );
                    None
                }
            },
        };
        out.push(Rule {
            condition,
            changes,
            when,
            allow_failure: check.bool(&join(&at, "allow_failure"), map.get("allow_failure")),
            variables: variables(check, &join(&at, "variables"), map.get("variables")),
        });
    }
    out
}

/// GitLab's globs (`**/*.rs`, `docs/**/*`), as the paths filter reads them.
fn patterns(check: &mut Check, path: &str, list: &[String]) -> Option<Patterns> {
    if list.is_empty() {
        check.problem(path, "names no file");
        return None;
    }
    match Patterns::include(list) {
        Ok(p) => Some(p),
        Err(why) => {
            check.problem(path, why);
            None
        }
    }
}

/// GitLab's durations: `3 hours 30 minutes`, `1h 30m`, `90 minutes`, `3600`.
fn duration_secs(text: &str) -> Option<u64> {
    let text = text.trim();
    if let Ok(secs) = text.parse::<u64>() {
        return Some(secs);
    }
    let mut total = 0u64;
    let mut number: Option<u64> = None;
    let mut word = String::new();
    let flush = |number: &mut Option<u64>, word: &mut String, total: &mut u64| -> Option<()> {
        if word.is_empty() {
            return Some(());
        }
        let n = number.take()?;
        let unit = match word.trim_end_matches('s').to_ascii_lowercase().as_str() {
            "s" | "sec" | "second" => 1,
            "m" | "min" | "minute" => 60,
            "h" | "hr" | "hour" => 3600,
            "d" | "day" => 86_400,
            "w" | "wk" | "week" => 7 * 86_400,
            "mo" | "month" => 30 * 86_400,
            "y" | "yr" | "year" => 365 * 86_400,
            _ => return None,
        };
        *total = total.checked_add(n.checked_mul(unit)?)?;
        word.clear();
        Some(())
    };
    let mut digits = String::new();
    for c in text.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() {
            if !word.is_empty() {
                flush(&mut number, &mut word, &mut total)?;
            }
            digits.push(c);
        } else {
            if !digits.is_empty() {
                if number.is_some() {
                    return None;
                }
                number = Some(digits.parse().ok()?);
                digits.clear();
            }
            if c.is_ascii_alphabetic() {
                word.push(c);
            } else if c.is_whitespace() || c == ',' {
                flush(&mut number, &mut word, &mut total)?;
            } else {
                return None;
            }
        }
    }
    if number.is_some() || total == 0 {
        return None;
    }
    Some(total)
}

/// The name a service is reached by where it has no `alias`, as GitLab
/// makes one of its image: the image without its tag, `/` written as `-`
/// (GitLab also gives a `__` form; one name is enough for dagron).
fn service_alias(image: &str) -> String {
    let without_digest = image.split('@').next().unwrap_or(image);
    let name = match without_digest.rsplit_once(':') {
        Some((name, tag)) if !tag.contains('/') => name,
        _ => without_digest,
    };
    name.replace('/', "-")
}

fn image_of(check: &mut Check, path: &str, value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(image)) => Some(image.clone()),
        Some(Value::Object(full)) => {
            check.only(
                path,
                full,
                &["name", "entrypoint", "pull_policy", "docker", "kubernetes"],
            );
            for refused in ["entrypoint", "pull_policy", "docker", "kubernetes"] {
                if full.contains_key(refused) {
                    check.problem(&join(path, refused), "is not taken yet");
                }
            }
            match full.get("name").and_then(Value::as_str) {
                Some(name) => Some(name.to_string()),
                None => {
                    check.problem(&join(path, "name"), "is the image's name");
                    None
                }
            }
        }
        Some(_) => {
            check.problem(path, "is an image's name, or name: and the image");
            None
        }
    }
}

fn job(
    check: &mut Check,
    name: &str,
    own: &Map<String, Value>,
    defaults: &Map<String, Value>,
    stages: &[String],
) -> Option<Job> {
    let mut known: Vec<&str> = JOB_KEYS.to_vec();
    known.extend(REFUSED_JOB_KEYS.iter().map(|(k, _)| *k));
    check.only(name, own, &known);
    for (key, why) in REFUSED_JOB_KEYS {
        if own.contains_key(*key) {
            check.problem(&join(name, key), *why);
        }
    }
    if name.len() > 255 {
        check.problem(name, "is a name of at most 255 characters");
    }
    let get = |key: &str| own.get(key).or_else(|| defaults.get(key));
    let at = |key: &str| join(name, key);

    let script = check.strings(&at("script"), own.get("script"));
    if script.is_empty() {
        check.problem(&at("script"), "is what the job runs: every job has one");
    }
    let stage = match own.get("stage") {
        None | Some(Value::Null) => "test".to_string(),
        Some(Value::String(stage)) => stage.clone(),
        Some(_) => {
            check.problem(&at("stage"), "is a stage's name");
            "test".to_string()
        }
    };
    if !stages.contains(&stage) {
        check.problem(
            &at("stage"),
            format!(
                "{stage:?} is not one of the pipeline's stages: {}",
                stages.join(", ")
            ),
        );
    }

    let when = match own.get("when") {
        None | Some(Value::Null) => When::OnSuccess,
        Some(Value::String(text)) => match When::read(text) {
            Some(when) => when,
            None if text == "delayed" => {
                check.problem(&at("when"), "delayed is not taken");
                When::OnSuccess
            }
            None => {
                check.problem(
                    &at("when"),
                    "is on_success, on_failure, always, manual or never",
                );
                When::OnSuccess
            }
        },
        Some(_) => {
            check.problem(
                &at("when"),
                "is on_success, on_failure, always, manual or never",
            );
            When::OnSuccess
        }
    };

    let allow_failure = match own.get("allow_failure") {
        None | Some(Value::Null) => None,
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::Object(full)) => {
            check.problem(
                &at("allow_failure"),
                "exit_codes is not taken yet: true or false",
            );
            let _ = full;
            None
        }
        Some(_) => {
            check.problem(&at("allow_failure"), "is true or false");
            None
        }
    };

    let needs = match own.get("needs") {
        None | Some(Value::Null) => None,
        Some(Value::Array(items)) => {
            let mut out = Vec::new();
            for (i, item) in items.iter().enumerate() {
                let path = format!("{}[{i}]", at("needs"));
                match item {
                    Value::String(job) => out.push(Need {
                        job: job.clone(),
                        artifacts: true,
                        optional: false,
                    }),
                    Value::Object(full) => {
                        check.only(
                            &path,
                            full,
                            &[
                                "job",
                                "artifacts",
                                "optional",
                                "pipeline",
                                "project",
                                "ref",
                                "parallel",
                            ],
                        );
                        for refused in ["pipeline", "project", "ref", "parallel"] {
                            if full.contains_key(refused) {
                                check.problem(&join(&path, refused), "is not taken yet");
                            }
                        }
                        match full.get("job").and_then(Value::as_str) {
                            Some(job) => out.push(Need {
                                job: job.to_string(),
                                artifacts: check
                                    .bool(&join(&path, "artifacts"), full.get("artifacts"))
                                    .unwrap_or(true),
                                optional: check
                                    .bool(&join(&path, "optional"), full.get("optional"))
                                    .unwrap_or(false),
                            }),
                            None => check.problem(&join(&path, "job"), "is the job needed"),
                        }
                    }
                    _ => check.problem(&path, "is a job's name, or job: and its name"),
                }
            }
            Some(out)
        }
        Some(_) => {
            check.problem(&at("needs"), "is a list of jobs");
            None
        }
    };

    let dependencies = match own.get("dependencies") {
        None | Some(Value::Null) => None,
        Some(value) => Some(check.strings(&at("dependencies"), Some(value))),
    };

    let parallel = match own.get("parallel") {
        None | Some(Value::Null) => Parallel::One,
        Some(Value::Number(n)) => match n.as_u64() {
            Some(n) if (1..=MAX_PARALLEL).contains(&n) => Parallel::Count(n),
            _ => {
                check.problem(
                    &at("parallel"),
                    format!("is 1 to {MAX_PARALLEL}, or matrix:"),
                );
                Parallel::One
            }
        },
        Some(Value::Object(full)) => {
            check.only(&at("parallel"), full, &["matrix"]);
            match full.get("matrix").and_then(Value::as_array) {
                Some(entries) => {
                    let mut out = Vec::new();
                    for (i, entry) in entries.iter().enumerate() {
                        let path = format!("{}.matrix[{i}]", at("parallel"));
                        let Some(map) = entry.as_object() else {
                            check.problem(&path, "is a mapping of variables to their values");
                            continue;
                        };
                        let mut row = Vec::new();
                        for (var, values) in map {
                            if !is_variable_name(var) {
                                check.problem(&join(&path, var), "is not a variable's name");
                                continue;
                            }
                            let list = match values {
                                Value::Array(items) => {
                                    if items.iter().any(|i| i.is_array() || i.is_object()) {
                                        check.problem(
                                            &join(&path, var),
                                            "is a list of values: a string, a number or true/false each",
                                        );
                                    }
                                    items.iter().filter_map(scalar).collect()
                                }
                                other => scalar(other).into_iter().collect(),
                            };
                            row.push((var.clone(), list));
                        }
                        out.push(row);
                    }
                    Parallel::Matrix(out)
                }
                None => {
                    check.problem(
                        &at("parallel.matrix"),
                        "is a list of variables and their values",
                    );
                    Parallel::One
                }
            }
        }
        Some(_) => {
            check.problem(&at("parallel"), "is a number, or matrix:");
            Parallel::One
        }
    };

    let artifacts = match get("artifacts") {
        None | Some(Value::Null) => None,
        Some(Value::Object(full)) => {
            let path = at("artifacts");
            check.only(
                &path,
                full,
                &[
                    "paths",
                    "exclude",
                    "expire_in",
                    "when",
                    "name",
                    "reports",
                    "expose_as",
                    "untracked",
                    "public",
                    "access",
                ],
            );
            for refused in ["untracked", "public", "access"] {
                if full.contains_key(refused) {
                    check.problem(&join(&path, refused), "is not taken yet");
                }
            }
            let when = match full.get("when").and_then(Value::as_str) {
                None | Some("on_success") => Upload::OnSuccess,
                Some("on_failure") => Upload::OnFailure,
                Some("always") => Upload::Always,
                Some(_) => {
                    check.problem(&join(&path, "when"), "is on_success, on_failure or always");
                    Upload::OnSuccess
                }
            };
            let expire_days = match full.get("expire_in") {
                None | Some(Value::Null) => None,
                Some(Value::String(text)) if text.trim() == "never" => None,
                Some(value) => match scalar(value).as_deref().and_then(duration_secs) {
                    Some(secs) => Some(secs.div_ceil(86_400).max(1)),
                    None => {
                        check.problem(
                            &join(&path, "expire_in"),
                            "is a duration: 1 week, 30 days, never",
                        );
                        None
                    }
                },
            };
            let mut junit = Vec::new();
            match full.get("reports") {
                None | Some(Value::Null) => {}
                Some(Value::Object(reports)) => {
                    for kind in reports.keys() {
                        if kind != "junit" {
                            check.problem(
                                &format!("{path}.reports.{kind}"),
                                "is not taken: artifacts:reports:junit is",
                            );
                        }
                    }
                    junit = check.strings(&format!("{path}.reports.junit"), reports.get("junit"));
                }
                Some(_) => check.problem(&join(&path, "reports"), "is a mapping: junit"),
            }
            let paths = check.strings(&join(&path, "paths"), full.get("paths"));
            for p in &paths {
                if !is_workspace_path(p) {
                    check.problem(
                        &join(&path, "paths"),
                        format!("{p:?} is not a path in the job's workspace"),
                    );
                }
            }
            Some(Artifacts {
                paths,
                exclude: check.strings(&join(&path, "exclude"), full.get("exclude")),
                expire_days,
                when,
                junit,
            })
        }
        Some(_) => {
            check.problem(
                &at("artifacts"),
                "is a mapping: paths, expire_in, when, reports",
            );
            None
        }
    };

    let cache = match get("cache") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .enumerate()
            .filter_map(|(i, c)| cache_of(check, &format!("{}[{i}]", at("cache")), c, name))
            .collect(),
        Some(one) => cache_of(check, &at("cache"), one, name)
            .into_iter()
            .collect(),
    };

    let services = match get("services") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => {
            let mut out = Vec::new();
            for (i, item) in items.iter().enumerate() {
                let path = format!("{}[{i}]", at("services"));
                match item {
                    Value::String(image) => out.push(Service {
                        alias: service_alias(image),
                        image: image.clone(),
                        variables: Vec::new(),
                    }),
                    Value::Object(full) => {
                        check.only(
                            &path,
                            full,
                            &[
                                "name",
                                "alias",
                                "variables",
                                "command",
                                "entrypoint",
                                "pull_policy",
                                "docker",
                                "kubernetes",
                            ],
                        );
                        for refused in [
                            "command",
                            "entrypoint",
                            "pull_policy",
                            "docker",
                            "kubernetes",
                        ] {
                            if full.contains_key(refused) {
                                check.problem(&join(&path, refused), "is not taken yet");
                            }
                        }
                        let Some(image) = full.get("name").and_then(Value::as_str) else {
                            check.problem(&join(&path, "name"), "is the service's image");
                            continue;
                        };
                        let alias = full
                            .get("alias")
                            .and_then(Value::as_str)
                            .map(|a| a.split(',').next().unwrap_or(a).trim().to_string())
                            .unwrap_or_else(|| service_alias(image));
                        out.push(Service {
                            alias,
                            image: image.to_string(),
                            variables: variables(
                                check,
                                &join(&path, "variables"),
                                full.get("variables"),
                            ),
                        });
                    }
                    _ => check.problem(&path, "is an image, or name: and the image"),
                }
            }
            out
        }
        Some(_) => {
            check.problem(&at("services"), "is a list of services");
            Vec::new()
        }
    };

    let environment = match own.get("environment") {
        None | Some(Value::Null) => None,
        Some(Value::String(env)) => Some(env.clone()),
        Some(Value::Object(full)) => {
            check.only(
                &at("environment"),
                full,
                &[
                    "name",
                    "url",
                    "action",
                    "on_stop",
                    "auto_stop_in",
                    "deployment_tier",
                    "kubernetes",
                ],
            );
            for refused in ["on_stop", "auto_stop_in", "kubernetes"] {
                if full.contains_key(refused) {
                    check.problem(&join(&at("environment"), refused), "is not taken yet");
                }
            }
            match full.get("name").and_then(Value::as_str) {
                Some(env) => Some(env.to_string()),
                None => {
                    check.problem(&at("environment.name"), "is the environment's name");
                    None
                }
            }
        }
        Some(_) => {
            check.problem(
                &at("environment"),
                "is an environment's name, or name: and it",
            );
            None
        }
    };

    let timeout_secs = match get("timeout") {
        None | Some(Value::Null) => None,
        Some(value) => match scalar(value).as_deref().and_then(duration_secs) {
            Some(secs) => Some(secs),
            None => {
                check.problem(&at("timeout"), "is a duration: 1h 30m, 90 minutes");
                None
            }
        },
    };
    let retry = match get("retry") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => n.as_u64().filter(|n| *n <= 2).or_else(|| {
            check.problem(&at("retry"), "is 0, 1 or 2");
            None
        }),
        Some(Value::Object(full)) => {
            check.only(&at("retry"), full, &["max", "when", "exit_codes"]);
            for refused in ["when", "exit_codes"] {
                if full.contains_key(refused) {
                    check.problem(
                        &join(&at("retry"), refused),
                        "is not taken yet: retry: max is",
                    );
                }
            }
            full.get("max")
                .and_then(Value::as_u64)
                .filter(|n| *n <= 2)
                .or_else(|| {
                    check.problem(&at("retry.max"), "is 0, 1 or 2");
                    None
                })
        }
        Some(_) => {
            check.problem(&at("retry"), "is 0, 1 or 2, or max:");
            None
        }
    };

    let rules = own
        .get("rules")
        .map(|r| rules_of(check, &at("rules"), r, false));
    let tags = check.strings(&at("tags"), get("tags"));
    let image = image_of(check, &at("image"), get("image"));
    let mut literal = BTreeSet::new();
    Some(Job {
        name: name.to_string(),
        stage,
        before_script: check.strings(&at("before_script"), get("before_script")),
        script,
        after_script: check.strings(&at("after_script"), get("after_script")),
        image,
        services,
        variables: variables_and(check, &at("variables"), own.get("variables"), &mut literal),
        rules,
        when,
        allow_failure,
        needs,
        dependencies,
        parallel,
        artifacts,
        cache,
        environment,
        tags,
        timeout_secs,
        retry,
        literal,
    })
}

/// Whether `path` is a path or glob of the job's workspace: relative, with
/// no way out of it.
fn is_workspace_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && path.split('/').all(|part| part != "..")
        && !path.chars().any(char::is_control)
}

fn cache_of(check: &mut Check, path: &str, value: &Value, job: &str) -> Option<Cache> {
    let Some(map) = value.as_object() else {
        check.problem(path, "is a mapping: key, paths");
        return None;
    };
    check.only(
        path,
        map,
        &[
            "key",
            "paths",
            "policy",
            "when",
            "untracked",
            "unprotect",
            "fallback_keys",
        ],
    );
    for refused in ["untracked", "unprotect", "fallback_keys", "when"] {
        if map.contains_key(refused) {
            check.problem(&join(path, refused), "is not taken yet");
        }
    }
    if let Some(policy) = map.get("policy").and_then(Value::as_str) {
        if policy != "pull-push" {
            check.problem(
                &join(path, "policy"),
                "is pull-push alone for now: a job both restores its cache and saves it",
            );
        }
    }
    let (prefix, key_files) = match map.get("key") {
        None | Some(Value::Null) => ("default".to_string(), Vec::new()),
        Some(Value::Object(full)) => {
            check.only(&join(path, "key"), full, &["files", "prefix"]);
            let files = check.strings(&join(path, "key.files"), full.get("files"));
            let prefix = full
                .get("prefix")
                .and_then(scalar)
                .unwrap_or_else(|| "files".to_string());
            (prefix, files)
        }
        Some(other) => match scalar(other) {
            Some(key) => (key, Vec::new()),
            None => {
                check.problem(&join(path, "key"), "is a string, or files: and prefix:");
                return None;
            }
        },
    };
    let paths = check.strings(&join(path, "paths"), map.get("paths"));
    if paths.is_empty() {
        check.problem(&join(path, "paths"), "names what the cache holds");
        return None;
    }
    let _ = job;
    Some(Cache {
        prefix,
        key_files,
        paths,
    })
}

// ---- lowering: the pipeline an event makes ----------------------------------

/// Variables a pipeline may not set: dagron's own, which its steps and the
/// step runner read, and the engine's.
const OWN_VARIABLES: &[&str] = &[
    "CI_ARTIFACT_",
    "CI_BASE_",
    "CI_CONTEXT",
    "CI_EVENT",
    "CI_FORGE_HOST",
    "CI_GROUP",
    "CI_JOB_ENVX_",
    "CI_JOB_ALLOW_FAILURE",
    "CI_JOB_IF",
    "CI_MATRIX",
    "CI_NEED_",
    "CI_OUTPUT_",
    "CI_PR",
    "CI_REF",
    "CI_REPO",
    "CI_REPORT_",
    "CI_SECRET_",
    "CI_SHA",
    "CI_STEP_",
    "CI_TRUST",
    "ACTIONS_",
    "DAGRON_",
];

fn is_own_variable(name: &str) -> bool {
    OWN_VARIABLES.iter().any(|own| {
        if own.ends_with('_') {
            name.starts_with(own)
        } else {
            name == *own
        }
    })
}

/// A pipeline as one event makes it.
#[derive(Clone, Debug, PartialEq)]
pub struct Lowered {
    /// The pipeline, for [`super::compile::compile`].
    pub pipeline: Pipeline,
    /// Why the event makes no pipeline of it, where it makes none:
    /// `workflow:rules`, or no job is in it.
    pub passed_over: Option<String>,
    /// Whether a `rules:changes` was reached with the changed files not yet
    /// known: the caller looks them up, and lowers it again.
    pub needs_changes: bool,
}

/// `$NAME` and `${NAME}` in `text` replaced by the variables' values, where
/// they are known; `$$` is a `$`. What is not known is left for the shell.
fn expand(text: &str, vars: &BTreeMap<String, String>) -> String {
    expand_with(text, &mut |name| vars.get(name).cloned())
}

/// The longest a variable may expand to.
const MAX_VALUE: usize = 1024 * 1024;

/// Variables as GitLab expands them: each value's references to the others
/// resolved, whatever order they are written in, and to `base` where none of
/// them has the name. A variable that names itself, or a circle of them, is
/// left for the shell; one of `literal` (`expand: false`) is its text as it
/// is written. Each is expanded once, and none past [`MAX_VALUE`].
fn resolved(
    raw: &[(String, String)],
    base: &BTreeMap<String, String>,
    literal: &BTreeSet<String>,
) -> Vec<(String, String)> {
    let table: BTreeMap<&str, &str> = raw.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    struct Ctx<'a> {
        table: BTreeMap<&'a str, &'a str>,
        base: &'a BTreeMap<String, String>,
        literal: &'a BTreeSet<String>,
        done: BTreeMap<String, String>,
    }
    fn value_of(name: &str, ctx: &mut Ctx<'_>, stack: &mut Vec<String>) -> Option<String> {
        if let Some(done) = ctx.done.get(name) {
            return Some(done.clone());
        }
        if stack.iter().any(|s| s == name) || stack.len() >= MAX_REFERENCE {
            return ctx.base.get(name).cloned();
        }
        let text = ctx.table.get(name).copied();
        match text {
            Some(text) if ctx.literal.contains(name) => Some(text.to_string()),
            Some(text) => {
                stack.push(name.to_string());
                let value = expand_with(text, &mut |n| value_of(n, ctx, stack));
                stack.pop();
                ctx.done.insert(name.to_string(), value.clone());
                Some(value)
            }
            None => ctx.base.get(name).cloned(),
        }
    }
    let mut ctx = Ctx {
        table,
        base,
        literal,
        done: BTreeMap::new(),
    };
    let names: Vec<String> = ctx.table.keys().map(|k| k.to_string()).collect();
    let mut values = BTreeMap::new();
    for name in names {
        let mut stack = Vec::new();
        if let Some(value) = value_of(&name, &mut ctx, &mut stack) {
            values.insert(name, value);
        }
    }
    raw.iter()
        .filter_map(|(name, _)| values.get(name).map(|v| (name.clone(), v.clone())))
        .collect()
}

/// The variables of `job` and of its pipeline that are not expanded.
fn literal_of(ci: &Ci, job: &Job) -> BTreeSet<String> {
    ci.literal.union(&job.literal).cloned().collect()
}

fn expand_with(text: &str, lookup: &mut dyn FnMut(&str) -> Option<String>) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    let name_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
    while i < chars.len() {
        // Past the longest a value may be: what is left is not expanded,
        // and the caller says so.
        if out.len() >= MAX_VALUE {
            break;
        }
        if chars[i] != '$' {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&'$') {
            out.push('$');
            i += 2;
            continue;
        }
        let (name, end) = if chars.get(i + 1) == Some(&'{') {
            match chars[i + 2..].iter().position(|c| *c == '}') {
                Some(n) => (
                    chars[i + 2..i + 2 + n].iter().collect::<String>(),
                    i + 3 + n,
                ),
                None => (String::new(), i + 1),
            }
        } else {
            let n = chars[i + 1..]
                .iter()
                .position(|c| !name_char(*c))
                .unwrap_or(chars.len() - i - 1);
            (
                chars[i + 1..i + 1 + n].iter().collect::<String>(),
                i + 1 + n,
            )
        };
        match (!name.is_empty()).then(|| lookup(&name)).flatten() {
            Some(value) => {
                out.push_str(&value);
                i = end;
            }
            None => {
                out.push('$');
                i += 1;
            }
        }
    }
    out
}

/// `CI_COMMIT_REF_SLUG`: lower case, every character but a letter or a
/// digit a `-`, at most 63, no `-` at either end.
fn slug(text: &str) -> String {
    let lowered: String = text
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(63)
        .collect();
    lowered.trim_matches('-').to_string()
}

/// A job's name as a task's: GitLab's names take spaces, `:` and `/`, and a
/// task's take letters, digits, `.`, `_` and `-`.
fn task_name(job: &str) -> String {
    let mut out = String::new();
    for c in job.chars() {
        let c = if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            c
        } else {
            '-'
        };
        if !(c == '-' && out.ends_with('-')) {
            out.push(c);
        }
    }
    let out = out.trim_matches('-').to_string();
    let out: String = out.chars().take(90).collect();
    if out.is_empty() {
        "job".to_string()
    } else {
        out
    }
}

/// A service's alias as a service's name dagron takes: lower case, letters,
/// digits and `-`, a letter first.
fn service_name(alias: &str) -> String {
    let mut out = String::new();
    for c in alias.to_ascii_lowercase().chars() {
        let c = if c.is_ascii_lowercase() || c.is_ascii_digit() {
            c
        } else {
            '-'
        };
        if !(c == '-' && (out.is_empty() || out.ends_with('-'))) {
            out.push(c);
        }
    }
    let mut out: String = out.trim_matches('-').chars().take(40).collect();
    while out.ends_with('-') {
        out.pop();
    }
    if !out.starts_with(|c: char| c.is_ascii_lowercase()) {
        out.insert(0, 's');
        out.truncate(40);
    }
    out
}

/// A cache's prefix as dagron takes one.
fn cache_prefix(key: &str) -> String {
    let out: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect();
    if out.is_empty() {
        "default".into()
    } else {
        out
    }
}

/// `value` as a shell word in which `$CI_PROJECT_DIR` (or `${…}`) is the
/// shell's and the rest is text: `"$CI_PROJECT_DIR"'/.cache/pip'`.
fn with_project_dir(value: &str) -> String {
    let quote = |text: &str| format!("'{}'", text.replace('\'', "'\\''"));
    let mut out = String::new();
    let mut rest = value;
    loop {
        let at = [rest.find("${CI_PROJECT_DIR}"), rest.find("$CI_PROJECT_DIR")]
            .into_iter()
            .flatten()
            .min();
        let Some(at) = at else { break };
        let token = if rest[at..].starts_with("${CI_PROJECT_DIR}") {
            "${CI_PROJECT_DIR}"
        } else {
            "$CI_PROJECT_DIR"
        };
        // `$CI_PROJECT_DIRS` names another variable.
        let after = &rest[at + token.len()..];
        if token == "$CI_PROJECT_DIR"
            && after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
        {
            out.push_str(&quote(&rest[..at + token.len()]));
            rest = after;
            continue;
        }
        if at > 0 {
            out.push_str(&quote(&rest[..at]));
        }
        out.push_str("\"$CI_PROJECT_DIR\"");
        rest = after;
    }
    if !rest.is_empty() || out.is_empty() {
        out.push_str(&quote(rest));
    }
    out
}

/// A script item, as a shell line that says it and then runs it, as
/// GitLab's runner shows each command it runs.
fn said(item: &str) -> String {
    let quoted = item.replace('\'', "'\\''");
    format!("printf '$ %s\\n' '{quoted}'\n{item}\n")
}

/// What every script begins with: the shell stops at the first command that
/// fails (and a pipe that fails, where the shell knows `pipefail`), and the
/// project's directory is where the job starts.
const PROLOGUE: &str =
    "set -e\n(set -o pipefail) 2>/dev/null && set -o pipefail\nexport CI_PROJECT_DIR=\"$PWD\"\n";

/// One instance of a job: its task's name, and the variables `parallel`
/// gives it.
struct Instance {
    task: String,
    vars: Vec<(String, String)>,
}

fn instances(check: &mut Check, job: &Job, base: &str) -> Vec<Instance> {
    match &job.parallel {
        Parallel::One => vec![Instance {
            task: base.to_string(),
            vars: Vec::new(),
        }],
        Parallel::Count(n) => (1..=*n)
            .map(|i| Instance {
                task: format!("{base}.{i}"),
                vars: vec![
                    ("CI_NODE_INDEX".into(), i.to_string()),
                    ("CI_NODE_TOTAL".into(), n.to_string()),
                ],
            })
            .collect(),
        Parallel::Matrix(entries) => {
            let mut rows: Vec<Vec<(String, String)>> = Vec::new();
            for entry in entries {
                let mut product: Vec<Vec<(String, String)>> = vec![Vec::new()];
                for (var, values) in entry {
                    let mut next = Vec::new();
                    for row in &product {
                        for value in values {
                            let mut row = row.clone();
                            row.push((var.clone(), value.clone()));
                            next.push(row);
                        }
                    }
                    product = next;
                    if product.len() > MAX_PARALLEL as usize {
                        break;
                    }
                }
                rows.extend(product);
            }
            if rows.len() > MAX_PARALLEL as usize {
                check.problem(
                    &join(&job.name, "parallel.matrix"),
                    format!("makes {} jobs; at most {MAX_PARALLEL}", rows.len()),
                );
                rows.truncate(MAX_PARALLEL as usize);
            }
            let total = rows.len();
            let mut seen = BTreeSet::new();
            rows.into_iter()
                .enumerate()
                .map(|(i, row)| {
                    let label = task_name(
                        &row.iter()
                            .map(|(_, v)| v.as_str())
                            .collect::<Vec<_>>()
                            .join("-"),
                    );
                    let mut task = format!("{base}.{label}");
                    if !seen.insert(task.clone()) {
                        task = format!("{base}.{}", i + 1);
                    }
                    let mut vars = row;
                    vars.push(("CI_NODE_INDEX".into(), (i + 1).to_string()));
                    vars.push(("CI_NODE_TOTAL".into(), total.to_string()));
                    Instance { task, vars }
                })
                .collect()
        }
    }
}

/// What a `rules:` list says for the job, with these variables: the first
/// rule that holds, or none (the job is not in the pipeline).
struct Decided {
    when: When,
    /// Whether the rule itself said `when`, and the job's own was not taken.
    rule_when: bool,
    allow_failure: Option<bool>,
    variables: Vec<(String, String)>,
}

impl Ci {
    /// The pipeline `event` makes of this file: `stem` names it.
    pub fn lower(
        &self,
        stem: &str,
        event: &Event,
        settings: &Settings,
    ) -> Result<Lowered, Vec<Problem>> {
        let mut check = Check::default();
        let mut needs_changes = false;
        let passed = |pipeline: Pipeline, why: String, needs_changes: bool| Lowered {
            pipeline,
            passed_over: Some(why),
            needs_changes,
        };
        let empty = Pipeline {
            name: stem.to_string(),
            on: Triggers::default(),
            concurrency: None,
            tasks: Vec::new(),
            spec: Map::new(),
        };

        // GitLab's source, and what it has a pipeline for.
        let source = match event.kind.as_str() {
            "push" | "tag" => "push",
            "pull_request" => "merge_request_event",
            "dispatch" => "api",
            other => {
                return check.done(passed(
                    empty,
                    format!("a GitLab pipeline is made for no {other} event"),
                    false,
                ))
            }
        };
        let predefined = predefined(event, source);
        let base_vars: BTreeMap<String, String> = predefined.iter().cloned().collect();
        // Variables are written here as they are written in the file, and
        // expanded only where they are read, with all of a job's at once:
        // GitLab expands a global variable with the job's own values.
        let overflowed = std::cell::Cell::new(false);
        let with =
            |raw: &[(String, String)], literal: &BTreeSet<String>| -> BTreeMap<String, String> {
                let mut out = base_vars.clone();
                let values = resolved(raw, &base_vars, literal);
                if values.iter().any(|(_, v)| v.len() >= MAX_VALUE) {
                    overflowed.set(true);
                }
                out.extend(values);
                out
            };
        // A dispatch's inputs are the pipeline's variables, as GitLab's API
        // takes them.
        let mut pipeline_vars: Vec<(String, String)> = Vec::new();
        for (name, value) in &event.inputs {
            // GitLab's own say what the event was, and are not a caller's
            // to say otherwise.
            let gitlabs = name.starts_with("CI_") || name.starts_with("GITLAB_") || name == "CI";
            if !is_variable_name(name) || is_own_variable(name) || gitlabs {
                check.problem(
                    &format!("inputs.{name}"),
                    "is not a variable's name a pipeline may be given: GitLab's own and \
                     dagron's are the event's",
                );
                continue;
            }
            pipeline_vars.push((name.clone(), value.clone()));
        }
        let mut global: Vec<(String, String)> = Vec::new();
        for (name, value) in &self.variables {
            if is_own_variable(name) {
                check.problem(
                    &join("variables", name),
                    "is a variable dagron's steps read, which a pipeline does not set",
                );
                continue;
            }
            global.push((name.clone(), value.clone()));
        }
        let vars = with(
            &[global.clone(), pipeline_vars.clone()].concat(),
            &self.literal,
        );
        if overflowed.get() {
            check.problem(
                "variables",
                format!("a variable expands to more than {MAX_VALUE} bytes"),
            );
            return Err(check.problems);
        }

        // Whether `changes` holds for this event.
        let changed = |patterns: &Patterns, needs: &mut bool| -> bool {
            let compared = match event.kind.as_str() {
                "push" => event.base_sha.is_some(),
                "pull_request" => true,
                // A tag, a new branch, a dispatch: GitLab's changes hold.
                _ => false,
            };
            if !compared {
                return true;
            }
            match &event.changed {
                Changes::Listed(files) => {
                    let files: Vec<&str> = files.iter().map(String::as_str).collect();
                    patterns.any(&files)
                }
                Changes::Unlisted(_) => true,
                Changes::Unknown => {
                    *needs = true;
                    true
                }
            }
        };
        let decide = |rules: &[Rule],
                      default_when: When,
                      vars: &BTreeMap<String, String>,
                      check: &mut Check,
                      path: &str,
                      needs: &mut bool|
         -> Option<Decided> {
            for rule in rules {
                if let Some(condition) = &rule.condition {
                    match condition.holds(vars) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(why) => {
                            check.problem(path, why);
                            return None;
                        }
                    }
                }
                if let Some(patterns) = &rule.changes {
                    if !changed(patterns, needs) {
                        continue;
                    }
                }
                // A rule that says no `when` takes the job's, as GitLab has
                // it.
                return Some(Decided {
                    when: rule.when.unwrap_or(default_when),
                    rule_when: rule.when.is_some(),
                    allow_failure: rule.allow_failure,
                    variables: rule.variables.clone(),
                });
            }
            None
        };

        if let Some(rules) = &self.workflow_rules {
            match decide(
                rules,
                When::Always,
                &vars,
                &mut check,
                "workflow.rules",
                &mut needs_changes,
            ) {
                Some(d) if d.when == When::Never => {
                    return check.done(passed(
                        empty,
                        "workflow:rules says never for this event".into(),
                        needs_changes,
                    ))
                }
                Some(d) => {
                    for (name, value) in d.variables {
                        if is_own_variable(&name) {
                            check.problem(
                                "workflow.rules",
                                format!(
                                    "{name} is a variable dagron's steps read, which a pipeline \
                                     does not set"
                                ),
                            );
                            continue;
                        }
                        global.push((name, value));
                    }
                }
                None => {
                    return check.done(passed(
                        empty,
                        "no rule of workflow:rules holds for this event".into(),
                        needs_changes,
                    ))
                }
            }
        }

        // Each job that is in the pipeline, with what its rules said.
        struct In<'a> {
            job: &'a Job,
            when: When,
            allow_failure: bool,
            vars: Vec<(String, String)>,
            base: String,
            instances: Vec<Instance>,
            stage: usize,
        }
        let mut taken: Vec<In> = Vec::new();
        let mut bases = BTreeSet::new();
        for job in &self.jobs {
            let mut own = Vec::new();
            for (name, value) in &job.variables {
                if is_own_variable(name) {
                    check.problem(
                        &format!("{}.variables.{name}", job.name),
                        "is a variable dagron's steps read, which a pipeline does not set",
                    );
                    continue;
                }
                own.push((name.clone(), value.clone()));
            }
            let job_name = vec![
                ("CI_JOB_NAME".to_string(), job.name.clone()),
                ("CI_JOB_STAGE".to_string(), job.stage.clone()),
            ];
            let literal: BTreeSet<String> = self.literal.union(&job.literal).cloned().collect();
            let job_vars = with(
                &[global.clone(), own.clone(), pipeline_vars.clone(), job_name].concat(),
                &literal,
            );
            let decided = match &job.rules {
                Some(rules) => decide(
                    rules,
                    job.when,
                    &job_vars,
                    &mut check,
                    &join(&job.name, "rules"),
                    &mut needs_changes,
                ),
                // No rules: a branch's and a tag's pipeline, not a merge
                // request's.
                None if source == "merge_request_event" => None,
                None => Some(Decided {
                    when: job.when,
                    rule_when: false,
                    allow_failure: None,
                    variables: Vec::new(),
                }),
            };
            let Some(decided) = decided else { continue };
            if decided.when == When::Never {
                continue;
            }
            for (name, value) in decided.variables {
                if is_own_variable(&name) {
                    check.problem(
                        &format!("{}.rules", job.name),
                        format!("{name} is a variable dagron's steps read, which a pipeline does not set"),
                    );
                    continue;
                }
                own.push((name, value));
            }
            // A manual job may fail, unless it says otherwise; one a rule
            // makes manual may not, as GitLab has them.
            let allow_failure = decided
                .allow_failure
                .or(job.allow_failure)
                .unwrap_or(decided.when == When::Manual && !decided.rule_when);
            let mut base = task_name(&job.name);
            if !bases.insert(base.clone()) {
                let mut n = 2;
                while !bases.insert(format!("{base}-{n}")) {
                    n += 1;
                }
                base = format!("{base}-{n}");
            }
            let instances = instances(&mut check, job, &base);
            taken.push(In {
                job,
                when: decided.when,
                allow_failure,
                vars: own,
                base,
                instances,
                stage: self
                    .stages
                    .iter()
                    .position(|s| *s == job.stage)
                    .unwrap_or(0),
            });
        }
        if taken.is_empty() {
            return check.done(passed(
                empty,
                "no job of the pipeline is in it for this event".into(),
                needs_changes,
            ));
        }
        let count: usize = taken.iter().map(|t| t.instances.len()).sum();
        if count > MAX_JOBS {
            check.problem("", format!("makes {count} jobs; at most {MAX_JOBS}"));
            return Err(check.problems);
        }

        let by_name: BTreeMap<&str, usize> = taken
            .iter()
            .enumerate()
            .map(|(i, t)| (t.job.name.as_str(), i))
            .collect();
        // A manual job that may fail is no job the stages after it wait for.
        let blocking = |t: &In| !(t.when == When::Manual && t.allow_failure);

        let mut tasks = Vec::new();
        // Jobs that are in the pipeline and have no task: what waits for
        // them waits for nothing of theirs.
        let mut dropped: BTreeSet<String> = BTreeSet::new();
        for t in &taken {
            let job = t.job;
            // What it waits for, and whose artifacts it fetches.
            let (waits, fetches): (Vec<usize>, Vec<usize>) = match &job.needs {
                Some(needs) => {
                    let mut waits = Vec::new();
                    let mut fetches = Vec::new();
                    for need in needs {
                        match by_name.get(need.job.as_str()) {
                            Some(i) => {
                                waits.push(*i);
                                if need.artifacts {
                                    fetches.push(*i);
                                }
                            }
                            None if need.optional => {}
                            None if self.jobs.iter().any(|j| j.name == need.job) => check.problem(
                                &join(&job.name, "needs"),
                                format!(
                                    "{:?} is not in this event's pipeline: its rules leave it out \
                                     (needs: {{job, optional: true}} says that is so)",
                                    need.job
                                ),
                            ),
                            None => check.problem(
                                &join(&job.name, "needs"),
                                format!("{:?} is no job of the pipeline", need.job),
                            ),
                        }
                    }
                    (waits, fetches)
                }
                None => {
                    let earlier: Vec<usize> = taken
                        .iter()
                        .enumerate()
                        .filter(|(_, o)| o.stage < t.stage && blocking(o))
                        .map(|(i, _)| i)
                        .collect();
                    let fetches = match &job.dependencies {
                        Some(names) => {
                            let mut out = Vec::new();
                            for name in names {
                                match by_name.get(name.as_str()) {
                                    Some(i) if taken[*i].stage < t.stage => out.push(*i),
                                    Some(_) => check.problem(
                                        &join(&job.name, "dependencies"),
                                        format!("{name:?} is not of a stage before the job's"),
                                    ),
                                    // In the file, and its rules leave it out
                                    // of this event's pipeline: nothing of it.
                                    None if self.jobs.iter().any(|j| j.name == *name) => {}
                                    None => check.problem(
                                        &join(&job.name, "dependencies"),
                                        format!("{name:?} is no job of the pipeline"),
                                    ),
                                }
                            }
                            out
                        }
                        None => taken
                            .iter()
                            .enumerate()
                            .filter(|(_, o)| o.stage < t.stage)
                            .map(|(i, _)| i)
                            .collect(),
                    };
                    (earlier, fetches)
                }
            };
            let mut depends_on: Vec<String> = waits
                .iter()
                .flat_map(|i| taken[*i].instances.iter().map(|x| x.task.clone()))
                .collect();
            let download: Vec<String> = fetches
                .iter()
                .filter(|i| {
                    taken[**i]
                        .job
                        .artifacts
                        .as_ref()
                        .is_some_and(|a| !a.paths.is_empty())
                })
                .flat_map(|i| taken[*i].instances.iter().map(|x| x.task.clone()))
                .collect();
            if t.when == When::OnFailure && depends_on.is_empty() {
                // Nothing before it can fail: GitLab never runs it.
                dropped.extend(t.instances.iter().map(|x| x.task.clone()));
                continue;
            }
            // A job of the stages after an `on_failure` one, which nothing
            // that went wrong runs, runs where that one was skipped: GitLab
            // passes over a job that did not run, and stops at one that
            // failed.
            let after_skipped = job.needs.is_none()
                && t.when == When::OnSuccess
                && waits.iter().any(|i| taken[*i].when == When::OnFailure);
            if t.when == When::Manual {
                let gate = format!("{}.approve", t.base);
                let mut fields = Map::new();
                fields.insert("type".into(), json!("approval"));
                // One that may fail is no job anything waits for: turned
                // down, it fails nothing.
                if t.allow_failure {
                    fields.insert("allow_failure".into(), json!(true));
                }
                fields.insert(
                    "approval_message".into(),
                    json!(super::github::inert(&format!(
                        "Run the manual job {}",
                        job.name
                    ))),
                );
                tasks.push(Task {
                    name: gate.clone(),
                    steps: Vec::new(),
                    depends_on: std::mem::take(&mut depends_on),
                    fields,
                    reports: Reports::default(),
                    job: None,
                    hooks: TaskHooks::default(),
                });
                depends_on.push(gate);
            }
            for instance in &t.instances {
                // The file's variables, the job's over them and the rule's
                // and its instance's over those; a pipeline's own over all,
                // as GitLab has them.
                let raw = [
                    global.clone(),
                    t.vars.clone(),
                    instance.vars.clone(),
                    pipeline_vars.clone(),
                    vec![
                        ("CI_JOB_NAME".to_string(), job.name.clone()),
                        ("CI_JOB_STAGE".to_string(), job.stage.clone()),
                    ],
                ]
                .concat();
                let known = with(&raw, &literal_of(self, job));
                let task = self.task(&mut check, t.job, instance, &known, settings, event);
                let Some(mut task) = task else { continue };
                task.depends_on = depends_on.clone();
                match t.when {
                    When::OnFailure => {
                        task.fields
                            .insert("trigger_rule".into(), json!("one_failed"));
                    }
                    When::Always => {
                        task.fields.insert("trigger_rule".into(), json!("all_done"));
                    }
                    _ if after_skipped => {
                        task.fields
                            .insert("trigger_rule".into(), json!("none_failed"));
                    }
                    _ => {}
                }
                // A failure of its own ends it as succeeded, said to have
                // failed (the step runner's): what comes after runs, and an
                // `on_failure` job does not, as on GitLab.
                task.hooks.allow_failure = t.allow_failure;
                task.hooks.download = download.clone();
                tasks.push(task);
            }
        }
        if !dropped.is_empty() {
            for task in &mut tasks {
                task.depends_on.retain(|d| !dropped.contains(d));
                task.hooks.download.retain(|d| !dropped.contains(d));
            }
        }
        if overflowed.get() {
            check.problem(
                "variables",
                format!("a variable expands to more than {MAX_VALUE} bytes"),
            );
        }
        if !check.problems.is_empty() {
            return Err(check.problems);
        }
        let mut spec = Map::new();
        spec.insert("parameters".into(), json!({ super::github::BRACES: "{{" }));
        let on = Triggers {
            push: Some(Default::default()),
            pull_request: Some(Default::default()),
            dispatch: Some(super::Dispatch {
                inputs: event
                    .inputs
                    .keys()
                    .map(|k| (k.clone(), super::Input::default()))
                    .collect(),
            }),
            ..Default::default()
        };
        check.done(Lowered {
            pipeline: Pipeline {
                name: stem.to_string(),
                on,
                concurrency: None,
                tasks,
                spec,
            },
            passed_over: None,
            needs_changes,
        })
    }

    /// One instance of a job, as a task, but for what it waits for.
    fn task(
        &self,
        check: &mut Check,
        job: &Job,
        instance: &Instance,
        vars: &BTreeMap<String, String>,
        settings: &Settings,
        event: &Event,
    ) -> Option<Task> {
        let inert = super::github::inert;
        let at = |key: &str| join(&job.name, key);
        let mut fields = Map::new();

        let image = match &job.image {
            Some(image) => Some(expand(image, vars)),
            None => job
                .tags
                .iter()
                .find_map(|tag| settings.tags.get(tag))
                .cloned(),
        };
        match image {
            Some(image) if super::is_image(&image) && !image.contains("{{") => {
                fields.insert("docker_image".into(), json!(image));
            }
            Some(image) => {
                check.problem(&at("image"), format!("{image:?} is not an image's name"));
                return None;
            }
            None => {
                let tags = if job.tags.is_empty() {
                    String::new()
                } else {
                    format!(", or map one of its tags ({}) to one", job.tags.join(", "))
                };
                check.problem(
                    &at("image"),
                    format!("names no image: set image:{tags} in .dagron/ci.yaml (gitlab: tags:)"),
                );
                return None;
            }
        }

        // The job's variables, a variable of every step.
        let env: Vec<Value> = vars
            .iter()
            .map(|(k, v)| json!({ "name": k, "value": inert(v) }))
            .collect();
        fields.insert("env".into(), Value::Array(env));

        if !job.services.is_empty() {
            if job.services.len() > super::MAX_SERVICES {
                check.problem(
                    &at("services"),
                    format!(
                        "names {} services; at most {}",
                        job.services.len(),
                        super::MAX_SERVICES
                    ),
                );
                return None;
            }
            let mut names = BTreeSet::new();
            let mut services = Vec::new();
            for service in &job.services {
                let name = service_name(&service.alias);
                if !super::is_service_name(&name) || !names.insert(name.clone()) {
                    check.problem(
                        &at("services"),
                        format!(
                            "{:?} is a second service of that name, or no name",
                            service.alias
                        ),
                    );
                    continue;
                }
                let image = expand(&service.image, vars);
                if !super::is_image(&image) || image.contains("{{") {
                    check.problem(&at("services"), format!("{image:?} is not an image's name"));
                    continue;
                }
                // A service has the job's variables, and its own over them,
                // as GitLab gives them.
                // All of the job's: the file's, its own, its rule's, its
                // instance's, and GitLab's.
                let mut service_vars: BTreeMap<String, String> = vars.clone();
                for (k, v) in &service.variables {
                    if !is_own_variable(k) {
                        service_vars.insert(k.clone(), expand(v, vars));
                    }
                }
                let mut entry = Map::new();
                entry.insert("name".into(), json!(name));
                entry.insert("image".into(), json!(image));
                if !service_vars.is_empty() {
                    entry.insert(
                        "env".into(),
                        service_vars
                            .iter()
                            .map(|(k, v)| json!({ "name": k, "value": inert(v) }))
                            .collect(),
                    );
                }
                services.push(Value::Object(entry));
            }
            fields.insert("services".into(), Value::Array(services));
        }

        if !job.cache.is_empty() {
            if job.cache.len() > 8 {
                check.problem(&at("cache"), "names more than 8 caches");
                return None;
            }
            let mut seen = BTreeSet::new();
            let mut caches = Vec::new();
            for cache in &job.cache {
                let prefix = cache_prefix(&expand(&cache.prefix, vars));
                if !seen.insert(prefix.clone()) {
                    check.problem(&at("cache"), format!("two caches have the key {prefix:?}"));
                    continue;
                }
                if cache.paths.len() > 16 || cache.key_files.len() > 32 {
                    check.problem(
                        &at("cache"),
                        "a cache has at most 16 paths and 32 key files",
                    );
                    continue;
                }
                let mut entry = Map::new();
                entry.insert("prefix".into(), json!(prefix));
                entry.insert(
                    "paths".into(),
                    json!(cache
                        .paths
                        .iter()
                        .map(|p| inert(&expand(p, vars)))
                        .collect::<Vec<_>>()),
                );
                if !cache.key_files.is_empty() {
                    entry.insert("key_files".into(), json!(cache.key_files));
                }
                caches.push(Value::Object(entry));
            }
            fields.insert("workspace".into(), json!({ "cache": caches }));
        }

        if let Some(environment) = &job.environment {
            let name = expand(environment, vars);
            if name.is_empty() || name.contains("{{") || name.chars().any(char::is_control) {
                check.problem(
                    &at("environment"),
                    format!("{name:?} is not an environment's name"),
                );
            } else {
                fields.insert("environment".into(), json!(name));
            }
        }
        if let Some(secs) = job.timeout_secs {
            fields.insert("timeout_secs".into(), json!(secs));
        }
        if let Some(retry) = job.retry.filter(|r| *r > 0) {
            fields.insert("max_attempts".into(), json!(retry + 1));
        }

        // Its scripts: `before_script` and `script` in one shell, then
        // `after_script` in another, whatever became of them.
        // A variable that names the project's directory is the directory
        // the job starts in, which the shell knows, as GitLab's runner has
        // it: `PIP_CACHE_DIR: $CI_PROJECT_DIR/.cache/pip`.
        let mut in_project = String::new();
        for (name, value) in vars {
            if name != "CI_PROJECT_DIR"
                && (value.contains("$CI_PROJECT_DIR") || value.contains("${CI_PROJECT_DIR}"))
            {
                in_project.push_str(&format!("export {name}={}\n", with_project_dir(value)));
            }
        }
        let mut run = String::from(PROLOGUE);
        run.push_str(&in_project);
        for item in job.before_script.iter().chain(job.script.iter()) {
            run.push_str(&said(item));
        }
        let mut steps = vec![Step {
            name: Some("script".into()),
            run: inert(&run),
            shell: Some(super::github::DEFAULT_SHELL.into()),
            ..Step::default()
        }];
        if !job.after_script.is_empty() {
            let mut after = String::from(
                "case \"$CI_JOB_STATUS\" in failure) CI_JOB_STATUS=failed ;; cancelled) CI_JOB_STATUS=canceled ;; esac\nexport CI_JOB_STATUS\nexport CI_PROJECT_DIR=\"$PWD\"\n",
            );
            after.push_str(&in_project);
            for item in &job.after_script {
                after.push_str(&said(item));
            }
            let mut late = BTreeMap::new();
            late.insert("CI_JOB_STATUS".to_string(), "${{ job.status }}".to_string());
            steps.push(Step {
                name: Some("after_script".into()),
                run: inert(&after),
                shell: Some(super::github::DEFAULT_SHELL.into()),
                condition: Some("always()".into()),
                continue_on_error: true,
                late_env: late,
                ..Step::default()
            });
        }
        let bytes: usize = steps.iter().map(|s| s.run.len()).sum();
        if bytes > super::MAX_STEPS_BYTES {
            check.problem(
                &at("script"),
                format!(
                    "is over the {} bytes a job's scripts may be",
                    super::MAX_STEPS_BYTES
                ),
            );
            return None;
        }

        let mut reports = Reports::default();
        let mut hooks = TaskHooks::default();
        if let Some(a) = &job.artifacts {
            for path in &a.junit {
                let path = expand(path, vars);
                if super::is_report_path(&path) {
                    reports.junit.push(path);
                } else {
                    check.problem(
                        &at("artifacts.reports.junit"),
                        format!("{path:?} is not taken: a file's path in the workspace, with no wildcard"),
                    );
                }
            }
            if !a.paths.is_empty() {
                hooks.upload = Some(super::ArtifactUpload {
                    name: instance.task.clone(),
                    paths: a.paths.iter().map(|p| inert(&expand(p, vars))).collect(),
                    exclude: a.exclude.iter().map(|p| inert(&expand(p, vars))).collect(),
                    when: match a.when {
                        Upload::OnSuccess => "on_success",
                        Upload::OnFailure => "on_failure",
                        Upload::Always => "always",
                    }
                    .into(),
                    expire_days: a.expire_days,
                });
            }
        }
        let _ = event;
        Some(Task {
            name: instance.task.clone(),
            steps,
            depends_on: Vec::new(),
            fields,
            reports,
            job: None,
            hooks,
        })
    }
}

/// The variables GitLab gives a pipeline, of the event.
fn predefined(event: &Event, source: &str) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = Vec::new();
    let mut set = |k: &str, value: &str| v.push((k.to_string(), value.to_string()));
    set("CI", "true");
    set("GITLAB_CI", "true");
    set("CI_PIPELINE_SOURCE", source);
    set("CI_SERVER_HOST", &event.forge_host);
    set("CI_SERVER_URL", &format!("https://{}", event.forge_host));
    set("CI_PROJECT_PATH", &event.repo);
    set("CI_PROJECT_PATH_SLUG", &slug(&event.repo));
    let (namespace, name) = event.repo.rsplit_once('/').unwrap_or(("", &event.repo));
    set("CI_PROJECT_NAME", name);
    set("CI_PROJECT_NAMESPACE", namespace);
    set(
        "CI_PROJECT_URL",
        &format!("https://{}/{}", event.forge_host, event.repo),
    );
    set("CI_COMMIT_SHA", &event.sha);
    set("CI_COMMIT_SHORT_SHA", &event.sha[..event.sha.len().min(8)]);
    if let Some(branch) = &event.default_branch {
        set("CI_DEFAULT_BRANCH", branch);
    }
    if let Some(actor) = &event.actor {
        set("GITLAB_USER_LOGIN", actor);
    }
    if let Some((id, number)) = event.run_numbers {
        set("CI_PIPELINE_IID", &number.to_string());
        set("CI_PIPELINE_ID", &id.to_string());
    }
    let short = |r: &str| {
        r.strip_prefix("refs/heads/")
            .or_else(|| r.strip_prefix("refs/tags/"))
            .unwrap_or(r)
            .to_string()
    };
    match event.kind.as_str() {
        "push" | "dispatch" => {
            let branch = short(&event.git_ref);
            set("CI_COMMIT_BRANCH", &branch);
            set("CI_COMMIT_REF_NAME", &branch);
            set("CI_COMMIT_REF_SLUG", &slug(&branch));
            set(
                "CI_COMMIT_BEFORE_SHA",
                event
                    .base_sha
                    .as_deref()
                    .unwrap_or("0000000000000000000000000000000000000000"),
            );
        }
        "tag" => {
            let tag = short(&event.git_ref);
            set("CI_COMMIT_TAG", &tag);
            set("CI_COMMIT_REF_NAME", &tag);
            set("CI_COMMIT_REF_SLUG", &slug(&tag));
        }
        "pull_request" => {
            let source_branch = event.head_ref.clone().unwrap_or_default();
            set("CI_COMMIT_REF_NAME", &source_branch);
            set("CI_COMMIT_REF_SLUG", &slug(&source_branch));
            if let Some(pr) = event.pr {
                set("CI_MERGE_REQUEST_IID", &pr.to_string());
            }
            set("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME", &source_branch);
            if let Some(target) = &event.base_ref {
                set("CI_MERGE_REQUEST_TARGET_BRANCH_NAME", &short(target));
            }
            if let Some(base) = &event.base_sha {
                set("CI_MERGE_REQUEST_DIFF_BASE_SHA", base);
            }
            set("CI_MERGE_REQUEST_EVENT_TYPE", "detached");
        }
        _ => {}
    }
    v
}

#[cfg(test)]
mod tests;
