//! A GitHub Actions workflow, as a pipeline of `.dagron/ci`.
//!
//! [`github_to_pipeline`] reads one file of `.github/workflows` and writes
//! the pipeline it would be: its triggers, its jobs as tasks, their steps,
//! a matrix as a task's items, services, timeouts. `docs/CI_MIGRATING.md`
//! has the same map as a table, for a person doing it by hand.
//!
//! **Nothing is dropped without a word.** Whatever of the workflow has no
//! place of its own in the pipeline is in the report ([`Note`]), as one of
//! three things:
//!
//! - *changed*: it is there, and reads differently (a job's image where it
//!   had a runner's label, a variable where a script had an expression);
//! - *left*: it is not there, and the pipeline does without (a cache, a
//!   `permissions:` block, a schedule);
//! - *to do*: it is not there, and the job is not right without it (an
//!   action, a job's outputs, a condition that could not be read).
//!
//! **A job with something to do does not pass.** It starts with a step
//! that fails and says what is left, and an action stands as a step that
//! fails in its place. What is left to do of the workflow as a whole, and
//! is no one job's, is in the step every job starts with. So the pipeline
//! that comes out is one dagron-ci reads and runs, and one that cannot be
//! taken for finished before it is.
//!
//! **No brace of the workflow's is a template here.** A pipeline reads
//! `{{ name }}` as its own, and a workflow's text may hold the same two
//! braces for a reason of its own (a Go template, a Jinja one). The
//! importer writes its own templates with two characters no workflow's
//! text is left with ([`OPEN`], [`CLOSE`]), and gives them their braces at
//! the end, when every `{{` still there is the workflow's: that one is
//! parted, and said.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use serde_yaml::{Mapping, Value};

use crate::common::*;
pub use crate::common::{Imported, Kind, Note};

/// What the importer is told beside the file.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// What the pipeline is called where the workflow's own name cannot be
    /// a pipeline's: its file's name, say.
    pub name: Option<String>,
    /// The image a `runs-on` label means. A label GitHub hosts has an image
    /// of its own name unless one is given here.
    pub images: BTreeMap<String, String>,
}

/// The images that stand for the labels GitHub hosts. A hosted runner holds
/// a great many tools, and these hold an operating system: the report says
/// so wherever one is used.
const HOSTED: &[(&str, &str)] = &[
    ("ubuntu-latest", "docker.io/library/ubuntu:24.04"),
    ("ubuntu-24.04", "docker.io/library/ubuntu:24.04"),
    ("ubuntu-22.04", "docker.io/library/ubuntu:22.04"),
    ("ubuntu-20.04", "docker.io/library/ubuntu:20.04"),
];

/// The actions that install a toolchain, and the image that already holds
/// it: the key of `with:` that names the version, the image before the
/// version, and after it.
const SETUP: &[(&str, &str, &str, &str)] = &[
    (
        "actions/setup-node",
        "node-version",
        "docker.io/library/node:",
        "-bookworm-slim",
    ),
    (
        "actions/setup-python",
        "python-version",
        "docker.io/library/python:",
        "-slim-bookworm",
    ),
    (
        "actions/setup-go",
        "go-version",
        "docker.io/library/golang:",
        "-bookworm",
    ),
    (
        "actions/setup-java",
        "java-version",
        "docker.io/library/eclipse-temurin:",
        "-jdk",
    ),
    (
        "dtolnay/rust-toolchain",
        "toolchain",
        "docker.io/library/rust:",
        "-bookworm",
    ),
    (
        "actions-rs/toolchain",
        "toolchain",
        "docker.io/library/rust:",
        "-bookworm",
    ),
];

/// The cache actions: each becomes a cache of the job's own
/// (`workspace.cache`), restored before its steps and saved after they
/// succeed.
const CACHES: &[&str] = &[
    "actions/cache",
    "Swatinem/rust-cache",
    "actions/cache/restore",
    "actions/cache/save",
];

/// A cache action as a cache of the job's: `Swatinem/rust-cache` as cargo's
/// directories, `actions/cache` (and its restore and save halves) from its
/// `path` and its `key`, whose own words make the prefix and whose
/// `hashFiles(…)` make the files of the key. `Err`: why it is not one.
fn cache_action(action: &str, with: Option<&Mapping>) -> Result<(JobCache, Vec<String>), String> {
    if action == "Swatinem/rust-cache" {
        return Ok((
            JobCache::new(
                "cargo",
                &["**/Cargo.lock"],
                &[
                    "/usr/local/cargo/registry",
                    "/usr/local/cargo/git",
                    "~/.cargo/registry",
                    "~/.cargo/git",
                    "target",
                ],
            ),
            vec![],
        ));
    }
    let text = |name: &str| with.and_then(|w| get(w, name)).and_then(scalar);
    let Some(path) = text("path") else {
        return Err("it names no path".into());
    };
    let Some(cache_key) = text("key") else {
        return Err("it names no key".into());
    };
    let paths: Vec<String> = path
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    if paths
        .iter()
        .any(|p| p.starts_with('!') || p.contains('*') || p.contains("${{"))
    {
        return Err("its path names files by a pattern or an expression, and a cache here keeps directories".into());
    }
    if paths.is_empty() || paths.len() > 16 || paths.iter().any(|p| p.split('/').any(|c| c == ".."))
    {
        return Err("its path is not one to 16 directories".into());
    }
    let mut words = String::new();
    let mut key_files = Vec::new();
    let mut not_read = Vec::new();
    let mut rest = cache_key.as_str();
    while let Some(start) = rest.find("${{") {
        words.push_str(&rest[..start]);
        words.push('-');
        let Some(len) = rest[start..].find("}}") else {
            return Err("its key holds an expression that does not end".into());
        };
        let expression = rest[start + 3..start + len].trim();
        rest = &rest[start + len + 2..];
        match expression
            .strip_prefix("hashFiles(")
            .and_then(|e| e.strip_suffix(')'))
        {
            Some(args) => {
                for arg in args.split(',') {
                    let arg = arg.trim().trim_matches(|c| c == '\'' || c == '"');
                    if arg.is_empty() || arg.starts_with('/') || arg.split('/').any(|c| c == "..") {
                        return Err(format!("its key's hashFiles names {arg:?}, which is not a file of the workspace"));
                    }
                    key_files.push(arg.to_string());
                }
            }
            None if matches!(expression, "runner.os" | "runner.arch") => {}
            None => not_read.push(expression.to_string()),
        }
    }
    words.push_str(rest);
    let prefix = words
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_'))
        .filter(|w| {
            !w.is_empty() && !matches!(w.to_ascii_lowercase().as_str(), "linux" | "ubuntu" | "x64")
        })
        .collect::<Vec<_>>()
        .join("-");
    let prefix: String = prefix.chars().take(64).collect();
    let prefix = if prefix.is_empty() {
        "cache".to_string()
    } else {
        prefix
    };
    let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
    let files: Vec<&str> = key_files.iter().map(String::as_str).collect();
    Ok((JobCache::new(&prefix, &files, &paths), not_read))
}

/// A setup action's own `cache:` input as a cache of the job's, for the
/// package managers whose directories are known. `None`: not one of those.
fn setup_cache(action: &str, asked: &str, dependency_path: Option<&str>) -> Option<JobCache> {
    let mut cache = match (action, asked) {
        ("actions/setup-node", "npm") => {
            JobCache::new("npm", &["**/package-lock.json"], &["~/.npm"])
        }
        ("actions/setup-node", "yarn") => {
            JobCache::new("yarn", &["**/yarn.lock"], &["~/.cache/yarn"])
        }
        ("actions/setup-node", "pnpm") => JobCache::new(
            "pnpm",
            &["**/pnpm-lock.yaml"],
            &["~/.local/share/pnpm/store"],
        ),
        ("actions/setup-python", "pip") => {
            JobCache::new("pip", &["**/requirements*.txt"], &["~/.cache/pip"])
        }
        ("actions/setup-go", "true") => {
            JobCache::new("go", &["**/go.sum"], &["/go/pkg/mod", "~/.cache/go-build"])
        }
        ("actions/setup-java", "maven") => {
            JobCache::new("maven", &["**/pom.xml"], &["~/.m2/repository"])
        }
        ("actions/setup-java", "gradle") => JobCache::new(
            "gradle",
            &["**/*.gradle*", "**/gradle-wrapper.properties"],
            &["~/.gradle/caches", "~/.gradle/wrapper"],
        ),
        _ => return None,
    };
    if let Some(paths) = dependency_path {
        let files: Vec<String> = paths
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect();
        if !files.is_empty()
            && files
                .iter()
                .all(|f| !f.starts_with('/') && !f.split('/').any(|c| c == ".."))
        {
            cache.key_files = files;
        }
    }
    Some(cache)
}

/// What a script reads of its event in a workflow, and the variable a
/// pipeline's step reads it from.
const CONTEXT_VARS: &[(&str, &str)] = &[
    ("github.sha", "CI_SHA"),
    ("github.ref", "CI_REF"),
    ("github.event_name", "CI_EVENT"),
    ("github.repository", "CI_REPO"),
    ("github.event.number", "CI_PR"),
    ("github.event.pull_request.number", "CI_PR"),
    ("github.event.pull_request.base.sha", "CI_BASE_SHA"),
    ("github.event.before", "CI_BASE_SHA"),
];

/// The variables GitHub's runner sets, and the ones a step has here. Every
/// other variable of the runner's (`GITHUB_…`, `RUNNER_…`) is one a step
/// here is not given, and a script that reads one is left to do.
const RUNNER_VARS: &[(&str, &str)] = &[
    ("GITHUB_SHA", "CI_SHA"),
    ("GITHUB_REF", "CI_REF"),
    ("GITHUB_EVENT_NAME", "CI_EVENT"),
    ("GITHUB_REPOSITORY", "CI_REPO"),
    ("GITHUB_STEP_SUMMARY", "CI_STEP_SUMMARY"),
];

/// The most combinations a matrix is written out to: GitHub's own limit
/// for the jobs of one.
const MATRIX_MOST: usize = 256;

/// What a pull request's pipeline runs for: the activities GitHub runs a
/// workflow for when it names none.
const PULL_REQUEST_RUNS: &[&str] = &["opened", "synchronize", "reopened"];

/// The `${{ … }}` of `text`, each with where it starts and ends, and what
/// is between the braces.
fn expressions(text: &str) -> Vec<(usize, usize, &str)> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(start) = text[from..].find("${{").map(|at| at + from) {
        let Some(end) = text[start..].find("}}").map(|at| at + start + 2) else {
            break;
        };
        found.push((start, end, text[start + 3..end - 2].trim()));
        from = end;
    }
    found
}

/// Whether `expr` is a name and its properties and nothing else:
/// `github.event.number`, `matrix.os`, `secrets.TOKEN`.
fn is_path(expr: &str) -> bool {
    !expr.is_empty()
        && expr.split('.').all(|part| {
            let mut chars = part.chars();
            matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        })
}

/// A step's name with its expressions shown as what they read, without
/// the braces a pipeline would take for its own, and on one line.
fn plain_name(name: &str) -> String {
    let mut out = String::new();
    let mut from = 0;
    for (start, end, expr) in expressions(name) {
        out.push_str(&name[from..start]);
        out.push_str(expr);
        from = end;
    }
    out.push_str(&name[from..]);
    let out = out.replace("{{", "").replace("}}", "");
    // A name folded over lines ends in one; dagron-ci takes one line.
    let line: String = out
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    line.trim_end().to_string()
}

/// A filter's pattern as one a pipeline's filter reads the same way, and
/// whether it had to be made to let in more.
///
/// Both read `*`, `**`, `[…]` and a leading `!` alike. GitHub's also has a
/// `+` and a `?` after a character, for one or more of it and for one or
/// none, and a pattern here has no word for either: a `?` here is any one
/// character, and a `+` is itself. So `x+` is written `x*` and `x?` is
/// written `*`, each of which lets in what the pattern did and more.
fn pattern_here(pattern: &str) -> (String, bool) {
    let mut out = String::new();
    // Where in `out` the character a `+` or a `?` would count begins.
    let mut counted: Option<usize> = None;
    let mut wider = false;
    // Whether `out` ends in a star that stands for a `+` or a `?`.
    let mut stands = false;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                counted = Some(out.len());
                out.push(c);
                out.extend(chars.next());
            }
            '[' => {
                counted = Some(out.len());
                out.push(c);
                // A `]` that is the set's first character is one of the set.
                let mut first = true;
                for inside in chars.by_ref() {
                    out.push(inside);
                    if inside == ']' && !first {
                        break;
                    }
                    first = first && inside == '!';
                }
            }
            '+' | '?' if counted.is_some() => {
                if c == '?' {
                    out.truncate(counted.unwrap_or(out.len()));
                }
                // Two stars would be a third thing: any number of segments.
                if !out.ends_with('*') {
                    out.push('*');
                }
                counted = None;
                wider = true;
                stands = true;
                continue;
            }
            // After a star that stands for a count, a star adds nothing.
            '*' if stands => {}
            // With nothing before it to count, it is itself.
            '?' => {
                out.push_str("\\?");
                counted = None;
            }
            '*' => {
                out.push(c);
                counted = None;
            }
            c => {
                counted = Some(out.len());
                out.push(c);
            }
        }
        stands = false;
    }
    (out, wider)
}

/// A timeout in minutes, as seconds. `None` where it is not a number.
fn seconds(minutes: &Value) -> Option<u64> {
    let minutes = minutes.as_f64().filter(|m| m.is_finite() && *m > 0.0)?;
    Some(((minutes * 60.0).ceil() as u64).max(1))
}

/// What a workflow's own top level gives every job.
#[derive(Default)]
struct Shared {
    env: Vec<(String, String)>,
    shell: Option<String>,
    working_directory: Option<String>,
    /// A dispatch's inputs, by the workflow's name for each, as the
    /// parameter the pipeline has for it.
    inputs: BTreeMap<String, String>,
    /// Whether the pipeline runs for an event that is no dispatch, and so
    /// for one that gives no input.
    other_events: bool,
}

/// A job's matrix, written out.
struct Matrix {
    /// The workflow's own axes, by name.
    axes: Vec<String>,
    /// Every key an instance's item has.
    keys: BTreeSet<String>,
    /// The instances, each with every key.
    combos: Vec<Mapping>,
}

impl Matrix {
    /// The template that reads `axis` of the instance's item. It is the
    /// item's key until the pipeline is done, when a matrix of one axis and
    /// nothing else is written as plain values ([`settle`]).
    fn template(&self, axis: &str) -> Option<String> {
        self.keys.contains(axis).then(|| tpl(&format!("item.{axis}")))
    }

    /// What `axis` is, in each instance.
    fn values(&self, axis: &str) -> Vec<&Value> {
        self.combos.iter().filter_map(|combo| combo.get(key(axis))).collect()
    }

    /// Give every instance a key of the importer's own making, one value
    /// for each, and say what it is called.
    fn derive(&mut self, wanted: &str, values: Vec<String>) -> String {
        let mut name = wanted.to_string();
        while self.keys.contains(&name) {
            name.push('_');
        }
        for (combo, value) in self.combos.iter_mut().zip(values) {
            combo.insert(key(&name), Value::String(value));
        }
        self.keys.insert(name.clone());
        name
    }

    /// The one axis, where the matrix has one and nothing else: its items
    /// are values, and `{{ item }}` is the axis.
    fn single(&self) -> Option<&str> {
        match (&self.axes[..], self.keys.len()) {
            ([axis], 1) => Some(axis),
            _ => None,
        }
    }

    fn items(&self) -> Vec<Value> {
        let single = self.single();
        self.combos
            .iter()
            .map(|combo| match single {
                Some(axis) => combo.get(key(axis)).cloned().unwrap_or(Value::Null),
                None => Value::Mapping(combo.clone()),
            })
            .collect()
    }
}

/// What a job gathers as its steps are read.
#[derive(Default)]
struct Gathered {
    /// Secrets a script or a variable read, by name.
    secrets: BTreeSet<String>,
    /// Those of them one step was given, and not the job.
    of_a_step: BTreeSet<String>,
    /// What is left to do in the job, for the step it starts with.
    todo: Vec<String>,
    /// Whether an action stands in the job as a step that fails.
    stands: bool,
}

/// A job as a task, before the step that says what is left of it.
struct Built {
    at: String,
    task: Mapping,
    todo: Vec<String>,
    /// Whether an action stands in the job as a step that fails.
    stands: bool,
    /// The axis of a matrix whose items are plain values.
    single: Option<String>,
}

struct Importer<'a> {
    options: &'a Options,
    report: Report,
}

impl Importer<'_> {
    /// A thing left to do in a job: in the report, and in the step the job
    /// starts with.
    fn todo(&mut self, job: &mut Gathered, at: &str, text: impl Into<String>) {
        self.report.todo(&mut job.todo, at, text);
    }

    /// Every key of `map` that is not one of `known`, as a note. One that
    /// is left to do is the job's, or with no job every job's.
    fn rest(
        &mut self,
        mut job: Option<&mut Gathered>,
        at: &str,
        map: &Mapping,
        known: &[&str],
        kind: Kind,
        why: &str,
    ) {
        for name in map.keys().filter_map(scalar) {
            if known.contains(&name.as_str()) {
                continue;
            }
            let at = match at {
                "" => name,
                at => format!("{at}.{name}"),
            };
            match (kind, job.as_deref_mut()) {
                (Kind::Todo, Some(job)) => self.todo(job, &at, why),
                (Kind::Todo, None) => self.report.todo_all(&at, why),
                _ => self.report.note(&at, kind, why),
            }
        }
    }

    fn name(&mut self, root: &Mapping) -> String {
        let own = get(root, "name").and_then(scalar);
        let named = own.as_deref().map(as_name).filter(|n| !n.is_empty());
        let name = named
            .clone()
            .or_else(|| {
                self.options
                    .name
                    .as_deref()
                    .map(as_name)
                    .filter(|n| !n.is_empty())
            })
            .unwrap_or_else(|| "ci".to_string());
        if let Some(own) = own {
            if own != name {
                self.report.note(
                    "name",
                    Kind::Changed,
                    format!("{own:?} is {name:?}: a pipeline's name is letters, digits, '.', '_' and '-', and its checks are called by it"),
                );
            }
        }
        name
    }

    /// A filter's patterns, under the name a pipeline has for it. A pattern
    /// GitHub reads in a way no pattern here does is written to let in what
    /// it did and more, and is left to do: what runs for more than it
    /// should is for a person to see.
    fn filters(&mut self, at: &str, of: Option<&Mapping>, known: &[&str], out: &mut Mapping) {
        let Some(of) = of else { return };
        for name in known {
            let Some(value) = get(of, name) else { continue };
            let mut patterns = Vec::new();
            for pattern in strings(value) {
                let (written, wider) = pattern_here(&pattern);
                if wider {
                    self.report.todo_all(
                        &format!("{at}.{name}"),
                        format!("`{pattern}` is written `{written}`, which lets in more than it did: a `+` or a `?` after a character is GitHub's word for how many of it, and a pattern here has no such word. Write the pattern for what it should let in"),
                    );
                }
                patterns.push(written);
            }
            out.insert(key(&name.replace('-', "_")), list(patterns));
        }
    }

    fn triggers(&mut self, root: &Mapping, shared: &mut Shared) -> Value {
        // An unquoted `on` is YAML 1.1's word for true, to some readers.
        let on = get(root, "on").or_else(|| root.get(Value::Bool(true)));
        let events: Vec<(String, Option<&Mapping>)> = match on {
            Some(Value::Mapping(map)) => map
                .iter()
                .filter_map(|(k, v)| Some((scalar(k)?, v.as_mapping())))
                .collect(),
            Some(other) => strings(other).into_iter().map(|e| (e, None)).collect(),
            None => Vec::new(),
        };
        let mut out = Mapping::new();
        for (event, filters) in events {
            let at = format!("on.{event}");
            match event.as_str() {
                "push" => {
                    let known = ["branches", "branches-ignore", "tags", "tags-ignore", "paths", "paths-ignore"];
                    let mut push = Mapping::new();
                    self.filters(&at, filters, &known, &mut push);
                    if let Some(of) = filters {
                        self.rest(None, &at, of, &known, Kind::Left, "a push takes branches, tags and paths, and nothing else");
                    }
                    out.insert(key("push"), Value::Mapping(push));
                }
                "pull_request" => {
                    let known = ["branches", "branches-ignore", "paths", "paths-ignore"];
                    if let Some(types) = filters.and_then(|of| get(of, "types")).map(strings) {
                        // A workflow for a pull request that is closed, or
                        // labelled, is not one for a pull request that is
                        // opened: it is not written as one.
                        if !types.is_empty() && !types.iter().any(|t| PULL_REQUEST_RUNS.contains(&t.as_str())) {
                            self.report.todo_all(
                                &format!("{at}.types"),
                                format!("the workflow ran when a pull request was {}, and a pipeline runs when one is opened, updated or reopened: the event is left out, and no pipeline runs for what it ran for", types.join(", ")),
                            );
                            continue;
                        }
                        self.report.note(
                            &format!("{at}.types"),
                            Kind::Left,
                            "a pull request runs when it is opened, updated or reopened, and for no other activity",
                        );
                    }
                    let mut pull = Mapping::new();
                    self.filters(&at, filters, &known, &mut pull);
                    if let Some(of) = filters {
                        let mut known = known.to_vec();
                        known.push("types");
                        self.rest(None, &at, of, &known, Kind::Left, "a pull request takes branches and paths, and nothing else");
                    }
                    out.insert(key("pull_request"), Value::Mapping(pull));
                }
                "merge_group" => {
                    out.insert(key("merge_group"), Value::Mapping(Mapping::new()));
                }
                "workflow_dispatch" => {
                    let mut dispatch = Mapping::new();
                    let inputs = filters.and_then(|of| get(of, "inputs")).and_then(Value::as_mapping);
                    if let Some(inputs) = inputs {
                        let mut declared = Mapping::new();
                        for (name, input) in inputs {
                            let Some(name) = scalar(name) else { continue };
                            let at = format!("{at}.inputs.{name}");
                            let parameter = as_parameter(&name);
                            // What dagron-ci takes for an input's name. One
                            // it does not take is not declared, and a script
                            // that reads it stops where it does.
                            if parameter.starts_with("ci_") || parameter.len() > 64 {
                                self.report.todo_all(&at, "an input here is named as a parameter is, in at most 64 characters, and not beginning ci_: it is left out. Give it another name, and say it where it is read");
                                continue;
                            }
                            if declared.contains_key(key(&parameter)) {
                                self.report.todo_all(&at, format!("two inputs come to the name {parameter:?}: this one is left out. Give it another name, and say it where it is read"));
                                continue;
                            }
                            if parameter != name {
                                self.report.note(&at, Kind::Changed, format!("the input is {parameter:?} here: an input is named as a parameter is"));
                            }
                            let mut one = Mapping::new();
                            if let Some(input) = input.as_mapping() {
                                if let Some(text) = get(input, "description").and_then(scalar) {
                                    one.insert(key("description"), Value::String(one_line(&text)));
                                }
                                if let Some(default) = get(input, "default").and_then(scalar) {
                                    one.insert(key("default"), Value::String(default));
                                }
                                if get(input, "required") == Some(&Value::Bool(true)) {
                                    one.insert(key("required"), Value::Bool(true));
                                }
                                if let Some(kind) = get(input, "type").and_then(scalar).filter(|t| t != "string") {
                                    self.report.note(&at, Kind::Left, format!("its type, {kind}: every input here is text, and what it may be is for the step that reads it to check"));
                                }
                            }
                            shared.inputs.insert(name, parameter.clone());
                            declared.insert(key(&parameter), Value::Mapping(one));
                        }
                        dispatch.insert(key("inputs"), Value::Mapping(declared));
                    }
                    out.insert(key("dispatch"), Value::Mapping(dispatch));
                }
                "schedule" => self.report.note(
                    &at,
                    Kind::Left,
                    "a pipeline has no schedule: ask for a run from a scheduler of your own, through the run API (on.dispatch)",
                ),
                "pull_request_target" => self.report.todo_all(
                    &at,
                    "no event here runs a pull request's pipeline as its base branch has it: a pull request's trust is decided by who sent it (docs/CI.md)",
                ),
                _ => self.report.note(&at, Kind::Left, "no such event is sent here: a pipeline runs for a push, a pull request, a merge group or a dispatch"),
            }
        }
        if out.is_empty() {
            self.report.todo_all(
                "on",
                "none of the workflow's events is one a pipeline runs for: it is given a dispatch, so that it can be asked for",
            );
            out.insert(key("dispatch"), Value::Mapping(Mapping::new()));
        }
        shared.other_events = out.keys().any(|event| event != &key("dispatch"));
        Value::Mapping(out)
    }

    /// A workflow's concurrency group, as a pipeline's key.
    fn concurrency(&mut self, at: &str, value: &Value, name: &str) -> Option<Value> {
        let (group, cancels) = match value {
            Value::Mapping(map) => (
                get(map, "group").and_then(scalar)?,
                get(map, "cancel-in-progress"),
            ),
            other => (scalar(other)?, None),
        };
        // Whether a newer run of the group takes the older one's place.
        let cancels = match cancels {
            Some(Value::Bool(true)) => true,
            None | Some(Value::Null) | Some(Value::Bool(false)) => {
                self.report.note(
                    at,
                    Kind::Changed,
                    "a newer run of the group waits its turn, and every run that waits has one, where GitHub keeps the newest of them alone",
                );
                false
            }
            Some(_) => {
                self.report.todo_all(
                    &format!("{at}.cancel-in-progress"),
                    "whether a newer run cancels the older one is decided as the run goes there, and is said when a run is made here: say concurrency.cancel_superseded as true, or leave it out and a newer run waits its turn",
                );
                false
            }
        };
        let mut text = String::new();
        let mut from = 0;
        let mut grouped = false;
        for (start, end, expr) in expressions(&group) {
            text.push_str(&group[from..start]);
            from = end;
            match expr {
                "github.workflow" => text.push_str(name),
                "github.ref"
                | "github.head_ref"
                | "github.ref_name"
                | "github.event.number"
                | "github.event.pull_request.number" => {
                    grouped = true;
                    text.push_str(&tpl("ci_group"));
                }
                other
                    if other.contains("github.ref")
                        || other.contains("head_ref")
                        || other.contains("pull_request.number") =>
                {
                    grouped = true;
                    text.push_str(&tpl("ci_group"));
                    self.report.note(
                        at,
                        Kind::Changed,
                        format!("`{other}` in the group is the run's own group, {{{{ ci_group }}}}: the branch, or the pull request"),
                    );
                }
                other => {
                    self.report.todo_all(
                        at,
                        format!("`{other}` in the group has no place in a key: the key is written without it"),
                    );
                }
            }
        }
        text.push_str(&group[from..]);
        if grouped {
            self.report.note(
                at,
                Kind::Changed,
                "the group is keyed by {{ ci_group }}: the branch, or the pull request",
            );
        }
        // A group that was all of what has no place in a key is no key.
        if text.trim().is_empty() {
            return None;
        }
        let mut out = Mapping::new();
        out.insert(key("key"), Value::String(text));
        if cancels {
            out.insert(key("cancel_superseded"), Value::Bool(true));
        }
        Some(Value::Mapping(out))
    }

    /// A job's matrix, written out as GitHub writes it out: the axes'
    /// combinations, less what `exclude` takes out, with what `include`
    /// adds.
    fn matrix(&mut self, job: &mut Gathered, at: &str, strategy: &Mapping) -> Option<Matrix> {
        if get(strategy, "fail-fast").is_some() {
            self.report.note(
                &format!("{at}.fail-fast"),
                Kind::Left,
                "an instance that fails does not stop the others here, whatever this said",
            );
        }
        if get(strategy, "max-parallel").is_some() {
            self.report.note(
                &format!("{at}.max-parallel"),
                Kind::Left,
                "how many instances run at once is the engine's to say (a pool's size), and not a job's",
            );
        }
        let at = format!("{at}.matrix");
        let matrix = match get(strategy, "matrix")? {
            Value::Mapping(matrix) => matrix,
            _ => {
                self.todo(job, &at, "a matrix that is made as the run goes is not imported: write its items out (with_items), or have a job print them (with_output_of)");
                return None;
            }
        };
        let mut axes: Vec<(String, Vec<Value>)> = Vec::new();
        for (name, values) in matrix {
            let Some(name) = scalar(name) else { continue };
            if matches!(name.as_str(), "include" | "exclude") {
                continue;
            }
            match values {
                Value::Sequence(values)
                    if values.iter().all(|v| scalar(v).is_some_and(|v| !v.contains("${{"))) =>
                {
                    axes.push((name, values.clone()))
                }
                _ => {
                    self.todo(
                        job,
                        &format!("{at}.{name}"),
                        "an axis that is not a list of plain values is not imported: write its values out",
                    );
                    return None;
                }
            }
        }
        // What is added and taken out is a list of plain combinations, or
        // it is made as the run goes, and is not read here.
        let mut listed: BTreeMap<&str, Vec<Mapping>> = BTreeMap::new();
        for name in ["include", "exclude"] {
            let Some(value) = get(matrix, name) else { continue };
            fn plain(entry: &Value) -> Option<&Mapping> {
                entry.as_mapping().filter(|entry| {
                    entry.iter().all(|(k, v)| {
                        scalar(k).is_some() && scalar(v).is_some_and(|v| !v.contains("${{"))
                    })
                })
            }
            match value.as_sequence().map(|list| list.iter().map(plain).collect::<Option<Vec<_>>>()) {
                Some(Some(entries)) => {
                    listed.insert(name, entries.into_iter().cloned().collect());
                }
                _ => {
                    self.todo(
                        job,
                        &format!("{at}.{name}"),
                        "what the matrix is given here is not a list of plain combinations: it is made as the run goes, and is not imported. Write the items out (with_items)",
                    );
                    return None;
                }
            }
        }
        let most = axes
            .iter()
            .fold(1usize, |all, (_, values)| all.saturating_mul(values.len()))
            .saturating_add(listed.get("include").map_or(0, Vec::len));
        if most > MATRIX_MOST {
            self.todo(
                job,
                &at,
                format!("the matrix comes to more than {MATRIX_MOST} combinations, which is more than GitHub runs of one: it is not written out"),
            );
            return None;
        }
        let mut combos: Vec<Mapping> = vec![Mapping::new()];
        for (name, values) in &axes {
            combos = combos
                .iter()
                .flat_map(|combo| {
                    values.iter().map(move |value| {
                        let mut next = combo.clone();
                        next.insert(key(name), value.clone());
                        next
                    })
                })
                .collect();
        }
        if axes.is_empty() {
            combos.clear();
        }
        let mut entries = |name: &str| listed.remove(name).unwrap_or_default();
        for exclude in entries("exclude") {
            combos.retain(|combo| !exclude.iter().all(|(k, v)| combo.get(k) == Some(v)));
        }
        let axis_names: BTreeSet<String> = axes.iter().map(|(name, _)| name.clone()).collect();
        let mut extra = Vec::new();
        for include in entries("include") {
            let fits = |combo: &Mapping| {
                include
                    .iter()
                    .filter(|(k, _)| scalar(k).is_some_and(|k| axis_names.contains(&k)))
                    .all(|(k, v)| combo.get(k) == Some(v))
            };
            if combos.iter().any(fits) {
                for combo in combos.iter_mut().filter(|combo| fits(combo)) {
                    for (k, v) in &include {
                        combo.insert(k.clone(), v.clone());
                    }
                }
            } else {
                extra.push(include);
            }
        }
        combos.extend(extra);
        if combos.is_empty() {
            self.todo(job, &at, "the matrix has no combination left");
            return None;
        }
        let keys: BTreeSet<String> = combos
            .iter()
            .flat_map(|c| c.keys().filter_map(scalar))
            .collect();
        // Every combination has every key, as GitHub reads a key a
        // combination was not given: as nothing.
        for combo in &mut combos {
            for name in &keys {
                if !combo.contains_key(key(name)) {
                    combo.insert(key(name), Value::String(String::new()));
                }
            }
        }
        Some(Matrix {
            axes: axes.into_iter().map(|(name, _)| name).collect(),
            keys,
            combos,
        })
    }

    /// A text with its expressions as templates a pipeline fills in, where
    /// each is one a variable's value or an image may hold. Where one is
    /// not, why not.
    fn templated(
        &self,
        text: &str,
        shared: &Shared,
        matrix: Option<&Matrix>,
    ) -> Result<String, String> {
        let mut out = String::new();
        let mut from = 0;
        for (start, end, expr) in expressions(text) {
            out.push_str(&text[from..start]);
            from = end;
            let template = match expr.split_once('.') {
                Some(("matrix", axis)) => matrix.and_then(|m| m.template(axis)),
                // An input is a parameter of a dispatch's run, and of no
                // other: dagron leaves a name it was not given as it is
                // written, braces and all.
                Some(("inputs", name)) if shared.other_events && shared.inputs.contains_key(name) => {
                    return Err(format!("`{expr}` is given by a dispatch alone, and this pipeline runs for other events too, where dagron would leave the input's name in braces: read it in a step's script instead"));
                }
                Some(("inputs", name)) => shared.inputs.get(name).map(|p| tpl(p)),
                _ => CONTEXT_VARS
                    .iter()
                    .find(|(path, _)| *path == expr)
                    .map(|(_, var)| tpl(&var.to_ascii_lowercase())),
            };
            out.push_str(&template.ok_or_else(|| format!("`{expr}` has nothing to stand for it here"))?);
        }
        out.push_str(&text[from..]);
        Ok(out)
    }

    /// A script with its expressions as the variables a step reads here.
    /// What a variable has to be given for is put in `env`.
    fn script(
        &mut self,
        job: &mut Gathered,
        at: &str,
        text: &str,
        shared: &Shared,
        matrix: Option<&Matrix>,
        env: &mut BTreeMap<String, String>,
    ) -> String {
        let mut out = String::new();
        let mut from = 0;
        // The inputs the script reads, as the variables it reads them from.
        let mut inputs = BTreeSet::new();
        let mut filled = false;
        let mut quoted = false;
        for (start, end, expr) in expressions(text) {
            out.push_str(&text[from..start]);
            from = end;
            let context = CONTEXT_VARS.iter().find(|(path, _)| *path == expr);
            let variable = match (context, expr.split_once('.')) {
                (Some((_, var)), _) => Some(var.to_string()),
                (None, Some(("env", name))) if is_shell_name(name) => Some(name.to_string()),
                (None, Some(("secrets", name)))
                    if is_shell_name(name) && !name.eq_ignore_ascii_case("GITHUB_TOKEN") =>
                {
                    job.secrets.insert(name.to_string());
                    job.of_a_step.insert(name.to_string());
                    Some(name.to_string())
                }
                (None, Some(("matrix", axis))) => {
                    matrix.and_then(|m| m.template(axis)).map(|template| {
                        let var = format!("MATRIX_{}", as_variable(axis));
                        env.insert(var.clone(), template);
                        var
                    })
                }
                (None, Some(("inputs", name))) => shared.inputs.get(name).map(|parameter| {
                    let var = format!("INPUT_{}", as_variable(name));
                    env.insert(var.clone(), tpl(parameter));
                    inputs.insert(var.clone());
                    var
                }),
                _ => None,
            };
            if variable.is_some() || expr == "github.server_url" {
                filled = true;
                quoted = quoted || in_single_quotes(&out);
            }
            match (variable, expr) {
                (Some(var), _) => out.push_str(&format!("${{{var}}}")),
                (None, "github.server_url") => out.push_str("https://${CI_FORGE_HOST}"),
                (None, "runner.os") => out.push_str("Linux"),
                (None, _) => {
                    // Not read here: the step stops where it would have
                    // used it, and says which.
                    // Named in words a shell reads as words and nothing
                    // else, wherever in the script it stands.
                    let said: String = expr
                        .chars()
                        .map(|c| match c {
                            c if c.is_ascii_alphanumeric() || "._-".contains(c) => c,
                            _ => ' ',
                        })
                        .collect();
                    out.push_str(&format!(
                        "${{IMPORT_TODO:?not imported, {}}}",
                        one_line(&said)
                    ));
                    self.todo(
                        job,
                        at,
                        format!("`{expr}` in the script has nothing to stand for it here: the step stops where it is read"),
                    );
                }
            }
        }
        out.push_str(&text[from..]);
        // GitHub wrote an expression's value into the script before any
        // shell read it. Here it is a variable, which the shell fills in
        // where a shell fills one in, and nowhere else.
        if filled {
            self.report.note(at, Kind::Changed, "what the script read in `${{ }}` is a variable of the shell's here, which the shell fills in as it runs the script");
        }
        if quoted {
            self.todo(job, at, "an expression stood inside single quotes, where the shell fills nothing in: the variable in its place is read there as it is written. Close the quotes around it");
        }
        if filled && has_quoted_heredoc(&out) {
            self.todo(job, at, "the script has a here-document whose mark is quoted, in which the shell fills nothing in: a variable that stands for an expression there is read as it is written");
        }
        for (theirs, ours) in RUNNER_VARS {
            if reads(&out, theirs) {
                out = renamed(&out, theirs, ours);
                self.report.note(at, Kind::Changed, format!("${theirs} is ${ours} here"));
            }
        }
        if reads(&out, "CI_EVENT") {
            self.report.note(at, Kind::Changed, "the event's kind is read from $CI_EVENT, which is `tag` for a push of a tag where GitHub says `push`, and `dispatch` where it says `workflow_dispatch`");
            if out.contains("workflow_dispatch") {
                self.todo(job, at, "the script names `workflow_dispatch`, and the event's kind is `dispatch` here: say that in its place");
            }
        }
        for theirs in variables(&out) {
            if theirs.starts_with("GITHUB_") || theirs.starts_with("RUNNER_") {
                self.todo(
                    job,
                    at,
                    format!("the script reads ${theirs}, which a step here is not given"),
                );
            }
        }
        if let Some(seen) = bashism(&out) {
            self.todo(job, at, format!("the script is bash's ({seen}), and a step here is run by `sh -e`: call bash in it, or write it for sh"));
        }
        // An input is a dispatch's, and nothing on any other event, as on
        // GitHub. dagron leaves a parameter it was not given as its name in
        // braces, so the step says nothing itself.
        if shared.other_events && !inputs.is_empty() {
            let unset: Vec<String> = inputs.iter().map(|var| format!("{var}=")).collect();
            out = format!("[ \"$CI_EVENT\" = dispatch ] || {{ {}; }}\n{out}", unset.join("; "));
            self.report.note(at, Kind::Changed, "an input is given by a dispatch alone: the script begins by making it nothing on any other event, as it is on GitHub");
        }
        out
    }

    /// A condition as the lines of shell that leave a step unrun where it
    /// is false. `None` where it is one that cannot be read so.
    fn guard(
        &self,
        condition: &str,
        matrix: Option<&Matrix>,
        env: &mut BTreeMap<String, String>,
    ) -> Option<String> {
        let mut lines = String::new();
        for part in unwrap(condition).split("&&") {
            let part = part.trim();
            let test = if let Some(prefix) = starts_with_ref(part) {
                format!("case \"$CI_REF\" in '{prefix}'*) ;; *) exit 0 ;; esac")
            } else {
                let (left, equal, right) = comparison(part)?;
                let var = match left.split_once('.') {
                    Some(("matrix", axis)) => {
                        let matrix = matrix?;
                        // GitHub compares a yes or a no with text as two
                        // numbers, and they are never the same one. What
                        // was meant by it is not for the importer to say.
                        if matrix.values(axis).iter().any(|v| v.is_bool()) {
                            return None;
                        }
                        let var = format!("MATRIX_{}", as_variable(axis));
                        env.insert(var.clone(), matrix.template(axis)?);
                        var
                    }
                    _ => CONTEXT_VARS
                        .iter()
                        .find(|(path, _)| *path == left)?
                        .1
                        .to_string(),
                };
                // Each in quotes: the text, and no pattern of the shell's,
                // and a text of nothing is still one.
                let wanted = match (var.as_str(), right.as_str()) {
                    // A push of a tag is a push to GitHub, and a tag here.
                    ("CI_EVENT", "push") => "'push'|'tag'".to_string(),
                    ("CI_EVENT", "workflow_dispatch") => "'dispatch'".to_string(),
                    (_, value)
                        if value
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || "._/-".contains(c)) =>
                    {
                        format!("'{value}'")
                    }
                    _ => return None,
                };
                match equal {
                    true => format!("case \"${var}\" in {wanted}) ;; *) exit 0 ;; esac"),
                    false => format!("case \"${var}\" in {wanted}) exit 0 ;; esac"),
                }
            };
            lines.push_str(&test);
            lines.push('\n');
        }
        Some(lines)
    }

    /// A job's condition as dagron reads one, or as a trigger rule.
    fn when(&mut self, job: &mut Gathered, at: &str, condition: &str, task: &mut Mapping) {
        let condition = unwrap(condition);
        match condition.replace(' ', "").as_str() {
            "always()" => {
                task.insert(key("trigger_rule"), key("all_done"));
                return;
            }
            "failure()" => {
                task.insert(key("trigger_rule"), key("one_failed"));
                return;
            }
            "success()" => return,
            _ => {}
        }
        let mut parts = Vec::new();
        for part in condition.split("&&") {
            let part = part.trim();
            if starts_with_ref(part) == Some("refs/tags/".to_string()) {
                parts.push(format!("{} == tag", tpl("ci_event")));
                continue;
            }
            match comparison(part) {
                Some((left, equal, right)) if left == "github.event_name" => {
                    let (op, value) = match (right.as_str(), equal) {
                        ("push", true) => ("in", "[push, tag]".to_string()),
                        ("push", false) => ("not in", "[push, tag]".to_string()),
                        ("workflow_dispatch", true) => ("==", "dispatch".to_string()),
                        ("workflow_dispatch", false) => ("!=", "dispatch".to_string()),
                        (event @ ("pull_request" | "merge_group"), true) => {
                            ("==", event.to_string())
                        }
                        (event @ ("pull_request" | "merge_group"), false) => {
                            ("!=", event.to_string())
                        }
                        _ => {
                            self.todo(
                                job,
                                at,
                                format!("`{part}` names an event no pipeline runs for"),
                            );
                            return;
                        }
                    };
                    parts.push(format!("{} {op} {value}", tpl("ci_event")));
                }
                _ => {
                    self.todo(
                        job,
                        at,
                        format!("`{condition}` is not a condition a job's when: can hold (a when: may name the event's kind, and nothing an outsider chose): put it in on:, or test it in a step"),
                    );
                    return;
                }
            }
        }
        task.insert(key("when"), Value::String(parts.join(" and ")));
    }

    /// A job's services, as a task's.
    fn services(&mut self, job: &mut Gathered, at: &str, services: &Mapping) -> Value {
        let mut out = Vec::new();
        let mut named = BTreeSet::new();
        for (name, service) in services {
            let Some(name) = scalar(name) else { continue };
            let at = format!("{at}.{name}");
            let Some(service) = service.as_mapping() else {
                continue;
            };
            // A service is reached at its name, which is a host's.
            let Some(host) = as_service_name(&name).filter(|host| named.insert(host.clone())) else {
                self.todo(job, &at, "the service's name cannot be a host's name here, or is another service's: it is left out. Name it in lowercase letters, digits and `-`, beginning with a letter");
                continue;
            };
            if host != name {
                self.todo(job, &at, format!("the service is reached as `{host}` here, where the workflow's scripts reach it as `{name}`: a service's name is a host's"));
            }
            let mut one = Mapping::new();
            one.insert(key("name"), Value::String(host.clone()));
            match get(service, "image").and_then(scalar) {
                Some(image) if !image.contains("${{") => {
                    one.insert(key("image"), Value::String(image));
                }
                _ => {
                    self.todo(job, &at, "the service's image is not a plain one: name it");
                    continue;
                }
            }
            if let Some(env) = get(service, "env").and_then(Value::as_mapping) {
                let mut vars = Vec::new();
                for (var, value) in env {
                    match (scalar(var), scalar(value)) {
                        (Some(var), Some(value)) if !value.contains("${{") => {
                            let mut pair = Mapping::new();
                            pair.insert(key("name"), Value::String(var));
                            pair.insert(key("value"), Value::String(value));
                            vars.push(Value::Mapping(pair));
                        }
                        (Some(var), _) => self.todo(job, &format!("{at}.env.{var}"), "a service is given plain values, and no secret: write the value, or leave the variable out"),
                        _ => {}
                    }
                }
                one.insert(key("env"), Value::Sequence(vars));
            }
            if let Some(options) = get(service, "options").and_then(scalar) {
                match health_command(&options) {
                    Some(ready) if ready.contains("${{") => self.todo(job, &format!("{at}.options"), "its health command reads an expression, and a service is told nothing of the run: say what makes it ready with `ready`"),
                    Some(ready) => {
                        one.insert(key("ready"), Value::String(ready));
                        self.report.note(&format!("{at}.options"), Kind::Changed, "its health command is the service's `ready`; its other options are left");
                    }
                    None => self.report.note(&format!("{at}.options"), Kind::Left, "a service takes no options of the container runtime: say what makes it ready with `ready`"),
                }
            }
            if get(service, "ports").is_some() {
                self.report.note(&format!("{at}.ports"), Kind::Left, format!("the job reaches the service at its name, `{host}`, on the port its image listens on"));
            }
            self.rest(
                Some(&mut *job),
                &at,
                service,
                &["image", "env", "options", "ports"],
                Kind::Todo,
                "a service has no such key here",
            );
            out.push(Value::Mapping(one));
        }
        Value::Sequence(out)
    }

    #[allow(clippy::too_many_lines)]
    fn task(&mut self, id: &str, job: &Mapping, shared: &Shared) -> Built {
        let at = format!("jobs.{id}");
        let mut gathered = Gathered::default();
        let mut task = Mapping::new();
        task.insert(key("name"), Value::String(as_name(id)));

        if let Some(uses) = get(job, "uses").and_then(scalar) {
            self.todo(&mut gathered, &at, format!("the job calls the workflow {uses}: no workflow is called here, so its jobs are written out as this pipeline's own, or it is a pipeline of its own"));
            self.rest(None, &at, job, &["uses"], Kind::Left, "it is the call's, and goes with it: nothing of the workflow that was called is here to take it");
            task.insert(key("steps"), Value::Sequence(Vec::new()));
            return Built {
                at,
                task,
                todo: gathered.todo,
                stands: false,
                single: None,
            };
        }

        let mut matrix = get(job, "strategy")
            .and_then(Value::as_mapping)
            .and_then(|strategy| self.matrix(&mut gathered, &format!("{at}.strategy"), strategy));

        // The image: the job's container, else what its label means.
        let container = match get(job, "container") {
            Some(Value::Mapping(container)) => {
                self.rest(
                    Some(&mut gathered),
                    &format!("{at}.container"),
                    container,
                    &["image"],
                    Kind::Todo,
                    "a job's container is its image here, and takes nothing else",
                );
                get(container, "image").and_then(scalar)
            }
            Some(other) => scalar(other),
            None => None,
        };
        let labels = get(job, "runs-on").map(strings).unwrap_or_default();
        let mut image = match container {
            Some(image) => match self.templated(&image, shared, matrix.as_ref()) {
                Ok(image) => Some(image),
                Err(why) => {
                    self.todo(
                        &mut gathered,
                        &format!("{at}.container"),
                        format!("the image: {why}. Name the image"),
                    );
                    None
                }
            },
            None => None,
        };
        // The action that named the job's image, should one have.
        let mut toolchain: Option<String> = None;
        // What the job's label means, should no step of it say better: an
        // action that installs a toolchain names the image that holds it.
        let contained = get(job, "container").is_some();
        let given = labels
            .iter()
            .find_map(|label| self.options.images.get(label).cloned());
        let hosted = labels.iter().find_map(|label| {
            HOSTED
                .iter()
                .find(|(name, _)| name == label)
                .map(|(name, image)| (*name, *image))
        });

        // Steps first: an action may decide the image, and what a script
        // reads decides the job's variables.
        let defaults = get(job, "defaults")
            .and_then(Value::as_mapping)
            .and_then(|d| get(d, "run"))
            .and_then(Value::as_mapping);
        let shell = defaults
            .and_then(|d| get(d, "shell"))
            .and_then(scalar)
            .or_else(|| shared.shell.clone());
        let directory = defaults
            .and_then(|d| get(d, "working-directory"))
            .and_then(scalar)
            .or_else(|| shared.working_directory.clone());
        let mut steps = Vec::new();
        let at_job = at.clone();
        let mut depth: Option<Value> = None;
        let mut caches: Vec<JobCache> = Vec::new();
        let mut referenced = BTreeSet::new();
        let empty = Vec::new();
        let listed = get(job, "steps")
            .and_then(Value::as_sequence)
            .unwrap_or(&empty);
        for step in listed {
            if let Some(text) = step
                .as_mapping()
                .and_then(|s| get(s, "run"))
                .and_then(scalar)
            {
                for (_, _, expr) in expressions(&text) {
                    if let Some(rest) = expr.strip_prefix("steps.") {
                        referenced.insert(rest.split('.').next().unwrap_or_default().to_string());
                    }
                }
            }
        }
        for (n, step) in listed.iter().enumerate() {
            let at = format!("{at}.steps[{n}]");
            let Some(step) = step.as_mapping() else {
                continue;
            };
            let name = get(step, "name").and_then(scalar);
            if let Some(uses) = get(step, "uses").and_then(scalar) {
                let action = uses.split('@').next().unwrap_or(&uses).to_string();
                let version = uses.split_once('@').map(|(_, v)| v.to_string());
                let with = get(step, "with").and_then(Value::as_mapping);
                // A step that is not one here: what it set beside its
                // action goes with it, and is said.
                let gone = "the step is not one here, and what it set goes with it";
                if action == "actions/checkout" {
                    if let Some(with) = with {
                        if let Some(asked) = get(with, "fetch-depth") {
                            depth = Some(asked.clone());
                        }
                        self.rest(Some(&mut gathered), &at, with, &["fetch-depth"], Kind::Todo, "the checkout here is the event's commit of this repository, and takes no such setting");
                    }
                    self.rest(None, &at, step, &["uses", "with", "name"], Kind::Left, gone);
                    continue;
                }
                if CACHES.contains(&action.as_str()) {
                    match cache_action(&action, with) {
                        Ok((cache, _)) if caches.iter().any(|c: &JobCache| c.prefix == cache.prefix) => {
                            self.report.note(&at, Kind::Left, format!("{action} is a second cache of the prefix {:?}, which the job keeps already, so it is not carried over: name a workspace.cache of another prefix for it by hand", cache.prefix));
                        }
                        Ok((cache, not_read)) => {
                            let said = cache.said();
                            caches.push(cache);
                            self.report.note(&at, Kind::Changed, format!("{action} is {said}: restored before the steps and saved after they succeed, from the run's own branch and then a pull request's base, the entry of that key or else the newest of the prefix"));
                            if !not_read.is_empty() {
                                self.report.note(&format!("{at}.with.key"), Kind::Changed, format!("the key's {} is not part of it here: one cache of the prefix serves each", not_read.join(", ")));
                            }
                            if let Some(with) = with {
                                self.rest(None, &format!("{at}.with"), with, &["path", "key"], Kind::Left, "a cache here takes a path and a key, and nothing else of the action's");
                            }
                        }
                        Err(why) => self.report.note(&at, Kind::Left, format!("{action}: {why}, so the job starts cold: name a workspace.cache for it by hand")),
                    }
                    self.rest(None, &at, step, &["uses", "with", "name"], Kind::Left, gone);
                    continue;
                }
                if let Some((_, version_key, before, after)) =
                    SETUP.iter().find(|(name, ..)| *name == action)
                {
                    self.rest(None, &at, step, &["uses", "with", "name"], Kind::Left, gone);
                    let asked = with
                        .and_then(|with| get(with, version_key))
                        .and_then(scalar)
                        .or_else(|| {
                            version
                                .clone()
                                .filter(|_| action == "dtolnay/rust-toolchain")
                        });
                    // A version the matrix gives is each instance's own,
                    // and so is the image that holds it: the item is given
                    // the image's name, under a key of its own.
                    let axis = asked.as_deref().and_then(matrix_axis).map(str::to_string);
                    let named = match (&axis, matrix.as_mut()) {
                        (Some(axis), Some(matrix)) if matrix.keys.contains(axis) => {
                            let images: Option<Vec<String>> = matrix
                                .values(axis)
                                .into_iter()
                                .map(|version| {
                                    let tag = image_tag(&action, &scalar(version)?)?;
                                    Some(format!("{before}{tag}{after}"))
                                })
                                .collect();
                            images.map(|images| {
                                let held = matrix.derive(&format!("{}_image", as_parameter(axis)), images);
                                tpl(&format!("item.{held}"))
                            })
                        }
                        _ => asked
                            .as_deref()
                            .and_then(|asked| image_tag(&action, asked))
                            .map(|tag| format!("{before}{tag}{after}")),
                    };
                    match named {
                        Some(_) if contained => self.report.note(&at, Kind::Left, format!("{action}: the job names its own container, which has to hold the toolchain")),
                        Some(_) if given.is_some() => self.report.note(&at, Kind::Left, format!("{action}: the image given for the job's label has to hold the toolchain")),
                        Some(_) if toolchain.is_some() => self.todo(&mut gathered, &at, format!("{action} is a second toolchain for the job, whose image is {}'s: one image holds one. Name an image that holds both, or install this one in a step", toolchain.as_deref().unwrap_or_default())),
                        Some(named) => {
                            let shown = match &axis {
                                Some(axis) => format!("each instance's own, by its {axis}"),
                                None => named.clone(),
                            };
                            self.report.note(&at, Kind::Changed, format!("{action} is the job's image, {shown}: the toolchain is in the image, and no step installs it"));
                            // A label that is no image's is a runner the
                            // toolchain's image does not stand for.
                            if hosted.is_none() {
                                self.todo(&mut gathered, &format!("{at_job}.runs-on"), format!("the job ran on {}, which no image stands for: {action}'s image is a Linux one. Name the job's docker_image, or give the importer one (--image LABEL=IMAGE)", labels.join(", ")));
                            }
                            image = Some(named);
                            toolchain = Some(action.clone());
                        }
                        None => self.todo(&mut gathered, &at, format!("{action} with no version that names an image: name an image for the job that holds the toolchain")),
                    }
                    if let Some(with) = with {
                        // What a Rust toolchain was asked to hold beside
                        // itself is added by a step, where the image has
                        // the toolchain alone.
                        for (asked, add) in [("components", "component"), ("targets", "target")] {
                            let listed = get(with, asked).map(strings).unwrap_or_default();
                            if listed.iter().any(|one| one.contains("${{")) {
                                self.todo(&mut gathered, &format!("{at}.with.{asked}"), format!("which are added is decided as the run goes: add them in a step (rustup {add} add)"));
                                continue;
                            }
                            let named: Vec<String> = listed
                                .iter()
                                .flat_map(|list| list.split([',', ' ']))
                                .filter(|one| {
                                    !one.is_empty()
                                        && one.bytes().all(|b| {
                                            b.is_ascii_alphanumeric() || b"-_.".contains(&b)
                                        })
                                })
                                .map(str::to_string)
                                .collect();
                            if !named.is_empty() {
                                let mut step = Mapping::new();
                                step.insert(
                                    key("run"),
                                    Value::String(format!("rustup {add} add {}", named.join(" "))),
                                );
                                steps.push(Value::Mapping(step));
                                self.report.note(&format!("{at}.with.{asked}"), Kind::Changed, format!("a step adds them to the image's toolchain: rustup {add} add"));
                            }
                        }
                        if let Some(asked) = get(with, "cache").and_then(scalar) {
                            let dependency_path =
                                get(with, "cache-dependency-path").and_then(scalar);
                            match setup_cache(&action, &asked, dependency_path.as_deref()) {
                                Some(cache) if caches.iter().any(|c: &JobCache| c.prefix == cache.prefix) => self.report.note(
                                    &format!("{at}.with.cache"),
                                    Kind::Left,
                                    format!("{asked} is a second cache of the prefix {:?}, which the job keeps already, so it is not carried over", cache.prefix),
                                ),
                                Some(cache) => {
                                    let said = cache.said();
                                    caches.push(cache);
                                    self.report.note(&format!("{at}.with.cache"), Kind::Changed, format!("{asked} is {said}"));
                                }
                                None => self.report.note(
                                    &format!("{at}.with.cache"),
                                    Kind::Left,
                                    format!("{asked} is not a cache the importer knows the directories of, so the job starts cold: name a workspace.cache for it by hand"),
                                ),
                            }
                        }
                        self.rest(
                            None,
                            &format!("{at}.with"),
                            with,
                            &[
                                version_key,
                                "cache",
                                "cache-dependency-path",
                                "components",
                                "targets",
                            ],
                            Kind::Left,
                            "the action's setting has no place in an image's name",
                        );
                    }
                    continue;
                }
                let with: Vec<String> = with
                    .map(|with| with.keys().filter_map(scalar).collect())
                    .unwrap_or_default();
                let text = match with.is_empty() {
                    true => format!("the action {uses} is not imported: write the commands it stood for in its place"),
                    false => format!("the action {uses} (with {}) is not imported: write the commands it stood for in its place", with.join(", ")),
                };
                self.report.note(&at, Kind::Todo, text.clone());
                self.rest(None, &at, step, &["uses", "with", "name"], Kind::Left, gone);
                gathered.stands = true;
                steps.push(failing_step(
                    &plain_name(name.as_deref().unwrap_or(&uses)),
                    &[format!("{at}: {text}")],
                ));
                continue;
            }
            let Some(run) = get(step, "run").and_then(scalar) else {
                self.todo(
                    &mut gathered,
                    &at,
                    "a step with neither `run` nor `uses` is not one",
                );
                continue;
            };
            let mut env: BTreeMap<String, String> = BTreeMap::new();
            if let Some(vars) = get(step, "env").and_then(Value::as_mapping) {
                for (var, value) in vars {
                    let (Some(var), Some(value)) = (scalar(var), scalar(value)) else {
                        continue;
                    };
                    self.variable(
                        &mut gathered,
                        &format!("{at}.env.{var}"),
                        &var,
                        &value,
                        shared,
                        matrix.as_ref(),
                        &mut env,
                        true,
                    );
                }
            }
            let mut script = self.script(&mut gathered, &at, &run, shared, matrix.as_ref(), &mut env);
            let step_directory = get(step, "working-directory")
                .and_then(scalar)
                .or_else(|| directory.clone());
            if let Some(directory) = step_directory {
                match directory.contains("${{") || directory.contains('\'') {
                    true => self.todo(
                        &mut gathered,
                        &at,
                        "its working directory is not a plain path: change into it in the script",
                    ),
                    false => script = format!("cd '{directory}'\n{script}"),
                }
            }
            match get(step, "shell")
                .and_then(scalar)
                .or_else(|| shell.clone())
            {
                Some(shell) if shell != "sh" => {
                    self.report.note(&at, Kind::Changed, format!("the step ran under {shell}, and a step here is `sh -e`: call {shell} in the script where it needs one"));
                }
                Some(_) => {}
                None => self.report.said_once("jobs", Kind::Changed, "a step that named no shell ran under bash on a runner GitHub hosts, and a step here is run by `sh -e`: a script that is bash's calls bash"),
            }
            if let Some(condition) = get(step, "if").and_then(scalar) {
                match self.guard(&condition, matrix.as_ref(), &mut env) {
                    Some(guard) => {
                        script = format!("{guard}{script}");
                        self.report.note(&at, Kind::Changed, "its `if:` is a test at the head of the script, which leaves the step where it is false: the two are compared as text, letter for letter");
                    }
                    None => self.todo(&mut gathered, &at, format!("its condition, `{}`, could not be read as a test: write it in the script", unwrap(&condition))),
                }
            }
            if let Some(id) = get(step, "id").and_then(scalar) {
                if referenced.contains(&id) {
                    self.todo(&mut gathered, &at, format!("a later step reads this one's outputs (steps.{id}): a step hands a later one a value through a file in the workspace"));
                }
            }
            let mut out = Mapping::new();
            if let Some(name) = name {
                out.insert(key("name"), Value::String(plain_name(&name)));
            }
            if !env.is_empty() {
                let vars: Mapping = env
                    .into_iter()
                    .map(|(k, v)| (Value::String(k), Value::String(v)))
                    .collect();
                out.insert(key("env"), Value::Mapping(vars));
            }
            out.insert(key("run"), Value::String(script));
            match get(step, "timeout-minutes").map(seconds) {
                Some(Some(secs)) => {
                    out.insert(key("timeout_secs"), Value::Number(secs.into()));
                }
                Some(None) => self.todo(
                    &mut gathered,
                    &at,
                    "its timeout is not a number of minutes: say it in seconds (timeout_secs)",
                ),
                None => {}
            }
            match get(step, "continue-on-error") {
                Some(Value::Bool(true)) => {
                    out.insert(key("continue_on_error"), Value::Bool(true));
                }
                Some(Value::Bool(false)) | None => {}
                Some(_) => self.todo(&mut gathered, &at, "whether the step may fail is decided as the run goes there: say it here as true or false (continue_on_error)"),
            }
            self.rest(
                Some(&mut gathered),
                &at,
                step,
                &[
                    "name",
                    "run",
                    "env",
                    "if",
                    "id",
                    "shell",
                    "working-directory",
                    "timeout-minutes",
                    "continue-on-error",
                ],
                Kind::Todo,
                "a step has no such key here",
            );
            steps.push(Value::Mapping(out));
        }

        if image.is_none() && !contained {
            image = match (given, hosted) {
                (Some(image), _) => Some(image),
                (None, Some((label, image))) => {
                    self.report.note(&format!("{at}.runs-on"), Kind::Changed, format!("{label} is the image {image}: an operating system, with none of the tools a hosted runner holds"));
                    Some(image.to_string())
                }
                (None, None) => {
                    self.todo(&mut gathered, &format!("{at}.runs-on"), format!("no image is known for {}: name the job's docker_image, or give the importer one (--image LABEL=IMAGE)", labels.join(", ")));
                    None
                }
            };
        }
        if let Some(image) = image {
            task.insert(key("docker_image"), Value::String(image));
        }
        let needs = get(job, "needs").map(strings).unwrap_or_default();
        if !needs.is_empty() {
            task.insert(key("depends_on"), list(needs.iter().map(|n| as_name(n))));
        }
        if let Some(condition) = get(job, "if").and_then(scalar) {
            self.when(&mut gathered, &format!("{at}.if"), &condition, &mut task);
        }
        if let Some(matrix) = &matrix {
            task.insert(key("with_items"), Value::Sequence(matrix.items()));
        }
        let environment = match get(job, "environment") {
            Some(Value::Mapping(environment)) => {
                self.rest(
                    None,
                    &format!("{at}.environment"),
                    environment,
                    &["name"],
                    Kind::Left,
                    "an environment is named here, and has no such setting",
                );
                get(environment, "name").and_then(scalar)
            }
            Some(other) => scalar(other),
            None => None,
        };
        match environment {
            // One that is chosen as the run goes is not one that is named:
            // a job's secrets are not said to be in it.
            Some(name) if name.contains("${{") => self.todo(&mut gathered, &format!("{at}.environment"), "which environment it is, is decided as the run goes there: name it here"),
            Some(name) => {
                task.insert(key("environment"), Value::String(name));
            }
            None => {}
        }

        // The job's variables: the workflow's, then its own, then the
        // secrets its steps read.
        let mut env: Vec<Value> = Vec::new();
        let mut pairs: Vec<(String, String)> = shared.env.clone();
        if let Some(own) = get(job, "env").and_then(Value::as_mapping) {
            for (var, value) in own {
                if let (Some(var), Some(value)) = (scalar(var), scalar(value)) {
                    pairs.retain(|(name, _)| *name != var);
                    pairs.push((var, value));
                }
            }
        }
        for (var, value) in pairs {
            let mut one = BTreeMap::new();
            self.variable(
                &mut gathered,
                &format!("{at}.env.{var}"),
                &var,
                &value,
                shared,
                matrix.as_ref(),
                &mut one,
                false,
            );
            for (name, value) in one {
                let mut pair = Mapping::new();
                pair.insert(key("name"), Value::String(name));
                pair.insert(key("value"), Value::String(value));
                env.push(Value::Mapping(pair));
            }
        }
        for secret in &gathered.secrets {
            let mut from = Mapping::new();
            from.insert(key("secret"), Value::String(secret.to_ascii_lowercase()));
            let mut pair = Mapping::new();
            pair.insert(key("name"), Value::String(secret.clone()));
            pair.insert(key("value_from"), Value::Mapping(from));
            env.push(Value::Mapping(pair));
        }
        if !gathered.secrets.is_empty() {
            let names = gathered
                .secrets
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            let kept = names.to_ascii_lowercase();
            match task.contains_key(key("environment")) {
                true => self.report.note(&at, Kind::Changed, format!("what was read from `secrets` ({names}) is read from the job's environment, as {kept}: keep it there, in dagron-api")),
                false => self.todo(&mut gathered, &at, format!("what was read from `secrets` ({names}) is an environment's here: name the environment that holds it (environment:), as {kept}")),
            }
        }
        // A secret is the job's here, and so every step's of it.
        if !gathered.of_a_step.is_empty() {
            let names = gathered.of_a_step.iter().cloned().collect::<Vec<_>>().join(", ");
            self.report.note(&at, Kind::Changed, format!("{names}: given to the job, and so to every step of it, where the workflow gave it to the steps that read it. A step that must not hold it is a job of its own"));
        }
        if !env.is_empty() {
            task.insert(key("env"), Value::Sequence(env));
        }
        let mut workspace = Mapping::new();
        if let Some(depth) = depth {
            match depth.as_u64() {
                Some(1) => {}
                Some(depth) => {
                    let mut checkout = Mapping::new();
                    checkout.insert(key("depth"), Value::Number(depth.into()));
                    workspace.insert(key("checkout"), Value::Mapping(checkout));
                }
                None => self.todo(&mut gathered, &at, "the checkout's fetch-depth is not a number: say how deep with workspace: { checkout: { depth: N } }"),
            }
        }
        if !caches.is_empty() {
            workspace.insert(
                key("cache"),
                Value::Sequence(caches.iter().map(JobCache::value).collect()),
            );
        }
        if !workspace.is_empty() {
            task.insert(key("workspace"), Value::Mapping(workspace));
        }
        if let Some(services) = get(job, "services").and_then(Value::as_mapping) {
            let services = self.services(&mut gathered, &format!("{at}.services"), services);
            task.insert(key("services"), services);
        }
        match get(job, "timeout-minutes").map(seconds) {
            Some(Some(secs)) => {
                task.insert(key("timeout_secs"), Value::Number(secs.into()));
            }
            Some(None) => self.todo(
                &mut gathered,
                &at,
                "the job's timeout is not a number of minutes: say it in seconds (timeout_secs)",
            ),
            None => self.report.said_once("jobs", Kind::Changed, "a job that set no timeout had GitHub's six hours, and has dagron-ci's own here (an hour, unless CI_DEFAULT_TIMEOUT_SECS says another): say timeout_secs where a job needs longer"),
        }
        match get(job, "continue-on-error") {
            Some(Value::Bool(true)) => {
                task.insert(key("allow_failure"), Value::Bool(true));
            }
            Some(Value::Bool(false)) | None => {}
            Some(_) => self.todo(&mut gathered, &at, "whether the job may fail is decided as the run goes there: say it here as true or false (allow_failure)"),
        }
        if get(job, "outputs").is_some() {
            self.todo(&mut gathered, &format!("{at}.outputs"), "a job hands a later one its status, and no value it made: jobs that share a value are steps of one job, or the value is in a file both can reach");
        }
        if get(job, "name").is_some() {
            self.report.note(
                &format!("{at}.name"),
                Kind::Left,
                format!(
                    "the job is called {}, by its id: a commit's checks are named by it",
                    as_name(id)
                ),
            );
        }
        if get(job, "permissions").is_some() {
            self.report.note(
                &format!("{at}.permissions"),
                Kind::Left,
                "a job is given no token for its forge, so there is nothing to narrow",
            );
        }
        if get(job, "concurrency").is_some() {
            self.report.note(
                &format!("{at}.concurrency"),
                Kind::Left,
                "a concurrency key is a pipeline's, and not one job's",
            );
        }
        self.rest(
            Some(&mut gathered),
            &at,
            job,
            &[
                "name",
                "runs-on",
                "container",
                "needs",
                "if",
                "strategy",
                "environment",
                "env",
                "defaults",
                "steps",
                "services",
                "timeout-minutes",
                "continue-on-error",
                "outputs",
                "permissions",
                "concurrency",
            ],
            Kind::Todo,
            "a job has no such key here",
        );
        if steps.is_empty() && gathered.todo.is_empty() {
            self.todo(
                &mut gathered,
                &at,
                "the job has no step that runs a command",
            );
        }
        task.insert(key("steps"), Value::Sequence(steps));
        Built {
            at,
            task,
            todo: gathered.todo,
            stands: gathered.stands,
            single: matrix.as_ref().and_then(|m| m.single().map(str::to_string)),
        }
    }

    /// One variable of a job or a step, as what a pipeline's may hold. A
    /// secret is the job's to be given, and is not written here.
    #[allow(clippy::too_many_arguments)]
    fn variable(
        &mut self,
        job: &mut Gathered,
        at: &str,
        name: &str,
        value: &str,
        shared: &Shared,
        matrix: Option<&Matrix>,
        out: &mut BTreeMap<String, String>,
        of_a_step: bool,
    ) {
        if name.starts_with("CI_") {
            self.todo(
                job,
                at,
                "a variable that begins CI_ is set from the event here: give it another name",
            );
            return;
        }
        if !is_shell_name(name) {
            self.todo(
                job,
                at,
                "the variable's name is not one a shell's variable may have (letters, digits and `_`, not beginning with a digit): it is left out",
            );
            return;
        }
        let found = expressions(value);
        if let [(0, end, expr)] = found[..] {
            if end == value.len() {
                if let Some(secret) = expr
                    .strip_prefix("secrets.")
                    .filter(|s| is_shell_name(s) && !s.eq_ignore_ascii_case("GITHUB_TOKEN"))
                {
                    job.secrets.insert(secret.to_string());
                    if of_a_step {
                        job.of_a_step.insert(secret.to_string());
                    }
                    if secret != name {
                        self.todo(job, at, format!("the variable {name} held the secret {secret}: a secret is given under its own name here, so the scripts read ${secret}"));
                    }
                    return;
                }
            }
        }
        match self.templated(value, shared, matrix) {
            Ok(value) => {
                out.insert(name.to_string(), value);
            }
            Err(why) => self.todo(job, at, format!("the variable's value: {why}. The variable is left out")),
        }
    }
}

/// The axis a text names, where it is `${{ matrix.axis }}` and nothing else.
fn matrix_axis(text: &str) -> Option<&str> {
    match expressions(text)[..] {
        [(0, end, expr)] if end == text.len() => expr.strip_prefix("matrix.").filter(|axis| is_path(axis)),
        _ => None,
    }
}

/// The tag of the image that holds the version a setup action was asked
/// for: a number as it is, and the few words an action takes for "the
/// current one". `None` for anything else.
fn image_tag(action: &str, asked: &str) -> Option<String> {
    let asked = asked.trim();
    let rust = matches!(action, "dtolnay/rust-toolchain" | "actions-rs/toolchain");
    let word = match (action, asked) {
        (_, "stable") if rust => Some("1"),
        ("actions/setup-go", "stable") => Some("1"),
        ("actions/setup-node", "lts/*") => Some("lts"),
        ("actions/setup-node", "latest" | "current" | "node") => Some("current"),
        ("actions/setup-python", "3.x") => Some("3"),
        _ => None,
    };
    if let Some(word) = word {
        return Some(word.to_string());
    }
    let number = asked.trim_start_matches('v').trim_end_matches(".x");
    let plain = !number.is_empty()
        && number
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()));
    plain.then(|| number.to_string())
}

/// A condition without the braces it may be written in.
fn unwrap(condition: &str) -> &str {
    let condition = condition.trim();
    condition
        .strip_prefix("${{")
        .and_then(|inner| inner.strip_suffix("}}"))
        .map_or(condition, str::trim)
}

/// `left == 'right'` or `left != 'right'`, as its parts and whether it is
/// the first.
fn comparison(text: &str) -> Option<(String, bool, String)> {
    let (left, equal, right) = match text.split_once("==") {
        Some((left, right)) => (left, true, right),
        None => {
            let (left, right) = text.split_once("!=")?;
            (left, false, right)
        }
    };
    let (left, right) = (left.trim(), right.trim());
    let quoted = right.strip_prefix('\'')?.strip_suffix('\'')?;
    (is_path(left) && !quoted.contains('\'')).then(|| (left.to_string(), equal, quoted.to_string()))
}

/// The prefix of `startsWith(github.ref, 'prefix')`.
fn starts_with_ref(text: &str) -> Option<String> {
    let inner = text.strip_prefix("startsWith(")?.strip_suffix(')')?;
    let (what, prefix) = inner.split_once(',')?;
    let prefix = prefix.trim().strip_prefix('\'')?.strip_suffix('\'')?;
    let plain = prefix
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._/-".contains(c));
    (what.trim() == "github.ref" && plain && !prefix.is_empty()).then(|| prefix.to_string())
}

/// The command a container's `--health-cmd` names.
fn health_command(options: &str) -> Option<String> {
    let rest = options
        .split("--health-cmd")
        .nth(1)?
        .trim_start_matches('=')
        .trim_start();
    let command = match rest.chars().next()? {
        quote @ ('"' | '\'') => rest[1..].split(quote).next()?,
        _ => rest.split(" --").next()?,
    };
    let command = command.trim();
    (!command.is_empty()).then(|| command.to_string())
}

/// Read a workflow file of GitHub Actions as a pipeline.
pub fn github_to_pipeline(text: &str, options: &Options) -> Result<Imported> {
    let mut root: Value =
        serde_yaml::from_str(text).context("the file is not YAML a workflow can be read from")?;
    clean(&mut root);
    let Some(root) = root.as_mapping() else {
        bail!("a workflow is a mapping, with `on` and `jobs`");
    };
    let jobs = get(root, "jobs")
        .and_then(Value::as_mapping)
        .filter(|jobs| !jobs.is_empty())
        .context("a workflow has `jobs`, a mapping of at least one job")?;
    let mut importer = Importer {
        options,
        report: Report::default(),
    };
    let mut shared = Shared::default();
    let name = importer.name(root);
    let on = importer.triggers(root, &mut shared);
    if let Some(env) = get(root, "env").and_then(Value::as_mapping) {
        shared.env = env
            .iter()
            .filter_map(|(k, v)| Some((scalar(k)?, scalar(v)?)))
            .collect();
    }
    if let Some(run) = get(root, "defaults")
        .and_then(Value::as_mapping)
        .and_then(|d| get(d, "run"))
        .and_then(Value::as_mapping)
    {
        shared.shell = get(run, "shell").and_then(scalar);
        shared.working_directory = get(run, "working-directory").and_then(scalar);
    }

    let mut out = Mapping::new();
    out.insert(key("name"), Value::String(name.clone()));
    out.insert(key("on"), on);
    if let Some(concurrency) = get(root, "concurrency") {
        if let Some(concurrency) = importer.concurrency("concurrency", concurrency, &name) {
            out.insert(key("concurrency"), concurrency);
        }
    }
    let mut names = BTreeSet::new();
    let mut built = Vec::new();
    for (id, job) in jobs {
        let (Some(id), Some(job)) = (scalar(id), job.as_mapping()) else {
            continue;
        };
        if as_name(&id).is_empty() {
            bail!("the job {id:?} has nothing to be named by: a pipeline's jobs are named by letters, digits, '.', '_' and '-'");
        }
        if !names.insert(as_name(&id)) {
            bail!("two jobs come to the name {:?}: a pipeline's jobs are named by letters, digits, '.', '_' and '-'", as_name(&id));
        }
        built.push(importer.task(&id, job, &shared));
    }

    if get(root, "permissions").is_some() {
        importer.report.note(
            "permissions",
            Kind::Left,
            "a job is given no token for its forge, so there is nothing to narrow",
        );
    }
    if get(root, "run-name").is_some() {
        importer.report.note(
            "run-name",
            Kind::Left,
            "a run is named by its pipeline and its event",
        );
    }
    importer.rest(
        None,
        "",
        root,
        &[
            "name",
            "on",
            "true",
            "env",
            "defaults",
            "concurrency",
            "jobs",
            "permissions",
            "run-name",
        ],
        Kind::Todo,
        "a workflow's key that is not imported",
    );

    // What is the pipeline's own, and no job's, is settled first: a brace of
    // the workflow's that is parted there is every job's to say.
    let mut head = Value::Mapping(out);
    let mut parted = Vec::new();
    settle(&mut head, "", None, &mut parted);
    for place in parted {
        importer.report.todo_all(&place, PARTED);
    }
    // Each job with the step it starts with while something is left to do:
    // what is left of it, and what is left of the pipeline as a whole.
    let tasks: Vec<Value> = built
        .into_iter()
        .map(|job| {
            importer.report.finish(
                &job.at,
                job.task,
                job.todo,
                job.stands,
                job.single.as_deref(),
                Some("continue-on-error"),
            )
        })
        .collect();
    if let Value::Mapping(head) = &mut head {
        head.insert(key("tasks"), Value::Sequence(tasks));
    }

    let pipeline = serde_yaml::to_string(&head).context("writing the pipeline")?;
    Ok(Imported {
        source: "a GitHub Actions workflow",
        pipeline,
        notes: importer.report.into_notes(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn import(text: &str) -> Imported {
        github_to_pipeline(text, &Options::default()).unwrap_or_else(|e| panic!("{e:#}"))
    }

    /// The pipeline, read back as YAML.
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

    const ONE_JOB: &str =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n";

    #[test]
    fn a_workflow_of_one_job_is_a_pipeline_of_one_task() {
        let imported = import(ONE_JOB);
        assert_eq!(
            imported.pipeline,
            "name: ci\non:\n  push: {}\ntasks:\n- name: build\n  docker_image: docker.io/library/ubuntu:24.04\n  steps:\n  - run: make\n"
        );
        assert_eq!(imported.todo(), 0);
        // What reads differently is said: the image is no hosted runner, a
        // step is run by `sh`, and a job has another timeout.
        let changed = notes(&imported, Kind::Changed);
        assert_eq!(changed.len(), 3, "{changed:?}");
        assert!(changed[0].starts_with("jobs.build.runs-on: ubuntu-latest is the image"));
        assert!(changed[1].starts_with("jobs: a step that named no shell ran under bash"));
        assert!(changed[2].starts_with("jobs: a job that set no timeout had GitHub's six hours"));
        // The report is the comment the file begins with, and the file is
        // still the pipeline.
        let printed = imported.with_report();
        assert!(printed.starts_with("# Imported from a GitHub Actions workflow by dagron-import.\n#\n# Changed on the way (3):\n"));
        assert_eq!(
            serde_yaml::from_str::<Value>(&printed).unwrap(),
            pipeline(&imported)
        );
    }

    #[test]
    fn a_name_is_made_one_a_pipeline_may_have() {
        assert_eq!(as_name("Build and test"), "build-and-test");
        assert_eq!(as_name("  CI / CD (main)  "), "ci-cd-main");
        assert_eq!(as_name("release_1.2"), "release_1.2");
        assert_eq!(as_name("***"), "");
        assert_eq!(as_parameter("dry-run"), "dry_run");
        assert_eq!(as_parameter("2fa"), "_2fa");
        assert_eq!(as_variable("python-version"), "PYTHON_VERSION");

        // The workflow's own name where it can be one, then the file's.
        let named = |text: &str, file: Option<&str>| {
            let options = Options {
                name: file.map(str::to_string),
                ..Options::default()
            };
            let imported = github_to_pipeline(text, &options).unwrap();
            pipeline(&imported)["name"].as_str().unwrap().to_string()
        };
        assert_eq!(
            named(&format!("name: Nightly & on demand\n{ONE_JOB}"), Some("x")),
            "nightly-on-demand"
        );
        assert_eq!(named(ONE_JOB, Some("release.yml")), "release.yml");
        assert_eq!(named(&format!("name: '***'\n{ONE_JOB}"), None), "ci");
    }

    #[test]
    fn a_file_that_is_no_workflow_is_refused() {
        let refused = |text: &str| {
            github_to_pipeline(text, &Options::default())
                .unwrap_err()
                .to_string()
        };
        assert!(refused("on: [push").contains("not YAML"));
        assert!(refused("- a\n- b\n").contains("a workflow is a mapping"));
        assert!(refused("on: push\n").contains("has `jobs`"));
        assert!(refused("on: push\njobs: {}\n").contains("has `jobs`"));
        let twice = "on: push\njobs:\n  a b:\n    runs-on: x\n    steps: [{run: a}]\n  a-b:\n    runs-on: x\n    steps: [{run: b}]\n";
        assert!(refused(twice).contains("two jobs come to the name"));
        let unnamed = "on: push\njobs:\n  '***':\n    runs-on: x\n    steps: [{run: a}]\n";
        assert!(refused(unnamed).contains("has nothing to be named by"));
    }

    /// The first step of a job, where it is the one that says what is left.
    fn says(task: &Value) -> Option<String> {
        let first = &task["steps"][0];
        (first["name"] == "imported: to do").then(|| first["run"].as_str().unwrap().to_string())
    }

    /// Whatever is left to do in a job, the job starts by saying it and
    /// failing: what the job's own keys leave, what a step's leave, and
    /// what a service's leave, as much as what a script leaves.
    #[test]
    fn a_job_with_anything_left_to_do_starts_by_failing() {
        let text = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    container:\n      image: alpine:3\n      options: --cpus 1\n    services:\n      db:\n        image: postgres:16\n        volumes: [a]\n    steps:\n      - uses: actions/checkout@v4\n        with: { submodules: true }\n      - run: make\n        with: { x: 1 }\n  clean:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make clean\n";
        let imported = import(text);
        let tasks = pipeline(&imported)["tasks"].clone();
        let said = says(&tasks[0]).expect("the job says what is left, first");
        for left in [
            "jobs.build.container.options:",
            "jobs.build.services.db.volumes:",
            "jobs.build.steps[0].submodules:",
            "jobs.build.steps[1].with:",
        ] {
            assert!(said.contains(left), "{left}: {said}");
        }
        assert!(said.ends_with("DAGRON_IMPORT\nexit 1\n"));
        assert_eq!(imported.todo(), 4, "{:#?}", notes(&imported, Kind::Todo));
        // A job with nothing left is as it was written.
        assert_eq!(says(&tasks[1]), None);
        assert_eq!(tasks[1]["steps"].as_sequence().unwrap().len(), 1);
    }

    /// What is left to do of the workflow as a whole is no one job's, and
    /// every job starts by saying it. A job that may fail is not written as
    /// one while something is left: the step that says so would fail
    /// nothing.
    #[test]
    fn what_is_left_of_the_whole_workflow_is_every_job_s_to_say() {
        let text = "on:\n  pull_request_target: {}\n  push: {}\nwhatever: 1\njobs:\n  a:\n    runs-on: ubuntu-latest\n    continue-on-error: true\n    steps:\n      - run: make\n  b:\n    runs-on: ubuntu-latest\n    continue-on-error: true\n    steps:\n      - uses: actions/upload-artifact@v4\n";
        let imported = import(text);
        let tasks = pipeline(&imported)["tasks"].clone();
        for task in tasks.as_sequence().unwrap() {
            let said = says(task).expect("every job says what is left of the workflow");
            assert!(said.contains("on.pull_request_target: no event here runs"), "{said}");
            assert!(said.contains("whatever: a workflow's key that is not imported"), "{said}");
            assert!(said.contains("the job may fail (continue-on-error) once nothing of it is left"), "{said}");
            assert!(task.get("allow_failure").is_none());
        }

        // With nothing left, a job that may fail is one that may.
        let done = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    continue-on-error: true\n    steps:\n      - run: make\n";
        let imported = import(done);
        assert_eq!(pipeline(&imported)["tasks"][0]["allow_failure"], true);
        assert_eq!(imported.todo(), 0);
        // An action that stands as a step that fails is something left.
        let stands = "on: push\njobs:\n  b:\n    runs-on: ubuntu-latest\n    continue-on-error: true\n    steps:\n      - uses: actions/upload-artifact@v4\n";
        let imported = import(stands);
        assert!(pipeline(&imported)["tasks"][0].get("allow_failure").is_none());
        assert_eq!(imported.todo(), 2, "{:#?}", notes(&imported, Kind::Todo));
    }

    /// GitHub's `+` and `?` say how many of the character before them, and
    /// a pattern here has no such word: it is written to let in what it did
    /// and more, and is left to do.
    #[test]
    fn a_pattern_github_reads_another_way_is_written_wider_and_left_to_do() {
        for (theirs, ours, wider) in [
            ("main", "main", false),
            ("releases/**", "releases/**", false),
            ("!docs/*.md", "!docs/*.md", false),
            ("v[0-9]+.[0-9]+.[0-9]+", "v[0-9]*.[0-9]*.[0-9]*", true),
            ("v1.2+", "v1.2*", true),
            ("colou?r", "colo*r", true),
            ("a+*", "a*", true),
            ("[a-z]?x", "*x", true),
            ("[]+]x", "[]+]x", false),
            ("a\\+b", "a\\+b", false),
            ("?x", "\\?x", false),
            ("+x", "+x", false),
        ] {
            assert_eq!(pattern_here(theirs), (ours.to_string(), wider), "{theirs}");
        }

        let text = "on:\n  push:\n    tags: ['v[0-9]+.[0-9]+.[0-9]+']\n    branches: [main]\njobs:\n  release:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make release\n";
        let imported = import(text);
        let read = pipeline(&imported);
        assert_eq!(read["on"]["push"]["tags"][0], "v[0-9]*.[0-9]*.[0-9]*");
        assert_eq!(read["on"]["push"]["branches"][0], "main");
        let todo = notes(&imported, Kind::Todo);
        assert_eq!(todo.len(), 1, "{todo:?}");
        assert!(todo[0].starts_with("on.push.tags: `v[0-9]+.[0-9]+.[0-9]+` is written `v[0-9]*.[0-9]*.[0-9]*`, which lets in more"));
        assert!(says(&read["tasks"][0]).unwrap().contains("on.push.tags"));
    }

    /// No brace of the workflow's is a template of the pipeline's: one in a
    /// script, a variable, a name or a key is parted, and left to do. The
    /// importer's own templates are whole.
    #[test]
    fn a_brace_of_the_workflow_s_is_no_template_here() {
        let text = "on: push\nconcurrency: \"ci-{{ ci_sha }}-${{ github.ref }}\"\njobs:\n  j:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        os: [a, b]\n    env:\n      FORMAT: \"{{ item }}\"\n    steps:\n      - run: docker inspect --format '{{json .}}' x && echo {{ item }} ${{ matrix.os }}\n";
        let imported = import(text);
        let read = pipeline(&imported);
        let task = &read["tasks"][0];
        let steps = task["steps"].as_sequence().unwrap();
        assert_eq!(
            steps[1]["run"],
            "docker inspect --format '{ {json .}}' x && echo { { item }} ${MATRIX_OS}"
        );
        assert_eq!(steps[1]["env"]["MATRIX_OS"], "{{ item }}", "the importer's own is whole");
        assert_eq!(task["env"][0]["value"], "{ { item }}");
        assert_eq!(read["concurrency"]["key"], "ci-{ { ci_sha }}-{{ ci_group }}");
        let todo = notes(&imported, Kind::Todo);
        assert_eq!(todo.len(), 3, "{todo:?}");
        for place in ["concurrency.key:", "jobs.j: env[0].value:", "jobs.j: steps[0].run:"] {
            assert!(todo.iter().any(|t| t.starts_with(place) && t.contains("holds two opening braces")), "{place}: {todo:?}");
        }
        // Every `{{` that is left is one the importer wrote.
        let left: Vec<&str> = imported.pipeline.match_indices("{{").map(|(at, _)| &imported.pipeline[at..at + 12]).collect();
        assert!(left.iter().all(|at| at.starts_with("{{ item }}") || at.starts_with("{{ ci_group")), "{left:?}");

        // The two characters its own templates are marked with are no text
        // a workflow can bring: one that does brings no template.
        let marked = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"echo \\x01ci_sha\\x02\"\n";
        let imported = import(marked);
        assert_eq!(pipeline(&imported)["tasks"][0]["steps"][0]["run"], "echo ci_sha");

        // Three braces, or five, are parted until no two stand together, and
        // a name whose closing braces were all that kept two apart is too.
        let many = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - name: \"a {}}{ ci_sha }}\"\n        run: echo '{{{ ci_sha }}} {{{{{'\n";
        let imported = import(many);
        assert!(!imported.pipeline.contains("{{"), "{}", imported.pipeline);
        let step = pipeline(&imported)["tasks"][0]["steps"][1].clone();
        assert_eq!(step["run"], "echo '{ { { ci_sha }}} { { { { {'");
        assert_eq!(step["name"], "a { { ci_sha");
    }

    /// The report is a comment, whatever a workflow's key is: one that
    /// holds a new line does not end it.
    #[test]
    fn a_key_of_the_workflow_s_cannot_end_the_report_s_comment() {
        let text = "on: push\n\"x\\nenvironment: prod\\n#\": 1\njobs:\n  \"a\\nb\":\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n";
        let imported = import(text);
        let printed = imported.with_report();
        let report: Vec<&str> = printed.lines().take_while(|line| *line != "name: ci").collect();
        assert!(report.iter().all(|line| line.starts_with('#')), "{printed}");
        assert!(printed.contains("#   x environment: prod #: a workflow's key that is not imported\n"), "{printed}");
        assert_eq!(serde_yaml::from_str::<Value>(&printed).unwrap(), pipeline(&imported));
    }

    /// An expression is a variable of the shell's here, and the shell fills
    /// one in where a shell does. Where it stood in single quotes, or in a
    /// here-document whose mark is quoted, that is left to do.
    #[test]
    fn an_expression_where_a_shell_fills_nothing_in_is_left_to_do() {
        assert!(in_single_quotes("echo '"));
        assert!(in_single_quotes("curl -d '{\"sha\":\""));
        assert!(!in_single_quotes("echo 'a' "));
        assert!(!in_single_quotes("echo \"it's "));
        assert!(!in_single_quotes("echo it\\'s "));
        assert!(!in_single_quotes("# don't\necho "));
        assert!(in_single_quotes("echo a#b '"));
        assert!(has_quoted_heredoc("cat <<'EOF'\nx\nEOF"));
        assert!(has_quoted_heredoc("cat <<- \"EOF\"\nx\nEOF"));
        assert!(has_quoted_heredoc("cat <<\\EOF\nx\nEOF"));
        assert!(!has_quoted_heredoc("cat <<EOF\nx\nEOF"));

        let todo = |run: &str| {
            let text = format!("on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: |\n          {}\n", run.replace('\n', "\n          "));
            let imported = import(&text);
            (notes(&imported, Kind::Todo), notes(&imported, Kind::Changed))
        };
        let (left, changed) = todo("echo \"${{ github.sha }}\"");
        assert!(left.is_empty(), "{left:?}");
        assert!(changed.iter().any(|c| c.contains("is a variable of the shell's here")), "{changed:?}");
        let (left, _) = todo("curl -d '{\"sha\":\"${{ github.sha }}\"}' x");
        assert!(left.len() == 1 && left[0].contains("stood inside single quotes"), "{left:?}");
        let (left, _) = todo("cat <<'EOF'\n${{ github.sha }}\nEOF");
        assert!(left.len() == 1 && left[0].contains("here-document whose mark is quoted"), "{left:?}");
        // A script with no expression says nothing of either.
        let (left, changed) = todo("echo '$HOME' && cat <<'EOF'\nx\nEOF");
        assert!(left.is_empty() && !changed.iter().any(|c| c.contains("variable of the shell's")), "{left:?} {changed:?}");
    }

    /// A secret is the job's here, and so every step's of it: where the
    /// workflow gave it to one step, the report says the others have it.
    #[test]
    fn a_step_s_secret_is_said_to_be_the_whole_job_s() {
        let step = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    environment: npm\n    steps:\n      - run: npm ci\n      - run: npm publish\n        env:\n          NPM_TOKEN: ${{ secrets.NPM_TOKEN }}\n";
        let changed = notes(&import(step), Kind::Changed);
        assert!(changed.iter().any(|c| c.starts_with("jobs.j: NPM_TOKEN: given to the job, and so to every step of it")), "{changed:?}");
        // One the workflow gave the job is the job's there as here.
        let job = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    environment: npm\n    env:\n      NPM_TOKEN: ${{ secrets.NPM_TOKEN }}\n    steps:\n      - run: npm publish\n";
        let changed = notes(&import(job), Kind::Changed);
        assert!(!changed.iter().any(|c| c.contains("to every step of it")), "{changed:?}");
        // GitHub's own token is none of a job's secrets, however it is spelt.
        let token = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: gh pr list\n        env:\n          GH_TOKEN: ${{ secrets.github_token }}\n";
        let imported = import(token);
        assert!(pipeline(&imported)["tasks"][0].get("env").is_none(), "{}", imported.pipeline);
        assert_eq!(imported.todo(), 1);
    }

    /// What a job's label means is not lost to an action that installs a
    /// toolchain: a label no image stands for is left to do, an image given
    /// for the label is the job's, and a second toolchain is no second
    /// image.
    #[test]
    fn a_toolchain_s_image_does_not_hide_what_the_job_ran_on() {
        let job = |runs_on: &str, steps: &str| format!("on: push\njobs:\n  j:\n    runs-on: {runs_on}\n    steps:\n{steps}      - run: make\n");
        let node = "      - uses: actions/setup-node@v4\n        with: { node-version: 22 }\n";
        let python = "      - uses: actions/setup-python@v5\n        with: { python-version: '3.12' }\n";

        let imported = import(&job("macos-latest", node));
        assert_eq!(pipeline(&imported)["tasks"][0]["docker_image"], "docker.io/library/node:22-bookworm-slim");
        let todo = notes(&imported, Kind::Todo);
        assert!(todo.len() == 1 && todo[0].starts_with("jobs.j.runs-on: the job ran on macos-latest, which no image stands for"), "{todo:?}");

        let options = Options {
            images: BTreeMap::from([("builder".to_string(), "registry.example/builder:1".to_string())]),
            ..Options::default()
        };
        let imported = github_to_pipeline(&job("builder", node), &options).unwrap();
        assert_eq!(pipeline(&imported)["tasks"][0]["docker_image"], "registry.example/builder:1");
        assert_eq!(imported.todo(), 0);
        assert!(notes(&imported, Kind::Left).iter().any(|l| l.contains("the image given for the job's label has to hold the toolchain")));

        let imported = import(&job("ubuntu-latest", &format!("{node}{python}")));
        assert_eq!(pipeline(&imported)["tasks"][0]["docker_image"], "docker.io/library/node:22-bookworm-slim");
        let todo = notes(&imported, Kind::Todo);
        assert!(todo.len() == 1 && todo[0].contains("actions/setup-python is a second toolchain for the job, whose image is actions/setup-node's"), "{todo:?}");
    }

    /// A version the matrix gives is each instance's own, and so is its
    /// image: each item is given the name of the image that holds its
    /// version. GitHub's own starter for Node is such a workflow.
    #[test]
    fn a_matrix_s_version_is_each_instance_s_own_image() {
        let text = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        node-version: [18.x, 20.x, 22.x]\n    steps:\n      - uses: actions/checkout@v4\n      - name: Use Node.js ${{ matrix.node-version }}\n        uses: actions/setup-node@v4\n        with:\n          node-version: ${{ matrix.node-version }}\n          cache: npm\n      - run: npm ci\n      - run: echo ${{ matrix.node-version }}\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        assert_eq!(task["docker_image"], "{{ item.node_version_image }}");
        assert_eq!(
            serde_yaml::to_string(&task["with_items"]).unwrap(),
            "- node-version: 18.x\n  node_version_image: docker.io/library/node:18-bookworm-slim\n- node-version: 20.x\n  node_version_image: docker.io/library/node:20-bookworm-slim\n- node-version: 22.x\n  node_version_image: docker.io/library/node:22-bookworm-slim\n"
        );
        // The axis is the item's key now, where it was the item.
        assert_eq!(task["steps"][1]["env"]["MATRIX_NODE_VERSION"], "{{ item.node-version }}");
        assert_eq!(imported.todo(), 0, "{:#?}", notes(&imported, Kind::Todo));

        // A version that names no image leaves the job to do.
        let nightly = text.replace("[18.x, 20.x, 22.x]", "[18.x, nightly]");
        let imported = import(&nightly);
        assert!(pipeline(&imported)["tasks"][0].get("docker_image").is_some(), "the label's image");
        let todo = notes(&imported, Kind::Todo);
        assert!(todo.len() == 1 && todo[0].contains("actions/setup-node with no version that names an image"), "{todo:?}");
    }

    /// A matrix that is made as the run goes, in whole or in part, is not
    /// read as one that is written, and one past what GitHub runs is not
    /// written out.
    #[test]
    fn a_matrix_that_is_not_plain_or_is_too_large_is_left_to_do() {
        let with = |matrix: &str| {
            let text = format!("on: push\njobs:\n  t:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n{matrix}    steps:\n      - run: make\n");
            let imported = import(&text);
            let listed = pipeline(&imported)["tasks"][0].get("with_items").is_some();
            (listed, notes(&imported, Kind::Todo))
        };
        let (listed, todo) = with("        os: [a, b]\n        include: ${{ fromJSON(needs.plan.outputs.extra) }}\n");
        assert!(!listed && todo[0].starts_with("jobs.t.strategy.matrix.include: what the matrix is given here is not a list of plain combinations"), "{todo:?}");
        let (listed, todo) = with("        os: [a, b]\n        exclude:\n          - os: ${{ inputs.skip }}\n");
        assert!(!listed && todo[0].starts_with("jobs.t.strategy.matrix.exclude:"), "{todo:?}");
        let (listed, todo) = with("        os: [a, \"${{ inputs.os }}\"]\n");
        assert!(!listed && todo[0].starts_with("jobs.t.strategy.matrix.os: an axis that is not a list of plain values"), "{todo:?}");
        // Seventeen by sixteen is more than GitHub runs of one matrix.
        let many = |n: usize| (0..n).map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
        let (listed, todo) = with(&format!("        a: [{}]\n        b: [{}]\n", many(17), many(16)));
        assert!(!listed && todo[0].contains("comes to more than 256 combinations"), "{todo:?}");
        let (listed, todo) = with(&format!("        a: [{}]\n        b: [{}]\n", many(16), many(16)));
        assert!(listed && todo.is_empty(), "{todo:?}");
    }

    /// What a step tests of its matrix is text here, and GitHub reads a yes
    /// or a no against text as never the same: that one is left to do.
    #[test]
    fn a_yes_or_no_tested_against_text_is_left_to_do() {
        let text = "on: push\njobs:\n  t:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        os: [a, b]\n        include:\n          - { os: a, coverage: true }\n    steps:\n      - if: matrix.coverage == 'true'\n        run: make coverage\n      - if: matrix.os == 'a'\n        run: make a\n";
        let imported = import(text);
        let steps = pipeline(&imported)["tasks"][0]["steps"].clone();
        assert_eq!(steps[1]["run"], "make coverage");
        assert_eq!(steps[2]["run"], "case \"$MATRIX_OS\" in 'a') ;; *) exit 0 ;; esac\nmake a");
        let todo = notes(&imported, Kind::Todo);
        assert!(todo.len() == 1 && todo[0].contains("`matrix.coverage == 'true'`, could not be read as a test"), "{todo:?}");
    }

    /// What a run decides is not written as if a person had: a toolchain's
    /// targets, a job's environment, a service's health command.
    #[test]
    fn what_the_run_decides_is_not_written_as_decided() {
        let text = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    environment: ${{ inputs.environment }}\n    strategy:\n      matrix:\n        target: [x86_64-unknown-linux-musl]\n    services:\n      db:\n        image: postgres:16\n        options: --health-cmd \"pg_isready -U ${{ env.PGUSER }}\"\n    steps:\n      - uses: dtolnay/rust-toolchain@stable\n        with:\n          targets: ${{ matrix.target }}\n      - run: echo ${{ secrets.TOKEN }}\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        assert!(task.get("environment").is_none());
        assert!(task["services"][0].get("ready").is_none());
        let steps = task["steps"].as_sequence().unwrap();
        assert!(
            !steps.iter().any(|s| s["run"].as_str().is_some_and(|run| run.starts_with("rustup"))),
            "{}",
            imported.pipeline
        );
        let todo = notes(&imported, Kind::Todo);
        for left in [
            "jobs.j.environment: which environment it is, is decided as the run goes",
            "jobs.j.services.db.options: its health command reads an expression",
            "jobs.j.steps[0].with.targets: which are added is decided as the run goes",
            // With no environment named, the secret has none to be in.
            "name the environment that holds it",
        ] {
            assert!(todo.iter().any(|t| t.contains(left)), "{left}: {todo:?}");
        }
        assert_eq!(todo.len(), 4, "{todo:?}");
    }

    /// What dagron-ci takes for a name is what is written: a service's is a
    /// host's, an input's is a parameter's, a variable's is a shell's, and a
    /// step's is one line. A concurrency group with nothing a key can hold
    /// is no key.
    #[test]
    fn what_is_written_is_what_dagron_ci_takes_for_a_name() {
        assert_eq!(as_service_name("my_db.1"), Some("my-db-1".into()));
        assert_eq!(as_service_name("Postgres"), Some("postgres".into()));
        assert_eq!(as_service_name("9lives"), None);
        assert_eq!(as_service_name("localhost"), None);
        assert_eq!(as_service_name("__"), None);
        assert_eq!(as_service_name(&"a".repeat(60)).map(|n| n.len()), Some(40));

        let text = "on:\n  workflow_dispatch:\n    inputs:\n      ci_level: { default: x }\n      dry-run: {}\n      dry_run: {}\n      ok: {}\nconcurrency: ${{ github.workflow_ref }}\njobs:\n  j:\n    runs-on: ubuntu-latest\n    services:\n      my_db:\n        image: postgres:16\n      my-db:\n        image: postgres:15\n      '9':\n        image: redis:7\n    env:\n      MY-VAR: x\n    steps:\n      - name: >\n          Build\n          it\n        run: make\n        env:\n          a.b: y\n";
        let imported = import(text);
        let read = pipeline(&imported);
        assert_eq!(
            serde_yaml::to_string(&read["on"]["dispatch"]["inputs"]).unwrap(),
            "dry_run: {}\nok: {}\n"
        );
        assert!(read.get("concurrency").is_none());
        let task = &read["tasks"][0];
        assert_eq!(serde_yaml::to_string(&task["services"]).unwrap(), "- name: my-db\n  image: postgres:16\n");
        assert!(task.get("env").is_none());
        let step = task["steps"].as_sequence().unwrap().last().unwrap();
        assert_eq!(step["name"], "Build it");
        assert!(step.get("env").is_none());
        let todo = notes(&imported, Kind::Todo);
        for left in [
            "on.workflow_dispatch.inputs.ci_level: an input here is named as a parameter is",
            "on.workflow_dispatch.inputs.dry_run: two inputs come to the name \"dry_run\"",
            "concurrency: `github.workflow_ref` in the group has no place in a key",
            "jobs.j.services.my_db: the service is reached as `my-db` here",
            "jobs.j.services.my-db: the service's name cannot be a host's name here, or is another service's",
            "jobs.j.services.9: the service's name cannot be a host's name here",
            "jobs.j.env.MY-VAR: the variable's name is not one a shell's variable may have",
            "jobs.j.steps[0].env.a.b: the variable's name is not one a shell's variable may have",
        ] {
            assert!(todo.iter().any(|t| t.starts_with(left)), "{left}: {todo:#?}");
        }
        assert_eq!(todo.len(), 8, "{todo:#?}");
    }

    /// What goes with a step that is not one here, and with a call of
    /// another workflow, is said.
    #[test]
    fn what_goes_with_a_step_or_a_call_that_is_not_here_is_said() {
        let text = "on: push\njobs:\n  call:\n    uses: ./.github/workflows/build.yml\n    with: { target: x }\n    secrets: inherit\n    needs: [j]\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n        if: github.event_name == 'push'\n      - uses: actions/setup-node@v4\n        id: node\n        with: { node-version: 22 }\n        continue-on-error: true\n      - uses: actions/cache@v4\n        env: { A: b }\n      - run: make\n";
        let imported = import(text);
        let left = notes(&imported, Kind::Left);
        for said in [
            "jobs.call.with: it is the call's",
            "jobs.call.secrets: it is the call's",
            "jobs.call.needs: it is the call's",
            "jobs.j.steps[0].if: the step is not one here",
            "jobs.j.steps[1].id: the step is not one here",
            "jobs.j.steps[1].continue-on-error: the step is not one here",
            "jobs.j.steps[2].env: the step is not one here",
        ] {
            assert!(left.iter().any(|l| l.starts_with(said)), "{said}: {left:#?}");
        }
        assert!(says(&pipeline(&imported)["tasks"][0]).unwrap().contains("the job calls the workflow"));
    }

    /// A step's script is `sh`'s here. What of it is plainly bash's is left
    /// to do, and a pipeline says once that its steps are run by `sh`.
    #[test]
    fn a_script_that_is_bash_s_is_left_to_do() {
        for (script, seen) in [
            ("if [[ -n \"$A\" ]]; then make; fi", Some("`[[`")),
            ("set -euo pipefail", Some("`pipefail`")),
            ("grep x <<< \"$A\"", Some("`<<<`")),
            ("source ./env.sh", Some("`declare`, `shopt` or `source`")),
            ("function build { make; }", Some("`function`")),
            ("make &> log", Some("`&>`")),
            ("files=(a b)", Some("an array")),
            ("if [ \"$A\" == b ]; then make; fi", Some("`==` in a test")),
            ("if [ \"$A\" = b ]; then make; fi", None),
            ("make test && echo sourced", None),
            // A word of bash's that is no command here is a word.
            ("echo built from source", None),
            ("./configure --with-libs=(none)", None),
            ("  declare -a list", Some("`declare`, `shopt` or `source`")),
        ] {
            assert_eq!(bashism(script), seen, "{script}");
        }
        let text = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: if [[ -f x ]]; then make; fi\n      - run: make\n        shell: sh\n";
        let imported = import(text);
        let todo = notes(&imported, Kind::Todo);
        assert!(todo.len() == 1 && todo[0].starts_with("jobs.j.steps[0]: the script is bash's (`[[`)"), "{todo:?}");
        assert_eq!(notes(&imported, Kind::Changed).iter().filter(|c| c.contains("named no shell")).count(), 1);
    }

    /// An input is a dispatch's. Where the pipeline runs for other events
    /// too, a script that reads one makes it nothing on those, as it is on
    /// GitHub, and a value a script does not read is left to do: dagron
    /// would leave the input's own name there, in braces.
    #[test]
    fn an_input_is_nothing_on_an_event_that_is_no_dispatch() {
        let text = "on:\n  push: {}\n  workflow_dispatch:\n    inputs:\n      level: { default: info }\n      dry-run: {}\njobs:\n  j:\n    runs-on: ubuntu-latest\n    env:\n      LEVEL: ${{ inputs.level }}\n    steps:\n      - run: echo ${{ inputs.level }} ${{ inputs.dry-run }}\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        let step = task["steps"].as_sequence().unwrap().last().unwrap().clone();
        assert_eq!(
            step["run"],
            "[ \"$CI_EVENT\" = dispatch ] || { INPUT_DRY_RUN=; INPUT_LEVEL=; }\necho ${INPUT_LEVEL} ${INPUT_DRY_RUN}"
        );
        assert_eq!(step["env"]["INPUT_LEVEL"], "{{ level }}");
        assert!(task.get("env").is_none());
        let todo = notes(&imported, Kind::Todo);
        assert!(todo.len() == 1 && todo[0].starts_with("jobs.j.env.LEVEL: the variable's value: `inputs.level` is given by a dispatch alone"), "{todo:?}");

        // A pipeline that runs for a dispatch alone has its inputs always.
        let alone = text.replace("  push: {}\n", "");
        let imported = import(&alone);
        let task = pipeline(&imported)["tasks"][0].clone();
        assert_eq!(task["env"][0]["value"], "{{ level }}");
        assert_eq!(task["steps"][0]["run"], "echo ${INPUT_LEVEL} ${INPUT_DRY_RUN}");
        assert_eq!(imported.todo(), 0);
    }

    /// A group whose newer run cancels is a key whose newer run takes the
    /// older one's place. One that does not waits its turn, which is said,
    /// since GitHub keeps only the newest of the runs that wait. One that
    /// decides as the run goes is left to do.
    #[test]
    fn a_group_s_newer_run_cancels_the_older_one_where_the_workflow_says_so() {
        let with = |concurrency: &str| {
            let text = format!("on: push\nconcurrency: {concurrency}\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n");
            let imported = import(&text);
            let written = serde_yaml::to_string(&pipeline(&imported)["concurrency"]).unwrap();
            let waits = notes(&imported, Kind::Changed).iter().any(|c| c.contains("waits its turn"));
            (written, waits, imported.todo())
        };
        assert_eq!(
            with("{ group: \"ci-${{ github.ref }}\", cancel-in-progress: true }"),
            ("key: ci-{{ ci_group }}\ncancel_superseded: true\n".to_string(), false, 0)
        );
        assert_eq!(
            with("{ group: deploy, cancel-in-progress: false }"),
            ("key: deploy\n".to_string(), true, 0)
        );
        assert_eq!(with("deploy"), ("key: deploy\n".to_string(), true, 0));
        let (written, waits, todo) = with("{ group: deploy, cancel-in-progress: \"${{ github.ref != 'refs/heads/main' }}\" }");
        assert_eq!((written.as_str(), waits, todo), ("key: deploy\n", false, 1));
    }

    /// A workflow for a pull request that is closed is not one for a pull
    /// request that is opened, and is not written as one.
    #[test]
    fn a_pull_request_s_other_activities_are_not_its_opening() {
        let with = |types: &str| {
            let text = format!("on:\n  pull_request:\n    types: {types}\n  push: {{}}\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n");
            let imported = import(&text);
            (pipeline(&imported)["on"].get("pull_request").is_some(), imported.todo())
        };
        assert_eq!(with("[closed]"), (false, 1));
        assert_eq!(with("[labeled, unlabeled]"), (false, 1));
        assert_eq!(with("[opened, closed]"), (true, 0));
        assert_eq!(with("[synchronize]"), (true, 0));
    }

    #[test]
    fn an_expression_is_found_whole_and_a_path_is_told_from_the_rest() {
        let found: Vec<&str> =
            expressions("a ${{ github.sha }} b ${{matrix.os}}${{ x || 'y' }} ${{ open")
                .into_iter()
                .map(|(_, _, expr)| expr)
                .collect();
        assert_eq!(found, ["github.sha", "matrix.os", "x || 'y'"]);
        assert!(is_path("github.event.pull_request.number"));
        assert!(is_path("matrix.python-version"));
        for not in ["", "a..b", "a.", "1a", "a == b", "f(x)", "a.b[0]"] {
            assert!(!is_path(not), "{not}");
        }
    }

    #[test]
    fn a_matrix_is_written_out_as_github_writes_it_out() {
        let items = |matrix: &str| {
            let text = format!(
                "on: push\njobs:\n  t:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n{matrix}    steps:\n      - run: echo ${{{{ matrix.a }}}}\n"
            );
            let imported = import(&text);
            let task = pipeline(&imported)["tasks"][0].clone();
            (
                serde_yaml::to_string(&task["with_items"]).unwrap(),
                task["steps"][0].clone(),
            )
        };
        // One axis: its values, and `{{ item }}` is the axis.
        let (listed, step) = items("        a: [1, 2, x]\n");
        assert_eq!(listed, "- 1\n- 2\n- x\n");
        assert_eq!(step["env"]["MATRIX_A"], "{{ item }}");
        assert_eq!(step["run"], "echo ${MATRIX_A}");
        // Two: every combination, the first axis slowest, as objects.
        let (listed, step) = items("        a: [1, 2]\n        b: [x, y]\n");
        assert_eq!(
            listed,
            "- a: 1\n  b: x\n- a: 1\n  b: y\n- a: 2\n  b: x\n- a: 2\n  b: y\n"
        );
        assert_eq!(step["env"]["MATRIX_A"], "{{ item.a }}");
        // An exclusion takes combinations out; an inclusion that fits one
        // adds to it, and one that fits none is a combination of its own.
        // A key some were not given is nothing in the others.
        let (listed, _) = items(
            "        a: [1, 2]\n        b: [x, y]\n        exclude:\n          - { a: 2, b: y }\n        include:\n          - { a: 1, b: x, slow: true }\n          - { a: 9, b: z }\n",
        );
        assert_eq!(
            listed,
            "- a: 1\n  b: x\n  slow: true\n- a: 1\n  b: y\n  slow: ''\n- a: 2\n  b: x\n  slow: ''\n- a: 9\n  b: z\n  slow: ''\n"
        );
        // An inclusion with no axis of the matrix's fits every combination.
        let (listed, _) = items("        a: [1, 2]\n        include:\n          - { extra: e }\n");
        assert_eq!(listed, "- a: 1\n  extra: e\n- a: 2\n  extra: e\n");

        // A matrix made as the run goes is left to do.
        let made = "on: push\njobs:\n  t:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}\n    steps:\n      - run: make\n";
        let imported = import(made);
        assert!(notes(&imported, Kind::Todo)[0].contains("made as the run goes"));
        assert!(pipeline(&imported)["tasks"][0].get("with_items").is_none());
    }

    #[test]
    fn a_condition_is_read_where_it_is_one_of_the_few_kinds() {
        assert_eq!(unwrap(" ${{ a == 'b' }} "), "a == 'b'");
        assert_eq!(unwrap("a == 'b'"), "a == 'b'");
        assert_eq!(
            comparison("github.event_name == 'push'"),
            Some(("github.event_name".into(), true, "push".into()))
        );
        assert_eq!(
            comparison("matrix.os != 'windows-latest'"),
            Some(("matrix.os".into(), false, "windows-latest".into()))
        );
        for not in [
            "a == b",
            "a == 'b' || c",
            "f(a) == 'b'",
            "a = 'b'",
            "a == 'it''s'",
        ] {
            assert_eq!(comparison(not), None, "{not}");
        }
        assert_eq!(
            starts_with_ref("startsWith(github.ref, 'refs/tags/v')"),
            Some("refs/tags/v".into())
        );
        assert_eq!(starts_with_ref("startsWith(github.head_ref, 'x')"), None);
        assert_eq!(starts_with_ref("startsWith(github.ref, 'a*')"), None);

        // A job's: the event's kind, a tag, and the status functions.
        let when = |condition: &str| {
            let text = format!(
                "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    if: {condition}\n    steps:\n      - run: make\n"
            );
            let imported = import(&text);
            let task = pipeline(&imported)["tasks"][0].clone();
            let said = task.get("when").or(task.get("trigger_rule")).cloned();
            (
                said.and_then(|v| v.as_str().map(str::to_string)),
                imported.todo(),
            )
        };
        assert_eq!(
            when("github.event_name == 'push'"),
            (Some("{{ ci_event }} in [push, tag]".into()), 0)
        );
        assert_eq!(
            when("github.event_name != 'push'"),
            (Some("{{ ci_event }} not in [push, tag]".into()), 0)
        );
        assert_eq!(
            when("${{ github.event_name == 'workflow_dispatch' }}"),
            (Some("{{ ci_event }} == dispatch".into()), 0)
        );
        assert_eq!(
            when("startsWith(github.ref, 'refs/tags/')"),
            (Some("{{ ci_event }} == tag".into()), 0)
        );
        assert_eq!(
            when("github.event_name == 'pull_request' && github.event_name != 'merge_group'"),
            (
                Some("{{ ci_event }} == pull_request and {{ ci_event }} != merge_group".into()),
                0
            )
        );
        assert_eq!(when("always()"), (Some("all_done".into()), 0));
        assert_eq!(when("failure()"), (Some("one_failed".into()), 0));
        assert_eq!(when("success()"), (None, 0));
        // What a when: cannot hold is left to do, and the job says so.
        for cannot in [
            "github.ref == 'refs/heads/main'",
            "github.event_name == 'release'",
            "contains(github.event.head_commit.message, 'x')",
            "github.actor != 'dependabot[bot]'",
        ] {
            assert_eq!(when(cannot), (None, 1), "{cannot}");
        }
    }

    #[test]
    fn a_step_s_condition_is_a_test_at_the_head_of_its_script() {
        let script = |condition: &str| {
            let text = format!(
                "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - if: {condition}\n        run: make\n"
            );
            let imported = import(&text);
            let steps = pipeline(&imported)["tasks"][0]["steps"].clone();
            let last = steps.as_sequence().unwrap().last().unwrap().clone();
            (last["run"].as_str().unwrap().to_string(), imported.todo())
        };
        assert_eq!(
            script("github.event_name == 'push'"),
            (
                "case \"$CI_EVENT\" in 'push'|'tag') ;; *) exit 0 ;; esac\nmake".into(),
                0
            )
        );
        assert_eq!(
            script("github.ref != 'refs/heads/main'"),
            (
                "case \"$CI_REF\" in 'refs/heads/main') exit 0 ;; esac\nmake".into(),
                0
            )
        );
        assert_eq!(
            script("startsWith(github.ref, 'refs/tags/')"),
            (
                "case \"$CI_REF\" in 'refs/tags/'*) ;; *) exit 0 ;; esac\nmake".into(),
                0
            )
        );
        // A text of nothing is a text: the test is one a shell reads.
        assert_eq!(
            script("github.event.before != ''"),
            (
                "case \"$CI_BASE_SHA\" in '') exit 0 ;; esac\nmake".into(),
                0
            )
        );
        // A value that would be a pattern of the shell's is not written as
        // one: the step is left to do.
        assert_eq!(script("github.ref == 'refs/heads/*'"), ("make".into(), 1));
        assert_eq!(
            script("steps.build.outcome == 'failure'"),
            ("make".into(), 1)
        );
    }

    #[test]
    fn what_a_script_reads_of_the_run_is_a_variable_here() {
        let text = "on:\n  workflow_dispatch:\n    inputs:\n      dry-run: { default: 'true' }\njobs:\n  j:\n    runs-on: ubuntu-latest\n    env:\n      A: plain\n    steps:\n      - run: echo ${{ github.sha }} ${{ env.A }} ${{ inputs.dry-run }} ${{ secrets.TOKEN }} ${{ runner.os }} ${{ github.actor }}\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        let steps = task["steps"].as_sequence().unwrap();
        // The job starts by saying what is left, and failing.
        assert_eq!(steps[0]["name"], "imported: to do");
        assert!(steps[0]["run"]
            .as_str()
            .unwrap()
            .ends_with("DAGRON_IMPORT\nexit 1\n"));
        assert_eq!(
            steps[1]["run"],
            "echo ${CI_SHA} ${A} ${INPUT_DRY_RUN} ${TOKEN} Linux ${IMPORT_TODO:?not imported, github.actor}"
        );
        assert_eq!(steps[1]["env"]["INPUT_DRY_RUN"], "{{ dry_run }}");
        // The secret is the job's to be given, by its own name.
        let env = serde_yaml::to_string(&task["env"]).unwrap();
        assert_eq!(
            env,
            "- name: A\n  value: plain\n- name: TOKEN\n  value_from:\n    secret: token\n"
        );
        let todo = notes(&imported, Kind::Todo);
        assert_eq!(todo.len(), 2, "{todo:?}");
        assert!(todo.iter().any(|t| t.contains("`github.actor`")));
        assert!(todo
            .iter()
            .any(|t| t.contains("name the environment that holds it")));
        // Nothing of a workflow's braces is left in a script.
        assert!(!imported.pipeline.contains("${{"));
    }

    /// What is written into a script is a shell's to read: a name that is
    /// no variable's, and an expression that is more than a name, are left
    /// to do in words a shell takes for words.
    #[test]
    fn what_a_shell_would_read_otherwise_is_left_to_do() {
        let text = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    timeout-minutes: ${{ matrix.minutes }}\n    continue-on-error: ${{ matrix.experimental }}\n    steps:\n      - name: Test on ${{ matrix.os }} {{ braces }}\n        run: echo ${{ env.MY-VAR }} ${{ secrets.MY-KEY }} ${{ format('{0}; rm -rf /', github.sha) }}\n        timeout-minutes: soon\n        continue-on-error: ${{ true }}\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        let steps = task["steps"].as_sequence().unwrap();
        let step = steps.last().unwrap();
        assert_eq!(step["name"], "Test on matrix.os  braces");
        assert_eq!(
            step["run"],
            "echo ${IMPORT_TODO:?not imported, env.MY-VAR} ${IMPORT_TODO:?not imported, secrets.MY-KEY} ${IMPORT_TODO:?not imported, format 0 rm -rf github.sha}"
        );
        // Neither timeout is a number, and neither flag a plain yes or no:
        // none is written, and each is said.
        for unset in ["timeout_secs", "continue_on_error"] {
            assert!(step.get(unset).is_none(), "{unset}");
        }
        for unset in ["timeout_secs", "allow_failure", "env"] {
            assert!(task.get(unset).is_none(), "{unset}");
        }
        assert_eq!(imported.todo(), 7, "{:#?}", notes(&imported, Kind::Todo));
        assert!(!imported.pipeline.contains("{{"), "{}", imported.pipeline);
        assert_eq!(seconds(&serde_yaml::from_str("2.5").unwrap()), Some(150));
        assert_eq!(seconds(&serde_yaml::from_str("0").unwrap()), None);
        assert!(
            is_shell_name("_A1")
                && !is_shell_name("A-B")
                && !is_shell_name("1A")
                && !is_shell_name("")
        );
    }

    /// A runner's variable is one where a shell reads it, and a word of the
    /// same letters anywhere else is left as it is. One the runner sets and
    /// a step here is not given is left to do, whichever it is.
    #[test]
    fn a_runner_s_variable_is_renamed_where_it_is_read_and_nowhere_else() {
        assert!(reads("echo $GITHUB_SHA", "GITHUB_SHA"));
        assert!(reads("${GITHUB_SHA}", "GITHUB_SHA"));
        assert!(reads("${GITHUB_SHA:-none}", "GITHUB_SHA"));
        assert!(!reads("$MY_GITHUB_SHA", "GITHUB_SHA"));
        assert!(!reads("$GITHUB_SHA256", "GITHUB_SHA"));
        assert!(!reads("--build-arg GITHUB_SHA=x", "GITHUB_SHA"));
        assert_eq!(
            renamed(
                "$GITHUB_SHA $GITHUB_SHA2 ${GITHUB_SHA}x X_GITHUB_SHA GITHUB_SHA=$GITHUB_SHA",
                "GITHUB_SHA",
                "CI_SHA"
            ),
            "$CI_SHA $GITHUB_SHA2 ${CI_SHA}x X_GITHUB_SHA GITHUB_SHA=$CI_SHA"
        );
        assert_eq!(
            variables("a $A ${B} $1 $$ ${C:-$D} '$E'").into_iter().collect::<Vec<_>>(),
            ["A", "B", "C", "D", "E"]
        );

        let text = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: docker build --build-arg GITHUB_SHA=$GITHUB_SHA .\n      - run: echo $GITHUB_JOB $RUNNER_ARCH ${GITHUB_EVENT_PATH}\n      - run: test \"$GITHUB_EVENT_NAME\" = workflow_dispatch\n";
        let imported = import(text);
        let steps = pipeline(&imported)["tasks"][0]["steps"].clone();
        assert_eq!(steps[1]["run"], "docker build --build-arg GITHUB_SHA=$CI_SHA .");
        assert_eq!(steps[3]["run"], "test \"$CI_EVENT\" = workflow_dispatch");
        let todo = notes(&imported, Kind::Todo);
        for unset in ["$GITHUB_JOB", "$RUNNER_ARCH", "$GITHUB_EVENT_PATH"] {
            assert!(todo.iter().any(|t| t.contains(&format!("reads {unset},"))), "{unset}: {todo:?}");
        }
        assert!(todo.iter().any(|t| t.contains("names `workflow_dispatch`")), "{todo:?}");
        assert_eq!(todo.len(), 4, "{todo:?}");
    }

    #[test]
    fn an_action_is_an_image_a_word_in_the_report_or_a_step_that_fails() {
        assert_eq!(image_tag("actions/setup-node", "22"), Some("22".into()));
        assert_eq!(image_tag("actions/setup-node", "20.x"), Some("20".into()));
        assert_eq!(image_tag("actions/setup-node", "lts/*"), Some("lts".into()));
        assert_eq!(
            image_tag("actions/setup-python", "3.12"),
            Some("3.12".into())
        );
        assert_eq!(
            image_tag("dtolnay/rust-toolchain", "stable"),
            Some("1".into())
        );
        assert_eq!(image_tag("actions/setup-go", "v1.23"), Some("1.23".into()));
        for not in ["nightly", ">=3.9", "", "1..2", "latest; rm -rf /", "${{ matrix.go }}"] {
            assert_eq!(image_tag("actions/setup-python", not), None, "{not}");
        }

        let text = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n        with: { fetch-depth: 0 }\n      - uses: actions/setup-python@v5\n        with: { python-version: '3.12', cache: pip }\n      - uses: actions/cache@v4\n      - name: Upload\n        uses: actions/upload-artifact@v4\n        with: { name: dist, path: dist }\n      - run: pytest\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        assert_eq!(
            task["docker_image"],
            "docker.io/library/python:3.12-slim-bookworm"
        );
        assert_eq!(task["workspace"]["checkout"]["depth"], 0);
        let steps = task["steps"].as_sequence().unwrap();
        assert_eq!(
            steps.len(),
            2,
            "the action that is not imported, and the command"
        );
        assert_eq!(steps[0]["name"], "Upload");
        let stands = steps[0]["run"].as_str().unwrap();
        assert!(
            stands.contains(
                "the action actions/upload-artifact@v4 (with name, path) is not imported"
            ),
            "{stands}"
        );
        assert!(stands.ends_with("exit 1\n"));
        assert_eq!(steps[1]["run"], "pytest");
        assert_eq!(imported.todo(), 1);
        let left = notes(&imported, Kind::Left);
        assert!(
            left.iter()
                .any(|l| l.contains("actions/cache") && l.contains("names no path")),
            "{left:?}"
        );
        // setup-python's pip cache is a cache of the job's.
        assert_eq!(task["workspace"]["cache"][0]["prefix"], "pip");
        assert_eq!(task["workspace"]["cache"][0]["paths"][0], "~/.cache/pip");
        let changed = notes(&imported, Kind::Changed);
        assert!(
            changed
                .iter()
                .any(|l| l.starts_with("jobs.j.steps[1].with.cache")),
            "{changed:?}"
        );
    }

    #[test]
    fn a_cache_action_is_a_cache_of_the_job_s() {
        let read = |text: &str| -> Value { serde_yaml::from_str(text).unwrap() };
        let with = read("path: |\n  ~/.cargo/registry\n  target\nkey: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}");
        let (cache, not_read) = cache_action("actions/cache", with.as_mapping()).unwrap();
        assert_eq!(
            cache,
            JobCache::new(
                "cargo",
                &["**/Cargo.lock"],
                &["~/.cargo/registry", "target"]
            )
        );
        assert!(not_read.is_empty());
        let with = read("path: node_modules\nkey: deps-${{ matrix.node }}-${{ hashFiles('a.lock', \"b.lock\") }}");
        let (cache, not_read) = cache_action("actions/cache/restore", with.as_mapping()).unwrap();
        assert_eq!(cache.prefix, "deps");
        assert_eq!(cache.key_files, vec!["a.lock", "b.lock"]);
        assert_eq!(not_read, vec!["matrix.node"]);
        assert_eq!(
            cache_action("Swatinem/rust-cache", None).unwrap().0.prefix,
            "cargo"
        );
        for (with, why) in [
            ("key: k", "names no path"),
            ("path: x", "names no key"),
            ("path: '**/*.o'\nkey: k", "by a pattern"),
            (
                "path: x\nkey: ${{ hashFiles('/etc/x') }}",
                "not a file of the workspace",
            ),
            ("path: ../up\nkey: k", "one to 16 directories"),
        ] {
            let err = cache_action("actions/cache", read(with).as_mapping()).unwrap_err();
            assert!(err.contains(why), "{with}: {err}");
        }
        assert_eq!(
            setup_cache("actions/setup-node", "npm", Some("web/package-lock.json"))
                .unwrap()
                .key_files,
            vec!["web/package-lock.json"]
        );
        assert!(setup_cache("actions/setup-node", "bun", None).is_none());
        // A second cache of one prefix is said to be left, and not carried over.
        let text = "on: push\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/setup-node@v4\n        with: { node-version: '20', cache: npm }\n      - uses: actions/cache@v4\n        with: { path: ~/.npm, key: \"npm-${{ hashFiles('x.lock') }}\" }\n      - run: npm test\n";
        let imported = import(text);
        let task = pipeline(&imported)["tasks"][0].clone();
        assert_eq!(task["workspace"]["cache"].as_sequence().unwrap().len(), 1);
        let left = notes(&imported, Kind::Left);
        assert!(
            left.iter()
                .any(|l| l.contains("a second cache of the prefix \"npm\"")),
            "{left:?}"
        );
    }

    #[test]
    fn a_service_s_health_command_is_what_makes_it_ready() {
        assert_eq!(
            health_command("--health-cmd \"pg_isready -U postgres\" --health-interval 10s"),
            Some("pg_isready -U postgres".into())
        );
        assert_eq!(
            health_command("--health-cmd pg_isready --health-retries 5"),
            Some("pg_isready".into())
        );
        assert_eq!(
            health_command("--health-cmd='redis-cli ping'"),
            Some("redis-cli ping".into())
        );
        assert_eq!(health_command("--cpus 2"), None);
    }

    /// What is said of many places is one line of the report, with every
    /// place in it, and counts as many times as it was said.
    #[test]
    fn what_is_said_of_many_places_is_said_once_with_every_place() {
        let note = |at: &str, kind: Kind, text: &str| Note {
            at: at.into(),
            kind,
            text: text.into(),
        };
        let imported = Imported {
            source: "a GitHub Actions workflow",
            pipeline: "name: p\n".into(),
            notes: vec![
                note("a", Kind::Changed, "x"),
                note("b", Kind::Changed, "y"),
                note("c", Kind::Changed, "x"),
                note("d", Kind::Todo, "x"),
            ],
        };
        assert_eq!(imported.todo(), 1);
        assert_eq!(
            imported.with_report(),
            "# Imported from a GitHub Actions workflow by dagron-import.\n#\n# To do by hand (1):\n#   d: x\n#\n# Changed on the way (3):\n#   a, c: x\n#   b: y\nname: p\n"
        );
    }

    #[test]
    fn a_workflow_with_no_event_a_pipeline_runs_for_can_still_be_asked_for() {
        let text = "on:\n  schedule:\n    - cron: '0 3 * * *'\n  release:\n    types: [published]\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n";
        let imported = import(text);
        assert_eq!(
            serde_yaml::to_string(&pipeline(&imported)["on"]).unwrap(),
            "dispatch: {}\n"
        );
        assert!(notes(&imported, Kind::Todo)[0].starts_with("on: none of the workflow's events"));
        let left = notes(&imported, Kind::Left);
        assert!(left[0].starts_with("on.schedule: a pipeline has no schedule"));
        assert!(left[1].starts_with("on.release: no such event is sent here"));
    }

    #[test]
    fn a_thing_left_to_do_said_twice_of_one_place_is_said_once() {
        // The script reads the same context twice: one place, one thing to
        // do, in the report and in the step the job starts with alike.
        let imported = import(
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    timeout-minutes: 5\n    steps:\n      - run: |\n          echo \"${{ github.event.head_commit.message }}\"\n          echo \"${{ github.event.head_commit.message }}\" > msg.txt\n",
        );
        let todo = notes(&imported, Kind::Todo);
        assert_eq!(todo.len(), 1, "{todo:?}");
        assert_eq!(imported.todo(), 1);
        let first = &pipeline(&imported)["tasks"][0]["steps"][0];
        assert_eq!(first["name"].as_str(), Some("imported: to do"));
        let said = serde_yaml::to_string(first).unwrap();
        assert_eq!(said.matches("jobs.build.steps[0]:").count(), 1, "{said}");
    }
}
