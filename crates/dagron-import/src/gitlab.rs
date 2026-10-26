//! A GitLab CI pipeline, as a pipeline of `.dagron/ci`.
//!
//! [`gitlab_to_pipeline`] reads a project's `.gitlab-ci.yml`, with the
//! files it includes from the project, and writes the pipeline it would be:
//! its jobs as tasks, the order its stages and `needs` give them, the
//! events `workflow:rules` and each job's `rules` run them for, their
//! scripts, images, services, caches and matrices.
//! `docs/CI_MIGRATING.md` has the same map as a table, for a person doing
//! it by hand.
//!
//! The report is the GitHub importer's ([`crate::common`]): what has no
//! place of its own is *changed*, *left* or *to do*, and a job with
//! something to do starts with a step that fails and says what.
//!
//! **What is GitLab's and is done the same way.**
//! - A job waits for every job of the stages before its own, unless it
//!   `needs` others, and then for those alone; a job a run leaves out
//!   takes its place in the order with it. A job that runs where one
//!   before it failed (`on_failure`) is waited for by none that run where
//!   they succeeded, and a manual job that may be left unrun by none.
//! - A job that may fail (`allow_failure`) is waited for, and its failure
//!   fails nothing: its steps go on past one that fails, and it ends as a
//!   success (`continue_on_error`).
//! - A job without `rules` (or `only`) is in a branch's and a tag's
//!   pipeline, and in a run asked for, and not in a merge request's; with
//!   `workflow:rules`, in every pipeline they let be.
//! - Its `before_script` and `script` are one shell, as GitLab runs them,
//!   which stops at the first command that fails.
//! - `extends`, `!reference`, YAML's anchors and `default:` are written out
//!   in each job: a pipeline here is one file, and says each thing where it
//!   is.
//!
//! **What a condition is.** GitLab decides which jobs a pipeline has as it
//! makes one, with the event in hand. Here the decision is written down
//! once for every event ([`rules`]): `workflow:rules` as the pipeline's
//! `on:`, and a job's own `rules` as its `when:`, which may name the
//! event's kind and its branch or tag (`{{ ci_group }}`), and nothing an
//! outsider chose.

mod rules;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_yaml::{Mapping, Value};

use crate::common::*;
use rules::{Event, Holds, Names, Read, EVENTS};

/// What the importer is told beside the file.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// What the pipeline is called: the file's name, say. `ci` unless one
    /// is given.
    pub name: Option<String>,
    /// The image a runner's tag means, for a job that names no image.
    pub images: BTreeMap<String, String>,
    /// The project's default branch, which `$CI_DEFAULT_BRANCH` names.
    /// `main` where none is given, and the report says so where it is read.
    pub default_branch: Option<String>,
    /// The files `include: local` names, by their path in the repository
    /// (without a leading `/`), as read. One that is not here is left to do.
    pub included: BTreeMap<String, String>,
}

/// The default stages, and the two that are always first and last.
const DEFAULT_STAGES: &[&str] = &["build", "test", "deploy"];
const PRE: &str = ".pre";
const POST: &str = ".post";

/// The top-level keys that are no job.
const GLOBAL_KEYS: &[&str] = &[
    "after_script",
    "before_script",
    "cache",
    "default",
    "image",
    "include",
    "services",
    "stages",
    "types",
    "variables",
    "workflow",
];

/// What `default:` gives every job, and the older top-level keys that do.
const DEFAULT_KEYS: &[&str] = &[
    "after_script",
    "artifacts",
    "before_script",
    "cache",
    "hooks",
    "id_tokens",
    "image",
    "interruptible",
    "retry",
    "services",
    "tags",
    "timeout",
];

/// How deep `extends` and `!reference` nest, as GitLab has them, and how
/// deep includes do.
const MAX_EXTENDS: usize = 11;
const MAX_REFERENCE: usize = 10;
const MAX_INCLUDE_DEPTH: usize = 100;

/// The most instances `parallel` makes of one job, as GitLab has it.
const MAX_PARALLEL: usize = 200;

/// GitLab's predefined variables a step here can be given: each is set at
/// the head of the script that reads it, from dagron-ci's own.
const PREDEFINED: &[(&str, &str)] = &[
    ("CI", "export CI=true"),
    ("GITLAB_CI", "export GITLAB_CI=true"),
    ("CI_COMMIT_SHA", "export CI_COMMIT_SHA=\"$CI_SHA\""),
    (
        "CI_COMMIT_SHORT_SHA",
        "export CI_COMMIT_SHORT_SHA=\"$(printf '%.8s' \"$CI_SHA\")\"",
    ),
    // A merge request's pipeline has none, as GitLab's has not.
    (
        "CI_COMMIT_BEFORE_SHA",
        "case \"$CI_EVENT\" in pull_request|merge_group) export CI_COMMIT_BEFORE_SHA=0000000000000000000000000000000000000000 ;; *) export CI_COMMIT_BEFORE_SHA=\"${CI_BASE_SHA:-0000000000000000000000000000000000000000}\" ;; esac",
    ),
    ("CI_COMMIT_REF_NAME", "export CI_COMMIT_REF_NAME=\"${CI_REF#refs/*/}\""),
    (
        "CI_COMMIT_REF_SLUG",
        "export CI_COMMIT_REF_SLUG=\"$(printf '%s' \"${CI_REF#refs/*/}\" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9' '-' | cut -c1-63 | sed 's/^-*//; s/-*$//')\"",
    ),
    (
        "CI_COMMIT_BRANCH",
        "case \"$CI_REF\" in refs/heads/*) export CI_COMMIT_BRANCH=\"${CI_REF#refs/heads/}\" ;; esac",
    ),
    (
        "CI_COMMIT_TAG",
        "case \"$CI_REF\" in refs/tags/*) export CI_COMMIT_TAG=\"${CI_REF#refs/tags/}\" ;; esac",
    ),
    (
        "CI_PIPELINE_SOURCE",
        "case \"$CI_EVENT\" in pull_request|merge_group) export CI_PIPELINE_SOURCE=merge_request_event ;; dispatch) export CI_PIPELINE_SOURCE=api ;; *) export CI_PIPELINE_SOURCE=push ;; esac",
    ),
    (
        "CI_MERGE_REQUEST_IID",
        "[ -z \"$CI_PR\" ] || export CI_MERGE_REQUEST_IID=\"$CI_PR\"",
    ),
    (
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
        "[ -z \"$CI_BASE_REF\" ] || export CI_MERGE_REQUEST_TARGET_BRANCH_NAME=\"${CI_BASE_REF#refs/heads/}\"",
    ),
    (
        "CI_MERGE_REQUEST_DIFF_BASE_SHA",
        "[ -z \"$CI_PR\" ] || export CI_MERGE_REQUEST_DIFF_BASE_SHA=\"$CI_BASE_SHA\"",
    ),
    ("CI_PROJECT_DIR", "export CI_PROJECT_DIR=\"$PWD\""),
    ("CI_PROJECT_PATH", "export CI_PROJECT_PATH=\"$CI_REPO\""),
    ("CI_PROJECT_NAME", "export CI_PROJECT_NAME=\"${CI_REPO##*/}\""),
    ("CI_PROJECT_NAMESPACE", "export CI_PROJECT_NAMESPACE=\"${CI_REPO%/*}\""),
    (
        "CI_PROJECT_URL",
        "export CI_PROJECT_URL=\"https://$CI_FORGE_HOST/$CI_REPO\"",
    ),
    ("CI_SERVER_HOST", "export CI_SERVER_HOST=\"$CI_FORGE_HOST\""),
    ("CI_SERVER_URL", "export CI_SERVER_URL=\"https://$CI_FORGE_HOST\""),
];

/// dagron-ci's own variables: a script that reads one is given it.
const DAGRON_VARS: &[&str] = &[
    "CI_BASE_REF",
    "CI_BASE_SHA",
    "CI_EVENT",
    "CI_FORGE_HOST",
    "CI_GROUP",
    "CI_PR",
    "CI_REF",
    "CI_REPO",
    "CI_SHA",
    "CI_STEP_SUMMARY",
    "CI_TRUST",
];

/// Variables GitLab's runner reads for itself, and no script.
const RUNNER_SETTINGS: &[&str] = &[
    "ARTIFACT_COMPRESSION_LEVEL",
    "ARTIFACT_DOWNLOAD_ATTEMPTS",
    "CACHE_COMPRESSION_LEVEL",
    "CACHE_FALLBACK_KEY",
    "CACHE_REQUEST_TIMEOUT",
    "CI_DEBUG_SERVICES",
    "CI_DEBUG_TRACE",
    "EXECUTOR_JOB_SECTION_ATTEMPTS",
    "GET_SOURCES_ATTEMPTS",
    "GIT_CHECKOUT",
    "GIT_CLEAN_FLAGS",
    "GIT_CLONE_PATH",
    "GIT_FETCH_EXTRA_FLAGS",
    "GIT_LFS_SKIP_SMUDGE",
    "GIT_SUBMODULE_DEPTH",
    "GIT_SUBMODULE_UPDATE_FLAGS",
    "RESTORE_CACHE_ATTEMPTS",
    "TRANSFER_METER_FREQUENCY",
];

/// What a job's Docker daemon is given of its own, and the job is not to
/// set.
const DAEMON_VARS: &[&str] = &[
    "DOCKER_CERT_PATH",
    "DOCKER_DRIVER",
    "DOCKER_HOST",
    "DOCKER_TLS_CERTDIR",
    "DOCKER_TLS_VERIFY",
];

/// Variables a script reads that no one sets in a pipeline file: the
/// shell's own, and an image's.
const ENVIRONMENT: &[&str] = &[
    "CARGO_HOME",
    "GOPATH",
    "GOROOT",
    "HOME",
    "HOSTNAME",
    "IFS",
    "JAVA_HOME",
    "LANG",
    "LC_ALL",
    "LINENO",
    "OLDPWD",
    "OPTARG",
    "OPTIND",
    "PATH",
    "PPID",
    "PWD",
    "RANDOM",
    "REPLY",
    "RUSTUP_HOME",
    "SECONDS",
    "SHELL",
    "TERM",
    "TMPDIR",
    "UID",
    "USER",
];

/// The command that says a service of a known image is ready, by the
/// image's name without its registry, its owner or its tag.
const READY: &[(&str, &str)] = &[
    ("postgres", "pg_isready -h localhost"),
    ("postgis", "pg_isready -h localhost"),
    ("mysql", "mysqladmin ping -h 127.0.0.1 --silent"),
    ("redis", "redis-cli ping"),
];

/// A variable of the file's: its value, and whether GitLab expands the
/// variables it names.
#[derive(Clone, Debug)]
struct Var {
    value: String,
    expand: bool,
    at: String,
}

impl Var {
    /// Whether its value is the text it is: nothing in it is expanded.
    fn plain(&self) -> bool {
        !self.expand || !self.value.contains('$')
    }
}

/// When a job runs, of what came before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum When {
    OnSuccess,
    OnFailure,
    Always,
    Manual,
}

/// A job, read, before it knows the jobs around it.
struct Job {
    /// Its name in the file, and as a task's.
    id: String,
    name: String,
    stage: usize,
    /// The jobs it `needs`, by their names in the file; `None`: by stages.
    needs: Option<Vec<Need>>,
    /// Whose files it takes (`dependencies`); `None`: the jobs it waits for.
    dependencies: Option<Vec<String>>,
    when: When,
    /// It may fail whatever its exit code: its steps go on where one fails,
    /// and it ends as a success (`continue_on_error`).
    allow_failure: bool,
    /// The exit codes alone it may fail with (`allow_failure: exit_codes`).
    allow_codes: Option<Vec<u8>>,
    holds: Holds,
    /// Files it keeps for the jobs after it (`artifacts:paths`).
    keeps_files: bool,
    fields: Vec<(&'static str, Value)>,
    steps: Vec<Value>,
    todo: Vec<String>,
    single: Option<String>,
    /// The variables its matrix gives each instance.
    matrix_keys: BTreeSet<String>,
    /// Never in a pipeline here.
    out: bool,
}

struct Need {
    job: String,
    artifacts: bool,
    optional: bool,
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
    "environment",
    "env",
    "workspace",
    "services",
    "docker_daemon",
    "timeout_secs",
    "max_attempts",
    "allow_failure",
    "report",
    "steps",
];

impl Job {
    fn set(&mut self, key: &'static str, value: Value) {
        self.fields.retain(|(k, _)| *k != key);
        self.fields.push((key, value));
    }

    fn has(&self, key: &str) -> bool {
        self.fields.iter().any(|(k, _)| *k == key)
    }

    fn task(&self) -> Mapping {
        let mut task = Mapping::new();
        task.insert(key("name"), Value::String(self.name.clone()));
        let mut fields: Vec<&(&'static str, Value)> = self.fields.iter().collect();
        fields.sort_by_key(|(k, _)| ORDER.iter().position(|o| o == k).unwrap_or(ORDER.len()));
        for (k, v) in fields {
            task.insert(key(k), v.clone());
        }
        task.insert(key("steps"), Value::Sequence(self.steps.clone()));
        task
    }
}

/// The local files a pipeline file includes, by their path in the
/// repository: what to read and give the importer
/// ([`Options::included`]). Those they include in turn are theirs to say.
pub fn local_includes(text: &str) -> Vec<String> {
    let Ok(value) = load(text) else {
        return Vec::new();
    };
    let Some(map) = value.as_mapping() else {
        return Vec::new();
    };
    includes(map)
        .into_iter()
        .filter_map(|(_, include, _)| match include {
            Include::Local(path) if !path.contains('*') => Some(path),
            _ => None,
        })
        .collect()
}

/// The most files [`read_included`] reads, each path as it is written, and
/// the largest it reads: GitLab's own bounds, 150 files to a pipeline and 2
/// MiB to a file.
const MAX_INCLUDED_FILES: usize = 150;
const MAX_INCLUDED_BYTES: u64 = 2 << 20;

/// The files a pipeline includes from the repository at `root`, and those
/// they include, as read: what [`Options::included`] is given. One that
/// cannot be read is not given, and the importer says so. A file of the
/// repository is one within it once its links are followed, as GitLab reads
/// none outside it: a path up out of it, or a link that leads out, is not
/// read. Nor is one past the first 150, or one over 2 MiB, as GitLab reads
/// neither.
pub fn read_included(root: &Path, text: &str) -> BTreeMap<String, String> {
    use std::io::Read;
    let mut read = BTreeMap::new();
    let Ok(root) = root.canonicalize() else {
        return read;
    };
    let mut wanted = local_includes(text);
    while let Some(file) = wanted.pop() {
        if read.len() >= MAX_INCLUDED_FILES {
            break;
        }
        if read.contains_key(&file) || file.split('/').any(|part| part == "..") {
            continue;
        }
        let Ok(path) = root.join(&file).canonicalize() else {
            continue;
        };
        if !path.starts_with(&root) {
            continue;
        }
        let mut text = String::new();
        let whole = std::fs::File::open(&path).and_then(|opened| {
            opened
                .take(MAX_INCLUDED_BYTES + 1)
                .read_to_string(&mut text)
        });
        if matches!(whole, Ok(n) if n as u64 <= MAX_INCLUDED_BYTES) {
            wanted.extend(local_includes(&text));
            read.insert(file, text);
        }
    }
    read
}

enum Include {
    Local(String),
    /// Anything else: from another project, a URL, GitLab's templates, a
    /// component; or an entry that is no include. What it is, said.
    Other(String),
}

/// The includes a file names, each with where in the file it is.
fn includes(map: &Mapping) -> Vec<(String, Include, Value)> {
    let entries: Vec<Value> = match get(map, "include") {
        None | Some(Value::Null) => return Vec::new(),
        Some(Value::Sequence(items)) => items.clone(),
        Some(other) => vec![other.clone()],
    };
    let local = |path: &str| Include::Local(path.trim_start_matches('/').to_string());
    entries
        .iter()
        .enumerate()
        .map(|(n, entry)| {
            let at = format!("include[{n}]");
            let include = match entry {
                Value::String(path)
                    if path.starts_with("https://") || path.starts_with("http://") =>
                {
                    Include::Other(format!("the file at {path}"))
                }
                Value::String(path) => local(path),
                Value::Mapping(entry) => match (
                    get(entry, "local").and_then(scalar),
                    get(entry, "project").and_then(scalar),
                    get(entry, "remote").and_then(scalar),
                    get(entry, "template").and_then(scalar),
                    get(entry, "component").and_then(scalar),
                ) {
                    (Some(path), ..) => local(&path),
                    (_, Some(project), ..) => {
                        Include::Other(format!("a file of the project {project}"))
                    }
                    (_, _, Some(url), ..) => Include::Other(format!("the file at {url}")),
                    (_, _, _, Some(template), _) => {
                        Include::Other(format!("GitLab's template {template}"))
                    }
                    (_, _, _, _, Some(component)) => {
                        Include::Other(format!("the component {component}"))
                    }
                    _ => Include::Other("an include that names no file".into()),
                },
                _ => Include::Other("an include that names no file".into()),
            };
            (at, include, entry.clone())
        })
        .collect()
}

/// A file of the pipeline as YAML, with its merge keys applied.
fn load(text: &str) -> Result<Value> {
    let mut value: Value =
        serde_yaml::from_str(text).context("the file is not YAML a pipeline can be read from")?;
    clean(&mut value);
    merge_keys(&mut value).map_err(anyhow::Error::msg)?;
    match value {
        Value::Mapping(_) => Ok(value),
        Value::Null => Ok(Value::Mapping(Mapping::new())),
        _ => bail!("a pipeline is a mapping of jobs and keys"),
    }
}

/// YAML's merge keys (`<<: *anchor`), applied at every level.
fn merge_keys(value: &mut Value) -> Result<(), String> {
    match value {
        Value::Mapping(_) => {
            value
                .apply_merge()
                .map_err(|e| format!("a merge key (<<) cannot be applied: {e}"))?;
            if let Value::Mapping(map) = value {
                for (_, v) in map.iter_mut() {
                    merge_keys(v)?;
                }
            }
        }
        Value::Sequence(items) => {
            for item in items {
                merge_keys(item)?;
            }
        }
        Value::Tagged(tagged) => merge_keys(&mut tagged.value)?,
        _ => {}
    }
    Ok(())
}

/// A deep merge, as GitLab merges an included file under the one that
/// includes it and a template under the job that extends it: mappings key
/// by key, anything else replaced.
fn deep_merge(base: &mut Value, over: &Value) {
    match (base, over) {
        (Value::Mapping(base), Value::Mapping(over)) => {
            for (k, v) in over {
                match base.get_mut(k) {
                    Some(existing) => deep_merge(existing, v),
                    None => {
                        base.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (base, over) => *base = over.clone(),
    }
}

/// Whether a value is `!reference`'s tag.
fn is_reference(tagged: &serde_yaml::value::TaggedValue) -> bool {
    tagged.tag == "!reference" || tagged.tag == "reference"
}

/// A shell word that is `text` and nothing else.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

/// The `$NAME` and `${NAME}` a value of GitLab's names, in order.
fn named(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '$' {
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&'$') {
            i += 2;
            continue;
        }
        let braced = chars.get(i + 1) == Some(&'{');
        let start = i + 1 + usize::from(braced);
        let mut end = start;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        if end > start
            && !chars[start].is_ascii_digit()
            && (!braced || chars.get(end) == Some(&'}'))
        {
            out.push(chars[start..end].iter().collect());
        }
        i = end.max(i + 1);
    }
    out
}

/// A value of GitLab's, its variables expanded by `lookup`: `$$` is a `$`.
/// `Err`: the names nothing was found for.
fn expanded(
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
    depth: usize,
) -> Result<String, Vec<String>> {
    if depth > 10 {
        return Err(vec!["(a variable that names itself)".into()]);
    }
    let mut out = String::new();
    let mut missing = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
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
        let braced = chars.get(i + 1) == Some(&'{');
        let start = i + 1 + usize::from(braced);
        let mut end = start;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        let closed = !braced || chars.get(end) == Some(&'}');
        if end == start || !closed || chars[start].is_ascii_digit() {
            out.push('$');
            i += 1;
            continue;
        }
        let name: String = chars[start..end].iter().collect();
        match lookup(&name) {
            Some(value) => match expanded(&value, lookup, depth + 1) {
                Ok(value) => out.push_str(&value),
                Err(mut more) => missing.append(&mut more),
            },
            None => missing.push(name),
        }
        i = end + usize::from(braced);
    }
    if missing.is_empty() {
        Ok(out)
    } else {
        Err(missing)
    }
}

/// A value of GitLab's as a shell word in double quotes, where the shell
/// expands the variables it names, as GitLab did, and nothing else.
fn double_quoted(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::from("\"");
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '$' if chars.get(i + 1) == Some(&'$') => {
                out.push_str("\\$");
                i += 2;
                continue;
            }
            '$' => {
                let next = chars.get(i + 1);
                let names = next.is_some_and(|n| n.is_ascii_alphabetic() || *n == '_' || *n == '{');
                out.push_str(if names { "$" } else { "\\$" });
            }
            '"' | '\\' | '`' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
        i += 1;
    }
    out.push('"');
    out
}

/// A duration as GitLab writes one (`1h 30m`, `90 minutes`, `3600`), in
/// seconds.
fn duration_secs(text: &str) -> Option<u64> {
    let text = text.trim();
    if let Ok(secs) = text.parse::<u64>() {
        return (secs > 0).then_some(secs);
    }
    let mut total: u64 = 0;
    let mut number: Option<u64> = None;
    let mut word = String::new();
    fn of(word: &str) -> Option<u64> {
        Some(match word {
            "s" | "sec" | "second" => 1,
            "m" | "min" | "minute" => 60,
            "h" | "hr" | "hour" => 3600,
            "d" | "day" => 86_400,
            "w" | "wk" | "week" => 7 * 86_400,
            _ => return None,
        })
    }
    // A unit as written (`s` is a second), else one of more than one
    // (`seconds`), in whatever case it is written.
    let unit = |word: &str| -> Option<u64> {
        let word = word.to_ascii_lowercase();
        of(&word).or_else(|| word.strip_suffix('s').and_then(of))
    };
    let mut digits = String::new();
    for c in text.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_digit() {
            if !word.is_empty() {
                total = total.checked_add(number.take()?.checked_mul(unit(&word)?)?)?;
                word.clear();
            }
            digits.push(c);
            continue;
        }
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
            if !word.is_empty() {
                total = total.checked_add(number.take()?.checked_mul(unit(&word)?)?)?;
                word.clear();
            }
        } else {
            return None;
        }
    }
    (number.is_none() && total > 0).then_some(total)
}

/// An image's name without its registry, its owner and its tag.
fn image_base(image: &str) -> &str {
    let without_digest = image.split('@').next().unwrap_or(image);
    let name = match without_digest.rsplit_once(':') {
        Some((name, tag)) if !tag.contains('/') => name,
        _ => without_digest,
    };
    name.rsplit('/').next().unwrap_or(name)
}

/// A name as a glob that matches it and nothing else.
fn literal_glob(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') || (out.is_empty() && c == '!') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Whether `{{ ci_group }}` holds a name as it is: of the characters a
/// ref keeps there, and short enough.
fn group_holds(prefix: &str, name: &str) -> bool {
    !name.is_empty()
        && prefix.len() + name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'))
}

struct Importer<'a> {
    options: &'a Options,
    report: Report,
    /// The document, its includes merged, its templates and references
    /// written out.
    doc: Mapping,
    /// What was found wrong in a job's templates and references, by the
    /// job: said as the job is read.
    problems: BTreeMap<String, Vec<(String, String)>>,
    stages: Vec<String>,
    variables: Vec<(String, Var)>,
    defaults: Mapping,
    workflow: Holds,
    /// Whether the file has `workflow:rules`: a job without rules of its own
    /// is then in every pipeline they let be, and not a branch's and a
    /// tag's alone.
    workflow_rules: bool,
    /// Whether a schedule's pipeline, and another pipeline's, are read as a
    /// run asked for ([`rules::Context::sources_as_dispatch`]).
    sources_as_dispatch: bool,
    /// The variables whose value names others, said where they are.
    said_vars: BTreeSet<String>,
}

impl<'a> Importer<'a> {
    fn default_branch(&self) -> &str {
        self.options.default_branch.as_deref().unwrap_or("main")
    }

    /// The pipelines a job without `rules` or `only` is in: a branch's, a
    /// tag's and a run asked for; and with `workflow:rules`, every one they
    /// let be, a merge request's too, as GitLab has it.
    fn default_holds(&self) -> Holds {
        match self.workflow_rules {
            true => Holds::all(),
            false => Holds::events(&[Event::Push, Event::Tag, Event::Dispatch]),
        }
    }

    /// What was read of a condition, said: what could not be read, and
    /// what was taken for granted.
    fn read_said(&mut self, at: &str, read: &Read) {
        if read.default_branch && self.options.default_branch.is_none() {
            self.report.said_once(
                at,
                Kind::Changed,
                "$CI_DEFAULT_BRANCH is taken to be main: give the importer the project's own (--default-branch) where it is another",
            );
        }
        if read.dispatch {
            self.report.said_once(
                at,
                Kind::Changed,
                "a run asked for here (dispatch, through the run API) is GitLab's web, api and trigger pipelines alike",
            );
        }
        for source in &read.sources {
            self.report.said_once(
                at,
                Kind::Left,
                format!("no pipeline here is a {source} pipeline: a condition that names one never holds here (a schedule asks for a run through the run API, as a dispatch)"),
            );
        }
        for source in &read.as_dispatch {
            self.report.said_once(
                at,
                Kind::Changed,
                format!("the pipeline runs for nothing here but a {source} pipeline, which is a run asked for here (dispatch): a schedule asks for one through the run API. What its rules say of a {source} pipeline is said of every run asked for"),
            );
        }
        if read.open_merge_requests {
            self.report.said_once(
                at,
                Kind::Changed,
                "$CI_OPEN_MERGE_REQUESTS is taken as unset: a branch's pipeline runs whether or not the branch has a merge request open, where GitLab left it out for one that had",
            );
        }
        for name in &read.file_vars {
            self.report.said_once(
                at,
                Kind::Changed,
                format!("${name} is read as the file sets it, for a branch's, a tag's and a merge request's pipeline: where the project's CI/CD variables set it otherwise, the job is in pipelines here it was not in, or not in those it was. Write its when: for what it should run for"),
            );
        }
    }

    /// Includes, merged as GitLab merges them: each under the file that
    /// includes it, the pipeline's own file over all.
    fn merged(&mut self, main: Value) -> Value {
        let mut doc = Value::Mapping(Mapping::new());
        let mut seen = BTreeSet::new();
        self.include("", &main, &mut seen, 0, &mut doc);
        let mut own = main;
        if let Value::Mapping(map) = &mut own {
            map.shift_remove("include");
        }
        deep_merge(&mut doc, &own);
        doc
    }

    fn include(
        &mut self,
        file: &str,
        value: &Value,
        seen: &mut BTreeSet<String>,
        depth: usize,
        doc: &mut Value,
    ) {
        let Some(map) = value.as_mapping() else {
            return;
        };
        let place = |at: &str| match file {
            "" => at.to_string(),
            file => format!("{file}: {at}"),
        };
        for (at, include, entry) in includes(map) {
            let at = place(&at);
            match include {
                Include::Other(what) => self.report.todo_all(
                    &at,
                    format!("{what} is not imported: what it holds is not in this file, and no job of it is here. Write what the pipeline needs of it here"),
                ),
                Include::Local(path) if path.contains('*') => self.report.todo_all(
                    &at,
                    format!("{path} names files by a pattern, which are not read: write each here, or name each to include"),
                ),
                Include::Local(path) => {
                    if !seen.insert(path.clone()) {
                        continue;
                    }
                    if depth >= MAX_INCLUDE_DEPTH {
                        self.report.todo_all(&at, "includes nest too deep to be read");
                        continue;
                    }
                    let Some(text) = self.options.included.get(&path) else {
                        self.report.todo_all(
                            &at,
                            format!("{path} was not read from the repository: import from its root (dagron-import's --root), or write its jobs here"),
                        );
                        continue;
                    };
                    match load(text) {
                        Ok(value) => {
                            self.include(&path, &value, seen, depth + 1, doc);
                            let mut own = value;
                            if let Value::Mapping(map) = &mut own {
                                map.shift_remove("include");
                            }
                            deep_merge(doc, &own);
                            self.report.note(&at, Kind::Changed, format!("what {path} holds is written out here"));
                        }
                        Err(e) => self.report.todo_all(&at, format!("{path}: {e:#}")),
                    }
                }
            }
            if let Some(entry) = entry.as_mapping() {
                for (k, why) in [
                    (
                        "rules",
                        "an include's rules are not read: it is included whatever they say",
                    ),
                    (
                        "inputs",
                        "an include's inputs are not given: the file is read as it is written",
                    ),
                ] {
                    if get(entry, k).is_some() {
                        self.report.todo_all(&format!("{at}.{k}"), why);
                    }
                }
            }
        }
    }

    fn problem(&mut self, job: &str, at: &str, text: impl Into<String>) {
        self.problems
            .entry(job.to_string())
            .or_default()
            .push((at.to_string(), text.into()));
    }

    /// Each job and template with its `extends` written out under it.
    fn extended(&mut self, doc: &Mapping) -> Mapping {
        let mut made: BTreeMap<String, Value> = BTreeMap::new();
        let mut out = Mapping::new();
        for (k, v) in doc {
            let Some(name) = scalar(k) else { continue };
            let value = if GLOBAL_KEYS.contains(&name.as_str()) {
                v.clone()
            } else {
                self.extend(doc, &name, &name, v, 0, &mut made)
            };
            out.insert(k.clone(), value);
        }
        out
    }

    fn extend(
        &mut self,
        doc: &Mapping,
        job: &str,
        name: &str,
        value: &Value,
        depth: usize,
        made: &mut BTreeMap<String, Value>,
    ) -> Value {
        if let Some(done) = made.get(name) {
            return done.clone();
        }
        let parents: Vec<String> = match value.as_mapping().and_then(|m| get(m, "extends")) {
            None => return value.clone(),
            Some(parents) => strings(parents),
        };
        if depth >= MAX_EXTENDS {
            self.problem(job, &format!("{name}.extends"), format!("extends nests more than {MAX_EXTENDS} deep, or in a circle: it is not written out"));
            return value.clone();
        }
        let mut out = Value::Mapping(Mapping::new());
        let mut named = BTreeSet::new();
        for parent in parents {
            if !named.insert(parent.clone()) {
                continue;
            }
            match get(doc, &parent) {
                Some(found) if found.is_mapping() => {
                    let base = self.extend(doc, job, &parent, found, depth + 1, made);
                    deep_merge(&mut out, &base);
                }
                _ => self.problem(
                    job,
                    &format!("{name}.extends"),
                    format!(
                        "{parent:?} is no job or template of the pipeline: nothing of it is here"
                    ),
                ),
            }
        }
        deep_merge(&mut out, value);
        if let Value::Mapping(m) = &mut out {
            m.shift_remove("extends");
        }
        // One whose templates were wrong is made again for each job that
        // extends it, so that each says so.
        if !self.problems.contains_key(job) {
            made.insert(name.to_string(), out.clone());
        }
        out
    }

    /// Every `!reference [job, key, …]` in `value` as what it names.
    fn referenced(&mut self, doc: &Mapping, job: &str, at: &str, value: &mut Value, depth: usize) {
        match value {
            Value::Tagged(tagged) if is_reference(tagged) => {
                let names: Vec<String> = tagged
                    .value
                    .as_sequence()
                    .map(|s| s.iter().filter_map(scalar).collect())
                    .unwrap_or_default();
                if depth >= MAX_REFERENCE || names.is_empty() {
                    self.problem(
                        job,
                        at,
                        "a !reference that names nothing, or nests too deep: it is left out",
                    );
                    *value = Value::Null;
                    return;
                }
                let mut found: Option<&Value> = None;
                let mut map = Some(doc);
                for name in &names {
                    found = map.and_then(|m| get(m, name));
                    map = found.and_then(Value::as_mapping);
                }
                match found {
                    Some(found) => {
                        let mut resolved = found.clone();
                        self.referenced(doc, job, at, &mut resolved, depth + 1);
                        *value = resolved;
                    }
                    None => {
                        self.problem(
                            job,
                            at,
                            format!(
                                "!reference [{}] names nothing in the pipeline: it is left out",
                                names.join(", ")
                            ),
                        );
                        *value = Value::Null;
                    }
                }
            }
            Value::Tagged(tagged) => {
                self.problem(
                    job,
                    at,
                    format!(
                        "{} is not a tag GitLab has: what it holds is left out",
                        tagged.tag
                    ),
                );
                *value = Value::Null;
            }
            Value::Mapping(map) => {
                for (k, v) in map.iter_mut() {
                    let at = match scalar(k) {
                        Some(k) => format!("{at}.{k}"),
                        None => at.to_string(),
                    };
                    self.referenced(doc, job, &at, v, depth);
                }
            }
            Value::Sequence(items) => {
                for (n, item) in items.iter_mut().enumerate() {
                    self.referenced(doc, job, &format!("{at}[{n}]"), item, depth);
                }
            }
            _ => {}
        }
    }

    /// A `variables:` block, as names and variables.
    fn variables_of(
        &mut self,
        at: &str,
        value: Option<&Value>,
        todo: Option<&mut Vec<String>>,
    ) -> Vec<(String, Var)> {
        let mut out = Vec::new();
        let Some(Value::Mapping(map)) = value else {
            return out;
        };
        let mut todo = todo;
        for (name, held) in map {
            let Some(name) = scalar(name) else { continue };
            let at = format!("{at}.{name}");
            let (value, expand) = match held {
                Value::Mapping(full) => {
                    for k in ["description", "options"] {
                        if get(full, k).is_some() {
                            self.report.note(
                                &format!("{at}.{k}"),
                                Kind::Left,
                                "a run's form is not here: the variable has its value on every run",
                            );
                        }
                    }
                    (
                        get(full, "value").and_then(scalar).unwrap_or_default(),
                        get(full, "expand") != Some(&Value::Bool(false)),
                    )
                }
                other => match scalar(other) {
                    Some(value) => (value, true),
                    None => {
                        let why = "a variable's value is text: it is left out";
                        match todo.as_deref_mut() {
                            Some(todo) => self.report.todo(todo, &at, why),
                            None => self.report.todo_all(&at, why),
                        }
                        continue;
                    }
                },
            };
            out.push((name, Var { value, expand, at }));
        }
        out
    }

    /// The file's own keys: stages, variables, defaults, workflow.
    fn globals(&mut self) {
        let doc = self.doc.clone();
        let stages = get(&doc, "stages").or_else(|| get(&doc, "types"));
        if get(&doc, "types").is_some() {
            self.report.note(
                "types",
                Kind::Changed,
                "types is GitLab's older word for stages, and is read as one",
            );
        }
        let listed: Vec<String> = match stages {
            Some(stages) => strings(stages),
            None => DEFAULT_STAGES.iter().map(|s| s.to_string()).collect(),
        };
        let mut all = vec![PRE.to_string()];
        all.extend(listed.into_iter().filter(|s| s != PRE && s != POST));
        all.push(POST.to_string());
        self.stages = all;

        self.variables = self.variables_of("variables", get(&doc, "variables"), None);

        let mut defaults = Mapping::new();
        for k in [
            "image",
            "services",
            "before_script",
            "after_script",
            "cache",
        ] {
            if let Some(v) = get(&doc, k) {
                defaults.insert(key(k), v.clone());
                self.report.note(
                    k,
                    Kind::Changed,
                    format!("the pipeline's {k} is each job's that does not set its own"),
                );
            }
        }
        if let Some(own) = get(&doc, "default").and_then(Value::as_mapping) {
            for (k, v) in own {
                match scalar(k) {
                    Some(name) if DEFAULT_KEYS.contains(&name.as_str()) => {
                        defaults.insert(k.clone(), v.clone());
                    }
                    Some(name) => self.report.todo_all(
                        &format!("default.{name}"),
                        "default: gives no such key to a job",
                    ),
                    None => {}
                }
            }
        }
        self.defaults = defaults;
        self.workflow();
    }

    /// `workflow:`: which pipelines there are.
    fn workflow(&mut self) {
        self.workflow =
            Holds::events(&[Event::Push, Event::Tag, Event::PullRequest, Event::Dispatch]);
        let Some(workflow) = get(&self.doc, "workflow")
            .and_then(Value::as_mapping)
            .cloned()
        else {
            return;
        };
        if get(&workflow, "name").is_some() {
            self.report.note(
                "workflow.name",
                Kind::Left,
                "a run is named by its pipeline and its event",
            );
        }
        if get(&workflow, "auto_cancel").is_some() {
            self.report.note("workflow.auto_cancel", Kind::Left, "a newer run takes an older one's place where the pipeline says so (concurrency: cancel_superseded), and jobs' interruptible says it here");
        }
        for k in workflow.keys().filter_map(scalar) {
            if !matches!(k.as_str(), "name" | "auto_cancel" | "rules") {
                self.report
                    .todo_all(&format!("workflow.{k}"), "workflow: has no such key here");
            }
        }
        if let Some(listed) = get(&workflow, "rules") {
            self.workflow_rules = true;
            let mut todo = Vec::new();
            let (holds, _) = self.rules(
                "workflow.rules",
                listed,
                None,
                When::OnSuccess,
                &mut todo,
                true,
            );
            for line in todo {
                self.report.everywhere.push(line);
            }
            self.workflow = holds;
        }
    }

    /// A list of rules: what it holds for, and when the job runs where
    /// it does. Each rule's `if`, `changes` and `exists` together; the
    /// first that holds decides, and `when: never` leaves the job out.
    #[allow(clippy::too_many_arguments)]
    fn rules(
        &mut self,
        at: &str,
        listed: &Value,
        job: Option<(&str, &BTreeMap<String, String>)>,
        default_when: When,
        todo: &mut Vec<String>,
        workflow: bool,
    ) -> (Holds, Option<When>) {
        let Some(listed) = listed.as_sequence() else {
            self.report.todo(
                todo,
                at,
                "rules is a list of rules: it is not read, and the job runs for every event",
            );
            return (Holds::all(), None);
        };
        // Rules a `!reference` brings in are a list among the rules: GitLab
        // reads them as one list.
        fn flatten(items: &[Value], out: &mut Vec<Value>, depth: usize) {
            for item in items {
                match item {
                    Value::Sequence(inner) if depth < MAX_REFERENCE => {
                        flatten(inner, out, depth + 1)
                    }
                    other => out.push(other.clone()),
                }
            }
        }
        let mut flat = Vec::new();
        flatten(listed, &mut flat, 0);
        let listed = &flat;
        let vars = job
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| plain_of(&self.variables));
        let lookup = move |name: &str| vars.get(name).cloned();
        let default_branch = self.default_branch().to_string();
        let cx = rules::Context {
            vars: &lookup,
            default_branch: &default_branch,
            job: job.map(|(name, _)| name),
            sources_as_dispatch: self.sources_as_dispatch,
        };
        let mut read = Read::default();
        let mut remaining = Holds::all();
        let mut result = Holds::none();
        let mut whens: Vec<When> = Vec::new();
        for (n, rule) in listed.iter().enumerate() {
            let at = format!("{at}[{n}]");
            let Some(rule) = rule.as_mapping() else {
                self.report
                    .todo(todo, &at, "a rule is a mapping: it is not read");
                continue;
            };
            let mut holds = Holds::all();
            if let Some(condition) = get(rule, "if").and_then(scalar) {
                match rules::parse(&condition) {
                    Ok(expr) => holds = holds.and(&rules::holds(&expr, &cx, &mut read)),
                    Err(why) => {
                        self.report.todo(todo, &format!("{at}.if"), format!("`{condition}` could not be read ({why}): it is taken to hold for every event"));
                        holds = rules::unread(&format!("`{condition}`"), &mut read);
                    }
                }
            }
            if get(rule, "changes").is_some() {
                holds = holds.and(&rules::changes(&mut read));
            }
            if get(rule, "exists").is_some() {
                holds = holds.and(&rules::unread("exists:", &mut read));
            }
            let when = match get(rule, "when").and_then(scalar).as_deref() {
                None => Some(default_when),
                Some("never") => None,
                Some(word) => match self.when_word(word) {
                    Ok(when) => Some(when),
                    Err(why) => {
                        self.report.todo(todo, &format!("{at}.when"), why);
                        Some(default_when)
                    }
                },
            };
            let fire = remaining.and(&holds);
            if let Some(when) = when {
                if fire.may_hold() && !whens.contains(&when) {
                    whens.push(when);
                }
                result = result.or(&fire);
            }
            remaining = remaining.and(&holds.not());
            let mut known = vec!["if", "changes", "exists", "when"];
            if workflow {
                for k in ["variables", "auto_cancel"] {
                    if get(rule, k).is_some() {
                        self.report.todo_all(&format!("{at}.{k}"), "a rule's own settings are not imported: the pipeline is the same whichever rule ran it. Write them as the jobs' own");
                    }
                }
                known.extend(["variables", "auto_cancel"]);
            }
            for k in rule.keys().filter_map(scalar) {
                if known.contains(&k.as_str()) {
                    continue;
                }
                self.report.todo(todo, &format!("{at}.{k}"), "a rule's own settings are not imported: the job is the same whichever rule put it in the pipeline. Write them as the job's own, where they hold for every event");
            }
        }
        self.read_said(at, &read);
        let inexact = EVENTS.iter().any(|e| !result.of(*e).is_exact());
        if inexact {
            let what: Vec<String> = read.unread.iter().cloned().collect();
            let what = if what.is_empty() {
                "something".to_string()
            } else {
                what.join(", ")
            };
            let text = match workflow {
                true => format!("{what} cannot be read before a run: the pipeline is written to run wherever it might have, which is more than it did. Write on: for what it should run for, and test the rest in a step"),
                false => format!("{what} cannot be read before a run: the job is written to run wherever it might have, which is more than it did. Test it in the job's script, or write the job's when: for what it should run for"),
            };
            self.report.todo(todo, at, text);
        }
        let when = match whens.as_slice() {
            [] => None,
            [one] => Some(*one),
            [first, ..] => {
                self.report.todo(todo, at, format!("its rules run it {} for some events and otherwise for others: it is written as {} for every one", when_said(*first), when_said(*first)));
                Some(*first)
            }
        };
        (result, when)
    }

    fn when_word(&self, word: &str) -> Result<When, String> {
        match word {
            "on_success" => Ok(When::OnSuccess),
            "on_failure" => Ok(When::OnFailure),
            "always" => Ok(When::Always),
            "manual" => Ok(When::Manual),
            "delayed" => Err("a delayed job is not taken: it runs when it would have started, and start_in is not waited for".into()),
            other => Err(format!("{other:?} is not a when GitLab has: the job runs when the jobs before it succeed")),
        }
    }

    /// `only:` and `except:`, GitLab's older form of `rules`.
    fn only_except(
        &mut self,
        at: &str,
        map: &Mapping,
        vars: &BTreeMap<String, String>,
        todo: &mut Vec<String>,
    ) -> Holds {
        let mut read = Read::default();
        let mut part = |this: &mut Self, name: &str, value: &Value, read: &mut Read| -> Holds {
            let at = format!("{at}.{name}");
            let (refs, extra): (Option<Vec<String>>, Option<&Mapping>) = match value {
                Value::Mapping(full) => (get(full, "refs").map(strings), Some(full)),
                other => (Some(strings(other)), None),
            };
            let sources_as_dispatch = this.sources_as_dispatch;
            let mut holds = match refs {
                Some(refs) => refs.iter().fold(Holds::none(), |all, r| {
                    all.or(&rules::reference(r, sources_as_dispatch, read))
                }),
                None if name == "only" => {
                    Holds::events(&[Event::Push, Event::Tag, Event::Dispatch])
                }
                None => Holds::all(),
            };
            if let Some(full) = extra {
                if let Some(exprs) = get(full, "variables") {
                    let lookup = |n: &str| vars.get(n).cloned();
                    let default_branch = this.default_branch().to_string();
                    let cx = rules::Context {
                        vars: &lookup,
                        default_branch: &default_branch,
                        job: None,
                        sources_as_dispatch,
                    };
                    let mut any = Holds::none();
                    for text in strings(exprs) {
                        match rules::parse(&text) {
                            Ok(expr) => any = any.or(&rules::holds(&expr, &cx, read)),
                            Err(why) => {
                                this.report.todo(
                                    todo,
                                    &format!("{at}.variables"),
                                    format!("`{text}` could not be read ({why})"),
                                );
                                any = any.or(&rules::unread(&text, read));
                            }
                        }
                    }
                    holds = holds.and(&any);
                }
                if get(full, "changes").is_some() {
                    holds = holds.and(&rules::changes(read));
                }
                if get(full, "kubernetes").is_some() {
                    holds = holds.and(&rules::unread("kubernetes: active", read));
                }
                for k in full.keys().filter_map(scalar) {
                    if !matches!(k.as_str(), "refs" | "variables" | "changes" | "kubernetes") {
                        this.report.todo(
                            todo,
                            &format!("{at}.{k}"),
                            format!("{name} has no such key"),
                        );
                    }
                }
            }
            holds
        };
        let only = match get(map, "only") {
            Some(value) => part(self, "only", value, &mut read),
            None => self.default_holds(),
        };
        let except = match get(map, "except") {
            Some(value) => part(self, "except", value, &mut read),
            None => Holds::none(),
        };
        let holds = only.and(&except.not());
        self.read_said(at, &read);
        if EVENTS.iter().any(|e| !holds.of(*e).is_exact()) {
            let what: Vec<String> = read.unread.iter().cloned().collect();
            self.report.todo(todo, at, format!("{} cannot be read before a run: the job is written to run wherever it might have, which is more than it did", what.join(", ")));
        }
        holds
    }

    /// A job's image, from its own, the defaults', or its runner's tag.
    fn image(
        &mut self,
        job: &mut Job,
        at: &str,
        value: Option<&Value>,
        vars: &[(String, Var)],
        matrix_keys: &BTreeSet<String>,
        tags: &[String],
    ) -> Option<String> {
        let (name, full) = match value {
            None | Some(Value::Null) => {
                let found = tags
                    .iter()
                    .find_map(|t| self.options.images.get(t).cloned());
                if found.is_none() {
                    let why = match tags.is_empty() {
                        true => "the job names no image, and had its runner's own: name its docker_image".to_string(),
                        false => format!("the job names no image, and had the one its runner ({}) gave it: name its docker_image, or give the importer one (--image TAG=IMAGE)", tags.join(", ")),
                    };
                    self.report.todo(&mut job.todo, &format!("{at}.image"), why);
                }
                return found;
            }
            Some(Value::Mapping(full)) => (get(full, "name").and_then(scalar), Some(full)),
            Some(other) => (scalar(other), None),
        };
        if let Some(full) = full {
            let at = format!("{at}.image");
            if let Some(entrypoint) = get(full, "entrypoint") {
                if !matches!(entrypoint, Value::Sequence(s) if s.is_empty()) {
                    self.report.todo(&mut job.todo, &format!("{at}.entrypoint"), "the image's own entrypoint runs the job's command here, as it is not set aside: name an image whose entrypoint runs a command it is given (or has none)");
                }
            }
            if let Some(policy) = get(full, "pull_policy") {
                let first = strings(policy).into_iter().next().unwrap_or_default();
                match first.as_str() {
                    "always" => job.set("image_pull_policy", key("always")),
                    "if-not-present" => job.set("image_pull_policy", key("if_not_present")),
                    "never" => job.set("image_pull_policy", key("never")),
                    _ => {}
                }
                if policy.is_sequence() {
                    self.report.note(
                        &format!("{at}.pull_policy"),
                        Kind::Changed,
                        format!("the first of the policies, {first}, is the job's"),
                    );
                }
            }
            for k in full.keys().filter_map(scalar) {
                if !matches!(k.as_str(), "name" | "entrypoint" | "pull_policy") {
                    self.report.todo(
                        &mut job.todo,
                        &format!("{at}.{k}"),
                        "an image here takes its name and its pull policy, and nothing else",
                    );
                }
            }
        }
        let name = name?;
        self.templated(
            job,
            &format!("{at}.image"),
            &name,
            vars,
            matrix_keys,
            "the image",
        )
    }

    /// A text GitLab expanded variables in, as one with none: the file's
    /// variables written out, and a matrix's as the item's.
    fn templated(
        &mut self,
        job: &mut Job,
        at: &str,
        text: &str,
        vars: &[(String, Var)],
        matrix_keys: &BTreeSet<String>,
        what: &str,
    ) -> Option<String> {
        let lookup = |name: &str| -> Option<String> {
            if matrix_keys.contains(name) {
                return Some(tpl(&format!("item.{name}")));
            }
            vars.iter()
                .rev()
                .find(|(n, _)| n == name)
                .map(|(_, v)| match v.expand {
                    true => v.value.clone(),
                    false => v.value.replace('$', "$$"),
                })
        };
        match expanded(text, &lookup, 0) {
            Ok(done) => Some(done),
            Err(missing) => {
                let names: Vec<String> = missing.iter().map(|n| format!("${n}")).collect();
                self.report.todo(&mut job.todo, at, format!("{what} names {}, which cannot be known before the job runs: name it in full", names.join(", ")));
                None
            }
        }
    }

    /// A job's services: dagron's, and a Docker daemon of its own.
    fn services(&mut self, job: &mut Job, at: &str, value: &Value, vars: &[(String, Var)]) {
        let Some(entries) = value.as_sequence() else {
            self.report
                .todo(&mut job.todo, at, "services is a list of images");
            return;
        };
        let mut out = Vec::new();
        let mut named = BTreeSet::new();
        let shared: Vec<Value> = vars
            .iter()
            .filter(|(n, v)| {
                v.plain()
                    && is_shell_name(n)
                    && !n.starts_with("CI_")
                    && !n.starts_with("GIT_")
                    && !n.starts_with("FF_")
                    && !RUNNER_SETTINGS.contains(&n.as_str())
                    && !DAEMON_VARS.contains(&n.as_str())
            })
            .fold(Vec::<(String, String)>::new(), |mut all, (n, v)| {
                all.retain(|(k, _)| k != n);
                all.push((n.clone(), v.value.clone()));
                all
            })
            .into_iter()
            .map(|(n, v)| {
                let mut pair = Mapping::new();
                pair.insert(key("name"), Value::String(n));
                pair.insert(key("value"), Value::String(v));
                Value::Mapping(pair)
            })
            .collect();
        for (n, service) in entries.iter().enumerate() {
            let at = format!("{at}[{n}]");
            let (image, full) = match service {
                Value::Mapping(full) => (get(full, "name").and_then(scalar), Some(full)),
                other => (scalar(other), None),
            };
            let Some(image) = image else {
                self.report
                    .todo(&mut job.todo, &at, "a service names its image");
                continue;
            };
            let base = image_base(&image);
            if base == "docker" && image.contains("dind") {
                job.set("docker_daemon", Value::Bool(true));
                self.report.note(&at, Kind::Changed, "docker:dind is the job's own Docker daemon here (docker_daemon), reached as DOCKER_HOST says, where the engine's operator offers one; a run that is not trusted is refused it");
                continue;
            }
            let alias = full
                .and_then(|f| get(f, "alias"))
                .and_then(scalar)
                .map(|a| a.split(',').next().unwrap_or_default().trim().to_string());
            let gitlab_name = alias.clone().unwrap_or_else(|| {
                let without_tag = match image.split('@').next().unwrap_or(&image).rsplit_once(':') {
                    Some((name, tag)) if !tag.contains('/') => name.to_string(),
                    _ => image.clone(),
                };
                without_tag.replace('/', "-")
            });
            let Some(host) = as_service_name(&gitlab_name).filter(|h| named.insert(h.clone()))
            else {
                self.report.todo(&mut job.todo, &at, format!("the service's name, {gitlab_name}, cannot be a host's name here, or is another service's: it is left out. Give it an alias of lowercase letters, digits and `-`, beginning with a letter"));
                continue;
            };
            if host != gitlab_name {
                self.report.todo(&mut job.todo, &at, format!("the service is reached as `{host}` here, where the job reached it as `{gitlab_name}`: a service's name is a host's"));
            }
            let mut one = Mapping::new();
            one.insert(key("name"), Value::String(host));
            one.insert(key("image"), Value::String(image.clone()));
            let mut env = shared.clone();
            if let Some(full) = full {
                for (name, var) in self.variables_of(
                    &format!("{at}.variables"),
                    get(full, "variables"),
                    Some(&mut job.todo),
                ) {
                    if var.plain() {
                        env.retain(|e| {
                            e.get("name").and_then(Value::as_str) != Some(name.as_str())
                        });
                        let mut pair = Mapping::new();
                        pair.insert(key("name"), Value::String(name));
                        pair.insert(key("value"), Value::String(var.value));
                        env.push(Value::Mapping(pair));
                    } else {
                        self.report.todo(
                            &mut job.todo,
                            &var.at,
                            "a service is given plain values: write the value out",
                        );
                    }
                }
                if let Some(command) = get(full, "command") {
                    one.insert(key("args"), list(strings(command)));
                }
                if get(full, "entrypoint").is_some() {
                    self.report.todo(&mut job.todo, &format!("{at}.entrypoint"), "a service runs its image's own entrypoint here: name an image that starts as the service should");
                }
                for k in full.keys().filter_map(scalar) {
                    if !matches!(
                        k.as_str(),
                        "name" | "alias" | "variables" | "command" | "entrypoint"
                    ) {
                        self.report.note(
                            &format!("{at}.{k}"),
                            Kind::Left,
                            "a service here takes its image, its variables and its command",
                        );
                    }
                }
            }
            if !shared.is_empty() {
                self.report.said_once(&at, Kind::Changed, "a service is given the job's plain variables, as GitLab gives them, each written in its env");
            }
            if !env.is_empty() {
                one.insert(key("env"), Value::Sequence(env));
            }
            match READY.iter().find(|(name, _)| *name == base) {
                Some((_, ready)) => {
                    one.insert(key("ready"), Value::String(ready.to_string()));
                    self.report.note(&at, Kind::Changed, format!("GitLab waited for the service's port to open: here the job waits for `{ready}`"));
                }
                None => self.report.note(&at, Kind::Left, "GitLab waited for the service's port to open: here the job's first step starts once its container has, unless `ready` says what makes it ready"),
            }
            out.push(Value::Mapping(one));
        }
        if out.len() > 8 {
            self.report
                .todo(&mut job.todo, at, "a job here has at most 8 services");
        }
        if !out.is_empty() {
            job.set("services", Value::Sequence(out));
        }
    }

    /// A job's caches, as `workspace.cache`.
    fn caches(&mut self, job: &mut Job, at: &str, value: &Value) -> Vec<Value> {
        let listed: Vec<(String, &Mapping)> = match value {
            Value::Mapping(one) => vec![(at.to_string(), one)],
            Value::Sequence(many) => many
                .iter()
                .enumerate()
                .filter_map(|(n, c)| c.as_mapping().map(|c| (format!("{at}[{n}]"), c)))
                .collect(),
            _ => return Vec::new(),
        };
        let mut out: Vec<JobCache> = Vec::new();
        for (at, cache) in listed {
            if cache.is_empty() {
                continue;
            }
            let paths: Vec<String> = get(cache, "paths").map(strings).unwrap_or_default();
            if paths.is_empty() {
                self.report
                    .note(&at, Kind::Left, "a cache that names no path keeps nothing");
                continue;
            }
            if paths
                .iter()
                .any(|p| p.contains(['*', '?', '[']) || p.split('/').any(|c| c == ".."))
                || paths.len() > 16
            {
                self.report.note(&at, Kind::Left, "its paths name files by a pattern, or are more than 16, and a cache here keeps directories: the job starts cold. Name a workspace.cache of its directories by hand");
                continue;
            }
            let paths: Vec<String> = paths
                .iter()
                .map(|p| {
                    p.replace("$CI_PROJECT_DIR/", "")
                        .replace("${CI_PROJECT_DIR}/", "")
                })
                .collect();
            let (prefix, key_files) = match get(cache, "key") {
                Some(Value::Mapping(k)) => (
                    get(k, "prefix")
                        .and_then(scalar)
                        .unwrap_or_else(|| "cache".into()),
                    get(k, "files").map(strings).unwrap_or_default(),
                ),
                Some(k) => (scalar(k).unwrap_or_default(), Vec::new()),
                None => ("default".to_string(), Vec::new()),
            };
            // The key's own words: the job's name where it names it, and
            // no other variable, since a cache is kept by branch here.
            let id = job.id.clone();
            let lookup = move |name: &str| {
                Some(if name == "CI_JOB_NAME" {
                    id.clone()
                } else {
                    "-".to_string()
                })
            };
            let words: String = expanded(&prefix, &lookup, 0)
                .unwrap_or_default()
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_'))
                .filter(|w| !w.is_empty())
                .collect::<Vec<_>>()
                .join("-");
            let mut prefix: String = words.chars().take(64).collect();
            if prefix.is_empty() {
                prefix = "default".into();
            }
            if named(&prefix_source(cache))
                .iter()
                .any(|n| n != "CI_JOB_NAME")
            {
                self.report.note(&format!("{at}.key"), Kind::Changed, "the variables its key named are not part of it here: a cache is kept for the run's branch, and restored from it and then from a merge request's target");
            }
            if key_files
                .iter()
                .any(|f| f.starts_with('/') || f.split('/').any(|c| c == ".."))
            {
                self.report.note(
                    &at,
                    Kind::Left,
                    "its key names files outside the workspace: the job starts cold",
                );
                continue;
            }
            if out.iter().any(|c| c.prefix == prefix) {
                self.report.note(&at, Kind::Left, format!("a second cache of the prefix {prefix:?}, which the job keeps already: name a workspace.cache of another prefix for it by hand"));
                continue;
            }
            let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
            let files: Vec<&str> = key_files.iter().map(String::as_str).collect();
            let made = JobCache::new(&prefix, &files, &paths);
            self.report.note(&at, Kind::Changed, format!("the cache is {}: restored before the steps and saved after they succeed, by a trusted run, from the run's own branch and then a merge request's target", made.said()));
            match get(cache, "policy").and_then(scalar).as_deref() {
                Some("pull") => self.report.note(
                    &format!("{at}.policy"),
                    Kind::Changed,
                    "the job saves the cache too, where GitLab's only read it",
                ),
                Some("push") => self.report.note(
                    &format!("{at}.policy"),
                    Kind::Changed,
                    "the job reads the cache too, where GitLab's only wrote it",
                ),
                _ => {}
            }
            for k in cache.keys().filter_map(scalar) {
                if !matches!(k.as_str(), "key" | "paths" | "policy") {
                    self.report.note(
                        &format!("{at}.{k}"),
                        Kind::Left,
                        "a cache here takes paths and a key, and nothing else",
                    );
                }
            }
            out.push(made);
        }
        if out.len() > 8 {
            self.report.note(
                at,
                Kind::Left,
                "a job here keeps at most 8 caches: those after the eighth are left out",
            );
            out.truncate(8);
        }
        out.iter().map(JobCache::value).collect()
    }

    /// A job's artifacts: its test report, and whether it keeps files for
    /// the jobs after it.
    fn artifacts(&mut self, job: &mut Job, at: &str, value: &Value) {
        let Some(artifacts) = value.as_mapping() else {
            return;
        };
        if let Some(reports) = get(artifacts, "reports").and_then(Value::as_mapping) {
            for (kind, paths) in reports {
                let Some(kind) = scalar(kind) else { continue };
                let at = format!("{at}.reports.{kind}");
                match kind.as_str() {
                    "junit" => {
                        let paths = strings(paths);
                        let plain = !paths.is_empty()
                            && paths.len() <= 8
                            && paths.iter().all(|p| {
                                !p.starts_with('/')
                                    && p.len() <= 240
                                    && p.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
                                    && p.split('/').all(|c| !c.is_empty() && c != "." && c != "..")
                            });
                        if plain {
                            let mut report = Mapping::new();
                            report.insert(key("junit"), list(paths));
                            job.set("report", Value::Mapping(report));
                            self.report.note(&at, Kind::Changed, "the test report is the job's report: what it holds is said with the job");
                        } else {
                            self.report.note(&at, Kind::Left, "a report here is up to 8 paths of the workspace, written out: a pattern is not read, and the job's tests are not shown");
                        }
                    }
                    "dotenv" => self.report.todo(&mut job.todo, &at, "a later job is given no variable an earlier one wrote: what is handed on so is written to a file both read, in one job"),
                    _ => self.report.note(&at, Kind::Left, "no such report is read here"),
                }
            }
        }
        let keeps = get(artifacts, "paths").is_some()
            || get(artifacts, "untracked") == Some(&Value::Bool(true));
        if keeps {
            job.keeps_files = true;
            self.report.note(&format!("{at}.paths"), Kind::Left, "a job's files are not kept, and not handed to the jobs after it: its workspace goes with it");
        }
        for k in artifacts.keys().filter_map(scalar) {
            if !matches!(k.as_str(), "reports" | "paths" | "untracked") {
                self.report.note(
                    &format!("{at}.{k}"),
                    Kind::Left,
                    "a job's files are not kept here",
                );
            }
        }
    }

    /// A job's scripts as its steps, each with what it reads of GitLab's
    /// variables set at its head.
    #[allow(clippy::too_many_arguments)]
    fn steps(
        &mut self,
        job: &mut Job,
        at: &str,
        map: &Mapping,
        vars: &[(String, Var)],
        stage: &str,
        environment: Option<&str>,
        parallel: Option<usize>,
    ) {
        let lines = |value: Option<&Value>| -> Option<Vec<String>> {
            fn flatten(value: &Value, out: &mut Vec<String>, depth: usize) -> bool {
                match value {
                    Value::Sequence(items) if depth < 10 => {
                        items.iter().all(|i| flatten(i, out, depth + 1))
                    }
                    Value::Null => true,
                    other => match scalar(other) {
                        Some(line) => {
                            out.push(line);
                            true
                        }
                        None => false,
                    },
                }
            }
            let mut out = Vec::new();
            value.map(|v| flatten(v, &mut out, 0).then_some(out))?
        };
        let before = lines(get(map, "before_script"));
        let script = lines(get(map, "script"));
        let after = lines(get(map, "after_script"));
        for (k, got) in [
            ("before_script", &before),
            ("script", &script),
            ("after_script", &after),
        ] {
            if get(map, k).is_some() && got.is_none() {
                self.report.todo(
                    &mut job.todo,
                    &format!("{at}.{k}"),
                    "a script is a list of commands: what is not one is left out",
                );
            }
        }
        let Some(script) = script.filter(|s| !s.is_empty()) else {
            if get(map, "trigger").is_none() {
                self.report.todo(&mut job.todo, at, "the job has no script");
            }
            return;
        };
        let mut main = before.unwrap_or_default();
        main.extend(script);
        let text = main.join("\n");
        let run = self.script(
            job,
            &format!("{at}.script"),
            &text,
            vars,
            stage,
            environment,
            parallel,
        );
        let run = match &job.allow_codes {
            Some(codes) if codes.iter().any(|c| *c != 0) => {
                let listed: Vec<String> = codes
                    .iter()
                    .filter(|c| **c != 0)
                    .map(u8::to_string)
                    .collect();
                self.report.note(&format!("{at}.allow_failure"), Kind::Changed, format!("the job may fail with exit code {} alone: its script runs in a subshell, and those codes end it as a success, which the jobs after it wait for, as on GitLab. The job's log says it failed, where GitLab marked the job", listed.join(" or ")));
                may_exit_with(&run, &listed)
            }
            _ => run,
        };
        let mut step = Mapping::new();
        step.insert(key("name"), key("script"));
        step.insert(key("run"), Value::String(run));
        if job.allow_failure {
            step.insert(key("continue_on_error"), Value::Bool(true));
            let said = match job.when {
                // A manual one is waited for by none, as on GitLab.
                When::Manual => "the job may fail: a step of it that fails does not end it (continue_on_error), and it ends as a success, as GitLab let it fail. The job's log says the step failed, where GitLab marked the job",
                _ => "the job may fail: a step of it that fails does not end it (continue_on_error), and it ends as a success, which the jobs after it wait for, as on GitLab. The job's log says the step failed, where GitLab marked the job",
            };
            self.report
                .note(&format!("{at}.allow_failure"), Kind::Changed, said);
        }
        job.steps.push(Value::Mapping(step));
        if let Some(after) = after.filter(|a| !a.is_empty()) {
            let run = self.script(
                job,
                &format!("{at}.after_script"),
                &after.join("\n"),
                vars,
                stage,
                environment,
                parallel,
            );
            let mut step = Mapping::new();
            step.insert(key("name"), key("after_script"));
            step.insert(key("run"), Value::String(run));
            step.insert(key("continue_on_error"), Value::Bool(true));
            job.steps.push(Value::Mapping(step));
            let said = match job.allow_failure {
                true => "after_script is the job's last step, and a failure of it does not fail the job: it runs whatever became of the script, as GitLab ran it",
                false => "after_script is the job's last step, and a failure of it does not fail the job: it runs where the script succeeded, and not after one that failed, where GitLab ran it whatever became of the script",
            };
            self.report
                .note(&format!("{at}.after_script"), Kind::Changed, said);
        }
        self.report.said_once(&format!("{at}.script"), Kind::Changed, "a job's script ran under bash where its image had bash, and a step here is run by `sh -e`: a script that is bash's calls bash");
    }

    /// A script, with the lines at its head that set what it reads of
    /// GitLab's own variables and of those the file's values expand.
    #[allow(clippy::too_many_arguments)]
    fn script(
        &mut self,
        job: &mut Job,
        at: &str,
        text: &str,
        vars: &[(String, Var)],
        stage: &str,
        environment: Option<&str>,
        parallel: Option<usize>,
    ) -> String {
        // The job's variables, its own over the pipeline's.
        let mut own: Vec<(String, Var)> = Vec::new();
        for (n, v) in vars {
            own.retain(|(k, _)| k != n);
            own.push((n.clone(), v.clone()));
        }
        let mut defined: BTreeSet<&str> = own.iter().map(|(n, _)| n.as_str()).collect();
        defined.extend(job.matrix_keys.iter().map(String::as_str));
        // What it reads, and what the variables it is given read, in turn.
        let mut wanted: BTreeSet<String> =
            variables(text).into_iter().map(str::to_string).collect();
        let expanding: Vec<&(String, Var)> = own
            .iter()
            .filter(|(n, v)| !v.plain() && is_shell_name(n) && !n.starts_with("CI_"))
            .collect();
        for (_, v) in &expanding {
            wanted.extend(named(&v.value));
        }
        let mut head = Vec::new();
        let mut set = Vec::new();
        for (name, line) in PREDEFINED {
            if wanted.contains(*name) && !defined.contains(name) {
                head.push(line.to_string());
                set.push(format!("${name}"));
            }
        }
        let literal = |name: &str, value: &str| format!("export {name}={}", quoted(value));
        let mut by_value: Vec<(&str, String)> = vec![
            ("CI_JOB_NAME", literal("CI_JOB_NAME", &job.id)),
            ("CI_JOB_STAGE", literal("CI_JOB_STAGE", stage)),
            (
                "CI_DEFAULT_BRANCH",
                literal("CI_DEFAULT_BRANCH", self.default_branch()),
            ),
        ];
        if let Some(environment) = environment {
            by_value.push((
                "CI_ENVIRONMENT_NAME",
                literal("CI_ENVIRONMENT_NAME", environment),
            ));
        }
        if let Some(total) = parallel {
            // An instance of `parallel: N` is its own number; one of a
            // matrix is counted from the first.
            let index = match job.matrix_keys.is_empty() {
                true => format!("export CI_NODE_INDEX={}", tpl("item")),
                false => format!("export CI_NODE_INDEX=$(({} + 1))", tpl("index")),
            };
            by_value.push(("CI_NODE_INDEX", index));
            by_value.push(("CI_NODE_TOTAL", format!("export CI_NODE_TOTAL={total}")));
        }
        for (name, line) in by_value {
            if wanted.contains(name) && !defined.contains(name) {
                head.push(line);
                set.push(format!("${name}"));
                if name == "CI_DEFAULT_BRANCH" && self.options.default_branch.is_none() {
                    self.report.said_once(at, Kind::Changed, "$CI_DEFAULT_BRANCH is taken to be main: give the importer the project's own (--default-branch) where it is another");
                }
            }
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
            self.report.said_once(at, Kind::Changed, "GitLab's variables a script reads are set at its head from dagron-ci's own (CI_SHA, CI_REF, …): a program it runs that reads one the script does not is not given it");
        }
        if set
            .iter()
            .any(|s| s == "$CI_COMMIT_REF_NAME" || s == "$CI_COMMIT_REF_SLUG")
        {
            self.report.said_once(at, Kind::Changed, "$CI_COMMIT_REF_NAME, in a merge request's pipeline, is the ref dagron-ci checks out (pull/7/head), and not the source branch's name");
        }
        if set.iter().any(|s| s == "$CI_PIPELINE_SOURCE") {
            self.report.said_once(at, Kind::Changed, "$CI_PIPELINE_SOURCE is push in a branch's and a tag's pipeline, merge_request_event in a merge request's, and api in a run asked for here (dispatch), which stands for GitLab's web, trigger and schedule pipelines too: a script that tests it for one of those takes the other branch");
        }
        // The variables whose values expand others, in an order in which
        // each is set before what reads it.
        let mut placed: BTreeSet<String> = BTreeSet::new();
        let mut pending: Vec<&(String, Var)> = expanding.clone();
        while !pending.is_empty() {
            let before = pending.len();
            pending.retain(|(name, var)| {
                let waits = named(&var.value).iter().any(|n| {
                    n != name && expanding.iter().any(|(e, _)| e == n) && !placed.contains(n)
                });
                if waits {
                    return true;
                }
                head.push(format!("export {name}={}", double_quoted(&var.value)));
                placed.insert(name.clone());
                false
            });
            if pending.len() == before {
                for (name, var) in pending.drain(..) {
                    self.report.todo(
                        &mut job.todo,
                        &var.at,
                        format!("{name} and a variable it names name each other: it is left out"),
                    );
                }
            }
        }
        // What it reads that no step here is given.
        let not_given: Vec<&String> = wanted
            .iter()
            .filter(|name| {
                let ours = PREDEFINED.iter().any(|(n, _)| n == name)
                    || matches!(
                        name.as_str(),
                        "CI_JOB_NAME" | "CI_JOB_STAGE" | "CI_DEFAULT_BRANCH"
                    )
                    || (name.as_str() == "CI_ENVIRONMENT_NAME" && environment.is_some())
                    || (matches!(name.as_str(), "CI_NODE_INDEX" | "CI_NODE_TOTAL")
                        && parallel.is_some());
                (name.starts_with("CI_") || name.starts_with("GITLAB_"))
                    && !ours
                    && !DAGRON_VARS.contains(&name.as_str())
                    && !defined.contains(name.as_str())
            })
            .collect();
        if !not_given.is_empty() {
            let registry = not_given.iter().any(|n| {
                n.starts_with("CI_REGISTRY")
                    || n.starts_with("CI_DEPLOY_")
                    || n.as_str() == "CI_JOB_TOKEN"
            });
            let hint = match registry {
                true => ": GitLab's registry and the job's own token are not here, and a registry's credentials are an environment's secrets, named in the job's env",
                false => "",
            };
            let names: Vec<String> = not_given.iter().map(|n| format!("${n}")).collect();
            self.report.todo(
                &mut job.todo,
                at,
                format!(
                    "the script reads {}, which a step here is not given{hint}",
                    names.join(", ")
                ),
            );
        }
        let unset: Vec<String> = wanted
            .iter()
            .filter(|n| {
                n.chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                    && n.starts_with(|c: char| c.is_ascii_uppercase())
                    && !n.starts_with("CI_")
                    && !n.starts_with("GITLAB_")
                    && !defined.contains(n.as_str())
                    && !ENVIRONMENT.contains(&n.as_str())
                    && !DAEMON_VARS.contains(&n.as_str())
                    && !assigns(text, n)
            })
            .cloned()
            .collect();
        let secret = |n: &str| {
            [
                "TOKEN",
                "PASSWORD",
                "PASSWD",
                "SECRET",
                "KEY",
                "CREDENTIAL",
                "AUTH",
                "PASS",
            ]
            .iter()
            .any(|w| n.split('_').any(|p| p == *w))
        };
        let (secrets, others): (Vec<String>, Vec<String>) =
            unset.into_iter().partition(|n| secret(n));
        if !secrets.is_empty() {
            let names: Vec<String> = secrets.iter().map(|n| format!("${n}")).collect();
            self.report.todo(&mut job.todo, at, format!("the script reads {}, which the job is not given in the file: a project's CI/CD variable is an environment's secret here, which the job names (environment:) and is given in its env (value_from: {{ secret: … }})", names.join(", ")));
        }
        if !others.is_empty() {
            let names: Vec<String> = others.iter().map(|n| format!("${n}")).collect();
            self.report.note(at, Kind::Left, format!("the script reads {}, which the job is not given in the file: where the project's CI/CD variables set one, give it to the job in its env", names.join(", ")));
        }
        if let Some(seen) = bashism(text) {
            self.report.todo(&mut job.todo, at, format!("the script is bash's ({seen}), and a step here is run by `sh -e`: call bash in it, or write it for sh"));
        }
        if head.is_empty() {
            text.to_string()
        } else {
            format!("{}\n{text}", head.join("\n"))
        }
    }

    /// A job, read.
    #[allow(clippy::too_many_lines)]
    fn job(&mut self, id: &str, map: &Mapping) -> Job {
        let at = id.to_string();
        let mut job = Job {
            id: id.to_string(),
            name: as_name(id),
            stage: 0,
            needs: None,
            dependencies: None,
            when: When::OnSuccess,
            allow_failure: false,
            allow_codes: None,
            holds: Holds::all(),
            keeps_files: false,
            fields: Vec::new(),
            steps: Vec::new(),
            todo: Vec::new(),
            single: None,
            matrix_keys: BTreeSet::new(),
            out: false,
        };
        for (place, text) in self.problems.remove(id).unwrap_or_default() {
            self.report.todo(&mut job.todo, &place, text);
        }

        // What it inherits.
        let inherit = get(map, "inherit").and_then(Value::as_mapping);
        let inherits = |what: &str, name: &str| -> bool {
            match inherit.and_then(|i| get(i, what)) {
                None => true,
                Some(Value::Bool(b)) => *b,
                Some(listed) => strings(listed).iter().any(|n| n == name),
            }
        };
        let mut map = map.clone();
        for (k, v) in &self.defaults {
            let Some(name) = scalar(k) else { continue };
            if !map.contains_key(k) && inherits("default", &name) {
                map.insert(k.clone(), v.clone());
            }
        }
        let mut vars: Vec<(String, Var)> = self
            .variables
            .iter()
            .filter(|(n, _)| inherits("variables", n))
            .cloned()
            .collect();
        for (n, v) in self.variables_of(
            &format!("{at}.variables"),
            get(&map, "variables"),
            Some(&mut job.todo),
        ) {
            vars.retain(|(k, _)| *k != n);
            vars.push((n, v));
        }

        // Its stage.
        let stage = get(&map, "stage")
            .and_then(scalar)
            .unwrap_or_else(|| "test".to_string());
        job.stage = match self.stages.iter().position(|s| *s == stage) {
            Some(n) => n,
            None => {
                self.report.todo(&mut job.todo, &format!("{at}.stage"), format!("{stage:?} is not one of the pipeline's stages: the job is taken to be of the last"));
                self.stages.len() - 1
            }
        };

        // Its matrix.
        let mut matrix_keys = BTreeSet::new();
        let mut parallel = None;
        match get(&map, "parallel") {
            None => {}
            Some(Value::Number(n)) => match n.as_u64().map(|n| n as usize) {
                Some(n) if (1..=MAX_PARALLEL).contains(&n) => {
                    parallel = Some(n);
                    let items: Vec<Value> =
                        (1..=n as u64).map(|i| Value::Number(i.into())).collect();
                    job.set("with_items", Value::Sequence(items));
                    self.report.note(
                        &format!("{at}.parallel"),
                        Kind::Changed,
                        format!("the job is {n} instances of one task (with_items)"),
                    );
                }
                _ => self.report.todo(
                    &mut job.todo,
                    &format!("{at}.parallel"),
                    format!("parallel is 1 to {MAX_PARALLEL}"),
                ),
            },
            Some(Value::Mapping(full)) => {
                if let Some((items, keys)) = self.matrix(
                    &mut job,
                    &format!("{at}.parallel.matrix"),
                    get(full, "matrix"),
                ) {
                    parallel = Some(items.len());
                    let single = keys.len() == 1;
                    let axis = keys.iter().next().cloned();
                    let items: Vec<Value> = items
                        .into_iter()
                        .map(|item| match (single, &axis) {
                            (true, Some(axis)) => {
                                item.get(key(axis)).cloned().unwrap_or(Value::Null)
                            }
                            _ => Value::Mapping(item),
                        })
                        .collect();
                    job.set("with_items", Value::Sequence(items));
                    if single {
                        job.single = axis.clone();
                    }
                    matrix_keys = keys;
                    job.matrix_keys = matrix_keys.clone();
                }
            }
            Some(_) => self.report.todo(
                &mut job.todo,
                &format!("{at}.parallel"),
                "parallel is a number, or matrix:",
            ),
        }

        // When it runs.
        let job_when = match get(&map, "when").and_then(scalar).as_deref() {
            None => When::OnSuccess,
            Some("never") => {
                job.out = true;
                When::OnSuccess
            }
            Some(word) => match self.when_word(word) {
                Ok(when) => when,
                Err(why) => {
                    self.report.todo(&mut job.todo, &format!("{at}.when"), why);
                    When::OnSuccess
                }
            },
        };
        let plain = plain_of(&vars);
        let (holds, when) = if let Some(listed) = get(&map, "rules") {
            if get(&map, "only").is_some() || get(&map, "except").is_some() {
                self.report.todo(
                    &mut job.todo,
                    &at,
                    "GitLab takes rules or only and except, not both: only and except are not read",
                );
            }
            let (holds, when) = self.rules(
                &format!("{at}.rules"),
                listed,
                Some((id, &plain)),
                job_when,
                &mut job.todo,
                false,
            );
            (holds, when.unwrap_or(job_when))
        } else if get(&map, "only").is_some() || get(&map, "except").is_some() {
            (self.only_except(&at, &map, &plain, &mut job.todo), job_when)
        } else {
            (self.default_holds(), job_when)
        };
        job.holds = holds;
        job.when = when;
        if !job.holds.may_hold() {
            job.out = true;
        }
        match job.when {
            When::OnFailure => job.set("trigger_rule", key("one_failed")),
            When::Always => job.set("trigger_rule", key("all_done")),
            _ => {}
        }

        // Whether it may fail: with any exit code, or with those it names.
        job.allow_failure = match get(&map, "allow_failure") {
            Some(Value::Bool(b)) => *b,
            Some(Value::Mapping(full)) => {
                let codes: Option<Vec<u8>> = match get(full, "exit_codes") {
                    Some(Value::Sequence(items)) if !items.is_empty() => items
                        .iter()
                        .map(|c| c.as_u64().and_then(|c| u8::try_from(c).ok()))
                        .collect(),
                    Some(one) => one
                        .as_u64()
                        .and_then(|c| u8::try_from(c).ok())
                        .map(|c| vec![c]),
                    None => None,
                };
                match codes {
                    Some(codes) => job.allow_codes = Some(codes),
                    None => self.report.todo(
                        &mut job.todo,
                        &format!("{at}.allow_failure"),
                        "allow_failure: exit_codes is an exit code (0 to 255) or a list of them: the job may not fail",
                    ),
                }
                false
            }
            Some(_) => {
                self.report.todo(
                    &mut job.todo,
                    &format!("{at}.allow_failure"),
                    "allow_failure is true or false",
                );
                false
            }
            // A manual job may be left unrun, as GitLab has it, where the job
            // says `when: manual`; one a rule makes manual may not.
            None => job_when == When::Manual,
        };

        // Its image and runner.
        let tags: Vec<String> = get(&map, "tags").map(strings).unwrap_or_default();
        if !tags.is_empty() {
            self.report.note(&format!("{at}.tags"), Kind::Left, "a job runs on the engine's capacity: a runner's tags are a runner_class or a pool here, which the operator names");
        }
        if get(&map, "trigger").is_some() {
            self.report.todo(&mut job.todo, &format!("{at}.trigger"), "a child or another project's pipeline is not started here: write its jobs here, or make it a pipeline of its own");
        } else if let Some(image) = self.image(
            &mut job,
            &at,
            get(&map, "image"),
            &vars,
            &matrix_keys,
            &tags,
        ) {
            job.set("docker_image", Value::String(image));
        }

        // Its environment.
        let environment = match get(&map, "environment") {
            Some(Value::Mapping(full)) => {
                for k in full.keys().filter_map(scalar) {
                    if k != "name" {
                        self.report.note(&format!("{at}.environment.{k}"), Kind::Left, "an environment is named here, for its secrets and who may use it; there is no page of deployments");
                    }
                }
                if get(full, "action")
                    .and_then(scalar)
                    .is_some_and(|a| a == "stop")
                {
                    self.report.todo(&mut job.todo, &format!("{at}.environment.action"), "a job that stops an environment is not one here: run what it runs where it is needed");
                }
                get(full, "name").and_then(scalar)
            }
            Some(other) => scalar(other),
            None => None,
        };
        let environment = match environment {
            Some(name) if name.contains('$') => {
                self.report.todo(
                    &mut job.todo,
                    &format!("{at}.environment"),
                    "which environment it is, is decided as the run goes there: name it here",
                );
                None
            }
            Some(name) => {
                job.set("environment", Value::String(name.clone()));
                Some(name)
            }
            None => None,
        };

        // Its services: a Docker daemon among them is the job's own.
        if let Some(services) = get(&map, "services") {
            self.services(&mut job, &format!("{at}.services"), services, &vars);
        }

        // Its variables, as dagron's own.
        let mut env = Vec::new();
        let mut workspace = Mapping::new();
        for (name, var) in &vars {
            match name.as_str() {
                "GIT_DEPTH" => {
                    match var.value.trim().parse::<u64>() {
                        Ok(depth) => {
                            let mut checkout = Mapping::new();
                            checkout.insert(key("depth"), Value::Number(depth.into()));
                            workspace.insert(key("checkout"), Value::Mapping(checkout));
                            self.report.note(&var.at, Kind::Changed, "how deep the checkout is: workspace.checkout.depth");
                        }
                        Err(_) => self.report.todo(&mut job.todo, &var.at, "the checkout's depth is a number: say it in workspace: { checkout: { depth: N } }"),
                    }
                    continue;
                }
                "GIT_STRATEGY" => {
                    match var.value.as_str() {
                        "none" | "empty" => {
                            workspace.insert(key("checkout"), Value::Bool(false));
                            self.report.note(
                                &var.at,
                                Kind::Changed,
                                "the job's workspace starts empty: workspace.checkout is false",
                            );
                        }
                        _ => self.report.note(
                            &var.at,
                            Kind::Left,
                            "a job's workspace is a checkout of the event's commit, made for it",
                        ),
                    }
                    continue;
                }
                "GIT_SUBMODULE_STRATEGY" if var.value != "none" => {
                    self.report.todo(&mut job.todo, &var.at, "a checkout here holds no submodules: fetch them in the script (git submodule update --init)");
                    continue;
                }
                _ => {}
            }
            if RUNNER_SETTINGS.contains(&name.as_str())
                || name.starts_with("FF_")
                || name == "GIT_SUBMODULE_STRATEGY"
            {
                self.report.note(
                    &var.at,
                    Kind::Left,
                    "a setting of GitLab's runner, which has no place here",
                );
                continue;
            }
            if DAEMON_VARS.contains(&name.as_str()) && job.has("docker_daemon") {
                self.report.note(
                    &var.at,
                    Kind::Left,
                    "the job's Docker daemon gives the job its own",
                );
                continue;
            }
            if name.starts_with("CI_") {
                self.report.todo(
                    &mut job.todo,
                    &var.at,
                    "a variable that begins CI_ is set from the event here: give it another name",
                );
                continue;
            }
            if !is_shell_name(name) {
                self.report.todo(&mut job.todo, &var.at, "the variable's name is not one a shell's variable may have (letters, digits and `_`, not beginning with a digit): it is left out");
                continue;
            }
            if !var.plain() {
                // Set at the head of each script, where the shell expands it.
                continue;
            }
            let mut pair = Mapping::new();
            pair.insert(key("name"), Value::String(name.clone()));
            pair.insert(key("value"), Value::String(var.value.clone()));
            env.push(Value::Mapping(pair));
        }
        for axis in &matrix_keys {
            if !is_shell_name(axis) || axis.starts_with("CI_") {
                continue;
            }
            let template = tpl(&format!("item.{axis}"));
            let mut pair = Mapping::new();
            pair.insert(key("name"), Value::String(axis.clone()));
            pair.insert(key("value"), Value::String(template));
            env.push(Value::Mapping(pair));
        }
        for (_, var) in vars.iter().filter(|(_, v)| !v.plain()) {
            if !self.said_vars.insert(var.at.clone()) {
                continue;
            }
            self.report.note(&var.at, Kind::Changed, "its value names other variables: it is set at the head of each script, where the shell expands it as GitLab did");
        }
        if !env.is_empty() {
            job.set("env", Value::Sequence(env));
        }

        // Caches, artifacts.
        if let Some(cache) = get(&map, "cache") {
            let caches = self.caches(&mut job, &format!("{at}.cache"), cache);
            if !caches.is_empty() {
                workspace.insert(key("cache"), Value::Sequence(caches));
            }
        }
        if !workspace.is_empty() {
            job.set("workspace", Value::Mapping(workspace));
        }
        if let Some(artifacts) = get(&map, "artifacts") {
            self.artifacts(&mut job, &format!("{at}.artifacts"), artifacts);
        }

        // Its order.
        if let Some(needs) = get(&map, "needs") {
            let mut listed = Vec::new();
            for (n, need) in needs
                .as_sequence()
                .cloned()
                .unwrap_or_default()
                .iter()
                .enumerate()
            {
                let at = format!("{at}.needs[{n}]");
                match need {
                    Value::Mapping(full) => {
                        if get(full, "pipeline").is_some() || get(full, "project").is_some() {
                            self.report.todo(
                                &mut job.todo,
                                &at,
                                "a job of another pipeline or project is not waited for here",
                            );
                            continue;
                        }
                        let Some(name) = get(full, "job").and_then(scalar) else {
                            self.report.todo(&mut job.todo, &at, "a need names its job");
                            continue;
                        };
                        if get(full, "parallel").is_some() {
                            self.report.note(&format!("{at}.parallel"), Kind::Changed, "the job waits for every instance of the job it needs, and not for some");
                        }
                        listed.push(Need {
                            job: name,
                            artifacts: get(full, "artifacts") != Some(&Value::Bool(false)),
                            optional: get(full, "optional") == Some(&Value::Bool(true)),
                        });
                    }
                    other => match scalar(other) {
                        Some(name) => listed.push(Need {
                            job: name,
                            artifacts: true,
                            optional: false,
                        }),
                        None => self.report.todo(&mut job.todo, &at, "a need names its job"),
                    },
                }
            }
            job.needs = Some(listed);
        }
        if let Some(dependencies) = get(&map, "dependencies") {
            job.dependencies = Some(strings(dependencies));
        }

        // Its limits.
        match get(&map, "timeout") {
            None => {}
            Some(timeout) => match scalar(timeout).as_deref().and_then(duration_secs) {
                Some(secs) => job.set("timeout_secs", Value::Number(secs.into())),
                None => self.report.todo(
                    &mut job.todo,
                    &format!("{at}.timeout"),
                    "the job's timeout could not be read: say it in seconds (timeout_secs)",
                ),
            },
        }
        match get(&map, "retry") {
            None => {}
            Some(retry) => {
                let max = match retry {
                    Value::Mapping(full) => {
                        for k in ["when", "exit_codes"] {
                            if get(full, k).is_some() {
                                self.report.note(
                                    &format!("{at}.retry.{k}"),
                                    Kind::Changed,
                                    "the job is tried again whatever made it fail",
                                );
                            }
                        }
                        get(full, "max").and_then(Value::as_u64).unwrap_or(0)
                    }
                    other => other.as_u64().unwrap_or(0),
                };
                if max > 0 {
                    job.set("max_attempts", Value::Number((max + 1).into()));
                    self.report.note(
                        &format!("{at}.retry"),
                        Kind::Changed,
                        format!(
                            "retry: {max} is max_attempts: {}, which counts the first attempt",
                            max + 1
                        ),
                    );
                }
            }
        }

        // Its script.
        let stage_name = self.stages[job.stage].clone();
        self.steps(
            &mut job,
            &at,
            &map,
            &vars,
            &stage_name,
            environment.as_deref(),
            parallel,
        );

        // Everything else it says.
        for (k, said, kind) in [
            ("coverage", "a job's coverage is not read from its log here", Kind::Left),
            ("hooks", "a job's hooks are not run: put what they run in its script", Kind::Todo),
            ("id_tokens", "a job's token for a cloud is dagron's identity here: `identity: { oidc: { audience: … } }`, and the job asks for one where DAGRON_OIDC_REQUEST_URL says", Kind::Todo),
            ("secrets", "a vault's secrets are not read here: they are an environment's secrets, named in the job's env", Kind::Todo),
            ("release", "a release is not made here: make it in the script", Kind::Todo),
            ("pages", "GitLab Pages is not here: publish the site in the script", Kind::Todo),
            ("identity", "a job's identity here is `identity: { oidc: { audience: … } }`", Kind::Todo),
            ("manual_confirmation", "an approval asks its own question (approval_message)", Kind::Left),
            ("dast_configuration", "is not taken here", Kind::Left),
            ("run", "GitLab's CI steps are not taken: write them as the script", Kind::Todo),
        ] {
            if get(&map, k).is_none() {
                continue;
            }
            match kind {
                Kind::Todo => self.report.todo(&mut job.todo, &format!("{at}.{k}"), said),
                kind => self.report.note(&format!("{at}.{k}"), kind, said),
            }
        }
        if get(&map, "start_in").is_some()
            && get(&map, "when").and_then(scalar).as_deref() != Some("delayed")
        {
            self.report.note(
                &format!("{at}.start_in"),
                Kind::Left,
                "start_in is a delayed job's, and this one is not",
            );
        }
        if get(&map, "interruptible") == Some(&Value::Bool(false)) {
            self.report.note(&format!("{at}.interruptible"), Kind::Changed, "where a newer run takes an older one's place, the older run's jobs are all cancelled, this one too");
        }
        if id == "pages" && get(&map, "pages").is_none() {
            self.report.todo(&mut job.todo, &at, "a job named pages published GitLab Pages, which is not here: publish the site in the script");
        }
        if let Some(group) = get(&map, "resource_group").and_then(scalar) {
            self.report.todo(&mut job.todo, &format!("{at}.resource_group"), format!("a job's own lock ({group}) is not here: a pipeline's runs of one concurrency key run one at a time (concurrency: {{ key: … }}), and a job that must not run twice at once is the only job of such a pipeline"));
        }
        let known = [
            "after_script",
            "allow_failure",
            "artifacts",
            "before_script",
            "cache",
            "coverage",
            "dast_configuration",
            "dependencies",
            "environment",
            "except",
            "extends",
            "hooks",
            "id_tokens",
            "identity",
            "image",
            "inherit",
            "interruptible",
            "manual_confirmation",
            "needs",
            "only",
            "pages",
            "parallel",
            "release",
            "resource_group",
            "retry",
            "rules",
            "run",
            "script",
            "secrets",
            "services",
            "stage",
            "start_in",
            "tags",
            "timeout",
            "trigger",
            "variables",
            "when",
        ];
        for k in map.keys().filter_map(scalar) {
            if !known.contains(&k.as_str()) {
                self.report.todo(
                    &mut job.todo,
                    &format!("{at}.{k}"),
                    "a job has no such key here",
                );
            }
        }
        job
    }

    /// `parallel: matrix:`, written out: its instances, and the keys each
    /// has.
    fn matrix(
        &mut self,
        job: &mut Job,
        at: &str,
        value: Option<&Value>,
    ) -> Option<(Vec<Mapping>, BTreeSet<String>)> {
        let Some(entries) = value.and_then(Value::as_sequence) else {
            self.report.todo(
                &mut job.todo,
                at,
                "a matrix is a list of variables and their values",
            );
            return None;
        };
        let mut items: Vec<Mapping> = Vec::new();
        for (n, entry) in entries.iter().enumerate() {
            let Some(entry) = entry.as_mapping() else {
                self.report.todo(
                    &mut job.todo,
                    &format!("{at}[{n}]"),
                    "a matrix's entry is variables and their values",
                );
                return None;
            };
            let mut combos = vec![Mapping::new()];
            for (name, values) in entry {
                let Some(name) = scalar(name) else { continue };
                let values: Vec<String> = strings(values);
                if values.is_empty() {
                    self.report.todo(
                        &mut job.todo,
                        &format!("{at}[{n}].{name}"),
                        "a matrix's variable has a value, or a list of them",
                    );
                    return None;
                }
                let name = &name;
                combos = combos
                    .iter()
                    .flat_map(|combo| {
                        values.iter().map(move |v| {
                            let mut next = combo.clone();
                            next.insert(key(name), Value::String(v.clone()));
                            next
                        })
                    })
                    .collect();
            }
            items.extend(combos);
            if items.len() > MAX_PARALLEL {
                self.report.todo(&mut job.todo, at, format!("the matrix comes to more than {MAX_PARALLEL} instances, which is more than GitLab runs of one: it is not written out"));
                return None;
            }
        }
        let keys: BTreeSet<String> = items
            .iter()
            .flat_map(|i| i.keys().filter_map(scalar))
            .collect();
        for item in &mut items {
            for k in &keys {
                if !item.contains_key(key(k)) {
                    item.insert(key(k), Value::String(String::new()));
                }
            }
        }
        self.report.note(at, Kind::Changed, format!("the job is {} instances of one task (with_items), each given its matrix's variables", items.len()));
        Some((items, keys))
    }
}

/// The text of a cache's key, as written, for what it names.
fn prefix_source(cache: &Mapping) -> String {
    match get(cache, "key") {
        Some(Value::Mapping(k)) => get(k, "prefix").and_then(scalar).unwrap_or_default(),
        Some(k) => scalar(k).unwrap_or_default(),
        None => String::new(),
    }
}

/// A script that ends as a success where it fails with one of `codes`
/// (`allow_failure: exit_codes`): it runs in a subshell of its own, which
/// stops at the first command that fails, as the step's shell would.
fn may_exit_with(script: &str, codes: &[String]) -> String {
    format!(
        "set +e\n(\nset -e\n{script}\n)\ncode=$?\nset -e\ncase $code in\n  {}) echo \"the script failed with exit code $code, which the job may (allow_failure: exit_codes)\" >&2 ;;\n  *) exit $code ;;\nesac",
        codes.join("|")
    )
}

/// Whether a script gives `name` a value of its own (`NAME=…`, `export
/// NAME`, `read NAME`, `for NAME in`).
fn assigns(text: &str, name: &str) -> bool {
    text.match_indices(name).any(|(at, _)| {
        let before = text[..at].chars().next_back();
        let after = &text[at + name.len()..];
        let starts =
            before.is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '{'));
        starts
            && (after.starts_with('=')
                || text[..at].trim_end().ends_with("export")
                || text[..at].trim_end().ends_with("read")
                || text[..at].trim_end().ends_with("-r")
                || text[..at].trim_end().ends_with("for"))
    })
}

/// The variables with a plain value, as a condition reads them: the last
/// of a name over those before.
fn plain_of(vars: &[(String, Var)]) -> BTreeMap<String, String> {
    vars.iter()
        .filter(|(_, v)| v.plain())
        .map(|(n, v)| (n.clone(), v.value.clone()))
        .collect()
}

fn when_said(when: When) -> &'static str {
    match when {
        When::OnSuccess => "on_success",
        When::OnFailure => "on_failure",
        When::Always => "always",
        When::Manual => "manual",
    }
}

/// What a job's `when:` says, of the events the pipeline runs for.
struct Written {
    /// The condition; `None` where the job runs for every event `on:` lets
    /// in, and needs none.
    when: Option<String>,
    /// What it lets in of each event: where the job is in a run.
    present: BTreeMap<Event, Names>,
    /// The events of which it lets in more than the job ran for: the names
    /// a `when:` cannot hold.
    wider: Vec<Event>,
}

/// The `when:` of a job that runs for `highs` of each event, in a pipeline
/// whose `on:` lets in `events`.
fn written(highs: &BTreeMap<Event, Names>, events: &BTreeMap<Event, Names>) -> Written {
    let mut plain: Vec<&str> = Vec::new();
    let mut named: Vec<String> = Vec::new();
    let mut present = BTreeMap::new();
    let mut wider = Vec::new();
    for (event, filter) in events {
        // What `on:` lets in already, the job need not say again; and what
        // dispatch lets in is every branch.
        let let_in = match event {
            Event::Dispatch => Names::all(),
            _ => filter.clone(),
        };
        let high = highs.get(event).cloned().unwrap_or_else(Names::none);
        if high.is_none() {
            continue;
        }
        if high == let_in {
            plain.push(event.word());
            present.insert(*event, let_in);
            continue;
        }
        match (event.ref_prefix(), &high) {
            (Some(prefix), Names::Only(names) | Names::AllBut(names))
                if names.iter().all(|n| group_holds(prefix, n)) =>
            {
                let refs: Vec<String> = names.iter().map(|n| format!("{prefix}{n}")).collect();
                let op = match high {
                    Names::Only(_) => "in",
                    Names::AllBut(_) => "not in",
                };
                named.push(format!(
                    "{} == {} and {} {op} [{}]",
                    tpl("ci_event"),
                    event.word(),
                    tpl("ci_group"),
                    refs.join(", ")
                ));
                present.insert(*event, high);
            }
            _ => {
                plain.push(event.word());
                wider.push(*event);
                present.insert(*event, let_in);
            }
        }
    }
    let when = match (plain.as_slice(), named.is_empty()) {
        (_, true) if plain.len() == events.len() => None,
        ([], true) => None,
        _ => {
            let mut parts = Vec::new();
            match plain.as_slice() {
                [] => {}
                [one] => parts.push(format!("{} == {one}", tpl("ci_event"))),
                many => parts.push(format!("{} in [{}]", tpl("ci_event"), many.join(", "))),
            }
            parts.extend(named);
            Some(parts.join(" or "))
        }
    };
    Written {
        when,
        present,
        wider,
    }
}

/// Read a project's `.gitlab-ci.yml` as a pipeline.
pub fn gitlab_to_pipeline(text: &str, options: &Options) -> Result<Imported> {
    // A pipeline that runs for no event here but a schedule's, or another
    // pipeline's, is read again, with those as a run asked for.
    match import(text, options, false)? {
        Some(imported) => Ok(imported),
        None => import(text, options, true)?
            .context("the pipeline runs for no event here, its schedules as runs asked for"),
    }
}

/// [`gitlab_to_pipeline`], with a schedule's pipeline and another pipeline's
/// read as a run asked for, or not; `None` where, without them, the
/// pipeline runs for no event here.
#[allow(clippy::too_many_lines)]
fn import(text: &str, options: &Options, sources_as_dispatch: bool) -> Result<Option<Imported>> {
    let main = load(text)?;
    let mut importer = Importer {
        options,
        report: Report::default(),
        doc: Mapping::new(),
        problems: BTreeMap::new(),
        stages: Vec::new(),
        variables: Vec::new(),
        defaults: Mapping::new(),
        workflow: Holds::all(),
        workflow_rules: false,
        sources_as_dispatch,
        said_vars: BTreeSet::new(),
    };
    let merged = importer.merged(main);
    let Some(merged) = merged.as_mapping().cloned() else {
        bail!("a pipeline is a mapping of jobs and keys");
    };
    let extended = importer.extended(&merged);
    let mut doc = Mapping::new();
    for (k, v) in &extended {
        let mut v = v.clone();
        let name = scalar(k).unwrap_or_default();
        importer.referenced(&extended, &name, &name, &mut v, 0);
        doc.insert(k.clone(), v);
    }
    importer.doc = doc.clone();
    for (job, problems) in std::mem::take(&mut importer.problems) {
        match (GLOBAL_KEYS.contains(&job.as_str()), job.starts_with('.')) {
            (true, _) => problems
                .into_iter()
                .for_each(|(at, text)| importer.report.todo_all(&at, text)),
            // A template is no job: each job that extends it says what is
            // wrong with it.
            (_, true) => problems
                .into_iter()
                .for_each(|(at, text)| importer.report.note(&at, Kind::Left, text)),
            _ => {
                importer.problems.insert(job, problems);
            }
        }
    }
    importer.globals();

    // The jobs, in the file's order.
    let mut names = BTreeSet::new();
    let mut jobs: Vec<Job> = Vec::new();
    for (k, v) in &doc {
        let Some(id) = scalar(k) else { continue };
        if GLOBAL_KEYS.contains(&id.as_str()) || id.starts_with('.') {
            continue;
        }
        let Some(map) = v.as_mapping() else {
            importer
                .report
                .todo_all(&id, "a key that is no job, and not imported");
            continue;
        };
        if as_name(&id).is_empty() {
            bail!("the job {id:?} has nothing to be named by: a pipeline's jobs are named by letters, digits, '.', '_' and '-'");
        }
        if !names.insert(as_name(&id)) {
            bail!("two jobs come to the name {:?}: a pipeline's jobs are named by letters, digits, '.', '_' and '-'", as_name(&id));
        }
        jobs.push(importer.job(&id, map));
    }
    if jobs.is_empty() {
        bail!("the pipeline has no job: a job is a key of the file that is no keyword and does not begin with '.'");
    }
    for job in jobs.iter().filter(|j| j.out) {
        importer.report.note(&job.id, Kind::Left, "no pipeline here has the job: its rules, or its when: never, leave it out of every one");
    }

    // The events the pipeline runs for, and what `on:` says of each.
    let mut workflow = importer.workflow.clone();
    // What `on:` lets in of each event: what the pipeline is made for, and
    // some job of it runs for.
    let mut events: BTreeMap<Event, Names> = BTreeMap::new();
    for event in EVENTS {
        let pipeline = workflow.of(event);
        let some_job = jobs
            .iter()
            .filter(|j| !j.out)
            .fold(Names::none(), |all, j| all.or(&j.holds.of(event).high));
        let let_in = pipeline.high.and(&some_job);
        if !let_in.is_none() {
            events.insert(event, let_in);
        }
    }
    if events.is_empty() && !sources_as_dispatch {
        return Ok(None);
    }
    let mut on = Mapping::new();
    let patterns = |names: &Names| -> (&'static str, Vec<String>) {
        match names {
            Names::Only(names) => ("", names.iter().map(|n| literal_glob(n)).collect()),
            Names::AllBut(names) if names.is_empty() => ("", vec!["**".to_string()]),
            Names::AllBut(names) => ("_ignore", names.iter().map(|n| literal_glob(n)).collect()),
        }
    };
    let push = events.get(&Event::Push);
    let tag = events.get(&Event::Tag);
    if push.is_some() || tag.is_some() {
        let mut filters = Mapping::new();
        let every = |n: Option<&Names>| n.is_some_and(Names::is_all);
        if !(every(push) && every(tag)) {
            if let Some(push) = push {
                let (suffix, globs) = patterns(push);
                filters.insert(key(&format!("branches{suffix}")), list(globs));
            }
            if let Some(tag) = tag {
                let (suffix, globs) = patterns(tag);
                filters.insert(key(&format!("tags{suffix}")), list(globs));
            }
        }
        on.insert(key("push"), Value::Mapping(filters));
    }
    if let Some(target) = events.get(&Event::PullRequest) {
        let mut filters = Mapping::new();
        if !target.is_all() {
            let (suffix, globs) = patterns(target);
            filters.insert(key(&format!("branches{suffix}")), list(globs));
        }
        on.insert(key("pull_request"), Value::Mapping(filters));
        importer.report.said_once("on.pull_request", Kind::Changed, "a merge request's pipeline is a pull request's here; GitLab's merged-results and merge-train pipelines are not told apart from it");
    }
    if events.contains_key(&Event::Dispatch) {
        on.insert(key("dispatch"), Value::Mapping(Mapping::new()));
        importer.report.said_once("on.dispatch", Kind::Changed, "a pipeline run from GitLab's page or its API is a run asked for here (dispatch), through the run API");
    }
    if on.is_empty() {
        importer.report.todo_all("on", "no job of the pipeline runs for any event a pipeline here runs for: it is given a dispatch, so that it can be asked for, and its jobs that run for one are in it");
        on.insert(key("dispatch"), Value::Mapping(Mapping::new()));
        events.insert(Event::Dispatch, Names::all());
        workflow = workflow.or(&Holds::events(&[Event::Dispatch]));
    }

    // Each job's `when:`, for the events the pipeline runs for, and where
    // the job is in a run.
    let mut present: Vec<BTreeMap<Event, Names>> = vec![BTreeMap::new(); jobs.len()];
    for (n, job) in jobs.iter_mut().enumerate() {
        if job.out {
            continue;
        }
        let highs: BTreeMap<Event, Names> = events
            .keys()
            .map(|e| (*e, job.holds.of(*e).high.and(&workflow.of(*e).high)))
            .collect();
        let said = written(&highs, &events);
        if said.present.is_empty() {
            job.out = true;
            importer.report.note(&job.id, Kind::Left, "no pipeline here has the job: its rules leave it out of every event the pipeline runs for");
            continue;
        }
        for event in &said.wider {
            let what = match event {
                Event::PullRequest => "the branch a merge request merges into",
                _ => "a branch or tag whose name a when: cannot hold",
            };
            importer.report.todo(&mut job.todo, &format!("{}.rules", job.id), format!("it ran for {what} alone, and a job's when: cannot say which: it is written to run for every {}", event.word()));
        }
        if let Some(when) = said.when {
            job.set("when", Value::String(when));
        }
        present[n] = said.present;
    }
    if jobs.iter().all(|j| j.out) {
        bail!("no job of the pipeline is in a pipeline here: their rules leave each out of every event a pipeline here runs for (a branch's, a tag's, a merge request's and a run asked for)");
    }

    // The order: `needs`, else the stages.
    let ids: BTreeMap<String, usize> = jobs
        .iter()
        .enumerate()
        .map(|(n, j)| (j.id.clone(), n))
        .collect();
    // Whether one job is in a run wherever another is (`o` of `n`), and
    // whether the two are ever in one run.
    let covers = |present: &[BTreeMap<Event, Names>], o: usize, n: usize| {
        present[n].iter().all(|(e, names)| match present[o].get(e) {
            Some(theirs) => names.and(&theirs.not()).is_none(),
            None => names.is_none(),
        })
    };
    let meets = |present: &[BTreeMap<Event, Names>], o: usize, n: usize| {
        present[n].iter().any(|(e, names)| {
            present[o]
                .get(e)
                .is_some_and(|theirs| !names.and(theirs).is_none())
        })
    };
    let mut deps: Vec<Vec<usize>> = vec![Vec::new(); jobs.len()];
    let mut by_stage: Vec<bool> = vec![false; jobs.len()];
    for n in 0..jobs.len() {
        if jobs[n].out {
            continue;
        }
        match jobs[n].needs.take() {
            Some(needs) => {
                for need in &needs {
                    match ids.get(&need.job) {
                        Some(&d) if jobs[d].out => {
                            let at = format!("{}.needs", jobs[n].id);
                            match need.optional {
                                true => importer.report.note(&at, Kind::Left, format!("{} is in no pipeline here, and the job does not wait for it", need.job)),
                                false => importer.report.todo(&mut jobs[n].todo, &at, format!("{} is in no pipeline here: the job does not wait for it", need.job)),
                            }
                        }
                        Some(&d) => deps[n].push(d),
                        None => {
                            let at = format!("{}.needs", jobs[n].id);
                            importer.report.todo(
                                &mut jobs[n].todo,
                                &at,
                                format!("{} is no job of the pipeline", need.job),
                            );
                        }
                    }
                }
                jobs[n].needs = Some(needs);
            }
            None => {
                by_stage[n] = true;
                for d in 0..jobs.len() {
                    let before = &jobs[d];
                    if before.out || before.stage >= jobs[n].stage {
                        continue;
                    }
                    // A manual job that may be left unrun holds nothing
                    // back, as on GitLab.
                    if before.when == When::Manual && before.allow_failure {
                        continue;
                    }
                    // One that runs where a job before it failed is skipped
                    // where none did: a job that runs where they succeeded
                    // does not wait for it.
                    if before.when == When::OnFailure
                        && !matches!(jobs[n].when, When::OnFailure | When::Always)
                    {
                        continue;
                    }
                    // One never in a run beside it is nothing to wait for.
                    if !meets(&present, d, n) {
                        continue;
                    }
                    deps[n].push(d);
                }
            }
        }
    }
    // The jobs each waits for, through those it waits for.
    let mut ancestors: Vec<Option<BTreeSet<usize>>> = vec![None; jobs.len()];
    fn ancestors_of(
        n: usize,
        deps: &[Vec<usize>],
        memo: &mut Vec<Option<BTreeSet<usize>>>,
        path: &mut Vec<usize>,
    ) -> Result<BTreeSet<usize>, usize> {
        if let Some(done) = &memo[n] {
            return Ok(done.clone());
        }
        if path.contains(&n) {
            return Err(n);
        }
        path.push(n);
        let mut all = BTreeSet::new();
        for &d in &deps[n] {
            all.insert(d);
            all.extend(ancestors_of(d, deps, memo, path)?);
        }
        path.pop();
        memo[n] = Some(all.clone());
        Ok(all)
    }
    for n in 0..jobs.len() {
        if let Err(at) = ancestors_of(n, &deps, &mut ancestors, &mut Vec::new()) {
            bail!(
                "the job {:?} needs itself, through the jobs it needs",
                jobs[at].id
            );
        }
    }
    // Each job after those it waits for: one waits for no more jobs than
    // those before it do.
    let mut order: Vec<usize> = (0..jobs.len()).collect();
    order.sort_by_key(|n| ancestors[*n].as_ref().map_or(0, BTreeSet::len));

    // A job that runs where one before it failed is in a run only where one
    // of those that may fail is: with none before it in the run, it would
    // run first, where nothing failed. One none of whose jobs may fail (a
    // failure they may have ends as a success) is in no run, as GitLab ran
    // it in no pipeline.
    for &n in &order {
        if jobs[n].out || jobs[n].when != When::OnFailure {
            continue;
        }
        let may_fail: Vec<usize> = deps[n]
            .iter()
            .copied()
            .filter(|u| !jobs[*u].out && !jobs[*u].allow_failure)
            .collect();
        let mut gated: BTreeMap<Event, Names> = BTreeMap::new();
        for (event, names) in &present[n] {
            let some = may_fail.iter().fold(Names::none(), |all, u| {
                all.or(present[*u].get(event).unwrap_or(&Names::none()))
            });
            gated.insert(*event, names.and(&some));
        }
        let said = written(&gated, &events);
        let at = jobs[n].id.clone();
        if said.present.is_empty() {
            jobs[n].out = true;
            present[n] = BTreeMap::new();
            importer.report.note(&at, Kind::Left, "it runs where a job before it failed, and no job before it in a pipeline here may fail it (a job that may fail ends as a success, as GitLab did not count its failure): no pipeline here has it");
            continue;
        }
        for event in &said.wider {
            importer.report.todo(&mut jobs[n].todo, &format!("{at}.when"), format!("it runs where a job before it failed, and is in a run only where one of those is, which a when: cannot say of every {}: where none of them is, it runs as a run's first job", event.word()));
        }
        jobs[n].fields.retain(|(k, _)| *k != "when");
        if let Some(when) = said.when {
            jobs[n].set("when", Value::String(when));
        }
        present[n] = said.present;
    }
    for listed in &mut deps {
        listed.retain(|d| !jobs[*d].out);
    }

    // A job waits for those the jobs it waits for do not wait for already:
    // where each job between is in a run wherever the job is, and runs
    // where those before it succeeded. A job left out of a run takes its
    // place in the order with it, and one that runs on what became of the
    // jobs before it, whatever that was, holds none of theirs back.
    let full = deps.clone();
    for n in 0..jobs.len() {
        if !by_stage[n] || jobs[n].out || matches!(jobs[n].when, When::OnFailure | When::Always) {
            continue;
        }
        let through = |x: usize| jobs[x].when == When::OnSuccess && covers(&present, x, n);
        let mut implied: BTreeSet<usize> = BTreeSet::new();
        let mut walk: Vec<usize> = full[n].iter().copied().filter(|o| through(*o)).collect();
        let mut seen: BTreeSet<usize> = BTreeSet::new();
        while let Some(x) = walk.pop() {
            if !seen.insert(x) {
                continue;
            }
            for &y in &full[x] {
                implied.insert(y);
                if through(y) {
                    walk.push(y);
                }
            }
        }
        deps[n].retain(|d| !implied.contains(d));
    }
    for n in 0..jobs.len() {
        if jobs[n].out {
            continue;
        }
        let names: Vec<String> = deps[n].iter().map(|d| jobs[*d].name.clone()).collect();
        if !names.is_empty() {
            jobs[n].set("depends_on", list(names));
        }
    }

    // Files a job read of the jobs before it.
    for n in 0..jobs.len() {
        if jobs[n].out {
            continue;
        }
        let from: Vec<usize> = match (&jobs[n].dependencies, &jobs[n].needs) {
            (Some(listed), _) => listed.iter().filter_map(|d| ids.get(d).copied()).collect(),
            (None, Some(needs)) => needs
                .iter()
                .filter(|need| need.artifacts)
                .filter_map(|need| ids.get(&need.job).copied())
                .collect(),
            (None, None) => (0..jobs.len())
                .filter(|d| jobs[*d].stage < jobs[n].stage)
                .collect(),
        };
        let keeping: Vec<String> = from
            .iter()
            .filter(|d| jobs[**d].keeps_files && !jobs[**d].out && meets(&present, **d, n))
            .map(|d| jobs[*d].id.clone())
            .collect();
        if !keeping.is_empty() {
            let at = jobs[n].id.clone();
            importer.report.todo(&mut jobs[n].todo, &at, format!("the files {} kept (artifacts) were in its workspace before its script, and a job's files are not handed to the next here: where it reads them, it and {} are steps of one job; where it does not, nothing is lost", keeping.join(", "), keeping.join(", ")));
        }
    }

    // A newer run in another's place.
    let interruptible = doc
        .iter()
        .filter(|(k, _)| scalar(k).is_some_and(|k| !GLOBAL_KEYS.contains(&k.as_str())))
        .any(|(_, v)| {
            v.as_mapping().and_then(|m| get(m, "interruptible")) == Some(&Value::Bool(true))
        })
        || importer.defaults.get(key("interruptible")) == Some(&Value::Bool(true));

    // The tasks, an approval before each manual job.
    let mut tasks = Vec::new();
    let mut taken: BTreeSet<String> = jobs.iter().map(|j| j.name.clone()).collect();
    for mut job in jobs {
        if job.out {
            continue;
        }
        if job.when == When::Manual {
            let mut gate = format!("approve-{}", job.name);
            while !taken.insert(gate.clone()) {
                gate.push('_');
            }
            let mut approval = Mapping::new();
            approval.insert(key("name"), Value::String(gate.clone()));
            approval.insert(key("type"), key("approval"));
            for k in ["depends_on", "when"] {
                if let Some((_, v)) = job.fields.iter().find(|(f, _)| *f == k) {
                    approval.insert(key(k), v.clone());
                }
            }
            approval.insert(
                key("approval_message"),
                Value::String(format!("Run {}?", job.id)),
            );
            let at = job.id.clone();
            let said = match job.allow_failure {
                true => "a manual job is run by a person's approval here (a gate before it), and the run waits for that decision where GitLab's pipeline ended without it",
                false => "a manual job is run by a person's approval here (a gate before it)",
            };
            importer
                .report
                .note(&format!("{at}.when"), Kind::Changed, said);
            let mut settled = Value::Mapping(approval);
            let mut parted = Vec::new();
            settle(&mut settled, "", None, &mut parted);
            for place in parted {
                importer
                    .report
                    .todo(&mut job.todo, &format!("{gate}: {place}"), PARTED);
            }
            tasks.push(settled);
            job.set("depends_on", list([gate]));
        }
        let at = job.id.clone();
        let task = job.task();
        let todo = std::mem::take(&mut job.todo);
        tasks.push(
            importer
                .report
                .finish(&at, task, todo, false, job.single.as_deref(), None),
        );
    }

    let name = options
        .name
        .as_deref()
        .map(as_name)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "ci".to_string());
    let mut out = Mapping::new();
    out.insert(key("name"), Value::String(name));
    out.insert(key("on"), Value::Mapping(on));
    if interruptible {
        let mut concurrency = Mapping::new();
        concurrency.insert(key("key"), Value::String(tpl("ci_group")));
        concurrency.insert(key("cancel_superseded"), Value::Bool(true));
        out.insert(key("concurrency"), Value::Mapping(concurrency));
        importer.report.said_once("interruptible", Kind::Changed, "a newer run of a branch or merge request takes the older one's place, all its jobs, where GitLab cancelled the interruptible ones (concurrency: cancel_superseded)");
    }
    let mut head = Value::Mapping(out);
    let mut parted = Vec::new();
    settle(&mut head, "", None, &mut parted);
    if let Value::Mapping(head) = &mut head {
        head.insert(key("tasks"), Value::Sequence(tasks));
    }
    let pipeline = serde_yaml::to_string(&head).context("writing the pipeline")?;
    Ok(Some(Imported {
        source: "a GitLab CI pipeline",
        pipeline,
        notes: importer.report.into_notes(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn import(text: &str) -> Imported {
        gitlab_to_pipeline(text, &Options::default()).unwrap_or_else(|e| panic!("{e:#}"))
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
    fn an_include_is_read_from_within_the_repository_alone() {
        let base =
            std::env::temp_dir().join(format!("dagron-import-included-{}", std::process::id()));
        let repo = base.join("repo");
        std::fs::create_dir_all(repo.join("ci")).unwrap();
        std::fs::write(
            repo.join("ci/inside.yml"),
            "include: { local: ci/nested.yml }\n",
        )
        .unwrap();
        std::fs::write(repo.join("ci/nested.yml"), "nested: { script: [true] }\n").unwrap();
        std::fs::write(base.join("outside.yml"), "secret: { script: [true] }\n").unwrap();
        let pipeline = "include:\n  - local: ci/inside.yml\n  - local: ci/escape.yml\n  - local: ../outside.yml\n";
        #[cfg(unix)]
        std::os::unix::fs::symlink(base.join("outside.yml"), repo.join("ci/escape.yml")).unwrap();

        let read = read_included(&repo, pipeline);
        let names: Vec<&str> = read.keys().map(String::as_str).collect();
        // What the included file includes is read too; neither a link out
        // of the repository nor a path up from it is.
        assert_eq!(names, ["ci/inside.yml", "ci/nested.yml"]);
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// At most 150 files are read, a file written many ways counted once
    /// for each, and none over 2 MiB, as GitLab reads no more.
    #[test]
    fn what_is_read_from_the_repository_is_bounded() {
        let base =
            std::env::temp_dir().join(format!("dagron-import-bounded-{}", std::process::id()));
        let repo = base.join("repo");
        std::fs::create_dir_all(repo.join("ci/a/b/c/d/e/f/g")).unwrap();
        std::fs::write(
            repo.join("ci/a/b/c/d/e/f/g/one.yml"),
            "one: { script: [true] }\n",
        )
        .unwrap();
        // 256 ways to write one path: each of its 8 separators one slash or
        // two.
        let parts = ["ci", "a", "b", "c", "d", "e", "f", "g", "one.yml"];
        let mut pipeline = "include:\n".to_string();
        for n in 0..256u32 {
            let mut path = parts[0].to_string();
            for (i, part) in parts[1..].iter().enumerate() {
                path.push_str(if n >> i & 1 == 1 { "//" } else { "/" });
                path.push_str(part);
            }
            pipeline.push_str(&format!("  - local: {path}\n"));
        }
        let read = read_included(&repo, &pipeline);
        assert_eq!(read.len(), MAX_INCLUDED_FILES);
        assert!(read
            .values()
            .all(|text| text == "one: { script: [true] }\n"));

        let big = format!("big: {{ script: [true] }}\n#{}\n", "x".repeat(2 << 20));
        std::fs::write(repo.join("ci/big.yml"), big).unwrap();
        let read = read_included(
            &repo,
            "include:\n  - local: ci/big.yml\n  - local: ci/a/b/c/d/e/f/g/one.yml\n",
        );
        let names: Vec<&str> = read.keys().map(String::as_str).collect();
        assert_eq!(names, ["ci/a/b/c/d/e/f/g/one.yml"]);
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn values_are_read_as_gitlab_writes_them() {
        assert_eq!(duration_secs("1h 30m"), Some(5400));
        assert_eq!(duration_secs("90 minutes"), Some(5400));
        assert_eq!(duration_secs("3600"), Some(3600));
        assert_eq!(duration_secs("2 days"), Some(172_800));
        assert_eq!(duration_secs("30s"), Some(30));
        assert_eq!(duration_secs("1h 30m 10s"), Some(5410));
        assert_eq!(duration_secs("90 MINUTES"), Some(5400));
        assert_eq!(duration_secs("1 hrs"), Some(3600));
        assert_eq!(duration_secs("soon"), None);
        assert_eq!(named("$A-${B}_$$C $1 ${D"), vec!["A", "B"]);
        let vars = |n: &str| {
            (n == "A")
                .then(|| "x$B".to_string())
                .or_else(|| (n == "B").then(|| "y".into()))
        };
        assert_eq!(expanded("$A/$$A", &vars, 0), Ok("xy/$A".to_string()));
        assert_eq!(expanded("$NOPE", &vars, 0), Err(vec!["NOPE".to_string()]));
        assert_eq!(
            double_quoted("a $B `c` \"d\" $$ $(e)"),
            "\"a $B \\`c\\` \\\"d\\\" \\$ \\$(e)\""
        );
        assert_eq!(literal_glob("feat*[x]"), "feat\\*\\[x\\]");
        assert_eq!(literal_glob("!x"), "\\!x");
        assert_eq!(
            image_base("registry.example.com:5000/team/postgres:16-alpine"),
            "postgres"
        );
    }

    #[test]
    fn a_job_waits_for_the_stages_before_its_own_or_for_what_it_needs() {
        let text = "image: alpine\nstages: [a, b, c]\none:\n  stage: a\n  script: [x]\ntwo:\n  stage: b\n  script: [x]\nthree:\n  stage: c\n  script: [x]\nfour:\n  stage: c\n  needs: [one]\n  script: [x]\nfive:\n  stage: c\n  needs: []\n  script: [x]\n";
        let read = pipeline(&import(text));
        let deps = |n: usize| serde_yaml::to_string(&read["tasks"][n]["depends_on"]).unwrap();
        assert_eq!(read["tasks"][0].get("depends_on"), None);
        assert_eq!(deps(1), "- one\n");
        // Two waits for one already: three need not say so.
        assert_eq!(deps(2), "- two\n");
        assert_eq!(deps(3), "- one\n");
        assert_eq!(read["tasks"][4].get("depends_on"), None);
    }

    #[test]
    fn a_job_waits_for_what_is_in_its_run_and_not_for_a_job_that_runs_on_failure() {
        let deps = |read: &Value, n: usize| -> Vec<String> {
            read["tasks"][n]
                .get("depends_on")
                .map(strings)
                .unwrap_or_default()
        };
        // GitLab's example: the test waits for the build, and not for the
        // cleanup that runs where the build failed, which it would wait for
        // to be skipped.
        let text = "image: alpine\nstages: [build, cleanup, test]\nbuild:\n  stage: build\n  script: [x]\ncleanup:\n  stage: cleanup\n  when: on_failure\n  script: [x]\ntest:\n  stage: test\n  script: [x]\n";
        let read = pipeline(&import(text));
        assert_eq!(deps(&read, 1), vec!["build"]);
        assert_eq!(read["tasks"][1]["trigger_rule"], "one_failed");
        assert_eq!(deps(&read, 2), vec!["build"]);
        // Where the scan is left out of a run, the deploy still waits for
        // the build: the scan does not stand for it.
        let text = "image: alpine\nstages: [build, scan, deploy]\nbuild:\n  stage: build\n  script: [x]\nscan:\n  stage: scan\n  script: [x]\n  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\ndeploy:\n  stage: deploy\n  script: [x]\n";
        let read = pipeline(&import(text));
        assert_eq!(deps(&read, 2), vec!["build", "scan"]);
        // One in every run the deploy is in does.
        let text = text.replace("  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\n", "");
        assert_eq!(deps(&pipeline(&import(&text)), 2), vec!["scan"]);
        // A job that runs where one before it failed is in a run only where
        // one of those is; where none may fail, in none.
        let text = "image: alpine\nstages: [deploy, rollback]\ndeploy:\n  stage: deploy\n  script: [x]\n  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\nrollback:\n  stage: rollback\n  when: on_failure\n  script: [x]\n";
        let read = pipeline(&import(text));
        assert_eq!(read["tasks"][0]["when"], read["tasks"][1]["when"]);
        let text = "image: alpine\nstages: [test, notify]\nflaky:\n  stage: test\n  allow_failure: true\n  script: [x]\npage:\n  stage: notify\n  when: on_failure\n  script: [x]\n";
        let imported = import(text);
        assert_eq!(pipeline(&imported)["tasks"].as_sequence().unwrap().len(), 1);
        assert!(notes(&imported, Kind::Left)[0]
            .starts_with("page: it runs where a job before it failed"));
    }

    #[test]
    fn a_job_that_may_fail_goes_on_and_is_waited_for() {
        let text = "image: alpine\nstages: [a, b]\nmigrate:\n  stage: a\n  allow_failure: true\n  script: [x]\ncodes:\n  stage: a\n  allow_failure:\n    exit_codes: [137, 255]\n  script: [x]\ngated:\n  stage: a\n  when: manual\n  script: [x]\ndeploy:\n  stage: b\n  script: [x]\n";
        let read = pipeline(&import(text));
        let task = |name: &str| {
            read["tasks"]
                .as_sequence()
                .unwrap()
                .iter()
                .find(|t| t["name"] == name)
                .unwrap()
                .clone()
        };
        assert_eq!(task("migrate")["steps"][0]["continue_on_error"], true);
        assert_eq!(task("migrate").get("allow_failure"), None);
        let run = task("codes")["steps"][0]["run"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(run.starts_with("set +e\n(\nset -e\nx\n)\n"), "{run}");
        assert!(run.contains("  137|255) echo"), "{run}");
        assert_eq!(task("codes")["steps"][0].get("continue_on_error"), None);
        // The deploy waits for both; a manual job that may be left unrun
        // holds nothing back.
        assert_eq!(
            strings(&task("deploy")["depends_on"]),
            vec!["migrate", "codes"]
        );
        // One a rule makes manual may not be left unrun.
        let text = "image: alpine\nstages: [a, b]\nprod:\n  stage: a\n  script: [x]\n  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\n      when: manual\nverify:\n  stage: b\n  script: [x]\n  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\n";
        let read = pipeline(&import(text));
        assert_eq!(read["tasks"][2]["name"], "verify");
        assert_eq!(strings(&read["tasks"][2]["depends_on"]), vec!["prod"]);
        assert_eq!(read["tasks"][1]["steps"][0].get("continue_on_error"), None);
    }

    #[test]
    fn a_gate_s_question_holds_no_template_of_the_file_s() {
        let imported = import("image: alpine\n\"deploy {{x}}\":\n  when: manual\n  script: [x]\n");
        let read = pipeline(&imported);
        assert_eq!(read["tasks"][0]["approval_message"], "Run deploy { {x}}?");
        let todo = notes(&imported, Kind::Todo);
        assert!(
            todo.iter()
                .any(|t| t.starts_with("approve-deploy-x: approval_message: the text holds")),
            "{todo:?}"
        );
    }

    #[test]
    fn rules_are_the_pipeline_s_events_and_a_job_s_when() {
        let text = "image: alpine\nworkflow:\n  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\n    - if: $CI_PIPELINE_SOURCE == \"merge_request_event\"\nbuild:\n  script: [make]\ndeploy:\n  script: [make deploy]\n  rules:\n    - if: $CI_COMMIT_BRANCH == \"main\"\n";
        let imported = import(text);
        let read = pipeline(&imported);
        assert_eq!(
            serde_yaml::to_string(&read["on"]).unwrap(),
            "push:\n  branches:\n  - main\npull_request: {}\ndispatch: {}\n"
        );
        // Each runs for a push to main, which on: lets in alone, and for a
        // run asked for on main, which on: cannot say: the pipeline's own
        // rules are each job's there. A job without rules is in every
        // pipeline workflow:rules lets be, a merge request's too.
        let dispatch = "{{ ci_event }} == dispatch and {{ ci_group }} in [refs/heads/main]";
        assert_eq!(
            read["tasks"][0]["when"],
            format!("{{{{ ci_event }}}} in [push, pull_request] or {dispatch}")
        );
        assert_eq!(
            read["tasks"][1]["when"],
            format!("{{{{ ci_event }}}} == push or {dispatch}")
        );
        assert_eq!(imported.todo(), 0, "{:#?}", notes(&imported, Kind::Todo));
    }

    #[test]
    fn what_cannot_be_read_runs_for_more_and_is_left_to_do() {
        let text = "image: alpine\nnightly:\n  script: [make]\n  rules:\n    - if: $RUN_NIGHTLY == \"1\"\n";
        let imported = import(text);
        let todo = notes(&imported, Kind::Todo);
        assert!(
            todo.len() == 1 && todo[0].contains("$RUN_NIGHTLY cannot be read before a run"),
            "{todo:?}"
        );
        assert_eq!(pipeline(&imported)["tasks"][0].get("when"), None);
        assert_eq!(
            pipeline(&imported)["tasks"][0]["steps"][0]["name"],
            "imported: to do"
        );
    }

    #[test]
    fn a_script_is_given_what_it_reads_of_gitlab_s_variables() {
        let text = "image: alpine\nvariables:\n  IMAGE: $CI_REGISTRY_IMAGE/app\nbuild:\n  script:\n    - echo \"$CI_COMMIT_SHORT_SHA on $CI_COMMIT_BRANCH\"\n";
        let imported = import(text);
        let run = pipeline(&imported)["tasks"][0]["steps"][1]["run"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(run.starts_with("export CI_COMMIT_SHORT_SHA="), "{run}");
        assert!(
            run.contains("case \"$CI_REF\" in refs/heads/*) export CI_COMMIT_BRANCH="),
            "{run}"
        );
        assert!(
            run.contains("export IMAGE=\"$CI_REGISTRY_IMAGE/app\""),
            "{run}"
        );
        let todo = notes(&imported, Kind::Todo);
        assert!(
            todo.len() == 1
                && todo[0].contains("$CI_REGISTRY_IMAGE, which a step here is not given"),
            "{todo:?}"
        );
    }

    #[test]
    fn a_file_that_is_no_pipeline_is_refused() {
        let refused = |text: &str| {
            gitlab_to_pipeline(text, &Options::default())
                .unwrap_err()
                .to_string()
        };
        assert!(refused("a: [b").contains("not YAML"));
        assert!(refused("- a\n").contains("a mapping of jobs"));
        assert!(refused("stages: [a]\n.hidden:\n  script: [x]\n").contains("has no job"));
        assert!(refused("a b:\n  script: [x]\na-b:\n  script: [y]\n")
            .contains("two jobs come to the name"));
        assert!(
            refused("a:\n  needs: [b]\n  script: [x]\nb:\n  needs: [a]\n  script: [y]\n")
                .contains("needs itself")
        );
    }

    #[test]
    fn included_files_are_read_and_others_left_to_do() {
        let text = "include:\n  - local: /ci/a.yml\n  - remote: https://x/y.yml\n  - ci/missing.yml\nb:\n  extends: .a\n  script: [x]\n";
        assert_eq!(
            local_includes(text),
            vec!["ci/a.yml".to_string(), "ci/missing.yml".to_string()]
        );
        let options = Options {
            included: BTreeMap::from([(
                "ci/a.yml".to_string(),
                ".a:\n  image: alpine\n".to_string(),
            )]),
            ..Options::default()
        };
        let imported = gitlab_to_pipeline(text, &options).unwrap();
        assert_eq!(pipeline(&imported)["tasks"][0]["docker_image"], "alpine");
        let todo = notes(&imported, Kind::Todo);
        assert_eq!(todo.len(), 2, "{todo:?}");
        assert!(todo[0].starts_with("include[1]: the file at https://x/y.yml"));
        assert!(todo[1].starts_with("include[2]: ci/missing.yml was not read"));
    }
}
